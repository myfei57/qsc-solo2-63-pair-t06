"""Batch lines: the system, not the operator, decides when a batch closes.

A *batch line* is opened on one carousel with a fixed countdown window. The
line then rolls on a regular time lattice: batch ``n`` owns the half-open
interval ``[opened_at, closes_at)`` and batch ``n+1`` starts exactly where
batch ``n`` ends. Closing is triggered lazily — every batch-sensitive action
settles expired batches first — so the close instant depends only on the
clock, never on an operator's judgement or on whether the belt has drained.

**Which clock a late bag is judged by.** A bag joins the batch that is open
at the moment the line *commits it to the carousel* (the placement gate),
never the batch that was open when it arrived at check-in. We chose the
chute-assignment side of the boundary and gave up the arrival side: a bag
that reached the plant before the countdown hit zero but reached the
carousel after it is booked into the next batch, even though it "was here on
time". What that buys: a batch's member list is exactly what physically sat
on the carousel when the boundary committed, so the boundary, the load
records and the carousel state are three names for the same set and the
carousel always reconciles. Judging by arrival instead would either hold the
batch open until every arrived bag drains (a close instant that depends on
sorter congestion — the guesswork this module exists to remove) or commit a
manifest the carousel has not loaded yet (the mismatch the carousel crew
keeps reporting).

**The commit point.** Closing writes, in one journal commit: the load
records for everything on the carousel, the carousel clear, the next batch
number, the boundary record itself and the opening of the successor batch.
The boundary is append-only and hash-chained to the previous boundary on the
same carousel; the reducer refuses a second boundary for the same
``(carousel, batch)``, so a batch is closed at most once no matter how often
the trigger fires. Once a batch is closed its member list is frozen — the
placement gate only ever targets the currently open batch, and late bags are
re-booked into it automatically.

Because every step carries the synthetic clock's timestamp, replaying the
journal prefix up to any instant rebuilds the batch history as of that
instant; :class:`bagsort.batch.history.BatchHistory` does exactly that.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta
from typing import Any

from bagsort.belt.belt import Belt
from bagsort.clock import Clock
from bagsort.errors import ConflictError, OrderingError, StaleError, ValidationError
from bagsort.model.entities import Bag, BatchBoundary, BatchRun, LoadRecord
from bagsort.model.enums import BagState
from bagsort.model.errors import unknown_batch_line
from bagsort.model.validate import require_text
from bagsort.store.store import Store
from .digest import GENESIS, boundary_digest
from .history import BatchHistory, BatchView


@dataclass(frozen=True)
class Placement:
    """The outcome of pushing one bag through the batch gate."""

    bag_id: str
    carousel: str
    batch: int
    on_carousel: int
    closed: tuple[BatchBoundary, ...] = ()

    def as_dict(self) -> dict[str, Any]:
        return {
            "bag_id": self.bag_id,
            "carousel": self.carousel,
            "batch": self.batch,
            "on_carousel": self.on_carousel,
            "closed": [boundary.as_dict() for boundary in self.closed],
        }


class BatchController:
    """Owns the open batch of every carousel line and closes them on time."""

    def __init__(self, store: Store, clock: Clock, belt: Belt) -> None:
        self._store = store
        self._clock = clock
        self._belt = belt
        self._history = BatchHistory(store)

    # -- line lifecycle ----------------------------------------------------

    def open_line(self, carousel: str, window_seconds: int, start_batch: int = 1) -> BatchRun:
        """Open a countdown-driven batch line on one carousel.

        The belt physically serves one carousel, so a second line cannot open
        while another is running; retire the running one first.
        """

        carousel = require_text(carousel, "carousel")
        if window_seconds < 1:
            raise ValidationError("window_seconds must be at least one")
        if start_batch < 0:
            raise OrderingError("batch number must not be negative")
        state = self._store.state()
        if carousel in state.batch_lines:
            raise ConflictError(f"carousel {carousel!r} already runs a batch line")
        if state.batch_lines:
            running = sorted(state.batch_lines)[0]
            raise ConflictError(
                f"the belt serves one carousel at a time; {running!r} is still open"
            )
        if state.belt_on_carousel and state.belt_carousel != carousel:
            raise ConflictError("the carousel still holds bags from another line")
        now = self._clock.now()
        run = BatchRun(
            carousel=carousel,
            batch=start_batch,
            opened_at=self._clock.iso(now),
            closes_at=self._clock.iso(now + timedelta(seconds=window_seconds)),
            window_seconds=window_seconds,
        )
        with self._store.session() as session:
            session.state.batch_lines[carousel] = run
            session.record("batch.open", run.as_dict(), run.opened_at)
            session.state.belt_carousel = carousel
            session.state.belt_batch = start_batch
            session.record(
                "belt.set", {"carousel": carousel, "batch": start_batch}, run.opened_at
            )
        return run

    def retire(self, carousel: str) -> BatchBoundary:
        """Close the open batch immediately and take the line down."""

        carousel = require_text(carousel, "carousel")
        run = self._store.state().batch_lines.get(carousel)
        if run is None:
            raise unknown_batch_line(carousel)
        boundary, _created = self.close(carousel, run.batch, reason="retired")
        with self._store.session() as session:
            session.state.batch_lines.pop(carousel, None)
            session.record("batch.retire", {"carousel": carousel}, self._clock.now_iso())
        return boundary

    def line(self, carousel: str) -> BatchRun:
        run = self._store.state().batch_lines.get(carousel)
        if run is None:
            raise unknown_batch_line(carousel)
        return run

    def lines(self) -> list[BatchRun]:
        lines = self._store.state().batch_lines
        return [lines[key] for key in sorted(lines)]

    def status(self) -> list[dict[str, Any]]:
        """The countdown of every open line: what closes, and when."""

        state = self._store.state()
        now = self._clock.now()
        rows: list[dict[str, Any]] = []
        for run in self.lines():
            closes = self._clock.parse(run.closes_at)
            remaining = max(0, int((closes - now).total_seconds()))
            rows.append(
                {
                    "carousel": run.carousel,
                    "batch": run.batch,
                    "opened_at": run.opened_at,
                    "closes_at": run.closes_at,
                    "remaining_seconds": remaining,
                    "overdue": now >= closes,
                    "on_carousel": (
                        len(state.belt_on_carousel)
                        if state.belt_carousel == run.carousel
                        else 0
                    ),
                }
            )
        return rows

    # -- the gate every placement passes -----------------------------------

    def place(self, bag: Bag) -> Placement:
        """Attribute a bag to the batch open *right now* and load it.

        Expired batches are settled before the bag lands, so a bag that
        reaches the carousel after the countdown is always re-booked into the
        next batch — never stuffed into the closed one.
        """

        carousel = self._store.state().belt_carousel
        if not carousel:
            raise OrderingError("the belt is not bound to a carousel")
        if carousel not in self._store.state().batch_lines:
            raise unknown_batch_line(carousel)
        closed = self._settle(carousel)
        run = self.line(carousel)
        on_carousel = self._belt.place(bag)
        return Placement(
            bag_id=bag.id,
            carousel=carousel,
            batch=run.batch,
            on_carousel=on_carousel,
            closed=tuple(closed),
        )

    def tick(self) -> list[BatchBoundary]:
        """Settle every line whose countdown has expired."""

        closed: list[BatchBoundary] = []
        for run in self.lines():
            closed.extend(self._settle(run.carousel))
        return closed

    # -- closing, the idempotent commit point -------------------------------

    def close(self, carousel: str, batch: int, reason: str = "operator") -> tuple[BatchBoundary, bool]:
        """Close ``batch`` on ``carousel`` exactly once.

        Asking to close a batch that already has a committed boundary returns
        that boundary without writing anything; asking to close any batch
        other than the open one is a stale reference. The successor batch
        always opens on the scheduled lattice point, so an early manual close
        never shifts the windows history is recomputed from.
        """

        state = self._store.state()
        run = state.batch_lines.get(carousel)
        if run is None:
            raise unknown_batch_line(carousel)
        existing = self._find_boundary(carousel, batch)
        if existing is not None:
            return existing, False
        if run.batch != batch:
            raise StaleError(
                f"batch {batch} is not open on {carousel!r}; the open batch is {run.batch}"
            )
        if state.belt_carousel != carousel or state.belt_batch != batch:
            raise ConflictError(
                f"the belt drifted from the batch line: it serves "
                f"{state.belt_carousel}#{state.belt_batch}, not {carousel}#{batch}"
            )
        at = self._clock.now_iso()
        members = tuple(state.belt_on_carousel)
        previous = self._last_boundary(carousel)
        boundary = BatchBoundary(
            carousel=carousel,
            batch=batch,
            opened_at=run.opened_at,
            closes_at=run.closes_at,
            closed_at=at,
            reason=require_text(reason, "reason"),
            members=members,
            load_count=len(members),
            prev_digest=previous.digest if previous else GENESIS,
        )
        boundary = boundary.evolved(digest=boundary_digest(boundary))
        next_opened = self._clock.parse(run.closes_at)
        next_run = BatchRun(
            carousel=carousel,
            batch=batch + 1,
            opened_at=run.closes_at,
            closes_at=self._clock.iso(next_opened + timedelta(seconds=run.window_seconds)),
            window_seconds=run.window_seconds,
        )
        with self._store.session() as session:
            for bag_id in members:
                bag = session.state.bags.get(bag_id)
                if bag is not None and bag.state is not BagState.LOADED:
                    updated = bag.evolved(state=BagState.LOADED)
                    session.state.bags[bag_id] = updated
                    session.record("bag.put", {"bag": updated.as_dict()}, at)
                record = LoadRecord(bag_id=bag_id, carousel=carousel, batch=batch, at=at)
                session.state.load_records.append(record)
                session.record("load.append", {"record": record.as_dict()}, at)
            session.state.belt_on_carousel = []
            session.record("belt.clear", {"carousel": carousel, "batch": batch}, at)
            session.state.belt_carousel = carousel
            session.state.belt_batch = batch + 1
            session.record("belt.set", {"carousel": carousel, "batch": batch + 1}, at)
            stale = [
                command
                for command in session.state.transfer_commands
                if command.batch < batch + 1
            ]
            if stale:
                session.state.transfer_commands = [
                    command
                    for command in session.state.transfer_commands
                    if command.batch >= batch + 1
                ]
                session.record("transfer.clear_before", {"batch": batch + 1}, at)
            session.state.batch_boundaries.append(boundary)
            session.record("batch.close", {"boundary": boundary.as_dict()}, at)
            session.state.batch_lines[carousel] = next_run
            session.record("batch.open", next_run.as_dict(), at)
            session.record("counter.add", {"name": "batch.closed", "delta": 1}, at)
        return boundary, True

    # -- committed history ---------------------------------------------------

    def boundaries(self, carousel: str | None = None) -> list[BatchBoundary]:
        committed = self._store.state().batch_boundaries
        if carousel is None:
            return list(committed)
        return [boundary for boundary in committed if boundary.carousel == carousel]

    def recompute(self, until_at: str) -> BatchView:
        """Rebuild the batch world as it stood at ``until_at``."""

        text = require_text(until_at, "until_at")
        try:
            self._clock.parse(text)
        except ValueError as exc:
            raise ValidationError(f"until_at {text!r} is not a valid timestamp") from exc
        return self._history.recompute(text)

    def verify(self) -> list[str]:
        """Replay the journal and list every broken batch invariant."""

        return self._history.verify()

    # -- internals -------------------------------------------------------------

    def _settle(self, carousel: str) -> list[BatchBoundary]:
        """Close every expired batch on the line, oldest first."""

        closed: list[BatchBoundary] = []
        while True:
            run = self._store.state().batch_lines.get(carousel)
            if run is None or self._clock.now() < self._clock.parse(run.closes_at):
                return closed
            boundary, _created = self.close(carousel, run.batch, reason="countdown")
            closed.append(boundary)

    def _find_boundary(self, carousel: str, batch: int) -> BatchBoundary | None:
        for boundary in self._store.state().batch_boundaries:
            if boundary.carousel == carousel and boundary.batch == batch:
                return boundary
        return None

    def _last_boundary(self, carousel: str) -> BatchBoundary | None:
        last: BatchBoundary | None = None
        for boundary in self._store.state().batch_boundaries:
            if boundary.carousel == carousel:
                last = boundary
        return last
