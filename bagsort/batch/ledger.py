"""The sorting batch ledger: countdown, idempotent close and sealed membership.

The ledger owns three invariants, and every public operation stages its
journal commands through one store session so they commit atomically:

* **Uniqueness** — ``(flight_id, number)`` is unique (it is the batch id)
  and one bag is booked into at most one batch.
* **Irreversible close** — a closed batch rejects every further item; the
  journal reducer enforces this as well, so replay cannot smuggle a line
  in. The close entry carries the journal commit sequence and a content
  digest chained to the previous closed batch of the same flight, making
  the boundary a tamper-evident, non-revertible commit point.
* **Idempotence** — closing a batch that is already closed returns the
  stored batch and writes nothing. A duplicate trigger can never close the
  same batch twice.

Membership is booked at chute allocation time (see :mod:`bagsort.batch.policy`).
When an allocation lands at/after the countdown boundary the ledger closes
the batch (carousel handoff staged in the same commit), opens the next one
and books the late bag there, all in one transaction.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Any, Protocol

from bagsort.clock import Clock
from bagsort.errors import OrderingError, ValidationError
from bagsort.model.entities import BatchItem, SortBatch
from bagsort.model.enums import BatchState
from bagsort.model.errors import unknown_flight
from bagsort.store.store import Session, Store
from .policy import (
    DEFAULT_WINDOW_SECONDS,
    is_due,
    planned_close,
    policy_description,
)

GENESIS_DIGEST = "0" * 64


class Handoff(Protocol):
    """The belt step a close commits together with its boundary.

    The carousel is cleared, producing the batch's load records, and the
    next batch is announced only after that. Doing it inside the ledger's
    session is what ties ledger boundary, load records and carousel state
    into one commit point. The close digest seals the post-announcement
    carousel state, which is exactly what a full replay rebuilds there.
    """

    def stage_handoff(
        self,
        session: Session,
        carousel: str,
        closed_number: int,
        next_number: int,
        at: str,
    ) -> None: ...


def batch_id(flight_id: str, number: int) -> str:
    return f"{flight_id}.b{number:04d}"


def close_material(batch: SortBatch, item_bag_ids: list[str], belt: dict[str, Any]) -> dict[str, Any]:
    """The facts sealed into a close digest.

    Membership, the boundary timestamps, the commit point and the carousel
    snapshot are all covered: changing any one of them after the fact
    changes the digest and breaks the per-flight chain.
    """

    return {
        "batch_id": batch.id,
        "flight_id": batch.flight_id,
        "number": batch.number,
        "carousel": batch.carousel,
        "opened_at": batch.opened_at,
        "planned_close_at": batch.planned_close_at,
        "closed_at": batch.closed_at,
        "commit_seq": batch.commit_seq,
        "item_bag_ids": sorted(item_bag_ids),
        "item_count": len(item_bag_ids),
        "belt": dict(belt),
    }


def digest_material(material: dict[str, Any]) -> str:
    raw = json.dumps(material, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def item_bag_ids(state: Any, batch_id_value: str) -> list[str]:
    return [item.bag_id for item in state.batch_items if item.batch_id == batch_id_value]


def belt_snapshot(state: Any) -> dict[str, Any]:
    return {
        "carousel": state.belt_carousel,
        "batch": state.belt_batch,
        "on_carousel": len(state.belt_on_carousel),
    }


@dataclass(frozen=True)
class Attribution:
    """Result of booking one chute allocation."""

    item: BatchItem
    batch: SortBatch
    closed: tuple[SortBatch, ...] = ()


def stage_attribution(
    ledger: "BatchLedger",
    session: Session,
    bag_id: str,
    flight_id: str,
    chute_id: str,
    at: str,
    handoff: Handoff | None = None,
    carousel: str = "",
    window_seconds: int = DEFAULT_WINDOW_SECONDS,
) -> Attribution:
    """Book one allocation against an open session; used by ledger and sorter.

    It first rolls the flight forward through every batch whose boundary has
    passed (closing each and opening the next one exactly at that boundary),
    so the windows are contiguous and history recomputes to the same batch
    numbers. A duplicate bag id is idempotent: its existing line is returned
    and nothing is staged.
    """

    for existing in session.state.batch_items:
        if existing.bag_id == bag_id:
            return Attribution(
                item=existing,
                batch=session.state.batches[existing.batch_id],
                closed=(),
            )
    ledger.require_flight(flight_id, session.state)
    open_batch = _open_batch(session.state, flight_id)
    if open_batch is None:
        latest = _latest_batch(session.state, flight_id)
        chosen_carousel = (
            latest.carousel
            if latest is not None
            else session.state.flights[flight_id].carousel or carousel.strip()
        )
        chosen_window = latest.window_seconds if latest is not None else window_seconds
        open_batch = ledger.stage_open(
            session, flight_id, chosen_carousel, chosen_window, at
        )
    rolled = ledger.stage_roll_to_time(session, open_batch, handoff, at)
    target = _open_batch(session.state, flight_id)
    item = ledger.stage_item(session, target, bag_id, flight_id, chute_id, at)
    return Attribution(item=item, batch=target, closed=tuple(rolled))


class BatchLedger:
    """Opens batches, counts them down and seals them."""

    def __init__(self, store: Store, clock: Clock) -> None:
        self._store = store
        self._clock = clock

    # -- read side ---------------------------------------------------------

    def get(self, batch_id_value: str) -> SortBatch:
        batch = self._store.state().batches.get(batch_id_value)
        if batch is None:
            raise ValidationError(f"batch {batch_id_value!r} does not exist")
        return batch

    def list_for(self, flight_id: str) -> list[SortBatch]:
        batches = self._store.state().batches.values()
        return sorted(
            (batch for batch in batches if batch.flight_id == flight_id),
            key=lambda batch: batch.number,
        )

    def open_for(self, flight_id: str) -> SortBatch | None:
        for batch in self.list_for(flight_id):
            if batch.state is BatchState.OPEN:
                return batch
        return None

    def items_for(self, batch_id_value: str) -> list[BatchItem]:
        return self._items_of(batch_id_value)

    def history(self) -> list[BatchItem]:
        return list(self._store.state().batch_items)

    def item_for_bag(self, bag_id: str) -> BatchItem | None:
        for item in self._store.state().batch_items:
            if item.bag_id == bag_id:
                return item
        return None

    def policy(self) -> dict[str, str]:
        return policy_description()

    # -- opening -----------------------------------------------------------

    def arm(
        self,
        flight_id: str,
        carousel: str = "",
        window_seconds: int = DEFAULT_WINDOW_SECONDS,
    ) -> SortBatch:
        """Open the current batch for a flight.

        Re-arming while a batch is open is a no-op that returns the open
        batch, so retried requests never mint a second current batch.
        """

        self.require_flight(flight_id)
        if window_seconds < 0:
            raise ValidationError("batch window must not be negative")
        with self._store.session() as session:
            existing = _open_batch(session.state, flight_id)
            if existing is not None:
                return existing
            batch = self.stage_open(session, flight_id, carousel.strip(), window_seconds)
        return batch

    # -- closing -----------------------------------------------------------

    def close_batch(self, batch_id_value: str, handoff: Handoff | None = None) -> SortBatch:
        """Seal a batch whose countdown has run out.

        Closing an already closed batch is idempotent (no journal write).
        Closing an open batch before its boundary raises; the system closes
        batches by the countdown, not by operator feeling.
        """

        at = self._clock.now_iso()
        with self._store.session() as session:
            batch = session.state.batches.get(batch_id_value)
            if batch is None:
                raise ValidationError(f"batch {batch_id_value!r} does not exist")
            if batch.state is BatchState.CLOSED:
                return batch
            if not is_due(batch.planned_close_at, at):
                raise OrderingError(
                    f"batch {batch_id_value!r} closes at {batch.planned_close_at}, "
                    f"not before"
                )
            return self.stage_roll_to_time(session, batch, handoff, at)[0]

    def tick(self, handoff: Handoff | None = None) -> list[SortBatch]:
        """Close every open batch whose countdown has elapsed.

        Closing also opens each successor at the boundary, so after a tick
        every flight has a fresh open batch ready for late baggage.
        """

        at = self._clock.now_iso()
        due = [
            batch
            for batch in self._store.state().batches.values()
            if batch.state is BatchState.OPEN and is_due(batch.planned_close_at, at)
        ]
        closed: list[SortBatch] = []
        for flight_id in sorted({batch.flight_id for batch in due}):
            with self._store.session() as session:
                live = _open_batch(session.state, flight_id)
                if live is not None and is_due(live.planned_close_at, at):
                    closed.extend(self.stage_roll_to_time(session, live, handoff, at))
        return sorted(closed, key=lambda item: (item.flight_id, item.number))

    # -- staging primitives ------------------------------------------------

    def stage_open(
        self,
        session: Session,
        flight_id: str,
        carousel: str,
        window_seconds: int,
        at: str = "",
    ) -> SortBatch:
        at = at or self._clock.now_iso()
        number = _next_number(session.state, flight_id)
        batch = SortBatch(
            id=batch_id(flight_id, number),
            flight_id=flight_id,
            number=number,
            carousel=carousel,
            state=BatchState.OPEN,
            window_seconds=window_seconds,
            opened_at=at,
            planned_close_at=planned_close(at, window_seconds),
        )
        session.state.batches[batch.id] = batch
        session.record("batch.open", {"batch": batch.as_dict()}, at)
        return batch

    def stage_close(
        self,
        session: Session,
        batch: SortBatch,
        handoff: Handoff | None,
    ) -> SortBatch:
        """Seal one batch at its promised boundary and open its successor.

        The successor opens *at* ``planned_close_at`` with the same window,
        keeping the batch intervals contiguous — there is never a gap a later
        recomputation could disagree about. Returns the sealed batch.
        """

        closed_at = batch.planned_close_at
        if handoff is not None:
            handoff.stage_handoff(
                session,
                carousel=batch.carousel,
                closed_number=batch.number,
                next_number=batch.number + 1,
                at=closed_at,
            )
        commit_seq = session.next_sequence()
        sealed = batch.evolved(
            state=BatchState.CLOSED,
            closed_at=closed_at,
            commit_seq=commit_seq,
            prev_digest=_last_digest(session.state, batch.flight_id),
        )
        material = close_material(sealed, item_bag_ids(session.state, sealed.id), belt_snapshot(session.state))
        sealed = sealed.evolved(digest=digest_material(material))
        session.state.batches[sealed.id] = sealed
        session.record(
            "batch.close",
            {"batch": sealed.as_dict(), "material": material},
            closed_at,
        )
        self.stage_open(
            session,
            batch.flight_id,
            batch.carousel,
            batch.window_seconds,
            closed_at,
        )
        return sealed

    def stage_roll_to_time(
        self,
        session: Session,
        open_batch: SortBatch,
        handoff: Handoff | None,
        at: str,
    ) -> list[SortBatch]:
        """Close every contiguous batch due by ``at``; successor is left open."""

        rolled: list[SortBatch] = []
        current = open_batch
        while current.state is BatchState.OPEN and is_due(current.planned_close_at, at):
            sealed = self.stage_close(session, current, handoff)
            rolled.append(sealed)
            nxt = _open_batch(session.state, current.flight_id)
            if nxt is None or nxt.number <= sealed.number:
                break
            current = nxt
        return rolled

    def stage_item(
        self,
        session: Session,
        batch: SortBatch,
        bag_id: str,
        flight_id: str,
        chute_id: str,
        at: str,
    ) -> BatchItem:
        item = BatchItem(
            bag_id=bag_id,
            batch_id=batch.id,
            flight_id=flight_id,
            number=batch.number,
            chute_id=chute_id,
            at=at,
        )
        session.state.batch_items.append(item)
        session.record("batch.item", {"item": item.as_dict()}, at)
        return item

    # -- helpers -----------------------------------------------------------

    def _items_of(self, batch_id_value: str) -> list[BatchItem]:
        return [
            item for item in self._store.state().batch_items if item.batch_id == batch_id_value
        ]

    def require_flight(self, flight_id: str, state: Any = None) -> None:
        view = state or self._store.state()
        if flight_id not in view.flights:
            raise unknown_flight(flight_id)


def _open_batch(state: Any, flight_id: str) -> SortBatch | None:
    for batch in state.batches.values():
        if batch.flight_id == flight_id and batch.state is BatchState.OPEN:
            return batch
    return None


def _latest_batch(state: Any, flight_id: str) -> SortBatch | None:
    batches = [batch for batch in state.batches.values() if batch.flight_id == flight_id]
    return max(batches, key=lambda item: item.number, default=None)


def _next_number(state: Any, flight_id: str) -> int:
    numbers = [
        batch.number for batch in state.batches.values() if batch.flight_id == flight_id
    ]
    return (max(numbers) + 1) if numbers else 1


def _last_digest(state: Any, flight_id: str) -> str:
    digest = GENESIS_DIGEST
    for batch in sorted(
        (
            batch
            for batch in state.batches.values()
            if batch.flight_id == flight_id and batch.state is BatchState.CLOSED
        ),
        key=lambda item: item.number,
    ):
        digest = batch.digest or digest
    return digest
