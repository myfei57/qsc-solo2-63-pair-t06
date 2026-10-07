"""Carousel and batch transfer.

Switching a batch must clear the carousel before the new batch is announced and
must retire the previous batch's transfer commands. Doing either in the other
order is how bags ride across batches and how a stale command re-delivers a bag
to the carousel it already left.

When a sorting batch closes, the batch ledger calls :meth:`Belt.stage_handoff`
inside its own commit transaction: clear (load records), then announce. The
load records are stamped from the sealed batch ledger membership, so the
loader and the batch ledger can never disagree about a bag's batch.
"""

from __future__ import annotations

from typing import Any

from bagsort.clock import Clock
from bagsort.errors import OrderingError
from bagsort.ids import IdFactory
from bagsort.model.entities import Bag, LoadRecord, TransferCommand
from bagsort.model.enums import BagState
from bagsort.model.validate import validate_transfer
from bagsort.store.store import Session, Store


def _batch_for_bag(state: Any, bag_id: str, fallback_carousel: str, fallback_batch: int):
    """Resolve a bag's sealed (carousel, batch) from the batch ledger.

    Falls back to the carousel currently running for bags that were loaded
    outside batch control (legacy flow), keeping that path working.
    """

    item = next(
        (line for line in state.batch_items if line.bag_id == bag_id),
        None,
    )
    if item is None:
        return fallback_carousel, fallback_batch
    batch = state.batches.get(item.batch_id)
    carousel = batch.carousel if batch and batch.carousel else fallback_carousel
    return carousel, item.number


def stage_clear(session: Session, at: str) -> int:
    """Clear the running carousel into load records, inside a session.

    Each bag on the carousel is marked loaded and earns one immutable load
    record stamped with the batch it was booked into.
    """

    state = session.state
    pending = list(state.belt_on_carousel)
    if not pending:
        return 0
    for bag_id in pending:
        bag = state.bags.get(bag_id)
        if bag is not None and bag.state is not BagState.LOADED:
            updated = bag.evolved(state=BagState.LOADED)
            state.bags[bag_id] = updated
            session.record("bag.put", {"bag": updated.as_dict()}, at)
        carousel, batch = _batch_for_bag(state, bag_id, state.belt_carousel, state.belt_batch)
        record = LoadRecord(bag_id=bag_id, carousel=carousel, batch=batch, at=at)
        state.load_records.append(record)
        session.record("load.append", {"record": record.as_dict()}, at)
    state.belt_on_carousel = []
    session.record(
        "belt.clear",
        {"carousel": state.belt_carousel, "batch": state.belt_batch},
        at,
    )
    session.record("counter.add", {"name": "belt.loaded", "delta": len(pending)}, at)
    return len(pending)


def stage_handoff(
    session: Session,
    carousel: str,
    closed_number: int,
    next_number: int,
    at: str,
) -> None:
    """Clear the closed batch into load records at the boundary.

    The belt's running batch pointer is a *global* carousel pointer and may
    be serving several flights at once, so a per-flight ledger boundary does
    not announce the carousel's next batch here — that stays with the
    explicit :meth:`Belt.switch` operator. Clearing is all the boundary needs
    to pin the closed batch's load records to this commit point.
    """

    stage_clear(session, at)


class Belt:
    """The outbound carousel and the transfer commands that feed it."""

    def __init__(self, store: Store, clock: Clock, ids: IdFactory) -> None:
        self._store = store
        self._clock = clock
        self._ids = ids

    def carousel(self) -> str:
        return self._store.state().belt_carousel

    def batch(self) -> int:
        return self._store.state().belt_batch

    def describe(self) -> dict[str, object]:
        return {
            "carousel": self.carousel(),
            "batch": self.batch(),
            "on_carousel": len(self._store.state().belt_on_carousel),
        }

    def set(self, carousel: str, batch: int) -> dict[str, object]:
        if batch < 0:
            raise OrderingError("batch number must not be negative")
        with self._store.session() as session:
            session.state.belt_carousel = carousel.strip()
            session.state.belt_batch = batch
            session.record(
                "belt.set",
                {"carousel": carousel.strip(), "batch": batch},
                self._clock.now_iso(),
            )
        return self.describe()

    def place(self, bag: Bag) -> int:
        with self._store.session() as session:
            if bag.id not in session.state.belt_on_carousel:
                session.state.belt_on_carousel.append(bag.id)
            session.record("belt.place", {"bag_id": bag.id}, self._clock.now_iso())
        return len(self._store.state().belt_on_carousel)

    def place_batch(self, bags: list[Bag]) -> int:
        for bag in bags:
            self.place(bag)
        return len(self._store.state().belt_on_carousel)

    def on_carousel(self) -> list[Bag]:
        state = self._store.state()
        return [state.bags[bag_id] for bag_id in state.belt_on_carousel if bag_id in state.bags]

    def loads(self) -> list[LoadRecord]:
        return list(self._store.state().load_records)

    def stage_handoff(
        self,
        session: Session,
        carousel: str,
        closed_number: int,
        next_number: int,
        at: str,
    ) -> None:
        """Handoff protocol used by the batch ledger inside its transaction."""

        stage_handoff(session, carousel, closed_number, next_number, at)

    def switch(self, carousel: str, batch: int) -> int:
        """Clear the current batch, then announce the next one."""

        if batch < 0:
            raise OrderingError("batch number must not be negative")
        cleared = self._clear_current()
        self.set(carousel, batch)
        self.discard_stale(batch)
        return cleared

    def rotate(self) -> int:
        """Retire transfer commands older than the running batch."""

        if not self.carousel():
            return 0
        return self.discard_stale(self.batch())

    def append_transfer(self, flight_id: str, carousel: str, batch: int) -> TransferCommand:
        command = TransferCommand(
            id=self._ids.next(),
            flight_id=flight_id.strip(),
            carousel=carousel.strip(),
            batch=batch,
            created_at=self._clock.now_iso(),
        )
        validate_transfer(command)
        with self._store.session() as session:
            session.state.transfer_commands.append(command)
            session.record(
                "transfer.append", {"command": command.as_dict()}, self._clock.now_iso()
            )
        return command

    def transfers(self) -> list[TransferCommand]:
        return list(self._store.state().transfer_commands)

    def replay_transfers(self) -> list[TransferCommand]:
        """Commands that still belong to the batch currently on the carousel."""

        batch = self.batch()
        return [
            command
            for command in self._store.state().transfer_commands
            if command.batch == batch
        ]

    def discard_stale(self, floor: int) -> int:
        """Drop every transfer command older than ``floor``; returns how many went."""

        doomed = [
            command for command in self._store.state().transfer_commands if command.batch < floor
        ]
        if not doomed:
            return 0
        with self._store.session() as session:
            session.state.transfer_commands = [
                command
                for command in session.state.transfer_commands
                if command.batch >= floor
            ]
            session.record(
                "transfer.clear_before", {"batch": floor}, self._clock.now_iso()
            )
        return len(doomed)

    def _clear_current(self) -> int:
        state = self._store.state()
        if not state.belt_on_carousel:
            return 0
        at = self._clock.now_iso()
        with self._store.session() as session:
            return stage_clear(session, at)
