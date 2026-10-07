"""Dispatch: the step that turns a committed reading into a sort record.

Dispatch refuses an uncommitted reading outright, and it refuses a bag that has
already been sorted. Both refusals are the contracts other subsystems rely on:
the desk must commit before routing, and a retried induction must not create a
second sort record.

When a batch attributor is wired, dispatch books the bag into its sorting
batch in the same transaction as the sort record. A late allocation rolls the
due batch over there and then, and returns which batches were sealed.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

from bagsort.clock import Clock
from bagsort.errors import ConflictError
from bagsort.model.entities import Bag, Reading, SortRecord
from bagsort.model.enums import BagState
from bagsort.sequence.verdict import VerdictTracker
from bagsort.store.store import Store
from bagsort.chute.assigner import ChuteAssigner
from bagsort.flight.mapping import BagMapping
from .records import SortLog
from .route import RouteDetail


class Attributor(Protocol):
    """Stages batch membership inside dispatch's session."""

    def stage_for(self, session, bag_id: str, flight_id: str, chute_id: str, at: str): ...


@dataclass(frozen=True)
class DispatchResult:
    """The sort record plus the batch boundary consequences of the dispatch."""

    record: SortRecord
    batch_id: str
    batch_number: int
    closed_batch_ids: tuple[str, ...] = ()

    @property
    def was_late(self) -> bool:
        return bool(self.closed_batch_ids)


class Sorter:
    """Routes committed bags to their flight's current chute."""

    def __init__(
        self,
        store: Store,
        mapping: BagMapping,
        chutes: ChuteAssigner,
        clock: Clock,
        verdict: VerdictTracker | None = None,
        attributor: Attributor | None = None,
    ) -> None:
        self._store = store
        self._mapping = mapping
        self._chutes = chutes
        self._clock = clock
        self._verdict = verdict or VerdictTracker()
        self._attributor = attributor
        self._log = SortLog(store)

    @property
    def log(self) -> SortLog:
        return self._log

    def route(self, bag_id: str) -> RouteDetail:
        flight_id = self._mapping.flight_of(bag_id)
        assignment = self._chutes.require(flight_id)
        return RouteDetail(
            bag_id=bag_id,
            flight_id=flight_id,
            chute_id=assignment.chute_id,
            generation=assignment.generation,
        )

    def dispatch(self, bag: Bag, reading: Reading) -> DispatchResult:
        if not reading.committed:
            raise ConflictError(f"barcode read for {bag.barcode!r} is not committed")
        if self._log.was_executed(bag.id):
            raise ConflictError(f"bag {bag.id!r} has already been sorted")
        detail = self.route(bag.id)
        at = self._clock.now_iso()
        record = SortRecord(
            bag_id=bag.id,
            flight_id=detail.flight_id,
            chute_id=detail.chute_id,
            sequence=bag.sequence,
            epoch=bag.epoch,
            at=at,
        )
        updated = bag.evolved(chute_id=detail.chute_id, state=BagState.SORTED)
        closed_batch_ids: tuple[str, ...] = ()
        batch_id = ""
        batch_number = 0
        with self._store.session() as session:
            session.state.sort_records.append(record)
            session.state.bags[updated.id] = updated
            session.record(
                "sort.record", {"record": record.as_dict()}, at
            )
            session.record("bag.put", {"bag": updated.as_dict()}, at)
            session.record(
                "counter.add", {"name": "sorter.dispatched", "delta": 1}, at
            )
            if self._attributor is not None:
                attribution = self._attributor.stage_for(
                    session,
                    bag_id=bag.id,
                    flight_id=detail.flight_id,
                    chute_id=detail.chute_id,
                    at=at,
                )
                batch_id = attribution.batch.id
                batch_number = attribution.batch.number
                closed_batch_ids = tuple(sealed.id for sealed in attribution.closed)
        return DispatchResult(
            record=record,
            batch_id=batch_id,
            batch_number=batch_number,
            closed_batch_ids=closed_batch_ids,
        )

    def divert(self, bag: Bag) -> Bag:
        updated = bag.evolved(state=BagState.DIVERTED)
        with self._store.session() as session:
            session.state.bags[updated.id] = updated
            session.record("bag.put", {"bag": updated.as_dict()}, self._clock.now_iso())
            session.record(
                "counter.add", {"name": "sorter.diverted", "delta": 1}, self._clock.now_iso()
            )
        return updated

    def verdict(self, epoch: int, sequence: int):
        """Classify an observed pair and advance the mark when it is fresh."""

        return self._verdict.observe(epoch, sequence)

    def is_fresh(self, epoch: int, sequence: int) -> bool:
        return self._verdict.is_fresh(epoch, sequence)

    def high_water(self) -> tuple[int, int]:
        return self._verdict.high_water()

    def was_executed(self, bag_id: str) -> bool:
        return self._log.was_executed(bag_id)

    def records(self) -> list[SortRecord]:
        return self._log.records()

    def assign_chute(self, flight_id: str, chute_id: str):
        """Convenience for the injection belt, which sets the lane then dispatches."""

        return self._chutes.assign(flight_id, chute_id)
