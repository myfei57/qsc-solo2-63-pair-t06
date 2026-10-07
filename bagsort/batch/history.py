"""Recompute batch history from the clock and reconcile it with the ledger.

The committed journal is the ground truth. This module rebuilds, for every
flight, which batch each bag *should* belong to if only the chute allocation
timestamps, the first batch's open instant and its window are known — no
peeking at the stored membership lines — and compares that recomputation
with what the ledger sealed.

Because the rule is a pure half-open window over timestamps, the
recomputation is deterministic: the same journal always folds to the same
history, on a restart or weeks after the fact.
"""

from __future__ import annotations

from dataclasses import dataclass

from bagsort.model.entities import BatchItem, SortBatch
from bagsort.model.enums import BatchState
from bagsort.model.state import State
from .ledger import GENESIS_DIGEST, digest_material
from .policy import in_current_window, is_due, planned_close


@dataclass(frozen=True)
class Window:
    """One recomputed batch interval and the bags that fall inside it."""

    flight_id: str
    number: int
    opened_at: str
    planned_close_at: str
    closed: bool
    bag_ids: tuple[str, ...] = ()
    late: bool = False

    def as_dict(self) -> dict[str, object]:
        return {
            "flight_id": self.flight_id,
            "number": self.number,
            "opened_at": self.opened_at,
            "planned_close_at": self.planned_close_at,
            "closed": self.closed,
            "late": self.late,
            "bag_ids": list(self.bag_ids),
        }


@dataclass(frozen=True)
class HistoryVerification:
    """Outcome of recomputing history and comparing it with the ledger."""

    ok: bool
    windows: tuple[Window, ...]
    mismatched_bags: tuple[str, ...] = ()
    broken_chains: tuple[str, ...] = ()
    load_mismatches: tuple[str, ...] = ()
    untracked_loads: tuple[str, ...] = ()

    def as_dict(self) -> dict[str, object]:
        return {
            "ok": self.ok,
            "windows": [window.as_dict() for window in self.windows],
            "mismatched_bags": list(self.mismatched_bags),
            "broken_chains": list(self.broken_chains),
            "load_mismatches": list(self.load_mismatches),
            "untracked_loads": list(self.untracked_loads),
        }


def recompute(state: State) -> list[Window]:
    """Rebuild batch windows from allocation timestamps, per flight.

    Window 1 is anchored at the first committed batch's ``opened_at`` (or
    the first allocation when the ledger was lazily created). Every later
    allocation at/after a boundary rolls the window forward; the next
    window opens exactly at the boundary with the same length. Bags in
    number >= 2 are flagged ``late``.
    """

    windows: list[Window] = []
    by_flight = _allocations_by_flight(state)
    now = _latest_stamp(state)
    for flight_id in sorted(by_flight):
        records = by_flight[flight_id]
        anchor, window_seconds = _anchor(state, flight_id, records[0].at)
        number = 1
        opened_at = anchor
        boundary = planned_close(opened_at, window_seconds)
        members: list[str] = []
        for record in records:
            while not in_current_window(record.at, boundary):
                windows.append(
                    Window(
                        flight_id, number, opened_at, boundary, True,
                        tuple(members), number > 1,
                    )
                )
                number += 1
                opened_at = boundary
                boundary = planned_close(opened_at, window_seconds)
                members = []
            members.append(record.bag_id)
        closed = bool(now and is_due(boundary, now))
        windows.append(
            Window(
                flight_id, number, opened_at, boundary, closed,
                tuple(members), number > 1,
            )
        )
    return windows


def verify_history(state: State) -> HistoryVerification:
    """Check the links the batch boundary promises.

    1. every stored membership line agrees with the time recomputation;
    2. closed-batch digests chain per flight (no boundary was rewritten);
    3. load records attribute to the batch their bag was sealed into.
    """

    windows = recompute(state)
    expected_number: dict[str, int] = {}
    for window in windows:
        for bag_id in window.bag_ids:
            expected_number[bag_id] = window.number

    mismatched: list[str] = []
    for item in state.batch_items:
        if expected_number.get(item.bag_id) != item.number:
            mismatched.append(item.bag_id)

    broken_chains = _broken_chains(state)
    load_mismatches, untracked = _load_mismatches(state)

    ok = not mismatched and not broken_chains and not load_mismatches
    return HistoryVerification(
        ok=ok,
        windows=tuple(windows),
        mismatched_bags=tuple(sorted(mismatched)),
        broken_chains=tuple(broken_chains),
        load_mismatches=tuple(load_mismatches),
        untracked_loads=tuple(untracked),
    )


# -- internals --------------------------------------------------------------


def _allocations_by_flight(state: State) -> dict[str, list[BatchItem]]:
    grouped: dict[str, list[BatchItem]] = {}
    for item in state.batch_items:
        grouped.setdefault(item.flight_id, []).append(item)
    for records in grouped.values():
        records.sort(key=lambda item: (item.at, item.bag_id))
    return grouped


def _latest_stamp(state: State) -> str:
    stamps = [item.at for item in state.batch_items if item.at]
    stamps.extend(batch.closed_at for batch in state.batches.values() if batch.closed_at)
    return max(stamps, default="")


def _anchor(state: State, flight_id: str, fallback_at: str) -> tuple[str, int]:
    first = min(
        (batch for batch in state.batches.values() if batch.flight_id == flight_id),
        key=lambda batch: batch.number,
        default=None,
    )
    if first is None:
        return fallback_at, 0
    return first.opened_at, first.window_seconds


def _broken_chains(state: State) -> list[str]:
    broken: list[str] = []
    flights = sorted({batch.flight_id for batch in state.batches.values()})
    for flight_id in flights:
        previous = GENESIS_DIGEST
        for batch in sorted(
            (
                batch
                for batch in state.batches.values()
                if batch.flight_id == flight_id and batch.state is BatchState.CLOSED
            ),
            key=lambda item: item.number,
        ):
            if batch.prev_digest != previous:
                broken.append(batch.id)
                continue
            material = state.sealed_materials.get(batch.id)
            if material is None or digest_material(material) != batch.digest:
                broken.append(batch.id)
            previous = batch.digest
    return broken


def _load_mismatches(state: State) -> tuple[list[str], list[str]]:
    by_bag = {item.bag_id: item for item in state.batch_items}
    bad: list[str] = []
    untracked: list[str] = []
    for record in state.load_records:
        item = by_bag.get(record.bag_id)
        if item is None:
            untracked.append(record.bag_id)
            continue
        batch = state.batches.get(item.batch_id)
        if batch is None:
            bad.append(record.bag_id)
            continue
        if record.batch != batch.number or (
            batch.carousel and record.carousel != batch.carousel
        ):
            bad.append(record.bag_id)
    return sorted(set(bad)), sorted(set(untracked))
