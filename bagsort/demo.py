"""Deterministic end to end scenario.

It drives the same operations the HTTP console exposes, in a fixed order, and
returns the counters a caller can assert on. Nothing here reads the wall clock,
the network or a random source, so two runs of the same command are identical.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from bagsort.console.seed import seed_defaults

if TYPE_CHECKING:  # pragma: no cover - typing only
    from bagsort.app import Plant


def run_demo(plant: "Plant", cycles: int = 6) -> dict[str, object]:
    """Exercise induction, sorting, recheck, transfer and cut-off."""

    if cycles < 1:
        raise ValueError("cycles must be at least one")
    seed_defaults(plant)

    flights = plant.book.list()
    if not flights:
        raise RuntimeError("demo needs at least one registered flight")
    flight = flights[0]
    chute = plant.namespace.chute_id("A")
    carousel = plant.namespace.carousel_id("CR1")
    plant.assign_chute(flight.id, chute)
    batch = plant.arm_batch(flight.id, carousel=carousel, window_seconds=60)
    plant.clock.advance(5)

    checked_in = [plant.check_in(f"BAG{i:04d}", flight.id) for i in range(cycles)]
    plant.clock.advance(20)  # t=25: still inside batch 1's window (closes at t=60)
    sorted_bags = [plant.push_bag(bag.id, chute) for bag in checked_in]
    plant.clock.advance(40)  # t=65: one boundary has passed, the next window is batch 2

    # A late allocation after the 60s countdown closes batch 1 (carousel
    # handoff in the same commit point) and books this bag into batch 2.
    late_bag = plant.check_in("LATE001", flight.id)
    plant.push_bag(late_bag.id, chute)
    late_item = plant.batches.item_for_bag(late_bag.id)
    closed_batch = plant.batches.get(batch.id)
    batch_check = plant.verify_batches()
    duplicate_retry = plant.retry_bag(checked_in[0].id)
    diverted = plant.divert_bag(checked_in[-1].id)

    recheck_bag = checked_in[1]
    plant.enqueue_recheck(recheck_bag.id)
    recheck_items = plant.recheck.length()
    processed = plant.process_recheck()

    for bag in checked_in[:2]:
        plant.belt.place(bag)
    on_carousel = len(plant.belt.on_carousel())
    transfer = plant.append_transfer(flight.id, carousel, 1)
    cleared = plant.switch_belt(carousel, 1)
    replayed = len(plant.belt.replay_transfers())

    closed = plant.close_flight(flight.id)
    loads = len(plant.belt.loads())

    summary: dict[str, object] = {
        "terminal": plant.namespace.terminal,
        "line": plant.topology.describe(),
        "checked_in": len(checked_in),
        "sorted": len(sorted_bags),
        "duplicate_retry_sorted": duplicate_retry,
        "diverted": diverted.id,
        "recheck_enqueued": recheck_items,
        "recheck_processed": processed.id if processed else "",
        "placed_on_carousel": on_carousel,
        "carousel_cleared": cleared,
        "transfer_id": transfer.id,
        "replayed_transfers": replayed,
        "load_records": loads,
        "flight_state": closed.state.value,
        "first_batch_closed": closed_batch.state.value,
        "first_batch_commit_seq": closed_batch.commit_seq,
        "late_bag_batch": late_item.number,
        "batch_history_ok": batch_check.ok,
        "audit_entries": plant.audit.count(),
        "audit_intact": plant.audit.verify(),
        "watermark": plant.store.watermark,
        "events": plant.telemetry.total(),
        "audit_verified": plant.verify(),
    }
    return summary


def invariants_hold(summary: dict[str, object]) -> bool:
    """The checks the demo must pass to be considered healthy."""

    return bool(
        summary["audit_intact"]
        and summary["duplicate_retry_sorted"] is False
        and summary["sorted"] == summary["checked_in"]
        and summary["carousel_cleared"] >= 1
        and summary["load_records"] >= 1
        and summary["flight_state"] == "closed"
        and summary["first_batch_closed"] == "closed"
        and summary["late_bag_batch"] == 2
        and summary["batch_history_ok"] is True
        and int(summary["watermark"]) > 0
    )
