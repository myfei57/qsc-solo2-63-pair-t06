"""Sorting batches: countdown close, late attribution, idempotence, sealing.

These pin the business rules the batch ledger exists to enforce:

* the system closes a batch on its countdown, not on operator feeling;
* a bag allocated at/after the boundary is booked into the *next* batch
  (membership uses the chute allocation clock, never arrival time);
* a closed batch cannot be reopened or receive another bag;
* closing is idempotent and the boundary is a persisted commit point with a
  tamper-evident digest chain;
* ledger, load records and carousel reconcile exactly, and history can be
  recomputed from timestamps alone.
"""

from __future__ import annotations

import json

import pytest

from bagsort.batch import policy
from bagsort.batch.ledger import batch_id
from bagsort.errors import OrderingError, ValidationError
from bagsort.model.enums import BatchState
from bagsort.store.replay import replay
from bagsort.store.store import Store
from tests.support import check_in, register_flight

WINDOW = 60


def _flight(plant, code="CA1234"):
    return register_flight(plant, code=code, carousel="CR1", chute="CHA")


def _arm(plant, flight):
    return plant.arm_batch(flight.id, carousel=flight.carousel, window_seconds=WINDOW)


def _push(plant, bag):
    return plant.push_bag(bag.id, "T1-CHA")


def test_policy_pins_down_the_clock_choice_and_the_tradeoff():
    description = policy.policy_description()
    assert description["membership_clock"] == "chute_allocated_at"
    # The other clock is explicitly named and recorded as given up.
    assert description["rejected_clock"] == "arrived_at"
    assert "arrival-order" in description["tradeoff"]
    assert "allocated_at >= planned_close_at -> next batch" in description["boundary"]


def test_on_time_bag_joins_the_current_batch(plant, flight):
    batch = _arm(plant, flight)
    bag = check_in(plant, flight.id, 1)[0]
    plant.clock.advance(10)
    _push(plant, bag)
    assert batch.state is BatchState.OPEN
    item = plant.batches.item_for_bag(bag.id)
    assert item.batch_id == batch.id
    assert policy.in_current_window(item.at, batch.planned_close_at)


def test_boundary_is_half_open_exactly_at_the_boundary_goes_next(plant, flight):
    batch1 = _arm(plant, flight)
    early = check_in(plant, flight.id, 1)[0]
    plant.clock.advance(10)
    _push(plant, early)

    # one second before the boundary is still batch 1
    plant.clock.advance(WINDOW - 10 - 1)
    second = check_in(plant, flight.id, 1, prefix="LATE")[0]
    _push(plant, second)
    assert plant.batches.item_for_bag(second.id).number == 1
    assert plant.batches.get(batch1.id).state is BatchState.OPEN

    # exactly at the boundary rolls the late bag into batch 2
    plant.clock.advance(1)
    third = check_in(plant, flight.id, 1, prefix="EDGE")[0]
    _push(plant, third)
    sealed = plant.batches.get(batch1.id)
    assert sealed.state is BatchState.CLOSED
    assert sealed.closed_at == batch1.planned_close_at
    assert plant.batches.item_for_bag(third.id).number == 2
    # the early bags stayed in batch 1; nothing was moved
    assert [i.bag_id for i in plant.batches.items_for(batch1.id)] == [early.id, second.id]


def test_close_requires_the_countdown_and_is_idempotent(plant, flight):
    batch = _arm(plant, flight)
    plant.clock.advance(5)
    with pytest.raises(OrderingError):
        plant.close_batch(batch.id)

    plant.clock.advance(WINDOW)
    sealed = plant.close_batch(batch.id)
    assert sealed.state is BatchState.CLOSED
    # commit_seq names the exact journal entry that carries the boundary
    close_entry = plant.store.entries()[sealed.commit_seq - 1]
    assert close_entry.sequence == sealed.commit_seq
    assert close_entry.kind == "batch.close"

    # a duplicate trigger writes nothing and returns the same batch
    watermark_after_first = plant.store.watermark
    again = plant.close_batch(batch.id)
    assert again == sealed
    assert plant.store.watermark == watermark_after_first
    assert again.commit_seq == sealed.commit_seq


def test_tick_closes_every_due_batch_once(plant, flight):
    batch = _arm(plant, flight)
    plant.clock.advance(WINDOW)
    closed = plant.tick_batches()
    assert [item.id for item in closed] == [batch.id]
    # ticking again with nothing new is an empty no-op
    assert plant.tick_batches() == []
    assert plant.batches.get(batch.id).state is BatchState.CLOSED


def test_a_closed_batch_cannot_take_another_bag_even_from_the_journal(plant, flight):
    batch = _arm(plant, flight)
    plant.clock.advance(WINDOW)
    plant.tick_batches()

    # the ledger only ever writes into an open batch ...
    late = check_in(plant, flight.id, 1)[0]
    _push(plant, late)
    assert plant.batches.item_for_bag(late.id).batch_id != batch.id

    # ... and the reducer refuses a forged item aimed at the sealed batch,
    # so replay can never smuggle a bag into a closed boundary either.
    forged = {
        "item": {
            "bag_id": "T1-FORGED",
            "batch_id": batch.id,
            "flight_id": flight.id,
            "number": batch.number,
            "chute_id": "T1-CHA",
            "at": batch.closed_at,
        }
    }
    journal = plant.store.journal
    journal.append("batch.item", forged, batch.closed_at)
    plant.store.commit()
    # replay folds the journal and the reducer rejects the forged line
    with pytest.raises(ValidationError):
        replay(plant.store.entries())


def test_reducer_rejects_a_duplicate_close_of_an_already_closed_batch(plant, flight):
    batch = _arm(plant, flight)
    plant.clock.advance(WINDOW)
    plant.tick_batches()
    sealed = plant.batches.get(batch.id)
    # append a second batch.close for the same already-sealed batch id
    plant.store.journal.append(
        "batch.close", {"batch": sealed.as_dict()}, sealed.closed_at
    )
    plant.store.commit()
    with pytest.raises(ValidationError):
        replay(plant.store.entries())


def test_batch_numbers_restart_per_flight_and_ids_are_unique(plant, flight):
    other = _flight(plant, code="MU5678")
    b1 = _arm(plant, flight)
    b2 = _arm(plant, other)
    assert b1.number == b2.number == 1
    assert b1.id != b2.id
    assert b1.id == batch_id(flight.id, 1)


def test_close_ties_ledger_load_records_and_carousel_in_one_commit(plant, flight):
    batch = _arm(plant, flight)
    bag = check_in(plant, flight.id, 1)[0]
    plant.clock.advance(10)
    _push(plant, bag)
    plant.belt.place(bag)
    assert plant.belt.batch() == 1

    plant.clock.advance(WINDOW)  # boundary reached; next dispatch triggers close
    late = check_in(plant, flight.id, 1, prefix="LATE")[0]
    _push(plant, late)

    # carousel was cleared (load record stamped with batch 1) inside the
    # boundary commit; the global carousel pointer advances via switch, not
    # as a side effect of another flight's boundary
    sealed = plant.batches.get(batch.id)
    [load] = [r for r in plant.belt.loads() if r.bag_id == bag.id]
    assert (load.batch, load.carousel) == (1, sealed.carousel)
    assert plant.belt.carousel() == sealed.carousel
    assert plant.bag(bag.id).state.value == "loaded"
    # the next flight batch is ready and open
    assert plant.batches.open_for(flight.id).number == 2


def test_sealed_boundary_persists_and_replays_identically(plant, flight):
    batch = _arm(plant, flight)
    bag = check_in(plant, flight.id, 1)[0]
    plant.clock.advance(10)
    _push(plant, bag)
    plant.clock.advance(WINDOW)
    late = check_in(plant, flight.id, 1, prefix="LATE")[0]
    _push(plant, late)  # close + persist happen inside push_bag
    sealed = plant.batches.get(batch.id)
    assert sealed.commit_seq > 0 and sealed.digest and sealed.prev_digest

    reloaded = Store.open(plant.store.path)
    rebuilt = replay(reloaded.entries())
    assert rebuilt.as_dict() == reloaded.state().as_dict()
    assert rebuilt.batches[batch.id] == sealed
    # digest chains per flight and survives restart
    from bagsort.batch.history import verify_history

    assert verify_history(rebuilt).ok


def test_tampering_with_a_sealed_boundary_is_detected(plant, flight):
    batch = _arm(plant, flight)
    bag = check_in(plant, flight.id, 1)[0]
    plant.clock.advance(10)
    _push(plant, bag)
    plant.clock.advance(WINDOW)
    plant.tick_batches()
    plant.store.persist()

    # The snapshot digest already blocks a tampered file from even loading;
    # independently, rewriting a sealed material in a rebuilt state breaks the
    # per-batch digest chain.
    reloaded = Store.open(plant.store.path)
    state = reloaded.state()
    state.sealed_materials[batch.id]["item_count"] = 999
    from bagsort.batch.history import verify_history as verify

    assert batch.id in verify(state).broken_chains
    # and the on-disk tamper is rejected outright
    raw = json.loads(open(plant.store.path, encoding="utf-8").read())
    for entry in raw["entries"]:
        if entry["kind"] == "batch.close":
            entry["payload"]["material"]["item_count"] = 999
    with open(plant.store.path, "w", encoding="utf-8") as handle:
        json.dump(raw, handle)
    with pytest.raises(ValidationError):
        Store.open(plant.store.path)


def test_history_recomputes_membership_from_timestamps(plant, flight):
    _arm(plant, flight)
    first = check_in(plant, flight.id, 1)[0]
    plant.clock.advance(10)
    _push(plant, first)
    plant.clock.advance(WINDOW)
    late = check_in(plant, flight.id, 1, prefix="LATE")[0]
    _push(plant, late)

    result = plant.verify_batches()
    assert result.ok
    by_number = {window.number: set(window.bag_ids) for window in result.windows}
    assert by_number[1] == {first.id}
    assert by_number[2] == {late.id}
    late_window = next(window for window in result.windows if window.number == 2)
    assert late_window.late is True


def test_shared_carousel_clear_stamps_each_bag_from_its_own_batch(plant, flight):
    # A second flight shares the same physical carousel but a different chute.
    other = plant.register_flight("MU5678", carousel="CR1", chute="")
    plant.assign_chute(other.id, "T1-CHB")
    _arm(plant, flight)
    _arm(plant, other)
    bag_a = check_in(plant, flight.id, 1)[0]
    bag_b = check_in(plant, other.id, 1, prefix="XX")[0]
    plant.clock.advance(10)
    plant.push_bag(bag_a.id, "T1-CHA")
    plant.push_bag(bag_b.id, "T1-CHB")
    plant.belt.place(bag_a)
    plant.belt.place(bag_b)

    # Only the first flight's batch becomes due. Closing it clears the shared
    # carousel; both bags become load records stamped from their own ledger.
    plant.clock.advance(WINDOW)
    late = check_in(plant, flight.id, 1, prefix="L1")[0]
    plant.push_bag(late.id, "T1-CHA")

    loads = {record.bag_id: (record.carousel, record.batch) for record in plant.belt.loads()}
    assert loads[bag_a.id] == (flight.carousel, 1)
    assert loads[bag_b.id] == (other.carousel, 1)
    assert plant.verify_batches().ok
