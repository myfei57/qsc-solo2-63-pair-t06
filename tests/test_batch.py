"""Batch lines: countdown closing, late bags, idempotent commit points."""

from __future__ import annotations

import pytest

from bagsort.app import Plant
from bagsort.errors import ConflictError, NotFoundError, OrderingError, StaleError, ValidationError
from bagsort.model.entities import BatchBoundary
from bagsort.model.state import fold
from bagsort.store.store import Store
from tests.support import check_in, make_plant, register_flight

WINDOW = 60


@pytest.fixture()
def line(plant):
    carousel = plant.namespace.carousel_id("CR1")
    plant.open_batch_line(carousel, WINDOW)
    return carousel


def test_open_line_starts_the_countdown(plant, line):
    (row,) = plant.batch_status()
    assert row["carousel"] == line
    assert row["batch"] == 1
    assert row["remaining_seconds"] == WINDOW
    assert row["overdue"] is False
    assert plant.belt.describe() == {"carousel": line, "batch": 1, "on_carousel": 0}


def test_open_line_validates_its_arguments(plant):
    with pytest.raises(ValidationError):
        plant.open_batch_line("T1-CRCR1", 0)
    with pytest.raises(ValidationError):
        plant.open_batch_line("  ", 60)
    with pytest.raises(OrderingError):
        plant.open_batch_line("T1-CRCR1", 60, start_batch=-1)


def test_a_second_line_cannot_open_while_one_runs(plant, line):
    with pytest.raises(ConflictError):
        plant.open_batch_line(plant.namespace.carousel_id("CR2"), WINDOW)
    with pytest.raises(ConflictError):
        plant.open_batch_line(line, WINDOW)


def test_countdown_close_lines_up_loads_carousel_and_boundary(plant, flight, line):
    bags = check_in(plant, flight.id, 2)
    for bag in bags:
        plant.place_on_carousel(bag.id)
    plant.clock.advance(WINDOW + 1)

    (boundary,) = plant.tick_batches()

    assert boundary.batch == 1
    assert boundary.reason == "countdown"
    assert boundary.members == tuple(bag.id for bag in bags)
    assert boundary.load_count == 2
    assert boundary.digest and boundary.prev_digest
    loads = [(record.bag_id, record.batch) for record in plant.belt.loads()]
    assert loads == [(bag.id, 1) for bag in bags]
    assert plant.belt.describe() == {"carousel": line, "batch": 2, "on_carousel": 0}
    assert plant.verify_batches() == []


def test_late_bag_is_rebooked_into_the_next_batch(plant, flight, line):
    on_time, late = check_in(plant, flight.id, 2)
    plant.place_on_carousel(on_time.id)
    plant.clock.advance(WINDOW + 1)

    placement = plant.place_on_carousel(late.id)

    assert placement.batch == 2
    assert [boundary.batch for boundary in placement.closed] == [1]
    (boundary,) = plant.batch_boundaries()
    assert boundary.members == (on_time.id,)
    assert plant.belt.describe()["batch"] == 2
    assert plant.verify_batches() == []


def test_a_bag_at_the_exact_boundary_joins_the_next_batch(plant, flight, line):
    (bag,) = check_in(plant, flight.id, 1)
    plant.clock.advance(WINDOW)  # lands exactly on the scheduled close instant

    placement = plant.place_on_carousel(bag.id)

    assert placement.batch == 2
    assert plant.batch_boundaries()[0].members == ()


def test_arrival_inside_the_window_does_not_save_a_late_placement(plant, flight, line):
    """The documented trade-off: the line judges the placement instant.

    The bag arrives (checks in) while batch 1 is open, but reaches the
    carousel after the countdown expired. Arrival-time attribution would put
    it in batch 1; the line books it into batch 2 so the manifest stays
    exactly what the carousel loaded.
    """

    (bag,) = check_in(plant, flight.id, 1)  # arrival: inside batch 1's window
    plant.clock.advance(WINDOW + 5)

    placement = plant.place_on_carousel(bag.id)

    assert placement.batch == 2
    assert plant.batch_boundaries()[0].members == ()
    assert plant.verify_batches() == []


def test_close_is_idempotent(plant, flight, line):
    (bag,) = check_in(plant, flight.id, 1)
    plant.place_on_carousel(bag.id)
    plant.clock.advance(WINDOW + 1)

    first, created = plant.close_batch(line, 1)
    watermark = plant.store.watermark
    second, created_again = plant.close_batch(line, 1)

    assert created is True
    assert created_again is False
    assert second == first
    assert plant.store.watermark == watermark  # the repeat trigger wrote nothing
    assert len(plant.batch_boundaries()) == 1
    assert plant.verify_batches() == []


def test_tick_is_idempotent_once_nothing_is_due(plant, line):
    assert plant.tick_batches() == []
    plant.clock.advance(WINDOW + 1)
    assert len(plant.tick_batches()) == 1
    assert plant.tick_batches() == []


def test_closing_a_batch_that_is_not_open_is_stale(plant, line):
    plant.clock.advance(WINDOW + 1)
    plant.tick_batches()
    with pytest.raises(StaleError):
        plant.close_batch(line, 3)
    boundary, created = plant.close_batch(line, 1)  # already committed: idempotent
    assert created is False and boundary.batch == 1


def test_a_closed_batch_accepts_no_more_bags(plant, flight, line):
    first, second = check_in(plant, flight.id, 2)
    plant.place_on_carousel(first.id)
    plant.clock.advance(WINDOW + 1)
    plant.tick_batches()

    placement = plant.place_on_carousel(second.id)

    assert placement.batch == 2
    boundary = plant.batch_boundaries()[0]
    assert boundary.members == (first.id,)
    assert [r.bag_id for r in plant.belt.loads() if r.batch == 1] == [first.id]
    assert plant.verify_batches() == []


def test_empty_batches_close_cleanly_when_the_clock_jumps(plant, line):
    plant.clock.advance(WINDOW * 3 + 10)

    closed = plant.tick_batches()

    assert [boundary.batch for boundary in closed] == [1, 2, 3]
    assert all(boundary.load_count == 0 for boundary in closed)
    # the lattice stays regular: each batch opens where the previous closed
    assert closed[1].opened_at == closed[0].closes_at
    assert closed[2].opened_at == closed[1].closes_at
    (row,) = plant.batch_status()
    assert row["batch"] == 4
    assert plant.verify_batches() == []


def test_boundaries_survive_a_restart(tmp_path, flight, line, plant):
    bags = check_in(plant, flight.id, 2)
    for bag in bags:
        plant.place_on_carousel(bag.id)
    plant.clock.advance(WINDOW + 1)
    plant.tick_batches()
    plant.store.persist()

    reopened = Plant.build(Store.open(plant.store.path), clock=plant.clock)

    assert [b.as_dict() for b in reopened.batch_boundaries()] == [
        b.as_dict() for b in plant.batch_boundaries()
    ]
    boundary, created = reopened.close_batch(line, 1)
    assert created is False and boundary.batch == 1
    assert reopened.verify_batches() == []


def test_batch_history_recomputes_as_of_any_instant(plant, flight, line):
    first, second = check_in(plant, flight.id, 2)
    plant.place_on_carousel(first.id)
    t_one = plant.clock.now_iso()
    plant.clock.advance(30)
    plant.place_on_carousel(second.id)
    t_two = plant.clock.now_iso()
    plant.clock.advance(WINDOW)  # past the first window
    plant.tick_batches()
    t_three = plant.clock.now_iso()

    early = plant.batch_history(t_one)
    assert [run.batch for run in early.lines] == [1]
    assert early.boundaries == ()
    assert early.on_carousel == (first.id,)

    mid = plant.batch_history(t_two)
    assert mid.on_carousel == (first.id, second.id)
    assert mid.boundaries == ()

    late = plant.batch_history(t_three)
    assert [run.batch for run in late.lines] == [2]
    assert [b.batch for b in late.boundaries] == [1]
    assert late.boundaries[0].members == (first.id, second.id)
    assert late.on_carousel == ()


def test_history_rejects_a_bad_instant(plant, line):
    with pytest.raises(ValidationError):
        plant.batch_history("not-a-time")
    with pytest.raises(ValidationError):
        plant.batch_history("")


def test_verify_flags_a_boundary_that_lies(plant, flight, line):
    (bag,) = check_in(plant, flight.id, 1)
    plant.place_on_carousel(bag.id)
    forged = BatchBoundary(
        carousel=line,
        batch=1,
        opened_at=plant.clock.now_iso(),
        closes_at=plant.clock.now_iso(),
        closed_at=plant.clock.now_iso(),
        reason="countdown",
        members=("GHOST-BAG",),
        load_count=1,
    )
    with plant.store.session() as session:
        session.record("batch.close", {"boundary": forged.as_dict()}, plant.clock.now_iso())

    problems = plant.verify_batches()

    assert any("members" in problem for problem in problems)
    assert any("digest" in problem for problem in problems)


def test_the_reducer_refuses_a_second_boundary_for_the_same_batch(plant, flight, line):
    plant.clock.advance(WINDOW + 1)
    plant.tick_batches()
    boundary = plant.batch_boundaries()[0]
    with pytest.raises(ValidationError):
        fold(
            [
                ("batch.close", {"boundary": boundary.as_dict()}),
                ("batch.close", {"boundary": boundary.as_dict()}),
            ]
        )


def test_a_manual_belt_move_breaks_the_line_loudly(plant, line):
    plant.belt.set(line, 7)  # operator bypasses the line: the belt drifts
    plant.clock.advance(WINDOW + 1)
    with pytest.raises(ConflictError):
        plant.tick_batches()


def test_placement_requires_a_line(plant, flight):
    (bag,) = check_in(plant, flight.id, 1)
    with pytest.raises(OrderingError):
        plant.place_on_carousel(bag.id)  # belt not bound to any carousel
    plant.belt.set(plant.namespace.carousel_id("CR1"), 1)
    with pytest.raises(NotFoundError):
        plant.place_on_carousel(bag.id)  # bound, but no batch line runs there


def test_retire_closes_the_open_batch_and_takes_the_line_down(plant, flight, line):
    (bag,) = check_in(plant, flight.id, 1)
    plant.place_on_carousel(bag.id)

    boundary = plant.retire_batch_line(line)

    assert boundary.reason == "retired"
    assert boundary.members == (bag.id,)
    assert plant.batch_status() == []
    with pytest.raises(NotFoundError):
        plant.place_on_carousel(bag.id)
    with pytest.raises(NotFoundError):
        plant.retire_batch_line(line)
    # the belt is free for a fresh line afterwards
    plant.open_batch_line(plant.namespace.carousel_id("CR2"), WINDOW)
    assert plant.batch_status()[0]["carousel"] == plant.namespace.carousel_id("CR2")


def test_transfer_commands_retire_with_the_batch(plant, flight, line):
    plant.append_transfer(flight.id, line, 1)
    plant.clock.advance(WINDOW + 1)
    plant.tick_batches()
    assert plant.belt.transfers() == []


def test_batch_close_is_audited(plant, flight, line):
    plant.clock.advance(WINDOW + 1)
    plant.tick_batches()
    kinds = [entry.kind for entry in plant.audit.entries()]
    assert "batch_close" in kinds
    assert plant.verify() is True


# ---------------------------------------------------------------------------
# console endpoints
# ---------------------------------------------------------------------------


def test_console_drives_the_whole_line(server):
    flight = register_flight(server.plant)
    carousel = flight.carousel
    created = server.call(
        "POST", "/api/batches/open", {"carousel": carousel, "window_seconds": WINDOW}
    )
    assert created.status == 201

    (bag,) = check_in(server.plant, flight.id, 1)
    placed = server.call("POST", "/api/batches/place", {"bag_id": bag.id})
    assert placed.status == 201

    server.plant.clock.advance(WINDOW + 1)
    ticked = server.call("POST", "/api/batches/tick")
    assert ticked.status == 200

    import json

    closed = json.loads(ticked.body)["closed"]
    assert [boundary["batch"] for boundary in closed] == [1]
    assert closed[0]["members"] == [bag.id]

    again = server.call("POST", "/api/batches/close", {"carousel": carousel, "batch": 1})
    assert json.loads(again.body)["created"] is False

    status = json.loads(server.call("GET", "/api/batches").body)
    assert status["lines"][0]["batch"] == 2

    boundaries = json.loads(server.call("GET", "/api/batches/boundaries").body)
    assert [boundary["batch"] for boundary in boundaries] == [1]

    verified = json.loads(server.call("GET", "/api/batches/verify").body)
    assert verified == {"ok": True, "problems": []}

    until = server.plant.clock.now_iso()
    history = json.loads(server.call("GET", "/api/batches/history", query={"until": [until]}).body)
    assert [boundary["batch"] for boundary in history["boundaries"]] == [1]

    retired = server.call("POST", "/api/batches/retire", {"carousel": carousel})
    assert retired.status == 200
    assert json.loads(server.call("GET", "/api/batches").body)["lines"] == []


def test_console_reports_unknown_lines(server):
    missing = server.call("POST", "/api/batches/close", {"carousel": "T1-CRCR9", "batch": 1})
    assert missing.status == 404
    bad = server.call("POST", "/api/batches/open", {"carousel": "T1-CRCR1", "window_seconds": 0})
    assert bad.status == 400
