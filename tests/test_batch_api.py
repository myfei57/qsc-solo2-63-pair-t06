"""HTTP console surface for the sorting batch ledger."""

from __future__ import annotations

import json

from tests.test_console_api import bootstrap, call
from tests.support import make_server


def test_policy_endpoint_documents_the_clock_choice(server):
    status, payload = call(server, "GET", "/api/batches/policy")
    assert status == 200
    assert payload["membership_clock"] == "chute_allocated_at"
    assert payload["rejected_clock"] == "arrived_at"
    assert payload["tradeoff"]


def test_batch_lifecycle_over_http_with_idempotent_close(server):
    flight_id = bootstrap(server)

    status, batch = call(
        server, "POST", "/api/batches/arm",
        {"flight_id": flight_id, "carousel": "CR1", "window_seconds": 60},
    )
    assert status == 201 and batch["number"] == 1 and batch["state"] == "open"
    bid = batch["id"]

    # re-arm is idempotent and returns the same open batch
    _, again = call(server, "POST", "/api/batches/arm", {"flight_id": flight_id})
    assert again["id"] == bid

    # an on-time bag is attributed to batch 1 by dispatch
    call(server, "POST", "/api/bags/checkin", {"barcode": "5001", "flight_id": flight_id})
    call(server, "POST", "/api/chutes/assign", {"flight_id": flight_id, "chute_id": "T1-CHA"})
    status, dispatch = call(server, "POST", "/api/sort/dispatch", {"bag_id": "T1-5001"})
    assert status == 201 and dispatch["batch_number"] == 1 and dispatch["late"] is False

    status, membership = call(server, "GET", "/api/bags/T1-5001/batch")
    assert status == 200 and membership["item"]["number"] == 1

    # closing before the countdown is refused
    status, err = call(server, "POST", f"/api/batches/{bid}/close")
    assert status in (400, 409)
    # advance the synthetic clock in-process (the service never reads the wall)
    server.plant.clock.advance(61)

    status, closed = call(server, "POST", f"/api/batches/{bid}/close")
    assert status == 200 and closed["state"] == "closed" and closed["digest"]
    first_commit = closed["commit_seq"]

    # duplicate close is idempotent: no new commit point
    status, second = call(server, "POST", f"/api/batches/{bid}/close")
    assert status == 200 and second["commit_seq"] == first_commit

    # verify history reconciles
    status, verification = call(server, "GET", "/api/batches/verify")
    assert status == 200 and verification["ok"] is True


def test_late_bag_after_tick_is_booked_into_next_batch(server):
    flight_id = bootstrap(server)
    _, batch = call(
        server, "POST", "/api/batches/arm",
        {"flight_id": flight_id, "carousel": "CR1", "window_seconds": 60},
    )
    call(server, "POST", "/api/chutes/assign", {"flight_id": flight_id, "chute_id": "T1-CHA"})
    call(server, "POST", "/api/bags/checkin", {"barcode": "6001", "flight_id": flight_id})
    call(server, "POST", "/api/sort/dispatch", {"bag_id": "T1-6001"})

    server.plant.clock.advance(61)
    status, tick = call(server, "POST", "/api/batches/tick")
    assert status == 200 and tick["count"] == 1

    # a bag arriving after the boundary lands in batch 2
    call(server, "POST", "/api/bags/checkin", {"barcode": "6002", "flight_id": flight_id})
    _, dispatch = call(server, "POST", "/api/sort/dispatch", {"bag_id": "T1-6002"})
    assert dispatch["batch_number"] == 2
    _, membership = call(server, "GET", "/api/bags/T1-6002/batch")
    assert membership["batch"]["state"] == "open"


def test_batch_history_and_listing_endpoints(server):
    flight_id = bootstrap(server)
    call(server, "POST", "/api/batches/arm", {"flight_id": flight_id, "window_seconds": 60})
    call(server, "POST", "/api/chutes/assign", {"flight_id": flight_id, "chute_id": "T1-CHA"})
    call(server, "POST", "/api/bags/checkin", {"barcode": "7001", "flight_id": flight_id})
    call(server, "POST", "/api/sort/dispatch", {"bag_id": "T1-7001"})

    _, listing = call(server, "GET", "/api/batches")
    assert len(listing) == 1
    _, for_flight = call(
        server, "GET", "/api/batches", query={"flight_id": [flight_id]}
    )
    assert {b["flight_id"] for b in for_flight} == {flight_id}
    _, history = call(server, "GET", "/api/batches/history")
    assert [item["bag_id"] for item in history] == ["T1-7001"]
    batch_id = listing[0]["id"]
    _, items = call(server, "GET", f"/api/batches/{batch_id}/items")
    assert items["count"] == 1
