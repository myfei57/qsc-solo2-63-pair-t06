"""The rule that decides which batch a bag belongs to.

The boundary decision
=====================

There are two natural clocks for booking a bag into a sorting batch:

1. **Arrival time** — when the bag physically reaches the handling line
   (check-in/injection). Membership is then known the moment the bag
   arrives, which mirrors the passenger side.
2. **Chute allocation time** — when the sorter commits the bag to a chute
   (the dispatch/sort record timestamp). Membership is only known once the
   bag has actually been sorted into a lane.

This service books on **chute allocation time**, deliberately. A batch is a
unit handed to a carousel, and the carousel can only ever see bags that
were actually allocated to a lane; arrival-time membership would book bags
that never reach the chute (still queued, diverted, rechecked) into a batch
and the carousel count would drift from the ledger — the exact "对不上数"
problem batches are meant to kill.

What that choice gives up
==========================

We sacrifice the arrival-order semantics: a bag that arrived before the
boundary but is allocated to a chute exactly on or after it lands in the
next batch, even though it was physically present in time. That trade is
accepted on purpose and documented on every API response
(``membership_clock`` / ``tradeoff``), because it is the only choice under
which ledger, load records and the carousel can reconcile exactly.

The boundary itself is a half-open interval: ``at < planned_close_at``
stays in the current batch, ``at >= planned_close_at`` is late and is
re-booked into the next batch. Equal timestamps therefore always resolve
the same way, on replay as well as live.
"""

from __future__ import annotations

from datetime import datetime, timedelta

#: Default countdown of an open batch, in seconds.
DEFAULT_WINDOW_SECONDS = 120

#: Machine readable name of the clock membership is derived from.
MEMBERSHIP_CLOCK = "chute_allocated_at"

#: The clock we did not pick, restated for operators.
REJECTED_CLOCK = "arrived_at"

#: The property this design deliberately gives up.
TRADEOFF = (
    "a bag that arrived before the boundary but is allocated to a chute "
    "on/after it is booked into the next batch; arrival-order membership "
    "is not tracked"
)


def parse_iso(value: str) -> datetime:
    """Parse the service's UTC second precision timestamps."""

    return datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ")


def format_iso(value: datetime) -> str:
    return value.strftime("%Y-%m-%dT%H:%M:%SZ")


def planned_close(opened_at: str, window_seconds: int) -> str:
    """Compute the deterministic close instant from an open instant."""

    if window_seconds < 0:
        raise ValueError("batch window must not be negative")
    return format_iso(parse_iso(opened_at) + timedelta(seconds=window_seconds))


def is_due(planned_close_at: str, now_iso: str) -> bool:
    """Whether the countdown has run out at ``now_iso``."""

    return parse_iso(now_iso) >= parse_iso(planned_close_at)


def in_current_window(allocated_at: str, planned_close_at: str) -> bool:
    """Half-open membership test: strictly before the boundary is current."""

    return parse_iso(allocated_at) < parse_iso(planned_close_at)


def policy_description() -> dict[str, str]:
    """The decision and its trade-off, exposed verbatim on the console."""

    return {
        "membership_clock": MEMBERSHIP_CLOCK,
        "rejected_clock": REJECTED_CLOCK,
        "boundary": "allocated_at < planned_close_at -> current batch; "
        "allocated_at >= planned_close_at -> next batch",
        "tradeoff": TRADEOFF,
    }
