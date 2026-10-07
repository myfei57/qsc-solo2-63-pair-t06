"""Bridge between the sorter's dispatch transaction and the batch ledger.

The sorter already commits the sort record in one store session. Membership
is booked in that *same* session, so a bag can never have a sort record
without a batch line or vice versa — ledger and sort log commit or roll
back together.
"""

from __future__ import annotations

from typing import Any, Protocol

from . import policy
from .ledger import Attribution, BatchLedger, Handoff, stage_attribution


class BatchAttributor:
    """Stages a batch membership line while the sorter is dispatching."""

    def __init__(
        self,
        ledger: BatchLedger,
        handoff: Handoff | None = None,
        default_carousel: str = "",
        default_window: int = policy.DEFAULT_WINDOW_SECONDS,
    ) -> None:
        self._ledger = ledger
        self._handoff = handoff
        self._carousel = default_carousel
        self._window = default_window

    def bind_handoff(self, handoff: Handoff | None) -> None:
        """Wire the carousel handoff once the belt has been constructed."""

        self._handoff = handoff

    def stage_for(
        self,
        session: Any,
        bag_id: str,
        flight_id: str,
        chute_id: str,
        at: str,
    ) -> Attribution:
        """Stage membership inside the sorter's open commit session."""

        return stage_attribution(
            self._ledger,
            session,
            bag_id=bag_id,
            flight_id=flight_id,
            chute_id=chute_id,
            at=at,
            handoff=self._handoff,
            carousel=self._carousel,
            window_seconds=self._window,
        )
