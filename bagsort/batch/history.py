"""Read-only reconstructions of batch history from the committed journal.

Two questions the carousel crew asks of the batch line — "what did the world
look like at time T" and "do the boundaries, the load records and the
carousel log still tell the same story" — are answered by replaying committed
journal entries, never by trusting a cached projection.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from bagsort.model.entities import BatchBoundary, BatchRun, LoadRecord
from bagsort.model.state import fold
from bagsort.store.replay import command_pairs
from bagsort.store.store import Store
from .digest import GENESIS, boundary_digest


@dataclass(frozen=True)
class BatchView:
    """The batch world as it stood at one instant."""

    until_at: str
    lines: tuple[BatchRun, ...]
    boundaries: tuple[BatchBoundary, ...]
    load_records: tuple[LoadRecord, ...]
    on_carousel: tuple[str, ...]

    def as_dict(self) -> dict[str, Any]:
        return {
            "until_at": self.until_at,
            "lines": [run.as_dict() for run in self.lines],
            "boundaries": [boundary.as_dict() for boundary in self.boundaries],
            "load_records": [record.as_dict() for record in self.load_records],
            "on_carousel": list(self.on_carousel),
        }


class BatchHistory:
    """Folds journal prefixes into as-of views and audits the commit points."""

    def __init__(self, store: Store) -> None:
        self._store = store

    def recompute(self, until_at: str) -> BatchView:
        """Replay every entry stamped no later than ``until_at``."""

        entries = [
            entry
            for entry in self._store.entries()
            if not entry.at or entry.at <= until_at
        ]
        state = fold(command_pairs(entries))
        return BatchView(
            until_at=until_at,
            lines=tuple(state.batch_lines[key] for key in sorted(state.batch_lines)),
            boundaries=tuple(state.batch_boundaries),
            load_records=tuple(state.load_records),
            on_carousel=tuple(state.belt_on_carousel),
        )

    def verify(self) -> list[str]:
        """Walk the journal once and check every committed boundary.

        For each batch line the walk proves, entry by entry, that a closed
        batch's member list is exactly the carousel content its clear removed
        and exactly the load records written for it, that batch numbers
        strictly increase, that each window opens where the previous one
        closed, and that the digest chain is unbroken. A healthy journal
        yields an empty list.
        """

        problems: list[str] = []
        on_carousel: list[str] = []
        last_cleared: list[str] = []
        loads: dict[tuple[str, int], list[str]] = {}
        last_batch: dict[str, int] = {}
        last_digest: dict[str, str] = {}
        last_closes_at: dict[str, str] = {}
        for entry in self._store.entries():
            kind, payload = entry.kind, entry.payload
            if kind == "belt.place":
                bag_id = str(payload.get("bag_id", ""))
                if bag_id not in on_carousel:
                    on_carousel.append(bag_id)
            elif kind == "belt.clear":
                last_cleared = list(on_carousel)
                on_carousel = []
            elif kind == "load.append":
                record = LoadRecord.from_dict(payload["record"])
                key = (record.carousel, record.batch)
                loads.setdefault(key, []).append(record.bag_id)
            elif kind == "batch.open":
                run = BatchRun.from_dict(payload)
                previous_close = last_closes_at.get(run.carousel)
                if previous_close is not None and run.opened_at != previous_close:
                    problems.append(
                        f"batch {run.carousel}#{run.batch} opens at {run.opened_at}, "
                        f"not where #{run.batch - 1} closed ({previous_close})"
                    )
                last_closes_at[run.carousel] = run.closes_at
            elif kind == "batch.close":
                boundary = BatchBoundary.from_dict(payload["boundary"])
                label = f"{boundary.carousel}#{boundary.batch}"
                members = list(boundary.members)
                seen = last_batch.get(boundary.carousel)
                if seen is not None and boundary.batch <= seen:
                    problems.append(f"batch {label} does not follow #{seen}")
                last_batch[boundary.carousel] = boundary.batch
                if members != last_cleared:
                    problems.append(
                        f"batch {label} members do not match the carousel content "
                        "cleared at the boundary"
                    )
                if loads.get((boundary.carousel, boundary.batch), []) != members:
                    problems.append(
                        f"batch {label} members do not match its load records"
                    )
                if boundary.load_count != len(members):
                    problems.append(
                        f"batch {label} counts {boundary.load_count} loads "
                        f"for {len(members)} members"
                    )
                if boundary.prev_digest != last_digest.get(boundary.carousel, GENESIS):
                    problems.append(f"batch {label} breaks the boundary digest chain")
                if boundary_digest(boundary) != boundary.digest:
                    problems.append(f"batch {label} carries a digest it does not earn")
                last_digest[boundary.carousel] = boundary.digest
        return problems
