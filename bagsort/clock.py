"""Deterministic clock used for every timestamp in the service.

The service never reads the wall clock directly. A :class:`Clock` starts at a
fixed instant and only moves when someone advances it, which keeps replayed
command streams and API responses reproducible in tests and in Docker.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

DEFAULT_EPOCH = datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
ISO_FORMAT = "%Y-%m-%dT%H:%M:%SZ"


class Clock:
    """A monotonic synthetic clock expressed in UTC."""

    def __init__(self, start: datetime | None = None) -> None:
        self._start = (start or DEFAULT_EPOCH).astimezone(timezone.utc)
        self._offset = 0.0

    def now(self) -> datetime:
        return self._start + timedelta(seconds=self._offset)

    def now_iso(self) -> str:
        return self.iso(self.now())

    def iso(self, moment: datetime) -> str:
        """Render ``moment`` in the canonical UTC form every timestamp uses."""

        return moment.astimezone(timezone.utc).strftime(ISO_FORMAT)

    def parse(self, text: str) -> datetime:
        """Read a timestamp produced by :meth:`iso` back into a UTC instant."""

        return datetime.strptime(text, ISO_FORMAT).replace(tzinfo=timezone.utc)

    def advance(self, seconds: float) -> datetime:
        """Move the clock forward and return the new instant."""

        if seconds < 0:
            raise ValueError("clock cannot move backwards")
        self._offset += seconds
        return self.now()
