"""Tamper-evident digests for committed batch boundaries.

Every boundary carries the digest of the boundary before it on the same
carousel, so rewriting a closed batch's member list or closing instant breaks
every digest that follows it. This mirrors the audit chain, scoped to one
batch line.
"""

from __future__ import annotations

import hashlib
import json

from bagsort.model.entities import BatchBoundary

GENESIS = "0" * 64


def boundary_digest(boundary: BatchBoundary) -> str:
    """Deterministic digest over the fields a closed batch must never change."""

    material = json.dumps(
        [
            boundary.carousel,
            boundary.batch,
            boundary.opened_at,
            boundary.closes_at,
            boundary.closed_at,
            boundary.reason,
            list(boundary.members),
            boundary.load_count,
            boundary.prev_digest,
        ],
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(material.encode("utf-8")).hexdigest()
