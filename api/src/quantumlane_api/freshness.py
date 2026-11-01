"""
Data-freshness readiness: the shared answer to "are realtime rows actually arriving?"

`/readyz` calls this. Any future surface that needs the same answer (an MCP health
tool, a CLI check) must import it rather than re-implement the query — one
data-access path per question, so the surfaces cannot drift apart.

Why this exists: `/health` proves the API process answers, and `/ready` proves the
database answers. Neither proves ingestion is alive. In August 2026 the disk filled,
Dagster died, and the API kept serving reads from a frozen table for ~72 hours while
the uptime monitor stayed green. This module measures the outcome — the age of the
newest row — instead of process liveness.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import UTC, datetime

from quantumlane_api import db

# Feeds whose silence means ingestion is broken. `service_alerts` is excluded on
# purpose: it is upserted, and can legitimately go an hour or more with no change.
REQUIRED_FEEDS: dict[str, str] = {
    "ttc.vehicle_positions": "realtime.vehicle_positions",
    "ttc.trip_updates": "realtime.trip_updates",
}

# Bound the scan. `received_at` leads each table's primary key and the tables are
# range-partitioned on it, so this predicate prunes to the current partition(s) and
# MAX() becomes an index tail read. Anything older than this is stale under any
# sane threshold, so the bound loses no information.
_LOOKBACK = "1 day"


@dataclass(frozen=True)
class FeedAge:
    feed_key: str
    last_record_at: datetime | None
    age_seconds: int | None
    stale: bool


@dataclass(frozen=True)
class Readiness:
    ready: bool
    stale_after_seconds: int
    checked_at: datetime
    feeds: list[FeedAge]

    def to_dict(self) -> dict:
        return {
            "status": "ready" if self.ready else "stale",
            "stale_after_seconds": self.stale_after_seconds,
            "checked_at": self.checked_at.isoformat(),
            "feeds": [
                {
                    **asdict(f),
                    "last_record_at": f.last_record_at.isoformat() if f.last_record_at else None,
                }
                for f in self.feeds
            ],
        }


def newest_record_at() -> dict[str, datetime | None]:
    """Newest `received_at` per required feed, or None if nothing arrived in the lookback."""
    out: dict[str, datetime | None] = {}
    for feed_key, table in REQUIRED_FEEDS.items():
        # `table` comes from the constant above, never from a request.
        row = db.fetch_one(
            f"SELECT MAX(received_at) AS last_record_at FROM {table} "
            f"WHERE received_at > NOW() - INTERVAL '{_LOOKBACK}'"
        )
        out[feed_key] = row["last_record_at"] if row else None
    return out


def readiness(stale_after_seconds: int, now: datetime | None = None) -> Readiness:
    """
    Ready iff every required feed has a row newer than `stale_after_seconds`.

    A feed with no row at all in the lookback window is stale. Raises whatever the
    database raises; the caller decides how to report an unreachable DB.
    """
    checked_at = now or datetime.now(UTC)
    feeds: list[FeedAge] = []
    for feed_key, last_at in newest_record_at().items():
        if last_at is None:
            feeds.append(FeedAge(feed_key, None, None, stale=True))
            continue
        if last_at.tzinfo is None:  # defensive: every column is TIMESTAMPTZ, but be explicit
            last_at = last_at.replace(tzinfo=UTC)
        age = int((checked_at - last_at).total_seconds())
        feeds.append(FeedAge(feed_key, last_at, age, stale=age > stale_after_seconds))
    return Readiness(
        ready=not any(f.stale for f in feeds),
        stale_after_seconds=stale_after_seconds,
        checked_at=checked_at,
        feeds=feeds,
    )
