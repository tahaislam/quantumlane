"""
Operational assets: freshness checks, partition maintenance, parquet exports.

These are the immune system of the platform. They run on schedules independent of
the ingestion assets and write to the ops.* schema.
"""
from __future__ import annotations

from datetime import UTC, date, datetime, timedelta

import structlog
from dagster import MetadataValue, asset

from quantumlane_ingestion.resources import PostgresResource, R2Resource

# NOTE: Asset `context` parameters are deliberately unannotated. See assets/ttc.py for rationale.

log = structlog.get_logger(__name__)

# Feed health thresholds. These define what "healthy" means and are deliberately conservative.
# Tweak with care — looser thresholds hide real outages; tighter ones cry wolf.
FRESHNESS_THRESHOLDS = {
    # feed_key: (healthy_max_lag_s, lagging_max_lag_s, stale_max_lag_s)
    "ttc.vehicle_positions": (60, 180, 600),     # 1 min healthy, 3 min lagging, 10 min stale, then down
    "ttc.trip_updates":      (60, 180, 600),
    "ttc.service_alerts":    (600, 1800, 3600),  # alerts can legitimately go an hour without changes
}


@asset(
    name="freshness_check",
    group_name="ops",
    compute_kind="python",
    description="Computes and persists a per-feed freshness snapshot. The /freshness page reads this.",
)
def freshness_check(
    context,
    postgres: PostgresResource,
) -> None:
    """
    For each tracked feed, compute:
      - last_record_at: max(received_at)
      - record_count_5min: rows in the last 5 minutes
      - record_count_1h: rows in the last hour
      - lag_seconds: now - last_record_at
      - status: derived from lag against thresholds

    A row is written for every feed every minute. Old snapshots are not deleted here;
    a separate retention job in v0.2 will trim ops.freshness_snapshot to 30 days.
    """
    queries = {
        "ttc.vehicle_positions": (
            "SELECT MAX(received_at), "
            "       COUNT(*) FILTER (WHERE received_at > NOW() - INTERVAL '5 minutes'), "
            "       COUNT(*) FILTER (WHERE received_at > NOW() - INTERVAL '1 hour') "
            "FROM realtime.vehicle_positions WHERE agency_id = 'ttc'"
        ),
        "ttc.trip_updates": (
            "SELECT MAX(received_at), "
            "       COUNT(*) FILTER (WHERE received_at > NOW() - INTERVAL '5 minutes'), "
            "       COUNT(*) FILTER (WHERE received_at > NOW() - INTERVAL '1 hour') "
            "FROM realtime.trip_updates WHERE agency_id = 'ttc'"
        ),
        "ttc.service_alerts": (
            "SELECT MAX(last_seen_at), "
            "       COUNT(*) FILTER (WHERE last_seen_at > NOW() - INTERVAL '5 minutes'), "
            "       COUNT(*) FILTER (WHERE last_seen_at > NOW() - INTERVAL '1 hour') "
            "FROM realtime.service_alerts WHERE agency_id = 'ttc'"
        ),
    }

    snapshot_at = datetime.now(UTC)
    summary: dict[str, str] = {}

    with postgres.connection() as conn:
        with conn.cursor() as cur:
            for feed_key, query in queries.items():
                cur.execute(query)
                row = cur.fetchone()
                last_at, count_5m, count_1h = row if row else (None, 0, 0)

                lag_s = int((snapshot_at - last_at).total_seconds()) if last_at else None
                status = _classify(feed_key, lag_s)

                cur.execute(
                    """
                    INSERT INTO ops.freshness_snapshot
                        (snapshot_at, feed_key, last_record_at, record_count_5min,
                         record_count_1h, lag_seconds, status)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """,
                    (snapshot_at, feed_key, last_at, count_5m, count_1h, lag_s, status),
                )
                summary[feed_key] = f"{status} (lag={lag_s}s, 5m={count_5m})"
        conn.commit()

    context.add_output_metadata(
        {feed_key: MetadataValue.text(s) for feed_key, s in summary.items()}
    )


def _classify(feed_key: str, lag_s: int | None) -> str:
    """Map a lag reading to a status string."""
    if lag_s is None:
        return "down"
    healthy, lagging, stale = FRESHNESS_THRESHOLDS.get(feed_key, (60, 180, 600))
    if lag_s <= healthy:
        return "healthy"
    if lag_s <= lagging:
        return "lagging"
    if lag_s <= stale:
        return "stale"
    return "down"


@asset(
    name="daily_partition_maintenance",
    group_name="ops",
    compute_kind="python",
    description="Creates the next 7 days of partitions and detaches partitions older than 30 days.",
)
def daily_partition_maintenance(
    context,
    postgres: PostgresResource,
) -> None:
    """
    Range partitioning requires the partitions to exist before data lands in them.
    We create 7 days ahead so a missed run doesn't break ingestion the next day.

    Detach (not drop) old partitions: the parquet export job is expected to have
    archived them to R2 first. Dropping happens in a separate job after R2 verification.
    """
    today = date.today()
    created = []
    detached = []

    with postgres.connection() as conn:
        with conn.cursor() as cur:
            for offset in range(0, 8):  # today through today+7
                day = today + timedelta(days=offset)
                for parent in ("realtime.vehicle_positions", "realtime.trip_updates"):
                    name = _partition_name(parent, day)
                    if _ensure_partition(cur, parent, name, day):
                        created.append(name)

            # Detach partitions whose data is > 3 days old
            cutoff = today - timedelta(days=3)
            for parent in ("realtime.vehicle_positions", "realtime.trip_updates"):
                detached.extend(_detach_old_partitions(cur, parent, cutoff))

        conn.commit()

    context.add_output_metadata(
        {
            "partitions_created": MetadataValue.int(len(created)),
            "partitions_detached": MetadataValue.int(len(detached)),
            "created_names": MetadataValue.text(", ".join(created) if created else "none"),
            "detached_names": MetadataValue.text(", ".join(detached) if detached else "none"),
        }
    )


def _partition_name(parent: str, day: date) -> str:
    """e.g. realtime.vehicle_positions + 2026-04-14 -> vehicle_positions_p20260414"""
    base = parent.split(".")[1]
    return f"{base}_p{day.strftime('%Y%m%d')}"


def _ensure_partition(cur, parent: str, name: str, day: date) -> bool:
    """Create partition if it doesn't exist. Returns True if created, False if already existed."""
    schema, _ = parent.split(".")
    cur.execute(
        """
        SELECT EXISTS (
            SELECT 1 FROM information_schema.tables
            WHERE table_schema = %s AND table_name = %s
        )
        """,
        (schema, name),
    )
    if cur.fetchone()[0]:
        return False

    next_day = day + timedelta(days=1)
    # Partition bounds use [start, end) — half-open intervals.
    cur.execute(
        f"""
        CREATE TABLE {schema}.{name}
        PARTITION OF {parent}
        FOR VALUES FROM ('{day.isoformat()}') TO ('{next_day.isoformat()}')
        """
    )
    return True


def _detach_old_partitions(cur, parent: str, cutoff: date) -> list[str]:
    """Detach partitions whose date is strictly before cutoff. Returns names detached."""
    schema, base = parent.split(".")
    cur.execute(
        """
        SELECT child.relname
        FROM pg_inherits
        JOIN pg_class parent_cls ON pg_inherits.inhparent = parent_cls.oid
        JOIN pg_class child ON pg_inherits.inhrelid = child.oid
        JOIN pg_namespace ns ON parent_cls.relnamespace = ns.oid
        WHERE ns.nspname = %s AND parent_cls.relname = %s
        """,
        (schema, base),
    )
    detached = []
    for (child_name,) in cur.fetchall():
        # Parse date from suffix "_pYYYYMMDD"
        try:
            suffix = child_name.rsplit("_p", 1)[1]
            child_date = date(int(suffix[0:4]), int(suffix[4:6]), int(suffix[6:8]))
        except (ValueError, IndexError):
            continue
        if child_date < cutoff:
            cur.execute(f"ALTER TABLE {parent} DETACH PARTITION {schema}.{child_name}")
            detached.append(child_name)
    return detached


@asset(
    name="daily_parquet_export",
    group_name="ops",
    compute_kind="python",
    description=(
        "Exports yesterday's realtime data to R2 as parquet. Stub in v0.1 — "
        "wiring up the export query → pyarrow → R2 upload is straightforward; tracked in v0.1.2."
    ),
)
def daily_parquet_export(
    context,
    postgres: PostgresResource,
    r2: R2Resource,
) -> None:
    if not r2.is_configured():
        context.log.warning("R2 not configured; skipping export.")
        context.add_output_metadata({"status": "skipped_no_r2"})
        return
    context.log.info("daily_parquet_export — implementation pending in v0.1.2.")
    context.add_output_metadata({"status": "stub"})
