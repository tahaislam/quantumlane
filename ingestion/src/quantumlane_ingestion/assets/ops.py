"""
Operational assets: freshness checks, partition maintenance, parquet exports.

These are the immune system of the platform. They run on schedules independent of
the ingestion assets and write to the ops.* schema.
"""
from __future__ import annotations

import os
import tempfile
from datetime import UTC, date, datetime, timedelta

import pyarrow as pa
import pyarrow.parquet as pq
import structlog
from dagster import MetadataValue, asset

from quantumlane_ingestion.resources import PostgresResource, S3Resource

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

# Cold-tier export config: which realtime tables get archived, and the timestamp
# column each is range-partitioned / ordered on.
EXPORT_FEEDS = {
    "trip_updates": "received_at",
    "vehicle_positions": "received_at",
}
EXPORT_CHUNK_ROWS = 200_000


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

    Retention: partitions older than 3 days are dropped (detached if attached,
    then dropped; orphaned tables from prior detach-without-drop behavior are
    swept up by name pattern). When the Iceberg cold tier (V0.3.3) lands, the
    archival write becomes an upstream step in the Dagster graph: the partition
    is archived to Iceberg, the archive is verified, and only then is it dropped.
    Until then, retention is hot-only with no archival — disposable by design
    per the V0.3.0 reset.
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

            # Drop partitions whose data is > 3 days old (was: detach-only, which leaked orphans)
            cutoff = today - timedelta(days=3)
            dropped = []
            for parent in ("realtime.vehicle_positions", "realtime.trip_updates"):
                dropped.extend(_drop_old_partitions(cur, parent, cutoff))
        conn.commit()
    context.add_output_metadata(
        {
            "partitions_created": MetadataValue.int(len(created)),
            "partitions_dropped": MetadataValue.int(len(dropped)),
            "created_names": MetadataValue.text(", ".join(created) if created else "none"),
            "dropped_names": MetadataValue.text(", ".join(dropped) if dropped else "none"),
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


def _drop_old_partitions(cur, parent: str, cutoff: date) -> list[str]:
    """
    Drop partitions whose date is strictly before cutoff. Returns names dropped.

    Finds partition-shaped tables by name pattern ({base}_pYYYYMMDD) via pg_class,
    NOT via pg_inherits — so this also reclaims previously-detached orphan tables
    left behind by the earlier detach-without-drop behavior. Attached partitions
    are detached first, then dropped; already-detached orphans are dropped directly.
    """
    schema, base = parent.split(".")

    # Names currently attached to this parent (so we know which need detaching first).
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
    attached = {row[0] for row in cur.fetchall()}

    # All partition-shaped tables for this parent, attached OR orphaned.
    cur.execute(
        """
        SELECT c.relname
        FROM pg_class c
        JOIN pg_namespace ns ON c.relnamespace = ns.oid
        WHERE ns.nspname = %s
          AND c.relkind IN ('r', 'p')
          AND c.relname ~ %s
        """,
        (schema, f"^{base}_p[0-9]{{8}}$"),
    )

    dropped = []
    for (child_name,) in cur.fetchall():
        suffix = child_name.rsplit("_p", 1)[1]
        try:
            child_date = date(int(suffix[0:4]), int(suffix[4:6]), int(suffix[6:8]))
        except (ValueError, IndexError):
            continue
        if child_date < cutoff:
            if child_name in attached:
                cur.execute(f"ALTER TABLE {parent} DETACH PARTITION {schema}.{child_name}")
            cur.execute(f"DROP TABLE IF EXISTS {schema}.{child_name}")
            dropped.append(child_name)
    return dropped


@asset(
    name="daily_parquet_export",
    group_name="ops",
    compute_kind="python",
    description=(
        "Exports the prior day's realtime feeds to the S3 cold tier as Parquet. "
        "Archives yesterday (not the partition about to drop) so there's a ~2-day "
        "buffer to catch a failed export before retention removes the source."
    ),
)
def daily_parquet_export(
    context,
    postgres: PostgresResource,
    s3: S3Resource,
) -> None:
    """
    Cold-tier archival: one Parquet file per feed per day, written to S3 under a
    Hive-style key (feed/dt=YYYY-MM-DD/part-0.parquet) so downstream Spark/Iceberg
    can prune by partition.

    Memory-safe by construction (the prod box is small and a day is millions of
    rows): a named (server-side) cursor streams the day in chunks, and a held-open
    ParquetWriter appends each chunk as a row group to a single local file before
    upload — so the full day never materializes in RAM, and we avoid the
    small-files problem.

    Idempotent: the key is deterministic per (feed, day); a re-run overwrites that
    day rather than duplicating it.

    Supersedes the v0.1 R2 stub. When the Iceberg cold tier (V0.3.3) lands, this
    becomes the archive step that runs upstream of the retention drop in
    daily_partition_maintenance (archive -> verify -> drop).
    """
    if not s3.is_configured():
        context.log.warning("S3 not configured; skipping export.")
        context.add_output_metadata({"status": MetadataValue.text("skipped_no_s3")})
        return

    # Yesterday, UTC. Half-open [day_start, day_end) so consecutive days never overlap.
    target_day = (datetime.now(UTC) - timedelta(days=1)).date()
    day_start = datetime.combine(target_day, datetime.min.time(), tzinfo=UTC)
    day_end = day_start + timedelta(days=1)

    client = s3.client()
    bucket = s3.bucket

    context.log.info(f"cold-tier export for {target_day} ({day_start} .. {day_end})")

    per_feed: dict[str, dict] = {}

    with postgres.connection() as conn:
        for table, ts_col in EXPORT_FEEDS.items():
            query = (
                f"SELECT * FROM realtime.{table} "
                f"WHERE {ts_col} >= %s AND {ts_col} < %s "
                f"ORDER BY {ts_col}"
            )

            with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
                local_path = tmp.name

            writer = None
            total_rows = 0
            try:
                # Named cursor => server-side streaming; itersize bounds the fetch batch.
                with conn.cursor(name=f"export_{table}_{target_day:%Y%m%d}") as cur:
                    cur.itersize = EXPORT_CHUNK_ROWS
                    cur.execute(query, (day_start, day_end))
                    cols = [d.name for d in cur.description]

                    batch: list[tuple] = []
                    for row in cur:
                        batch.append(row)
                        if len(batch) >= EXPORT_CHUNK_ROWS:
                            writer, n = _write_batch(writer, local_path, cols, batch)
                            total_rows += n
                            batch = []
                    if batch:
                        writer, n = _write_batch(writer, local_path, cols, batch)
                        total_rows += n

                if writer is None:
                    context.log.warning(f"{table}: 0 rows for {target_day} — skipping")
                    per_feed[table] = {"rows": 0, "bytes": 0}
                    continue
                writer.close()
                writer = None

                key = f"{table}/dt={target_day.isoformat()}/part-0.parquet"
                client.upload_file(local_path, bucket, key)
                size = os.path.getsize(local_path)

                context.log.info(
                    f"{table}: {total_rows:,} rows -> s3://{bucket}/{key} "
                    f"({size / 1e6:.2f} MB, zstd)"
                )
                per_feed[table] = {"rows": total_rows, "bytes": size}
            finally:
                if writer is not None:
                    writer.close()
                if os.path.exists(local_path):
                    os.remove(local_path)

    context.add_output_metadata(
        {
            "status": MetadataValue.text("ok"),
            "target_day": MetadataValue.text(str(target_day)),
            "trip_updates_rows": MetadataValue.int(per_feed.get("trip_updates", {}).get("rows", 0)),
            "vehicle_positions_rows": MetadataValue.int(
                per_feed.get("vehicle_positions", {}).get("rows", 0)
            ),
            "total_bytes": MetadataValue.int(sum(f["bytes"] for f in per_feed.values())),
        }
    )


def _write_batch(writer, local_path: str, cols: list[str], batch: list[tuple]):
    """Append a batch of rows to the open ParquetWriter, opening it on first use.

    Returns (writer, rows_written). The first batch defines the schema.
    """
    arrow_batch = pa.Table.from_pylist([dict(zip(cols, r, strict=True)) for r in batch])
    if writer is None:
        writer = pq.ParquetWriter(local_path, arrow_batch.schema, compression="zstd")
    writer.write_table(arrow_batch)
    return writer, len(batch)