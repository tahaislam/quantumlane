"""
TTC GTFS-Realtime ingestion assets.

Each asset:
    1. Fetches the protobuf payload from the TTC endpoint
    2. Parses it into row dicts via the pure-functions parser
    3. Bulk-inserts into the appropriate realtime table
    4. Records a field signature for schema-drift detection
    5. Updates ops.ingestion_runs

Failure handling:
    Any uncaught exception is recorded in ops.ingestion_failures with a sample
    of the payload (first 4KB) before being re-raised so Dagster marks the run failed.
    Tenacity-level retries happen inside the GTFSRTResource for transient HTTP errors;
    by the time we reach this layer, retries have been exhausted.
"""
from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import psycopg
import structlog
from dagster import (
    Backoff,
    DailyPartitionsDefinition,
    Jitter,
    MetadataValue,
    RetryPolicy,
    asset,
)

# NOTE: Asset `context` parameters are deliberately left unannotated.
# Under `from __future__ import annotations` (used throughout this codebase for modern
# type syntax), annotations become string literals at runtime. Dagster's context-type
# validator compares the annotation against its expected classes and rejects the string.
# The simplest robust fix is to omit the annotation on `context` specifically.
# Resources passed as keyword args (gtfs_rt, postgres) are annotated normally because
# Dagster resolves those via resource_defs keys, not via type hints.

from quantumlane_ingestion.parser import (
    field_signature,
    parse_feed,
    service_alert_rows,
    trip_update_rows,
    vehicle_position_rows,
)
from quantumlane_ingestion.resources import GTFSRTResource, PostgresResource
from quantumlane_ingestion.settings import get_settings

log = structlog.get_logger(__name__)

AGENCY_ID = "ttc"

# Retry policy for the asset itself (separate from the in-resource HTTP retries).
# We keep this conservative: most failures should already have been handled at the HTTP layer.
ASSET_RETRY = RetryPolicy(max_retries=2, delay=5, backoff=Backoff.EXPONENTIAL, jitter=Jitter.FULL)


def _record_failure(
    pg: PostgresResource,
    feed_key: str,
    exc: BaseException,
    sample: bytes | None,
) -> None:
    """Persist a failure to ops.ingestion_failures. Never raises — failure to record is logged only."""
    try:
        truncated = sample is not None and len(sample) > 4096
        sample_bytes = (sample[:4096] if sample else None)
        pg.execute(
            """
            INSERT INTO ops.ingestion_failures
                (feed_key, error_class, error_message, sample_payload, sample_payload_truncated)
            VALUES (%s, %s, %s, %s, %s)
            """,
            (feed_key, type(exc).__name__, str(exc)[:2000], sample_bytes, truncated),
        )
    except Exception:
        log.exception("failed_to_record_failure", feed_key=feed_key)


def _record_field_signature(
    pg: PostgresResource,
    feed_key: str,
    sig_hash: str,
    fields: list[str],
) -> None:
    """Upsert a field signature observation. New signatures appear as new rows."""
    pg.execute(
        """
        INSERT INTO ops.feed_field_signatures (feed_key, signature_hash, populated_fields)
        VALUES (%s, %s, %s)
        ON CONFLICT (feed_key, signature_hash) DO UPDATE
            SET last_seen_at = NOW(),
                sample_count = ops.feed_field_signatures.sample_count + 1
        """,
        (feed_key, sig_hash, fields),
    )


# -----------------------------------------------------------------------------
# Vehicle Positions
# -----------------------------------------------------------------------------

@asset(
    name="ttc_vehicle_positions",
    group_name="ttc_realtime",
    compute_kind="python",
    retry_policy=ASSET_RETRY,
    description="TTC GTFS-RT VehiclePositions feed. Polled every 30 seconds.",
)
def ttc_vehicle_positions(
    context,
    gtfs_rt: GTFSRTResource,
    postgres: PostgresResource,
) -> None:
    settings = get_settings()
    feed_key = "ttc.vehicle_positions"
    payload: bytes | None = None
    try:
        payload = gtfs_rt.fetch_protobuf(settings.ttc_vehicle_positions_url)
        feed = parse_feed(payload)
        received_at = datetime.now(UTC)
        rows = vehicle_position_rows(feed, agency_id=AGENCY_ID, received_at=received_at)

        if not rows:
            context.log.warning("Feed parsed successfully but contained zero vehicle positions.")
            context.add_output_metadata({"rows_inserted": 0, "feed_entities": len(feed.entity)})
            return

        with postgres.connection() as conn:
            _bulk_insert_vehicle_positions(conn, rows)
            conn.commit()

        sig_hash, sig_fields = field_signature(feed)
        _record_field_signature(postgres, feed_key, sig_hash, sig_fields)

        context.add_output_metadata(
            {
                "rows_inserted": len(rows),
                "feed_entities": len(feed.entity),
                "feed_timestamp": MetadataValue.text(
                    datetime.fromtimestamp(feed.header.timestamp, tz=UTC).isoformat()
                    if feed.header.HasField("timestamp")
                    else "n/a"
                ),
                "field_signature": MetadataValue.text(sig_hash[:12]),
            }
        )
    except Exception as exc:
        _record_failure(postgres, feed_key, exc, payload)
        raise


def _bulk_insert_vehicle_positions(conn: psycopg.Connection, rows: list[dict[str, Any]]) -> None:
    """
    Bulk insert with ON CONFLICT DO NOTHING.
    The composite PK (received_at, agency_id, vehicle_id, feed_timestamp) means
    re-running a poll within the same second is idempotent at the row level.
    """
    sql = """
        INSERT INTO realtime.vehicle_positions (
            received_at, feed_timestamp, agency_id, vehicle_id, trip_id, route_id,
            direction_id, location, bearing, speed_mps, odometer_m,
            current_status, current_stop_sequence, stop_id, congestion_level,
            occupancy_status, raw_payload_hash
        ) VALUES (
            %(received_at)s, %(feed_timestamp)s, %(agency_id)s, %(vehicle_id)s,
            %(trip_id)s, %(route_id)s, %(direction_id)s,
            ST_SetSRID(ST_MakePoint(%(longitude)s, %(latitude)s), 4326)::geography,
            %(bearing)s, %(speed_mps)s, %(odometer_m)s,
            %(current_status)s, %(current_stop_sequence)s, %(stop_id)s,
            %(congestion_level)s, %(occupancy_status)s, %(raw_payload_hash)s
        )
        ON CONFLICT DO NOTHING
    """
    with conn.cursor() as cur:
        cur.executemany(sql, rows)


# -----------------------------------------------------------------------------
# Trip Updates
# -----------------------------------------------------------------------------

@asset(
    name="ttc_trip_updates",
    group_name="ttc_realtime",
    compute_kind="python",
    retry_policy=ASSET_RETRY,
    description="TTC GTFS-RT TripUpdates feed. Polled every 30 seconds.",
)
def ttc_trip_updates(
    context,
    gtfs_rt: GTFSRTResource,
    postgres: PostgresResource,
) -> None:
    settings = get_settings()
    feed_key = "ttc.trip_updates"
    payload: bytes | None = None
    try:
        payload = gtfs_rt.fetch_protobuf(settings.ttc_trip_updates_url)
        feed = parse_feed(payload)
        received_at = datetime.now(UTC)
        rows = trip_update_rows(feed, agency_id=AGENCY_ID, received_at=received_at)

        if not rows:
            context.log.warning("Feed parsed successfully but contained zero trip updates.")
            context.add_output_metadata({"rows_inserted": 0, "feed_entities": len(feed.entity)})
            return

        with postgres.connection() as conn:
            _bulk_insert_trip_updates(conn, rows)
            conn.commit()

        sig_hash, sig_fields = field_signature(feed)
        _record_field_signature(postgres, feed_key, sig_hash, sig_fields)

        context.add_output_metadata(
            {
                "rows_inserted": len(rows),
                "feed_entities": len(feed.entity),
                "field_signature": MetadataValue.text(sig_hash[:12]),
            }
        )
    except Exception as exc:
        _record_failure(postgres, feed_key, exc, payload)
        raise


def _bulk_insert_trip_updates(conn: psycopg.Connection, rows: list[dict[str, Any]]) -> None:
    sql = """
        INSERT INTO realtime.trip_updates (
            received_at, feed_timestamp, agency_id, trip_id, route_id, direction_id,
            start_date, schedule_relationship, stop_sequence, stop_id,
            arrival_time, arrival_delay_s, departure_time, departure_delay_s, raw_payload_hash
        ) VALUES (
            %(received_at)s, %(feed_timestamp)s, %(agency_id)s, %(trip_id)s,
            %(route_id)s, %(direction_id)s, %(start_date)s, %(schedule_relationship)s,
            %(stop_sequence)s, %(stop_id)s, %(arrival_time)s, %(arrival_delay_s)s,
            %(departure_time)s, %(departure_delay_s)s, %(raw_payload_hash)s
        )
        ON CONFLICT DO NOTHING
    """
    with conn.cursor() as cur:
        cur.executemany(sql, rows)


# -----------------------------------------------------------------------------
# Service Alerts (upsert pattern, not append)
# -----------------------------------------------------------------------------

@asset(
    name="ttc_service_alerts",
    group_name="ttc_realtime",
    compute_kind="python",
    retry_policy=ASSET_RETRY,
    description="TTC GTFS-RT ServiceAlerts feed. Polled every 5 minutes.",
)
def ttc_service_alerts(
    context,
    gtfs_rt: GTFSRTResource,
    postgres: PostgresResource,
) -> None:
    settings = get_settings()
    feed_key = "ttc.service_alerts"
    payload: bytes | None = None
    try:
        payload = gtfs_rt.fetch_protobuf(settings.ttc_service_alerts_url)
        feed = parse_feed(payload)
        received_at = datetime.now(UTC)
        rows = service_alert_rows(feed, agency_id=AGENCY_ID, received_at=received_at)

        with postgres.connection() as conn:
            if rows:
                _upsert_service_alerts(conn, rows)
            conn.commit()

        sig_hash, sig_fields = field_signature(feed)
        _record_field_signature(postgres, feed_key, sig_hash, sig_fields)

        context.add_output_metadata(
            {
                "rows_upserted": len(rows),
                "feed_entities": len(feed.entity),
                "field_signature": MetadataValue.text(sig_hash[:12]),
            }
        )
    except Exception as exc:
        _record_failure(postgres, feed_key, exc, payload)
        raise


def _upsert_service_alerts(conn: psycopg.Connection, rows: list[dict[str, Any]]) -> None:
    sql = """
        INSERT INTO realtime.service_alerts (
            alert_id, agency_id, first_seen_at, last_seen_at, feed_timestamp,
            cause, effect, severity_level, header_text, description_text,
            affected_routes, affected_stops, affected_trips,
            active_period_start, active_period_end, raw_payload_hash
        ) VALUES (
            %(alert_id)s, %(agency_id)s, %(first_seen_at)s, %(last_seen_at)s, %(feed_timestamp)s,
            %(cause)s, %(effect)s, %(severity_level)s, %(header_text)s, %(description_text)s,
            %(affected_routes)s, %(affected_stops)s, %(affected_trips)s,
            %(active_period_start)s, %(active_period_end)s, %(raw_payload_hash)s
        )
        ON CONFLICT (agency_id, alert_id) DO UPDATE SET
            last_seen_at = EXCLUDED.last_seen_at,
            feed_timestamp = EXCLUDED.feed_timestamp,
            cause = EXCLUDED.cause,
            effect = EXCLUDED.effect,
            severity_level = EXCLUDED.severity_level,
            header_text = EXCLUDED.header_text,
            description_text = EXCLUDED.description_text,
            affected_routes = EXCLUDED.affected_routes,
            affected_stops = EXCLUDED.affected_stops,
            affected_trips = EXCLUDED.affected_trips,
            active_period_start = EXCLUDED.active_period_start,
            active_period_end = EXCLUDED.active_period_end,
            raw_payload_hash = EXCLUDED.raw_payload_hash
    """
    with conn.cursor() as cur:
        cur.executemany(sql, rows)


# -----------------------------------------------------------------------------
# Static GTFS (daily reload)
# -----------------------------------------------------------------------------

# Daily partitioned: one materialization per day. The partition key is the snapshot date.
DAILY_PARTITIONS = DailyPartitionsDefinition(start_date="2026-04-01")


@asset(
    name="ttc_static_gtfs",
    group_name="ttc_static",
    compute_kind="python",
    partitions_def=DAILY_PARTITIONS,
    description="Daily snapshot of TTC's static GTFS zip. Full reload pattern (small data).",
)
def ttc_static_gtfs(
    context,
    gtfs_rt: GTFSRTResource,
    postgres: PostgresResource,
) -> None:
    """
    Reload the static GTFS reference tables.

    Why full reload, not incremental:
        Static GTFS is small (a few MB unzipped). Incremental would require
        diffing across snapshots, which adds complexity for no operational benefit
        at this scale. Full reload inside a transaction also gives us atomic snapshot
        semantics: queries either see the old data or the new, never a mix.

    The implementation is stubbed for v0.1 — wiring up the zip download, csv parsing,
    and bulk loads is straightforward but ~150 lines of mostly-mechanical code.
    Track in v0.1.1 issue.
    """
    snapshot_date = context.partition_key
    context.log.info(
        f"static_gtfs reload for {snapshot_date} — implementation pending in v0.1.1. "
        "See docs/ARCHITECTURE.md §4.1 for the design. The realtime feeds work without this."
    )
    context.add_output_metadata({"status": "stub", "snapshot_date": snapshot_date})
