"""
TTC GTFS ingestion assets — realtime feeds and the static schedule loader.

Realtime assets (vehicle positions, trip updates, service alerts) each:
    1. Fetch the protobuf payload from the TTC endpoint
    2. Parse it into row dicts via the pure-functions parser
    3. Bulk-insert into the appropriate realtime table
    4. Record a field signature for schema-drift detection
    5. Update ops.ingestion_runs

Static asset (ttc_static_gtfs) downloads the TTC's published static GTFS zip and
full-replaces the static_gtfs.* reference tables inside one transaction (atomic
snapshot). It is non-partitioned: the TTC publishes only the *current* schedule,
so there is nothing to backfill by date.

Failure handling (realtime):
    Any uncaught exception is recorded in ops.ingestion_failures with a sample
    of the payload (first 4KB) before being re-raised so Dagster marks the run failed.
    Tenacity-level retries happen inside the GTFSRTResource for transient HTTP errors;
    by the time we reach this layer, retries have been exhausted.
"""
from __future__ import annotations

import csv
import io
import zipfile
from datetime import UTC, date, datetime
from typing import Any

import httpx
import psycopg
import structlog
from dagster import (
    Backoff,
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
# Static GTFS (daily full-replace load)
# -----------------------------------------------------------------------------
#
# Non-partitioned: the TTC publishes only the *current* schedule — there's no
# "schedule as of last Tuesday" to fetch, so there's nothing to backfill by date.
# A plain ScheduleDefinition (definitions.py) drives the daily run.
#
# Full-replace (truncate-all + reload in one transaction) because the schema is
# single-snapshot (PKs are NOT compound with snapshot_date) and the reference data
# is small. One transaction gives atomic snapshot semantics — every query sees the
# old schedule or the new one, never a mix. snapshot_date is just a "loaded-on" stamp.
#
# FK load order: agency -> routes -> stops -> trips -> stop_times. calendar and
# calendar_dates are independent. shapes is large and map-only (v0.2); skipped.
#
# MEMORY: stop_times.txt is ~200 MB for TTC. It is NEVER parsed into a Python list
# or an in-memory CSV buffer — it's streamed row-by-row from the open zip straight
# into COPY (see _copy_stop_times), so only ~one row is resident at a time. The
# small tables are parsed as lists (cheap). This keeps the loader within the small
# box's memory budget. The whole DB load runs inside the open-zip context because
# the stop_times stream reads from the live zip handle.

LOAD_SHAPES = False

# Truncated together in one statement, which sidesteps per-table FK ordering for the
# truncate (the load below still respects FK order).
STATIC_TABLES = [
    "static_gtfs.stop_times",
    "static_gtfs.trips",
    "static_gtfs.stops",
    "static_gtfs.routes",
    "static_gtfs.agency",
    "static_gtfs.calendar",
    "static_gtfs.calendar_dates",
    "static_gtfs.shapes",
]

# GTFS zip magic bytes — guards against a moved/redirected URL returning HTML.
_ZIP_MAGIC = b"PK\x03\x04"


def _b(val: str | None) -> bool:
    """GTFS 0/1 -> bool."""
    return str(val).strip() == "1"


def _i(val: str | None) -> int | None:
    """Parse int; empty string -> None."""
    if val is None or val.strip() == "":
        return None
    return int(val)


def _f(val: str | None) -> float | None:
    """Parse float; empty string -> None."""
    if val is None or val.strip() == "":
        return None
    return float(val)


def _t(val: str | None) -> str | None:
    """Trim text; empty string -> None so optional columns are NULL, not ''."""
    if val is None:
        return None
    v = val.strip()
    return v or None


def _interval(val: str | None) -> str | None:
    """
    GTFS time string ('HH:MM:SS', possibly >24h like '25:30:00') -> the same string.
    Postgres parses it directly as an INTERVAL on input. Empty -> None.
    """
    if val is None or val.strip() == "":
        return None
    return val.strip()


def _gtfs_date(val: str | None) -> date | None:
    """GTFS date 'YYYYMMDD' -> date. Empty -> None."""
    if val is None or val.strip() == "":
        return None
    s = val.strip()
    return date(int(s[0:4]), int(s[4:6]), int(s[6:8]))


def _csv_null(val: Any) -> str:
    r"""Render None as the COPY NULL token (\N); everything else as its string."""
    return r"\N" if val is None else str(val)


def _read_csv(zf: zipfile.ZipFile, name: str) -> list[dict[str, str]]:
    """Read one (small) GTFS .txt from the zip into dict rows.

    For small tables only — do NOT use on stop_times (~200 MB), which is streamed.
    Returns [] if the file isn't present. utf-8-sig strips a BOM so the first header
    key isn't mangled.
    """
    try:
        raw = zf.read(name)
    except KeyError:
        log.warning("gtfs_file_absent", file=name)
        return []
    text = io.TextIOWrapper(io.BytesIO(raw), encoding="utf-8-sig")
    return list(csv.DictReader(text))


@asset(
    name="ttc_static_gtfs",
    group_name="ttc_static",
    compute_kind="python",
    description="Daily full-replace load of TTC's static GTFS into static_gtfs.* (atomic snapshot).",
)
def ttc_static_gtfs(
    context,
    postgres: PostgresResource,
) -> None:
    """
    Download the TTC static GTFS zip, parse the CSVs, and full-replace the
    static_gtfs.* tables inside one transaction (atomic snapshot).

    Non-partitioned and full-replace by design — see the module-level note above.
    stop_times (~200 MB) is streamed into COPY to stay within memory.
    """
    settings = get_settings()
    snapshot_date = datetime.now(UTC).date()

    # 1. Download the zip.
    url = settings.ttc_static_gtfs_url
    context.log.info(f"Downloading static GTFS from {url}")
    with httpx.Client(
        timeout=settings.http_timeout_seconds,
        headers={"User-Agent": settings.http_user_agent},
        follow_redirects=True,
    ) as client:
        resp = client.get(url)
        resp.raise_for_status()
    zip_bytes = resp.content
    context.log.info(f"Downloaded {len(zip_bytes) / 1e6:.1f} MB")

    # Guard: a moved/redirected URL often returns an HTML page (200 OK), which would
    # fail later with a cryptic BadZipFile. Fail early and clearly instead.
    if not zip_bytes.startswith(_ZIP_MAGIC):
        raise ValueError(
            f"Downloaded content from {url} is not a zip (first bytes: {zip_bytes[:16]!r}). "
            "The URL may have moved or returned an error page."
        )

    counts: dict[str, int] = {}

    # 2+3. Parse small tables, then truncate-all + load — ALL inside the open-zip
    # context, because stop_times streams directly from the zip handle into COPY.
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        agency_rows = _read_csv(zf, "agency.txt")
        routes_rows = _read_csv(zf, "routes.txt")
        stops_rows = _read_csv(zf, "stops.txt")
        trips_rows = _read_csv(zf, "trips.txt")
        calendar_rows = _read_csv(zf, "calendar.txt")
        calendar_dates_rows = _read_csv(zf, "calendar_dates.txt")
        # stop_times is NOT read here — streamed below to avoid OOM.
        shapes_rows = _read_csv(zf, "shapes.txt") if LOAD_SHAPES else []

        with postgres.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("TRUNCATE " + ", ".join(STATIC_TABLES))

                # --- agency ---
                agency_data = [
                    (
                        _t(r.get("agency_id")) or "ttc",
                        _t(r.get("agency_name")),
                        _t(r.get("agency_url")),
                        _t(r.get("agency_timezone")),
                        _t(r.get("agency_lang")),
                        snapshot_date,
                    )
                    for r in agency_rows
                ]
                cur.executemany(
                    "INSERT INTO static_gtfs.agency "
                    "(agency_id, agency_name, agency_url, agency_timezone, agency_lang, snapshot_date) "
                    "VALUES (%s, %s, %s, %s, %s, %s)",
                    agency_data,
                )
                counts["agency"] = len(agency_data)

                # --- routes ---
                routes_data = [
                    (
                        _t(r.get("route_id")),
                        _t(r.get("agency_id")) or "ttc",
                        _t(r.get("route_short_name")),
                        _t(r.get("route_long_name")),
                        _i(r.get("route_type")),
                        _t(r.get("route_color")),
                        _t(r.get("route_text_color")),
                        snapshot_date,
                    )
                    for r in routes_rows
                ]
                cur.executemany(
                    "INSERT INTO static_gtfs.routes "
                    "(route_id, agency_id, route_short_name, route_long_name, route_type, "
                    " route_color, route_text_color, snapshot_date) "
                    "VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
                    routes_data,
                )
                counts["routes"] = len(routes_data)

                # --- stops --- (lon FIRST in ST_MakePoint)
                stops_data = [
                    (
                        _t(r.get("stop_id")),
                        _t(r.get("stop_code")),
                        _t(r.get("stop_name")),
                        _t(r.get("stop_desc")),
                        _f(r.get("stop_lon")),
                        _f(r.get("stop_lat")),
                        _t(r.get("zone_id")),
                        _i(r.get("location_type")),
                        _t(r.get("parent_station")),
                        snapshot_date,
                    )
                    for r in stops_rows
                ]
                cur.executemany(
                    "INSERT INTO static_gtfs.stops "
                    "(stop_id, stop_code, stop_name, stop_desc, location, zone_id, "
                    " location_type, parent_station, snapshot_date) "
                    "VALUES (%s, %s, %s, %s, "
                    "        ST_SetSRID(ST_MakePoint(%s, %s), 4326)::geography, "
                    "        %s, %s, %s, %s)",
                    stops_data,
                )
                counts["stops"] = len(stops_data)

                # --- trips ---
                trips_data = [
                    (
                        _t(r.get("trip_id")),
                        _t(r.get("route_id")),
                        _t(r.get("service_id")),
                        _t(r.get("trip_headsign")),
                        _i(r.get("direction_id")),
                        _t(r.get("block_id")),
                        _t(r.get("shape_id")),
                        snapshot_date,
                    )
                    for r in trips_rows
                ]
                cur.executemany(
                    "INSERT INTO static_gtfs.trips "
                    "(trip_id, route_id, service_id, trip_headsign, direction_id, "
                    " block_id, shape_id, snapshot_date) "
                    "VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
                    trips_data,
                )
                counts["trips"] = len(trips_data)

                # --- stop_times --- (~200 MB: streamed from the zip into COPY)
                counts["stop_times"] = _copy_stop_times(cur, zf, snapshot_date)

                # --- calendar --- (0/1 -> bool, YYYYMMDD -> date)
                calendar_data = [
                    (
                        _t(r.get("service_id")),
                        _b(r.get("monday")),
                        _b(r.get("tuesday")),
                        _b(r.get("wednesday")),
                        _b(r.get("thursday")),
                        _b(r.get("friday")),
                        _b(r.get("saturday")),
                        _b(r.get("sunday")),
                        _gtfs_date(r.get("start_date")),
                        _gtfs_date(r.get("end_date")),
                        snapshot_date,
                    )
                    for r in calendar_rows
                ]
                cur.executemany(
                    "INSERT INTO static_gtfs.calendar "
                    "(service_id, monday, tuesday, wednesday, thursday, friday, saturday, "
                    " sunday, start_date, end_date, snapshot_date) "
                    "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                    calendar_data,
                )
                counts["calendar"] = len(calendar_data)

                # --- calendar_dates ---
                cal_dates_data = [
                    (
                        _t(r.get("service_id")),
                        _gtfs_date(r.get("date")),
                        _i(r.get("exception_type")),
                        snapshot_date,
                    )
                    for r in calendar_dates_rows
                ]
                cur.executemany(
                    "INSERT INTO static_gtfs.calendar_dates "
                    "(service_id, exception_date, exception_type, snapshot_date) "
                    "VALUES (%s, %s, %s, %s)",
                    cal_dates_data,
                )
                counts["calendar_dates"] = len(cal_dates_data)

                # --- shapes --- (skipped by default; map-only, v0.2)
                if LOAD_SHAPES and shapes_rows:
                    shapes_data = [
                        (
                            _t(r.get("shape_id")),
                            _i(r.get("shape_pt_sequence")),
                            _f(r.get("shape_pt_lon")),
                            _f(r.get("shape_pt_lat")),
                            _f(r.get("shape_dist_traveled")),
                            snapshot_date,
                        )
                        for r in shapes_rows
                    ]
                    cur.executemany(
                        "INSERT INTO static_gtfs.shapes "
                        "(shape_id, shape_pt_sequence, shape_pt_location, shape_dist_traveled, "
                        " snapshot_date) "
                        "VALUES (%s, %s, ST_SetSRID(ST_MakePoint(%s, %s), 4326)::geography, %s, %s)",
                        shapes_data,
                    )
                    counts["shapes"] = len(shapes_data)

            conn.commit()  # single atomic commit — the snapshot flips here

    context.log.info(f"Static GTFS loaded for {snapshot_date}: {counts}")
    context.add_output_metadata(
        {
            "snapshot_date": MetadataValue.text(str(snapshot_date)),
            **{f"{k}_rows": MetadataValue.int(v) for k, v in counts.items()},
            "shapes_loaded": MetadataValue.bool(LOAD_SHAPES),
        }
    )


def _copy_stop_times(cur, zf: zipfile.ZipFile, snapshot_date: date) -> int:
    """
    Stream stop_times.txt from the open zip straight into COPY, one row at a time.

    stop_times is ~200 MB for TTC — never materialize it as a Python list or an
    in-memory CSV buffer (either would OOM the small box). We read a row from the
    zip, format a single CSV line, write it to the COPY stream, and move on, holding
    ~one row at a time.

    arrival_time/departure_time are GTFS strings (possibly >24h) parsed by Postgres
    as INTERVAL on input. Empty optional fields -> the COPY NULL token (\\N).
    """
    n = 0
    line_buf = io.StringIO()
    line_writer = csv.writer(line_buf)
    with zf.open("stop_times.txt") as raw:
        text = io.TextIOWrapper(raw, encoding="utf-8-sig")
        reader = csv.DictReader(text)
        with cur.copy(
            "COPY static_gtfs.stop_times "
            "(trip_id, stop_sequence, stop_id, arrival_time, departure_time, "
            " pickup_type, drop_off_type, shape_dist_traveled, snapshot_date) "
            "FROM STDIN WITH (FORMAT csv, NULL '\\N')"
        ) as copy:
            for r in reader:
                line_buf.seek(0)
                line_buf.truncate(0)
                line_writer.writerow(
                    [
                        (r.get("trip_id") or "").strip(),
                        _i(r.get("stop_sequence")),
                        (r.get("stop_id") or "").strip(),
                        _csv_null(_interval(r.get("arrival_time"))),
                        _csv_null(_interval(r.get("departure_time"))),
                        _csv_null(_i(r.get("pickup_type"))),
                        _csv_null(_i(r.get("drop_off_type"))),
                        _csv_null(_f(r.get("shape_dist_traveled"))),
                        snapshot_date.isoformat(),
                    ]
                )
                copy.write(line_buf.getvalue())
                n += 1
    return n