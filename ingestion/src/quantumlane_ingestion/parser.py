"""
Pure functions that convert GTFS-RT protobuf messages into row dicts
ready for insertion. Kept separate from Dagster so they're trivially unit-testable.

Design notes:
    - These functions take bytes in, return list[dict] out. No I/O, no logging side effects.
    - Field-population signatures are computed here too (used for schema-drift detection).
    - GTFS-RT optional fields are *frequently* not populated; defensive .HasField checks throughout.
"""
from __future__ import annotations

import hashlib
from datetime import UTC, datetime
from typing import Any

from google.transit import gtfs_realtime_pb2  # type: ignore[import-untyped]


def parse_feed(payload: bytes) -> gtfs_realtime_pb2.FeedMessage:
    """Parse raw protobuf bytes into a FeedMessage. Raises on invalid protobuf."""
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(payload)
    return feed


def feed_timestamp(feed: gtfs_realtime_pb2.FeedMessage) -> datetime:
    """Extract the feed's header timestamp as a UTC datetime."""
    if not feed.header.HasField("timestamp"):
        # Some agencies don't populate header timestamp. Fall back to now.
        return datetime.now(UTC)
    return datetime.fromtimestamp(feed.header.timestamp, tz=UTC)


def payload_hash(payload: bytes) -> str:
    """sha256 of the full protobuf payload. Used for dedupe and audit."""
    return hashlib.sha256(payload).hexdigest()


def vehicle_position_rows(
    feed: gtfs_realtime_pb2.FeedMessage,
    *,
    agency_id: str,
    received_at: datetime,
) -> list[dict[str, Any]]:
    """
    Convert VehiclePosition entities to row dicts matching realtime.vehicle_positions schema.

    Notes on GTFS-RT semantics that matter here:
        - vehicle.timestamp is per-vehicle and may differ from the feed header timestamp.
          We persist feed_timestamp (header) for ingestion lag analysis, and use vehicle.timestamp
          where available for the actual position observation time (not modeled in v0.1; deferred).
        - position.bearing is degrees clockwise from north, [0, 360).
        - position.speed is meters/second per the spec, but some agencies emit km/h. We trust the spec.
    """
    fts = feed_timestamp(feed)
    rows: list[dict[str, Any]] = []
    payload_hash_str = payload_hash(feed.SerializeToString())

    for entity in feed.entity:
        if not entity.HasField("vehicle"):
            continue
        v = entity.vehicle
        if not v.HasField("position"):
            continue  # without a position, the row is meaningless

        pos = v.position
        trip = v.trip if v.HasField("trip") else None
        vehicle = v.vehicle if v.HasField("vehicle") else None

        rows.append(
            {
                "received_at": received_at,
                "feed_timestamp": fts,
                "agency_id": agency_id,
                "vehicle_id": vehicle.id if vehicle and vehicle.HasField("id") else None,
                "trip_id": trip.trip_id if trip and trip.HasField("trip_id") else None,
                "route_id": trip.route_id if trip and trip.HasField("route_id") else None,
                "direction_id": trip.direction_id if trip and trip.HasField("direction_id") else None,
                "longitude": pos.longitude,
                "latitude": pos.latitude,
                "bearing": pos.bearing if pos.HasField("bearing") else None,
                "speed_mps": pos.speed if pos.HasField("speed") else None,
                "odometer_m": pos.odometer if pos.HasField("odometer") else None,
                "current_status": v.current_status if v.HasField("current_status") else None,
                "current_stop_sequence": (
                    v.current_stop_sequence if v.HasField("current_stop_sequence") else None
                ),
                "stop_id": v.stop_id if v.HasField("stop_id") else None,
                "congestion_level": v.congestion_level if v.HasField("congestion_level") else None,
                "occupancy_status": v.occupancy_status if v.HasField("occupancy_status") else None,
                "raw_payload_hash": payload_hash_str,
            }
        )
    return rows


def trip_update_rows(
    feed: gtfs_realtime_pb2.FeedMessage,
    *,
    agency_id: str,
    received_at: datetime,
) -> list[dict[str, Any]]:
    """
    Convert TripUpdate entities to row dicts. One row per StopTimeUpdate
    (so a single TripUpdate with 30 stops becomes 30 rows).
    """
    fts = feed_timestamp(feed)
    rows: list[dict[str, Any]] = []
    payload_hash_str = payload_hash(feed.SerializeToString())

    for entity in feed.entity:
        if not entity.HasField("trip_update"):
            continue
        tu = entity.trip_update
        trip = tu.trip
        for stu in tu.stop_time_update:
            arrival_dt = (
                datetime.fromtimestamp(stu.arrival.time, tz=UTC)
                if stu.HasField("arrival") and stu.arrival.HasField("time")
                else None
            )
            departure_dt = (
                datetime.fromtimestamp(stu.departure.time, tz=UTC)
                if stu.HasField("departure") and stu.departure.HasField("time")
                else None
            )
            rows.append(
                {
                    "received_at": received_at,
                    "feed_timestamp": fts,
                    "agency_id": agency_id,
                    "trip_id": trip.trip_id if trip.HasField("trip_id") else None,
                    "route_id": trip.route_id if trip.HasField("route_id") else None,
                    "direction_id": trip.direction_id if trip.HasField("direction_id") else None,
                    "start_date": _parse_yyyymmdd(trip.start_date) if trip.HasField("start_date") else None,
                    "schedule_relationship": (
                        trip.schedule_relationship if trip.HasField("schedule_relationship") else None
                    ),
                    "stop_sequence": stu.stop_sequence if stu.HasField("stop_sequence") else None,
                    "stop_id": stu.stop_id if stu.HasField("stop_id") else None,
                    "arrival_time": arrival_dt,
                    "arrival_delay_s": (
                        stu.arrival.delay if stu.HasField("arrival") and stu.arrival.HasField("delay") else None
                    ),
                    "departure_time": departure_dt,
                    "departure_delay_s": (
                        stu.departure.delay
                        if stu.HasField("departure") and stu.departure.HasField("delay")
                        else None
                    ),
                    "raw_payload_hash": payload_hash_str,
                }
            )
    return rows


def service_alert_rows(
    feed: gtfs_realtime_pb2.FeedMessage,
    *,
    agency_id: str,
    received_at: datetime,
) -> list[dict[str, Any]]:
    """
    Convert Alert entities to row dicts. Alerts are upserted on (agency_id, alert_id)
    with last_seen_at refreshed each cycle.
    """
    fts = feed_timestamp(feed)
    rows: list[dict[str, Any]] = []
    payload_hash_str = payload_hash(feed.SerializeToString())

    for entity in feed.entity:
        if not entity.HasField("alert"):
            continue
        a = entity.alert

        affected_routes = sorted({s.route_id for s in a.informed_entity if s.HasField("route_id")})
        affected_stops = sorted({s.stop_id for s in a.informed_entity if s.HasField("stop_id")})
        affected_trips = sorted(
            {s.trip.trip_id for s in a.informed_entity if s.HasField("trip") and s.trip.HasField("trip_id")}
        )

        # active_period is repeated; v0.1 takes the first window. v0.2 may model multi-window alerts.
        active_start = active_end = None
        if a.active_period:
            ap = a.active_period[0]
            if ap.HasField("start"):
                active_start = datetime.fromtimestamp(ap.start, tz=UTC)
            if ap.HasField("end"):
                active_end = datetime.fromtimestamp(ap.end, tz=UTC)

        rows.append(
            {
                "alert_id": entity.id,
                "agency_id": agency_id,
                "first_seen_at": received_at,
                "last_seen_at": received_at,
                "feed_timestamp": fts,
                "cause": a.cause if a.HasField("cause") else None,
                "effect": a.effect if a.HasField("effect") else None,
                "severity_level": a.severity_level if a.HasField("severity_level") else None,
                "header_text": _translated_text(a.header_text),
                "description_text": _translated_text(a.description_text),
                "affected_routes": affected_routes,
                "affected_stops": affected_stops,
                "affected_trips": affected_trips,
                "active_period_start": active_start,
                "active_period_end": active_end,
                "raw_payload_hash": payload_hash_str,
            }
        )
    return rows


def field_signature(feed: gtfs_realtime_pb2.FeedMessage) -> tuple[str, list[str]]:
    """
    Compute a stable signature of which optional fields are populated across all entities.
    Used for schema-drift detection: if the upstream starts populating a new field
    (or stops populating one), the signature hash changes and ops can investigate.

    Returns (sha256_hex, sorted_field_paths).
    """
    paths: set[str] = set()
    for entity in feed.entity:
        _collect_populated_paths(entity, "", paths)
    sorted_paths = sorted(paths)
    sig = hashlib.sha256("\n".join(sorted_paths).encode()).hexdigest()
    return sig, sorted_paths


def _collect_populated_paths(message: Any, prefix: str, out: set[str]) -> None:
    """
    Walk a protobuf message and collect dotted paths of populated fields.

    Uses the modern protobuf 5.x FieldDescriptor API (is_repeated, type attrs).
    The older `label == LABEL_REPEATED` pattern doesn't work under the upb-backed
    C++ implementation shipped with protobuf >= 5.
    """
    from google.protobuf.descriptor import FieldDescriptor

    for field, value in message.ListFields():
        path = f"{prefix}.{field.name}" if prefix else field.name
        out.add(path)
        if field.type == FieldDescriptor.TYPE_MESSAGE:
            if field.is_repeated:
                for item in value:
                    _collect_populated_paths(item, path, out)
            else:
                _collect_populated_paths(value, path, out)


def _translated_text(translated: Any) -> str | None:
    """Pull a single string out of a TranslatedString. Prefers English, falls back to first available."""
    if not translated.translation:
        return None
    for t in translated.translation:
        if t.language and t.language.lower().startswith("en"):
            return t.text
    return translated.translation[0].text


def _parse_yyyymmdd(s: str) -> Any:
    """Parse a GTFS-RT YYYYMMDD date string into a date. Returns None on parse failure."""
    from datetime import date

    try:
        return date(int(s[0:4]), int(s[4:6]), int(s[6:8]))
    except (ValueError, IndexError):
        return None
