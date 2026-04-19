"""
Unit tests for the GTFS-RT parser.

The parser is pure functions — bytes in, dicts out — so it's the easiest piece to test
thoroughly. We construct synthetic FeedMessage protobufs in-process rather than fixtures
so the tests don't depend on captured TTC payloads (which would go stale).
"""
from __future__ import annotations

from datetime import UTC, datetime

import pytest
from google.transit import gtfs_realtime_pb2

from quantumlane_ingestion.parser import (
    field_signature,
    parse_feed,
    service_alert_rows,
    trip_update_rows,
    vehicle_position_rows,
)


def _build_vehicle_position_feed() -> gtfs_realtime_pb2.FeedMessage:
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.header.gtfs_realtime_version = "2.0"
    feed.header.timestamp = 1_700_000_000

    e = feed.entity.add()
    e.id = "v1"
    v = e.vehicle
    v.vehicle.id = "1234"
    v.trip.trip_id = "T-001"
    v.trip.route_id = "504"
    v.trip.direction_id = 0
    v.position.latitude = 43.6532
    v.position.longitude = -79.3832
    v.position.bearing = 90.0
    v.position.speed = 12.5
    v.current_status = 2  # in_transit_to
    v.current_stop_sequence = 5
    v.stop_id = "STOP-A"
    return feed


def _build_trip_update_feed() -> gtfs_realtime_pb2.FeedMessage:
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.header.gtfs_realtime_version = "2.0"
    feed.header.timestamp = 1_700_000_000
    e = feed.entity.add()
    e.id = "tu1"
    tu = e.trip_update
    tu.trip.trip_id = "T-001"
    tu.trip.route_id = "504"
    tu.trip.start_date = "20260414"
    stu = tu.stop_time_update.add()
    stu.stop_sequence = 5
    stu.stop_id = "STOP-A"
    stu.arrival.time = 1_700_000_300
    stu.arrival.delay = 60
    return feed


def _build_alert_feed() -> gtfs_realtime_pb2.FeedMessage:
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.header.gtfs_realtime_version = "2.0"
    feed.header.timestamp = 1_700_000_000
    e = feed.entity.add()
    e.id = "alert-001"
    a = e.alert
    a.cause = 9          # CONSTRUCTION
    a.effect = 4         # DETOUR
    a.severity_level = 2 # WARNING
    t = a.header_text.translation.add()
    t.text = "504 King: detour at Spadina"
    t.language = "en"
    ie = a.informed_entity.add()
    ie.route_id = "504"
    ap = a.active_period.add()
    ap.start = 1_700_000_000
    ap.end = 1_700_010_000
    return feed


@pytest.fixture
def now() -> datetime:
    return datetime(2026, 4, 14, 12, 0, 0, tzinfo=UTC)


def test_parse_feed_roundtrip() -> None:
    feed = _build_vehicle_position_feed()
    payload = feed.SerializeToString()
    parsed = parse_feed(payload)
    assert parsed.header.timestamp == 1_700_000_000
    assert len(parsed.entity) == 1


def test_vehicle_position_rows_extracts_all_fields(now: datetime) -> None:
    feed = _build_vehicle_position_feed()
    rows = vehicle_position_rows(feed, agency_id="ttc", received_at=now)
    assert len(rows) == 1
    row = rows[0]
    assert row["agency_id"] == "ttc"
    assert row["vehicle_id"] == "1234"
    assert row["trip_id"] == "T-001"
    assert row["route_id"] == "504"
    assert row["latitude"] == pytest.approx(43.6532)
    assert row["longitude"] == pytest.approx(-79.3832)
    assert row["bearing"] == pytest.approx(90.0)
    assert row["speed_mps"] == pytest.approx(12.5)
    assert row["current_status"] == 2
    assert row["current_stop_sequence"] == 5
    assert row["stop_id"] == "STOP-A"
    assert row["received_at"] == now
    assert row["raw_payload_hash"] is not None and len(row["raw_payload_hash"]) == 64


def test_vehicle_position_rows_skips_entities_without_position(now: datetime) -> None:
    """Entities without a position should be silently skipped, not raise."""
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.header.gtfs_realtime_version = "2.0"
    e = feed.entity.add()
    e.id = "no-position"
    e.vehicle.vehicle.id = "ghost"
    # Deliberately no position field
    rows = vehicle_position_rows(feed, agency_id="ttc", received_at=now)
    assert rows == []


def test_vehicle_position_rows_handles_missing_optional_fields(now: datetime) -> None:
    """Optional fields should serialize as None, not raise."""
    feed = gtfs_realtime_pb2.FeedMessage()
    # header.gtfs_realtime_version is a required field in the GTFS-RT protobuf schema,
    # so we must populate it even when exercising the "minimal feed" path.
    feed.header.gtfs_realtime_version = "2.0"
    e = feed.entity.add()
    e.id = "v1"
    v = e.vehicle
    v.position.latitude = 43.0
    v.position.longitude = -79.0
    # Everything else omitted
    rows = vehicle_position_rows(feed, agency_id="ttc", received_at=now)
    assert len(rows) == 1
    row = rows[0]
    assert row["vehicle_id"] is None
    assert row["trip_id"] is None
    assert row["bearing"] is None
    assert row["speed_mps"] is None


def test_trip_update_rows(now: datetime) -> None:
    feed = _build_trip_update_feed()
    rows = trip_update_rows(feed, agency_id="ttc", received_at=now)
    assert len(rows) == 1
    row = rows[0]
    assert row["trip_id"] == "T-001"
    assert row["route_id"] == "504"
    assert row["stop_id"] == "STOP-A"
    assert row["arrival_delay_s"] == 60
    assert row["arrival_time"] == datetime(2023, 11, 14, 22, 18, 20, tzinfo=UTC)


def test_service_alert_rows(now: datetime) -> None:
    feed = _build_alert_feed()
    rows = service_alert_rows(feed, agency_id="ttc", received_at=now)
    assert len(rows) == 1
    row = rows[0]
    assert row["alert_id"] == "alert-001"
    assert row["cause"] == 9
    assert row["effect"] == 4
    assert row["header_text"] == "504 King: detour at Spadina"
    assert row["affected_routes"] == ["504"]
    assert row["active_period_start"] == datetime(2023, 11, 14, 22, 13, 20, tzinfo=UTC)


def test_field_signature_is_stable_across_identical_feeds() -> None:
    f1 = _build_vehicle_position_feed()
    f2 = _build_vehicle_position_feed()
    sig1, _ = field_signature(f1)
    sig2, _ = field_signature(f2)
    assert sig1 == sig2


def test_field_signature_changes_when_optional_field_added() -> None:
    """If the upstream starts populating a new optional field, the signature should change."""
    f1 = _build_vehicle_position_feed()
    sig1, _ = field_signature(f1)

    f2 = _build_vehicle_position_feed()
    f2.entity[0].vehicle.occupancy_status = 1  # newly populated
    sig2, _ = field_signature(f2)

    assert sig1 != sig2
