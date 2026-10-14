"""
Unit tests for the pure helpers in the ops assets.

The assets themselves need a database; the pieces that decide what a freshness
snapshot says do not, so those are tested here.
"""
from __future__ import annotations

from datetime import UTC, datetime

from quantumlane_ingestion.assets.ops import _classify, _resolve_last_record

HOT = datetime(2026, 10, 13, 12, 0, tzinfo=UTC)
WATERMARK = datetime(2026, 9, 10, 15, 50, tzinfo=UTC)


def test_hot_tier_wins_when_present() -> None:
    assert _resolve_last_record(HOT, WATERMARK) == (HOT, "hot")
    assert _resolve_last_record(HOT, None) == (HOT, "hot")


def test_watermark_backfills_rotated_hot_tier() -> None:
    # Outage longer than retention: MAX(received_at) is NULL but the feed did flow once.
    assert _resolve_last_record(None, WATERMARK) == (WATERMARK, "watermark")


def test_never_ingested_is_distinguishable() -> None:
    assert _resolve_last_record(None, None) == (None, None)


def test_classify_thresholds() -> None:
    assert _classify("ttc.vehicle_positions", 30) == "healthy"
    assert _classify("ttc.vehicle_positions", 120) == "lagging"
    assert _classify("ttc.vehicle_positions", 400) == "stale"
    assert _classify("ttc.vehicle_positions", 2_889_542) == "down"
    # Alerts tolerate an hour of silence; the same lag is "healthy" there.
    assert _classify("ttc.service_alerts", 400) == "healthy"


def test_classify_no_timestamp_is_down() -> None:
    assert _classify("ttc.vehicle_positions", None) == "down"
