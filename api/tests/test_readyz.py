"""
/readyz — data-freshness readiness.

The DB is mocked at the data-access boundary (`db.fetch_one`), so these tests pin
the contract an uptime monitor relies on: 200 only when every required feed has a
recent row; 503 for stale, empty, or unreachable.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from unittest.mock import patch

from fastapi.testclient import TestClient

from quantumlane_api import freshness

VP = "realtime.vehicle_positions"
TU = "realtime.trip_updates"


def _fetch_one_returning(by_table: dict[str, datetime | None]):
    """Build a fetch_one stand-in that answers per table named in the SQL."""

    def fake(sql: str, params=()):
        for table, value in by_table.items():
            if table in sql:
                return {"last_record_at": value}
        raise AssertionError(f"unexpected query: {sql}")

    return fake


def test_readyz_200_when_all_feeds_fresh(api_client: TestClient) -> None:
    now = datetime.now(UTC)
    with patch(
        "quantumlane_api.db.fetch_one",
        side_effect=_fetch_one_returning({VP: now - timedelta(seconds=5), TU: now}),
    ):
        r = api_client.get("/readyz")
    assert r.status_code == 200
    body = r.json()
    assert body["status"] == "ready"
    assert body["stale_after_seconds"] == 300
    assert {f["feed_key"] for f in body["feeds"]} == set(freshness.REQUIRED_FEEDS)
    assert not any(f["stale"] for f in body["feeds"])


def test_readyz_503_when_one_feed_stale(api_client: TestClient) -> None:
    now = datetime.now(UTC)
    with patch(
        "quantumlane_api.db.fetch_one",
        side_effect=_fetch_one_returning({VP: now - timedelta(minutes=20), TU: now}),
    ):
        r = api_client.get("/readyz")
    assert r.status_code == 503
    body = r.json()
    assert body["status"] == "stale"
    stale = {f["feed_key"]: f["stale"] for f in body["feeds"]}
    assert stale == {"ttc.vehicle_positions": True, "ttc.trip_updates": False}
    vp = next(f for f in body["feeds"] if f["feed_key"] == "ttc.vehicle_positions")
    assert vp["age_seconds"] >= 20 * 60


def test_readyz_503_when_no_rows_at_all(api_client: TestClient) -> None:
    """Empty table (MAX returns NULL) is stale, not 'unknown' — never ingested is not healthy."""
    with patch(
        "quantumlane_api.db.fetch_one",
        side_effect=_fetch_one_returning({VP: None, TU: None}),
    ):
        r = api_client.get("/readyz")
    assert r.status_code == 503
    assert all(f["last_record_at"] is None and f["stale"] for f in r.json()["feeds"])


def test_readyz_503_when_db_unreachable(api_client: TestClient) -> None:
    with patch(
        "quantumlane_api.db.fetch_one", side_effect=RuntimeError("DB pool not initialized.")
    ):
        r = api_client.get("/readyz")
    assert r.status_code == 503
    assert r.json() == {"status": "db_unreachable"}


def test_readyz_supports_head(api_client: TestClient) -> None:
    """UptimeRobot sends HEAD by default; a 405 here would be a false alarm (see INCIDENTS 2026-06-10)."""
    now = datetime.now(UTC)
    with patch(
        "quantumlane_api.db.fetch_one",
        side_effect=_fetch_one_returning({VP: now, TU: now}),
    ):
        r = api_client.head("/readyz")
    assert r.status_code == 200


def test_readiness_threshold_is_strictly_greater_than() -> None:
    """Exactly at the threshold is still fresh; one second past is stale."""
    now = datetime(2026, 11, 1, 12, 0, tzinfo=UTC)
    at_limit = now - timedelta(seconds=300)
    with patch(
        "quantumlane_api.db.fetch_one",
        side_effect=_fetch_one_returning({VP: at_limit, TU: at_limit}),
    ):
        assert freshness.readiness(300, now=now).ready is True
    with patch(
        "quantumlane_api.db.fetch_one",
        side_effect=_fetch_one_returning({VP: at_limit - timedelta(seconds=1), TU: at_limit}),
    ):
        assert freshness.readiness(300, now=now).ready is False
