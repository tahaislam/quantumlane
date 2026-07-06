"""
Tests for /v1/alerts — the alerts + inferred-classification read path.

db.fetch_all is patched with canned rows (the dict shape psycopg's dict_row
produces for the lateral-join query), so these exercise the shared query
shaping in quantumlane_api.queries plus the response models, without a DB.
"""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import patch

from fastapi.testclient import TestClient

NOW = datetime(2026, 7, 6, 12, 0, 0, tzinfo=UTC)


def _row(alert_id: str, classified: bool) -> dict:
    """One row as returned by the alerts+classifications lateral join."""
    return {
        "agency_id": "ttc",
        "alert_id": alert_id,
        "header_text": "504 King detour via Queen",
        "description_text": None,
        "cause": None,
        "effect": 6,
        "severity_level": None,
        "affected_routes": ["504"],
        "affected_stops": [],
        "active_period_start": None,
        "active_period_end": None,
        "first_seen_at": NOW,
        "last_seen_at": NOW,
        "cause_inferred": "CONSTRUCTION" if classified else None,
        "cause_detail": "watermain construction" if classified else None,
        "severity_inferred": "WARNING" if classified else None,
        "inferred_model": "claude-haiku-4-5" if classified else None,
        "inferred_prompt_version": "v1" if classified else None,
        "classified_at": NOW if classified else None,
    }


def test_alerts_returns_native_and_inferred_fields(api_client: TestClient) -> None:
    rows = [_row("A1", classified=True), _row("A2", classified=False)]
    with patch("quantumlane_api.db.fetch_all", return_value=rows):
        response = api_client.get("/v1/alerts")

    assert response.status_code == 200
    data = response.json()["data"]
    assert len(data) == 2

    classified = data[0]
    assert classified["alert_id"] == "A1"
    assert classified["effect"] == 6  # native enum code, untouched
    assert classified["cause"] is None  # native cause stays empty — never backfilled
    assert classified["inferred"]["cause"] == "CONSTRUCTION"
    assert classified["inferred"]["cause_detail"] == "watermain construction"
    assert classified["inferred"]["severity"] == "WARNING"
    assert classified["inferred"]["model"] == "claude-haiku-4-5"
    assert classified["inferred"]["prompt_version"] == "v1"

    unclassified = data[1]
    assert unclassified["alert_id"] == "A2"
    assert unclassified["inferred"] is None


def test_alerts_rejects_out_of_range_limit(api_client: TestClient) -> None:
    assert api_client.get("/v1/alerts?limit=0").status_code == 400
    assert api_client.get("/v1/alerts?limit=501").status_code == 400


def test_alerts_in_openapi_spec(api_client: TestClient) -> None:
    spec = api_client.get("/openapi.json").json()
    assert "/v1/alerts" in spec["paths"]
