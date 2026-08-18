"""
Tests for the deep, data-freshness-aware readiness probe.

/health and /ready both stayed green during the 2026-08-14 disk-full incident
because they only check that the process/DB connection is alive, not that
ingestion is actually producing data. /readyz closes that gap by checking the
age of the newest realtime.vehicle_positions row.
"""
from __future__ import annotations

from unittest.mock import patch

from fastapi.testclient import TestClient


def test_readyz_returns_200_when_data_is_fresh(api_client: TestClient) -> None:
    with patch("quantumlane_api.db.vehicle_positions_age_seconds", return_value=12.0):
        response = api_client.get("/readyz")
    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "ready"
    assert body["data_age_seconds"] == 12


def test_readyz_returns_503_when_data_is_stale(api_client: TestClient) -> None:
    with patch("quantumlane_api.db.vehicle_positions_age_seconds", return_value=600.0):
        response = api_client.get("/readyz")
    assert response.status_code == 503
    body = response.json()
    assert body["status"] == "stale"
    assert body["data_age_seconds"] == 600


def test_readyz_returns_503_when_no_data_at_all(api_client: TestClient) -> None:
    with patch("quantumlane_api.db.vehicle_positions_age_seconds", return_value=None):
        response = api_client.get("/readyz")
    assert response.status_code == 503
    assert response.json()["status"] == "no_data"


def test_readyz_returns_503_when_db_unreachable(api_client: TestClient) -> None:
    with patch("quantumlane_api.db.vehicle_positions_age_seconds", side_effect=RuntimeError("boom")):
        response = api_client.get("/readyz")
    assert response.status_code == 503
    assert response.json()["status"] == "db_unreachable"


def test_readyz_boundary_is_exactly_on_threshold(api_client: TestClient) -> None:
    """Age equal to the threshold is still ready; only strictly-over is stale."""
    from quantumlane_api.settings import get_settings

    threshold = get_settings().readyz_max_staleness_seconds
    with patch("quantumlane_api.db.vehicle_positions_age_seconds", return_value=float(threshold)):
        response = api_client.get("/readyz")
    assert response.status_code == 200
    assert response.json()["status"] == "ready"
