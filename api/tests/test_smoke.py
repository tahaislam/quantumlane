"""
Smoke tests for the API.

These hit endpoints that don't depend on populated data — health and freshness
work even on an empty DB. Full integration tests with seeded data are deferred to v0.2.
"""
from __future__ import annotations

from fastapi.testclient import TestClient


def test_health_returns_200(api_client: TestClient) -> None:
    response = api_client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_openapi_docs_load(api_client: TestClient) -> None:
    response = api_client.get("/openapi.json")
    assert response.status_code == 200
    spec = response.json()
    assert spec["info"]["title"] == "QuantumLane API"
    assert "/v1/freshness" in spec["paths"]
