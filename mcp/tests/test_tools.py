"""
Unit tests for the MCP tools.

The valuable thing to test is resolve_route — the human-name -> route_id bridge,
which has real matching logic and real failure modes. The tools themselves are thin
wrappers, so we test them with api_client monkeypatched (no network), asserting the
resolution path and the SHAPE of what each tool returns.

We deliberately do NOT test the live API through the MCP here — that's an
integration concern, separate from these unit tests.
"""
from __future__ import annotations

import pytest

from quantumlane_mcp import api_client, server


# ---------------------------------------------------------------------------
# resolve_route — the core logic worth covering
# ---------------------------------------------------------------------------

@pytest.fixture(autouse=True)
def _patch_list_routes(monkeypatch, fake_routes):
    """Make api_client.list_routes() return the fake catalog, no network."""
    monkeypatch.setattr(api_client, "list_routes", lambda: fake_routes)


def test_resolve_exact_short_name():
    """'504' matches the 504 by exact short_name."""
    r = api_client.resolve_route("504")
    assert r is not None
    assert r["route_id"] == "r-504"


def test_resolve_is_case_insensitive_on_long_name():
    """'king' (lowercase, partial) matches '504 King' via long-name substring."""
    r = api_client.resolve_route("king")
    assert r is not None
    assert r["route_id"] == "r-504"


def test_resolve_short_name_beats_long_name():
    """A query that could match both prefers the exact short_name match.

    '1' is the subway's short_name AND a substring of several long names; exact
    short_name must win.
    """
    r = api_client.resolve_route("1")
    assert r is not None
    assert r["route_id"] == "r-line1"


def test_resolve_strips_whitespace():
    r = api_client.resolve_route("  504  ")
    assert r is not None
    assert r["route_id"] == "r-504"


def test_resolve_unknown_returns_none():
    assert api_client.resolve_route("nonexistent-route-xyz") is None


def test_resolve_name_word():
    """'Dufferin' matches the 29 Dufferin by long-name substring."""
    r = api_client.resolve_route("Dufferin")
    assert r is not None
    assert r["route_id"] == "r-29"


# ---------------------------------------------------------------------------
# vehicles_on_route — tool-level behavior
# ---------------------------------------------------------------------------

def test_vehicles_on_route_unmatched_returns_error(monkeypatch):
    """An unresolvable route yields a helpful error dict, not an exception."""
    monkeypatch.setattr(api_client, "resolve_route", lambda route: None)
    out = server.vehicles_on_route("not-a-route")
    assert "error" in out
    assert "not-a-route" in out["error"]


def test_vehicles_on_route_shape(monkeypatch):
    """A matched route returns the expected shape: route, count, vehicles list."""
    monkeypatch.setattr(
        api_client, "resolve_route",
        lambda route: {"route_id": "r-504", "route_short_name": "504", "route_long_name": "504 King"},
    )
    fake_vehicles = [
        {"vehicle_id": "1234", "route_id": "r-504", "latitude": 43.64, "longitude": -79.40},
        {"vehicle_id": "5678", "route_id": "r-504", "latitude": 43.65, "longitude": -79.38},
    ]
    monkeypatch.setattr(api_client, "vehicles_for_route_id", lambda route_id: fake_vehicles)

    out = server.vehicles_on_route("504")
    assert out["route"]["route_id"] == "r-504"
    assert out["route"]["short_name"] == "504"
    assert out["vehicle_count"] == 2
    assert out["vehicles"] == fake_vehicles
    assert "note" in out


def test_vehicles_on_route_empty_is_not_an_error(monkeypatch):
    """No vehicles reporting is a valid (empty) result, not an error."""
    monkeypatch.setattr(
        api_client, "resolve_route",
        lambda route: {"route_id": "r-504", "route_short_name": "504", "route_long_name": "504 King"},
    )
    monkeypatch.setattr(api_client, "vehicles_for_route_id", lambda route_id: [])
    out = server.vehicles_on_route("504")
    assert "error" not in out
    assert out["vehicle_count"] == 0
    assert out["vehicles"] == []


# ---------------------------------------------------------------------------
# nearest_stops — validation + shape
# ---------------------------------------------------------------------------

def test_nearest_stops_rejects_bad_coords():
    """Out-of-range coordinates return an error dict without calling the API."""
    out = server.nearest_stops(latitude=200.0, longitude=-79.0)
    assert "error" in out


def test_nearest_stops_clamps_limit(monkeypatch):
    """limit is clamped to 1..25; the clamped value reaches the api_client call."""
    captured = {}

    def fake_stops_near(lat, lon, limit):
        captured["limit"] = limit
        return []

    monkeypatch.setattr(api_client, "stops_near", fake_stops_near)
    server.nearest_stops(latitude=43.64, longitude=-79.39, limit=1000)
    assert captured["limit"] == 25  # clamped from 1000


def test_nearest_stops_shape(monkeypatch):
    fake_stops = [
        {"stop_id": "s1", "stop_name": "Front St", "latitude": 43.64, "longitude": -79.39, "distance_m": 50.0},
    ]
    monkeypatch.setattr(api_client, "stops_near", lambda lat, lon, limit: fake_stops)
    out = server.nearest_stops(latitude=43.6426, longitude=-79.3871, limit=5)
    assert out["stop_count"] == 1
    assert out["stops"] == fake_stops
    assert out["query_point"]["latitude"] == 43.6426


# ---------------------------------------------------------------------------
# list_routes — shape
# ---------------------------------------------------------------------------

def test_list_routes_shape(fake_routes):
    out = server.list_routes()
    assert out["route_count"] == len(fake_routes)
    assert out["routes"] == fake_routes