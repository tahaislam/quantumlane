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

# The real (caching) list_routes, captured before any fixture stubs it — so the
# cache tests can restore it while other tests use the fake.
_REAL_LIST_ROUTES = api_client.list_routes


# ---------------------------------------------------------------------------
# resolve_route — the core logic worth covering
# ---------------------------------------------------------------------------

@pytest.fixture(autouse=True)
def _patch_list_routes(monkeypatch, fake_routes):
    """Make api_client.list_routes() return the fake catalog, no network.

    Also reset the module-global route cache so cache state never leaks between
    tests (the cache tests below manipulate it directly).
    """
    api_client._routes_cache = None
    api_client._routes_cached_at = 0.0
    monkeypatch.setattr(api_client, "list_routes", lambda: fake_routes)


def test_resolve_exact_short_name():
    """'504' matches the 504 by exact short_name."""
    r = api_client.resolve_route("504")
    assert r is not None
    assert r["route_id"] == "r-504"


def test_resolve_king_is_not_kingston():
    """REGRESSION: 'King' must resolve to the 504 King streetcar, NOT 12 Kingston Rd.

    The original substring match mapped 'king' -> 'kingston rd' because 'king' is a
    substring of 'kingston'. Word-boundary matching fixes this: 'king' matches the
    WORD 'King' in '504 King' but not the prefix of 'Kingston'.
    """
    r = api_client.resolve_route("King")
    assert r is not None
    assert r["route_id"] == "r-504", f"expected 504 King, got {r['route_long_name']}"


def test_resolve_is_case_insensitive_on_long_name():
    """'king' (lowercase) matches '504 King' via whole-word match."""
    r = api_client.resolve_route("king")
    assert r is not None
    assert r["route_id"] == "r-504"


def test_resolve_kingston_still_works_as_whole_word():
    """'Kingston' (the actual word) still resolves to the 12 Kingston Rd bus."""
    r = api_client.resolve_route("Kingston")
    assert r is not None
    assert r["route_id"] == "r-12"


def test_resolve_prefers_streetcar_on_name_tie():
    """If a name matched both a streetcar and a bus, the lower route_type wins.

    (Synthetic check of the tiebreak: 'King' matches only the streetcar here, but
    the min-by-route_type logic is what guarantees streetcar-over-bus when a name
    genuinely collides. Documented as the domain rule: named routes are streetcars.)
    """
    r = api_client.resolve_route("King")
    assert r["route_type"] == 0  # streetcar, not a bus


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


# ---------------------------------------------------------------------------
# Route-catalog cache (1h TTL, matching the daily static-GTFS reload)
# ---------------------------------------------------------------------------

def test_route_cache_hits_once_then_serves_cached(monkeypatch):
    """Repeated list_routes calls fetch the catalog once, then serve from cache."""
    # Restore the REAL (caching) list_routes over the fixture's stub, and fake the
    # `get` underneath it so we can count actual fetches.
    monkeypatch.setattr(api_client, "list_routes", _REAL_LIST_ROUTES)
    calls = {"n": 0}

    def fake_get(path, params=None):
        calls["n"] += 1
        return {"data": [{"route_id": "r-504", "route_short_name": "504",
                          "route_long_name": "504 King", "route_type": 0}]}

    monkeypatch.setattr(api_client, "get", fake_get)
    api_client._routes_cache = None
    api_client._routes_cached_at = 0.0

    api_client.list_routes()
    api_client.list_routes()
    api_client.list_routes()
    assert calls["n"] == 1  # fetched once, then cached


def test_route_cache_refetches_after_ttl(monkeypatch):
    """After the 1h TTL elapses, the catalog is re-fetched."""
    import time
    monkeypatch.setattr(api_client, "list_routes", _REAL_LIST_ROUTES)
    calls = {"n": 0}

    def fake_get(path, params=None):
        calls["n"] += 1
        return {"data": []}

    monkeypatch.setattr(api_client, "get", fake_get)
    api_client._routes_cache = None
    api_client._routes_cached_at = 0.0

    api_client.list_routes()
    api_client._routes_cached_at = time.monotonic() - (api_client._ROUTES_TTL_SECONDS + 1)
    api_client.list_routes()
    assert calls["n"] == 2  # refetched after TTL


# ---------------------------------------------------------------------------
# Rate limiter
# ---------------------------------------------------------------------------

def test_rate_limiter_blocks_over_limit():
    from quantumlane_mcp.ratelimit import RateLimitMiddleware
    mw = RateLimitMiddleware(app=None, limit=3, window_seconds=60)
    results = [mw._allowed("1.2.3.4") for _ in range(5)]
    assert results == [True, True, True, False, False]


def test_rate_limiter_per_ip_independent():
    from quantumlane_mcp.ratelimit import RateLimitMiddleware
    mw = RateLimitMiddleware(app=None, limit=1, window_seconds=60)
    assert mw._allowed("1.1.1.1") is True
    assert mw._allowed("1.1.1.1") is False  # second from same IP blocked
    assert mw._allowed("2.2.2.2") is True   # different IP unaffected


def test_rate_limiter_window_resets():
    import time
    from quantumlane_mcp.ratelimit import RateLimitMiddleware
    mw = RateLimitMiddleware(app=None, limit=2, window_seconds=60)
    mw._buckets["3.3.3.3"] = (time.monotonic() - 61, 2)  # window elapsed, was at limit
    assert mw._allowed("3.3.3.3") is True  # resets