"""
Data-access layer for the MCP server — the db.py analogue.

This module is the ONLY place that talks to the outside world. It wraps the
QuantumLane public API over HTTPS. server.py (the tool layer) calls into here and
never makes an HTTP call itself, exactly as the API's main.py calls db.py and never
touches psycopg directly. Keeping this seam means:
    - the tool layer is testable by mocking this module (no network in unit tests)
    - there is one place that knows the API's URL shape and envelope format
    - the MCP needs no DB credentials — it only needs outbound HTTPS

Config: the single value QL_API_BASE (the public API root). One env var doesn't
justify a settings module, so it's read here directly. If config ever grows
(auth, multiple upstreams), promote it to a settings.py then.
"""
from __future__ import annotations

import os

import httpx

# The public API root the tools wrap. Defaults to production; override via env for
# local testing (e.g. http://localhost:8000/api against a locally-run API).
API_BASE = os.environ.get("QL_API_BASE", "https://quantumlane.io/api")

# One shared client, reused across calls (connection pooling). Timeout is generous
# but bounded — a hung upstream must not hang the model's tool call indefinitely.
_http = httpx.Client(
    base_url=API_BASE,
    timeout=10.0,
    headers={"User-Agent": "quantumlane-mcp/0.1"},
)


def get(path: str, params: dict | None = None) -> dict:
    """GET a public API endpoint; return the parsed JSON envelope ({data, meta}).

    Raises httpx.HTTPStatusError on a non-2xx response so failures surface to the
    model as a tool error rather than silently returning empty data.
    """
    resp = _http.get(path, params=params or {})
    resp.raise_for_status()
    return resp.json()


def list_routes() -> list[dict]:
    """Return the full route catalog (the `data` list from /v1/routes)."""
    return get("/v1/routes").get("data", [])


def vehicles_for_route_id(route_id: str) -> list[dict]:
    """Return live vehicle positions for an internal route_id."""
    return get(f"/v1/routes/{route_id}/vehicles").get("data", [])


def stops_near(lat: float, lon: float, limit: int) -> list[dict]:
    """Return the nearest stops to a coordinate (the `data` list from /v1/stops/nearby)."""
    return get("/v1/stops/nearby", params={"lat": lat, "lon": lon, "limit": limit}).get("data", [])


def resolve_route(route: str) -> dict | None:
    """Resolve a human route string ('504', 'King', 'Dufferin') to a route record.

    The human-name -> route_id bridge: the crux of the tool design. The model passes
    what the user said; we match it against the route catalog.

    Match strategy, most-specific first:
        1. exact route_short_name  ('504' -> the 504)
        2. case-insensitive substring of route_long_name ('king' -> '504 King')
    Returns the first matching route record, or None if nothing matches.

    Lives here (not in server.py) because it's pure data-access logic with real
    failure modes — and that makes it the one piece genuinely worth unit-testing.
    """
    routes = list_routes()
    q = route.strip().lower()

    # 1. exact short_name (the route number people usually mean)
    for r in routes:
        if (r.get("route_short_name") or "").lower() == q:
            return r
    # 2. substring of long name ('king' in '504 King')
    for r in routes:
        if q in (r.get("route_long_name") or "").lower():
            return r
    return None