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
import re

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


def _route_words(long_name: str | None) -> set[str]:
    """Lowercased word tokens of a route long name, punctuation stripped.

    '504 King' -> {'504', 'king'};  '12 Kingston Rd' -> {'12', 'kingston', 'rd'}.
    Used so 'king' matches the WORD 'King', not the prefix of 'Kingston'.
    """
    if not long_name:
        return set()
    # Split on any non-alphanumeric run, drop empties.
    return {w for w in re.split(r"[^a-z0-9]+", long_name.lower()) if w}


def resolve_route(route: str) -> dict | None:
    """Resolve a human route string ('504', 'King', 'Dufferin') to a route record.

    The human-name -> route_id bridge: the crux of the tool design. The model passes
    what the user said; we match it against the route catalog. Designed around a real
    failure: a naive substring match maps 'King' -> '12 Kingston Rd' (a bus) because
    'king' is a substring of 'kingston'. A rider saying 'King' means the 504 streetcar.

    Match strategy, most-specific first:
        1. exact route_short_name           ('504' -> the 504)
        2. WHOLE-WORD match in long_name, preferring lower route_type on ties.
           Word-boundary (not substring) so 'king' matches the word 'King' in
           '504 King' but NOT 'Kingston'. route_type tiebreak encodes the domain
           fact that named routes ('King', 'Queen', 'Spadina') are the streetcars:
           GTFS route_type 0=streetcar, 1=subway, 3=bus — lower wins, so a named
           match resolves to the streetcar over a same-named bus.
        3. substring fallback (last resort) for partial inputs that aren't whole
           words (e.g. a typo or fragment); also route_type-preferred.

    Returns the single best matching route record, or None if nothing matches.

    NOTE (alternative design): instead of picking one on ambiguity, we could return
    ALL name matches and let the model disambiguate ('did you mean 504 King streetcar
    or 12 Kingston Rd bus?'). That's arguably more correct for an LLM client but costs
    a round-trip on every ambiguous call. We pick-best here so the common case
    ('King' -> 504) just works; revisit if ambiguous routes need surfacing.

    Lives here (not in server.py) because it's pure data-access logic with real
    failure modes — the one piece genuinely worth unit-testing.
    """
    routes = list_routes()
    q = route.strip().lower()

    # 1. exact short_name (the route number people usually mean) — unambiguous, wins.
    for r in routes:
        if (r.get("route_short_name") or "").lower() == q:
            return r

    def _route_type(r: dict) -> int:
        # Missing/None route_type sorts last (treat as a high number).
        rt = r.get("route_type")
        return rt if isinstance(rt, int) else 99

    # 2. whole-word match in long_name, preferring lower route_type (streetcar/subway
    #    over bus) on ties. This is the fix for 'King' -> 'Kingston'.
    word_matches = [r for r in routes if q in _route_words(r.get("route_long_name"))]
    if word_matches:
        return min(word_matches, key=_route_type)

    # 3. substring fallback (last resort) for non-whole-word fragments, still
    #    route_type-preferred. Kept so partial inputs degrade gracefully rather than
    #    returning nothing — but ranked below whole-word so it can't hijack 'King'.
    substr_matches = [
        r for r in routes if q in (r.get("route_long_name") or "").lower()
    ]
    if substr_matches:
        return min(substr_matches, key=_route_type)

    return None