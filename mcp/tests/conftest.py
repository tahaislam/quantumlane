"""Shared fixtures for the MCP tests.

The tools bottom out in api_client, which makes HTTP calls. Unit tests must NOT
hit the network, so we monkeypatch api_client's functions with fakes. This fixture
provides a small, realistic route catalog to resolve against.
"""
from __future__ import annotations

import pytest

# A small slice of a realistic TTC route catalog: a streetcar, a bus, a subway.
FAKE_ROUTES = [
    {"route_id": "r-504", "route_short_name": "504", "route_long_name": "504 King", "route_type": 0},
    {"route_id": "r-501", "route_short_name": "501", "route_long_name": "501 Queen", "route_type": 0},
    {"route_id": "r-29", "route_short_name": "29", "route_long_name": "29 Dufferin", "route_type": 3},
    {"route_id": "r-line1", "route_short_name": "1", "route_long_name": "Line 1 Yonge-University", "route_type": 1},
]


@pytest.fixture
def fake_routes() -> list[dict]:
    return list(FAKE_ROUTES)