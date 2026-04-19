"""Shared API test fixtures."""
from __future__ import annotations

from collections.abc import Iterator
from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient


@pytest.fixture
def api_client() -> Iterator[TestClient]:
    """
    Test client that doesn't actually open a DB pool.

    We patch the db module's pool functions before importing the app so that
    the lifespan startup hook is a no-op. Endpoints that hit the DB will fail
    loudly if called without further mocking — that's intentional for smoke tests.
    """
    # Import db module eagerly so patch() can find its attributes.
    from quantumlane_api import db  # noqa: F401

    # Patch the db module's functions so lifespan doesn't try to connect.
    with patch("quantumlane_api.db.init_pool"), patch("quantumlane_api.db.close_pool"):
        from quantumlane_api.main import app
        with TestClient(app) as client:
            yield client
