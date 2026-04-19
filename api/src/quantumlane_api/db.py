"""
Database connection pool for the API.

We use a sync psycopg pool rather than async because:
    - Our queries are short and the pool size is small; async overhead isn't justified.
    - Sync code is easier to read and debug.
    - FastAPI handles concurrency at the request layer; queries run in the threadpool.
    - We can swap to async later if profiling shows the threadpool is the bottleneck.
"""
from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager

import psycopg
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool

from quantumlane_api.settings import get_settings

_pool: ConnectionPool | None = None


def init_pool() -> None:
    """Open the pool. Called at app startup."""
    global _pool
    if _pool is not None:
        return
    settings = get_settings()
    _pool = ConnectionPool(
        conninfo=settings.postgres_dsn,
        min_size=1,
        max_size=settings.postgres_pool_size,
        open=False,
        kwargs={"row_factory": dict_row, "autocommit": True},
    )
    _pool.open(wait=True, timeout=30.0)


def close_pool() -> None:
    """Close the pool. Called at app shutdown."""
    global _pool
    if _pool is not None:
        _pool.close()
        _pool = None


@contextmanager
def connection() -> Iterator[psycopg.Connection]:
    """Yield a pooled connection. Always use as a context manager."""
    if _pool is None:
        raise RuntimeError("DB pool not initialized.")
    with _pool.connection() as conn:
        yield conn


def fetch_all(sql: str, params: tuple = ()) -> list[dict]:
    with connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            return cur.fetchall()


def fetch_one(sql: str, params: tuple = ()) -> dict | None:
    with connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            return cur.fetchone()


def ping() -> bool:
    """Returns True if a trivial query against the pool succeeds."""
    try:
        with connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
                cur.fetchone()
        return True
    except Exception:
        return False
