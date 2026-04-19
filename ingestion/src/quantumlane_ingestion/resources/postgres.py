"""
Postgres resource. Wraps a psycopg connection pool that persists for the lifetime
of the Dagster code server process.

Why a pool, not a connection per asset:
    Asset materializations run frequently (every 30s for the realtime feeds).
    Establishing a new connection each time burns latency and pgbouncer-style
    connection counts. A small bounded pool keeps things predictable.
"""
from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager

import psycopg
from dagster import ConfigurableResource, InitResourceContext
from psycopg_pool import ConnectionPool
from pydantic import PrivateAttr


class PostgresResource(ConfigurableResource):
    """Bounded connection pool for the QuantumLane database."""

    dsn: str
    pool_min_size: int = 1
    pool_max_size: int = 5

    _pool: ConnectionPool | None = PrivateAttr(default=None)

    def setup_for_execution(self, context: InitResourceContext) -> None:
        # Open=False so we can wait_connection lazily; saves boot time when DB is slow to come up.
        self._pool = ConnectionPool(
            conninfo=self.dsn,
            min_size=self.pool_min_size,
            max_size=self.pool_max_size,
            open=False,
            kwargs={"autocommit": False},
        )
        self._pool.open(wait=True, timeout=30.0)

    def teardown_after_execution(self, context: InitResourceContext) -> None:
        if self._pool is not None:
            self._pool.close()
            self._pool = None

    @contextmanager
    def connection(self) -> Iterator[psycopg.Connection]:
        """Yield a connection from the pool. Caller is responsible for commit/rollback."""
        if self._pool is None:
            raise RuntimeError("Postgres pool not initialized. Did setup_for_execution run?")
        with self._pool.connection() as conn:
            yield conn

    def execute(self, sql: str, params: tuple = ()) -> None:
        """Convenience for fire-and-forget statements (DDL, single-row writes)."""
        with self.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)
            conn.commit()

    def fetch_one(self, sql: str, params: tuple = ()) -> tuple | None:
        with self.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                return cur.fetchone()

    def fetch_all(self, sql: str, params: tuple = ()) -> list[tuple]:
        with self.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                return cur.fetchall()
