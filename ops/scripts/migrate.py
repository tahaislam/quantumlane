#!/usr/bin/env python3
"""
QuantumLane migration runner.

Usage:
    python ops/scripts/migrate.py            # apply all pending
    python ops/scripts/migrate.py --status   # show what's applied/pending
    python ops/scripts/migrate.py --target N # apply up to and including version N

Migrations live in db/migrations/NNNN_description.sql, applied in order.
Each migration must end by inserting a row into ops.schema_versions.
Forward-only — there is no down migration. Roll forward by writing a new migration.

Why not Alembic / Flyway / sqitch:
    At this scale the value-add of those tools is autogeneration and rollback,
    neither of which we want. Forward-only raw SQL is more honest about what runs
    in the database, and the runner is 80 lines we can reason about end-to-end.
"""
from __future__ import annotations

import argparse
import hashlib
import os
import re
import sys
from pathlib import Path

import psycopg

MIGRATIONS_DIR = Path(__file__).resolve().parent.parent.parent / "db" / "migrations"
MIGRATION_PATTERN = re.compile(r"^(\d{4})_[a-z0-9_]+\.sql$")


def discover_migrations() -> list[tuple[int, Path]]:
    """Return [(version, path), ...] sorted by version."""
    found: list[tuple[int, Path]] = []
    for path in sorted(MIGRATIONS_DIR.iterdir()):
        if not path.is_file():
            continue
        match = MIGRATION_PATTERN.match(path.name)
        if not match:
            print(f"WARN: ignoring non-conforming file: {path.name}", file=sys.stderr)
            continue
        found.append((int(match.group(1)), path))
    if not found:
        raise SystemExit(f"No migrations found in {MIGRATIONS_DIR}")
    # Sanity check: no gaps, starts at 1
    versions = [v for v, _ in found]
    expected = list(range(1, len(versions) + 1))
    if versions != expected:
        raise SystemExit(
            f"Migration versions are not contiguous starting from 0001. Got {versions}"
        )
    return found


def get_applied_versions(conn: psycopg.Connection) -> set[int]:
    """Return set of applied version numbers, or empty set if schema_versions doesn't exist yet."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT EXISTS (
                SELECT 1 FROM information_schema.tables
                WHERE table_schema = 'ops' AND table_name = 'schema_versions'
            )
            """
        )
        exists = cur.fetchone()[0]
        if not exists:
            return set()
        cur.execute("SELECT version FROM ops.schema_versions")
        return {row[0] for row in cur.fetchall()}


def checksum(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def apply_migration(conn: psycopg.Connection, version: int, path: Path) -> None:
    sql = path.read_text()
    print(f"  applying {path.name} ...", end=" ", flush=True)
    with conn.cursor() as cur:
        cur.execute(sql)
        # Update checksum after the migration's own INSERT (the migration's INSERT
        # may run with checksum=NULL since the file can't reference its own hash).
        cur.execute(
            "UPDATE ops.schema_versions SET checksum = %s WHERE version = %s AND checksum IS NULL",
            (checksum(path), version),
        )
    conn.commit()
    print("ok")


def cmd_status(conn: psycopg.Connection) -> int:
    applied = get_applied_versions(conn)
    discovered = discover_migrations()
    print(f"{'VERSION':<10}{'STATUS':<12}FILE")
    for version, path in discovered:
        status = "applied" if version in applied else "PENDING"
        print(f"{version:<10}{status:<12}{path.name}")
    return 0


def cmd_apply(conn: psycopg.Connection, target: int | None) -> int:
    applied = get_applied_versions(conn)
    discovered = discover_migrations()
    pending = [(v, p) for v, p in discovered if v not in applied]
    if target is not None:
        pending = [(v, p) for v, p in pending if v <= target]
    if not pending:
        print("Nothing to apply. Database is up to date.")
        return 0
    print(f"Applying {len(pending)} migration(s):")
    for version, path in pending:
        try:
            apply_migration(conn, version, path)
        except psycopg.Error as exc:
            print(f"\nFAILED on {path.name}: {exc}", file=sys.stderr)
            conn.rollback()
            return 1
    print("Done.")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--status", action="store_true", help="Show migration status and exit.")
    parser.add_argument("--target", type=int, default=None, help="Apply up to and including this version.")
    parser.add_argument(
        "--dsn",
        default=os.environ.get("QL_POSTGRES_DSN"),
        help="PostgreSQL DSN. Defaults to $QL_POSTGRES_DSN.",
    )
    args = parser.parse_args()

    if not args.dsn:
        print("ERROR: provide --dsn or set $QL_POSTGRES_DSN", file=sys.stderr)
        return 2

    with psycopg.connect(args.dsn, autocommit=False) as conn:
        if args.status:
            return cmd_status(conn)
        return cmd_apply(conn, args.target)


if __name__ == "__main__":
    sys.exit(main())
