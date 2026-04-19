# ADR 006: Raw SQL migrations, not Alembic

**Status:** Accepted
**Date:** 2026-04

## Context

Any project with a managed database needs a migration system. Python projects typically reach for Alembic (SQLAlchemy's migration tool) by default. Ruby uses ActiveRecord migrations, Node uses Knex or Prisma, and so on.

QuantumLane has a schema that will evolve over many versions. Migrations must be reviewable, ordered, and reproducible across local development, staging, and production.

## Decision

Use **plain numbered `.sql` files in `db/migrations/`**, applied in order by a small Python runner (`ops/scripts/migrate.py`). Forward-only — no down migrations. Track applied versions in `ops.schema_versions`.

## Alternatives considered

### Alembic
The conventional Python choice.

*Rejected because:*
- Alembic's autogeneration encourages drift: the ORM model becomes the source of truth, the SQL becomes a generated artifact. For a data-engineering project where the schema is the contract, this inversion is wrong.
- Debugging a bad autogeneration is harder than reading a 20-line SQL file.
- QuantumLane uses SQLAlchemy only in the API for ORM-style reads. The ingestion layer uses raw SQL by design. An ORM-centric migration tool is poorly matched.

### sqitch
Dependency-aware SQL migrations. No ORM assumption.

*Rejected because:*
- Dependency graphs are overkill for forward-only linear history.
- Adds a Perl-or-equivalent tool dependency that isn't otherwise in the stack.

### Atlas, Bytebase, other HCL/declarative tools
Modern declarative schema management.

*Rejected because:*
- Declarative schema-diffing shifts the complexity into the diff engine and its assumptions. Explicit imperative migrations are more readable and more debuggable.
- New tooling — smaller ecosystems, more upgrade risk.

### Down migrations
The symmetric "undo" pattern.

*Rejected because:*
- In practice, production databases are rarely rolled back. The pattern is to roll forward with a new migration that fixes the problem.
- Down migrations are rarely tested and often broken when actually needed.
- Committing to forward-only forces careful thinking about compatibility.

## Implementation

```
db/migrations/0001_init.sql
db/migrations/0002_static_gtfs.sql
db/migrations/0003_realtime.sql
db/migrations/0004_ops.sql
```

Each migration:
- Wraps its content in `BEGIN ... COMMIT` so partial application isn't possible
- Ends with `INSERT INTO ops.schema_versions (version, description) VALUES (N, '...');`
- Is immutable after being applied to any environment — edits require a new migration

The runner (`ops/scripts/migrate.py`) reads applied versions from `ops.schema_versions`, discovers files matching `NNNN_*.sql`, and applies any with version numbers not yet recorded. Rejects gaps in numbering. Computes a SHA-256 checksum of each file at apply time to enable future tamper detection.

## Consequences

**Accepted costs:**
- No "generate a migration from my model" convenience. Schema changes require hand-writing SQL. At this project's rate of schema change, this is a feature, not a cost.
- Anyone working on the project must be comfortable with SQL DDL. This is the expected skill level for data engineering work.

**Benefits:**
- Migrations are reviewable by anyone who can read SQL. No tool-specific mental model required.
- The database state is exactly, byte-for-byte, what the migration files say it is.
- Twenty lines of Python replace a dependency on a hundred-thousand-line framework.
- Schema history is self-documenting — the file names and commit log tell the story.

**Watch for:**
- If the team grows and one engineer writes a destructive migration that slips through review, the forward-only model means recovery depends on backups. Invest in the backup and restore path, not in reversibility at the migration layer.
