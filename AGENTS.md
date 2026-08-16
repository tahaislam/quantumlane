# AGENTS.md — QuantumLane

Operational guidance for AI coding agents working in this repository. This file
documents what you **cannot infer** from the code: invariants, workflow traps,
and boundaries. Read it before planning any change.

---

## What QuantumLane is (one paragraph, not a tour)

A small, opinionated data platform ingesting public GTA transit data
(TTC GTFS-Realtime feeds) and serving it. Stack: Dagster (orchestration) +
PostgreSQL 16 / PostGIS + FastAPI + static HTML/Tailwind + Caddy (reverse proxy),
all in Docker Compose. Read `README.md` and `docs/ARCHITECTURE.md` for structure;
they're accurate. The rest of this file is the stuff that isn't written in the code.

---

## The Makefile is the interface — use it, don't bypass it

All common operations go through `make`. Do not invent raw `docker compose`
invocations when a target exists; the targets encode setup steps you will miss.

- `make up` — start the stack. **After starting, it waits for Postgres health
  and then calls `ops.ensure_today_partition()`.** Bypassing `make up` with a
  raw `docker compose up` skips that partition bootstrap and ingestion will fail
  on a missing partition.
- `make down` — stop the stack (preserves volumes / data).
- `make build` — build images.
- `make psql` — open psql against the main database.
- `make migrate` / `make migrate-status` — apply / inspect DB migrations.
- `make test` / `make lint` / `make fmt` — tests, ruff+mypy, autoformat.
- `make logs` / `make logs-ingestion` — tail logs.

The compose invocation is always:
`docker compose -f ops/compose/docker-compose.yml --env-file .env`
If you must call compose directly, use that exact form.

## CRITICAL WORKFLOW TRAP: rebuild after editing source

`make up` does **not** rebuild images. It starts whatever images already exist.
After editing any source under `ingestion/`, `api/`, or `ops/`, a plain
`make up` will run the **old code** and your change will silently not take effect.

**Always rebuild after a source edit:**
```
make build && make up
```
or `docker compose -f ops/compose/docker-compose.yml --env-file .env up -d --build`

This has burned us. Symptom of forgetting: change code, run the asset, and
the behavior is identical to before — because the container is running the
pre-edit image. If a fix "isn't working," verify the image was rebuilt before
debugging the logic.

## Which reload command — depends on WHERE the change lives

The "my change didn't take effect" trap has three different correct fixes
depending on what you edited. This matters because the wrong fix silently
leaves the old version running:

- **Edited a bind-mounted config file** (the Caddyfile, anything under
  `/srv/website`) → the file is live to the container, but a long-running
  process already read the old version at startup. Bounce the process:
  `docker compose ... restart caddy`. (Closest analog to `systemctl restart`.)
- **Edited source baked into an image** (`ingestion/`, `api/`, `ops/` Python) →
  the code lives in the image, not on a mount. Rebuild and recreate:
  `make build && make up`. A plain `restart` reuses the OLD image — not enough.
- **Edited the compose definition** (`ports:`, `environment:`, `volumes:` in
  `docker-compose.yml`) → `docker compose ... up -d` applies it (recreates the
  changed services). `restart` does NOT pick up definition changes.
- **Unsure, or want a clean slate for one service** →
  `docker compose ... up -d --force-recreate <service>`.
- Files under `/srv/website` are bind-mounted and served per-request; edits are live on `git pull`, no restart needed.

Note: `docker compose up -d` only recreates containers whose **image or
definition** changed. It does NOT watch the *contents* of bind-mounted files —
so editing the Caddyfile and running `up -d` will leave the old Caddy container
running untouched. Use `restart caddy` (re-reads the mount) or `--force-recreate
caddy`. This is the same stale-process trap as the source-rebuild one, in a
different costume.

## Disk and Docker hygiene (production is small — 80 GB)

- **Never run `docker system prune -a` (or `-af`) on this project.** The `-a`
  flag removes all unused images, including the locally-built `quantumlane-*`
  images (dagster, ingestion, api) — they are NOT on any registry, so the next
  `make up` fails with "pull access denied." Use `docker system prune -f`
  (no `-a`) to reclaim space safely, or `docker builder prune -f` for build cache.
- If images do get wiped, recover with `make build` (or `up -d --build`).

---

## Data model invariants — do not violate these

- **Hot retention is 3 days.** `daily_partition_maintenance` (in
  `ingestion/src/quantumlane_ingestion/assets/ops.py`) drops `realtime.*`
  partitions older than 3 days. The production box (80 GB) cannot hold more at
  observed ingest rates (~10 GB/day). Do not raise this without a plan for where
  the extra data goes. Long-term history belongs in the Iceberg cold tier
  (planned), not in a larger hot window.
- **Partitions are dropped, not detached.** Retention must `DROP TABLE`
  expired partitions (detaching first if still attached). A previous bug
  *detached without dropping*, leaking orphaned tables that are invisible to
  `pg_inherits`-based partition listings but still consume disk. If you touch
  retention, the partition must actually be dropped, and the logic must also
  sweep pre-existing orphans by name pattern (`{table}_pYYYYMMDD`) via `pg_class`.
- **Partition naming:** `{parent}_pYYYYMMDD`, e.g. `trip_updates_p20260531`.
  The maintenance job also pre-creates ~7 days of empty forward partitions so a
  missed run doesn't break next-day ingestion. Empty forward partitions are
  expected, not a bug.
- **All timestamps are `TIMESTAMPTZ`, stored in UTC.** Do not introduce naive
  datetimes or local-time storage anywhere.
- **Partition boundaries are by UTC day.** `ops.ensure_today_partition()` is
  idempotent and safe to call repeatedly; `make up` calls it on every start.

## Cold tier / Iceberg (planned — V0.3.x)

When implementing the Postgres → Iceberg archival, the **archive must succeed
and be verified before the partition is dropped.** Wire it upstream of the
retention drop in the Dagster graph (archive → verify → drop). Dropping before a
confirmed archive reintroduces data loss. The drop logic itself is already correct.

---

## Boundaries — do not touch

- **`secrets/prod.env` and `.env`** — contain real credentials (DB passwords,
  AWS keys). Never read them into output, never commit them, never echo their
  contents. They are gitignored; keep it that way. `.env.example` is the
  committable template.
- **Never commit secrets** of any kind — keys, tokens, passwords, connection
  strings with embedded credentials.
- **Migrations are append-only.** Add a new numbered migration; never edit or
  reorder an existing applied migration.
- **Don't add Spark/PySpark to anything yet** unless the task explicitly is the
  v0.3 lakehouse work. It's a deliberate, staged addition, not a casual import.

---

## Conventions

- **Python:** ruff for lint + format, mypy for types. Run `make lint` before
  considering work done. Type hints are expected on new code.
- **Dagster orchestrates everything.** Scheduled work is a Dagster asset/op, not
  a cron entry, not a standalone script invoked elsewhere. Spark jobs (when they
  exist) will be orchestrated by Dagster too — Dagster is not being replaced.
- **The API and any future MCP layer call the SAME data-access functions.**
  Don't reimplement queries in a second place; import the shared repo functions
  so the HTTP and MCP surfaces can't drift apart.
- **CI:** GitHub Actions runs lint + tests. `mypy` is currently advisory
  (`continue-on-error: true`); don't assume a green mypy gate.
- **Dagster UI** runs on port 3000 (dedicated port, not a Caddy subpath — subpath
  proxying collides with Dagster's absolute-path HTML).
- **Postgres reachability differs by where code runs.** Containers reach it at
  `postgres:5432` over the compose network. Host-side tooling (Spark scripts,
  local psql) reaches it at `127.0.0.1:5432` — published loopback-only in
  compose. Never publish it on `0.0.0.0`; dev and prod share this compose file,
  and a bare `5432:5432` would expose the production database to the internet.

## Verification — what "done" looks like

Before declaring a change complete:
1. `make build && make up` (rebuilt, not stale).
2. `make lint` passes (ruff clean; mypy advisory).
3. `make test` passes for the package you touched.
4. For ingestion/retention changes: confirm via `make psql` that the
   `realtime.*` partition list is what you expect (3 days + today + forward
   partitions, nothing older), and that no orphaned partition tables remain.
5. For production-affecting changes: state explicitly that they need a
   `git pull` + `make build && make up` on the Hetzner box, and that the box has
   live data (operations preserve the volume; never `make nuke` on production).

---

## Deployment shape (context, not a runbook)

Dev runs on the developer's machine; production runs on a single Hetzner CPX21
(x86, Ashburn US-East) — chosen because the TTC GTFS-RT feeds geo-block European
IPs. Production deploys flow through git: edit on dev → push → `git pull` on the
box → `make build && make up`. There is no separate registry; images are built
on each host.