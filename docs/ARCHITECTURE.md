# QuantumLane — Architecture

A single-box data platform for public transit data in the Greater Toronto Area.

---

## 1. Scope

**What QuantumLane is:**
A continuously running data system that ingests public transit feeds, persists them with schema and quality controls, exposes them through a small read-only API and a public MCP server, and surfaces its own operational health.

**What it isn't:**
- A passenger-facing transit app
- A real-time analytics platform
- A machine learning product

---

## 2. Architectural principles

These rules guide every design decision. New features that violate one of them need a written justification in the relevant module's README or a new ADR.

1. **Boring technology that runs forever beats novel technology that runs for a month.**
   The novelty is in the reasoning, not the stack.

2. **Observability is a first-class feature, not an afterthought.**
   Every pipeline reports health on the public website. Freshness, completeness, and schema drift are visible by default.

3. **Document the trade-off, not the tool.**
   Every non-trivial decision has a short ADR in `docs/adr/`. "We chose X over Y because..." is the artifact, not "we used X."

4. **Schema is contract.**
   Database migrations are versioned and forward-only. No `ALTER TABLE` in production via psql.

5. **Local development equals production in a smaller box.**
   `docker compose up` runs the same images that run in production. No `if env == 'dev'` branches in code.

6. **Model by access pattern, not by volume.**
   The right store, grain, and tier follow from how data is read, not how much of it there is. The hot/cold split exists because live operational queries and historical aggregation are different workloads — not because the raw feeds are large.

7. **Cost discipline is part of the design.**
   Target: under CAD $20/month all-in. Features that push past that without commensurate value do not ship.

8. **Public means public.**
   Anyone can read the API, connect to the MCP server, see the dashboards, fork the repo. No auth wall on read endpoints. Rate limits, not gates.

---

## 3. System overview

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                               PUBLIC INTERNET                               │
└────────┬────────────────────┬─────────────────────────┬─────────────────────┘
         │                    │                         │
 ┌───────▼────────┐  ┌────────▼─────────┐      ┌────────▼─────────┐
 │ quantumlane.io │  │   MCP clients    │      │ TTC feeds:       │
 │   (browser)    │  │ (Claude, ChatGPT)│      │ GTFS-RT ×3,      │
 └───────┬────────┘  └────────┬─────────┘      │ static GTFS zip  │
         │ HTTPS              │ streamable     └────────┬─────────┘
         │                    │ HTTP                    │ RT: every 1 min
┌────────▼────────────────────▼──────────┐              │ static: daily
│       Caddy (reverse proxy + TLS)      │              │
└──┬──────────────┬───────────────┬──────┘              │
   │ static       │ /api/*        │ /mcp*               │
┌──▼─────────┐ ┌──▼──────────┐ ┌──▼─────────┐           │
│ Static     │ │  FastAPI    │ │ MCP server │           │
│ site files │ │ (read-only) │ │ (FastMCP)  │           │
└────────────┘ └──▲───┬──────┘ └──┬─────────┘           │
                  │   │           │ wraps the           │
                  └───┼───────────┘ public API          │
                      │                                 │
                      │            ┌────────────────────▼───┐
                      │            │ Dagster (daemon, code   │
                      │            │ server, webserver*)     │
                      │            │ *UI is not public —     │
                      │            │  reached via SSH tunnel │
                      │            └───────────┬─────────────┘
                      │                        │ ingestion assets
               ┌──────▼────────────────────────▼──────┐
               │      PostgreSQL 16 + PostGIS         │
               │  realtime.* (3-day hot, partitioned) │
               │  static_gtfs.*  ·  ops.*             │
               └──────────────────┬───────────────────┘
                                  │ nightly Parquet export
                         ┌────────▼────────┐
                         │    Amazon S3    │
                         │   cold tier:    │
                         │  zstd Parquet,  │
                         │  dt=YYYY-MM-DD  │
                         └─────────────────┘
```

A single Hetzner CPX21 (x86, Ashburn US-East) runs the entire stack. The original plan was a cheaper ARM box in Nuremberg; the TTC's feeds geo-block European IPs, which forced the move (ADR-004 update pending — deployment region is now a data-source constraint, not just a cost decision). If and when components need to split, they will. Premature distribution is more expensive than its benefits at this scale.

---

## 4. Component design

### 4.1 Ingestion (`/ingestion`)

**Stack:** Dagster, Python 3.12, `gtfs-realtime-bindings`, SQLAlchemy, psycopg (streaming `COPY`), httpx, pyarrow, boto3.

See [ADR-001](adr/001-dagster-over-airflow.md) for the orchestrator choice.

**Assets and schedules:**

| Asset / Job | Source | Cadence | Target |
|---|---|---|---|
| `ttc_vehicle_positions` | TTC GTFS-RT VehiclePositions | Every minute | `realtime.vehicle_positions` |
| `ttc_trip_updates` | TTC GTFS-RT TripUpdates | Every minute | `realtime.trip_updates` |
| `ttc_service_alerts` | TTC GTFS-RT ServiceAlerts | Every 5 minutes | `realtime.service_alerts` |
| `ttc_static_gtfs` | TTC static GTFS zip (CKAN) | Daily 04:00 Toronto | `static_gtfs.*` (full replace) |
| `freshness_check` | Database query | Every minute | `ops.freshness_snapshot` |
| `daily_parquet_export` | Database query | Nightly (partitioned by UTC day) | S3 `feed/dt=YYYY-MM-DD/part-0.parquet` |
| `daily_partition_maintenance` | — | Daily 02:00 Toronto | Create next 7 days, drop >3 days |

Notes on the two assets that earn their complexity:

- **`ttc_static_gtfs`** downloads the zip from Toronto's CKAN open-data portal (the original `opendata.toronto.ca` URL rotted when the city migrated; a zip magic-byte guard now fails loudly on a future move). The load full-replaces `static_gtfs.*` in one transaction, FK-ordered. `stop_times` (~200 MB, 4.2M rows) is streamed row-by-row from the open zip handle into psycopg `COPY` — one row resident at a time — so the load fits a 4 GB box (~1.7 GB peak).
- **`daily_parquet_export`** streams a day of `trip_updates` + `vehicle_positions` through a server-side named cursor in 200k-row chunks into a held-open zstd `ParquetWriter` — one file per feed per day, idempotent, Hive-style keys. Transit data is extremely low-cardinality, so real compression is ~20–25×. The asset is day-partitioned in Dagster, so past days (within the hot window) backfill from the UI.

**Resources** (Dagster `ConfigurableResource`):
- `PostgresResource`: connection pool, exposes engine and helpers
- `S3Resource`: boto3-backed client for the AWS S3 cold tier
- `GTFSRTResource`: configured httpx client with retry, timeout, and User-Agent

**Failure handling:**
- Retries: up to 3 attempts with exponential backoff, only for transient errors (connection timeout, 5xx). 4xx and parse errors fail fast.
- Dead letter table: `ops.ingestion_failures` records failed runs with feed, error class, sample payload (truncated), and timestamp.
- The freshness check is the backstop. If retries mask a real outage, freshness catches it within a minute.
- **Run monitoring** is enabled in `dagster.yaml` (`start_timeout_seconds: 300`, `max_resume_run_attempts: 0` — mandatory with the subprocess launcher, which can detect-and-fail but not resume). Without it, runs whose process dies at an OOM or ungraceful shutdown linger as "in progress" forever and silently pin the `max_concurrent_runs` pool — a failure mode this project hit in production.
- **Backfill throttling:** a `tag_concurrency_limits` rule serializes any run tagged `backfill` to 1, so heavy catch-up reads never run in parallel against live ingestion on the small box. The global limit stays at 10 for the minutely realtime assets.

### 4.2 Database (`/db`)

**Stack:** PostgreSQL 16 with PostGIS 3.4.

**Schemas:**

```
static_gtfs.*  — daily full-replace of TTC static GTFS (routes, stops, trips, stop_times, calendar)
realtime.*     — append-only event tables, partitioned by day
ops.*          — pipeline metadata: freshness, runs, failures, schema versions
olap.*         — OLAP aggregates (planned; populated by the v0.3 lakehouse arc — supersedes the
                 earlier `analytics.*` placeholder naming)
```

**Time zones:**
All timestamps are stored as `TIMESTAMPTZ` in UTC. Partition boundaries
are UTC days, so partition names (`_pYYYYMMDD`) reflect UTC dates.
During evening hours in Toronto, the UTC date may already be the
following calendar day — this is expected. The `daily_partition_maintenance`
job runs at 02:00 America/Toronto (06:00–07:00 UTC depending on DST),
at which point UTC and Toronto dates match and future partitions are
created for the next 7 UTC days.

**Partitioning strategy:**
`realtime.vehicle_positions` and `realtime.trip_updates` are range-partitioned by `received_at::date`. Daily partitions. The `daily_partition_maintenance` job creates the next 7 days of partitions each night and **drops** partitions older than 3 days directly. The original design was two-stage (detach → archive → drop after verification), but the archive step was never completed in v0.1 and the drop never ran — detached partitions accumulated invisibly (`pg_inherits`-based monitoring can't see parentless tables) and filled the disk. Partitions are now dropped directly; the archive-then-drop ordering returns as an upstream Dagster step when the Iceberg cold tier lands (ADR-016, planned).

**Retention:**
- Hot in Postgres: 3 days
- Cold in S3 as zstd Parquet: indefinite — years of history at trivial cost (the export runs nightly and is the current cold tier; Iceberg absorbs or complements it in v0.3)

**Indexes:** created in migrations, not ad hoc. Each index carries a SQL comment explaining the query it supports.

**Migrations:** plain `.sql` files in `db/migrations/`, applied by `ops/scripts/migrate.py`. Numbered `NNNN_description.sql`. Forward-only. See [ADR-006](adr/006-raw-sql-migrations.md).

### 4.3 API (`/api`)

**Stack:** FastAPI, Pydantic v2, SQLAlchemy 2.x (sync), uvicorn.

**Endpoints (current):**

```
GET       /health                        — liveness probe (GET and HEAD; uptime monitors send HEAD)
GET       /ready                         — readiness probe (DB-aware)
GET       /v1/agencies                   — list agencies
GET       /v1/freshness                  — per-feed freshness summary
GET       /v1/vehicle-positions/latest   — most recent position per vehicle
GET       /v1/routes                     — list routes from static GTFS
GET       /v1/stops/nearby               — nearest stops to a coordinate (PostGIS KNN on the
                                           stops GIST index; distance in metres via geography cast)
GET       /v1/routes/{route_id}/vehicles — current vehicles on a route
GET       /v1/stats/daily                — record counts per feed per day, last 3 days
GET       /v1/ops/runs                   — recent Dagster run summary
```

All read-only. All return JSON with a consistent envelope:

```json
{
  "data": [...],
  "meta": {
    "fetched_at": "2026-04-14T12:34:56Z",
    "data_age_seconds": 12,
    "next_cursor": null
  }
}
```

**Rate limiting:** 60 requests per minute per IP via `slowapi`.

**OpenAPI spec** auto-generated and served at `/api/docs`.

### 4.4 Website (`/website`)

**Stack:** Plain HTML, Tailwind CSS via CDN, vanilla JavaScript.

See [ADR-005](adr/005-static-html-no-framework.md) for why there is no build system.

**Pages:**

1. `/` — Landing. One-paragraph description; live freshness widget; MCP demo section (video + link to the connect guide); links to freshness, explore, architecture, GitHub.
2. `/freshness` — Real-time freshness page. Polls `/v1/freshness` every 10 seconds. Shows per-feed last-update timestamps, ingestion lag, success rate over the last 24 hours, and schema-drift flags.
3. `/explore` — Pre-canned queries with the SQL shown alongside the result. Live data.
4. `/architecture` — A condensed view of this document.
5. `/connect` — How to add the MCP server as a connector in Claude or ChatGPT, with the demo video, the tool table, and an honest single-small-server disclaimer.

No analytics tracking, no cookies, no JavaScript frameworks.

### 4.5 Ops (`/ops`)

**Stack:** Docker Compose, Caddy, a few shell scripts.

The compose stack runs everything: main PostgreSQL, Dagster metadata PostgreSQL, Dagster webserver/daemon/code server, the API, the MCP server, and Caddy as the reverse proxy. The MCP service publishes no host port — it is reachable only over the compose network via Caddy's `/mcp*` route (same discipline as the database: nothing internal is published on `0.0.0.0`).

**Deployment:** git-based — commit and push, then `git pull && make build && make up` on the box, so production is always an auditable checkout of what's in git. (`make deploy` still exists as an rsync path but conflicts with the git workflow — it leaves the box's git history stale behind its files — and is slated to be retired or converted to git-based; tracked in the backlog.) Bind-mounted config (Caddyfile, `dagster.yaml`) needs a `--force-recreate` of the affected container, not a plain `up`; bind-mounted static files (the website) are live on pull with no rebuild.

**Backups:** `ops/scripts/backup.sh` does a manual `pg_dump | gzip` via `make backup`. **Caveat: its upload step still targets the retired Cloudflare R2 configuration** (`QL_R2_*` env vars that no longer exist since the S3 migration), so uploads silently no-op — migrating the script to the S3 cold tier and scheduling it is an open item. Until then, backups are manual and effectively local.

**Secrets:** `.env` file, never committed. `.env.example` documents required variables. Production secrets live in `secrets/prod.env` on the dev machine and are copied to the box as `.env`.

### 4.6 MCP server (`/mcp`)

**Stack:** FastMCP (Python), httpx, uvicorn. Remote streamable-HTTP transport, hosted behind Caddy at `https://quantumlane.io/mcp`.

A Model Context Protocol server that lets LLM clients — Claude and ChatGPT — answer live transit questions in plain English. Three hand-written tools:

| Tool | Answers |
|---|---|
| `list_routes` | "What TTC routes exist?" — the route catalog |
| `vehicles_on_route` | "Where is the 504 right now?" — live positions, with human-name → `route_id` resolution |
| `nearest_stops` | "What stops are near the CN Tower?" — PostGIS KNN via `/v1/stops/nearby` |

**Design decisions (ADR-018 candidate):**

- **It wraps the public HTTP API, not the database.** The MCP layer holds no DB credentials and adds no second data path — it calls the same `https://quantumlane.io/api` anyone can. One source of truth; the API remains the platform's single read surface.
- **Tools are hand-written, not auto-generated from routes.** The tool descriptions and argument schemas are the product: they are what make the model pick the right tool and pass a route *ID* where the API needs one. Getting that wording right took more iteration than building the tools.
- **Remote streamable-HTTP, not stdio.** Required for ChatGPT (which only supports remote HTTPS MCP servers) and works for Claude on any plan. The transport choice is dictated by the clients, not by preference.
- **Right-sized guardrails, deliberately short of auth.** A per-IP fixed-window rate limit (60 req/min, Starlette middleware) and a 1-hour route-catalog cache (the catalog changes once a day with the static GTFS reload; caching it collapses a full-catalog read per tool call into a handful per day — the TTL matches the data's real change rate). No API keys, no quotas, no WAF: nothing in the MCP's path is metered, so the risk is box resource exhaustion, not cost — and the rate limit addresses exactly that. The server publishes no host port and is reachable only through Caddy.
- **Name resolution is honest about ambiguity.** `resolve_route` matches rider language ("504", "the King car") to route IDs by word-boundary matching with a substring fallback. Route numbers resolve most reliably; the real TTC catalog contains genuinely ambiguous names (304 King and 504 King are both `route_type 0`, long name `King`), and the model's own context — e.g. time of day — is often the better disambiguator than any hardcoded tiebreak.

Analytical tools ("how reliable is the 504 usually", headway comparisons) are the planned differentiator over generic feed-wrapper MCP servers; they arrive with the OLAP layer.

---

## 5. Roadmap

The backlog (`BACKLOG.md`) is the operational source of truth; this is the shape of it. The original versioned roadmap (v0.2 multi-agency → v0.3 VFH analytics → v0.4 datasets) has been superseded by the lakehouse arc, so milestones below are grouped by status rather than forced into the old numbering.

### Shipped (v0.1 → v0.4)

TTC GTFS-RT and static GTFS flowing continuously on a public box; `quantumlane.io` live behind TLS; 3-day partitioned hot tier with self-maintaining retention; nightly zstd Parquet export to the S3 cold tier (partitioned by UTC day, backfillable); the public read-only API including nearest-stops; per-feed freshness telemetry and schema-drift detection on the site; Dagster run monitoring; the public MCP server with its connect guide and demo.

### In progress — the PySpark + Iceberg lakehouse arc (backlog "v0.3")

- **PySpark fluency** against the operational data — first transform shipped (`spark/headway_reliability.py`, an RT-only headway-regularity reliability metric, the prototype of the daily OLAP aggregation).
- **One focused week on AWS EMR** (spot) for distributed-scale concepts.
- **Iceberg cold-tier write path** — restores the archive-then-drop ordering the retention job was originally designed for; the plain-Parquet export is then either absorbed or kept as a public-dataset path.
- **First OLAP aggregations** into `olap.*` — route reliability, headway distributions — reading from the cold tier, written back to Postgres, queryable by the API.
- **Delay / reliability capture** (the next major build): live schedule-adherence delay via a stop-level overwrite — one upserted row per `(trip_id, stop_sequence)`, finalized on arrival — collapsing millions of superseded predictions per day into one durable row per stop-event that serves both the live gauge and the historical aggregate. Unblocked now that the static schedule loads daily.

### Next

- **Multi-agency ingestion** — GO Transit (Metrolinx), MiWay, Brampton. The central design work is schema reconciliation across agencies' differing GTFS-RT dialects, plus a data-quality dimension framework.
- **Public dataset publication** — versioned Parquet exports with stable URLs and a catalog page.
- **Analytical MCP tools and prompts** over the OLAP layer — the questions a live-feed wrapper structurally can't answer.

### Possible directions

- City of Toronto Vehicle-for-Hire joins; Bike Share Toronto; capital-project disruption overlays; short-horizon arrival prediction (a legitimate ML use case — deferred until enough history has accumulated in the cold tier).

### Deliberately not here

- Streaming frameworks (Kafka, Redpanda) — overkill at this message rate (ADR-009)
- Authentication on reads — public data, public API (ADR-010, planned)
- Interactive map UIs as a product — the platform is the point, not the pixels

---

## 6. Decision log

ADRs live in `docs/adr/`. Status marked honestly — several decisions are referenced here before their ADR is written; the decision is real, the write-up is debt.

| # | Decision | Alternative considered | Status |
|---|---|---|---|
| 001 | Dagster, not Airflow | Airflow, Prefect, cron | Written |
| 002 | Single PostgreSQL for all app data | DuckDB, ClickHouse, separate analytics DB | Written |
| 003 | Single VPS, not Kubernetes | k3s, managed services | Written |
| 004 | Hetzner hosting, not AWS | AWS, GCP, managed PaaS | Written — update pending: data-source geo-restrictions constrain viable regions |
| 005 | Plain HTML, not Next.js or Astro | Next.js, Astro, SvelteKit | Written |
| 006 | Raw SQL migrations, not Alembic | Alembic, sqitch | Written |
| 007 | FastAPI, not Flask or Django REST | Flask, Django REST Framework | Decision made; ADR not yet written |
| 008 | ~~Cloudflare R2, not S3~~ | — | Superseded before written — v0.3 moved the cold tier to AWS S3 (see 013) |
| 009 | No streaming framework | Kafka, Redpanda, Kinesis | Written |
| 010 | Public API with no auth | API keys, OAuth | Decision made; ADR not yet written |
| 011 | v0.1 operational lessons | — | Planned |
| 012 | Hot/cold tier split with Iceberg cold tier | Single-tier Postgres | Planned |
| 013 | AWS S3 over Hetzner Object Storage | Hetzner Object Storage, R2 | Planned |
| 014 | JDBC catalog for Iceberg | Glue, Hadoop catalog | Planned |
| 015 | 3-day hot retention | 14-day original target | Planned |
| 016 | Partition retention: drop directly until Iceberg archival lands | Detach-then-archive-then-drop (the v0.1 design that leaked) | Planned |
| 017 | Event-time delay computation + stop-level overwrite | Recompute from raw RT + schedule snapshots | Drafted |
| 018 | MCP server wraps the public API; hand-written tools; remote HTTP; rate-limit-not-auth | DB-direct import seam; auto-generated tools; stdio | Candidate |