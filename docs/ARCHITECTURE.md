# QuantumLane — Architecture

A single-box data platform for public transit data in the Greater Toronto Area.

---

## 1. Scope

**What QuantumLane is:**
A continuously running data system that ingests public transit feeds, persists them with schema and quality controls, exposes them through a small read-only API, and surfaces its own operational health.

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

6. **Cost discipline is part of the design.**
   Target: under CAD $20/month all-in. Features that push past that without commensurate value do not ship.

7. **Public means public.**
   Anyone can read the API, see the dashboards, fork the repo. No auth wall on read endpoints. Rate limits, not gates.

---

## 3. System overview

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         PUBLIC INTERNET                                  │
└──────────────┬───────────────────────────────────┬──────────────────────┘
               │                                   │
       ┌───────▼───────┐                  ┌────────▼────────┐
       │  quantumlane  │                  │ TTC GTFS-RT     │
       │     .com      │                  │ feeds (3)       │
       │ (static site) │                  └────────┬────────┘
       └───────┬───────┘                           │
               │ fetch JSON                        │ pull every 30s
               │                                   │
       ┌───────▼────────────────────────────────────▼────────┐
       │                  Caddy (reverse proxy + TLS)         │
       └────┬───────────────────────┬─────────────┬───────────┘
            │                       │             │
     ┌──────▼──────┐         ┌──────▼──────┐    ┌─▼──────────┐
     │   FastAPI   │         │   Dagster   │    │   Static   │
     │  (read-only)│         │  webserver  │    │   files    │
     └──────┬──────┘         └──────┬──────┘    └────────────┘
            │                       │
            │              ┌────────▼────────┐
            │              │ Dagster daemon  │
            │              │ (scheduler +    │
            │              │  sensors)       │
            │              └────────┬────────┘
            │                       │
            │            ┌──────────▼──────────┐
            │            │  ingestion workers  │
            │            │  (Python processes) │
            │            └──────────┬──────────┘
            │                       │
       ┌────▼───────────────────────▼────┐
       │   PostgreSQL 16 + PostGIS       │
       │   (single instance, on box)     │
       └─────────────────────────────────┘

                  ┌─────────────────────┐
                  │   Cloudflare R2     │
                  │ (raw protobuf       │
                  │  snapshots, daily   │
                  │  parquet exports)   │
                  └─────────────────────┘
```

A single Hetzner CAX21 box runs the entire stack. If and when components need to split, they will. Premature distribution is more expensive than its benefits at this scale.

---

## 4. Component design

### 4.1 Ingestion (`/ingestion`)

**Stack:** Dagster, Python 3.12, `gtfs-realtime-bindings`, SQLAlchemy, httpx.

See [ADR-001](adr/001-dagster-over-airflow.md) for the orchestrator choice.

**Assets and schedules:**

| Asset / Job | Source | Cadence | Target |
|---|---|---|---|
| `ttc_vehicle_positions` | TTC GTFS-RT VehiclePositions | Every minute (v0.1) | `realtime.vehicle_positions` |
| `ttc_trip_updates` | TTC GTFS-RT TripUpdates | Every minute (v0.1) | `realtime.trip_updates` |
| `ttc_service_alerts` | TTC GTFS-RT ServiceAlerts | Every 5 minutes | `realtime.service_alerts` |
| `ttc_static_gtfs` | TTC static GTFS zip | Daily 04:00 ET | `static_gtfs.*` (full reload) |
| `freshness_check` | Database query | Every minute | `ops.freshness_snapshot` |
| `daily_parquet_export` | Database query | Daily 03:00 ET | R2 `parquet/dt=YYYY-MM-DD/*.parquet` |
| `daily_partition_maintenance` | — | Daily 02:00 ET | Create next 7 days, detach >30 days |

**Resources** (Dagster `ConfigurableResource`):
- `PostgresResource`: connection pool, exposes engine and helpers
- `R2Resource`: S3-compatible client for Cloudflare R2
- `GTFSRTResource`: configured httpx client with retry, timeout, and User-Agent

**Failure handling:**
- Retries: up to 3 attempts with exponential backoff, only for transient errors (connection timeout, 5xx). 4xx and parse errors fail fast.
- Dead letter table: `ops.ingestion_failures` records failed runs with feed, error class, sample payload (truncated), and timestamp.
- The freshness check is the backstop. If retries mask a real outage, freshness catches it within a minute.

### 4.2 Database (`/db`)

**Stack:** PostgreSQL 16 with PostGIS 3.4.

**Schemas:**

```
static_gtfs.*  — daily snapshots of TTC static GTFS (routes, stops, trips, stop_times, calendar, shapes)
realtime.*     — append-only event tables, partitioned by day
ops.*          — pipeline metadata: freshness, runs, failures, schema versions
analytics.*    — derived views (empty in v0.1; populated in v0.3)
```

**Time zones:**
All timestamps are stored as `TIMESTAMPTZ` in UTC. Partition boundaries
are UTC days, so partition names (`_pYYYYMMDD`) reflect UTC dates.
During evening hours in Toronto, the UTC date may already be the
following calendar day — this is expected. The `daily_partition_maintenance`
job runs at 02:00 America/Toronto (06:00-07:00 UTC depending on DST),
at which point UTC and Toronto dates match and future partitions are
created for the next 7 UTC days.

**Partitioning strategy:**
`realtime.vehicle_positions` and `realtime.trip_updates` are range-partitioned by `received_at::date`. Daily partitions. The `daily_partition_maintenance` job creates the next 7 days of partitions each night and detaches partitions older than 30 days. Detached partitions are dropped after their parquet export is verified in R2.

**Retention:**
- Hot in Postgres: 30 days
- Cold in R2 as parquet: indefinite (cheap)
- Raw protobuf snapshots in R2: 7 days (debugging and replay only)

**Indexes:** created in migrations, not ad hoc. Each index carries a SQL comment explaining the query it supports.

**Migrations:** plain `.sql` files in `db/migrations/`, applied by `ops/scripts/migrate.py`. Numbered `NNNN_description.sql`. Forward-only. See [ADR-006](adr/006-raw-sql-migrations.md).

### 4.3 API (`/api`)

**Stack:** FastAPI, Pydantic v2, SQLAlchemy 2.x (sync), uvicorn.

**Endpoints (v0.1):**

```
GET  /health                        — liveness probe
GET  /ready                         — readiness probe (DB-aware)
GET  /v1/agencies                   — list agencies
GET  /v1/freshness                  — per-feed freshness summary
GET  /v1/vehicle-positions/latest   — most recent position per vehicle
GET  /v1/routes                     — list routes from static GTFS
GET  /v1/routes/{route_id}/vehicles — current vehicles on a route
GET  /v1/stats/daily                — record counts per feed per day, last 14 days
GET  /v1/ops/runs                   — recent Dagster run summary (last 50)
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

1. `/` — Landing. One-paragraph description; live freshness widget; links to freshness, explore, architecture, GitHub.
2. `/freshness` — Real-time freshness page. Polls `/v1/freshness` every 10 seconds. Shows per-feed last-update timestamps, ingestion lag, success rate over the last 24 hours, and schema-drift flags.
3. `/explore` — Pre-canned queries with the SQL shown alongside the result. Live data.
4. `/architecture` — A condensed view of this document with the system diagram.

No analytics tracking, no cookies, no JavaScript frameworks.

### 4.5 Ops (`/ops`)

**Stack:** Docker Compose, Caddy, a few shell scripts.

The compose stack runs everything: main PostgreSQL, Dagster metadata PostgreSQL, Dagster webserver/daemon/code server, the API, and Caddy as the reverse proxy.

**Deployment:** `make deploy` rsyncs the repo to the host, uploads the production `.env` from local `secrets/prod.env`, and runs `docker compose up -d` followed by migrations.

**Backups:** `pg_dump` to R2 daily. Restore path is documented in `docs/RUNBOOKS.md` (to be added).

**Secrets:** `.env` file, never committed. `.env.example` documents required variables.

---

## 5. Roadmap

### v0.1 — TTC end-to-end

**Goal:** TTC GTFS-RT and static GTFS flowing continuously, deployed, with the freshness page live.

**Non-goals for v0.1:**
- Other agencies
- Historical analytics
- ML or forecasting
- Streaming frameworks
- Interactive map visualizations
- Authentication

### v0.2 — Multi-agency and schema heterogeneity

Add GO Transit (Metrolinx), MiWay, and Brampton GTFS-RT feeds. Each agency emits GTFS-RT differently — different optional fields populated, different ID schemes, different update cadences, different reliability. The central design work is the schema reconciliation layer and the documented handling of each agency's quirks.

Add:
- Agency dimension table
- Per-agency configuration system (one Python module per agency)
- Schema-drift detection with alerting
- Data quality dimension framework (timeliness, completeness, validity, uniqueness, consistency)
- DQ scorecard endpoint and page

### v0.3 — VFH join and analytics layer

Add City of Toronto Vehicle-for-Hire data (monthly batch). Populate the `analytics.*` schema with derived models: hourly route performance, daily VFH demand, weather-joined demand.

### v0.4 — Public dataset publication

Versioned daily parquet exports published to R2 with a stable URL pattern. A dataset catalog page. QuantumLane becomes a small piece of public infrastructure.

### v0.5+

Possible directions:
- Bike Share Toronto
- Capital project disruption overlay (Metrolinx ON-Corridor work × TTC routes affected)
- Short-horizon arrival prediction (legitimate ML use case; defer until ≥6 months of historical data)

---

## 6. Initial decision log

Decisions with their own ADRs in `docs/adr/`:

| # | Decision | Alternative considered |
|---|---|---|
| 001 | Dagster, not Airflow | Airflow, Prefect, cron |
| 002 | Single PostgreSQL for all app data | DuckDB, ClickHouse, separate analytics DB |
| 003 | Single VPS, not Kubernetes | k3s, managed services |
| 004 | Hetzner hosting, not AWS | AWS, GCP, managed PaaS |
| 005 | Plain HTML, not Next.js or Astro | Next.js, Astro, SvelteKit |
| 006 | Raw SQL migrations, not Alembic | Alembic, sqitch |
| 007 | FastAPI, not Flask or Django REST | Flask, Django REST Framework |
| 008 | Cloudflare R2, not S3 | S3, Backblaze B2 |
| 009 | No streaming framework | Kafka, Redpanda, Kinesis |
| 010 | Public API with no auth | API keys, OAuth |
