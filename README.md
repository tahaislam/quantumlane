# QuantumLane

A small, opinionated data platform for GTA transit data.

[![status](https://img.shields.io/badge/status-v0.4--active--development-orange)]()
[![license](https://img.shields.io/badge/license-MIT-blue)]()

## What it does

QuantumLane ingests public transit feeds, persists them across a hot/cold storage split with schema and quality controls, exposes them via a read-only API, and surfaces its own operational health on a public website.

The current release ingests TTC GTFS-Realtime (vehicle positions, trip updates, service alerts) and TTC static GTFS. Real-time data lands in a hot tier (PostgreSQL/PostGIS) for live queries; a daily job archives it to a partitioned cold tier (S3, Hive-style keys) in Parquet for historical analytics. Live operational queries and historical aggregation are deliberately served by different access paths rather than one general-purpose store.

Later versions add GO Transit, MiWay, and other GTA agencies.

The architecture, the trade-offs, and the observability are the design focus — not the dashboards.

For the full design rationale and decision log, see [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md).

## Live

- Website / status: https://quantumlane.io
- API docs: https://quantumlane.io/api/docs

The `/freshness` page reports live ingestion health.

## Stack

| Layer | Choice |
|---|---|
| Orchestration | Dagster |
| Database | PostgreSQL 16 + PostGIS |
| Object storage | S3 (cold-tier Parquet, Hive-partitioned) |
| API | FastAPI + Pydantic v2 |
| Website | Static HTML + Tailwind (CDN) |
| Reverse proxy | Caddy |
| Local dev & deploy | Docker Compose |

Each choice has an ADR in [`docs/adr/`](docs/adr/) explaining what was rejected and why.

## Architecture at a glance

- **Ingestion** — Dagster assets poll TTC GTFS-RT on a schedule and full-replace the static GTFS tables. Large static files (e.g. `stop_times`, ~4M rows) are streamed row-by-row into `COPY` from the open archive to stay within memory on a small box.
- **Hot tier** — recent real-time data in PostgreSQL/PostGIS, serving live API reads and near-real-time queries.
- **Cold tier** — a daily Parquet export to S3, partitioned by day with Hive-style keys, for historical/OLAP analytics. Reads stream through a server-side cursor into a held-open Parquet writer to bound memory.
- **Delay & reliability** — three distinct features (headway regularity, live schedule-adherence delay, historical reliability) are modelled separately by access pattern rather than collapsed into one metric. See [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) and the relevant ADR.

## Quick start

Prerequisites: Docker, docker compose v2, make.

```bash
git clone <repo-url>
cd quantumlane
cp .env.example .env                    # fill in passwords and, optionally, S3 credentials
make bootstrap                          # build images, run migrations
make up                                 # start the stack
```

Once running locally:
- Website: http://localhost:8080
- API docs: http://localhost:8080/api/docs
- Dagster UI: http://localhost:8080/dagster

Enable the schedules in the Dagster UI. Within two minutes the `/freshness` page should show live data.

## Repository layout

```
quantumlane/
├── docs/                    Architecture, ADRs
├── ingestion/               Dagster project (assets, resources, jobs)
├── api/                     FastAPI service
├── db/migrations/           Numbered SQL migrations (forward-only)
├── website/                 Static HTML/JS/CSS
├── ops/
│   ├── compose/             docker-compose stack + Caddyfile
│   └── scripts/             Migrations, deploy, backup
├── .github/workflows/       CI: lint + tests on every PR
├── Makefile                 The interface
└── README.md
```

## Development

```bash
make test        # run all tests
make lint        # ruff + mypy
make fmt         # auto-format
make logs        # tail service logs
make psql        # open psql against the main DB
```

## Roadmap

- **MCP server** — a Model Context Protocol server over the public API, letting LLM clients (Claude, ChatGPT) answer live transit questions: vehicles on a route (with human-name → `route_id` resolution), nearest stops via PostGIS, route lookup. Wraps the deployed HTTP API; remote-hosted and publicly accessible.
- **Historical reliability** — daily OLAP aggregation of event-time-computed delays.
- **Additional agencies** — GO Transit, MiWay, and other GTA feeds.

## Contributing

This is a personal project and is not currently accepting pull requests. Issues and discussion are welcome.

## License

MIT. See `LICENSE`.