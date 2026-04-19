# QuantumLane

A small, opinionated data platform for GTA transit data.

[![status](https://img.shields.io/badge/status-v0.1--in--development-orange)]()
[![license](https://img.shields.io/badge/license-MIT-blue)]()

## What it does

QuantumLane ingests public transit feeds, persists them with schema and quality controls, exposes them via a small read-only API, and surfaces its own operational health on a public website.

v0.1 ingests TTC GTFS-Realtime (vehicle positions, trip updates, service alerts) and TTC static GTFS. Later versions add GO Transit, MiWay, and other GTA agencies.

The architecture, the trade-offs, and the observability are the design focus — not the dashboards.

For the full design rationale and decision log, see [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md).

## Stack

| Layer | Choice |
|---|---|
| Orchestration | Dagster |
| Database | PostgreSQL 16 + PostGIS |
| Object storage | Cloudflare R2 |
| API | FastAPI + Pydantic v2 |
| Website | Static HTML + Tailwind (CDN) |
| Reverse proxy | Caddy |
| Local dev & deploy | Docker Compose |

Each choice has an ADR in [`docs/adr/`](docs/adr/) explaining what was rejected and why.

## Quick start

Prerequisites: Docker, docker compose v2, make.

```bash
git clone <repo-url>
cd quantumlane
cp .env.example .env                    # fill in passwords and, optionally, R2 credentials
make bootstrap                          # build images, run migrations
make up                                 # start the stack
```

Once running:
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

## Contributing

This is a personal project and is not currently accepting pull requests. Issues and discussion are welcome.

## License

MIT. See `LICENSE`.
