# ADR 001: Use Dagster as the orchestrator

**Status:** Accepted
**Date:** 2026-04

## Context

QuantumLane needs an orchestrator to:
- Poll three GTFS-RT feeds on short cadences (30s / 5m)
- Run daily maintenance jobs (partition management, parquet export, static GTFS reload)
- Provide a UI for inspecting runs and debugging failures
- Run on a single small box alongside everything else

The project has access to extensive Airflow experience (custom operators, platform-level testing, on-prem and AWS deployments). Reusing that familiar tool is the easy choice.

## Decision

Use **Dagster** as the orchestrator.

## Alternatives considered

### Apache Airflow
Well-documented, large community, and already familiar to the project.

*Rejected because:*
- Scheduler + webserver + workers + metadata DB is a heavy footprint for a single-box deployment.
- DAG-centric model ("what runs") obscures the lakehouse mental model ("what exists and is fresh").
- Airflow upgrades are historically painful.
- Reusing what's already familiar adds no learning value for a project explicitly intended to exercise current practice.

### Prefect
Considered. Good DX, asset-like concepts in v2.

*Rejected because:*
- Smaller mindshare in the data engineering ecosystem compared to Dagster.
- Commercial tilt toward Prefect Cloud is harder to ignore than Dagster's.
- No single-container self-hosted story as clean as Dagster's.

### Cron + Python scripts
The nuclear option: systemd timers or cron, bash/Python glue.

*Rejected because:*
- No asset lineage, no UI, no retry semantics, no partitions.
- Fine for 3 jobs; painful by 10.
- Demonstrates nothing — we're here to show we can use real orchestration.

## Consequences

**Accepted costs:**
- Learning curve — Dagster is less familiar than Airflow in this context. First weeks of development will be slower.
- Dagster's "everything is Python" ethos requires discipline to avoid the resources and assets becoming entangled. We mitigate by keeping the parser pure (no Dagster imports).

**Benefits:**
- The ingestion logic gets a sensible structure (asset definitions + resources) on day one.
- Local development via `dagster dev` is a single command.
- Asset-centric UI makes "is this thing fresh" the default question — which is exactly what the `/freshness` page needs to answer.
- Writing a comparison piece between Dagster and Airflow from hands-on experience becomes possible.

**Watch for:**
- If the footprint of running three Dagster containers (code server + webserver + daemon) becomes disproportionate to the workload, revisit with a single-process deployment pattern.
- If Dagster's Postgres schema changes break upgrades, the dagster-postgres instance is isolated enough that the fix is contained.
