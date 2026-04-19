# ADR 002: Single PostgreSQL for all application data

**Status:** Accepted
**Date:** 2026-04

## Context

QuantumLane mixes workloads:
- Append-heavy realtime ingestion (millions of rows per day)
- Reference-table reads (static GTFS, mostly unchanging)
- Spatial queries (PostGIS)
- Operational metadata (freshness, run history)
- Small future analytics workloads

Industry convention often splits these across engines: an OLTP store for realtime, a columnar store (DuckDB, ClickHouse) for analytics, sometimes a separate geospatial tool.

## Decision

Use a **single PostgreSQL 16 + PostGIS instance** for all application data. Partition the high-volume realtime tables by day. Defer any multi-engine architecture until profiling shows the single engine is failing at a specific, named workload.

The Dagster metadata database is a separate PostgreSQL instance for blast-radius reasons, but that's an orchestrator concern, not a data-engine concern.

## Alternatives considered

### DuckDB for analytics, Postgres for serving
Appealing for query performance on historical aggregations.

*Rejected because:*
- The analytics workload in v0.1–v0.3 is small enough that Postgres handles it comfortably with proper indexing.
- Two-engine setups double the operational surface: backups, monitoring, schema evolution, and the integration glue that copies between them.
- DuckDB as a server-mode analytic engine is still maturing; embedded use is its strength.

### ClickHouse for realtime analytics
Good fit for high-cardinality time-series aggregations.

*Rejected because:*
- Operational complexity (ZooKeeper or Keeper, replica topology, specialized schema design) is disproportionate to the workload.
- PostgreSQL with daily partitioning handles the ingestion rate and retention comfortably.

### Separate read replica for the API
Classic pattern for isolating analytical reads from writes.

*Deferred:* if API query load begins affecting ingestion latency, add a streaming replica via `pg_basebackup` or logical replication. Until then, it's complexity without a named problem.

## Consequences

**Accepted costs:**
- When analytics workloads grow, some queries will get slower before we migrate them. That migration cost is a future problem, reserved for when it's actually justified.
- PostgreSQL's columnar story is weaker than purpose-built columnar engines. Aggregations over hundreds of millions of rows will want help (proper partitioning, materialized views, or the `citus` columnar extension if we need it).

**Benefits:**
- One set of backups, one set of credentials, one set of monitoring concerns.
- Every engineer and every AI assistant knows PostgreSQL; the tooling ecosystem is exhaustive.
- PostGIS is best-in-class for geospatial; keeping it in the primary store avoids ETL between stores for spatial queries.

**Watch for:**
- `realtime.*` table sizes as agencies are added. Partition pruning must be effective; if queries ever scan all partitions, that's a bug, not a performance issue.
- The `ops.freshness_snapshot` table growth rate. At one row per feed per minute, it's ~4K rows/day/feed. Add a retention policy before it exceeds a million rows.
