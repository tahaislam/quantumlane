# Spark workspace

Local-mode PySpark experiments against the QuantumLane operational Postgres.
This is a **learning / prototyping** workspace for the v0.3 lakehouse arc — these
scripts are not yet wired into Dagster. They prototype transforms that will later
graduate into the orchestrated pipeline (see `BACKLOG.md`, V0.3.x).

## Prerequisites

**Java 17.** Spark 3.5 does not support Java 21 (support arrived in Spark 4.0).
The system default may be 21; the venv pins 17 via `JAVA_HOME`.

```bash
sudo apt install openjdk-17-jre-headless
# add to .venv-spark/bin/activate so it's set on every activation:
#   export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
#   export PATH="$JAVA_HOME/bin:$PATH"
```

Note: `JAVA_HOME` is what Spark actually reads — `java -version` showing 21 on
PATH is fine as long as `echo $JAVA_HOME` points at 17 inside the activated venv.

**Python venv + PySpark.**

```bash
python3 -m venv .venv-spark
source .venv-spark/bin/activate
pip install "pyspark==3.5.*"
```

PySpark bundles Spark itself — no separate Spark install. Pinned to 3.5 to match
AWS EMR 7.x (the Phase 2 target) rather than running newer locally.

**Postgres JDBC driver.** Spark talks to Postgres over JDBC (a Java driver, not
psycopg). Not committed — download once:

```bash
mkdir -p spark/jars
curl -o spark/jars/postgresql-42.7.4.jar \
  https://jdbc.postgresql.org/download/postgresql-42.7.4.jar
```

**Dev stack up, Postgres reachable from the host.** The compose file publishes
Postgres on loopback (`127.0.0.1:5432`) for host-side tooling like these scripts.
Containers use `postgres:5432` internally; the host uses `localhost:5432`.

```bash
docker compose -f ops/compose/docker-compose.yml --env-file .env up -d
```

Credentials are read from the project `.env` (the same file compose uses) — no
passwords are hardcoded in the scripts.

## Gitignored

`.venv-spark/` and `spark/jars/` are gitignored (virtualenv + a 1 MB binary jar
don't belong in git). The download/setup steps above are the reproducibility trail.

## Spark UI

The live UI (http://localhost:4040) is served by a running SparkSession and dies
when the script exits — scripts hold the process open with `input(...)` at the end
so the UI stays up for inspection.

For comparing runs after they finish, the History Server reads event logs
(scripts write to `/tmp/spark-events`):

```bash
mkdir -p /tmp/spark-events
$(python -c "import pyspark, os; print(os.path.dirname(pyspark.__file__))")/sbin/start-history-server.sh
# browse http://localhost:18080 ; stop with stop-history-server.sh
```

## Local-mode tuning notes

- `spark.driver.memory=4g` — local-mode default heap is small (~1 GB); the full
  3.4M-row read plus window/groupBy shuffles OOMs at the default. Must be set
  before the JVM starts (fine via `getOrCreate()` on a fresh process; kill any
  lingering JVM with `pkill -f pyspark` if a config doesn't take).
- `spark.sql.shuffle.partitions=16` — the default of 200 is cluster-tuned and
  absurd for local mode on this data size (200 tiny partitions = per-task
  overhead dominates). Right-size to the machine.

## Scripts

- `headway_reliability.py` — RT-only transit reliability via headway regularity.
  Prototypes the future `olap.route_reliability` aggregation.

### headway_reliability.py — what it does

The TTC GTFS-RT `trip_updates` feed populates predicted arrival/departure *times*
but leaves the delay columns (`arrival_delay_s`, `departure_delay_s`) entirely
NULL (verified: 0 of 3.4M rows). True schedule-adherence delay needs the static
GTFS schedule (not yet loaded — see P2.1). So this computes a reliability metric
from RT data **alone**: headway regularity.

Pipeline:
1. **Dynamic partition bounds** — a pushed-down `min/max(received_at)` subquery
   (cheap, index-backed) frozen slightly behind `now()` so live ingestion writes
   don't skew the final read partition.
2. **Partitioned JDBC read** — 4 partitions on `received_at` for parallelism.
3. **Per-poll dedup** — the feed re-reports each trip+stop every poll; collapse to
   one arrival per `(route, direction, stop, trip)` via `max(arrival_time)`, the
   most-refined prediction. (Grain correction #1.)
4. **Headway via window function** — `Window.partitionBy(route, direction, stop)
   .orderBy(arrival_time)` + `lag` gives the previous vehicle's arrival; the
   difference (cast to epoch seconds) is the headway. Partitioning by direction
   is correctness-critical: without it, opposite-direction vehicles interleave
   into one sequence and `lag` measures meaningless cross-direction gaps.
5. **Filter artifacts** — `30 < headway_s < 7200` drops near-duplicate arrivals
   and overnight gaps.
6. **Aggregate to route+direction** — headway is a vehicle/route property (the
   same gap shows at every stop along a route), so the honest reporting grain is
   route+direction, not stop. (Grain correction #2.) Reliability = coefficient of
   variation (stddev/mean of headways): ~0 = perfectly regular, >1 = severe
   bunching.

Built to the **canonical GTFS schema**, not TTC's populated subset:
`direction_id` is carried through even though TTC leaves it NULL (one degenerate
group today; correctly splits eastbound/westbound the moment it's pointed at an
agency that populates it). Absent agency data should produce a degenerate result,
never require different code.

Validated against reality: streetcar routes (504/505/506/507/509/510/511) cluster
high on irregularity (real-world bunching); `avg_headway_min` tracks route
frequency correctly (frequent routes short, infrequent long).

### Graduation path

Add a date dimension (`groupBy route_id, direction_id, day`) and this becomes
`olap.route_reliability_daily` — the scheduled Phase 3b aggregation reading from
the Iceberg cold tier and writing back to Postgres `olap.*`.