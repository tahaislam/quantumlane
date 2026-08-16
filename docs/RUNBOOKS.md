# RUNBOOKS

Operational procedures for QuantumLane. Each entry is a self-contained,
copy-pasteable sequence. Commands assume you are in the repo root on the host
where the stack runs (dev machine or the Hetzner box).

The compose invocation is always the same; this doc abbreviates it as `$COMPOSE`:

```bash
COMPOSE="docker compose -f ops/compose/docker-compose.yml --env-file .env"
```

Contents:
- [restore-from-backup](#restore-from-backup)
- [collation-reindex](#collation-reindex) (P2.16)
- [deploy](#deploy)
- [provisioning-pre-flight](#provisioning-pre-flight)

---

## restore-from-backup

Restores the main database from a dump produced by `ops/scripts/backup.sh`. A
backup you have never restored is a hypothesis, not a backup — run this against
a throwaway target periodically to keep it honest.

**What backup.sh produces:** a gzipped `pg_dump` (`--clean --if-exists`) named
`quantumlane-<UTC-timestamp>.sql.gz`, uploaded to
`s3://$QL_S3_BUCKET/backups/`. Because the dump carries `--clean --if-exists`,
restoring it **drops and recreates** the existing objects — it overwrites, it
does not merge.

### 1. Pick the dump to restore

```bash
set -a; source .env; set +a   # load QL_S3_* credentials
AWS_ACCESS_KEY_ID="$QL_S3_ACCESS_KEY_ID" \
AWS_SECRET_ACCESS_KEY="$QL_S3_SECRET_ACCESS_KEY" \
AWS_DEFAULT_REGION="${QL_S3_REGION:-us-east-1}" \
aws s3 ls "s3://${QL_S3_BUCKET}/backups/"
```

### 2. Download and decompress

```bash
DUMP=quantumlane-YYYYMMDDTHHMMSSZ.sql.gz   # from the listing above
AWS_ACCESS_KEY_ID="$QL_S3_ACCESS_KEY_ID" \
AWS_SECRET_ACCESS_KEY="$QL_S3_SECRET_ACCESS_KEY" \
AWS_DEFAULT_REGION="${QL_S3_REGION:-us-east-1}" \
aws s3 cp "s3://${QL_S3_BUCKET}/backups/${DUMP}" "/tmp/${DUMP}"
```

### 3. Restore into Postgres

Prefer restoring into a **scratch database first** to verify the dump before
touching live data. To validate only:

```bash
$COMPOSE exec -T postgres psql -U "${POSTGRES_USER:-quantumlane}" -d postgres \
    -c "CREATE DATABASE restore_check;"
gunzip -c "/tmp/${DUMP}" | \
    $COMPOSE exec -T postgres psql -U "${POSTGRES_USER:-quantumlane}" -d restore_check
# ...inspect restore_check, then:
$COMPOSE exec -T postgres psql -U "${POSTGRES_USER:-quantumlane}" -d postgres \
    -c "DROP DATABASE restore_check;"
```

To restore over the **live** database (destructive — `--clean` drops existing
objects). Stop writers first so ingestion isn't racing the restore:

```bash
$COMPOSE stop dagster-daemon dagster-code api mcp
gunzip -c "/tmp/${DUMP}" | \
    $COMPOSE exec -T postgres psql -U "${POSTGRES_USER:-quantumlane}" -d "${POSTGRES_DB:-quantumlane}"
$COMPOSE start dagster-daemon dagster-code api mcp
```

### 4. Post-restore

- Ensure today's partition exists (the dump may predate it):
  ```bash
  $COMPOSE exec -T postgres psql -U "${POSTGRES_USER:-quantumlane}" \
      -d "${POSTGRES_DB:-quantumlane}" -c "CALL ops.ensure_today_partition();"
  ```
- Confirm the `realtime.*` partition list looks sane (see the data-model
  invariants in `AGENTS.md`).
- `rm -f "/tmp/${DUMP}"`.

---

## collation-reindex

**When you need this (P2.16):** after a Postgres base-image bump (or any host
libc/ICU upgrade), the OS collation library version can change out from under
existing indexes. Postgres then warns:

```
WARNING:  database "quantumlane" has a collation version mismatch
DETAIL:   The database was created using collation version X, but the operating system provides version Y.
HINT:     Rebuild all objects in this database that use the default collation ...
```

Text indexes built under the old collation can now be subtly **corrupt** —
ordering and uniqueness are no longer guaranteed. Do not ignore the warning; the
fix is to rebuild the affected indexes and then record the new collation version.

### 1. See who's mismatched

```bash
$COMPOSE exec -T postgres psql -U "${POSTGRES_USER:-quantumlane}" -d "${POSTGRES_DB:-quantumlane}" -c "
SELECT datname, datcollversion, pg_database_collation_actual_version(oid) AS os_version
FROM pg_database WHERE datname = current_database();"
```

If `datcollversion` differs from `os_version`, the indexes need rebuilding.

### 2. REINDEX

Rebuild every index in the database. `CONCURRENTLY` avoids taking write locks on
the tables (important while ingestion is running); it is slower and cannot run
inside a transaction block.

```bash
$COMPOSE exec -T postgres reindexdb -U "${POSTGRES_USER:-quantumlane}" \
    --concurrently -d "${POSTGRES_DB:-quantumlane}"
```

If a `CONCURRENTLY` rebuild is interrupted it can leave an `INVALID` index
behind. Find and drop any leftovers, then re-run:

```bash
$COMPOSE exec -T postgres psql -U "${POSTGRES_USER:-quantumlane}" -d "${POSTGRES_DB:-quantumlane}" -c "
SELECT indexrelid::regclass FROM pg_index WHERE NOT indisvalid;"
```

### 3. Record the new collation version

Only after the REINDEX succeeds — this clears the warning by telling Postgres the
current indexes match the current OS collation:

```bash
$COMPOSE exec -T postgres psql -U "${POSTGRES_USER:-quantumlane}" -d "${POSTGRES_DB:-quantumlane}" \
    -c "ALTER DATABASE \"${POSTGRES_DB:-quantumlane}\" REFRESH COLLATION VERSION;"
```

Refreshing the version **without** reindexing first just silences the alarm while
leaving corrupt indexes in place. Always REINDEX, then REFRESH.

---

## deploy

Production runs on a single Hetzner box. Deploys flow through git: edit on dev →
push → pull on the box → rebuild. There is no registry; images are built on the
host.

> `ops/scripts/deploy.sh` (`make deploy`) is the rsync-based alternative that
> pushes the working tree from your machine. The git-pull flow below is the
> primary path and the one to use when the box already has the repo cloned.

### 1. On dev: push your change

```bash
git push origin main
```

### 2. On the box: pull and rebuild

```bash
ssh ql@quantumlane.io
cd /home/ql/projects/quantumlane
git pull
```

Now pick the reload that matches **what you changed** — using the wrong one
silently leaves the old version running:

- **Python source** (`ingestion/`, `api/`, `ops/`, `mcp/`) — baked into images.
  Rebuild and recreate:
  ```bash
  make build && make up
  ```
  A plain `restart` reuses the OLD image and is not enough.

- **Compose definition** (`ports:`, `environment:`, `volumes:` in
  `docker-compose.yml`) — apply by recreating changed services:
  ```bash
  docker compose -f ops/compose/docker-compose.yml --env-file .env up -d
  ```

- **Bind-mounted config** (the Caddyfile, anything under `/srv/website` that a
  long-running process reads at startup) — the file is already live to the
  container, but the process read the old copy at boot. `up -d` will NOT notice a
  changed mount's *contents*; bounce or force-recreate the process:
  ```bash
  docker compose -f ops/compose/docker-compose.yml --env-file .env restart caddy
  # or, for a clean slate:
  docker compose -f ops/compose/docker-compose.yml --env-file .env up -d --force-recreate caddy
  ```
  Static files under `/srv/website` are served per-request — a `git pull` makes
  them live with no restart.

### 3. Migrations

If the change added a migration:

```bash
make migrate          # apply
make migrate-status   # verify
```

### 4. Verify

```bash
docker compose -f ops/compose/docker-compose.yml --env-file .env ps
```

The box holds live data. Operations here preserve the volume — **never**
`make nuke` on production.

---

## provisioning-pre-flight

Run this **before** committing to a new host or region. The TTC GTFS-RT feeds
geo-block non-North-American IPs — this is why production lives in Ashburn
(US-East) and not a cheaper European box. A host that cannot reach the feeds is
useless no matter how well the stack deploys, so check reachability first.

From the candidate host (or an SSH session on it):

```bash
for url in \
    https://bustime.ttc.ca/gtfsrt/vehicles \
    https://bustime.ttc.ca/gtfsrt/trips \
    https://bustime.ttc.ca/gtfsrt/alerts; do
    printf '%s -> ' "$url"
    curl -m 10 -sS -o /dev/null -w '%{http_code}\n' "$url" || echo "UNREACHABLE"
done
```

Interpreting the result:

- **`200`** on all three — the region is viable; proceed with provisioning.
- **`403` / `401`, or a connection that hangs until the `-m 10` timeout** — the
  IP is geo-blocked. Pick a North American region and re-run. Do not proceed;
  the ingestion assets will fail identically once deployed.

The `-m 10` (10-second cap) matters: a geo-block often manifests as a silent
hang rather than a clean rejection, and you want the check to fail fast rather
than wedge.
