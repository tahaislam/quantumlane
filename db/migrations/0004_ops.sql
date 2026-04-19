-- 0004_ops.sql
-- Operational metadata. This is what the /freshness page reads from.
-- Treat these tables as the system's immune system: they're how we know it's healthy.

BEGIN;

-- Per-feed freshness snapshots, written by the freshness_check Dagster sensor every minute.
-- We keep history (not just current state) so the website can show trend charts later.
CREATE TABLE ops.freshness_snapshot (
    snapshot_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    feed_key            TEXT NOT NULL,           -- e.g. 'ttc.vehicle_positions'
    last_record_at      TIMESTAMPTZ,             -- max(received_at) for that feed
    record_count_5min   BIGINT NOT NULL,
    record_count_1h     BIGINT NOT NULL,
    lag_seconds         INTEGER,                 -- snapshot_at - last_record_at
    status              TEXT NOT NULL,           -- 'healthy' | 'lagging' | 'stale' | 'down'
    PRIMARY KEY (snapshot_at, feed_key)
);

CREATE INDEX idx_freshness_feed_time ON ops.freshness_snapshot (feed_key, snapshot_at DESC);
COMMENT ON INDEX ops.idx_freshness_feed_time IS
    'Supports "latest snapshot per feed" and "last 24h trend per feed" queries.';

-- Ingestion run history. One row per Dagster asset materialization.
-- Mirrors what's in the Dagster event log but in a stable, queryable shape under our control.
CREATE TABLE ops.ingestion_runs (
    run_id           TEXT PRIMARY KEY,           -- Dagster run_id
    asset_key        TEXT NOT NULL,
    started_at       TIMESTAMPTZ NOT NULL,
    completed_at     TIMESTAMPTZ,
    status           TEXT NOT NULL,              -- 'running' | 'success' | 'failure' | 'canceled'
    records_written  BIGINT,
    bytes_processed  BIGINT,
    error_class      TEXT,
    error_message    TEXT
);

CREATE INDEX idx_runs_asset_started ON ops.ingestion_runs (asset_key, started_at DESC);

-- Dead-letter table for failed ingestion attempts.
-- A row here means: we tried, we failed, we recorded enough to debug it later.
CREATE TABLE ops.ingestion_failures (
    failure_id        BIGSERIAL PRIMARY KEY,
    occurred_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    feed_key          TEXT NOT NULL,
    error_class       TEXT NOT NULL,
    error_message     TEXT,
    sample_payload    BYTEA,                      -- truncated to first 4KB
    sample_payload_truncated BOOLEAN NOT NULL DEFAULT FALSE,
    retry_count       INTEGER NOT NULL DEFAULT 0,
    resolved_at       TIMESTAMPTZ                 -- set manually when investigated
);

CREATE INDEX idx_failures_feed_time ON ops.ingestion_failures (feed_key, occurred_at DESC);
CREATE INDEX idx_failures_unresolved ON ops.ingestion_failures (occurred_at DESC) WHERE resolved_at IS NULL;

-- Schema drift detection. The hash of the populated-field set for each feed is recorded
-- on every ingest. If the hash changes, we know the upstream feed shape changed
-- (a field newly populated, or one that stopped being populated). v0.2 adds alerting on this.
CREATE TABLE ops.feed_field_signatures (
    feed_key         TEXT NOT NULL,
    signature_hash   TEXT NOT NULL,              -- sha256 of sorted populated field paths
    populated_fields TEXT[] NOT NULL,
    first_seen_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_seen_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    sample_count     BIGINT NOT NULL DEFAULT 1,
    PRIMARY KEY (feed_key, signature_hash)
);

INSERT INTO ops.schema_versions (version, description)
VALUES (4, 'Ops tables: freshness, runs, failures, schema drift');

COMMIT;
