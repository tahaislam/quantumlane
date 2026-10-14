-- 0006_feed_watermark.sql
-- Per-feed high-water mark that outlives hot-tier retention.
--
-- Why: freshness_check computes last_record_at as MAX(received_at) over the
-- realtime.* tables. Those tables keep 3 days. Once an outage outlasts the
-- retention window, daily_partition_maintenance rotates the last real rows out
-- and MAX() goes NULL — the /freshness page then reads "Last record: never",
-- which reports retention, not history. (service_alerts is not partitioned, so
-- it kept its timestamp while the other two feeds lost theirs — see
-- docs/DATA_GAPS.md, 2026-09-10 entry.)
--
-- Fix: keep the newest observed record timestamp per feed in a tiny table that
-- retention never touches. freshness_check advances it (never backwards) and
-- falls back to it when the hot tier is empty.

BEGIN;

CREATE TABLE ops.feed_watermark (
    feed_key        TEXT PRIMARY KEY,            -- e.g. 'ttc.vehicle_positions'
    last_record_at  TIMESTAMPTZ NOT NULL,        -- newest received_at ever observed in the hot tier
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

COMMENT ON TABLE ops.feed_watermark IS
    'Newest record timestamp ever observed per feed. Survives hot-tier rotation; '
    'freshness_check falls back to it when the realtime.* tables are empty.';

-- Where the snapshot's last_record_at came from: 'hot' (MAX over the realtime
-- table) or 'watermark' (hot tier empty, value carried from ops.feed_watermark).
-- NULL on rows written before this migration.
ALTER TABLE ops.freshness_snapshot
    ADD COLUMN last_record_source TEXT
        CHECK (last_record_source IN ('hot', 'watermark'));

-- Seed from snapshot history. Snapshots are written every minute and never
-- trimmed, so the newest last_record_at they recorded is the true high-water
-- mark even for feeds whose hot-tier rows have already been dropped.
INSERT INTO ops.feed_watermark (feed_key, last_record_at)
SELECT feed_key, MAX(last_record_at)
FROM ops.freshness_snapshot
WHERE last_record_at IS NOT NULL
GROUP BY feed_key;

INSERT INTO ops.schema_versions (version, description)
VALUES (6, 'Per-feed watermark that survives hot-tier retention');

COMMIT;
