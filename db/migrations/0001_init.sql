-- 0001_init.sql
-- Initial schemas, extensions, and the migration tracking table itself.
-- Forward-only. Never edit a migration after it has been applied to any environment.

BEGIN;

-- Extensions
CREATE EXTENSION IF NOT EXISTS postgis;
CREATE EXTENSION IF NOT EXISTS pg_trgm;  -- for fuzzy text search later (route names, alerts)

-- Schemas
CREATE SCHEMA IF NOT EXISTS static_gtfs;
CREATE SCHEMA IF NOT EXISTS realtime;
CREATE SCHEMA IF NOT EXISTS ops;
CREATE SCHEMA IF NOT EXISTS analytics;

COMMENT ON SCHEMA static_gtfs IS 'Daily snapshot of static GTFS feeds (routes, stops, trips, calendar, shapes).';
COMMENT ON SCHEMA realtime IS 'Append-only event tables from GTFS-RT feeds, partitioned by day.';
COMMENT ON SCHEMA ops IS 'Pipeline metadata: freshness, run history, failures, schema versions.';
COMMENT ON SCHEMA analytics IS 'Derived models and views. Empty in v0.1; populated in v0.3.';

-- Migration tracking
CREATE TABLE ops.schema_versions (
    version       INTEGER PRIMARY KEY,
    description   TEXT NOT NULL,
    applied_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    applied_by    TEXT NOT NULL DEFAULT CURRENT_USER,
    checksum      TEXT  -- sha256 of the migration file at apply time
);

COMMENT ON TABLE ops.schema_versions IS
    'One row per applied migration. Updated by ops/scripts/migrate.py. Never written to by application code.';

INSERT INTO ops.schema_versions (version, description, checksum)
VALUES (1, 'Initial schemas, extensions, and migration tracking', NULL);

COMMIT;
