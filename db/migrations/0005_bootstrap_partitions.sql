-- 0005_bootstrap_partitions.sql
-- Ensures today's partitions exist for the range-partitioned realtime tables.
--
-- The daily_partition_maintenance Dagster job creates future partitions nightly
-- at 02:00 ET, but this leaves two gaps:
--   1. First boot: no partition for today until 02:00 ET, so ingestion fails.
--   2. Return from multi-day downtime: missed 02:00 ticks during the downtime
--      mean today's partition doesn't exist either.
--
-- We solve both by defining a stored procedure that creates today's partition
-- if missing, and calling it from two places:
--   - This migration (so a fresh install has today's partition immediately)
--   - make up (so every container bring-up also ensures today's partition)
--
-- Partition names follow the _pYYYYMMDD convention used by the maintenance job
-- so its retention/detach logic treats them identically to scheduled partitions.

BEGIN;

CREATE OR REPLACE PROCEDURE ops.ensure_today_partition()
LANGUAGE plpgsql
AS $$
DECLARE
    today_str TEXT := to_char(CURRENT_DATE, 'YYYYMMDD');
    today_start DATE := CURRENT_DATE;
    today_end DATE := CURRENT_DATE + 1;
BEGIN
    EXECUTE format(
        'CREATE TABLE IF NOT EXISTS realtime.vehicle_positions_p%s
         PARTITION OF realtime.vehicle_positions
         FOR VALUES FROM (%L) TO (%L)',
        today_str, today_start, today_end
    );
    EXECUTE format(
        'CREATE TABLE IF NOT EXISTS realtime.trip_updates_p%s
         PARTITION OF realtime.trip_updates
         FOR VALUES FROM (%L) TO (%L)',
        today_str, today_start, today_end
    );
    RAISE NOTICE 'ops.ensure_today_partition: ensured partitions for %', today_start;
END;
$$;

COMMENT ON PROCEDURE ops.ensure_today_partition() IS
    'Idempotently creates today''s partition for realtime.vehicle_positions and realtime.trip_updates. Called by migration 0005 and by the Makefile up target.';

-- Call it once now, so fresh installs have today's partition without
-- needing to wait for make up or for daily_partition_maintenance.
CALL ops.ensure_today_partition();

INSERT INTO ops.schema_versions (version, description)
VALUES (5, 'Bootstrap partitions procedure and initial call');

COMMIT;