-- 0003_realtime.sql
-- Realtime event tables. Range-partitioned by day on received_at.
-- Daily partitions are created by the daily_partition_maintenance Dagster job
-- which runs nightly and creates the next 7 days ahead.
--
-- Why partitioning at this scale:
--   - At 30s polling × 2000 active TTC vehicles, vehicle_positions takes ~5.7M rows/day.
--   - 30 days hot retention = ~170M rows. Without partitioning, vacuum and index maintenance
--     get progressively painful and queries against the most-recent-day case-degrade.
--   - Partitioning makes "drop data older than 30 days" a metadata operation (DETACH + DROP),
--     not a slow DELETE.
--   - Time-bounded queries (the common case) benefit from partition pruning.
--
-- The cost: queries that don't include a received_at predicate scan all partitions.
-- Mitigation: API endpoints always pass a time bound. Document this as a constraint, not a bug.

BEGIN;

-- Vehicle positions
CREATE TABLE realtime.vehicle_positions (
    received_at      TIMESTAMPTZ NOT NULL,
    feed_timestamp   TIMESTAMPTZ NOT NULL,         -- timestamp from the feed header
    agency_id        TEXT NOT NULL,
    vehicle_id       TEXT,
    trip_id          TEXT,
    route_id         TEXT,
    direction_id     INTEGER,
    location         GEOGRAPHY(POINT, 4326) NOT NULL,
    bearing          REAL,
    speed_mps        REAL,
    odometer_m       DOUBLE PRECISION,
    current_status   SMALLINT,                      -- enum: 0=incoming_at, 1=stopped_at, 2=in_transit_to
    current_stop_sequence INTEGER,
    stop_id          TEXT,
    congestion_level SMALLINT,
    occupancy_status SMALLINT,
    raw_payload_hash TEXT,                          -- sha256 of source protobuf message; helps dedupe
    PRIMARY KEY (received_at, agency_id, vehicle_id, feed_timestamp)
) PARTITION BY RANGE (received_at);

COMMENT ON TABLE realtime.vehicle_positions IS
    'GTFS-RT VehiclePositions feed. One row per vehicle per poll cycle. Partitioned by day on received_at.';

CREATE INDEX idx_vp_route_received ON realtime.vehicle_positions (route_id, received_at DESC);
COMMENT ON INDEX realtime.idx_vp_route_received IS
    'Supports /v1/routes/{route_id}/vehicles latest-positions query.';

CREATE INDEX idx_vp_vehicle_received ON realtime.vehicle_positions (vehicle_id, received_at DESC);
COMMENT ON INDEX realtime.idx_vp_vehicle_received IS
    'Supports per-vehicle history queries.';

CREATE INDEX idx_vp_location ON realtime.vehicle_positions USING GIST (location);
COMMENT ON INDEX realtime.idx_vp_location IS
    'Supports spatial "vehicles in bounding box" queries (planned v0.2).';

-- Trip updates (predicted arrivals/departures)
CREATE TABLE realtime.trip_updates (
    received_at         TIMESTAMPTZ NOT NULL,
    feed_timestamp      TIMESTAMPTZ NOT NULL,
    agency_id           TEXT NOT NULL,
    trip_id             TEXT NOT NULL,
    route_id            TEXT,
    direction_id        INTEGER,
    start_date          DATE,
    schedule_relationship SMALLINT,
    stop_sequence       INTEGER,
    stop_id             TEXT,
    arrival_time        TIMESTAMPTZ,
    arrival_delay_s     INTEGER,
    departure_time      TIMESTAMPTZ,
    departure_delay_s   INTEGER,
    raw_payload_hash    TEXT,
    PRIMARY KEY (received_at, agency_id, trip_id, stop_sequence, feed_timestamp)
) PARTITION BY RANGE (received_at);

COMMENT ON TABLE realtime.trip_updates IS
    'GTFS-RT TripUpdates feed. One row per stop_time_update per poll cycle. Partitioned by day.';

CREATE INDEX idx_tu_trip_received ON realtime.trip_updates (trip_id, received_at DESC);
CREATE INDEX idx_tu_route_received ON realtime.trip_updates (route_id, received_at DESC);

-- Service alerts (small volume, not partitioned; upsert pattern)
CREATE TABLE realtime.service_alerts (
    alert_id          TEXT NOT NULL,
    agency_id         TEXT NOT NULL,
    first_seen_at     TIMESTAMPTZ NOT NULL,
    last_seen_at      TIMESTAMPTZ NOT NULL,
    feed_timestamp    TIMESTAMPTZ NOT NULL,
    cause             SMALLINT,
    effect            SMALLINT,
    severity_level    SMALLINT,
    header_text       TEXT,
    description_text  TEXT,
    affected_routes   TEXT[],
    affected_stops    TEXT[],
    affected_trips    TEXT[],
    active_period_start TIMESTAMPTZ,
    active_period_end   TIMESTAMPTZ,
    raw_payload_hash  TEXT,
    PRIMARY KEY (agency_id, alert_id)
);

COMMENT ON TABLE realtime.service_alerts IS
    'GTFS-RT ServiceAlerts feed. Upserted on (agency_id, alert_id) with last_seen_at refreshed each cycle.';

CREATE INDEX idx_alerts_last_seen ON realtime.service_alerts (last_seen_at DESC);

INSERT INTO ops.schema_versions (version, description)
VALUES (3, 'Realtime tables with daily partitioning');

COMMIT;
