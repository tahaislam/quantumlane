-- 0002_static_gtfs.sql
-- Static GTFS reference tables. Reloaded daily from the TTC's published static GTFS zip.
-- These are dimension-like: relatively slow to change, joined frequently from realtime tables.

BEGIN;

CREATE TABLE static_gtfs.agency (
    agency_id        TEXT PRIMARY KEY,
    agency_name      TEXT NOT NULL,
    agency_url       TEXT,
    agency_timezone  TEXT NOT NULL,
    agency_lang      TEXT,
    snapshot_date    DATE NOT NULL
);

CREATE TABLE static_gtfs.routes (
    route_id         TEXT PRIMARY KEY,
    agency_id        TEXT NOT NULL,
    route_short_name TEXT,
    route_long_name  TEXT,
    route_type       INTEGER NOT NULL,
    route_color      TEXT,
    route_text_color TEXT,
    snapshot_date    DATE NOT NULL
);

CREATE INDEX idx_routes_short_name ON static_gtfs.routes (route_short_name);
COMMENT ON INDEX static_gtfs.idx_routes_short_name IS
    'Supports lookups like "route 504" by the human-facing short name in API queries.';

CREATE TABLE static_gtfs.stops (
    stop_id        TEXT PRIMARY KEY,
    stop_code      TEXT,
    stop_name      TEXT NOT NULL,
    stop_desc      TEXT,
    location       GEOGRAPHY(POINT, 4326) NOT NULL,
    zone_id        TEXT,
    location_type  INTEGER,
    parent_station TEXT,
    snapshot_date  DATE NOT NULL
);

CREATE INDEX idx_stops_location ON static_gtfs.stops USING GIST (location);
COMMENT ON INDEX static_gtfs.idx_stops_location IS
    'Supports "stops within N meters of point" queries used by the /v1/stops/nearby endpoint (planned v0.2).';

CREATE INDEX idx_stops_name_trgm ON static_gtfs.stops USING GIN (stop_name gin_trgm_ops);
COMMENT ON INDEX static_gtfs.idx_stops_name_trgm IS
    'Trigram index for fuzzy stop name search.';

CREATE TABLE static_gtfs.trips (
    trip_id        TEXT PRIMARY KEY,
    route_id       TEXT NOT NULL REFERENCES static_gtfs.routes(route_id),
    service_id     TEXT NOT NULL,
    trip_headsign  TEXT,
    direction_id   INTEGER,
    block_id       TEXT,
    shape_id       TEXT,
    snapshot_date  DATE NOT NULL
);

CREATE INDEX idx_trips_route ON static_gtfs.trips (route_id);
CREATE INDEX idx_trips_service ON static_gtfs.trips (service_id);

CREATE TABLE static_gtfs.stop_times (
    trip_id            TEXT NOT NULL,
    stop_sequence      INTEGER NOT NULL,
    stop_id            TEXT NOT NULL REFERENCES static_gtfs.stops(stop_id),
    arrival_time       INTERVAL,   -- can exceed 24h for trips spanning midnight, hence INTERVAL not TIME
    departure_time     INTERVAL,
    pickup_type        INTEGER,
    drop_off_type      INTEGER,
    shape_dist_traveled DOUBLE PRECISION,
    snapshot_date      DATE NOT NULL,
    PRIMARY KEY (trip_id, stop_sequence)
);

CREATE INDEX idx_stop_times_stop ON static_gtfs.stop_times (stop_id);

CREATE TABLE static_gtfs.calendar (
    service_id    TEXT PRIMARY KEY,
    monday        BOOLEAN NOT NULL,
    tuesday       BOOLEAN NOT NULL,
    wednesday     BOOLEAN NOT NULL,
    thursday      BOOLEAN NOT NULL,
    friday        BOOLEAN NOT NULL,
    saturday      BOOLEAN NOT NULL,
    sunday        BOOLEAN NOT NULL,
    start_date    DATE NOT NULL,
    end_date      DATE NOT NULL,
    snapshot_date DATE NOT NULL
);

CREATE TABLE static_gtfs.calendar_dates (
    service_id      TEXT NOT NULL,
    exception_date  DATE NOT NULL,
    exception_type  INTEGER NOT NULL,  -- 1 = added, 2 = removed
    snapshot_date   DATE NOT NULL,
    PRIMARY KEY (service_id, exception_date)
);

-- shapes is large and only needed for map rendering (deferred to v0.2).
-- Defined here for completeness; loader can skip it via config in v0.1.
CREATE TABLE static_gtfs.shapes (
    shape_id            TEXT NOT NULL,
    shape_pt_sequence   INTEGER NOT NULL,
    shape_pt_location   GEOGRAPHY(POINT, 4326) NOT NULL,
    shape_dist_traveled DOUBLE PRECISION,
    snapshot_date       DATE NOT NULL,
    PRIMARY KEY (shape_id, shape_pt_sequence)
);

INSERT INTO ops.schema_versions (version, description)
VALUES (2, 'Static GTFS reference tables');

COMMIT;
