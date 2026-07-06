-- 0006_service_alert_classifications.sql
-- LLM-inferred classifications for service alerts.
--
-- Live TTC alerts almost never populate the native cause (~3%) and never populate
-- severity_level, while header_text is always present. A classifier (Anthropic API,
-- see quantumlane_ingestion.classifier) infers cause and severity from the alert text
-- and stores the result HERE — never in realtime.service_alerts. Native and inferred
-- values are joined at read time so provenance stays unambiguous.
--
-- Caching: input_hash is sha256 over the normalized header_text + description_text.
-- The unique key (agency_id, alert_id, input_hash) means an alert is classified once
-- per distinct text; a poll cycle that sees unchanged text is a cache hit and makes
-- no API call. If the text changes, a NEW row is appended (the old row is kept), and
-- the read path picks the latest by classified_at.
--
-- Retention: NOT partitioned and retained indefinitely. Volume is tiny (tens of rows
-- per day) and the history has analytical value. The 3-day realtime.* retention in
-- daily_partition_maintenance does not apply: that job only touches the partitioned
-- parents (vehicle_positions, trip_updates) and tables matching the _pYYYYMMDD name
-- pattern, neither of which matches this table. Do not add it to retention.

BEGIN;

CREATE TABLE realtime.service_alert_classifications (
    agency_id         TEXT NOT NULL,
    alert_id          TEXT NOT NULL,
    input_hash        TEXT NOT NULL,               -- sha256 of normalized header+description text
    cause_inferred    TEXT NOT NULL,               -- GTFS-RT Cause label inferred from alert text
    cause_detail      TEXT,                        -- short free-text phrase from the alert, e.g. 'sewer replacement'
    severity_inferred TEXT NOT NULL,               -- GTFS-RT SeverityLevel label inferred from alert text
    model             TEXT NOT NULL,               -- model id that produced the classification
    prompt_version    TEXT NOT NULL,               -- PROMPT_VERSION constant at classification time
    classified_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    raw_response      JSONB,                       -- parsed model response, kept for audit/debugging
    PRIMARY KEY (agency_id, alert_id, input_hash),
    -- Python validates against the same label sets BEFORE any write; these CHECKs are
    -- the backstop, not the validator.
    CONSTRAINT chk_sac_cause_inferred CHECK (cause_inferred IN (
        'UNKNOWN_CAUSE', 'OTHER_CAUSE', 'TECHNICAL_PROBLEM', 'STRIKE',
        'DEMONSTRATION', 'ACCIDENT', 'HOLIDAY', 'WEATHER', 'MAINTENANCE',
        'CONSTRUCTION', 'POLICE_ACTIVITY', 'MEDICAL_EMERGENCY'
    )),
    CONSTRAINT chk_sac_severity_inferred CHECK (severity_inferred IN (
        'UNKNOWN_SEVERITY', 'INFO', 'WARNING', 'SEVERE'
    ))
);

COMMENT ON TABLE realtime.service_alert_classifications IS
    'LLM-inferred cause/severity per (alert, text-hash). Append-only, unpartitioned, retained '
    'indefinitely — exempt from the 3-day realtime retention. Inferred values are NEVER written '
    'into realtime.service_alerts; join at read time.';

-- Read path fetches the latest classification per alert (ORDER BY classified_at DESC LIMIT 1
-- in a lateral join). The PK covers the cache-hit lookup by exact triple.
CREATE INDEX idx_sac_alert_latest
    ON realtime.service_alert_classifications (agency_id, alert_id, classified_at DESC);

INSERT INTO ops.schema_versions (version, description)
VALUES (6, 'Service alert LLM classifications (cause/severity inferred from alert text)');

COMMIT;
