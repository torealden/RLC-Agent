-- Migration 106: bronze.epa_echo_facility_audit
--
-- Phase 2 of the EPA ECHO architecture flip (Task #66).
-- The existing bronze.epa_echo_facility table holds CURRENT state — every
-- daily refresh overwrites yesterday's row by FRS ID. That means a facility
-- transitioning Operating -> Idle -> Operating leaves no historical trace.
--
-- This audit table records every state change observed by the new
-- FRS-driven enrichment collector. We only insert when fields of interest
-- actually change vs the previous bronze.epa_echo_facility row, so it stays
-- compact (~5-20 rows/day across the whole 2,000-facility book).
--
-- Fields tracked are the ones with operational/strategic meaning:
--   operating_status — Operating, Idle, Permanently Closed, etc.
--   compliance_status — CAA SNC, QtrsNC counts
--   enforcement_actions — High Priority Violation, etc.
--   air_classification — Major / Synthetic Minor / etc.

CREATE TABLE IF NOT EXISTS bronze.epa_echo_facility_audit (
    id                   BIGSERIAL PRIMARY KEY,
    frs_registry_id      TEXT NOT NULL,
    facility_name        TEXT,
    state                TEXT,
    field                TEXT NOT NULL,              -- which column changed
    previous_value       TEXT,                       -- old value (may be NULL on first observation)
    new_value            TEXT,                       -- new value
    observed_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    collector_run_id     TEXT,
    notes                TEXT
);

CREATE INDEX IF NOT EXISTS idx_echo_facility_audit_frs
    ON bronze.epa_echo_facility_audit(frs_registry_id);
CREATE INDEX IF NOT EXISTS idx_echo_facility_audit_field_time
    ON bronze.epa_echo_facility_audit(field, observed_at);
CREATE INDEX IF NOT EXISTS idx_echo_facility_audit_observed
    ON bronze.epa_echo_facility_audit(observed_at);

-- Convenience view: most recent change per facility/field
CREATE OR REPLACE VIEW gold.epa_echo_state_transitions AS
SELECT DISTINCT ON (frs_registry_id, field)
    frs_registry_id,
    facility_name,
    state,
    field,
    previous_value,
    new_value,
    observed_at,
    collector_run_id
FROM bronze.epa_echo_facility_audit
ORDER BY frs_registry_id, field, observed_at DESC;

-- Convenience view: facilities that flipped to "Permanently Closed" in the last 90 days
CREATE OR REPLACE VIEW gold.epa_echo_recent_closures AS
SELECT
    frs_registry_id,
    facility_name,
    state,
    previous_value AS prior_status,
    new_value AS current_status,
    observed_at,
    collector_run_id
FROM bronze.epa_echo_facility_audit
WHERE field = 'operating_status'
  AND new_value ILIKE '%closed%'
  AND observed_at > NOW() - INTERVAL '90 days'
ORDER BY observed_at DESC;
