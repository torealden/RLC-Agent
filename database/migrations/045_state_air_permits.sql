-- =============================================================================
-- Migration 045: state_air_permits + state_air_permit_units
--
-- Consolidated bronze tables for state-issued air-quality operating permits
-- (Title V, FESOP, Synthetic Minor). Receives data from per-state collectors
-- (Iowa DNR, Indiana IDEM, Illinois EPA, Nebraska DEE, etc.) plus the LLM
-- post-processing layer (scripts/ollama/extract_titlev_permits.py).
--
-- Two-table design: one row per permit, many rows per emission unit.
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS bronze;

-- -----------------------------------------------------------------------------
-- Permit-level table
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS bronze.state_air_permits (
    id                       SERIAL PRIMARY KEY,
    state                    CHAR(2) NOT NULL,
    facility_id              TEXT,                  -- state's permit ID (e.g. "99-01-001")
    facility_name            TEXT NOT NULL,
    operator                 TEXT,                  -- parent company
    city                     TEXT,
    county                   TEXT,
    latitude                 NUMERIC,
    longitude                NUMERIC,
    permit_number            TEXT,                  -- e.g. "05-TV-005R3"
    permit_type              TEXT,                  -- 'Title V', 'FESOP', 'Synthetic Minor'
    issue_date               DATE,
    expiration_date          DATE,
    industry                 TEXT,                  -- 'oilseed_crush' | 'biodiesel' | ...
    facility_description     TEXT,
    facility_totals          JSONB,                 -- LLM-extracted facility-wide rollups
    raw_pdf_path             TEXT,
    raw_pdf_sha256           CHAR(64),              -- detect re-issued permits
    raw_pdf_pages            INTEGER,
    source                   TEXT NOT NULL,         -- 'iowa_dnr', 'indiana_idem', etc.
    extraction_method        TEXT,                  -- 'regex_v1' | 'llm:qwen3-coder:30b' | 'manual'
    extraction_notes         TEXT,
    extracted_at             TIMESTAMPTZ DEFAULT NOW(),
    collected_at             TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(state, facility_id, permit_number)
);

CREATE INDEX IF NOT EXISTS idx_state_air_permits_state
    ON bronze.state_air_permits(state);
CREATE INDEX IF NOT EXISTS idx_state_air_permits_industry
    ON bronze.state_air_permits(industry);
CREATE INDEX IF NOT EXISTS idx_state_air_permits_expiration
    ON bronze.state_air_permits(expiration_date);
CREATE INDEX IF NOT EXISTS idx_state_air_permits_facility_name
    ON bronze.state_air_permits(facility_name);

-- -----------------------------------------------------------------------------
-- Per-emission-unit detail
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS bronze.state_air_permit_units (
    id                       SERIAL PRIMARY KEY,
    permit_id                INTEGER NOT NULL
                              REFERENCES bronze.state_air_permits(id)
                              ON DELETE CASCADE,
    unit_id                  TEXT NOT NULL,         -- 'EU-001', '1.01'
    description              TEXT,
    category                 TEXT,                  -- 'extraction', 'refining', 'boiler', ...
    rated_capacity           NUMERIC,
    rated_capacity_unit      TEXT,
    throughput_limit         NUMERIC,
    throughput_limit_unit    TEXT,
    control_devices          JSONB,                 -- ['CD-001', 'baghouse-A']
    extra                    JSONB,                 -- catch-all for fields LLM extracts beyond schema
    extracted_at             TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(permit_id, unit_id)
);

CREATE INDEX IF NOT EXISTS idx_state_air_permit_units_permit
    ON bronze.state_air_permit_units(permit_id);
CREATE INDEX IF NOT EXISTS idx_state_air_permit_units_category
    ON bronze.state_air_permit_units(category);

-- -----------------------------------------------------------------------------
-- Convenience view: one row per facility with primary capacity rollups
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW silver.facility_air_permit_capacity AS
SELECT
    p.state,
    p.facility_name,
    p.operator,
    p.city,
    p.county,
    p.industry,
    p.permit_number,
    p.permit_type,
    p.expiration_date,
    -- Aggregate capacities by category — converted to common units would be a
    -- gold-layer concern; here we just expose the JSONB roll-up so the user
    -- can see what's in each unit class.
    jsonb_agg(
        jsonb_build_object(
            'unit_id', u.unit_id,
            'category', u.category,
            'rated_capacity', u.rated_capacity,
            'rated_capacity_unit', u.rated_capacity_unit,
            'throughput_limit', u.throughput_limit,
            'throughput_limit_unit', u.throughput_limit_unit
        ) ORDER BY u.unit_id
    ) FILTER (WHERE u.id IS NOT NULL) AS units,
    COUNT(u.id) AS n_units,
    p.facility_totals,
    p.raw_pdf_path,
    p.extraction_method,
    p.extracted_at
FROM bronze.state_air_permits p
LEFT JOIN bronze.state_air_permit_units u ON u.permit_id = p.id
GROUP BY p.id;

-- Document
COMMENT ON TABLE bronze.state_air_permits IS
    'State air-quality operating permits (Title V / FESOP / Synthetic Minor). One row per (state, facility, permit_number).';
COMMENT ON TABLE bronze.state_air_permit_units IS
    'Per-emission-unit detail for state_air_permits — capacity, throughput, control devices.';
COMMENT ON VIEW silver.facility_air_permit_capacity IS
    'Per-facility view of permitted equipment with category-level rollups (units field is JSONB array).';
