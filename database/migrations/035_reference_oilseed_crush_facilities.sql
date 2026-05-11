-- ============================================================================
-- Migration 035: reference.oilseed_crush_facilities
-- ============================================================================
-- Materialize the oilseed crush facility roster from
-- models/Oilseeds/us_oilseed_crushing_capacity.xlsm into a queryable table.
--
-- Per Iowa Crush Agent spec §6.1 — single source of truth for facility
-- static data, referenced by silver.facility_state, silver.strategic_plan,
-- bronze.daily_decisions, gold.monthly_crush_estimates.
--
-- Convention:
--   facility_id (PRIMARY KEY) matches core.kg_node.node_key for facilities
--   that exist in the KG (all 20 IA facilities from batch 021). For non-IA
--   facilities, facility_id follows the same pattern: '{state}.{op}_{city}'.
-- ============================================================================

CREATE SCHEMA IF NOT EXISTS reference;

CREATE TABLE IF NOT EXISTS reference.oilseed_crush_facilities (
    -- Identity — facility_id matches KG node_key for facilities in the KG
    facility_id              TEXT PRIMARY KEY,
    name                     TEXT NOT NULL,
    operator                 TEXT NOT NULL,
    parent_company           TEXT,
    operator_kg_key          TEXT,            -- joins to core.kg_node.node_key for company

    -- Location
    address                  TEXT,
    city                     TEXT NOT NULL,
    county                   TEXT,
    state                    CHAR(2) NOT NULL,
    zip                      TEXT,
    lat                      NUMERIC(9,6),
    lon                      NUMERIC(9,6),
    county_kg_key            TEXT,            -- joins to core.kg_node.node_key for region

    -- Capacity (multiple unit views; native is bushels/day for oilseeds)
    primary_oilseed          TEXT NOT NULL DEFAULT 'soybean',
    nameplate_bpd            NUMERIC(10,2),   -- bushels per day
    nameplate_tpd            NUMERIC(10,2)
                             GENERATED ALWAYS AS (nameplate_bpd * 60 / 2000) STORED,
    nameplate_mmbu_yr        NUMERIC(8,3),    -- million bushels per year (workbook value)
    operating_days_year      INTEGER DEFAULT 350,
    commissioned_year        INTEGER,         -- year first operational
    last_expansion_year      INTEGER,
    expansion_details        TEXT,

    -- Refining / biofuel co-location
    refining_capability      TEXT,            -- 'Crude' / 'Degumming' / 'RB' / 'RBD' / 'Unknown' / NULL
    refining_capacity        TEXT,
    biodiesel_capacity_mgy   NUMERIC(8,2),
    has_co_located_biofuel   BOOLEAN GENERATED ALWAYS AS (biodiesel_capacity_mgy > 0) STORED,

    -- Regulatory IDs (cross-system match)
    title_v_permit           TEXT,            -- Iowa DNR (or equivalent state authority)
    state_dnr_facility_num   TEXT,
    epa_frs_id               TEXT,            -- TBD — needs FRS lookup
    epa_rfs_rin_id           TEXT,            -- TBD
    eia_plant_id             TEXT,            -- TBD — for co-located biofuel
    naics_code               TEXT,

    -- Market role
    nopa_member              BOOLEAN DEFAULT FALSE,
    status                   TEXT DEFAULT 'Operating',  -- 'Operating' / 'Idle' / 'Construction' / 'Closed'
    investment_usd_mm        NUMERIC(10,2),
    employees                INTEGER,

    -- Agent-system fields (per spec §6.1 ALTER)
    oil_destination_split    NUMERIC(4,3) DEFAULT 0.550,  -- BBD share of oil output
    catchment_basis_ticker   TEXT,            -- proxy basis ticker for daily quotes
    rail_terminal_id         TEXT,
    crush_model_xlsx_path    TEXT,            -- per-facility XLSX in object storage

    -- Catchment (per user 2026-04-26 convention)
    draw_radius_miles        INTEGER DEFAULT 50,
    draw_area_class          TEXT DEFAULT 'default_uniform_50mi',
                             -- 'default_uniform_50mi' | 'rail_truck_only_50mi'
                             -- | 'barge_access_250mi' | 'rail_hub_major_500mi'
                             -- | 'bespoke_<facility_id>'

    -- Provenance
    notes                    TEXT,
    sources                  TEXT,
    data_gaps                JSONB,           -- list of fields known to be empty/uncertain
    created_at               TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at               TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_oilseed_crush_state           ON reference.oilseed_crush_facilities (state);
CREATE INDEX IF NOT EXISTS idx_oilseed_crush_operator        ON reference.oilseed_crush_facilities (operator);
CREATE INDEX IF NOT EXISTS idx_oilseed_crush_status          ON reference.oilseed_crush_facilities (status);
CREATE INDEX IF NOT EXISTS idx_oilseed_crush_primary_oilseed ON reference.oilseed_crush_facilities (primary_oilseed);
CREATE INDEX IF NOT EXISTS idx_oilseed_crush_operator_kg     ON reference.oilseed_crush_facilities (operator_kg_key);

COMMENT ON TABLE reference.oilseed_crush_facilities IS
'Canonical roster of US oilseed crushing facilities. facility_id mirrors
 core.kg_node.node_key for facilities that exist in the KG. Read-mostly
 reference table — source of truth is models/Oilseeds/us_oilseed_crushing_capacity.xlsm,
 refreshed via scripts/build_reference_oilseed_crush_facilities.py.';

GRANT SELECT ON reference.oilseed_crush_facilities TO PUBLIC;
