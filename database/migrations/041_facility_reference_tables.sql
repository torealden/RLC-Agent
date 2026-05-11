-- ============================================================================
-- Migration 041: Multi-type facility reference tables
-- ============================================================================
-- Per Tore convention 2026-04-27:
--   - Existing reference.oilseed_crush_facilities stays as our master roster;
--     incoming third-party lists (Bob, World Crushing Plants, etc.) only ADD
--     facilities we don't already have, never overwrite our enrichments.
--   - Each facility TYPE gets its own table (biodiesel, RD, ethanol,
--     beef/pork slaughter, oil refining), structurally similar but with
--     type-specific columns.
--   - reference.all_facilities is a UNION view across all tables.
--   - All imports tagged with data_source + verified_at = NULL so future
--     capacity work can flag for verification.
-- ============================================================================

-- ---------------------------------------------------------------------------
-- 0. Extend oilseed_crush_facilities with provenance columns
-- ---------------------------------------------------------------------------
ALTER TABLE reference.oilseed_crush_facilities
    ADD COLUMN IF NOT EXISTS data_source        TEXT,
    ADD COLUMN IF NOT EXISTS verified_at         TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS verified_by         TEXT,
    ADD COLUMN IF NOT EXISTS verification_method TEXT,
    ADD COLUMN IF NOT EXISTS country             CHAR(2) DEFAULT 'US';

-- ---------------------------------------------------------------------------
-- 1. Common helper: facility table template via macro-like approach
-- ---------------------------------------------------------------------------
-- Rather than DRY via stored procedure, we declare each table independently
-- so unique columns per facility type can be added cleanly.

-- ---------------------------------------------------------------------------
-- 2. reference.biodiesel_facilities
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS reference.biodiesel_facilities (
    facility_id              TEXT PRIMARY KEY,           -- e.g., 'us.bd.reg_seneca'
    name                     TEXT NOT NULL,
    operator                 TEXT,
    parent_company           TEXT,
    address                  TEXT,
    city                     TEXT,
    county                   TEXT,
    state                    CHAR(2),
    zip                      TEXT,
    country                  CHAR(2) DEFAULT 'US',
    lat                      NUMERIC(9,6),
    lon                      NUMERIC(9,6),
    nameplate_mmgy           NUMERIC(8,2),               -- million gallons per year
    feedstock_primary        TEXT,                       -- soybean_oil / uco / tallow / multi
    epa_rfs_rin_id           TEXT,
    eia_plant_id             TEXT,
    status                   TEXT DEFAULT 'Operating',
    commissioned_year        INTEGER,
    contact_info             JSONB,
    data_source              TEXT,
    verified_at              TIMESTAMPTZ,
    notes                    TEXT,
    created_at               TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at               TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_biodiesel_state ON reference.biodiesel_facilities (state);

-- ---------------------------------------------------------------------------
-- 3. reference.renewable_diesel_facilities
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS reference.renewable_diesel_facilities (
    facility_id              TEXT PRIMARY KEY,
    name                     TEXT NOT NULL,
    operator                 TEXT,
    parent_company           TEXT,
    project_partners         TEXT,                       -- e.g., 'Marathon-Neste JV'
    city                     TEXT,
    state                    CHAR(2),
    country                  CHAR(2) DEFAULT 'US',
    lat                      NUMERIC(9,6),
    lon                      NUMERIC(9,6),
    nameplate_mmgy           NUMERIC(10,2),
    feedstock_primary        TEXT,
    feedstock_secondary      TEXT,
    technology               TEXT DEFAULT 'HEFA',        -- HEFA / Co-processing / Fischer-Tropsch
    status                   TEXT DEFAULT 'Operating',   -- Operating / Construction / Announced / Cancelled
    start_date               DATE,
    air_permit               TEXT,
    epa_rfs_rin_id           TEXT,
    eia_plant_id             TEXT,
    capacity_jacobsen_mmgy   NUMERIC(10,2),              -- comparison source
    capacity_jpmorgan_mmgy   NUMERIC(10,2),              -- comparison source
    notes                    TEXT,
    data_source              TEXT,
    verified_at              TIMESTAMPTZ,
    created_at               TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at               TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_rd_state ON reference.renewable_diesel_facilities (state);
CREATE INDEX IF NOT EXISTS idx_rd_status ON reference.renewable_diesel_facilities (status);

-- ---------------------------------------------------------------------------
-- 4. reference.ethanol_facilities
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS reference.ethanol_facilities (
    facility_id              TEXT PRIMARY KEY,
    name                     TEXT NOT NULL,
    operator                 TEXT,
    city                     TEXT,
    state                    CHAR(2),
    zip                      TEXT,
    country                  CHAR(2) DEFAULT 'US',
    lat                      NUMERIC(9,6),
    lon                      NUMERIC(9,6),
    nameplate_mmgy           NUMERIC(8,2),
    feedstock                TEXT DEFAULT 'Corn',        -- Corn / Sorghum / Cellulosic
    rin_d_code               TEXT,                       -- D6 / D5 / D3
    process_type             TEXT,                       -- Dry mill / Wet mill
    epa_rfs_rin_id           TEXT,
    status                   TEXT DEFAULT 'Operating',
    commissioned_year        INTEGER,
    notes                    TEXT,
    data_source              TEXT,
    verified_at              TIMESTAMPTZ,
    created_at               TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at               TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_ethanol_state ON reference.ethanol_facilities (state);

-- ---------------------------------------------------------------------------
-- 5. reference.beef_slaughter_facilities + pork
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS reference.beef_slaughter_facilities (
    facility_id              TEXT PRIMARY KEY,
    name                     TEXT NOT NULL,
    operator                 TEXT,
    parent_company           TEXT,
    city                     TEXT,
    state                    CHAR(2),
    zip                      TEXT,
    country                  CHAR(2) DEFAULT 'US',
    lat                      NUMERIC(9,6),
    lon                      NUMERIC(9,6),
    capacity_head_per_day    INTEGER,
    has_rail                 BOOLEAN,
    implied_bft_lbs_year     NUMERIC(12,0),              -- beef tallow yield, lbs/yr
    implied_edible_tech_lbs_year NUMERIC(12,0),
    status                   TEXT DEFAULT 'Operating',
    notes                    TEXT,
    data_source              TEXT,
    verified_at              TIMESTAMPTZ,
    created_at               TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at               TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_beef_state ON reference.beef_slaughter_facilities (state);

CREATE TABLE IF NOT EXISTS reference.pork_slaughter_facilities (
    facility_id              TEXT PRIMARY KEY,
    name                     TEXT NOT NULL,
    operator                 TEXT,
    parent_company           TEXT,
    city                     TEXT,
    state                    CHAR(2),
    zip                      TEXT,
    country                  CHAR(2) DEFAULT 'US',
    lat                      NUMERIC(9,6),
    lon                      NUMERIC(9,6),
    capacity_head_per_day    INTEGER,
    has_rail                 BOOLEAN,
    implied_lard_lbs_year    NUMERIC(12,0),
    implied_cwg_lbs_year     NUMERIC(12,0),
    status                   TEXT DEFAULT 'Operating',
    notes                    TEXT,
    data_source              TEXT,
    verified_at              TIMESTAMPTZ,
    created_at               TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at               TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_pork_state ON reference.pork_slaughter_facilities (state);

-- ---------------------------------------------------------------------------
-- 6. reference.oil_refining_facilities (vegetable oil refiners, distinct from crushers)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS reference.oil_refining_facilities (
    facility_id              TEXT PRIMARY KEY,
    name                     TEXT NOT NULL,
    operator                 TEXT,
    city                     TEXT,
    state                    CHAR(2),
    country                  CHAR(2) DEFAULT 'US',
    lat                      NUMERIC(9,6),
    lon                      NUMERIC(9,6),
    refining_capacity_lbs_year NUMERIC(14,0),
    refining_type            TEXT,                       -- Crude / Degumming / RB / RBD
    primary_oil              TEXT,                       -- soybean / canola / palm / etc.
    co_located_crusher_id    TEXT REFERENCES reference.oilseed_crush_facilities(facility_id),
    status                   TEXT DEFAULT 'Operating',
    notes                    TEXT,
    data_source              TEXT,
    verified_at              TIMESTAMPTZ,
    created_at               TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at               TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_oil_refining_state ON reference.oil_refining_facilities (state);

-- ---------------------------------------------------------------------------
-- 7. bronze.rd_capacity_projection — Bob's time-series projection
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS bronze.rd_capacity_projection (
    id                       BIGSERIAL PRIMARY KEY,
    source_label             TEXT NOT NULL,              -- 'bob_2021' / 'jacobsen_2020' / etc.
    facility_name_raw        TEXT NOT NULL,              -- as given in source
    facility_id              TEXT,                       -- nullable; filled when matched to reference
    scenario                 TEXT,                       -- 'Domestic' / 'Rumored' / 'Co-Processing'
    year                     INTEGER NOT NULL,
    projected_capacity_gal   NUMERIC(12,0),              -- gallons per year
    projected_capacity_mmgy  NUMERIC(8,2)                -- million gallons per year (convenience)
                             GENERATED ALWAYS AS (projected_capacity_gal / 1e6) STORED,
    notes                    TEXT,
    ingested_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (source_label, facility_name_raw, year, scenario)
);
CREATE INDEX IF NOT EXISTS idx_rd_proj_year ON bronze.rd_capacity_projection (year);
CREATE INDEX IF NOT EXISTS idx_rd_proj_facility ON bronze.rd_capacity_projection (facility_id);

-- ---------------------------------------------------------------------------
-- 8. reference.all_facilities — UNION view across all types
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW reference.all_facilities AS
SELECT
    facility_id, name, operator, city, state, country, lat, lon,
    nameplate_bpd::NUMERIC          AS capacity_value,
    'bushels_per_day'                AS capacity_unit,
    'oilseed_crusher'                AS facility_type,
    primary_oilseed                  AS process_input,
    status, data_source, verified_at, updated_at
FROM reference.oilseed_crush_facilities
UNION ALL
SELECT facility_id, name, operator, city, state, country, lat, lon,
       nameplate_mmgy, 'mil_gal_per_year', 'biodiesel',
       feedstock_primary, status, data_source, verified_at, updated_at
FROM reference.biodiesel_facilities
UNION ALL
SELECT facility_id, name, operator, city, state, country, lat, lon,
       nameplate_mmgy, 'mil_gal_per_year', 'renewable_diesel',
       feedstock_primary, status, data_source, verified_at, updated_at
FROM reference.renewable_diesel_facilities
UNION ALL
SELECT facility_id, name, operator, city, state, country, lat, lon,
       nameplate_mmgy, 'mil_gal_per_year', 'ethanol',
       feedstock, status, data_source, verified_at, updated_at
FROM reference.ethanol_facilities
UNION ALL
SELECT facility_id, name, operator, city, state, country, lat, lon,
       capacity_head_per_day, 'head_per_day', 'beef_slaughter',
       'cattle', status, data_source, verified_at, updated_at
FROM reference.beef_slaughter_facilities
UNION ALL
SELECT facility_id, name, operator, city, state, country, lat, lon,
       capacity_head_per_day, 'head_per_day', 'pork_slaughter',
       'hogs', status, data_source, verified_at, updated_at
FROM reference.pork_slaughter_facilities
UNION ALL
SELECT facility_id, name, operator, city, state, country, lat, lon,
       refining_capacity_lbs_year, 'lbs_per_year', 'oil_refiner',
       primary_oil, status, data_source, verified_at, updated_at
FROM reference.oil_refining_facilities;

COMMENT ON VIEW reference.all_facilities IS
'Unified view across all facility-type tables. Use facility_type to filter. Capacity values are in their native unit per facility type — see capacity_unit column.';

GRANT SELECT ON reference.biodiesel_facilities         TO PUBLIC;
GRANT SELECT ON reference.renewable_diesel_facilities  TO PUBLIC;
GRANT SELECT ON reference.ethanol_facilities           TO PUBLIC;
GRANT SELECT ON reference.beef_slaughter_facilities    TO PUBLIC;
GRANT SELECT ON reference.pork_slaughter_facilities    TO PUBLIC;
GRANT SELECT ON reference.oil_refining_facilities      TO PUBLIC;
GRANT SELECT ON bronze.rd_capacity_projection          TO PUBLIC;
GRANT SELECT ON reference.all_facilities                TO PUBLIC;
