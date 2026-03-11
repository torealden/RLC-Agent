-- Migration 029: Tier 1 Collector Tables
-- ComexStat Brazil trade, INDEC Argentina trade, FGIS Export Inspections
-- Created: 2026-03-10

-- =====================================================
-- 1. ComexStat Brazil Trade (monthly export/import)
-- =====================================================
CREATE TABLE IF NOT EXISTS bronze.comexstat_trade (
    id              SERIAL PRIMARY KEY,
    flow            TEXT NOT NULL,           -- 'export' or 'import'
    year            INTEGER NOT NULL,
    month           INTEGER,
    ncm_code        TEXT NOT NULL,           -- 8-digit NCM code
    ncm_description TEXT,
    heading_code    TEXT,                    -- 4-digit SH4
    commodity       TEXT,                    -- normalized name (soybeans, corn, etc.)
    country         TEXT,                    -- partner country name
    state           TEXT,                    -- Brazilian state of origin
    fob_usd         BIGINT,                 -- FOB value in USD
    weight_kg       BIGINT,                 -- net weight in kg
    cif_usd         BIGINT,                 -- CIF (imports only)
    freight_usd     BIGINT,                 -- freight (imports only)
    source          TEXT DEFAULT 'ComexStat',
    collected_at    TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (flow, year, month, ncm_code, country, state)
);

CREATE INDEX IF NOT EXISTS idx_comexstat_commodity ON bronze.comexstat_trade (commodity, year, month);
CREATE INDEX IF NOT EXISTS idx_comexstat_flow_year ON bronze.comexstat_trade (flow, year);
CREATE INDEX IF NOT EXISTS idx_comexstat_country ON bronze.comexstat_trade (country, year);

-- =====================================================
-- 2. INDEC Argentina Trade (monthly export/import)
-- =====================================================
CREATE TABLE IF NOT EXISTS bronze.indec_trade (
    id              SERIAL PRIMARY KEY,
    flow            TEXT NOT NULL,           -- 'export' or 'import'
    year            INTEGER NOT NULL,
    month           INTEGER NOT NULL,
    ncm_code        TEXT NOT NULL,           -- 8-digit NCM code
    ncm_description TEXT,
    commodity       TEXT,                    -- normalized name
    country_code    TEXT,                    -- INDEC country code
    country_name    TEXT,                    -- country name (Spanish)
    weight_kg       NUMERIC,                -- net weight in kg
    fob_usd         NUMERIC,                -- FOB value in USD
    cif_usd         NUMERIC,                -- CIF (imports only)
    freight_usd     NUMERIC,                -- freight (imports only)
    insurance_usd   NUMERIC,                -- insurance (imports only)
    ica_report_id   TEXT,                    -- ICA report identifier
    source          TEXT DEFAULT 'INDEC',
    collected_at    TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (flow, year, month, ncm_code, country_code)
);

CREATE INDEX IF NOT EXISTS idx_indec_commodity ON bronze.indec_trade (commodity, year, month);
CREATE INDEX IF NOT EXISTS idx_indec_flow_year ON bronze.indec_trade (flow, year);

-- =====================================================
-- 3. FGIS Export Inspections (weekly grain inspections)
-- =====================================================
CREATE TABLE IF NOT EXISTS bronze.fgis_inspections (
    id              SERIAL PRIMARY KEY,
    inspection_date DATE NOT NULL,
    week            INTEGER,
    month           INTEGER,
    year            INTEGER NOT NULL,
    grain           TEXT NOT NULL,           -- CORN, SOYBEANS, WHEAT, etc.
    commodity       TEXT,                    -- normalized name
    grade           TEXT,
    grain_class     TEXT,                    -- YC, SRW, HRW, etc.
    destination     TEXT NOT NULL,           -- destination country
    port            TEXT,                    -- port area
    ams_region      TEXT,
    state           TEXT,                    -- US state
    metric_tons     NUMERIC,
    pounds          NUMERIC,
    carrier_type    TEXT,                    -- VESSEL, CONTAINER, etc.
    source          TEXT DEFAULT 'FGIS',
    collected_at    TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (inspection_date, grain, destination, port, grain_class, carrier_type, state)
);

CREATE INDEX IF NOT EXISTS idx_fgis_grain_date ON bronze.fgis_inspections (grain, inspection_date);
CREATE INDEX IF NOT EXISTS idx_fgis_dest ON bronze.fgis_inspections (destination, year);
CREATE INDEX IF NOT EXISTS idx_fgis_week ON bronze.fgis_inspections (year, week);
