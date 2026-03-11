-- Migration 031: Statistics Canada Bronze Table
-- Field crop production, grain stocks, farm prices
-- Created: 2026-03-11

CREATE TABLE IF NOT EXISTS bronze.canada_statscan (
    id              SERIAL PRIMARY KEY,
    table_key       TEXT NOT NULL,            -- e.g. 'field_crop_production', 'grain_stocks'
    product_id      TEXT NOT NULL,            -- StatsCan product ID e.g. '32100359'
    ref_date        DATE NOT NULL,            -- reference period (first day)
    ref_date_raw    TEXT,                     -- original REF_DATE string ('2024', '2024-03')
    geo             TEXT,                     -- geography name ('Canada', 'Saskatchewan', etc.)
    province_code   TEXT,                     -- normalised 2-letter code (CA, SK, AB, etc.)
    commodity       TEXT,                     -- normalised commodity name (wheat, canola, etc.)
    attribute       TEXT,                     -- dimension label(s) e.g. 'Production (metric tonnes) | Canola (rapeseed)'
    uom             TEXT,                     -- unit of measure
    scalar_factor   TEXT,                     -- 'units', 'thousands', 'millions'
    value           DOUBLE PRECISION,         -- numeric value
    vector          TEXT,                     -- StatsCan vector ID e.g. 'v46457'
    coordinate      TEXT,                     -- StatsCan coordinate string
    status          TEXT,                     -- data quality flag ('' = OK, '..' = not available)
    decimals        INTEGER DEFAULT 0,
    source          TEXT DEFAULT 'STATSCAN',
    collected_at    TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (product_id, vector, ref_date)
);

-- Primary query patterns
CREATE INDEX IF NOT EXISTS idx_statscan_commodity
    ON bronze.canada_statscan (commodity, ref_date);

CREATE INDEX IF NOT EXISTS idx_statscan_table_key
    ON bronze.canada_statscan (table_key, ref_date);

CREATE INDEX IF NOT EXISTS idx_statscan_province
    ON bronze.canada_statscan (province_code, commodity, ref_date);

CREATE INDEX IF NOT EXISTS idx_statscan_product_ref
    ON bronze.canada_statscan (product_id, ref_date);

-- =============================================================
-- Helper view: latest Canadian crop production (metric units)
-- =============================================================
CREATE OR REPLACE VIEW gold.canada_crop_production AS
SELECT
    ref_date,
    ref_date_raw,
    geo,
    province_code,
    commodity,
    attribute,
    uom,
    scalar_factor,
    value,
    collected_at
FROM bronze.canada_statscan
WHERE table_key = 'field_crop_production'
  AND attribute ILIKE '%production%metric%'
ORDER BY ref_date DESC, province_code, commodity;

-- =============================================================
-- Helper view: latest Canadian grain stocks
-- =============================================================
CREATE OR REPLACE VIEW gold.canada_grain_stocks AS
SELECT
    ref_date,
    ref_date_raw,
    geo,
    province_code,
    commodity,
    attribute,
    uom,
    scalar_factor,
    value,
    collected_at
FROM bronze.canada_statscan
WHERE table_key = 'grain_stocks'
ORDER BY ref_date DESC, province_code, commodity;
