-- ============================================================================
-- Migration 134: bronze.ers_oil_crops_yearbook
-- ============================================================================
-- Full ingest of the USDA ERS Oil Crops Yearbook normalized CSV
-- (OilCropsAllTables.csv, 43 tables, ~30K rows). Source of pre-NASS-horizon
-- history: NASS QuickStats fats & oils only goes back to 2014/15; the
-- Yearbook carries annual series to 1980/81 and, critically, MONTHLY series:
--   Table 6 - soybean crush by month,        2000/01+  (thousand bushels)
--   Table 7 - soybean meal S&D by month,     2007/08+  (thousand short tons)
--   Table 8 - soybean oil S&D by month,      2007/08+  (thousand pounds)
-- These backfill the gap months in us_oilseed_crush.xlsm that currently rely
-- on Tore's estimates (gray #EAEAEA cells, see rescale_oilseed_crush_legacy_cells).
--
-- Monthly rows are mapped into silver.monthly_realized with source='ERS_OCY'
-- by scripts/ingest_oil_crops_yearbook.py. NASS rows are never overwritten:
-- the unique key on monthly_realized includes source.
--
-- Empty Amount cells in the CSV (USDA suppression / not available) load as
-- NULL amount rather than being dropped, so suppression is queryable.
-- ============================================================================

CREATE TABLE IF NOT EXISTS bronze.ers_oil_crops_yearbook (
    id               BIGSERIAL PRIMARY KEY,
    table_number     INTEGER     NOT NULL,
    table_name       TEXT,
    timeperiod_desc  TEXT        NOT NULL,
    marketing_year   TEXT        NOT NULL,   -- as published: '1999/00' or '1980'
    my_start_year    INTEGER,                -- parsed: 1999
    my_definition    TEXT,                   -- 'September-August', 'October-September', ...
    commodity_group  TEXT,
    commodity        TEXT        NOT NULL,
    commodity_desc2  TEXT        NOT NULL DEFAULT '',
    attribute_desc   TEXT        NOT NULL,
    attribute_desc2  TEXT        NOT NULL DEFAULT '',
    geography_desc   TEXT        NOT NULL DEFAULT '',
    geography_desc2  TEXT        NOT NULL DEFAULT '',
    amount           NUMERIC,                -- NULL = suppressed / not published
    unit_desc        TEXT,
    source_file      TEXT,
    ingested_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (table_number, marketing_year, timeperiod_desc, commodity,
            commodity_desc2, attribute_desc, attribute_desc2,
            geography_desc, geography_desc2)
);

CREATE INDEX IF NOT EXISTS idx_ers_ocy_commodity_attr
    ON bronze.ers_oil_crops_yearbook (commodity, attribute_desc, my_start_year);

CREATE INDEX IF NOT EXISTS idx_ers_ocy_table
    ON bronze.ers_oil_crops_yearbook (table_number, my_start_year);

COMMENT ON TABLE bronze.ers_oil_crops_yearbook IS
    'USDA ERS Oil Crops Yearbook, all tables, normalized long format. Loaded by scripts/ingest_oil_crops_yearbook.py from data/raw/oilseeds_fats_greases/OilCropsAllTables.csv';
