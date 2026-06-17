-- Migration 139: bronze.corn_products_raw
-- =============================================================================
-- Raw landing for the ERS Sugar & Sweeteners Yearbook "U.S. corn sweetener
-- supply and use" workbook (HFCS-42/55, glucose, dextrose production). Long/
-- tidy, one observation per row. Vintage (source_release_date) retained so ERS
-- revisions never silently overwrite history (feeds forecast-accuracy work).
--
-- Corn-input-by-product (cols B-E of the corn_products tab) is NOT here -- that
-- already lives in bronze.ers_feed_grains_yearbook (Table 31).
--
-- product : hfcs_42 | hfcs_55 | hfcs_total | glucose | dextrose
-- measure : production | imports | exports | total_use | stocks_change | total_supply
-- period_type : annual_calendar | annual_fiscal | quarterly
-- =============================================================================

CREATE TABLE IF NOT EXISTS bronze.corn_products_raw (
    id                  BIGSERIAL PRIMARY KEY,
    source              TEXT NOT NULL DEFAULT 'ERS_SUGAR_SWEETENERS',
    source_url          TEXT,
    source_release_date DATE,
    source_table        TEXT NOT NULL,        -- e.g. 'Table 30'
    product             TEXT NOT NULL,
    measure             TEXT NOT NULL,
    period_type         TEXT NOT NULL,
    period_label        TEXT,                 -- e.g. 'Q1 (Jan-Mar)', 'Calendar year'
    year                INTEGER NOT NULL,
    quarter             INTEGER,              -- 1-4 for quarterly rows, else NULL
    raw_value           NUMERIC,
    raw_unit            TEXT DEFAULT '1000 short tons, dry',
    vintage             DATE,                 -- = source_release_date (forecast tracking)
    pull_ts             TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Expression-based unique index (COALESCE not allowed in a table UNIQUE
-- constraint). The connector's ON CONFLICT targets this exact expression set.
CREATE UNIQUE INDEX IF NOT EXISTS corn_products_raw_uniq
    ON bronze.corn_products_raw
       (source_table, product, measure, period_type, year,
        COALESCE(quarter, 0), COALESCE(vintage, '1900-01-01'));

CREATE INDEX IF NOT EXISTS idx_corn_products_raw_prod
    ON bronze.corn_products_raw (product, measure, period_type);
CREATE INDEX IF NOT EXISTS idx_corn_products_raw_year
    ON bronze.corn_products_raw (year);
