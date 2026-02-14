-- ============================================================================
-- Migration: 025_nass_cpc_gridded_layers.sql
-- Date: 2026-02-12
-- Description: Create bronze layer for NASS Crop Progress & Condition
--              Gridded Layers (weekly GeoTIFF rasters, 9km resolution).
--
--   1. bronze.cpc_series_catalog — 8 series (4 crops x 2 products)
--   2. bronze.cpc_ingest_run — one row per collector run
--   3. bronze.cpc_file_manifest — one row per GeoTIFF file
--   4. bronze.cpc_region_stats — zonal statistics per region/week
--   5. Gold views for analytics
--   6. Seed data for the 8 series
-- ============================================================================

-- ============================================================================
-- 1. SERIES CATALOG
-- ============================================================================

CREATE TABLE IF NOT EXISTS bronze.cpc_series_catalog (
    series_id       TEXT PRIMARY KEY,
    product         TEXT NOT NULL CHECK (product IN ('condition', 'progress')),
    crop            TEXT NOT NULL CHECK (crop IN ('corn', 'soybeans', 'cotton', 'winter_wheat')),
    units           TEXT NOT NULL,
    valid_min       FLOAT NOT NULL,
    valid_max       FLOAT NOT NULL,
    resolution_m    INTEGER NOT NULL DEFAULT 9000,
    crs_name        TEXT NOT NULL DEFAULT 'NAD 1983 Contiguous USA Albers',
    nodata_value    FLOAT NOT NULL DEFAULT -9999,
    coverage_start_year INTEGER NOT NULL DEFAULT 2015,
    source_url      TEXT DEFAULT 'https://www.nass.usda.gov/Research_and_Science/Crop_Progress_Gridded_Layers/',
    dataset_version TEXT DEFAULT 'v1',
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    updated_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_cpc_series_crop ON bronze.cpc_series_catalog(crop);
CREATE INDEX IF NOT EXISTS idx_cpc_series_product ON bronze.cpc_series_catalog(product);

-- ============================================================================
-- 2. INGEST RUN
-- ============================================================================

CREATE TABLE IF NOT EXISTS bronze.cpc_ingest_run (
    ingest_run_id   UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    run_ts_utc      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    collector_version TEXT,
    source          TEXT DEFAULT 'local' CHECK (source IN ('local', 'web')),
    years_processed TEXT,
    crops_processed TEXT,
    files_added     INTEGER DEFAULT 0,
    files_updated   INTEGER DEFAULT 0,
    files_skipped   INTEGER DEFAULT 0,
    qa_failures     INTEGER DEFAULT 0,
    qa_alerts       TEXT,
    duration_sec    FLOAT
);

-- ============================================================================
-- 3. FILE MANIFEST
-- ============================================================================

CREATE TABLE IF NOT EXISTS bronze.cpc_file_manifest (
    id              SERIAL PRIMARY KEY,
    file_sha256     TEXT NOT NULL,
    series_id       TEXT NOT NULL REFERENCES bronze.cpc_series_catalog(series_id),
    ingest_run_id   UUID REFERENCES bronze.cpc_ingest_run(ingest_run_id),

    -- Temporal
    year            INTEGER NOT NULL,
    nass_week       INTEGER NOT NULL,
    week_ending_date DATE,

    -- File info
    file_path       TEXT NOT NULL,
    file_name       TEXT NOT NULL,
    file_bytes      BIGINT,
    modified_utc    TIMESTAMPTZ,

    -- Raster metadata
    crs_wkt         TEXT,
    pixel_size_m    FLOAT,
    dtype           TEXT,
    nodata_value    FLOAT,
    width           INTEGER,
    height          INTEGER,
    bbox_xmin       FLOAT,
    bbox_ymin       FLOAT,
    bbox_xmax       FLOAT,
    bbox_ymax       FLOAT,

    -- Statistics
    value_min       FLOAT,
    value_max       FLOAT,
    value_mean      FLOAT,
    pct_nodata      FLOAT,

    -- QA
    qa_passed       BOOLEAN DEFAULT TRUE,
    qa_notes        TEXT,

    -- Metadata
    collected_at    TIMESTAMPTZ DEFAULT NOW(),

    CONSTRAINT uq_cpc_manifest_series_week UNIQUE (series_id, year, nass_week)
);

CREATE INDEX IF NOT EXISTS idx_cpc_manifest_sha256 ON bronze.cpc_file_manifest(file_sha256);
CREATE INDEX IF NOT EXISTS idx_cpc_manifest_series ON bronze.cpc_file_manifest(series_id);
CREATE INDEX IF NOT EXISTS idx_cpc_manifest_year ON bronze.cpc_file_manifest(year);
CREATE INDEX IF NOT EXISTS idx_cpc_manifest_week_date ON bronze.cpc_file_manifest(week_ending_date);

-- ============================================================================
-- 4. REGION STATS
-- ============================================================================

CREATE TABLE IF NOT EXISTS bronze.cpc_region_stats (
    id              SERIAL PRIMARY KEY,
    series_id       TEXT NOT NULL REFERENCES bronze.cpc_series_catalog(series_id),
    year            INTEGER NOT NULL,
    nass_week       INTEGER NOT NULL,
    week_ending_date DATE,
    region_id       TEXT NOT NULL,
    region_type     TEXT NOT NULL DEFAULT 'national' CHECK (region_type IN ('national', 'state', 'custom')),
    stat_name       TEXT NOT NULL,
    value           FLOAT,
    pixel_count     INTEGER,
    mask_version    TEXT,
    ingest_run_id   UUID REFERENCES bronze.cpc_ingest_run(ingest_run_id),
    collected_at    TIMESTAMPTZ DEFAULT NOW(),

    CONSTRAINT uq_cpc_region_stat UNIQUE (series_id, year, nass_week, region_id, stat_name)
);

CREATE INDEX IF NOT EXISTS idx_cpc_region_series ON bronze.cpc_region_stats(series_id);
CREATE INDEX IF NOT EXISTS idx_cpc_region_week ON bronze.cpc_region_stats(week_ending_date);
CREATE INDEX IF NOT EXISTS idx_cpc_region_id ON bronze.cpc_region_stats(region_id);

-- ============================================================================
-- 5. GOLD VIEWS
-- ============================================================================

-- Weekly condition index by crop and region (pivoted national stats)
CREATE OR REPLACE VIEW gold.cpc_condition_weekly AS
SELECT
    sc.crop,
    rs.region_id,
    rs.region_type,
    rs.year,
    rs.nass_week,
    rs.week_ending_date,
    MAX(CASE WHEN rs.stat_name = 'mean' THEN rs.value END) AS condition_mean,
    MAX(CASE WHEN rs.stat_name = 'median' THEN rs.value END) AS condition_median,
    MAX(CASE WHEN rs.stat_name = 'p10' THEN rs.value END) AS condition_p10,
    MAX(CASE WHEN rs.stat_name = 'p90' THEN rs.value END) AS condition_p90,
    MAX(CASE WHEN rs.stat_name = 'pct_nodata' THEN rs.value END) AS pct_nodata,
    MAX(CASE WHEN rs.stat_name = 'pixel_count' THEN rs.value END)::INTEGER AS pixel_count
FROM bronze.cpc_region_stats rs
JOIN bronze.cpc_series_catalog sc ON rs.series_id = sc.series_id
WHERE sc.product = 'condition'
GROUP BY sc.crop, rs.region_id, rs.region_type, rs.year, rs.nass_week, rs.week_ending_date
ORDER BY sc.crop, rs.region_id, rs.year, rs.nass_week;

-- Weekly progress index by crop and region
CREATE OR REPLACE VIEW gold.cpc_progress_weekly AS
SELECT
    sc.crop,
    rs.region_id,
    rs.region_type,
    rs.year,
    rs.nass_week,
    rs.week_ending_date,
    MAX(CASE WHEN rs.stat_name = 'mean' THEN rs.value END) AS progress_mean,
    MAX(CASE WHEN rs.stat_name = 'median' THEN rs.value END) AS progress_median,
    MAX(CASE WHEN rs.stat_name = 'p10' THEN rs.value END) AS progress_p10,
    MAX(CASE WHEN rs.stat_name = 'p90' THEN rs.value END) AS progress_p90,
    MAX(CASE WHEN rs.stat_name = 'pct_nodata' THEN rs.value END) AS pct_nodata,
    MAX(CASE WHEN rs.stat_name = 'pixel_count' THEN rs.value END)::INTEGER AS pixel_count
FROM bronze.cpc_region_stats rs
JOIN bronze.cpc_series_catalog sc ON rs.series_id = sc.series_id
WHERE sc.product = 'progress'
GROUP BY sc.crop, rs.region_id, rs.region_type, rs.year, rs.nass_week, rs.week_ending_date
ORDER BY sc.crop, rs.region_id, rs.year, rs.nass_week;

-- Year-over-year condition comparison
CREATE OR REPLACE VIEW gold.cpc_condition_yoy AS
WITH current_year AS (
    SELECT crop, region_id, nass_week, week_ending_date, condition_mean, year
    FROM gold.cpc_condition_weekly
    WHERE year = EXTRACT(YEAR FROM CURRENT_DATE)
),
prior_year AS (
    SELECT crop, region_id, nass_week, condition_mean AS prior_year_mean, year
    FROM gold.cpc_condition_weekly
    WHERE year = EXTRACT(YEAR FROM CURRENT_DATE) - 1
),
five_year_avg AS (
    SELECT crop, region_id, nass_week,
           AVG(condition_mean) AS avg_5yr_mean,
           MIN(condition_mean) AS min_5yr,
           MAX(condition_mean) AS max_5yr
    FROM gold.cpc_condition_weekly
    WHERE year BETWEEN EXTRACT(YEAR FROM CURRENT_DATE) - 5 AND EXTRACT(YEAR FROM CURRENT_DATE) - 1
    GROUP BY crop, region_id, nass_week
)
SELECT
    cy.crop,
    cy.region_id,
    cy.year,
    cy.nass_week,
    cy.week_ending_date,
    cy.condition_mean AS current_mean,
    py.prior_year_mean,
    fa.avg_5yr_mean,
    fa.min_5yr AS five_yr_min,
    fa.max_5yr AS five_yr_max,
    cy.condition_mean - py.prior_year_mean AS yoy_change,
    cy.condition_mean - fa.avg_5yr_mean AS vs_5yr_avg
FROM current_year cy
LEFT JOIN prior_year py ON cy.crop = py.crop AND cy.region_id = py.region_id AND cy.nass_week = py.nass_week
LEFT JOIN five_year_avg fa ON cy.crop = fa.crop AND cy.region_id = fa.region_id AND cy.nass_week = fa.nass_week
ORDER BY cy.crop, cy.region_id, cy.nass_week;

-- ============================================================================
-- 6. SEED DATA — 8 series (4 crops x 2 products)
-- ============================================================================

INSERT INTO bronze.cpc_series_catalog (series_id, product, crop, units, valid_min, valid_max, resolution_m, crs_name, nodata_value, coverage_start_year, dataset_version)
VALUES
    ('cpc_cond_corn_9km_v1',         'condition', 'corn',         'index_1_5', 1.0, 5.0, 9000, 'NAD 1983 Contiguous USA Albers', -9999, 2015, 'v1'),
    ('cpc_cond_soybeans_9km_v1',     'condition', 'soybeans',     'index_1_5', 1.0, 5.0, 9000, 'NAD 1983 Contiguous USA Albers', -9999, 2015, 'v1'),
    ('cpc_cond_cotton_9km_v1',       'condition', 'cotton',       'index_1_5', 1.0, 5.0, 9000, 'NAD 1983 Contiguous USA Albers', -9999, 2022, 'v1'),
    ('cpc_cond_winter_wheat_9km_v1', 'condition', 'winter_wheat', 'index_1_5', 1.0, 5.0, 9000, 'NAD 1983 Contiguous USA Albers', -9999, 2015, 'v1'),
    ('cpc_prog_corn_9km_v1',         'progress',  'corn',         'index_0_1', 0.0, 1.0, 9000, 'NAD 1983 Contiguous USA Albers', -9999, 2015, 'v1'),
    ('cpc_prog_soybeans_9km_v1',     'progress',  'soybeans',     'index_0_1', 0.0, 1.0, 9000, 'NAD 1983 Contiguous USA Albers', -9999, 2015, 'v1'),
    ('cpc_prog_cotton_9km_v1',       'progress',  'cotton',       'index_0_1', 0.0, 1.0, 9000, 'NAD 1983 Contiguous USA Albers', -9999, 2022, 'v1'),
    ('cpc_prog_winter_wheat_9km_v1', 'progress',  'winter_wheat', 'index_0_1', 0.0, 1.0, 9000, 'NAD 1983 Contiguous USA Albers', -9999, 2015, 'v1')
ON CONFLICT (series_id) DO NOTHING;
