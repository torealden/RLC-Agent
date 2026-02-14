-- ============================================================================
-- Migration: 026_yield_model_tables.sql
-- Date: 2026-02-12
-- Description: Create tables and views for the yield forecast model.
--
--   1. bronze.nass_state_yields — Historical state-level crop yields
--   2. silver.yield_trend — Trend yield coefficients per state/crop
--   3. silver.yield_features — Weekly feature vectors (model input)
--   4. silver.yield_model_run — Model execution log
--   5. gold.yield_forecast — Yield projections (model output)
--   6. gold.yield_monitor — Weekly dashboard view
--   7. Ensure reference.crop_region exists (from schema 014)
--   8. Seed reference.weather_climatology with US crop region normals
-- ============================================================================

-- ============================================================================
-- 0. ENSURE PREREQUISITE SCHEMAS AND TABLES EXIST
-- ============================================================================

CREATE SCHEMA IF NOT EXISTS reference;

-- reference.crop_region (from 014_ndvi_schema.sql)
CREATE TABLE IF NOT EXISTS reference.crop_region (
    id SERIAL PRIMARY KEY,
    region_code VARCHAR(20) UNIQUE NOT NULL,
    region_name VARCHAR(100) NOT NULL,
    country_code VARCHAR(10) NOT NULL,
    primary_commodity VARCHAR(50),
    secondary_commodities TEXT[],
    bounding_box JSONB,
    states_provinces TEXT[],
    area_hectares NUMERIC(12,2),
    planting_start_month INT,
    planting_end_month INT,
    harvest_start_month INT,
    harvest_end_month INT,
    pct_national_production NUMERIC(5,2),
    usda_region_id VARCHAR(20),
    crop_explorer_id VARCHAR(20),
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

INSERT INTO reference.crop_region (region_code, region_name, country_code, primary_commodity, secondary_commodities, states_provinces, planting_start_month, harvest_start_month, pct_national_production)
VALUES
    ('US_CORN_BELT', 'US Corn Belt', 'US', 'corn', ARRAY['soybeans'], ARRAY['IA', 'IL', 'NE', 'MN', 'IN', 'OH', 'SD', 'WI', 'MO', 'KS'], 4, 9, 75.0),
    ('US_SOY_BELT', 'US Soybean Belt', 'US', 'soybeans', ARRAY['corn'], ARRAY['IL', 'IA', 'MN', 'IN', 'NE', 'OH', 'MO', 'SD', 'ND', 'AR'], 5, 10, 80.0),
    ('US_WHEAT_WINTER', 'US Winter Wheat Belt', 'US', 'wheat', ARRAY['sorghum'], ARRAY['KS', 'OK', 'TX', 'CO', 'NE', 'MT'], 9, 6, 45.0),
    ('US_WHEAT_SPRING', 'US Spring Wheat Belt', 'US', 'wheat', ARRAY['barley'], ARRAY['ND', 'MT', 'MN', 'SD'], 4, 8, 25.0),
    ('BR_MATO_GROSSO', 'Brazil Mato Grosso', 'BR', 'soybeans', ARRAY['corn', 'cotton'], ARRAY['MT'], 9, 2, 28.0),
    ('BR_PARANA', 'Brazil Parana', 'BR', 'soybeans', ARRAY['corn', 'wheat'], ARRAY['PR'], 9, 3, 15.0),
    ('BR_RIO_GRANDE', 'Brazil Rio Grande do Sul', 'BR', 'soybeans', ARRAY['rice', 'wheat'], ARRAY['RS'], 10, 3, 12.0),
    ('BR_GOIAS', 'Brazil Goias', 'BR', 'soybeans', ARRAY['corn'], ARRAY['GO'], 9, 2, 10.0),
    ('AR_PAMPAS', 'Argentina Pampas', 'AR', 'soybeans', ARRAY['corn', 'wheat'], ARRAY['Buenos Aires', 'Santa Fe', 'Cordoba'], 10, 4, 85.0),
    ('UA_CENTRAL', 'Ukraine Central', 'UA', 'wheat', ARRAY['corn', 'sunflowerseed'], ARRAY['Kyiv', 'Poltava', 'Cherkasy'], 9, 7, 40.0),
    ('RU_SOUTHERN', 'Russia Southern District', 'RU', 'wheat', ARRAY['sunflowerseed', 'corn'], ARRAY['Krasnodar', 'Rostov', 'Stavropol'], 9, 7, 35.0),
    ('AU_EASTERN', 'Australia Eastern Wheat Belt', 'AU', 'wheat', ARRAY['barley'], ARRAY['NSW', 'QLD', 'VIC'], 5, 11, 60.0),
    ('IN_PUNJAB', 'India Punjab-Haryana', 'IN', 'wheat', ARRAY['rice'], ARRAY['Punjab', 'Haryana'], 11, 4, 35.0),
    ('CN_NORTHEAST', 'China Northeast', 'CN', 'corn', ARRAY['soybeans', 'rice'], ARRAY['Heilongjiang', 'Jilin', 'Liaoning'], 4, 9, 40.0)
ON CONFLICT (region_code) DO NOTHING;

-- reference.weather_climatology (from 015_weather_forecast_schema.sql)
CREATE TABLE IF NOT EXISTS reference.weather_climatology (
    id SERIAL PRIMARY KEY,
    region_code VARCHAR(30) NOT NULL,
    month INT NOT NULL,
    day_of_month INT,
    precip_normal_mm NUMERIC(8,2),
    tmin_normal_c NUMERIC(6,2),
    tmax_normal_c NUMERIC(6,2),
    gdd_normal NUMERIC(8,2),
    precip_stddev NUMERIC(8,2),
    temp_stddev NUMERIC(6,2),
    UNIQUE (region_code, month, day_of_month)
);

-- ============================================================================
-- 1. HISTORICAL STATE-LEVEL YIELDS (training target)
-- ============================================================================

CREATE TABLE IF NOT EXISTS bronze.nass_state_yields (
    id              SERIAL PRIMARY KEY,
    commodity       TEXT NOT NULL,
    state           TEXT NOT NULL,
    state_abbrev    TEXT,
    year            INTEGER NOT NULL,
    area_planted    FLOAT,
    area_harvested  FLOAT,
    yield_per_acre  FLOAT,
    production      FLOAT,
    yield_unit      TEXT DEFAULT 'bu/acre',
    production_unit TEXT DEFAULT '1000_bu',
    source          TEXT DEFAULT 'NASS_CROP_PRODUCTION',
    collected_at    TIMESTAMPTZ DEFAULT NOW(),

    CONSTRAINT uq_state_yield UNIQUE (commodity, state, year)
);

CREATE INDEX IF NOT EXISTS idx_state_yields_commodity ON bronze.nass_state_yields(commodity);
CREATE INDEX IF NOT EXISTS idx_state_yields_state ON bronze.nass_state_yields(state);
CREATE INDEX IF NOT EXISTS idx_state_yields_year ON bronze.nass_state_yields(year);

-- ============================================================================
-- 2. TREND YIELD COEFFICIENTS
-- ============================================================================

CREATE TABLE IF NOT EXISTS silver.yield_trend (
    id                  SERIAL PRIMARY KEY,
    commodity           TEXT NOT NULL,
    state               TEXT NOT NULL,
    trend_type          TEXT NOT NULL DEFAULT 'linear',
    intercept           FLOAT NOT NULL,
    slope               FLOAT NOT NULL,
    slope_quadratic     FLOAT,
    r_squared           FLOAT,
    years_used          TEXT,
    trend_yield_current FLOAT,
    updated_at          TIMESTAMPTZ DEFAULT NOW(),

    CONSTRAINT uq_yield_trend UNIQUE (commodity, state, trend_type)
);

-- ============================================================================
-- 3. WEEKLY FEATURE VECTORS (model input)
-- ============================================================================

CREATE TABLE IF NOT EXISTS silver.yield_features (
    id                   SERIAL PRIMARY KEY,
    state                TEXT NOT NULL,
    crop                 TEXT NOT NULL,
    year                 INTEGER NOT NULL,
    week                 INTEGER NOT NULL,
    week_ending_date     DATE,

    -- Weather features (cumulative from planting)
    gdd_cum              FLOAT,
    gdd_vs_normal_pct    FLOAT,
    precip_cum_mm        FLOAT,
    precip_vs_normal_pct FLOAT,
    stress_days_heat     INTEGER DEFAULT 0,
    stress_days_drought  INTEGER DEFAULT 0,
    excess_moisture_days INTEGER DEFAULT 0,
    frost_events         INTEGER DEFAULT 0,

    -- Temperature summary for current week
    tmax_weekly_avg      FLOAT,
    tmin_weekly_avg      FLOAT,
    tavg_weekly          FLOAT,

    -- NDVI / vegetation features
    ndvi_mean            FLOAT,
    ndvi_anomaly         FLOAT,
    ndvi_trend_4wk       FLOAT,

    -- CPC gridded condition/progress
    condition_index      FLOAT,
    condition_vs_5yr     FLOAT,
    progress_index       FLOAT,
    progress_vs_normal   FLOAT,

    -- NASS tabular crop status
    pct_planted          FLOAT,
    pct_emerged          FLOAT,
    pct_silking          FLOAT,
    pct_dough            FLOAT,
    pct_mature           FLOAT,
    pct_harvested        FLOAT,
    good_excellent_pct   FLOAT,

    -- World Weather signals
    ww_risk_score        FLOAT,
    ww_outlook_sentiment FLOAT,

    -- Growth stage
    growth_stage         TEXT,

    -- Metadata
    feature_version      TEXT DEFAULT 'v1',
    updated_at           TIMESTAMPTZ DEFAULT NOW(),

    CONSTRAINT uq_yield_features UNIQUE (state, crop, year, week)
);

CREATE INDEX IF NOT EXISTS idx_yf_crop_year ON silver.yield_features(crop, year);
CREATE INDEX IF NOT EXISTS idx_yf_state_crop ON silver.yield_features(state, crop);
CREATE INDEX IF NOT EXISTS idx_yf_week_date ON silver.yield_features(week_ending_date);

-- ============================================================================
-- 4. MODEL EXECUTION LOG
-- ============================================================================

CREATE TABLE IF NOT EXISTS silver.yield_model_run (
    run_id           UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    run_ts           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    model_version    TEXT NOT NULL,
    model_type       TEXT NOT NULL,
    crops_processed  TEXT,
    states_processed TEXT,
    forecast_week    INTEGER,
    training_years   TEXT,
    feature_count    INTEGER,
    rmse_cv          FLOAT,
    mae_cv           FLOAT,
    r2_cv            FLOAT,
    notes            TEXT,
    duration_sec     FLOAT
);

-- ============================================================================
-- 5. YIELD PROJECTIONS (model output)
-- ============================================================================

CREATE TABLE IF NOT EXISTS gold.yield_forecast (
    id                      SERIAL PRIMARY KEY,
    run_id                  UUID REFERENCES silver.yield_model_run(run_id),
    commodity               TEXT NOT NULL,
    state                   TEXT NOT NULL,
    year                    INTEGER NOT NULL,
    forecast_week           INTEGER NOT NULL,
    forecast_date           DATE NOT NULL,

    yield_forecast          FLOAT NOT NULL,
    yield_low               FLOAT,
    yield_high              FLOAT,

    trend_yield             FLOAT,
    vs_trend_pct            FLOAT,
    last_year_yield         FLOAT,
    vs_last_year_pct        FLOAT,
    avg_5yr_yield           FLOAT,

    model_type              TEXT,
    confidence              FLOAT,
    primary_driver          TEXT,
    analog_years            TEXT,

    national_production_est FLOAT,
    national_yield_est      FLOAT,

    prev_week_forecast      FLOAT,
    wow_change              FLOAT,

    created_at              TIMESTAMPTZ DEFAULT NOW(),

    CONSTRAINT uq_yield_forecast UNIQUE (commodity, state, year, forecast_week, model_type)
);

CREATE INDEX IF NOT EXISTS idx_yfc_commodity_year ON gold.yield_forecast(commodity, year);
CREATE INDEX IF NOT EXISTS idx_yfc_forecast_week ON gold.yield_forecast(forecast_week);
CREATE INDEX IF NOT EXISTS idx_yfc_run_id ON gold.yield_forecast(run_id);

-- ============================================================================
-- 6. YIELD MONITOR VIEW (dashboard)
-- ============================================================================

CREATE OR REPLACE VIEW gold.yield_monitor AS
SELECT
    yf.commodity,
    yf.state,
    yf.year,
    yf.forecast_week,
    yf.forecast_date,
    yf.yield_forecast,
    yf.yield_low,
    yf.yield_high,
    yf.trend_yield,
    yf.vs_trend_pct,
    yf.confidence,
    yf.primary_driver,
    yf.analog_years,
    yf.prev_week_forecast,
    yf.wow_change,
    feat.gdd_cum,
    feat.gdd_vs_normal_pct,
    feat.precip_cum_mm,
    feat.precip_vs_normal_pct,
    feat.stress_days_heat,
    feat.stress_days_drought,
    feat.condition_index,
    feat.condition_vs_5yr,
    feat.good_excellent_pct,
    feat.growth_stage,
    feat.ww_risk_score,
    CASE
        WHEN feat.stress_days_drought > 7 AND feat.growth_stage = 'reproductive' THEN 'HIGH: Drought during critical window'
        WHEN feat.stress_days_heat > 5 AND feat.growth_stage = 'reproductive' THEN 'HIGH: Heat stress during critical window'
        WHEN feat.condition_vs_5yr < -0.5 THEN 'ELEVATED: Below-average condition'
        WHEN feat.precip_vs_normal_pct < -25 THEN 'MODERATE: Below-normal precipitation'
        WHEN feat.gdd_vs_normal_pct < -10 THEN 'LOW: Behind-pace development'
        ELSE 'NORMAL'
    END AS risk_level
FROM gold.yield_forecast yf
LEFT JOIN silver.yield_features feat
    ON yf.state = feat.state
    AND LOWER(yf.commodity) = feat.crop
    AND yf.year = feat.year
    AND yf.forecast_week = feat.week
WHERE yf.model_type = 'ensemble'
ORDER BY yf.commodity, yf.state, yf.forecast_week DESC;

-- ============================================================================
-- 7. SEED CLIMATOLOGY NORMALS (US crop regions, monthly)
-- ============================================================================
-- Approximate 30-year normals for growing season months.
-- precip_normal_mm is monthly total, temps are monthly averages (Celsius).

INSERT INTO reference.weather_climatology (region_code, month, precip_normal_mm, tmin_normal_c, tmax_normal_c, gdd_normal)
VALUES
    -- US Corn Belt (IA/IL/IN average)
    ('US_CORN_BELT', 4, 89, 3.3, 16.1, 95),
    ('US_CORN_BELT', 5, 114, 9.4, 22.2, 195),
    ('US_CORN_BELT', 6, 117, 15.0, 27.8, 305),
    ('US_CORN_BELT', 7, 104, 17.8, 30.0, 370),
    ('US_CORN_BELT', 8, 102, 16.7, 28.9, 340),
    ('US_CORN_BELT', 9, 84, 11.7, 24.4, 230),
    ('US_CORN_BELT', 10, 69, 5.0, 17.2, 110),
    -- US Soybean Belt (similar to corn belt, slightly shifted)
    ('US_SOY_BELT', 5, 112, 9.4, 22.2, 195),
    ('US_SOY_BELT', 6, 114, 15.0, 27.8, 305),
    ('US_SOY_BELT', 7, 102, 17.8, 30.0, 370),
    ('US_SOY_BELT', 8, 99, 16.7, 28.9, 340),
    ('US_SOY_BELT', 9, 81, 11.7, 24.4, 230),
    ('US_SOY_BELT', 10, 71, 5.0, 17.2, 110),
    -- US Winter Wheat Belt (KS/OK/TX)
    ('US_WHEAT_WINTER', 3, 51, 1.1, 13.3, 45),
    ('US_WHEAT_WINTER', 4, 64, 6.7, 19.4, 120),
    ('US_WHEAT_WINTER', 5, 102, 12.2, 25.0, 220),
    ('US_WHEAT_WINTER', 6, 94, 17.8, 31.7, 360),
    ('US_WHEAT_WINTER', 9, 51, -2.2, 8.3, 15),
    ('US_WHEAT_WINTER', 10, 56, -6.7, 3.3, 5),
    -- US Spring Wheat Belt (ND/MT/MN)
    ('US_WHEAT_SPRING', 4, 38, -1.7, 11.1, 20),
    ('US_WHEAT_SPRING', 5, 64, 5.6, 18.9, 120),
    ('US_WHEAT_SPRING', 6, 89, 11.1, 25.0, 250),
    ('US_WHEAT_SPRING', 7, 71, 13.9, 28.3, 310),
    ('US_WHEAT_SPRING', 8, 56, 12.2, 26.7, 280)
ON CONFLICT (region_code, month, day_of_month) DO NOTHING;
