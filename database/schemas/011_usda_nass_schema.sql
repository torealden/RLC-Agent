-- =============================================================================
-- USDA NASS (National Agricultural Statistics Service) Schema
-- =============================================================================
-- Bronze layer tables for NASS Quick Stats data
-- Includes: Crop Progress, Condition, Acreage, Production, Stocks
-- =============================================================================

-- -----------------------------------------------------------------------------
-- BRONZE LAYER - Raw NASS data as received from API
-- -----------------------------------------------------------------------------

-- Crop Progress (weekly during growing season)
-- Tracks: planting, emerged, silking, dough, dented, mature, harvested
CREATE TABLE IF NOT EXISTS bronze.nass_crop_progress (
    id SERIAL PRIMARY KEY,
    commodity VARCHAR(50) NOT NULL,
    year INTEGER NOT NULL,
    week_ending DATE,
    reference_period VARCHAR(50),
    state VARCHAR(10) DEFAULT 'US',
    agg_level VARCHAR(20) DEFAULT 'NATIONAL',
    statisticcat VARCHAR(50),
    short_desc TEXT,
    unit VARCHAR(20),
    value DECIMAL(12,4),
    source VARCHAR(50) DEFAULT 'USDA_NASS',
    collected_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE(commodity, year, week_ending, state, short_desc)
);

-- Crop Condition Ratings (weekly during growing season)
-- Tracks: Excellent, Good, Fair, Poor, Very Poor percentages
CREATE TABLE IF NOT EXISTS bronze.nass_crop_condition (
    id SERIAL PRIMARY KEY,
    commodity VARCHAR(50) NOT NULL,
    year INTEGER NOT NULL,
    week_ending DATE,
    reference_period VARCHAR(50),
    state VARCHAR(10) DEFAULT 'US',
    agg_level VARCHAR(20) DEFAULT 'NATIONAL',
    condition_category VARCHAR(20),  -- EXCELLENT, GOOD, FAIR, POOR, VERY POOR
    short_desc TEXT,
    unit VARCHAR(20),
    value DECIMAL(8,4),  -- Percentage
    source VARCHAR(50) DEFAULT 'USDA_NASS',
    collected_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE(commodity, year, week_ending, state, condition_category)
);

-- Acreage Data (planted and harvested)
-- Released: Prospective Plantings (March), Acreage (June), Final (January)
CREATE TABLE IF NOT EXISTS bronze.nass_acreage (
    id SERIAL PRIMARY KEY,
    commodity VARCHAR(50) NOT NULL,
    year INTEGER NOT NULL,
    reference_period VARCHAR(50),
    state VARCHAR(10) DEFAULT 'US',
    agg_level VARCHAR(20) DEFAULT 'NATIONAL',
    statisticcat VARCHAR(50),  -- AREA PLANTED, AREA HARVESTED
    short_desc TEXT,
    unit VARCHAR(20),
    value DECIMAL(15,4),  -- Acres (can be large numbers)
    source VARCHAR(50) DEFAULT 'USDA_NASS',
    collected_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE(commodity, year, reference_period, state, statisticcat)
);

-- Production Estimates
-- Released: Monthly (August - January), Final (January)
CREATE TABLE IF NOT EXISTS bronze.nass_production (
    id SERIAL PRIMARY KEY,
    commodity VARCHAR(50) NOT NULL,
    year INTEGER NOT NULL,
    reference_period VARCHAR(50),  -- YEAR, AUG FORECAST, SEP FORECAST, etc.
    state VARCHAR(10) DEFAULT 'US',
    agg_level VARCHAR(20) DEFAULT 'NATIONAL',
    statisticcat VARCHAR(50),
    short_desc TEXT,
    unit VARCHAR(20),
    value DECIMAL(18,4),  -- Production (bushels, tons - can be very large)
    source VARCHAR(50) DEFAULT 'USDA_NASS',
    collected_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE(commodity, year, reference_period, state, short_desc)
);

-- Grain Stocks (quarterly)
-- Released: January 1, March 1, June 1, September 1
CREATE TABLE IF NOT EXISTS bronze.nass_stocks (
    id SERIAL PRIMARY KEY,
    commodity VARCHAR(50) NOT NULL,
    year INTEGER NOT NULL,
    reference_period VARCHAR(50),  -- DEC 1, MAR 1, JUN 1, SEP 1
    state VARCHAR(10) DEFAULT 'US',
    agg_level VARCHAR(20) DEFAULT 'NATIONAL',
    statisticcat VARCHAR(50),
    short_desc TEXT,
    unit VARCHAR(20),
    value DECIMAL(18,4),  -- Stocks in bushels
    source VARCHAR(50) DEFAULT 'USDA_NASS',
    collected_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE(commodity, year, reference_period, state, short_desc)
);

-- -----------------------------------------------------------------------------
-- INDEXES for query performance
-- -----------------------------------------------------------------------------

CREATE INDEX IF NOT EXISTS idx_nass_progress_commodity_year
    ON bronze.nass_crop_progress(commodity, year);
CREATE INDEX IF NOT EXISTS idx_nass_progress_week
    ON bronze.nass_crop_progress(week_ending);

CREATE INDEX IF NOT EXISTS idx_nass_condition_commodity_year
    ON bronze.nass_crop_condition(commodity, year);
CREATE INDEX IF NOT EXISTS idx_nass_condition_week
    ON bronze.nass_crop_condition(week_ending);

CREATE INDEX IF NOT EXISTS idx_nass_acreage_commodity_year
    ON bronze.nass_acreage(commodity, year);

CREATE INDEX IF NOT EXISTS idx_nass_production_commodity_year
    ON bronze.nass_production(commodity, year);

CREATE INDEX IF NOT EXISTS idx_nass_stocks_commodity_year
    ON bronze.nass_stocks(commodity, year);

-- -----------------------------------------------------------------------------
-- SILVER LAYER - Cleaned and standardized views
-- -----------------------------------------------------------------------------

-- Combined Good/Excellent ratings by week
CREATE OR REPLACE VIEW silver.nass_crop_condition_ge AS
SELECT
    commodity,
    year,
    week_ending,
    state,
    SUM(CASE WHEN condition_category IN ('GOOD', 'EXCELLENT') THEN value ELSE 0 END) as good_excellent_pct,
    SUM(CASE WHEN condition_category = 'EXCELLENT' THEN value ELSE 0 END) as excellent_pct,
    SUM(CASE WHEN condition_category = 'GOOD' THEN value ELSE 0 END) as good_pct,
    SUM(CASE WHEN condition_category = 'FAIR' THEN value ELSE 0 END) as fair_pct,
    SUM(CASE WHEN condition_category IN ('POOR', 'VERY POOR') THEN value ELSE 0 END) as poor_very_poor_pct,
    MAX(collected_at) as last_updated
FROM bronze.nass_crop_condition
GROUP BY commodity, year, week_ending, state;

-- Latest crop progress by commodity
CREATE OR REPLACE VIEW silver.nass_latest_progress AS
SELECT DISTINCT ON (commodity, short_desc)
    commodity,
    year,
    week_ending,
    short_desc,
    value as progress_pct,
    collected_at
FROM bronze.nass_crop_progress
WHERE state = 'US'
ORDER BY commodity, short_desc, week_ending DESC;

-- National production summary
CREATE OR REPLACE VIEW silver.nass_production_summary AS
SELECT
    commodity,
    year,
    reference_period,
    short_desc,
    value as production,
    unit,
    collected_at
FROM bronze.nass_production
WHERE state = 'US'
  AND agg_level = 'NATIONAL'
ORDER BY commodity, year DESC, reference_period;

-- National acreage summary
CREATE OR REPLACE VIEW silver.nass_acreage_summary AS
SELECT
    commodity,
    year,
    statisticcat as acreage_type,
    value as acres,
    unit,
    collected_at
FROM bronze.nass_acreage
WHERE state = 'US'
  AND agg_level = 'NATIONAL'
  AND reference_period = 'YEAR'
ORDER BY commodity, year DESC;

-- -----------------------------------------------------------------------------
-- GOLD LAYER - Analytics-ready views
-- -----------------------------------------------------------------------------

-- Crop condition trend analysis (YoY comparison)
CREATE OR REPLACE VIEW gold.nass_condition_yoy AS
SELECT
    c.commodity,
    c.week_ending,
    c.good_excellent_pct as current_ge_pct,
    p.good_excellent_pct as prior_year_ge_pct,
    c.good_excellent_pct - COALESCE(p.good_excellent_pct, 0) as ge_change,
    c.last_updated
FROM silver.nass_crop_condition_ge c
LEFT JOIN silver.nass_crop_condition_ge p
    ON c.commodity = p.commodity
    AND c.state = p.state
    AND p.week_ending = c.week_ending - INTERVAL '1 year'
WHERE c.state = 'US'
ORDER BY c.commodity, c.week_ending DESC;

-- Production history by commodity (last 5 years)
CREATE OR REPLACE VIEW gold.nass_production_history AS
SELECT
    commodity,
    year,
    MAX(CASE WHEN unit = 'BU' THEN value END) as production_bu,
    MAX(CASE WHEN statisticcat = 'AREA HARVESTED' THEN value END) as area_harvested,
    MAX(CASE WHEN statisticcat = 'YIELD' THEN value END) as yield_per_acre
FROM (
    SELECT commodity, year, unit, value, 'PRODUCTION' as statisticcat
    FROM bronze.nass_production
    WHERE state = 'US' AND agg_level = 'NATIONAL' AND reference_period = 'YEAR'
    UNION ALL
    SELECT commodity, year, unit, value, statisticcat
    FROM bronze.nass_acreage
    WHERE state = 'US' AND agg_level = 'NATIONAL' AND reference_period = 'YEAR'
) combined
WHERE year >= EXTRACT(YEAR FROM CURRENT_DATE) - 5
GROUP BY commodity, year
ORDER BY commodity, year DESC;

-- Comments for documentation
COMMENT ON TABLE bronze.nass_crop_progress IS 'Weekly crop progress data from USDA NASS Quick Stats API';
COMMENT ON TABLE bronze.nass_crop_condition IS 'Weekly crop condition ratings (Excellent/Good/Fair/Poor/Very Poor)';
COMMENT ON TABLE bronze.nass_acreage IS 'Planted and harvested acreage from NASS';
COMMENT ON TABLE bronze.nass_production IS 'Crop production estimates and final figures';
COMMENT ON TABLE bronze.nass_stocks IS 'Quarterly grain stocks reports';
