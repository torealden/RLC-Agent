-- =============================================================================
-- Balance Sheet Tracking Schema
-- User Estimates, Monthly Realized Data, and Variance Tracking
-- =============================================================================
-- Created: 2026-01-29
-- Purpose: Track user S&D estimates vs realized monthly data, enabling
--          variance analysis and forecast adjustments throughout the
--          marketing year.
-- =============================================================================

-- Ensure silver schema exists
CREATE SCHEMA IF NOT EXISTS silver;

-- =============================================================================
-- User S&D Estimates (Annual Balance Sheets)
-- Loaded from CSV files in domain_knowledge/balance_sheets/
-- =============================================================================
CREATE TABLE IF NOT EXISTS silver.user_sd_estimate (
    id BIGSERIAL PRIMARY KEY,

    -- Commodity/Location identification
    commodity VARCHAR(50) NOT NULL,          -- corn, soybeans, wheat, etc.
    country VARCHAR(100) NOT NULL DEFAULT 'US',
    marketing_year INT NOT NULL,             -- e.g., 2025 for MY 2024/25

    -- When this estimate was made
    estimate_date DATE NOT NULL,

    -- SUPPLY SIDE (user's unit - typically mil bu or 1000 MT)
    area_planted NUMERIC(18,2),
    area_harvested NUMERIC(18,2),
    yield NUMERIC(18,4),
    beginning_stocks NUMERIC(18,2),
    production NUMERIC(18,2),
    imports NUMERIC(18,2),
    total_supply NUMERIC(18,2),

    -- DEMAND SIDE
    crush NUMERIC(18,2),                     -- Soybeans
    feed_residual NUMERIC(18,2),             -- Corn - feed & residual
    fsi NUMERIC(18,2),                       -- Food/Seed/Industrial
    ethanol NUMERIC(18,2),                   -- Corn for ethanol
    domestic_use NUMERIC(18,2),              -- Total domestic
    exports NUMERIC(18,2),
    total_use NUMERIC(18,2),

    -- ENDING POSITION
    ending_stocks NUMERIC(18,2),
    stocks_use_ratio NUMERIC(8,4),           -- Stocks/Use %

    -- Units and metadata
    unit VARCHAR(20) DEFAULT 'mil bu',       -- mil bu, 1000 MT, etc.
    source_file VARCHAR(255),                -- CSV file this came from
    notes TEXT,
    is_current BOOLEAN DEFAULT TRUE,         -- Most recent estimate?

    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),

    -- Natural key: one estimate per commodity/country/MY/date
    UNIQUE (commodity, country, marketing_year, estimate_date)
);

CREATE INDEX IF NOT EXISTS idx_user_sd_current
    ON silver.user_sd_estimate(commodity, country, marketing_year)
    WHERE is_current = TRUE;
CREATE INDEX IF NOT EXISTS idx_user_sd_date
    ON silver.user_sd_estimate(estimate_date DESC);

COMMENT ON TABLE silver.user_sd_estimate IS
    'User S&D balance sheet estimates loaded from CSV files. Annual totals that are tracked against monthly realized data.';


-- =============================================================================
-- Monthly Realized Data
-- Populated from NOPA, Fats & Oils, Census, WASDE, etc.
-- =============================================================================
CREATE TABLE IF NOT EXISTS silver.monthly_realized (
    id BIGSERIAL PRIMARY KEY,

    -- Commodity/Location identification
    commodity VARCHAR(50) NOT NULL,
    country VARCHAR(100) NOT NULL DEFAULT 'US',

    -- Time period
    marketing_year INT NOT NULL,
    month INT NOT NULL CHECK (month BETWEEN 1 AND 12),
    calendar_year INT NOT NULL,

    -- What we're measuring
    attribute VARCHAR(50) NOT NULL,          -- crush, exports, imports, etc.
    realized_value NUMERIC(18,2),
    unit VARCHAR(20),

    -- Source tracking
    source VARCHAR(50) NOT NULL,             -- NOPA, FATS_OILS, CENSUS, WASDE, etc.
    report_date DATE,                        -- Date of source report
    is_preliminary BOOLEAN DEFAULT FALSE,    -- Will this be revised?

    -- Metadata
    collected_at TIMESTAMPTZ DEFAULT NOW(),
    ingest_run_id UUID,

    -- Natural key: one value per commodity/attribute/month/source
    UNIQUE (commodity, country, marketing_year, month, attribute, source)
);

CREATE INDEX IF NOT EXISTS idx_monthly_realized_lookup
    ON silver.monthly_realized(commodity, country, marketing_year, attribute);
CREATE INDEX IF NOT EXISTS idx_monthly_realized_source
    ON silver.monthly_realized(source, report_date DESC);

COMMENT ON TABLE silver.monthly_realized IS
    'Monthly realized S&D data from various sources (NOPA, Fats & Oils, Census, etc.). Used to track progress vs annual estimates.';


-- =============================================================================
-- Monthly Expectations (User Projections)
-- User's expected monthly values for remaining months in the marketing year
-- =============================================================================
CREATE TABLE IF NOT EXISTS silver.monthly_expectation (
    id BIGSERIAL PRIMARY KEY,

    -- Commodity/Location identification
    commodity VARCHAR(50) NOT NULL,
    country VARCHAR(100) NOT NULL DEFAULT 'US',

    -- Time period
    marketing_year INT NOT NULL,
    month INT NOT NULL CHECK (month BETWEEN 1 AND 12),

    -- What we're projecting
    attribute VARCHAR(50) NOT NULL,          -- crush, exports, imports, etc.
    expected_value NUMERIC(18,2),
    unit VARCHAR(20),

    -- Confidence and notes
    confidence VARCHAR(20),                  -- HIGH, MEDIUM, LOW
    notes TEXT,

    -- Version tracking
    estimate_date DATE NOT NULL,             -- When this projection was made
    is_current BOOLEAN DEFAULT TRUE,         -- Most recent projection?

    created_at TIMESTAMPTZ DEFAULT NOW(),

    -- Natural key: one projection per commodity/attribute/month/date
    UNIQUE (commodity, country, marketing_year, month, attribute, estimate_date)
);

CREATE INDEX IF NOT EXISTS idx_monthly_exp_current
    ON silver.monthly_expectation(commodity, country, marketing_year, attribute)
    WHERE is_current = TRUE;

COMMENT ON TABLE silver.monthly_expectation IS
    'User projections for monthly S&D values. Used for remaining months in the marketing year until realized data arrives.';


-- =============================================================================
-- Attribute Reference
-- Standard attributes tracked in S&D balance sheets
-- =============================================================================
CREATE TABLE IF NOT EXISTS silver.sd_attribute_ref (
    code VARCHAR(50) PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    category VARCHAR(50),                    -- supply, demand, ending
    typical_unit VARCHAR(20),
    description TEXT
);

INSERT INTO silver.sd_attribute_ref (code, name, category, typical_unit, description) VALUES
    -- Supply
    ('beginning_stocks', 'Beginning Stocks', 'supply', 'mil bu', 'Carryover from prior marketing year'),
    ('production', 'Production', 'supply', 'mil bu', 'Total production'),
    ('imports', 'Imports', 'supply', 'mil bu', 'Total imports'),
    ('total_supply', 'Total Supply', 'supply', 'mil bu', 'Beg stocks + production + imports'),

    -- Demand - Common
    ('exports', 'Exports', 'demand', 'mil bu', 'Total exports'),
    ('domestic_use', 'Domestic Use', 'demand', 'mil bu', 'Total domestic disappearance'),
    ('total_use', 'Total Use', 'demand', 'mil bu', 'Exports + domestic use'),

    -- Demand - Corn specific
    ('feed_residual', 'Feed & Residual', 'demand', 'mil bu', 'Feed use plus statistical residual'),
    ('ethanol', 'Ethanol', 'demand', 'mil bu', 'Corn used for fuel ethanol'),
    ('fsi', 'FSI', 'demand', 'mil bu', 'Food, seed, and industrial use (non-ethanol)'),

    -- Demand - Oilseed specific
    ('crush', 'Crush', 'demand', 'mil bu', 'Oilseed crush volume'),
    ('seed', 'Seed', 'demand', 'mil bu', 'Seed use'),

    -- Ending
    ('ending_stocks', 'Ending Stocks', 'ending', 'mil bu', 'Carryout to next marketing year'),
    ('stocks_use', 'Stocks/Use Ratio', 'ending', '%', 'Ending stocks as percent of total use')
ON CONFLICT (code) DO NOTHING;


-- =============================================================================
-- Marketing Year Reference
-- When marketing years start/end for each commodity
-- =============================================================================
CREATE TABLE IF NOT EXISTS silver.marketing_year_ref (
    commodity VARCHAR(50) PRIMARY KEY,
    start_month INT NOT NULL,                -- Month MY starts (1-12)
    description VARCHAR(100)
);

INSERT INTO silver.marketing_year_ref (commodity, start_month, description) VALUES
    ('corn', 9, 'September - August'),
    ('soybeans', 9, 'September - August'),
    ('wheat', 6, 'June - May'),
    ('soybean_meal', 10, 'October - September'),
    ('soybean_oil', 10, 'October - September'),
    ('sorghum', 9, 'September - August'),
    ('cotton', 8, 'August - July'),
    ('rice', 8, 'August - July')
ON CONFLICT (commodity) DO UPDATE SET
    start_month = EXCLUDED.start_month,
    description = EXCLUDED.description;


-- =============================================================================
-- Trigger to update timestamps
-- =============================================================================
CREATE OR REPLACE FUNCTION silver.update_timestamp()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS user_sd_estimate_timestamp ON silver.user_sd_estimate;
CREATE TRIGGER user_sd_estimate_timestamp
    BEFORE UPDATE ON silver.user_sd_estimate
    FOR EACH ROW EXECUTE FUNCTION silver.update_timestamp();


-- =============================================================================
-- Helper function to mark old estimates as not current
-- =============================================================================
CREATE OR REPLACE FUNCTION silver.mark_previous_estimates_not_current(
    p_commodity VARCHAR,
    p_country VARCHAR,
    p_marketing_year INT
) RETURNS INT AS $$
DECLARE
    rows_updated INT;
BEGIN
    UPDATE silver.user_sd_estimate
    SET is_current = FALSE
    WHERE commodity = p_commodity
      AND country = p_country
      AND marketing_year = p_marketing_year
      AND is_current = TRUE;

    GET DIAGNOSTICS rows_updated = ROW_COUNT;
    RETURN rows_updated;
END;
$$ LANGUAGE plpgsql;


-- =============================================================================
-- Helper function to get MY month number from calendar month
-- =============================================================================
CREATE OR REPLACE FUNCTION silver.get_my_month(
    p_commodity VARCHAR,
    p_calendar_month INT
) RETURNS INT AS $$
DECLARE
    my_start INT;
BEGIN
    SELECT start_month INTO my_start
    FROM silver.marketing_year_ref
    WHERE commodity = p_commodity;

    IF my_start IS NULL THEN
        my_start := 9;  -- Default to Sep-Aug
    END IF;

    -- Convert calendar month to MY month (1-12)
    IF p_calendar_month >= my_start THEN
        RETURN p_calendar_month - my_start + 1;
    ELSE
        RETURN p_calendar_month + (12 - my_start) + 1;
    END IF;
END;
$$ LANGUAGE plpgsql;


-- =============================================================================
-- Grants
-- =============================================================================
GRANT SELECT ON silver.user_sd_estimate TO readonly_role;
GRANT SELECT ON silver.monthly_realized TO readonly_role;
GRANT SELECT ON silver.monthly_expectation TO readonly_role;
GRANT SELECT ON silver.sd_attribute_ref TO readonly_role;
GRANT SELECT ON silver.marketing_year_ref TO readonly_role;

GRANT ALL ON silver.user_sd_estimate TO collector_role;
GRANT ALL ON silver.monthly_realized TO collector_role;
GRANT ALL ON silver.monthly_expectation TO collector_role;
