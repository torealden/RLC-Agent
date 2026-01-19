-- =============================================================================
-- RLC Commodities Database Schema - Food Expenditure Data (Bronze Layer)
-- Version: 1.0.0
-- =============================================================================
--
-- USDA ERS Food Expenditure Series
-- Source: https://www.ers.usda.gov/data-products/food-expenditure-series
--
-- This data measures the U.S. food system by quantifying the value of food
-- acquired in the United States by type of product, outlet, and purchaser.
--
-- Includes:
-- - Monthly sales of food with taxes and tips
-- - Food at home (FAH) vs food away from home (FAFH)
-- - Breakdown by outlet type (grocery stores, restaurants, etc.)
--
-- Release Schedule:
-- - Annual data: Released each June
-- - Monthly data: Released June through February (17th-21st of each month)
-- =============================================================================

-- Add data source for ERS Food Expenditure Series
INSERT INTO public.data_source (code, name, description, base_url, api_type, update_frequency)
VALUES (
    'USDA_ERS_FOOD_EXPENDITURE',
    'USDA ERS Food Expenditure Series',
    'Monthly and annual food sales data with taxes and tips by outlet type',
    'https://www.ers.usda.gov/data-products/food-expenditure-series',
    'FILE',
    'MONTHLY'
)
ON CONFLICT (code) DO UPDATE SET
    name = EXCLUDED.name,
    description = EXCLUDED.description,
    base_url = EXCLUDED.base_url,
    updated_at = NOW();

-- =============================================================================
-- BRONZE TABLE: Monthly Food Sales with Taxes and Tips
-- =============================================================================

CREATE TABLE IF NOT EXISTS bronze.ers_food_sales_monthly (
    id BIGSERIAL PRIMARY KEY,

    -- Natural key components
    year INT NOT NULL,
    month INT NOT NULL,
    outlet_type VARCHAR(200) NOT NULL,      -- e.g., 'Grocery stores', 'Full-service restaurants'
    purchaser_type VARCHAR(100),            -- e.g., 'All purchasers', 'Household', 'Government'

    -- Values from source
    sales_value NUMERIC(20, 4),             -- Sales value in millions of dollars
    sales_value_real NUMERIC(20, 4),        -- Real (inflation-adjusted) value if available

    -- Additional attributes from source
    food_category VARCHAR(100),             -- 'Food at home', 'Food away from home', 'Total'
    subcategory VARCHAR(200),               -- More specific category if available

    -- Raw data preservation
    raw_value_text VARCHAR(100),            -- Original text value from CSV
    raw_row_data JSONB,                     -- Complete row as JSON for audit

    -- Source file metadata
    source_file VARCHAR(500),               -- Original filename
    source_row_number INT,                  -- Row number in source file
    data_revision VARCHAR(50),              -- e.g., 'September 2025'

    -- Tracking
    ingest_run_id UUID,

    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),

    -- Natural key for idempotent upserts
    UNIQUE (year, month, outlet_type, COALESCE(purchaser_type, 'ALL'), COALESCE(food_category, 'TOTAL'))
);

-- Indexes for common queries
CREATE INDEX IF NOT EXISTS idx_ers_food_sales_date
    ON bronze.ers_food_sales_monthly(year, month);
CREATE INDEX IF NOT EXISTS idx_ers_food_sales_outlet
    ON bronze.ers_food_sales_monthly(outlet_type);
CREATE INDEX IF NOT EXISTS idx_ers_food_sales_category
    ON bronze.ers_food_sales_monthly(food_category);
CREATE INDEX IF NOT EXISTS idx_ers_food_sales_ingest
    ON bronze.ers_food_sales_monthly(ingest_run_id);

COMMENT ON TABLE bronze.ers_food_sales_monthly IS
    'Monthly food sales with taxes and tips from USDA ERS Food Expenditure Series. '
    'Values are in millions of dollars. Updated monthly.';

-- =============================================================================
-- BRONZE TABLE: Annual Food Expenditure Summary
-- =============================================================================

CREATE TABLE IF NOT EXISTS bronze.ers_food_expenditure_annual (
    id BIGSERIAL PRIMARY KEY,

    -- Natural key components
    year INT NOT NULL,
    expenditure_category VARCHAR(200) NOT NULL,  -- e.g., 'Food at home', 'Food away from home'
    subcategory VARCHAR(200),                     -- e.g., 'Grocery stores', 'Full-service restaurants'

    -- Values
    expenditure_value NUMERIC(20, 4),            -- Value in millions of dollars
    expenditure_share NUMERIC(8, 4),             -- Share of total food expenditure (0-100)
    per_capita_value NUMERIC(15, 4),             -- Per capita value if available

    -- Additional context
    purchaser_type VARCHAR(100),                  -- 'All purchasers', 'Household', etc.
    price_basis VARCHAR(50),                      -- 'Nominal', 'Real (2017 dollars)', etc.

    -- Raw data preservation
    raw_row_data JSONB,

    -- Source metadata
    source_file VARCHAR(500),
    data_revision VARCHAR(50),

    -- Tracking
    ingest_run_id UUID,

    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),

    -- Natural key
    UNIQUE (year, expenditure_category, COALESCE(subcategory, 'TOTAL'), COALESCE(purchaser_type, 'ALL'))
);

CREATE INDEX IF NOT EXISTS idx_ers_food_exp_annual_year
    ON bronze.ers_food_expenditure_annual(year);
CREATE INDEX IF NOT EXISTS idx_ers_food_exp_annual_category
    ON bronze.ers_food_expenditure_annual(expenditure_category);

COMMENT ON TABLE bronze.ers_food_expenditure_annual IS
    'Annual food expenditure summary from USDA ERS. Values in millions of dollars.';

-- =============================================================================
-- EXAMPLE UPSERT PATTERNS
-- =============================================================================

-- Example: Upsert monthly food sales record
--
-- INSERT INTO bronze.ers_food_sales_monthly (
--     year, month, outlet_type, purchaser_type, food_category,
--     sales_value, raw_value_text, raw_row_data,
--     source_file, source_row_number, data_revision, ingest_run_id
-- ) VALUES (
--     2024, 9, 'Grocery stores', 'All purchasers', 'Food at home',
--     65432.10, '65432.10', '{"Year": 2024, "Month": 9, ...}'::jsonb,
--     'monthly-sales-of-food-with-taxes-and-tips.csv', 15, 'September 2025',
--     'abc123...'
-- )
-- ON CONFLICT (year, month, outlet_type, COALESCE(purchaser_type, 'ALL'), COALESCE(food_category, 'TOTAL'))
-- DO UPDATE SET
--     sales_value = EXCLUDED.sales_value,
--     raw_value_text = EXCLUDED.raw_value_text,
--     raw_row_data = EXCLUDED.raw_row_data,
--     data_revision = EXCLUDED.data_revision,
--     ingest_run_id = EXCLUDED.ingest_run_id,
--     updated_at = NOW();

-- =============================================================================
-- END OF FOOD EXPENDITURE SCHEMA
-- =============================================================================
