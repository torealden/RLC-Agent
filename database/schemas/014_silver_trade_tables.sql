-- =============================================================================
-- Silver Layer Trade Tables (Actual Tables, Not Views)
-- =============================================================================
-- Transformed and validated trade data ready for spreadsheet integration
-- Data flows: Bronze → Validation → Silver (with logging) → Verification → Gold
-- =============================================================================

-- -----------------------------------------------------------------------------
-- VALIDATION/TRANSFORMATION LOG TABLE
-- -----------------------------------------------------------------------------
-- Tracks every transformation from bronze to silver with verification status
CREATE TABLE IF NOT EXISTS silver.transformation_log (
    id SERIAL PRIMARY KEY,
    batch_id UUID NOT NULL,                    -- Groups records transformed together
    table_name VARCHAR(100) NOT NULL,          -- Source bronze table
    transformation_type VARCHAR(50) NOT NULL,  -- 'UNIT_CONVERSION', 'AGGREGATION', etc.

    -- Record counts
    bronze_records_processed INTEGER NOT NULL,
    silver_records_created INTEGER NOT NULL,
    silver_records_updated INTEGER NOT NULL,

    -- Validation status
    source_validated BOOLEAN DEFAULT FALSE,    -- Was bronze data checked against source?
    source_validation_time TIMESTAMP WITH TIME ZONE,
    source_validation_result VARCHAR(20),      -- 'PASS', 'FAIL', 'SKIPPED'
    source_validation_notes TEXT,

    -- Verification status (post-transformation math check)
    transformation_verified BOOLEAN DEFAULT FALSE,
    verification_time TIMESTAMP WITH TIME ZONE,
    verification_result VARCHAR(20),           -- 'PASS', 'FAIL', 'WARNINGS'
    verification_discrepancies INTEGER DEFAULT 0,
    verification_notes TEXT,

    -- Timing
    started_at TIMESTAMP WITH TIME ZONE NOT NULL,
    completed_at TIMESTAMP WITH TIME ZONE,

    -- Who/what ran it
    triggered_by VARCHAR(100),                 -- 'SCHEDULER', 'MANUAL', 'API_WEBHOOK'
    agent_name VARCHAR(100),

    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- -----------------------------------------------------------------------------
-- SILVER LAYER: Census Trade (Monthly Totals)
-- -----------------------------------------------------------------------------
-- Transformed trade data with standardized units
-- This is the PRIMARY table for spreadsheet integration
CREATE TABLE IF NOT EXISTS silver.census_trade_monthly (
    id SERIAL PRIMARY KEY,
    year INTEGER NOT NULL,
    month INTEGER NOT NULL,
    flow VARCHAR(10) NOT NULL,                 -- 'imports' or 'exports'

    -- HS Code and Commodity
    hs_code VARCHAR(10) NOT NULL,
    commodity_group VARCHAR(50),               -- From reference.hs_codes
    commodity_description VARCHAR(255),

    -- Original values (from bronze, for verification)
    bronze_quantity DECIMAL(18,4),             -- Original quantity in census units
    bronze_unit VARCHAR(10),                   -- KG, T, etc.
    bronze_value_usd DECIMAL(18,2),

    -- Converted values (standardized for spreadsheets)
    quantity_mt DECIMAL(18,4),                 -- Metric tons
    quantity_short_tons DECIMAL(18,4),         -- For meals (MT * 1.10231)
    quantity_1000_lbs DECIMAL(18,4),           -- For oils (KG * 2.20462 / 1000)
    quantity_bushels DECIMAL(18,4),            -- For grains
    value_usd DECIMAL(18,2),

    -- Conversion metadata
    conversion_factor DECIMAL(12,6),           -- Factor used for this conversion
    conversion_formula VARCHAR(100),           -- e.g., 'KG / 1000 * 1.10231'

    -- Lineage
    bronze_record_ids INTEGER[],               -- Array of bronze.census_trade.id values
    transformation_batch_id UUID,              -- Links to transformation_log

    -- Verification
    verified BOOLEAN DEFAULT FALSE,
    verified_at TIMESTAMP WITH TIME ZONE,
    verification_delta DECIMAL(18,6),          -- Difference from independent calculation

    -- Timestamps
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),

    UNIQUE(year, month, flow, hs_code)
);

-- -----------------------------------------------------------------------------
-- SILVER LAYER: Commodity Group Aggregates
-- -----------------------------------------------------------------------------
-- Pre-aggregated totals by commodity group (e.g., all soybean meal codes combined)
CREATE TABLE IF NOT EXISTS silver.trade_by_commodity_group (
    id SERIAL PRIMARY KEY,
    year INTEGER NOT NULL,
    month INTEGER NOT NULL,
    flow VARCHAR(10) NOT NULL,

    commodity_group VARCHAR(50) NOT NULL,      -- e.g., 'SOYBEAN_MEAL_ALL'
    commodity_group_name VARCHAR(100),

    -- Aggregated quantities
    total_mt DECIMAL(18,4),
    total_short_tons DECIMAL(18,4),
    total_1000_lbs DECIMAL(18,4),
    total_bushels DECIMAL(18,4),
    total_value_usd DECIMAL(18,2),

    -- Component HS codes included
    hs_codes_included VARCHAR(10)[],
    num_hs_codes INTEGER,

    -- Lineage
    transformation_batch_id UUID,

    -- Timestamps
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),

    UNIQUE(year, month, flow, commodity_group)
);

-- -----------------------------------------------------------------------------
-- VERIFICATION DISCREPANCY LOG
-- -----------------------------------------------------------------------------
-- Records any discrepancies found during verification
CREATE TABLE IF NOT EXISTS silver.verification_discrepancies (
    id SERIAL PRIMARY KEY,
    batch_id UUID NOT NULL,
    silver_table VARCHAR(100) NOT NULL,
    silver_record_id INTEGER NOT NULL,

    -- What was checked
    field_name VARCHAR(50) NOT NULL,           -- e.g., 'quantity_short_tons'
    expected_value DECIMAL(18,6),              -- Independent calculation result
    actual_value DECIMAL(18,6),                -- What's in silver table
    difference DECIMAL(18,6),
    difference_pct DECIMAL(8,4),

    -- Context
    hs_code VARCHAR(10),
    year INTEGER,
    month INTEGER,
    flow VARCHAR(10),

    -- Resolution
    resolved BOOLEAN DEFAULT FALSE,
    resolved_at TIMESTAMP WITH TIME ZONE,
    resolution_notes TEXT,

    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- -----------------------------------------------------------------------------
-- INDEXES
-- -----------------------------------------------------------------------------
CREATE INDEX IF NOT EXISTS idx_silver_trade_year_month ON silver.census_trade_monthly(year, month);
CREATE INDEX IF NOT EXISTS idx_silver_trade_hs_code ON silver.census_trade_monthly(hs_code);
CREATE INDEX IF NOT EXISTS idx_silver_trade_commodity_group ON silver.census_trade_monthly(commodity_group);
CREATE INDEX IF NOT EXISTS idx_silver_trade_flow ON silver.census_trade_monthly(flow);
CREATE INDEX IF NOT EXISTS idx_silver_trade_batch ON silver.census_trade_monthly(transformation_batch_id);

CREATE INDEX IF NOT EXISTS idx_silver_group_year_month ON silver.trade_by_commodity_group(year, month);
CREATE INDEX IF NOT EXISTS idx_silver_group_commodity ON silver.trade_by_commodity_group(commodity_group);

CREATE INDEX IF NOT EXISTS idx_transform_log_batch ON silver.transformation_log(batch_id);
CREATE INDEX IF NOT EXISTS idx_transform_log_time ON silver.transformation_log(started_at);

CREATE INDEX IF NOT EXISTS idx_discrepancy_batch ON silver.verification_discrepancies(batch_id);
CREATE INDEX IF NOT EXISTS idx_discrepancy_unresolved ON silver.verification_discrepancies(resolved) WHERE resolved = FALSE;

-- -----------------------------------------------------------------------------
-- GOLD LAYER VIEWS (Analytics-Ready)
-- -----------------------------------------------------------------------------

-- Soybean Meal Monthly Trade (for spreadsheets)
CREATE OR REPLACE VIEW gold.soybean_meal_trade AS
SELECT
    year,
    month,
    flow,
    total_mt,
    total_short_tons,
    total_value_usd,
    hs_codes_included,
    updated_at
FROM silver.trade_by_commodity_group
WHERE commodity_group = 'SOYBEAN_MEAL_ALL'
ORDER BY year DESC, month DESC, flow;

-- Soybean Oil Monthly Trade (for spreadsheets)
CREATE OR REPLACE VIEW gold.soybean_oil_trade AS
SELECT
    year,
    month,
    flow,
    total_mt,
    total_1000_lbs,
    total_1000_lbs / 1000 as total_million_lbs,
    total_value_usd,
    hs_codes_included,
    updated_at
FROM silver.trade_by_commodity_group
WHERE commodity_group = 'SOYBEAN_OIL_ALL'
ORDER BY year DESC, month DESC, flow;

-- Marketing Year Aggregates
CREATE OR REPLACE VIEW gold.trade_by_marketing_year AS
SELECT
    commodity_group,
    flow,
    -- Marketing year: Oct-Sep for meals/oils, Sep-Aug for beans
    CASE
        WHEN commodity_group LIKE '%SOYBEAN_MEAL%' OR commodity_group LIKE '%SOYBEAN_OIL%'
            THEN CASE WHEN month >= 10 THEN year ELSE year - 1 END
        ELSE CASE WHEN month >= 9 THEN year ELSE year - 1 END
    END as marketing_year,
    SUM(total_mt) as total_mt,
    SUM(total_short_tons) as total_short_tons,
    SUM(total_1000_lbs) as total_1000_lbs,
    SUM(total_value_usd) as total_value_usd
FROM silver.trade_by_commodity_group
GROUP BY commodity_group, flow,
    CASE
        WHEN commodity_group LIKE '%SOYBEAN_MEAL%' OR commodity_group LIKE '%SOYBEAN_OIL%'
            THEN CASE WHEN month >= 10 THEN year ELSE year - 1 END
        ELSE CASE WHEN month >= 9 THEN year ELSE year - 1 END
    END
ORDER BY commodity_group, marketing_year DESC, flow;

-- Recent Transformation Status (for monitoring)
CREATE OR REPLACE VIEW gold.transformation_status AS
SELECT
    batch_id,
    table_name,
    transformation_type,
    bronze_records_processed,
    silver_records_created,
    source_validation_result,
    verification_result,
    verification_discrepancies,
    started_at,
    completed_at,
    completed_at - started_at as duration,
    triggered_by
FROM silver.transformation_log
ORDER BY started_at DESC
LIMIT 50;

-- Unresolved Discrepancies (for alerts)
CREATE OR REPLACE VIEW gold.unresolved_discrepancies AS
SELECT
    d.id,
    d.silver_table,
    d.field_name,
    d.hs_code,
    d.year,
    d.month,
    d.flow,
    d.expected_value,
    d.actual_value,
    d.difference,
    d.difference_pct,
    d.created_at
FROM silver.verification_discrepancies d
WHERE d.resolved = FALSE
ORDER BY d.created_at DESC;

-- -----------------------------------------------------------------------------
-- COMMENTS
-- -----------------------------------------------------------------------------
COMMENT ON TABLE silver.census_trade_monthly IS 'Transformed Census trade data with standardized units - primary source for spreadsheets';
COMMENT ON TABLE silver.trade_by_commodity_group IS 'Pre-aggregated trade totals by commodity group (e.g., all soybean meal HS codes)';
COMMENT ON TABLE silver.transformation_log IS 'Audit log of all bronze→silver transformations with validation/verification status';
COMMENT ON TABLE silver.verification_discrepancies IS 'Records of any discrepancies found during post-transformation verification';
