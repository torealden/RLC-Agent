-- =============================================================================
-- CONAB Brazilian Soybean Data Schema
-- Version: 1.0.0
-- =============================================================================
--
-- This schema supports the CONAB Soybean data pipeline for Brazilian
-- soybean supply, demand, and price data.
--
-- Source: CONAB (Companhia Nacional de Abastecimento)
-- URL: https://www.gov.br/conab/pt-br/atuacao/informacoes-agropecuarias/safras/series-historicas
--
-- =============================================================================

-- =============================================================================
-- BRONZE LAYER: Raw Data Storage
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Bronze: CONAB Soybean Production (Raw)
-- Stores production data exactly as received from CONAB Excel files
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS bronze.conab_soybean_production (
    id BIGSERIAL PRIMARY KEY,

    -- Natural key
    crop_year VARCHAR(20) NOT NULL,
    state VARCHAR(50) NOT NULL DEFAULT 'BRASIL',

    -- Raw data as received
    commodity VARCHAR(50) NOT NULL DEFAULT 'SOYBEANS',
    commodity_pt VARCHAR(50) DEFAULT 'SOJA',

    -- Production data (in 1000 units as published)
    planted_area_1000ha NUMERIC(15, 3),
    harvested_area_1000ha NUMERIC(15, 3),
    production_1000t NUMERIC(15, 3),
    yield_kg_ha NUMERIC(15, 3),

    -- Metadata
    source VARCHAR(50) DEFAULT 'CONAB',
    sheet_name VARCHAR(200),
    ingest_run_id UUID,
    file_hash VARCHAR(64),
    raw_data JSONB,

    -- Timestamps
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),

    -- Natural key constraint for idempotent upserts
    UNIQUE(crop_year, state)
);

CREATE INDEX IF NOT EXISTS idx_bronze_conab_soy_prod_year
    ON bronze.conab_soybean_production(crop_year);
CREATE INDEX IF NOT EXISTS idx_bronze_conab_soy_prod_state
    ON bronze.conab_soybean_production(state);
CREATE INDEX IF NOT EXISTS idx_bronze_conab_soy_prod_ingest
    ON bronze.conab_soybean_production(ingest_run_id);

COMMENT ON TABLE bronze.conab_soybean_production IS
    'Raw CONAB soybean production data. One row per crop year and state.';

-- -----------------------------------------------------------------------------
-- Bronze: CONAB Soybean Supply & Demand (Raw)
-- Stores S&D balance sheet items from CONAB
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS bronze.conab_soybean_supply_demand (
    id BIGSERIAL PRIMARY KEY,

    -- Natural key
    crop_year VARCHAR(20) NOT NULL,
    item_type VARCHAR(100) NOT NULL,

    -- Value (in 1000 metric tons as published)
    value_1000t NUMERIC(15, 3),

    -- Raw fields
    raw_item_name VARCHAR(200),
    raw_value VARCHAR(100),

    -- Metadata
    source VARCHAR(50) DEFAULT 'CONAB',
    ingest_run_id UUID,

    -- Timestamps
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),

    -- Natural key constraint
    UNIQUE(crop_year, item_type)
);

CREATE INDEX IF NOT EXISTS idx_bronze_conab_soy_sd_year
    ON bronze.conab_soybean_supply_demand(crop_year);
CREATE INDEX IF NOT EXISTS idx_bronze_conab_soy_sd_item
    ON bronze.conab_soybean_supply_demand(item_type);

COMMENT ON TABLE bronze.conab_soybean_supply_demand IS
    'Raw CONAB soybean supply and demand balance sheet items.';

-- -----------------------------------------------------------------------------
-- Bronze: CONAB Soybean Prices (Raw)
-- Stores price data from CONAB
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS bronze.conab_soybean_prices (
    id BIGSERIAL PRIMARY KEY,

    -- Natural key
    price_date DATE NOT NULL,
    state VARCHAR(50) NOT NULL,
    municipality VARCHAR(200),

    -- Price data
    price_brl_per_60kg NUMERIC(12, 4),
    price_brl_per_ton NUMERIC(12, 4),

    -- Metadata
    source VARCHAR(50) DEFAULT 'CONAB',
    ingest_run_id UUID,

    -- Timestamps
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),

    -- Natural key constraint
    UNIQUE(price_date, state, COALESCE(municipality, ''))
);

CREATE INDEX IF NOT EXISTS idx_bronze_conab_soy_price_date
    ON bronze.conab_soybean_prices(price_date);
CREATE INDEX IF NOT EXISTS idx_bronze_conab_soy_price_state
    ON bronze.conab_soybean_prices(state);

COMMENT ON TABLE bronze.conab_soybean_prices IS
    'Raw CONAB soybean price data by state and municipality.';

-- =============================================================================
-- SILVER LAYER: Standardized Data
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Silver: CONAB Soybean Production (Standardized)
-- Cleaned and standardized production data with derived metrics
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS silver.conab_soybean_production (
    id BIGSERIAL PRIMARY KEY,

    -- Keys
    crop_year VARCHAR(20) NOT NULL,
    marketing_year_start DATE,
    marketing_year_end DATE,
    state VARCHAR(50) NOT NULL DEFAULT 'BRASIL',

    -- Standardized production data (full units, not 1000s)
    planted_area_ha NUMERIC(18, 2),          -- Hectares
    harvested_area_ha NUMERIC(18, 2),        -- Hectares
    production_mt NUMERIC(18, 2),            -- Metric tons
    production_mmt NUMERIC(12, 4),           -- Million metric tons
    yield_kg_ha NUMERIC(12, 4),              -- kg/ha
    yield_mt_ha NUMERIC(12, 6),              -- MT/ha

    -- Year-over-year changes
    production_yoy_change NUMERIC(18, 2),
    production_yoy_pct NUMERIC(10, 4),
    area_yoy_change NUMERIC(18, 2),
    area_yoy_pct NUMERIC(10, 4),
    yield_yoy_change NUMERIC(12, 4),
    yield_yoy_pct NUMERIC(10, 4),

    -- Comparisons to historical averages
    production_vs_5yr_avg NUMERIC(10, 4),
    yield_vs_5yr_avg NUMERIC(10, 4),

    -- Quality indicators
    quality_flag VARCHAR(50) DEFAULT 'OK',
    data_source VARCHAR(50) DEFAULT 'CONAB',

    -- Timestamps
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),

    -- Natural key constraint
    UNIQUE(crop_year, state)
);

CREATE INDEX IF NOT EXISTS idx_silver_conab_soy_prod_year
    ON silver.conab_soybean_production(crop_year);
CREATE INDEX IF NOT EXISTS idx_silver_conab_soy_prod_state
    ON silver.conab_soybean_production(state);
CREATE INDEX IF NOT EXISTS idx_silver_conab_soy_prod_quality
    ON silver.conab_soybean_production(quality_flag);

COMMENT ON TABLE silver.conab_soybean_production IS
    'Standardized CONAB soybean production data with derived metrics.';

-- -----------------------------------------------------------------------------
-- Silver: CONAB Soybean Balance Sheet (Standardized)
-- Cleaned supply/demand balance sheet
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS silver.conab_soybean_balance_sheet (
    id BIGSERIAL PRIMARY KEY,

    -- Keys
    crop_year VARCHAR(20) NOT NULL UNIQUE,
    marketing_year_start DATE,
    marketing_year_end DATE,

    -- Supply (metric tons)
    beginning_stocks_mt NUMERIC(18, 2),
    production_mt NUMERIC(18, 2),
    imports_mt NUMERIC(18, 2),
    total_supply_mt NUMERIC(18, 2),

    -- Demand (metric tons)
    domestic_consumption_mt NUMERIC(18, 2),
    crush_mt NUMERIC(18, 2),
    food_use_mt NUMERIC(18, 2),
    seed_mt NUMERIC(18, 2),
    exports_mt NUMERIC(18, 2),
    total_use_mt NUMERIC(18, 2),

    -- Balance
    ending_stocks_mt NUMERIC(18, 2),

    -- Ratios
    stocks_to_use_ratio NUMERIC(10, 6),
    export_share_pct NUMERIC(10, 4),
    crush_share_pct NUMERIC(10, 4),

    -- Year-over-year changes
    production_yoy_pct NUMERIC(10, 4),
    exports_yoy_pct NUMERIC(10, 4),
    ending_stocks_yoy_pct NUMERIC(10, 4),

    -- Quality indicators
    quality_flag VARCHAR(50) DEFAULT 'OK',
    data_source VARCHAR(50) DEFAULT 'CONAB',

    -- Timestamps
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_silver_conab_soy_bs_year
    ON silver.conab_soybean_balance_sheet(crop_year);
CREATE INDEX IF NOT EXISTS idx_silver_conab_soy_bs_quality
    ON silver.conab_soybean_balance_sheet(quality_flag);

COMMENT ON TABLE silver.conab_soybean_balance_sheet IS
    'Standardized CONAB soybean supply and demand balance sheet.';

-- =============================================================================
-- GOLD LAYER: Analytics Views
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Gold View: Brazil Soybean Summary
-- High-level summary of Brazilian soybean production
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW gold.brazil_soybean_summary AS
SELECT
    crop_year,
    state,
    production_mmt,
    planted_area_ha / 1000000 AS planted_area_mha,
    yield_mt_ha,
    production_yoy_pct,
    production_vs_5yr_avg,
    quality_flag
FROM silver.conab_soybean_production
ORDER BY crop_year DESC, state;

COMMENT ON VIEW gold.brazil_soybean_summary IS
    'Summary view of Brazilian soybean production by year and state.';

-- -----------------------------------------------------------------------------
-- Gold View: Brazil Soybean National Production
-- National-level production trends
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW gold.brazil_soybean_national AS
SELECT
    crop_year,
    production_mmt,
    planted_area_ha / 1000000 AS planted_area_mha,
    harvested_area_ha / 1000000 AS harvested_area_mha,
    yield_mt_ha,
    production_yoy_pct,
    production_vs_5yr_avg
FROM silver.conab_soybean_production
WHERE state = 'BRASIL'
ORDER BY crop_year DESC;

COMMENT ON VIEW gold.brazil_soybean_national IS
    'National-level Brazilian soybean production trends.';

-- -----------------------------------------------------------------------------
-- Gold View: Brazil Soybean By State
-- State-level production with rankings
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW gold.brazil_soybean_by_state AS
SELECT
    crop_year,
    state,
    production_mmt,
    RANK() OVER (PARTITION BY crop_year ORDER BY production_mmt DESC) as rank_in_year,
    production_mmt * 100.0 / SUM(production_mmt) OVER (PARTITION BY crop_year) as share_pct
FROM silver.conab_soybean_production
WHERE state != 'BRASIL'
ORDER BY crop_year DESC, production_mmt DESC;

COMMENT ON VIEW gold.brazil_soybean_by_state IS
    'Brazilian soybean production by state with rankings and market share.';

-- -----------------------------------------------------------------------------
-- Gold View: Brazil Soybean Yield Trends
-- Historical yield analysis with moving averages
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW gold.brazil_soybean_yield_trends AS
SELECT
    crop_year,
    yield_mt_ha,
    AVG(yield_mt_ha) OVER (
        ORDER BY crop_year
        ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
    ) as yield_5yr_ma,
    yield_yoy_pct,
    yield_vs_5yr_avg
FROM silver.conab_soybean_production
WHERE state = 'BRASIL'
ORDER BY crop_year DESC;

COMMENT ON VIEW gold.brazil_soybean_yield_trends IS
    'Brazilian soybean yield trends with 5-year moving average.';

-- -----------------------------------------------------------------------------
-- Gold View: Brazil Soybean Balance Sheet
-- Formatted balance sheet for analysis
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW gold.brazil_soybean_balance_sheet AS
SELECT
    crop_year,
    -- Supply (MMT)
    beginning_stocks_mt / 1000000 AS beginning_stocks_mmt,
    production_mt / 1000000 AS production_mmt,
    imports_mt / 1000000 AS imports_mmt,
    total_supply_mt / 1000000 AS total_supply_mmt,
    -- Demand (MMT)
    domestic_consumption_mt / 1000000 AS domestic_consumption_mmt,
    crush_mt / 1000000 AS crush_mmt,
    exports_mt / 1000000 AS exports_mmt,
    total_use_mt / 1000000 AS total_use_mmt,
    -- Balance (MMT)
    ending_stocks_mt / 1000000 AS ending_stocks_mmt,
    -- Ratios
    stocks_to_use_ratio,
    export_share_pct,
    -- Changes
    production_yoy_pct,
    exports_yoy_pct,
    ending_stocks_yoy_pct
FROM silver.conab_soybean_balance_sheet
ORDER BY crop_year DESC;

COMMENT ON VIEW gold.brazil_soybean_balance_sheet IS
    'Brazilian soybean supply and demand balance sheet (in MMT).';

-- =============================================================================
-- HELPER FUNCTIONS
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Function: Get Brazilian marketing year dates
-- Brazilian soybean marketing year: February - January
-- -----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION public.get_brazil_soybean_marketing_year(
    p_crop_year VARCHAR(20)
)
RETURNS TABLE(start_date DATE, end_date DATE) AS $$
DECLARE
    v_start_year INTEGER;
BEGIN
    -- Extract start year from crop_year (e.g., '2023/24' -> 2023)
    v_start_year := CAST(SUBSTRING(p_crop_year FROM 1 FOR 4) AS INTEGER);

    RETURN QUERY SELECT
        (v_start_year || '-02-01')::DATE,
        (v_start_year + 1 || '-01-31')::DATE;
END;
$$ LANGUAGE plpgsql IMMUTABLE;

COMMENT ON FUNCTION public.get_brazil_soybean_marketing_year IS
    'Returns start and end dates for Brazilian soybean marketing year.';

-- =============================================================================
-- SAMPLE DATA UPSERT PATTERNS
-- =============================================================================

-- Example: Upsert production record
--
-- INSERT INTO bronze.conab_soybean_production (
--     crop_year, state, commodity, commodity_pt,
--     planted_area_1000ha, harvested_area_1000ha,
--     production_1000t, yield_kg_ha,
--     source, ingest_run_id
-- ) VALUES (
--     '2023/24', 'BRASIL', 'SOYBEANS', 'SOJA',
--     45000, 44500, 150000, 3371,
--     'CONAB', 'abc123...'
-- )
-- ON CONFLICT (crop_year, state)
-- DO UPDATE SET
--     planted_area_1000ha = EXCLUDED.planted_area_1000ha,
--     harvested_area_1000ha = EXCLUDED.harvested_area_1000ha,
--     production_1000t = EXCLUDED.production_1000t,
--     yield_kg_ha = EXCLUDED.yield_kg_ha,
--     ingest_run_id = EXCLUDED.ingest_run_id,
--     updated_at = NOW();

-- =============================================================================
-- END OF CONAB SOYBEAN SCHEMA
-- =============================================================================
