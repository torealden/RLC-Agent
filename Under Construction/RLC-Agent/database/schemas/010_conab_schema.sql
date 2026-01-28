-- =============================================================================
-- CONAB Brazilian Crop Data Schema (All Commodities)
-- Version: 2.0.0
-- =============================================================================
--
-- This schema supports the CONAB data pipeline for Brazilian agricultural
-- production, supply/demand, and price data across all commodities.
--
-- Commodities supported:
-- - Soybeans (soja)
-- - Corn (milho) - first crop and safrinha
-- - Wheat (trigo)
-- - Rice (arroz)
-- - Cotton (algodao)
-- - Sorghum (sorgo)
-- - Barley (cevada)
-- - Beans (feijao)
-- - Oats (aveia)
-- - Sunflower (girassol)
-- - Peanuts (amendoim)
-- - Canola (canola)
-- - Castor bean (mamona)
--
-- Source: CONAB (Companhia Nacional de Abastecimento)
-- URL: https://www.gov.br/conab/pt-br/atuacao/informacoes-agropecuarias/safras
--
-- NOTE: This schema replaces the soybean-specific 010_conab_soybean.sql
-- =============================================================================

-- =============================================================================
-- BRONZE LAYER: Raw Data Storage
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Bronze: CONAB Production (Raw) - All Commodities
-- Stores production data exactly as received from CONAB Excel/CSV files
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS bronze.conab_production (
    id BIGSERIAL PRIMARY KEY,

    -- Natural key (crop_year + state + commodity)
    crop_year VARCHAR(20) NOT NULL,
    state VARCHAR(50) NOT NULL DEFAULT 'BRASIL',
    commodity VARCHAR(50) NOT NULL,
    commodity_pt VARCHAR(50),

    -- Production data (in 1000 units as published)
    planted_area_1000ha NUMERIC(15, 3),
    harvested_area_1000ha NUMERIC(15, 3),
    production_1000t NUMERIC(15, 3),
    yield_kg_ha NUMERIC(15, 3),

    -- Additional fields for corn (first crop vs safrinha)
    crop_type VARCHAR(50) DEFAULT '',  -- 'first_crop', 'safrinha', 'winter' (for wheat)
    region VARCHAR(100),

    -- Metadata
    source VARCHAR(50) DEFAULT 'CONAB',
    sheet_name VARCHAR(200),
    survey_number INTEGER,  -- CONAB releases up to 12 surveys per year
    ingest_run_id UUID,
    file_hash VARCHAR(64),
    raw_data JSONB,

    -- Timestamps
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),

    -- Natural key handled by unique index below
    CONSTRAINT conab_production_natural_key UNIQUE(crop_year, state, commodity, crop_type)
);

CREATE INDEX IF NOT EXISTS idx_bronze_conab_prod_year
    ON bronze.conab_production(crop_year);
CREATE INDEX IF NOT EXISTS idx_bronze_conab_prod_state
    ON bronze.conab_production(state);
CREATE INDEX IF NOT EXISTS idx_bronze_conab_prod_commodity
    ON bronze.conab_production(commodity);
CREATE INDEX IF NOT EXISTS idx_bronze_conab_prod_ingest
    ON bronze.conab_production(ingest_run_id);

COMMENT ON TABLE bronze.conab_production IS
    'Raw CONAB production data for all commodities. One row per crop year, state, and commodity.';

-- -----------------------------------------------------------------------------
-- Bronze: CONAB Supply & Demand (Raw) - All Commodities
-- Stores S&D balance sheet items from CONAB
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS bronze.conab_supply_demand (
    id BIGSERIAL PRIMARY KEY,

    -- Natural key
    crop_year VARCHAR(20) NOT NULL,
    commodity VARCHAR(50) NOT NULL,
    item_type VARCHAR(100) NOT NULL,

    -- Value (in 1000 metric tons as published)
    value_1000t NUMERIC(15, 3),

    -- Raw fields
    raw_item_name VARCHAR(200),
    raw_value VARCHAR(100),

    -- Metadata
    source VARCHAR(50) DEFAULT 'CONAB',
    survey_number INTEGER,
    ingest_run_id UUID,

    -- Timestamps
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),

    -- Natural key constraint
    UNIQUE(crop_year, commodity, item_type)
);

CREATE INDEX IF NOT EXISTS idx_bronze_conab_sd_year
    ON bronze.conab_supply_demand(crop_year);
CREATE INDEX IF NOT EXISTS idx_bronze_conab_sd_commodity
    ON bronze.conab_supply_demand(commodity);
CREATE INDEX IF NOT EXISTS idx_bronze_conab_sd_item
    ON bronze.conab_supply_demand(item_type);

COMMENT ON TABLE bronze.conab_supply_demand IS
    'Raw CONAB supply and demand balance sheet items for all commodities.';

-- -----------------------------------------------------------------------------
-- Bronze: CONAB Prices (Raw) - All Commodities
-- Stores price data from CONAB
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS bronze.conab_prices (
    id BIGSERIAL PRIMARY KEY,

    -- Natural key
    price_date DATE NOT NULL,
    commodity VARCHAR(50) NOT NULL,
    state VARCHAR(50) NOT NULL,
    municipality VARCHAR(200) DEFAULT '',

    -- Price data (Brazilian Real)
    price_brl_per_60kg NUMERIC(12, 4),  -- Standard Brazilian sack
    price_brl_per_ton NUMERIC(12, 4),
    price_brl_per_15kg NUMERIC(12, 4),  -- Used for some commodities
    price_usd_per_ton NUMERIC(12, 4),   -- If available

    -- Exchange rate at time of collection
    usd_brl_rate NUMERIC(10, 4),

    -- Metadata
    source VARCHAR(50) DEFAULT 'CONAB',
    ingest_run_id UUID,

    -- Timestamps
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),

    -- Natural key handled by unique index
    CONSTRAINT conab_prices_natural_key UNIQUE(price_date, commodity, state, municipality)
);

CREATE INDEX IF NOT EXISTS idx_bronze_conab_price_date
    ON bronze.conab_prices(price_date);
CREATE INDEX IF NOT EXISTS idx_bronze_conab_price_commodity
    ON bronze.conab_prices(commodity);
CREATE INDEX IF NOT EXISTS idx_bronze_conab_price_state
    ON bronze.conab_prices(state);

COMMENT ON TABLE bronze.conab_prices IS
    'Raw CONAB price data by commodity, state, and municipality.';

-- =============================================================================
-- SILVER LAYER: Standardized Data
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Silver: CONAB Production (Standardized) - All Commodities
-- Cleaned and standardized production data with derived metrics
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS silver.conab_production (
    id BIGSERIAL PRIMARY KEY,

    -- Keys
    crop_year VARCHAR(20) NOT NULL,
    marketing_year_start DATE,
    marketing_year_end DATE,
    state VARCHAR(50) NOT NULL DEFAULT 'BRASIL',
    commodity VARCHAR(50) NOT NULL,
    crop_type VARCHAR(50) DEFAULT '',  -- 'first_crop', 'safrinha', 'winter', etc.

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

    -- Natural key
    CONSTRAINT silver_conab_production_natural_key UNIQUE(crop_year, state, commodity, crop_type)
);

CREATE INDEX IF NOT EXISTS idx_silver_conab_prod_year
    ON silver.conab_production(crop_year);
CREATE INDEX IF NOT EXISTS idx_silver_conab_prod_state
    ON silver.conab_production(state);
CREATE INDEX IF NOT EXISTS idx_silver_conab_prod_commodity
    ON silver.conab_production(commodity);
CREATE INDEX IF NOT EXISTS idx_silver_conab_prod_quality
    ON silver.conab_production(quality_flag);

COMMENT ON TABLE silver.conab_production IS
    'Standardized CONAB production data for all commodities with derived metrics.';

-- -----------------------------------------------------------------------------
-- Silver: CONAB Balance Sheet (Standardized) - All Commodities
-- Cleaned supply/demand balance sheet
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS silver.conab_balance_sheet (
    id BIGSERIAL PRIMARY KEY,

    -- Keys
    crop_year VARCHAR(20) NOT NULL,
    commodity VARCHAR(50) NOT NULL,
    marketing_year_start DATE,
    marketing_year_end DATE,

    -- Supply (metric tons)
    beginning_stocks_mt NUMERIC(18, 2),
    production_mt NUMERIC(18, 2),
    imports_mt NUMERIC(18, 2),
    total_supply_mt NUMERIC(18, 2),

    -- Demand (metric tons)
    domestic_consumption_mt NUMERIC(18, 2),
    crush_mt NUMERIC(18, 2),           -- For oilseeds
    feed_use_mt NUMERIC(18, 2),        -- For grains
    food_use_mt NUMERIC(18, 2),
    industrial_use_mt NUMERIC(18, 2),  -- Ethanol for corn, etc.
    seed_mt NUMERIC(18, 2),
    exports_mt NUMERIC(18, 2),
    total_use_mt NUMERIC(18, 2),

    -- Balance
    ending_stocks_mt NUMERIC(18, 2),

    -- Ratios
    stocks_to_use_ratio NUMERIC(10, 6),
    export_share_pct NUMERIC(10, 4),
    crush_share_pct NUMERIC(10, 4),
    feed_share_pct NUMERIC(10, 4),

    -- Year-over-year changes
    production_yoy_pct NUMERIC(10, 4),
    exports_yoy_pct NUMERIC(10, 4),
    ending_stocks_yoy_pct NUMERIC(10, 4),

    -- Quality indicators
    quality_flag VARCHAR(50) DEFAULT 'OK',
    data_source VARCHAR(50) DEFAULT 'CONAB',

    -- Timestamps
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),

    -- Natural key constraint
    UNIQUE(crop_year, commodity)
);

CREATE INDEX IF NOT EXISTS idx_silver_conab_bs_year
    ON silver.conab_balance_sheet(crop_year);
CREATE INDEX IF NOT EXISTS idx_silver_conab_bs_commodity
    ON silver.conab_balance_sheet(commodity);
CREATE INDEX IF NOT EXISTS idx_silver_conab_bs_quality
    ON silver.conab_balance_sheet(quality_flag);

COMMENT ON TABLE silver.conab_balance_sheet IS
    'Standardized CONAB supply and demand balance sheet for all commodities.';

-- =============================================================================
-- GOLD LAYER: Analytics Views
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Gold View: Brazil Crop Summary (All Commodities)
-- High-level summary of Brazilian crop production
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW gold.brazil_crop_summary AS
SELECT
    crop_year,
    commodity,
    state,
    crop_type,
    production_mmt,
    planted_area_ha / 1000000 AS planted_area_mha,
    yield_mt_ha,
    production_yoy_pct,
    production_vs_5yr_avg,
    quality_flag
FROM silver.conab_production
ORDER BY crop_year DESC, commodity, state;

COMMENT ON VIEW gold.brazil_crop_summary IS
    'Summary view of Brazilian crop production by year, commodity, and state.';

-- -----------------------------------------------------------------------------
-- Gold View: Brazil National Production (All Commodities)
-- National-level production trends by commodity
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW gold.brazil_national_production AS
SELECT
    crop_year,
    commodity,
    crop_type,
    production_mmt,
    planted_area_ha / 1000000 AS planted_area_mha,
    harvested_area_ha / 1000000 AS harvested_area_mha,
    yield_mt_ha,
    production_yoy_pct,
    production_vs_5yr_avg
FROM silver.conab_production
WHERE state = 'BRASIL'
ORDER BY crop_year DESC, commodity;

COMMENT ON VIEW gold.brazil_national_production IS
    'National-level Brazilian crop production trends for all commodities.';

-- -----------------------------------------------------------------------------
-- Gold View: Brazil Soybeans (Filtered for Soybeans)
-- Backwards compatibility - soybean-specific view
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW gold.brazil_soybean_production AS
SELECT
    crop_year,
    state,
    production_mmt,
    planted_area_ha / 1000000 AS planted_area_mha,
    harvested_area_ha / 1000000 AS harvested_area_mha,
    yield_mt_ha,
    production_yoy_pct,
    production_vs_5yr_avg
FROM silver.conab_production
WHERE commodity = 'soybeans'
ORDER BY crop_year DESC, state;

COMMENT ON VIEW gold.brazil_soybean_production IS
    'Brazilian soybean production - filtered view for backwards compatibility.';

-- -----------------------------------------------------------------------------
-- Gold View: Brazil Corn (Filtered for Corn)
-- Corn-specific view with first crop and safrinha breakdown
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW gold.brazil_corn_production AS
SELECT
    crop_year,
    state,
    crop_type,
    production_mmt,
    planted_area_ha / 1000000 AS planted_area_mha,
    harvested_area_ha / 1000000 AS harvested_area_mha,
    yield_mt_ha,
    production_yoy_pct,
    production_vs_5yr_avg
FROM silver.conab_production
WHERE commodity IN ('corn', 'corn_first_crop', 'corn_safrinha')
ORDER BY crop_year DESC, crop_type, state;

COMMENT ON VIEW gold.brazil_corn_production IS
    'Brazilian corn production with first crop and safrinha breakdown.';

-- -----------------------------------------------------------------------------
-- Gold View: Brazil Wheat (Filtered for Wheat)
-- Wheat-specific view
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW gold.brazil_wheat_production AS
SELECT
    crop_year,
    state,
    production_mmt,
    planted_area_ha / 1000000 AS planted_area_mha,
    harvested_area_ha / 1000000 AS harvested_area_mha,
    yield_mt_ha,
    production_yoy_pct,
    production_vs_5yr_avg
FROM silver.conab_production
WHERE commodity = 'wheat'
ORDER BY crop_year DESC, state;

COMMENT ON VIEW gold.brazil_wheat_production IS
    'Brazilian wheat production by year and state.';

-- -----------------------------------------------------------------------------
-- Gold View: Brazil Balance Sheet by Commodity
-- Formatted balance sheet for all commodities
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW gold.brazil_balance_sheet AS
SELECT
    crop_year,
    commodity,
    -- Supply (MMT)
    beginning_stocks_mt / 1000000 AS beginning_stocks_mmt,
    production_mt / 1000000 AS production_mmt,
    imports_mt / 1000000 AS imports_mmt,
    total_supply_mt / 1000000 AS total_supply_mmt,
    -- Demand (MMT)
    domestic_consumption_mt / 1000000 AS domestic_consumption_mmt,
    crush_mt / 1000000 AS crush_mmt,
    feed_use_mt / 1000000 AS feed_use_mmt,
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
FROM silver.conab_balance_sheet
ORDER BY crop_year DESC, commodity;

COMMENT ON VIEW gold.brazil_balance_sheet IS
    'Brazilian supply and demand balance sheet for all commodities (in MMT).';

-- -----------------------------------------------------------------------------
-- Gold View: Brazil Production by State Rankings
-- State-level production rankings for each commodity
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW gold.brazil_production_by_state AS
SELECT
    crop_year,
    commodity,
    state,
    production_mmt,
    RANK() OVER (PARTITION BY crop_year, commodity ORDER BY production_mmt DESC) as rank_in_year,
    production_mmt * 100.0 / SUM(production_mmt) OVER (PARTITION BY crop_year, commodity) as share_pct
FROM silver.conab_production
WHERE state != 'BRASIL'
ORDER BY crop_year DESC, commodity, production_mmt DESC;

COMMENT ON VIEW gold.brazil_production_by_state IS
    'Brazilian crop production by state with rankings and market share.';

-- -----------------------------------------------------------------------------
-- Gold View: Brazil Corn Ethanol Potential
-- Corn available for ethanol production (industrial use)
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW gold.brazil_corn_ethanol AS
SELECT
    crop_year,
    commodity,
    production_mt / 1000000 AS production_mmt,
    industrial_use_mt / 1000000 AS industrial_use_mmt,  -- Ethanol production
    CASE
        WHEN production_mt > 0
        THEN (industrial_use_mt / production_mt) * 100
        ELSE NULL
    END AS ethanol_share_pct,
    exports_mt / 1000000 AS exports_mmt,
    ending_stocks_mt / 1000000 AS ending_stocks_mmt
FROM silver.conab_balance_sheet
WHERE commodity IN ('corn', 'corn_first_crop', 'corn_safrinha')
ORDER BY crop_year DESC;

COMMENT ON VIEW gold.brazil_corn_ethanol IS
    'Brazilian corn for ethanol production analysis.';

-- =============================================================================
-- REFERENCE DATA
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Reference: Commodity Mapping (Portuguese to English)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS public.conab_commodity_map (
    id SERIAL PRIMARY KEY,
    commodity_pt VARCHAR(50) NOT NULL UNIQUE,
    commodity_en VARCHAR(50) NOT NULL,
    category VARCHAR(50),  -- 'oilseeds', 'grains', 'fiber'
    unit VARCHAR(20) DEFAULT '1000 t',
    notes TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Insert commodity mappings
INSERT INTO public.conab_commodity_map (commodity_pt, commodity_en, category, notes) VALUES
    ('soja', 'soybeans', 'oilseeds', 'Primary oilseed crop'),
    ('milho', 'corn', 'grains', 'Total corn (all crops)'),
    ('milho_1_safra', 'corn_first_crop', 'grains', 'First crop (summer) corn'),
    ('milho_2_safra', 'corn_safrinha', 'grains', 'Second crop (safrinha) corn'),
    ('trigo', 'wheat', 'grains', 'Winter wheat'),
    ('arroz', 'rice', 'grains', 'Paddy rice'),
    ('algodao', 'cotton', 'fiber', 'Cotton lint'),
    ('feijao', 'beans', 'grains', 'Dry beans'),
    ('sorgo', 'sorghum', 'grains', 'Grain sorghum'),
    ('cevada', 'barley', 'grains', 'Malting and feed barley'),
    ('aveia', 'oats', 'grains', 'Oats'),
    ('girassol', 'sunflower', 'oilseeds', 'Sunflower seed'),
    ('amendoim', 'peanuts', 'oilseeds', 'Groundnuts'),
    ('mamona', 'castor_bean', 'oilseeds', 'Castor bean'),
    ('canola', 'canola', 'oilseeds', 'Rapeseed/canola')
ON CONFLICT (commodity_pt) DO NOTHING;

COMMENT ON TABLE public.conab_commodity_map IS
    'Reference table mapping CONAB Portuguese commodity names to English.';

-- -----------------------------------------------------------------------------
-- Reference: Brazilian States
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS public.brazil_states (
    id SERIAL PRIMARY KEY,
    state_code VARCHAR(2) NOT NULL UNIQUE,
    state_name VARCHAR(100) NOT NULL,
    region VARCHAR(50),
    is_major_producer BOOLEAN DEFAULT FALSE,
    notes TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Insert Brazilian states
INSERT INTO public.brazil_states (state_code, state_name, region, is_major_producer) VALUES
    ('AC', 'Acre', 'Norte', FALSE),
    ('AL', 'Alagoas', 'Nordeste', FALSE),
    ('AP', 'Amapa', 'Norte', FALSE),
    ('AM', 'Amazonas', 'Norte', FALSE),
    ('BA', 'Bahia', 'Nordeste', TRUE),
    ('CE', 'Ceara', 'Nordeste', FALSE),
    ('DF', 'Distrito Federal', 'Centro-Oeste', FALSE),
    ('ES', 'Espirito Santo', 'Sudeste', FALSE),
    ('GO', 'Goias', 'Centro-Oeste', TRUE),
    ('MA', 'Maranhao', 'Nordeste', TRUE),
    ('MT', 'Mato Grosso', 'Centro-Oeste', TRUE),
    ('MS', 'Mato Grosso do Sul', 'Centro-Oeste', TRUE),
    ('MG', 'Minas Gerais', 'Sudeste', TRUE),
    ('PA', 'Para', 'Norte', FALSE),
    ('PB', 'Paraiba', 'Nordeste', FALSE),
    ('PR', 'Parana', 'Sul', TRUE),
    ('PE', 'Pernambuco', 'Nordeste', FALSE),
    ('PI', 'Piaui', 'Nordeste', TRUE),
    ('RJ', 'Rio de Janeiro', 'Sudeste', FALSE),
    ('RN', 'Rio Grande do Norte', 'Nordeste', FALSE),
    ('RS', 'Rio Grande do Sul', 'Sul', TRUE),
    ('RO', 'Rondonia', 'Norte', FALSE),
    ('RR', 'Roraima', 'Norte', FALSE),
    ('SC', 'Santa Catarina', 'Sul', TRUE),
    ('SP', 'Sao Paulo', 'Sudeste', TRUE),
    ('SE', 'Sergipe', 'Nordeste', FALSE),
    ('TO', 'Tocantins', 'Norte', TRUE)
ON CONFLICT (state_code) DO NOTHING;

COMMENT ON TABLE public.brazil_states IS
    'Reference table of Brazilian states with agricultural production indicators.';

-- =============================================================================
-- HELPER FUNCTIONS
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Function: Get Brazilian marketing year dates
-- Different crops have different marketing years
-- -----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION public.get_brazil_marketing_year(
    p_crop_year VARCHAR(20),
    p_commodity VARCHAR(50) DEFAULT 'soybeans'
)
RETURNS TABLE(start_date DATE, end_date DATE) AS $$
DECLARE
    v_start_year INTEGER;
BEGIN
    -- Extract start year from crop_year (e.g., '2023/24' -> 2023)
    v_start_year := CAST(SUBSTRING(p_crop_year FROM 1 FOR 4) AS INTEGER);

    -- Marketing years vary by commodity
    IF p_commodity IN ('soybeans', 'corn', 'corn_first_crop', 'corn_safrinha') THEN
        -- Soybeans and corn: February - January
        RETURN QUERY SELECT
            (v_start_year || '-02-01')::DATE,
            (v_start_year + 1 || '-01-31')::DATE;
    ELSIF p_commodity = 'wheat' THEN
        -- Wheat: August - July
        RETURN QUERY SELECT
            (v_start_year || '-08-01')::DATE,
            (v_start_year + 1 || '-07-31')::DATE;
    ELSE
        -- Default: Calendar year
        RETURN QUERY SELECT
            (v_start_year || '-01-01')::DATE,
            (v_start_year || '-12-31')::DATE;
    END IF;
END;
$$ LANGUAGE plpgsql IMMUTABLE;

COMMENT ON FUNCTION public.get_brazil_marketing_year IS
    'Returns start and end dates for Brazilian marketing year by commodity.';

-- -----------------------------------------------------------------------------
-- Function: Convert Portuguese commodity name to English
-- -----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION public.conab_commodity_translate(
    p_commodity_pt VARCHAR(50)
)
RETURNS VARCHAR(50) AS $$
DECLARE
    v_commodity_en VARCHAR(50);
BEGIN
    SELECT commodity_en INTO v_commodity_en
    FROM public.conab_commodity_map
    WHERE commodity_pt = LOWER(p_commodity_pt);

    RETURN COALESCE(v_commodity_en, p_commodity_pt);
END;
$$ LANGUAGE plpgsql STABLE;

COMMENT ON FUNCTION public.conab_commodity_translate IS
    'Translates CONAB Portuguese commodity names to English.';

-- =============================================================================
-- MIGRATION: Copy data from old soybean-specific tables if they exist
-- =============================================================================

-- This DO block safely migrates data from old tables if they exist
DO $$
BEGIN
    -- Migrate production data
    IF EXISTS (SELECT 1 FROM information_schema.tables
               WHERE table_schema = 'bronze'
               AND table_name = 'conab_soybean_production') THEN
        INSERT INTO bronze.conab_production (
            crop_year, state, commodity, commodity_pt,
            planted_area_1000ha, harvested_area_1000ha,
            production_1000t, yield_kg_ha,
            source, sheet_name, ingest_run_id, file_hash, raw_data,
            created_at, updated_at
        )
        SELECT
            crop_year, state, commodity, commodity_pt,
            planted_area_1000ha, harvested_area_1000ha,
            production_1000t, yield_kg_ha,
            source, sheet_name, ingest_run_id, file_hash, raw_data,
            created_at, updated_at
        FROM bronze.conab_soybean_production
        ON CONFLICT ON CONSTRAINT conab_production_natural_key
        DO NOTHING;

        RAISE NOTICE 'Migrated data from bronze.conab_soybean_production';
    END IF;

    -- Migrate S&D data
    IF EXISTS (SELECT 1 FROM information_schema.tables
               WHERE table_schema = 'bronze'
               AND table_name = 'conab_soybean_supply_demand') THEN
        INSERT INTO bronze.conab_supply_demand (
            crop_year, commodity, item_type, value_1000t,
            raw_item_name, raw_value, source, ingest_run_id,
            created_at, updated_at
        )
        SELECT
            crop_year, 'soybeans', item_type, value_1000t,
            raw_item_name, raw_value, source, ingest_run_id,
            created_at, updated_at
        FROM bronze.conab_soybean_supply_demand
        ON CONFLICT (crop_year, commodity, item_type)
        DO NOTHING;

        RAISE NOTICE 'Migrated data from bronze.conab_soybean_supply_demand';
    END IF;

    -- Migrate price data
    IF EXISTS (SELECT 1 FROM information_schema.tables
               WHERE table_schema = 'bronze'
               AND table_name = 'conab_soybean_prices') THEN
        INSERT INTO bronze.conab_prices (
            price_date, commodity, state, municipality,
            price_brl_per_60kg, price_brl_per_ton,
            source, ingest_run_id, created_at, updated_at
        )
        SELECT
            price_date, 'soybeans', state, municipality,
            price_brl_per_60kg, price_brl_per_ton,
            source, ingest_run_id, created_at, updated_at
        FROM bronze.conab_soybean_prices
        ON CONFLICT ON CONSTRAINT conab_prices_natural_key
        DO NOTHING;

        RAISE NOTICE 'Migrated data from bronze.conab_soybean_prices';
    END IF;

    -- Migrate silver production data
    IF EXISTS (SELECT 1 FROM information_schema.tables
               WHERE table_schema = 'silver'
               AND table_name = 'conab_soybean_production') THEN
        INSERT INTO silver.conab_production (
            crop_year, marketing_year_start, marketing_year_end, state, commodity,
            planted_area_ha, harvested_area_ha, production_mt, production_mmt,
            yield_kg_ha, yield_mt_ha, production_yoy_change, production_yoy_pct,
            area_yoy_change, area_yoy_pct, yield_yoy_change, yield_yoy_pct,
            production_vs_5yr_avg, yield_vs_5yr_avg, quality_flag, data_source,
            created_at, updated_at
        )
        SELECT
            crop_year, marketing_year_start, marketing_year_end, state, 'soybeans',
            planted_area_ha, harvested_area_ha, production_mt, production_mmt,
            yield_kg_ha, yield_mt_ha, production_yoy_change, production_yoy_pct,
            area_yoy_change, area_yoy_pct, yield_yoy_change, yield_yoy_pct,
            production_vs_5yr_avg, yield_vs_5yr_avg, quality_flag, data_source,
            created_at, updated_at
        FROM silver.conab_soybean_production
        ON CONFLICT ON CONSTRAINT conab_production_natural_key
        DO NOTHING;

        RAISE NOTICE 'Migrated data from silver.conab_soybean_production';
    END IF;

    -- Migrate silver balance sheet data
    IF EXISTS (SELECT 1 FROM information_schema.tables
               WHERE table_schema = 'silver'
               AND table_name = 'conab_soybean_balance_sheet') THEN
        INSERT INTO silver.conab_balance_sheet (
            crop_year, commodity, marketing_year_start, marketing_year_end,
            beginning_stocks_mt, production_mt, imports_mt, total_supply_mt,
            domestic_consumption_mt, crush_mt, food_use_mt, seed_mt, exports_mt, total_use_mt,
            ending_stocks_mt, stocks_to_use_ratio, export_share_pct, crush_share_pct,
            production_yoy_pct, exports_yoy_pct, ending_stocks_yoy_pct,
            quality_flag, data_source, created_at, updated_at
        )
        SELECT
            crop_year, 'soybeans', marketing_year_start, marketing_year_end,
            beginning_stocks_mt, production_mt, imports_mt, total_supply_mt,
            domestic_consumption_mt, crush_mt, food_use_mt, seed_mt, exports_mt, total_use_mt,
            ending_stocks_mt, stocks_to_use_ratio, export_share_pct, crush_share_pct,
            production_yoy_pct, exports_yoy_pct, ending_stocks_yoy_pct,
            quality_flag, data_source, created_at, updated_at
        FROM silver.conab_soybean_balance_sheet
        ON CONFLICT (crop_year, commodity)
        DO NOTHING;

        RAISE NOTICE 'Migrated data from silver.conab_soybean_balance_sheet';
    END IF;
END $$;

-- =============================================================================
-- SAMPLE DATA UPSERT PATTERNS
-- =============================================================================

-- Example: Upsert production record for any commodity
--
-- INSERT INTO bronze.conab_production (
--     crop_year, state, commodity, commodity_pt, crop_type,
--     planted_area_1000ha, harvested_area_1000ha,
--     production_1000t, yield_kg_ha,
--     source, survey_number, ingest_run_id
-- ) VALUES (
--     '2024/25', 'MT', 'corn_safrinha', 'milho_2_safra', 'safrinha',
--     5500, 5400, 44000, 8148,
--     'CONAB', 4, 'abc123...'
-- )
-- ON CONFLICT ON CONSTRAINT conab_production_natural_key
-- DO UPDATE SET
--     planted_area_1000ha = EXCLUDED.planted_area_1000ha,
--     harvested_area_1000ha = EXCLUDED.harvested_area_1000ha,
--     production_1000t = EXCLUDED.production_1000t,
--     yield_kg_ha = EXCLUDED.yield_kg_ha,
--     survey_number = EXCLUDED.survey_number,
--     ingest_run_id = EXCLUDED.ingest_run_id,
--     updated_at = NOW();

-- =============================================================================
-- END OF CONAB SCHEMA (All Commodities)
-- =============================================================================
