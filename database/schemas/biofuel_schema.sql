-- ============================================================================
-- RLC Biofuel Holding Sheet — PostgreSQL Gold Schema
-- Round Lakes Companies
--
-- Creates gold-layer views/tables for the BiofuelDataUpdater VBA module.
-- Data flows: collectors → bronze tables → silver transforms → gold views
--
-- The VBA module queries gold.* views only.
-- Collectors write to bronze.* tables.
-- Silver layer handles normalization and quality checks.
--
-- Usage: psql -U postgres -d rlc_commodities -f biofuel_schema.sql
-- ============================================================================

-- Create schemas if they don't exist
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

-- ═══════════════════════════════════════════════════════════════════════════
-- BRONZE LAYER — Raw collector output, append-only
-- ═══════════════════════════════════════════════════════════════════════════

-- EPA EMTS RIN data (raw from collector)
CREATE TABLE IF NOT EXISTS bronze.emts_rin_data (
    id SERIAL PRIMARY KEY,
    period DATE NOT NULL,
    d_code VARCHAR(5) NOT NULL,
    rin_type VARCHAR(30) NOT NULL,  -- 'generation', 'separation', 'retirement', 'available'
    fuel_category VARCHAR(50),      -- 'biodiesel', 'renewable_diesel', 'other', NULL
    rin_count NUMERIC(14,1),        -- millions of RINs
    physical_volume NUMERIC(12,1),  -- million gallons (generation only)
    rvo_target NUMERIC(10,1),       -- billion gallons (retirement only)
    collected_at TIMESTAMPTZ DEFAULT NOW(),
    source_url VARCHAR(500),
    collector_version VARCHAR(20),
    run_id UUID,
    UNIQUE (period, d_code, rin_type, fuel_category)
);

-- EIA feedstock consumption (raw from Form 819 Table 2 PDF parsing)
CREATE TABLE IF NOT EXISTS bronze.eia_feedstock_raw (
    id SERIAL PRIMARY KEY,
    period DATE NOT NULL,
    feedstock_name VARCHAR(100) NOT NULL,
    plant_type VARCHAR(30),  -- 'biodiesel', 'renewable_diesel', 'all'
    quantity_mil_lbs NUMERIC(10,1),
    collected_at TIMESTAMPTZ DEFAULT NOW(),
    source_url VARCHAR(500),
    collector_version VARCHAR(20),
    run_id UUID,
    UNIQUE (period, feedstock_name, plant_type)
);

-- EIA biofuel production (raw from API series)
CREATE TABLE IF NOT EXISTS bronze.eia_biofuel_raw (
    id SERIAL PRIMARY KEY,
    period DATE NOT NULL,
    series_id VARCHAR(100) NOT NULL,
    series_name VARCHAR(200),
    value NUMERIC(12,2),
    units VARCHAR(50),
    collected_at TIMESTAMPTZ DEFAULT NOW(),
    source_url VARCHAR(500),
    collector_version VARCHAR(20),
    run_id UUID,
    UNIQUE (period, series_id)
);

-- EIA capacity (raw from annual XLSX download)
CREATE TABLE IF NOT EXISTS bronze.eia_capacity_raw (
    id SERIAL PRIMARY KEY,
    company VARCHAR(200),
    plant_location VARCHAR(200),
    state VARCHAR(5),
    padd VARCHAR(10),
    fuel_type VARCHAR(50),
    nameplate_capacity_mmgy NUMERIC(10,1),
    operable_status VARCHAR(50),
    year_online INTEGER,
    feedstock_primary VARCHAR(100),
    notes VARCHAR(500),
    collected_at TIMESTAMPTZ DEFAULT NOW(),
    source_url VARCHAR(500),
    collector_version VARCHAR(20),
    run_id UUID
);

-- ═══════════════════════════════════════════════════════════════════════════
-- GOLD LAYER — Views that the VBA module queries
-- These will eventually be views over silver tables, but we create them
-- as tables initially so the collectors can write directly while silver
-- transforms are being built.
-- ═══════════════════════════════════════════════════════════════════════════

-- Gold: RIN Generation (Sheet 1)
CREATE TABLE IF NOT EXISTS gold.rin_generation (
    year INTEGER NOT NULL,
    month INTEGER NOT NULL,
    d3_rins NUMERIC(12,1),
    d4_rins NUMERIC(12,1),
    d5_rins NUMERIC(12,1),
    d6_rins NUMERIC(12,1),
    d7_rins NUMERIC(12,1),
    d4_biodiesel_rins NUMERIC(12,1),
    d4_rd_rins NUMERIC(12,1),
    d4_other_rins NUMERIC(12,1),
    physical_vol_total NUMERIC(12,1),
    physical_vol_biodiesel NUMERIC(12,1),
    physical_vol_rd NUMERIC(12,1),
    PRIMARY KEY (year, month)
);

-- Gold: RIN Separation & Available (Sheet 2)
CREATE TABLE IF NOT EXISTS gold.rin_separation (
    year INTEGER NOT NULL,
    month INTEGER NOT NULL,
    sep_d3 NUMERIC(12,1), sep_d4 NUMERIC(12,1), sep_d5 NUMERIC(12,1),
    sep_d6 NUMERIC(12,1), sep_d7 NUMERIC(12,1),
    avail_d3 NUMERIC(12,1), avail_d4 NUMERIC(12,1), avail_d5 NUMERIC(12,1),
    avail_d6 NUMERIC(12,1), avail_d7 NUMERIC(12,1),
    PRIMARY KEY (year, month)
);

-- Gold: RIN Retirement (Sheet 3)
CREATE TABLE IF NOT EXISTS gold.rin_retirement (
    year INTEGER NOT NULL,
    month INTEGER NOT NULL,
    ret_d3 NUMERIC(12,1), ret_d4 NUMERIC(12,1), ret_d5 NUMERIC(12,1),
    ret_d6 NUMERIC(12,1), ret_d7 NUMERIC(12,1),
    rvo_target NUMERIC(10,1),
    PRIMARY KEY (year, month)
);

-- Gold: Feedstock Consumption (Sheet 4)
CREATE TABLE IF NOT EXISTS gold.eia_feedstock (
    year INTEGER NOT NULL,
    month INTEGER NOT NULL,
    soybean_oil NUMERIC(10,1),
    corn_oil NUMERIC(10,1),
    canola_oil NUMERIC(10,1),
    palm_oil NUMERIC(10,1),
    other_veg_oil NUMERIC(10,1),
    tallow NUMERIC(10,1),
    poultry_fat NUMERIC(10,1),
    white_grease NUMERIC(10,1),
    yellow_grease_uco NUMERIC(10,1),
    other_animal NUMERIC(10,1),
    distillers_corn_oil NUMERIC(10,1),
    tall_oil NUMERIC(10,1),
    waste_oils_fats NUMERIC(10,1),
    other_biomass NUMERIC(10,1),
    total_biodiesel_plants NUMERIC(10,1),
    total_rd_plants NUMERIC(10,1),
    PRIMARY KEY (year, month)
);
COMMENT ON TABLE gold.eia_feedstock IS 'EIA Form 819 Table 2 feedstock consumption, monthly (mil lbs)';

-- Gold: Feedstock by Plant Type (Sheet 5)
CREATE TABLE IF NOT EXISTS gold.eia_feedstock_plant_type (
    year INTEGER NOT NULL,
    month INTEGER NOT NULL,
    sbo_biodiesel NUMERIC(10,1),
    sbo_rd NUMERIC(10,1),
    corn_oil_biodiesel NUMERIC(10,1),
    corn_oil_rd NUMERIC(10,1),
    tallow_biodiesel NUMERIC(10,1),
    tallow_rd NUMERIC(10,1),
    uco_biodiesel NUMERIC(10,1),
    uco_rd NUMERIC(10,1),
    all_feeds_biodiesel NUMERIC(10,1),
    all_feeds_rd NUMERIC(10,1),
    PRIMARY KEY (year, month)
);

-- Gold: Biofuel Production (Sheet 6)
CREATE TABLE IF NOT EXISTS gold.eia_biofuel_production (
    year INTEGER NOT NULL,
    month INTEGER NOT NULL,
    ethanol_prod_kbd NUMERIC(10,1),
    biodiesel_prod_kbd NUMERIC(10,1),
    rd_prod_kbd NUMERIC(10,1),
    other_biofuel_prod_kbd NUMERIC(10,1),
    ethanol_stocks_kbbl INTEGER,
    biodiesel_stocks_kbbl INTEGER,
    rd_stocks_kbbl INTEGER,
    PRIMARY KEY (year, month)
);

-- Gold: Biofuel Trade (Sheet 7)
CREATE TABLE IF NOT EXISTS gold.eia_biofuel_trade (
    year INTEGER NOT NULL,
    month INTEGER NOT NULL,
    ethanol_imports INTEGER,
    ethanol_exports INTEGER,
    biodiesel_imports INTEGER,
    biodiesel_exports INTEGER,
    rd_imports INTEGER,
    rd_exports INTEGER,
    PRIMARY KEY (year, month)
);

-- Gold: Capacity (Sheet 8)
CREATE TABLE IF NOT EXISTS gold.eia_biofuel_capacity (
    id SERIAL PRIMARY KEY,
    company VARCHAR(200),
    plant_location VARCHAR(200),
    state VARCHAR(5),
    padd VARCHAR(10),
    fuel_type VARCHAR(50),
    nameplate_capacity_mmgy NUMERIC(10,1),
    operable_status VARCHAR(50),
    year_online INTEGER,
    feedstock_primary VARCHAR(100),
    notes VARCHAR(500)
);
CREATE INDEX IF NOT EXISTS idx_cap_fuel_type ON gold.eia_biofuel_capacity(fuel_type);
CREATE INDEX IF NOT EXISTS idx_cap_state ON gold.eia_biofuel_capacity(state);

-- Gold: Blending Context (Sheet 9)
CREATE TABLE IF NOT EXISTS gold.eia_blending_context (
    year INTEGER NOT NULL,
    month INTEGER NOT NULL,
    gasoline_prod_kbd NUMERIC(10,1),
    gasoline_demand_kbd NUMERIC(10,1),
    gasoline_stocks_kbbl INTEGER,
    diesel_prod_kbd NUMERIC(10,1),
    diesel_demand_kbd NUMERIC(10,1),
    diesel_stocks_kbbl INTEGER,
    PRIMARY KEY (year, month)
);
