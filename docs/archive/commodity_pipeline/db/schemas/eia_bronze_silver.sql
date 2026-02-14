-- =============================================================================
-- EIA ENERGY & BIOFUELS - BRONZE AND SILVER SCHEMAS
-- =============================================================================
-- Source: US Energy Information Administration (EIA) API v2
-- Release: Weekly (Wednesday 10:30 ET) for petroleum status
--          Daily for spot prices
--          Weekly (Thursday) for natural gas storage
-- Documentation: https://www.eia.gov/opendata/documentation.php
-- =============================================================================

-- Create schemas if they don't exist
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;
CREATE SCHEMA IF NOT EXISTS meta;

-- =============================================================================
-- BRONZE LAYER - Raw API Responses
-- =============================================================================

-- Raw ingestion table for all EIA data
CREATE TABLE IF NOT EXISTS bronze.eia_raw_ingestion (
    id SERIAL PRIMARY KEY,
    source_system VARCHAR(50) NOT NULL DEFAULT 'eia',
    series_id VARCHAR(100) NOT NULL,           -- e.g., 'crude_oil_stocks', 'wti_spot'
    category VARCHAR(50) NOT NULL,             -- e.g., 'petroleum', 'biofuels', 'natural_gas'
    report_date DATE NOT NULL,                 -- Date the data represents
    publication_date TIMESTAMP,                -- When EIA published
    ingestion_ts TIMESTAMP DEFAULT NOW(),      -- When we captured it
    raw_payload JSONB NOT NULL,                -- Exact API response
    api_route TEXT,                            -- API endpoint used
    api_params JSONB,                          -- Parameters sent
    record_count INTEGER,                      -- Number of records in response
    file_hash VARCHAR(64),                     -- SHA-256 of payload for dedup

    UNIQUE(source_system, series_id, report_date, file_hash)
);

CREATE INDEX IF NOT EXISTS idx_eia_bronze_series_date
    ON bronze.eia_raw_ingestion(series_id, report_date);
CREATE INDEX IF NOT EXISTS idx_eia_bronze_category
    ON bronze.eia_raw_ingestion(category, report_date);
CREATE INDEX IF NOT EXISTS idx_eia_bronze_payload
    ON bronze.eia_raw_ingestion USING GIN(raw_payload);

COMMENT ON TABLE bronze.eia_raw_ingestion IS
'Raw EIA API responses. Immutable archive for audit and reprocessing.';

-- =============================================================================
-- SILVER LAYER - Normalized, Validated Data
-- =============================================================================

-- Petroleum stocks and supply/demand
CREATE TABLE IF NOT EXISTS silver.eia_petroleum_weekly (
    id SERIAL PRIMARY KEY,
    bronze_id INTEGER REFERENCES bronze.eia_raw_ingestion(id),

    -- Temporal
    period_date DATE NOT NULL,                 -- Week ending date
    report_date DATE NOT NULL,                 -- Publication date

    -- Crude Oil
    crude_stocks_total_kb NUMERIC(12,1),       -- Total US crude stocks (thousand barrels)
    crude_stocks_cushing_kb NUMERIC(12,1),     -- Cushing, OK stocks
    crude_stocks_padd2_kb NUMERIC(12,1),       -- PADD 2 (Midwest) stocks
    crude_stocks_padd3_kb NUMERIC(12,1),       -- PADD 3 (Gulf Coast) stocks
    crude_production_kbd NUMERIC(10,1),        -- Production (thousand barrels/day)
    crude_imports_kbd NUMERIC(10,1),           -- Imports (thousand barrels/day)
    refinery_inputs_kbd NUMERIC(10,1),         -- Refinery inputs (thousand barrels/day)
    refinery_utilization_pct NUMERIC(5,2),     -- Refinery utilization (%)

    -- Gasoline
    gasoline_stocks_kb NUMERIC(12,1),          -- Motor gasoline stocks
    gasoline_demand_kbd NUMERIC(10,1),         -- Product supplied (demand proxy)

    -- Distillate (Diesel/Heating Oil)
    distillate_stocks_kb NUMERIC(12,1),        -- Distillate stocks

    -- Ethanol
    ethanol_production_kbd NUMERIC(10,1),      -- Ethanol production
    ethanol_stocks_kb NUMERIC(12,1),           -- Ethanol stocks
    ethanol_imports_kbd NUMERIC(10,1),         -- Ethanol imports

    -- Natural Gas (included for convenience)
    natgas_storage_bcf NUMERIC(10,1),          -- Working gas storage (Bcf)
    natgas_storage_change_bcf NUMERIC(10,1),   -- Net weekly change

    -- Audit
    validation_status VARCHAR(20) DEFAULT 'pending',
    validation_errors JSONB,
    created_ts TIMESTAMP DEFAULT NOW(),
    updated_ts TIMESTAMP DEFAULT NOW(),

    UNIQUE(period_date)
);

CREATE INDEX IF NOT EXISTS idx_eia_petroleum_period
    ON silver.eia_petroleum_weekly(period_date DESC);

COMMENT ON TABLE silver.eia_petroleum_weekly IS
'Weekly EIA petroleum status report data, normalized and validated.';

-- Daily spot prices
CREATE TABLE IF NOT EXISTS silver.eia_spot_prices_daily (
    id SERIAL PRIMARY KEY,
    bronze_id INTEGER REFERENCES bronze.eia_raw_ingestion(id),

    -- Temporal
    price_date DATE NOT NULL,

    -- Crude Oil Prices
    wti_spot_bbl NUMERIC(8,2),                 -- WTI Cushing ($/barrel)
    brent_spot_bbl NUMERIC(8,2),               -- Brent ($/barrel)
    wti_brent_spread NUMERIC(8,2),             -- Calculated: Brent - WTI

    -- Refined Products
    rbob_gasoline_gal NUMERIC(6,4),            -- RBOB ($/gallon)
    ulsd_diesel_gal NUMERIC(6,4),              -- Ultra-low sulfur diesel ($/gallon)
    heating_oil_gal NUMERIC(6,4),              -- No. 2 heating oil ($/gallon)

    -- Crack Spreads (calculated)
    gasoline_crack_bbl NUMERIC(8,2),           -- (RBOB * 42) - WTI
    diesel_crack_bbl NUMERIC(8,2),             -- (ULSD * 42) - WTI

    -- Natural Gas
    henry_hub_mmbtu NUMERIC(6,3),              -- Henry Hub ($/MMBtu)

    -- Audit
    validation_status VARCHAR(20) DEFAULT 'pending',
    created_ts TIMESTAMP DEFAULT NOW(),

    UNIQUE(price_date)
);

CREATE INDEX IF NOT EXISTS idx_eia_prices_date
    ON silver.eia_spot_prices_daily(price_date DESC);

COMMENT ON TABLE silver.eia_spot_prices_daily IS
'Daily EIA spot prices for crude oil, refined products, and natural gas.';

-- Monthly biofuel data (biodiesel is monthly only)
CREATE TABLE IF NOT EXISTS silver.eia_biofuels_monthly (
    id SERIAL PRIMARY KEY,
    bronze_id INTEGER REFERENCES bronze.eia_raw_ingestion(id),

    -- Temporal
    period_month DATE NOT NULL,                -- First day of month

    -- Biodiesel
    biodiesel_production_mgal NUMERIC(10,1),   -- Production (million gallons)
    biodiesel_stocks_mgal NUMERIC(10,1),       -- Stocks
    biodiesel_consumption_mgal NUMERIC(10,1),  -- Consumption

    -- D4 RIN Prices (if available from other source)
    d4_rin_price NUMERIC(6,4),                 -- D4 biomass-based diesel RIN

    -- Audit
    validation_status VARCHAR(20) DEFAULT 'pending',
    created_ts TIMESTAMP DEFAULT NOW(),

    UNIQUE(period_month)
);

COMMENT ON TABLE silver.eia_biofuels_monthly IS
'Monthly EIA biofuel production and stocks data.';

-- =============================================================================
-- GOLD LAYER - Analytics Ready
-- =============================================================================

-- Unified energy prices view for analysis
CREATE OR REPLACE VIEW gold.v_energy_prices_daily AS
SELECT
    price_date,
    wti_spot_bbl,
    brent_spot_bbl,
    wti_brent_spread,
    rbob_gasoline_gal,
    rbob_gasoline_gal * 42 AS rbob_per_barrel,
    ulsd_diesel_gal,
    ulsd_diesel_gal * 42 AS ulsd_per_barrel,
    henry_hub_mmbtu,
    gasoline_crack_bbl,
    diesel_crack_bbl,
    -- Rolling averages
    AVG(wti_spot_bbl) OVER (ORDER BY price_date ROWS 4 PRECEDING) AS wti_5d_ma,
    AVG(wti_spot_bbl) OVER (ORDER BY price_date ROWS 19 PRECEDING) AS wti_20d_ma,
    -- Price changes
    wti_spot_bbl - LAG(wti_spot_bbl, 1) OVER (ORDER BY price_date) AS wti_1d_change,
    wti_spot_bbl - LAG(wti_spot_bbl, 5) OVER (ORDER BY price_date) AS wti_1w_change
FROM silver.eia_spot_prices_daily
WHERE validation_status = 'validated'
ORDER BY price_date DESC;

COMMENT ON VIEW gold.v_energy_prices_daily IS
'Analytics-ready daily energy prices with rolling averages and changes.';

-- Weekly inventory analysis view
CREATE OR REPLACE VIEW gold.v_petroleum_inventory_analysis AS
SELECT
    period_date,
    crude_stocks_total_kb,
    crude_stocks_cushing_kb,
    -- Week-over-week changes
    crude_stocks_total_kb - LAG(crude_stocks_total_kb, 1) OVER (ORDER BY period_date) AS crude_wow_change,
    crude_stocks_cushing_kb - LAG(crude_stocks_cushing_kb, 1) OVER (ORDER BY period_date) AS cushing_wow_change,
    -- Year-over-year comparison (52 weeks ago)
    crude_stocks_total_kb - LAG(crude_stocks_total_kb, 52) OVER (ORDER BY period_date) AS crude_yoy_change,
    -- 5-year average would require historical data
    gasoline_stocks_kb,
    gasoline_demand_kbd,
    distillate_stocks_kb,
    ethanol_production_kbd,
    ethanol_stocks_kb,
    refinery_utilization_pct
FROM silver.eia_petroleum_weekly
WHERE validation_status = 'validated'
ORDER BY period_date DESC;

COMMENT ON VIEW gold.v_petroleum_inventory_analysis IS
'Weekly petroleum inventory with WoW and YoY comparisons.';

-- =============================================================================
-- META LAYER - Ingestion Tracking
-- =============================================================================

CREATE TABLE IF NOT EXISTS meta.eia_ingestion_log (
    id SERIAL PRIMARY KEY,
    run_id UUID DEFAULT gen_random_uuid(),
    series_id VARCHAR(100),
    category VARCHAR(50),
    started_at TIMESTAMP DEFAULT NOW(),
    completed_at TIMESTAMP,
    status VARCHAR(20),                        -- 'success', 'partial', 'failed'
    records_fetched INTEGER,
    records_inserted INTEGER,
    records_updated INTEGER,
    records_skipped INTEGER,
    error_message TEXT,
    api_calls_made INTEGER,
    api_response_time_ms INTEGER
);

CREATE INDEX IF NOT EXISTS idx_eia_log_status
    ON meta.eia_ingestion_log(status, started_at DESC);

COMMENT ON TABLE meta.eia_ingestion_log IS
'Tracks all EIA data ingestion runs for monitoring and debugging.';

-- =============================================================================
-- HELPER FUNCTIONS
-- =============================================================================

-- Function to get latest petroleum data
CREATE OR REPLACE FUNCTION silver.get_latest_petroleum()
RETURNS TABLE (
    period_date DATE,
    crude_stocks_kb NUMERIC,
    crude_wow_change NUMERIC,
    gasoline_stocks_kb NUMERIC,
    distillate_stocks_kb NUMERIC,
    refinery_util_pct NUMERIC
) AS $$
BEGIN
    RETURN QUERY
    SELECT
        p.period_date,
        p.crude_stocks_total_kb,
        p.crude_stocks_total_kb - LAG(p.crude_stocks_total_kb) OVER (ORDER BY p.period_date),
        p.gasoline_stocks_kb,
        p.distillate_stocks_kb,
        p.refinery_utilization_pct
    FROM silver.eia_petroleum_weekly p
    WHERE p.validation_status = 'validated'
    ORDER BY p.period_date DESC
    LIMIT 1;
END;
$$ LANGUAGE plpgsql;
