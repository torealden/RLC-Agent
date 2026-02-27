-- =============================================================================
-- EIA GOLD LAYER - Analytics-Ready Views for Power BI
-- =============================================================================
-- These views flatten the bronze JSONB data into tabular format
-- optimized for visualization and analysis
-- =============================================================================

-- Drop existing views to recreate
DROP VIEW IF EXISTS gold.eia_prices_daily CASCADE;
DROP VIEW IF EXISTS gold.eia_petroleum_weekly CASCADE;
DROP VIEW IF EXISTS gold.eia_natural_gas_weekly CASCADE;
DROP VIEW IF EXISTS gold.eia_ethanol_weekly CASCADE;
DROP VIEW IF EXISTS gold.eia_regional_stocks CASCADE;
DROP VIEW IF EXISTS gold.eia_dashboard_summary CASCADE;
DROP MATERIALIZED VIEW IF EXISTS gold.eia_prices_timeseries CASCADE;
DROP MATERIALIZED VIEW IF EXISTS gold.eia_inventory_timeseries CASCADE;

-- =============================================================================
-- 1. DAILY ENERGY PRICES
-- =============================================================================
CREATE OR REPLACE VIEW gold.eia_prices_daily AS
WITH prices AS (
    SELECT
        report_date,
        series_id,
        (raw_payload->'record'->>'value')::numeric AS price,
        raw_payload->'record'->>'units' AS units,
        raw_payload->'record'->>'product-name' AS product_name
    FROM bronze.eia_raw_ingestion
    WHERE series_id IN ('wti_spot', 'brent_spot', 'rbob_gasoline', 'ulsd_diesel', 'heating_oil', 'henry_hub_spot')
)
SELECT
    report_date,
    MAX(CASE WHEN series_id = 'wti_spot' THEN price END) AS wti_crude_usd_bbl,
    MAX(CASE WHEN series_id = 'brent_spot' THEN price END) AS brent_crude_usd_bbl,
    MAX(CASE WHEN series_id = 'rbob_gasoline' THEN price END) AS rbob_gasoline_usd_gal,
    MAX(CASE WHEN series_id = 'ulsd_diesel' THEN price END) AS ulsd_diesel_usd_gal,
    MAX(CASE WHEN series_id = 'heating_oil' THEN price END) AS heating_oil_usd_gal,
    MAX(CASE WHEN series_id = 'henry_hub_spot' THEN price END) AS henry_hub_usd_mmbtu,
    -- Calculated spreads
    MAX(CASE WHEN series_id = 'brent_spot' THEN price END) -
        MAX(CASE WHEN series_id = 'wti_spot' THEN price END) AS brent_wti_spread,
    -- Crack spreads (gasoline/diesel to crude)
    (MAX(CASE WHEN series_id = 'rbob_gasoline' THEN price END) * 42) -
        MAX(CASE WHEN series_id = 'wti_spot' THEN price END) AS gasoline_crack_spread,
    (MAX(CASE WHEN series_id = 'ulsd_diesel' THEN price END) * 42) -
        MAX(CASE WHEN series_id = 'wti_spot' THEN price END) AS diesel_crack_spread
FROM prices
GROUP BY report_date
ORDER BY report_date DESC;

COMMENT ON VIEW gold.eia_prices_daily IS 'Daily energy spot prices with calculated spreads for Power BI';

-- =============================================================================
-- 2. WEEKLY PETROLEUM SUPPLY/DEMAND
-- =============================================================================
CREATE OR REPLACE VIEW gold.eia_petroleum_weekly AS
WITH petroleum AS (
    SELECT
        report_date,
        series_id,
        (raw_payload->'record'->>'value')::numeric AS value
    FROM bronze.eia_raw_ingestion
    WHERE category = 'petroleum'
      AND series_id NOT LIKE 'padd%'
)
SELECT
    report_date AS week_ending,
    -- Stocks (thousand barrels)
    MAX(CASE WHEN series_id = 'crude_oil_stocks' THEN value END) AS crude_stocks_total_kb,
    MAX(CASE WHEN series_id = 'crude_oil_stocks_ex_spr' THEN value END) AS crude_stocks_ex_spr_kb,
    MAX(CASE WHEN series_id = 'crude_oil_spr' THEN value END) AS spr_stocks_kb,
    MAX(CASE WHEN series_id = 'gasoline_stocks' THEN value END) AS gasoline_stocks_kb,
    MAX(CASE WHEN series_id = 'distillate_stocks' THEN value END) AS distillate_stocks_kb,
    -- Production/Imports (thousand barrels per day)
    MAX(CASE WHEN series_id = 'crude_oil_production' THEN value END) AS crude_production_kbd,
    MAX(CASE WHEN series_id = 'crude_oil_imports' THEN value END) AS crude_imports_kbd,
    MAX(CASE WHEN series_id = 'refinery_inputs' THEN value END) AS refinery_inputs_kbd,
    -- Refinery utilization
    MAX(CASE WHEN series_id = 'refinery_utilization' THEN value END) AS refinery_utilization_pct,
    -- Days of supply
    MAX(CASE WHEN series_id = 'gasoline_days_supply' THEN value END) AS gasoline_days_supply,
    -- Week-over-week changes (calculated in Power BI or here)
    MAX(CASE WHEN series_id = 'crude_oil_stocks' THEN value END) -
        LAG(MAX(CASE WHEN series_id = 'crude_oil_stocks' THEN value END))
        OVER (ORDER BY report_date) AS crude_stocks_wow_change,
    MAX(CASE WHEN series_id = 'gasoline_stocks' THEN value END) -
        LAG(MAX(CASE WHEN series_id = 'gasoline_stocks' THEN value END))
        OVER (ORDER BY report_date) AS gasoline_stocks_wow_change
FROM petroleum
GROUP BY report_date
ORDER BY report_date DESC;

COMMENT ON VIEW gold.eia_petroleum_weekly IS 'Weekly petroleum inventory and production metrics';

-- =============================================================================
-- 3. NATURAL GAS STORAGE BY REGION
-- =============================================================================
CREATE OR REPLACE VIEW gold.eia_natural_gas_weekly AS
WITH ng_data AS (
    SELECT
        report_date,
        series_id,
        (raw_payload->'record'->>'value')::numeric AS value
    FROM bronze.eia_raw_ingestion
    WHERE category = 'natural_gas'
)
SELECT
    report_date AS week_ending,
    MAX(CASE WHEN series_id = 'storage_working_gas' THEN value END) AS total_storage_bcf,
    MAX(CASE WHEN series_id = 'storage_east' THEN value END) AS east_storage_bcf,
    MAX(CASE WHEN series_id = 'storage_midwest' THEN value END) AS midwest_storage_bcf,
    MAX(CASE WHEN series_id = 'storage_south_central' THEN value END) AS south_central_storage_bcf,
    MAX(CASE WHEN series_id = 'henry_hub_spot' THEN value END) AS henry_hub_price,
    -- Week-over-week change
    MAX(CASE WHEN series_id = 'storage_working_gas' THEN value END) -
        LAG(MAX(CASE WHEN series_id = 'storage_working_gas' THEN value END))
        OVER (ORDER BY report_date) AS storage_wow_change_bcf
FROM ng_data
GROUP BY report_date
ORDER BY report_date DESC;

COMMENT ON VIEW gold.eia_natural_gas_weekly IS 'Weekly natural gas storage by region with price';

-- =============================================================================
-- 4. ETHANOL/BIOFUELS WEEKLY
-- =============================================================================
CREATE OR REPLACE VIEW gold.eia_ethanol_weekly AS
WITH ethanol AS (
    SELECT
        report_date,
        series_id,
        (raw_payload->'record'->>'value')::numeric AS value
    FROM bronze.eia_raw_ingestion
    WHERE category = 'biofuels'
)
SELECT
    report_date AS week_ending,
    MAX(CASE WHEN series_id = 'ethanol_production' THEN value END) AS ethanol_production_kbd,
    MAX(CASE WHEN series_id = 'ethanol_stocks' THEN value END) AS ethanol_stocks_kb,
    MAX(CASE WHEN series_id = 'ethanol_blender_input' THEN value END) AS ethanol_blender_input_kbd,
    -- Production vs consumption balance
    MAX(CASE WHEN series_id = 'ethanol_production' THEN value END) -
        MAX(CASE WHEN series_id = 'ethanol_blender_input' THEN value END) AS ethanol_balance_kbd
FROM ethanol
GROUP BY report_date
ORDER BY report_date DESC;

COMMENT ON VIEW gold.eia_ethanol_weekly IS 'Weekly ethanol production, stocks, and consumption';

-- =============================================================================
-- 5. REGIONAL CRUDE STOCKS (PADD + Cushing)
-- =============================================================================
CREATE OR REPLACE VIEW gold.eia_regional_stocks AS
WITH regional AS (
    SELECT
        report_date,
        series_id,
        (raw_payload->'record'->>'value')::numeric AS value
    FROM bronze.eia_raw_ingestion
    WHERE category = 'regional'
)
SELECT
    report_date AS week_ending,
    MAX(CASE WHEN series_id = 'cushing_crude_stocks' THEN value END) AS cushing_stocks_kb,
    MAX(CASE WHEN series_id = 'padd1_crude_stocks' THEN value END) AS padd1_east_coast_kb,
    MAX(CASE WHEN series_id = 'padd2_crude_stocks' THEN value END) AS padd2_midwest_kb,
    MAX(CASE WHEN series_id = 'padd3_crude_stocks' THEN value END) AS padd3_gulf_coast_kb,
    MAX(CASE WHEN series_id = 'padd5_crude_stocks' THEN value END) AS padd5_west_coast_kb,
    -- Cushing as % of PADD2
    ROUND(MAX(CASE WHEN series_id = 'cushing_crude_stocks' THEN value END)::numeric /
        NULLIF(MAX(CASE WHEN series_id = 'padd2_crude_stocks' THEN value END), 0) * 100, 1)
        AS cushing_pct_of_padd2
FROM regional
GROUP BY report_date
ORDER BY report_date DESC;

COMMENT ON VIEW gold.eia_regional_stocks IS 'Regional crude oil stocks by PADD district';

-- =============================================================================
-- 6. UNIFIED PRICES TIMESERIES (for charts)
-- =============================================================================
CREATE MATERIALIZED VIEW gold.eia_prices_timeseries AS
SELECT
    report_date,
    series_id,
    CASE series_id
        WHEN 'wti_spot' THEN 'WTI Crude Oil'
        WHEN 'brent_spot' THEN 'Brent Crude Oil'
        WHEN 'rbob_gasoline' THEN 'RBOB Gasoline'
        WHEN 'ulsd_diesel' THEN 'ULSD Diesel'
        WHEN 'heating_oil' THEN 'Heating Oil'
        WHEN 'henry_hub_spot' THEN 'Henry Hub Natural Gas'
        ELSE series_id
    END AS commodity,
    CASE series_id
        WHEN 'wti_spot' THEN 'Crude Oil'
        WHEN 'brent_spot' THEN 'Crude Oil'
        WHEN 'rbob_gasoline' THEN 'Refined Products'
        WHEN 'ulsd_diesel' THEN 'Refined Products'
        WHEN 'heating_oil' THEN 'Refined Products'
        WHEN 'henry_hub_spot' THEN 'Natural Gas'
        ELSE 'Other'
    END AS category,
    (raw_payload->'record'->>'value')::numeric AS price,
    raw_payload->'record'->>'units' AS units
FROM bronze.eia_raw_ingestion
WHERE series_id IN ('wti_spot', 'brent_spot', 'rbob_gasoline', 'ulsd_diesel', 'heating_oil', 'henry_hub_spot')
ORDER BY report_date, series_id;

CREATE INDEX idx_eia_prices_ts_date ON gold.eia_prices_timeseries(report_date);
CREATE INDEX idx_eia_prices_ts_commodity ON gold.eia_prices_timeseries(commodity);

COMMENT ON MATERIALIZED VIEW gold.eia_prices_timeseries IS 'Long-format price timeseries for Power BI line charts';

-- =============================================================================
-- 7. UNIFIED INVENTORY TIMESERIES (for charts)
-- =============================================================================
CREATE MATERIALIZED VIEW gold.eia_inventory_timeseries AS
SELECT
    report_date,
    series_id,
    CASE series_id
        WHEN 'crude_oil_stocks' THEN 'Crude Oil (Total)'
        WHEN 'crude_oil_stocks_ex_spr' THEN 'Crude Oil (Ex-SPR)'
        WHEN 'crude_oil_spr' THEN 'Strategic Petroleum Reserve'
        WHEN 'gasoline_stocks' THEN 'Motor Gasoline'
        WHEN 'distillate_stocks' THEN 'Distillate Fuel Oil'
        WHEN 'ethanol_stocks' THEN 'Fuel Ethanol'
        WHEN 'storage_working_gas' THEN 'Natural Gas Storage'
        WHEN 'cushing_crude_stocks' THEN 'Cushing OK Crude'
        ELSE series_id
    END AS inventory_type,
    CASE series_id
        WHEN 'storage_working_gas' THEN 'Natural Gas'
        WHEN 'ethanol_stocks' THEN 'Biofuels'
        ELSE 'Petroleum'
    END AS category,
    (raw_payload->'record'->>'value')::numeric AS volume,
    CASE
        WHEN series_id = 'storage_working_gas' THEN 'Bcf'
        ELSE 'Thousand Barrels'
    END AS units
FROM bronze.eia_raw_ingestion
WHERE series_id IN (
    'crude_oil_stocks', 'crude_oil_stocks_ex_spr', 'crude_oil_spr',
    'gasoline_stocks', 'distillate_stocks', 'ethanol_stocks',
    'storage_working_gas', 'cushing_crude_stocks'
)
ORDER BY report_date, series_id;

CREATE INDEX idx_eia_inv_ts_date ON gold.eia_inventory_timeseries(report_date);
CREATE INDEX idx_eia_inv_ts_type ON gold.eia_inventory_timeseries(inventory_type);

COMMENT ON MATERIALIZED VIEW gold.eia_inventory_timeseries IS 'Long-format inventory timeseries for Power BI line charts';

-- =============================================================================
-- 8. DASHBOARD SUMMARY (Current Week vs Prior Week)
-- =============================================================================
CREATE OR REPLACE VIEW gold.eia_dashboard_summary AS
WITH latest AS (
    SELECT MAX(report_date) AS latest_date
    FROM bronze.eia_raw_ingestion
),
current_week AS (
    SELECT
        series_id,
        (raw_payload->'record'->>'value')::numeric AS value
    FROM bronze.eia_raw_ingestion b, latest l
    WHERE b.report_date = l.latest_date
),
prior_week AS (
    SELECT
        series_id,
        (raw_payload->'record'->>'value')::numeric AS value
    FROM bronze.eia_raw_ingestion b, latest l
    WHERE b.report_date = l.latest_date - INTERVAL '7 days'
)
SELECT
    c.series_id,
    CASE c.series_id
        WHEN 'wti_spot' THEN 'WTI Crude'
        WHEN 'brent_spot' THEN 'Brent Crude'
        WHEN 'henry_hub_spot' THEN 'Henry Hub NG'
        WHEN 'crude_oil_stocks' THEN 'Crude Stocks'
        WHEN 'gasoline_stocks' THEN 'Gasoline Stocks'
        WHEN 'distillate_stocks' THEN 'Distillate Stocks'
        WHEN 'ethanol_production' THEN 'Ethanol Production'
        WHEN 'storage_working_gas' THEN 'NG Storage'
        WHEN 'cushing_crude_stocks' THEN 'Cushing Stocks'
        WHEN 'refinery_utilization' THEN 'Refinery Util %'
        ELSE c.series_id
    END AS metric_name,
    c.value AS current_value,
    p.value AS prior_week_value,
    c.value - p.value AS wow_change,
    ROUND((c.value - p.value) / NULLIF(p.value, 0) * 100, 2) AS wow_change_pct
FROM current_week c
LEFT JOIN prior_week p ON c.series_id = p.series_id
WHERE c.series_id IN (
    'wti_spot', 'brent_spot', 'henry_hub_spot',
    'crude_oil_stocks', 'gasoline_stocks', 'distillate_stocks',
    'ethanol_production', 'storage_working_gas', 'cushing_crude_stocks',
    'refinery_utilization'
)
ORDER BY c.series_id;

COMMENT ON VIEW gold.eia_dashboard_summary IS 'Current vs prior week summary for dashboard KPIs';

-- =============================================================================
-- Refresh function for materialized views
-- =============================================================================
CREATE OR REPLACE FUNCTION gold.refresh_eia_views()
RETURNS void AS $$
BEGIN
    REFRESH MATERIALIZED VIEW gold.eia_prices_timeseries;
    REFRESH MATERIALIZED VIEW gold.eia_inventory_timeseries;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION gold.refresh_eia_views IS 'Refresh EIA materialized views after new data ingestion';
