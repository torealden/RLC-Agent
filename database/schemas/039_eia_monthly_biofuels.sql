-- 039_eia_monthly_biofuels.sql
-- Bronze table for EIA monthly biofuel + monthly fuel ethanol data pulled
-- via EIA v2 API. One row per (period, series_id, region) tuple.
--
-- Companion to bronze.eia_capacity_monthly + bronze.eia_feedstock_monthly
-- (which cover capacity and feedstock inputs from xlsx downloads).
--
-- This table covers the FINISHED-FUEL side: production, imports, exports,
-- stocks, consumption, blender input for biodiesel, renewable diesel,
-- "other biofuels" (incl SAF), and fuel ethanol.

CREATE SCHEMA IF NOT EXISTS bronze;

CREATE TABLE IF NOT EXISTS bronze.eia_monthly_biofuels (
    id            BIGSERIAL PRIMARY KEY,
    period_month  DATE        NOT NULL,
    series_id     VARCHAR(64) NOT NULL,
    fuel_type     VARCHAR(32) NOT NULL,   -- biodiesel|renewable_diesel|other_biofuels|fuel_ethanol|combined_bd_rd
    attribute     VARCHAR(32) NOT NULL,   -- production|imports|exports|stocks|consumption|blender_input|net_input
    region        VARCHAR(16) NOT NULL DEFAULT 'NUS',  -- NUS|R10|R20|R30|R40|R50
    value         NUMERIC(18,6),
    units         VARCHAR(16),            -- MBBL|MBBLD
    description   TEXT,
    raw_payload   JSONB,
    collected_at  TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE (period_month, series_id, region)
);

CREATE INDEX IF NOT EXISTS idx_eia_mb_period
    ON bronze.eia_monthly_biofuels(period_month);
CREATE INDEX IF NOT EXISTS idx_eia_mb_fuel_attr
    ON bronze.eia_monthly_biofuels(fuel_type, attribute);
CREATE INDEX IF NOT EXISTS idx_eia_mb_series
    ON bronze.eia_monthly_biofuels(series_id);

COMMENT ON TABLE bronze.eia_monthly_biofuels IS
    'Monthly biofuel finished-product data from EIA v2 API. '
    'Covers BD, RD, Other Biofuels (incl SAF), and fuel ethanol. '
    'PADD-level data preserved via region column. '
    'Loaded by EIAMonthlyBiofuelsCollector (registered as eia_biofuels_monthly in dispatcher).';
