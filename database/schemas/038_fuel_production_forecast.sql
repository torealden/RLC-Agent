-- ============================================================================
-- 038: Fuel Production Forecast
-- ============================================================================
-- Monthly fuel production by type — canonical input to feedstock allocator.
-- Analyst maintains balance sheet Excel files; ingestion script reads monthly
-- production forecasts into this table. The allocator queries it to determine
-- how much fuel each facility produces (and therefore how much feedstock it needs).
--
-- Depends on: 035_feedstock_allocation_engine.sql (reference.biofuel_facilities)
-- ============================================================================

-- Monthly production forecasts / actuals by fuel type
CREATE TABLE IF NOT EXISTS silver.fuel_production_forecast (
    id                SERIAL PRIMARY KEY,
    period            DATE NOT NULL,              -- First of month
    fuel_type         VARCHAR(30) NOT NULL,       -- biodiesel, renewable_diesel, saf, ethanol
    production_mmgal  NUMERIC(12,4),              -- Million gallons produced
    capacity_mmgy     NUMERIC(12,2),              -- Nameplate capacity (MMGY) that month
    utilization_pct   NUMERIC(5,1),               -- Operating rate (%)
    is_forecast       BOOLEAN DEFAULT FALSE,      -- TRUE = analyst projection, FALSE = actual
    source            VARCHAR(50) DEFAULT 'balance_sheet',  -- balance_sheet, eia_actual, capacity_model
    source_file       VARCHAR(200),               -- Which Excel file this came from
    updated_at        TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (period, fuel_type)
);

COMMENT ON TABLE silver.fuel_production_forecast IS
    'Monthly fuel production by type. Ingested from analyst balance sheet Excel files. '
    'Read by the feedstock allocation engine to determine facility-level production.';

COMMENT ON COLUMN silver.fuel_production_forecast.production_mmgal IS
    'Total US production in million gallons for this fuel type and month.';

COMMENT ON COLUMN silver.fuel_production_forecast.is_forecast IS
    'TRUE for analyst projections (future months), FALSE for EIA-reported actuals.';

-- Index for allocator lookups (by period)
CREATE INDEX IF NOT EXISTS idx_fuel_prod_forecast_period
    ON silver.fuel_production_forecast (period);

-- Gold view: latest fuel production with YoY comparison
CREATE OR REPLACE VIEW gold.fuel_production_summary AS
SELECT
    f.period,
    f.fuel_type,
    f.production_mmgal,
    f.capacity_mmgy,
    f.utilization_pct,
    f.is_forecast,
    -- Year-over-year
    LAG(f.production_mmgal, 12) OVER (
        PARTITION BY f.fuel_type ORDER BY f.period
    ) AS production_mmgal_yoy,
    ROUND(
        (f.production_mmgal - LAG(f.production_mmgal, 12) OVER (
            PARTITION BY f.fuel_type ORDER BY f.period
        )) / NULLIF(LAG(f.production_mmgal, 12) OVER (
            PARTITION BY f.fuel_type ORDER BY f.period
        ), 0) * 100, 1
    ) AS yoy_change_pct,
    -- Running 12-month total
    SUM(f.production_mmgal) OVER (
        PARTITION BY f.fuel_type ORDER BY f.period
        ROWS BETWEEN 11 PRECEDING AND CURRENT ROW
    ) AS trailing_12m_mmgal,
    f.source,
    f.updated_at
FROM silver.fuel_production_forecast f
ORDER BY f.fuel_type, f.period;

COMMENT ON VIEW gold.fuel_production_summary IS
    'Fuel production with YoY comparison and trailing 12-month totals. '
    'Used for dashboards and allocator validation.';
