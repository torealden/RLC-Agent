-- ============================================================================
-- OILSEED CRUSH MARGIN ENGINE — Schema
-- ============================================================================
-- Supports margin calculation, volume estimation, and validation for all
-- US oilseed crushing operations.
--
-- Phase 1: Margin calculation (soybeans → minor oilseeds)
-- Phase 2: Volume estimation with NASS calibration
-- Phase 3: Plant-level agent model (per-facility scheduling)
-- ============================================================================

-- ============================================================================
-- 1. REFERENCE: Oilseed crush parameters
-- ============================================================================
-- Semi-static parameters per oilseed. Seeded from YAML config,
-- updated as better data becomes available.

CREATE TABLE IF NOT EXISTS reference.oilseed_crush_params (
    oilseed_code        VARCHAR(20)  PRIMARY KEY,
    oilseed_name        VARCHAR(100) NOT NULL,

    -- Extraction rates (5-year NASS seasonal averages)
    oil_yield_pct       NUMERIC(6,3) NOT NULL,   -- e.g., 18.5 for soybeans
    meal_yield_pct      NUMERIC(6,3) NOT NULL,   -- e.g., 79.5 for soybeans
    hull_yield_pct      NUMERIC(6,3) DEFAULT 0,  -- tracked for validation

    -- Cost parameters
    processing_cost_per_unit  NUMERIC(10,4) NOT NULL,
    seed_unit           VARCHAR(20)  NOT NULL,    -- 'bushel', 'short_ton', 'cwt'
    seed_lbs_per_unit   NUMERIC(8,2) NOT NULL,    -- 60 for soy, 50 for canola

    -- Price source specifications (parsed by PriceResolver)
    oil_price_source    VARCHAR(100),  -- 'futures:ZL', 'ams:canola_oil', 'differential:ZL:0.15'
    meal_price_source   VARCHAR(100),  -- 'futures:ZM', 'ratio:ZM:0.82'
    seed_price_source   VARCHAR(100),  -- 'futures:ZS', 'ams:cottonseed'

    -- Marketing year
    my_start_month      INTEGER NOT NULL DEFAULT 9,

    -- Data availability
    has_nass_monthly    BOOLEAN NOT NULL DEFAULT FALSE,
    nass_source         VARCHAR(50),   -- 'NASS_FATS_OILS', 'NASS_PEANUT'
    nass_attribute      VARCHAR(50),   -- 'oil_production_crude', 'peanuts_crushed'

    -- Annual estimates (for oilseeds without monthly data)
    usda_annual_crush   NUMERIC(18,2),
    usda_annual_unit    VARCHAR(20),

    -- Capacity
    capacity_annual_thou_tons  NUMERIC(10,1),

    -- Seasonal pattern (12-element array: Jan through Dec share of annual)
    -- NULL = use soybean pattern as default
    seasonal_pattern    NUMERIC(6,4)[],

    -- Regression coefficients (populated by calibration)
    reg_intercept       NUMERIC(12,6),
    reg_margin_coeff    NUMERIC(12,6),
    reg_margin_lag_coeff NUMERIC(12,6),
    reg_r_squared       NUMERIC(6,4),
    reg_calibrated_at   TIMESTAMPTZ,

    created_at          TIMESTAMPTZ DEFAULT NOW(),
    updated_at          TIMESTAMPTZ DEFAULT NOW()
);

COMMENT ON TABLE reference.oilseed_crush_params IS
    'Semi-static parameters for each oilseed in the crush margin model. '
    'Extraction rates use 5-year NASS seasonal averages. '
    'Regression coefficients populated by calibration against NASS monthly actuals.';

-- ============================================================================
-- 2. SILVER: Monthly crush margins
-- ============================================================================

CREATE TABLE IF NOT EXISTS silver.oilseed_crush_margin (
    id                  SERIAL PRIMARY KEY,
    period              DATE NOT NULL,           -- first of month
    oilseed_code        VARCHAR(20) NOT NULL REFERENCES reference.oilseed_crush_params(oilseed_code),

    -- Input prices (standardized)
    oil_price_cents_lb      NUMERIC(10,4),   -- cents per pound
    meal_price_per_ton      NUMERIC(10,4),   -- $/short ton
    seed_price_per_unit     NUMERIC(10,4),   -- in seed_unit ($/bu or $/ton)

    -- Revenue components per unit of seed
    oil_revenue_per_unit    NUMERIC(10,4),
    meal_revenue_per_unit   NUMERIC(10,4),
    gross_processing_value  NUMERIC(10,4),   -- oil + meal revenue

    -- Costs per unit of seed
    seed_cost_per_unit      NUMERIC(10,4),
    processing_cost_per_unit NUMERIC(10,4),

    -- Margin
    crush_margin            NUMERIC(10,4),   -- GPV - seed - processing
    margin_pct              NUMERIC(8,4),    -- margin / seed_cost * 100

    -- Metadata
    price_sources           JSONB,           -- {"oil": "ZL avg 52.3", "meal": "ZM avg 312.5", ...}
    run_date                TIMESTAMPTZ DEFAULT NOW(),

    UNIQUE (period, oilseed_code)
);

CREATE INDEX IF NOT EXISTS idx_crush_margin_period
    ON silver.oilseed_crush_margin(period);
CREATE INDEX IF NOT EXISTS idx_crush_margin_oilseed
    ON silver.oilseed_crush_margin(oilseed_code);

COMMENT ON TABLE silver.oilseed_crush_margin IS
    'Monthly crush margins for all oilseeds. GPV = oil revenue + meal revenue per unit of seed. '
    'Margin = GPV - seed cost - processing cost.';

-- ============================================================================
-- 3. SILVER: Monthly crush volume estimates
-- ============================================================================

CREATE TABLE IF NOT EXISTS silver.oilseed_crush_estimate (
    id                  SERIAL PRIMARY KEY,
    period              DATE NOT NULL,
    oilseed_code        VARCHAR(20) NOT NULL REFERENCES reference.oilseed_crush_params(oilseed_code),
    run_id              UUID NOT NULL,
    scenario            VARCHAR(30) DEFAULT 'base',

    -- Estimated volumes
    estimated_crush_thou_tons   NUMERIC(12,2),
    estimated_crush_mil_bu      NUMERIC(12,4),
    estimated_oil_prod_mil_lbs  NUMERIC(12,2),
    estimated_meal_prod_thou_tons NUMERIC(12,2),

    -- Model components (what drove the estimate)
    margin_signal           NUMERIC(8,4),    -- normalized margin z-score
    capacity_util_pct       NUMERIC(6,2),    -- estimated capacity utilization
    seasonal_factor         NUMERIC(6,4),    -- month's share of annual

    -- Validation (populated for oilseeds with NASS monthly data)
    actual_crush_value      NUMERIC(12,2),   -- from silver.monthly_realized
    actual_unit             VARCHAR(30),
    error_value             NUMERIC(12,2),
    error_pct               NUMERIC(8,4),

    created_at              TIMESTAMPTZ DEFAULT NOW(),

    UNIQUE (period, oilseed_code, run_id)
);

CREATE INDEX IF NOT EXISTS idx_crush_estimate_period
    ON silver.oilseed_crush_estimate(period);
CREATE INDEX IF NOT EXISTS idx_crush_estimate_run
    ON silver.oilseed_crush_estimate(run_id);

COMMENT ON TABLE silver.oilseed_crush_estimate IS
    'Model-estimated monthly crush volumes. For calibrated oilseeds (has_nass_monthly=TRUE), '
    'includes actual vs. estimated comparison for validation.';

-- ============================================================================
-- 4. GOLD VIEWS
-- ============================================================================

-- Current margins across all oilseeds
CREATE OR REPLACE VIEW gold.oilseed_crush_margin_latest AS
SELECT
    m.oilseed_code,
    p.oilseed_name,
    m.period,
    m.oil_price_cents_lb,
    m.meal_price_per_ton,
    m.seed_price_per_unit,
    p.seed_unit,
    m.gross_processing_value,
    m.crush_margin,
    m.margin_pct,
    -- YoY comparison
    LAG(m.crush_margin, 12) OVER (
        PARTITION BY m.oilseed_code ORDER BY m.period
    ) AS margin_yoy,
    m.crush_margin - LAG(m.crush_margin, 12) OVER (
        PARTITION BY m.oilseed_code ORDER BY m.period
    ) AS margin_yoy_change
FROM silver.oilseed_crush_margin m
JOIN reference.oilseed_crush_params p ON m.oilseed_code = p.oilseed_code
ORDER BY m.oilseed_code, m.period DESC;

COMMENT ON VIEW gold.oilseed_crush_margin_latest IS
    'Crush margins for all oilseeds with year-over-year comparison.';

-- Model validation: estimated vs actual
CREATE OR REPLACE VIEW gold.oilseed_crush_validation AS
SELECT
    e.oilseed_code,
    p.oilseed_name,
    e.period,
    e.estimated_crush_thou_tons,
    e.actual_crush_value,
    e.actual_unit,
    e.error_value,
    e.error_pct,
    e.margin_signal,
    e.capacity_util_pct,
    e.seasonal_factor,
    m.crush_margin,
    m.margin_pct
FROM silver.oilseed_crush_estimate e
JOIN reference.oilseed_crush_params p ON e.oilseed_code = p.oilseed_code
LEFT JOIN silver.oilseed_crush_margin m ON e.period = m.period AND e.oilseed_code = m.oilseed_code
WHERE e.actual_crush_value IS NOT NULL
  AND e.run_id = (
      SELECT run_id FROM silver.oilseed_crush_estimate
      ORDER BY created_at DESC LIMIT 1
  )
ORDER BY e.oilseed_code, e.period DESC;

COMMENT ON VIEW gold.oilseed_crush_validation IS
    'Model validation: estimated crush vs NASS actuals for calibrated oilseeds. '
    'Shows latest model run only.';

-- Model accuracy summary
CREATE OR REPLACE VIEW gold.oilseed_crush_model_accuracy AS
SELECT
    oilseed_code,
    COUNT(*) AS n_months,
    ROUND(AVG(ABS(error_pct))::numeric, 2) AS mape,
    ROUND(AVG(error_pct)::numeric, 2) AS mean_bias_pct,
    ROUND(STDDEV(error_pct)::numeric, 2) AS error_std_pct,
    MIN(period) AS first_period,
    MAX(period) AS last_period
FROM silver.oilseed_crush_estimate
WHERE actual_crush_value IS NOT NULL
  AND run_id = (
      SELECT run_id FROM silver.oilseed_crush_estimate
      ORDER BY created_at DESC LIMIT 1
  )
GROUP BY oilseed_code
ORDER BY mape;

COMMENT ON VIEW gold.oilseed_crush_model_accuracy IS
    'Model accuracy summary by oilseed: MAPE, bias, and standard deviation of errors.';
