-- ============================================================================
-- Migration 042: core.forecasts_historical
-- ============================================================================
-- Bootstraps the symbiotic forecasting endpoint (project_forecast_comparison.md +
-- project_symbiotic_forecasting.md in memory) with multi-year baseline of
-- weekly biofuel feedstock price forecasts captured 2018-12 through 2020-04.
--
-- Source: D:\Forecast Measurement\Biofuels Forecasts - Copy\ (194 CSVs)
--         + D:\Forecast Measurement\Archive\Forecasts - 2\ (2 CSVs)
--
-- File-naming pattern: {YYYYMMDD}_{type}_{commodity}_{location}.csv
--   type:     'LowCI' (low-CI feedstocks) or 'VegOils' (vegetable oils)
--   commodity: BFT (beef tallow) / CWG (choice white grease) / DCO (distillers
--              corn oil) / PF (poultry fat) / UCO (used cooking oil) /
--              PO (palm oil) / SBO (soybean oil)
--   location:  PackerCHI / MoRiver / Gulf / ILWI / MidSouth / None / etc.
--
-- Each CSV has columns:
--   Measure Names  ('Low' / 'High' / 'Current'),
--   Month of Date  ('20-May' = 2020-05-01),
--   Measure Values (price)
-- ============================================================================

CREATE SCHEMA IF NOT EXISTS core;

CREATE TABLE IF NOT EXISTS core.forecasts_historical (
    id                BIGSERIAL PRIMARY KEY,
    forecast_date     DATE NOT NULL,           -- the date the forecast was MADE
    forecast_type     TEXT NOT NULL,           -- 'LowCI' | 'VegOils' | future
    commodity         TEXT NOT NULL,           -- BFT/CWG/DCO/PF/UCO/PO/SBO/etc.
    location          TEXT,                    -- PackerCHI/MoRiver/Gulf/ILWI/MidSouth/None
    target_month      DATE NOT NULL,           -- the month the forecast is FOR (1st of month)
    horizon_months    INTEGER GENERATED ALWAYS AS (
                          CAST(
                              (EXTRACT(YEAR FROM target_month) * 12 + EXTRACT(MONTH FROM target_month))
                            - (EXTRACT(YEAR FROM forecast_date) * 12 + EXTRACT(MONTH FROM forecast_date))
                            AS INTEGER)
                      ) STORED,
    low_price         NUMERIC(10,4),
    high_price        NUMERIC(10,4),
    current_price     NUMERIC(10,4),           -- spot price at forecast date
    forecast_unit     TEXT DEFAULT 'cents_per_lb',  -- inferred from data
    source_file       TEXT,
    source_label      TEXT NOT NULL DEFAULT 'tore_legacy_2018_2020',
    forecaster        TEXT NOT NULL DEFAULT 'tore_alden',  -- can extend when other forecasters added
    -- Validation hooks (populated when actual realized prices become known)
    realized_price    NUMERIC(10,4),
    realized_at       TIMESTAMPTZ,
    error_pct         NUMERIC(8,3) GENERATED ALWAYS AS (
                          CASE WHEN current_price > 0 AND realized_price IS NOT NULL
                               THEN 100.0 * (realized_price - current_price) / current_price
                               ELSE NULL END
                      ) STORED,
    ingested_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (forecast_date, forecast_type, commodity, location, target_month)
);

CREATE INDEX IF NOT EXISTS idx_forecasts_hist_date      ON core.forecasts_historical (forecast_date);
CREATE INDEX IF NOT EXISTS idx_forecasts_hist_target    ON core.forecasts_historical (target_month);
CREATE INDEX IF NOT EXISTS idx_forecasts_hist_commodity ON core.forecasts_historical (commodity);
CREATE INDEX IF NOT EXISTS idx_forecasts_hist_horizon   ON core.forecasts_historical (horizon_months);

COMMENT ON TABLE core.forecasts_historical IS
'Historical price forecasts from Tore''s 2018-2020 weekly forecast cadence. Each row = one (forecast_date, commodity, location, target_month) observation with Low/High/Current prices. realized_price + error_pct populated post-hoc when actuals become known. Source for symbiotic forecasting endpoint backtest.';


-- ----------------------------------------------------------------------------
-- Convenience view: aggregate forecast accuracy by (commodity, horizon)
-- ----------------------------------------------------------------------------
CREATE OR REPLACE VIEW core.v_forecast_accuracy_by_commodity AS
SELECT
    commodity,
    horizon_months,
    COUNT(*)                                AS n_forecasts,
    COUNT(realized_price)                   AS n_evaluated,
    AVG(ABS(error_pct))                     AS mae_pct,
    AVG(error_pct)                          AS bias_pct,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY ABS(error_pct)) AS median_abs_error_pct
FROM core.forecasts_historical
WHERE realized_price IS NOT NULL
GROUP BY commodity, horizon_months;

COMMENT ON VIEW core.v_forecast_accuracy_by_commodity IS
'Per-commodity, per-horizon accuracy summary. Drives the Forecast Accuracy section of the BBD weekly report and the Calls Register validation in the Iowa Crush Agent system.';


-- ----------------------------------------------------------------------------
-- Convenience view: most-recent forecast per (commodity, location, target_month)
-- ----------------------------------------------------------------------------
CREATE OR REPLACE VIEW core.v_forecasts_latest AS
SELECT DISTINCT ON (commodity, location, target_month)
    commodity, location, target_month,
    forecast_date, low_price, high_price, current_price,
    horizon_months, source_file
FROM core.forecasts_historical
ORDER BY commodity, location, target_month, forecast_date DESC;


GRANT SELECT, INSERT, UPDATE ON core.forecasts_historical            TO PUBLIC;
GRANT SELECT ON core.v_forecast_accuracy_by_commodity                TO PUBLIC;
GRANT SELECT ON core.v_forecasts_latest                               TO PUBLIC;
GRANT USAGE, SELECT ON SEQUENCE core.forecasts_historical_id_seq      TO PUBLIC;
