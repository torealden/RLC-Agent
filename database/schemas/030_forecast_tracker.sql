-- =============================================================================
-- 030: Forecast Tracker Schema
-- =============================================================================
-- Stores forecasts, actuals, accuracy metrics, and feedback for continuous
-- improvement. All tables in the 'core' schema alongside event_log and
-- collection_status.
--
-- Usage:
--   psql -d rlc_commodities -f database/schemas/030_forecast_tracker.sql
-- =============================================================================

-- Forecasts: stores all predictions with vintage tracking
CREATE TABLE IF NOT EXISTS core.forecasts (
    id              SERIAL PRIMARY KEY,
    forecast_id     TEXT UNIQUE NOT NULL,
    forecast_date   DATE NOT NULL,              -- When forecast was made (vintage)
    target_date     DATE NOT NULL,              -- What date/period is being forecasted
    commodity       TEXT NOT NULL,
    country         TEXT NOT NULL DEFAULT 'US',
    forecast_type   TEXT NOT NULL,              -- yield, production, ending_stocks, price, exports, crush, area, crop_condition
    value           DOUBLE PRECISION NOT NULL,
    unit            TEXT,
    confidence_low  DOUBLE PRECISION,
    confidence_high DOUBLE PRECISION,
    marketing_year  TEXT,
    notes           TEXT,
    source          TEXT DEFAULT 'RLC',         -- 'user', 'model_yield_ensemble', 'usda_wasde', 'usda_nass'
    analyst         TEXT,
    created_at      TIMESTAMPTZ DEFAULT NOW(),

    -- Prevent duplicate forecasts for same vintage + target + commodity
    UNIQUE(forecast_date, target_date, commodity, country, forecast_type, source)
);

CREATE INDEX IF NOT EXISTS idx_forecasts_commodity   ON core.forecasts(commodity);
CREATE INDEX IF NOT EXISTS idx_forecasts_target      ON core.forecasts(target_date);
CREATE INDEX IF NOT EXISTS idx_forecasts_vintage     ON core.forecasts(forecast_date);
CREATE INDEX IF NOT EXISTS idx_forecasts_type        ON core.forecasts(forecast_type);
CREATE INDEX IF NOT EXISTS idx_forecasts_source      ON core.forecasts(source);


-- Actuals: stores reported values with revision tracking
CREATE TABLE IF NOT EXISTS core.actuals (
    id              SERIAL PRIMARY KEY,
    actual_id       TEXT UNIQUE NOT NULL,
    report_date     DATE NOT NULL,              -- When the actual was reported
    target_date     DATE NOT NULL,              -- What period this represents
    commodity       TEXT NOT NULL,
    country         TEXT NOT NULL DEFAULT 'US',
    value_type      TEXT NOT NULL,              -- Same categories as forecast_type
    value           DOUBLE PRECISION NOT NULL,
    unit            TEXT,
    marketing_year  TEXT,
    source          TEXT,                        -- USDA, CONAB, NASS, etc.
    revision_number INTEGER DEFAULT 0,
    notes           TEXT,
    created_at      TIMESTAMPTZ DEFAULT NOW(),

    -- Allow multiple revisions per target+commodity+source
    UNIQUE(target_date, commodity, country, value_type, source, revision_number)
);

CREATE INDEX IF NOT EXISTS idx_actuals_commodity ON core.actuals(commodity);
CREATE INDEX IF NOT EXISTS idx_actuals_target    ON core.actuals(target_date);
CREATE INDEX IF NOT EXISTS idx_actuals_type      ON core.actuals(value_type);


-- Forecast-Actual pairs: links forecasts to their corresponding actuals with error metrics
CREATE TABLE IF NOT EXISTS core.forecast_actual_pairs (
    id                          SERIAL PRIMARY KEY,
    forecast_id                 TEXT NOT NULL REFERENCES core.forecasts(forecast_id),
    actual_id                   TEXT NOT NULL REFERENCES core.actuals(actual_id),
    error                       DOUBLE PRECISION,           -- actual - forecast
    percentage_error            DOUBLE PRECISION,           -- (actual - forecast) / actual * 100
    absolute_error              DOUBLE PRECISION,
    absolute_percentage_error   DOUBLE PRECISION,
    direction_correct           SMALLINT,                   -- 1 = correct, 0 = wrong
    days_ahead                  INTEGER,                    -- How far ahead the forecast was
    created_at                  TIMESTAMPTZ DEFAULT NOW(),

    UNIQUE(forecast_id, actual_id)
);

CREATE INDEX IF NOT EXISTS idx_pairs_forecast ON core.forecast_actual_pairs(forecast_id);
CREATE INDEX IF NOT EXISTS idx_pairs_actual   ON core.forecast_actual_pairs(actual_id);


-- Accuracy metrics: aggregated scores by commodity/type/horizon
CREATE TABLE IF NOT EXISTS core.accuracy_metrics (
    id                      SERIAL PRIMARY KEY,
    metric_id               TEXT UNIQUE NOT NULL,
    computed_date           DATE NOT NULL,
    commodity               TEXT NOT NULL,
    country                 TEXT,
    forecast_type           TEXT NOT NULL,
    horizon                 TEXT,                           -- Forecast horizon evaluated
    n_forecasts             INTEGER NOT NULL,

    -- Core metrics
    mae                     DOUBLE PRECISION,
    mape                    DOUBLE PRECISION,
    rmse                    DOUBLE PRECISION,
    mpe                     DOUBLE PRECISION,               -- Bias measure

    -- Directional
    directional_accuracy    DOUBLE PRECISION,

    -- Bias
    mean_error              DOUBLE PRECISION,
    median_error            DOUBLE PRECISION,

    -- Range
    min_error               DOUBLE PRECISION,
    max_error               DOUBLE PRECISION,
    std_error               DOUBLE PRECISION,

    -- Advanced
    theil_u                 DOUBLE PRECISION,

    -- Period
    period_start            DATE,
    period_end              DATE,

    created_at              TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_metrics_commodity ON core.accuracy_metrics(commodity);
CREATE INDEX IF NOT EXISTS idx_metrics_type      ON core.accuracy_metrics(forecast_type);


-- Feedback log: tracks methodology adjustments
CREATE TABLE IF NOT EXISTS core.forecast_feedback (
    id                      SERIAL PRIMARY KEY,
    feedback_date           DATE NOT NULL,
    commodity               TEXT,
    country                 TEXT,
    forecast_type           TEXT,
    issue_identified        TEXT,
    root_cause              TEXT,
    adjustment_made         TEXT,
    expected_improvement    TEXT,
    analyst                 TEXT,
    created_at              TIMESTAMPTZ DEFAULT NOW()
);


-- =============================================================================
-- Convenience views
-- =============================================================================

-- Latest accuracy by commodity+type
CREATE OR REPLACE VIEW gold.forecast_accuracy_latest AS
SELECT DISTINCT ON (commodity, forecast_type)
    commodity, forecast_type, n_forecasts,
    mape, directional_accuracy, mpe as bias_pct,
    theil_u, computed_date
FROM core.accuracy_metrics
ORDER BY commodity, forecast_type, computed_date DESC;


-- Model vs USDA comparison view
CREATE OR REPLACE VIEW gold.model_vs_usda_comparison AS
SELECT
    m.commodity,
    m.target_date,
    m.marketing_year,
    m.value AS model_forecast,
    m.forecast_date AS model_date,
    u.value AS usda_estimate,
    u.forecast_date AS usda_date,
    ROUND((m.value - u.value)::numeric, 2) AS model_minus_usda,
    ROUND(((m.value - u.value) / NULLIF(u.value, 0) * 100)::numeric, 2) AS pct_diff
FROM core.forecasts m
JOIN core.forecasts u
    ON m.commodity = u.commodity
    AND m.target_date = u.target_date
    AND m.country = u.country
    AND m.forecast_type = u.forecast_type
WHERE m.source = 'model_yield_ensemble'
  AND u.source = 'usda_wasde'
ORDER BY m.commodity, m.target_date DESC;


-- Forecast vs actual detail view for dashboards
CREATE OR REPLACE VIEW gold.forecast_vs_actual AS
SELECT
    f.forecast_id,
    f.forecast_date,
    f.target_date,
    f.commodity,
    f.country,
    f.forecast_type,
    f.value AS forecast_value,
    f.unit,
    f.marketing_year,
    f.source AS forecast_source,
    a.value AS actual_value,
    a.source AS actual_source,
    p.error,
    p.percentage_error,
    p.absolute_percentage_error,
    p.direction_correct,
    p.days_ahead
FROM core.forecast_actual_pairs p
JOIN core.forecasts f ON p.forecast_id = f.forecast_id
JOIN core.actuals a ON p.actual_id = a.actual_id;
