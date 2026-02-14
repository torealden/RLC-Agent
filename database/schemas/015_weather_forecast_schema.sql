-- =============================================================================
-- Weather Forecast Schema for Crop Yield Prediction
-- =============================================================================
-- Sources: NOAA GFS, GEFS (ensembles), CFSv2 (seasonal), CPC outlooks
-- Architecture: GRIB2 → NetCDF → PostgreSQL → JSON for LLMs
-- Key principle: Store derived agronomic metrics, not raw meteorology
-- =============================================================================

-- Bronze: Raw forecast ingestion metadata (not the grids themselves)
CREATE TABLE IF NOT EXISTS bronze.weather_forecast_run (
    id BIGSERIAL PRIMARY KEY,

    -- Model identification
    model VARCHAR(20) NOT NULL,           -- 'GFS', 'GEFS', 'CFSv2', 'CPC'
    model_run_date DATE NOT NULL,         -- Model initialization date
    model_run_hour INT NOT NULL,          -- 00, 06, 12, 18 UTC

    -- Coverage
    forecast_hours INT[],                 -- Array of forecast hours available
    max_horizon_days INT,                 -- Max forecast horizon

    -- Source tracking
    source VARCHAR(50) NOT NULL,          -- 'aws_opendata', 'nomads'
    source_url TEXT,

    -- Processing status
    status VARCHAR(20) DEFAULT 'pending', -- 'pending', 'processing', 'completed', 'failed'
    regions_processed TEXT[],
    variables_processed TEXT[],

    -- Metadata
    file_count INT,
    total_size_mb NUMERIC(10,2),
    processing_time_sec INT,
    collected_at TIMESTAMPTZ DEFAULT NOW(),
    processed_at TIMESTAMPTZ,

    UNIQUE (model, model_run_date, model_run_hour)
);

-- Silver: Daily aggregated forecasts by crop region
-- This is the core table - pre-aggregated for efficiency
CREATE TABLE IF NOT EXISTS silver.weather_forecast_daily (
    id BIGSERIAL PRIMARY KEY,

    -- Temporal
    forecast_date DATE NOT NULL,          -- Model run date
    valid_date DATE NOT NULL,             -- Target forecast day
    lead_days INT GENERATED ALWAYS AS (valid_date - forecast_date) STORED,

    -- Model
    model VARCHAR(20) NOT NULL,           -- 'GFS', 'GEFS', 'CFSv2'
    model_run_hour INT DEFAULT 0,

    -- Region (linked to reference.crop_region)
    region_code VARCHAR(30) NOT NULL,     -- 'US_CORN_BELT', 'BR_MATO_GROSSO', etc.

    -- Core weather variables (area-weighted means)
    precip_mm NUMERIC(8,2),               -- Daily precipitation
    tmin_c NUMERIC(6,2),                  -- Minimum temperature
    tmax_c NUMERIC(6,2),                  -- Maximum temperature
    tavg_c NUMERIC(6,2),                  -- Average temperature

    -- Derived agronomic metrics
    gdd_base10 NUMERIC(8,2),              -- Growing degree days (base 10°C)
    gdd_base8 NUMERIC(8,2),               -- GDD base 8°C (for wheat)
    gdd_corn NUMERIC(8,2),                -- GDD with 30°C cap (corn-specific)

    -- Stress indicators
    heat_stress_hours INT,                -- Hours > 30°C
    extreme_heat_hours INT,               -- Hours > 35°C
    frost_risk BOOLEAN,                   -- Tmin < 0°C

    -- Moisture metrics
    precip_deficit_mm NUMERIC(8,2),       -- vs climatology
    consecutive_dry_days INT,             -- Running count
    excess_moisture_flag BOOLEAN,         -- > 25mm in day

    -- Additional variables (when available)
    solar_radiation_mj NUMERIC(8,2),      -- MJ/m²/day
    relative_humidity_pct NUMERIC(5,2),
    wind_speed_ms NUMERIC(6,2),
    soil_moisture_idx NUMERIC(6,3),       -- 0-1 index

    -- Ensemble statistics (for GEFS)
    ensemble_members INT,
    precip_p10 NUMERIC(8,2),
    precip_p50 NUMERIC(8,2),
    precip_p90 NUMERIC(8,2),
    temp_p10 NUMERIC(6,2),
    temp_p50 NUMERIC(6,2),
    temp_p90 NUMERIC(6,2),

    -- Anomalies (vs 30-year climatology)
    precip_anomaly_pct NUMERIC(8,2),
    temp_anomaly_c NUMERIC(6,2),
    gdd_anomaly_pct NUMERIC(8,2),

    -- Quality/confidence
    data_quality VARCHAR(20),             -- 'good', 'partial', 'interpolated'
    coverage_pct NUMERIC(5,2),            -- % of region with valid data

    -- Metadata
    created_at TIMESTAMPTZ DEFAULT NOW(),

    UNIQUE (model, forecast_date, model_run_hour, valid_date, region_code)
);

-- Indexes for common query patterns
CREATE INDEX IF NOT EXISTS idx_forecast_region_valid
    ON silver.weather_forecast_daily(region_code, valid_date DESC);
CREATE INDEX IF NOT EXISTS idx_forecast_model_date
    ON silver.weather_forecast_daily(model, forecast_date DESC);
CREATE INDEX IF NOT EXISTS idx_forecast_lead_days
    ON silver.weather_forecast_daily(lead_days);

-- Silver: Weekly/period aggregations for seasonal analysis
CREATE TABLE IF NOT EXISTS silver.weather_forecast_period (
    id BIGSERIAL PRIMARY KEY,

    -- Period definition
    forecast_date DATE NOT NULL,
    period_start DATE NOT NULL,
    period_end DATE NOT NULL,
    period_type VARCHAR(20) NOT NULL,     -- '7day', '14day', 'monthly'

    model VARCHAR(20) NOT NULL,
    region_code VARCHAR(30) NOT NULL,

    -- Aggregated metrics
    total_precip_mm NUMERIC(10,2),
    avg_tmin_c NUMERIC(6,2),
    avg_tmax_c NUMERIC(6,2),
    total_gdd NUMERIC(10,2),

    -- Stress counts
    heat_stress_days INT,
    extreme_heat_days INT,
    frost_days INT,
    dry_days INT,
    wet_days INT,

    -- Period anomalies
    precip_anomaly_pct NUMERIC(8,2),
    temp_anomaly_c NUMERIC(6,2),
    gdd_anomaly_pct NUMERIC(8,2),

    -- Risk assessment
    drought_risk_score NUMERIC(5,2),      -- 0-100
    flood_risk_score NUMERIC(5,2),
    heat_risk_score NUMERIC(5,2),

    -- Ensemble spread (uncertainty)
    precip_spread_mm NUMERIC(8,2),        -- P90 - P10
    temp_spread_c NUMERIC(6,2),

    created_at TIMESTAMPTZ DEFAULT NOW(),

    UNIQUE (model, forecast_date, period_type, region_code)
);

-- Gold: LLM-ready forecast summaries (JSON-friendly)
CREATE OR REPLACE VIEW gold.weather_forecast_summary AS
SELECT
    f.region_code,
    r.region_name,
    r.country_code,
    r.primary_commodity,
    f.forecast_date,
    f.valid_date,
    f.lead_days,
    f.model,

    -- Core metrics
    f.precip_mm,
    f.tavg_c,
    f.tmin_c,
    f.tmax_c,
    f.gdd_corn,

    -- Anomalies
    f.precip_anomaly_pct,
    f.temp_anomaly_c,
    f.gdd_anomaly_pct,

    -- Stress flags
    f.heat_stress_hours > 0 AS has_heat_stress,
    f.frost_risk,
    f.excess_moisture_flag,
    f.consecutive_dry_days,

    -- Ensemble uncertainty (GEFS)
    f.precip_p10,
    f.precip_p90,
    f.temp_p10,
    f.temp_p90,

    -- Interpretive signals
    CASE
        WHEN f.precip_anomaly_pct < -30 AND f.consecutive_dry_days > 5 THEN 'Drought stress developing'
        WHEN f.precip_anomaly_pct < -15 THEN 'Below normal precipitation'
        WHEN f.precip_anomaly_pct > 50 THEN 'Excessive moisture risk'
        WHEN f.precip_anomaly_pct > 20 THEN 'Above normal precipitation'
        ELSE 'Near normal moisture'
    END AS moisture_signal,

    CASE
        WHEN f.extreme_heat_hours > 4 THEN 'Severe heat stress'
        WHEN f.heat_stress_hours > 8 THEN 'Moderate heat stress'
        WHEN f.frost_risk THEN 'Frost risk'
        WHEN f.temp_anomaly_c > 3 THEN 'Above normal temperatures'
        WHEN f.temp_anomaly_c < -3 THEN 'Below normal temperatures'
        ELSE 'Near normal temperatures'
    END AS temperature_signal,

    CASE
        WHEN f.gdd_anomaly_pct > 10 THEN 'Accelerated crop development'
        WHEN f.gdd_anomaly_pct < -10 THEN 'Delayed crop development'
        ELSE 'Normal crop development pace'
    END AS development_signal

FROM silver.weather_forecast_daily f
LEFT JOIN reference.crop_region r ON f.region_code = r.region_code
WHERE f.lead_days <= 16  -- Only show actionable forecast window
ORDER BY f.forecast_date DESC, f.valid_date, f.region_code;

-- Gold: Latest 7-day outlook by region
CREATE OR REPLACE VIEW gold.weather_7day_outlook AS
WITH latest_run AS (
    SELECT MAX(forecast_date) as latest_date
    FROM silver.weather_forecast_daily
    WHERE model = 'GFS'
),
forecast_7day AS (
    SELECT
        f.region_code,
        SUM(f.precip_mm) AS total_precip_mm,
        AVG(f.tavg_c) AS avg_temp_c,
        MIN(f.tmin_c) AS min_temp_c,
        MAX(f.tmax_c) AS max_temp_c,
        SUM(f.gdd_corn) AS total_gdd,
        SUM(CASE WHEN f.heat_stress_hours > 0 THEN 1 ELSE 0 END) AS heat_stress_days,
        SUM(CASE WHEN f.frost_risk THEN 1 ELSE 0 END) AS frost_days,
        AVG(f.precip_anomaly_pct) AS avg_precip_anomaly,
        AVG(f.temp_anomaly_c) AS avg_temp_anomaly,
        MAX(f.consecutive_dry_days) AS max_dry_spell
    FROM silver.weather_forecast_daily f
    CROSS JOIN latest_run lr
    WHERE f.forecast_date = lr.latest_date
      AND f.model = 'GFS'
      AND f.lead_days BETWEEN 1 AND 7
    GROUP BY f.region_code
)
SELECT
    f7.region_code,
    r.region_name,
    r.country_code,
    r.primary_commodity,
    f7.total_precip_mm,
    f7.avg_temp_c,
    f7.min_temp_c,
    f7.max_temp_c,
    f7.total_gdd,
    f7.heat_stress_days,
    f7.frost_days,
    f7.avg_precip_anomaly,
    f7.avg_temp_anomaly,
    f7.max_dry_spell,
    CASE
        WHEN f7.avg_precip_anomaly < -25 AND f7.max_dry_spell > 4 THEN 'Yield risk: Drought stress'
        WHEN f7.heat_stress_days >= 3 THEN 'Yield risk: Heat stress'
        WHEN f7.frost_days > 0 AND r.primary_commodity IN ('corn', 'soybeans') THEN 'Yield risk: Frost damage'
        WHEN f7.avg_precip_anomaly > 40 THEN 'Yield risk: Excess moisture'
        WHEN f7.avg_precip_anomaly BETWEEN -10 AND 20 AND f7.heat_stress_days = 0 THEN 'Favorable conditions'
        ELSE 'Mixed conditions'
    END AS yield_outlook
FROM forecast_7day f7
LEFT JOIN reference.crop_region r ON f7.region_code = r.region_code
ORDER BY r.country_code, f7.region_code;

-- Reference: Climatology normals for anomaly calculation
CREATE TABLE IF NOT EXISTS reference.weather_climatology (
    id SERIAL PRIMARY KEY,
    region_code VARCHAR(30) NOT NULL,
    month INT NOT NULL,                   -- 1-12
    day_of_month INT,                     -- NULL for monthly, 1-31 for daily

    -- Normal values (30-year average)
    precip_normal_mm NUMERIC(8,2),
    tmin_normal_c NUMERIC(6,2),
    tmax_normal_c NUMERIC(6,2),
    gdd_normal NUMERIC(8,2),

    -- Standard deviations for anomaly context
    precip_stddev NUMERIC(8,2),
    temp_stddev NUMERIC(6,2),

    UNIQUE (region_code, month, day_of_month)
);

-- Comments
COMMENT ON TABLE bronze.weather_forecast_run IS 'Metadata for forecast model runs ingested from NOAA';
COMMENT ON TABLE silver.weather_forecast_daily IS 'Daily forecasts aggregated by crop region - core analysis table';
COMMENT ON TABLE silver.weather_forecast_period IS 'Multi-day period aggregations for seasonal analysis';
COMMENT ON VIEW gold.weather_forecast_summary IS 'LLM-ready forecast summaries with interpretive signals';
COMMENT ON VIEW gold.weather_7day_outlook IS '7-day outlook by crop region with yield risk assessment';
