-- =============================================================================
-- RLC Commodities Database Schema - Weather Layer
-- Version: 1.0.0
-- =============================================================================
--
-- WEATHER DATA ARCHITECTURE
-- -------------------------
-- Weather data follows the medallion pattern:
--
-- BRONZE: Raw weather API responses (OpenWeather, Open-Meteo, email extracts)
-- SILVER: Standardized daily observations per location
-- GOLD: Business views for dashboards and analysis
--
-- Sources:
-- - OpenWeather API: Current/forecast (daily collection)
-- - Open-Meteo API: Historical data (backfill)
-- - Email extracts: LLM-extracted weather mentions
--
-- =============================================================================

-- =============================================================================
-- CONFIGURATION TABLE (public schema)
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Weather Locations: Registered agricultural weather stations
-- Mirrors weather_locations.json for database queries
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS public.weather_location (
    id VARCHAR(50) PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    display_name VARCHAR(150) NOT NULL,
    region VARCHAR(50) NOT NULL,
    country VARCHAR(5) NOT NULL,
    latitude DECIMAL(9, 6) NOT NULL,
    longitude DECIMAL(9, 6) NOT NULL,
    timezone VARCHAR(50) DEFAULT 'UTC',
    commodities TEXT[] DEFAULT '{}',
    is_active BOOLEAN DEFAULT TRUE,
    notes TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_weather_location_region ON public.weather_location(region);
CREATE INDEX IF NOT EXISTS idx_weather_location_country ON public.weather_location(country);
CREATE INDEX IF NOT EXISTS idx_weather_location_active ON public.weather_location(is_active) WHERE is_active = TRUE;

COMMENT ON TABLE public.weather_location IS 'Registry of agricultural weather monitoring locations';

-- -----------------------------------------------------------------------------
-- Weather Location Aliases: Fuzzy matching for email extraction
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS public.weather_location_alias (
    alias VARCHAR(100) PRIMARY KEY,
    location_id VARCHAR(50) NOT NULL REFERENCES public.weather_location(id),
    created_at TIMESTAMPTZ DEFAULT NOW()
);

COMMENT ON TABLE public.weather_location_alias IS 'Alias mappings for location name resolution';

-- =============================================================================
-- BRONZE LAYER - Raw Weather Data
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Weather Raw: Raw API responses stored as JSONB
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS bronze.weather_raw (
    id BIGSERIAL PRIMARY KEY,

    -- Source identification
    location_id VARCHAR(50) NOT NULL,
    source VARCHAR(50) NOT NULL,           -- 'openweather', 'open_meteo', 'email_extract'
    source_api VARCHAR(100),               -- API endpoint used

    -- Raw data
    raw_response JSONB NOT NULL,           -- Complete API response

    -- Time context
    observation_date DATE NOT NULL,        -- Date the observation applies to
    observation_time TIMESTAMPTZ,          -- Specific time if available
    collected_at TIMESTAMPTZ DEFAULT NOW(),

    -- Batch tracking
    batch_id UUID,
    ingest_run_id UUID,

    -- Processing status
    is_processed BOOLEAN DEFAULT FALSE,
    processed_at TIMESTAMPTZ,
    processing_error TEXT,

    -- File reference (for bulk loads)
    source_file VARCHAR(500),

    created_at TIMESTAMPTZ DEFAULT NOW(),

    -- Natural key for idempotent upserts
    UNIQUE (location_id, source, observation_date)
);

CREATE INDEX IF NOT EXISTS idx_weather_raw_location ON bronze.weather_raw(location_id);
CREATE INDEX IF NOT EXISTS idx_weather_raw_date ON bronze.weather_raw(observation_date DESC);
CREATE INDEX IF NOT EXISTS idx_weather_raw_source ON bronze.weather_raw(source);
CREATE INDEX IF NOT EXISTS idx_weather_raw_batch ON bronze.weather_raw(batch_id);
CREATE INDEX IF NOT EXISTS idx_weather_raw_unprocessed ON bronze.weather_raw(is_processed) WHERE is_processed = FALSE;

COMMENT ON TABLE bronze.weather_raw IS 'Raw weather API responses. One row per location per day per source.';

-- -----------------------------------------------------------------------------
-- Weather Email Extract: LLM-extracted weather mentions from emails
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS bronze.weather_email_extract (
    id BIGSERIAL PRIMARY KEY,

    -- Email reference
    email_id VARCHAR(200) NOT NULL,        -- Gmail message ID
    email_subject VARCHAR(500),
    email_from VARCHAR(200),
    email_date TIMESTAMPTZ NOT NULL,

    -- Extracted data (from LLM)
    extracted_locations TEXT[],            -- Cities/regions mentioned
    matched_location_ids TEXT[],           -- Resolved to our location IDs
    weather_summary TEXT,                  -- LLM summary of weather content
    extracted_conditions JSONB,            -- Structured conditions if parseable

    -- Processing info
    llm_model VARCHAR(50),
    llm_prompt_tokens INT,
    llm_completion_tokens INT,
    processing_time_ms INT,

    -- Tracking
    batch_id UUID,
    collected_at TIMESTAMPTZ DEFAULT NOW(),

    -- Natural key
    UNIQUE (email_id)
);

CREATE INDEX IF NOT EXISTS idx_weather_email_date ON bronze.weather_email_extract(email_date DESC);
CREATE INDEX IF NOT EXISTS idx_weather_email_locations ON bronze.weather_email_extract USING GIN(matched_location_ids);

COMMENT ON TABLE bronze.weather_email_extract IS 'Weather information extracted from forwarded emails';

-- =============================================================================
-- SILVER LAYER - Standardized Weather Observations
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Weather Observation: Standardized daily weather metrics
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS silver.weather_observation (
    id BIGSERIAL PRIMARY KEY,

    -- Location
    location_id VARCHAR(50) NOT NULL,

    -- Time
    observation_date DATE NOT NULL,

    -- Temperature (Celsius - standard SI unit)
    temp_high_c DECIMAL(5, 2),
    temp_low_c DECIMAL(5, 2),
    temp_avg_c DECIMAL(5, 2),

    -- Temperature (Fahrenheit - for US convenience)
    temp_high_f DECIMAL(5, 1),
    temp_low_f DECIMAL(5, 1),
    temp_avg_f DECIMAL(5, 1),

    -- Precipitation
    precipitation_mm DECIMAL(8, 2),
    precipitation_in DECIMAL(6, 3),        -- For US convenience
    precipitation_hours DECIMAL(5, 1),

    -- Atmospheric
    humidity_pct DECIMAL(5, 2),
    pressure_hpa DECIMAL(7, 2),
    cloud_cover_pct DECIMAL(5, 2),

    -- Wind
    wind_speed_ms DECIMAL(6, 2),           -- m/s standard
    wind_speed_mph DECIMAL(6, 2),          -- For US convenience
    wind_gust_ms DECIMAL(6, 2),
    wind_gust_mph DECIMAL(6, 2),
    wind_direction_deg INT,

    -- Agricultural metrics
    soil_moisture_0_7cm DECIMAL(5, 3),
    soil_temp_0_7cm_c DECIMAL(5, 2),
    evapotranspiration_mm DECIMAL(5, 2),   -- ET0 FAO

    -- Conditions
    conditions_code INT,                    -- WMO weather code
    conditions_text VARCHAR(100),

    -- Data quality
    source VARCHAR(50) NOT NULL,           -- Primary source for this record
    quality_flag VARCHAR(20) DEFAULT 'OK',
    is_interpolated BOOLEAN DEFAULT FALSE,

    -- Lineage
    bronze_id BIGINT,                      -- Reference to bronze.weather_raw
    ingest_run_id UUID,

    -- Timestamps
    processed_at TIMESTAMPTZ DEFAULT NOW(),
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),

    -- Natural key: one observation per location per day per source
    UNIQUE (location_id, observation_date, source)
);

CREATE INDEX IF NOT EXISTS idx_weather_obs_location ON silver.weather_observation(location_id);
CREATE INDEX IF NOT EXISTS idx_weather_obs_date ON silver.weather_observation(observation_date DESC);
CREATE INDEX IF NOT EXISTS idx_weather_obs_loc_date ON silver.weather_observation(location_id, observation_date DESC);
CREATE INDEX IF NOT EXISTS idx_weather_obs_source ON silver.weather_observation(source);
CREATE INDEX IF NOT EXISTS idx_weather_obs_quality ON silver.weather_observation(quality_flag) WHERE quality_flag != 'OK';

COMMENT ON TABLE silver.weather_observation IS 'Standardized daily weather observations. One row per location per day.';

-- =============================================================================
-- GOLD LAYER - Business Views
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Weather Summary: Latest weather with location details
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW gold.weather_summary AS
SELECT
    wo.location_id,
    wl.display_name,
    wl.region,
    wl.country,
    wo.observation_date,
    wo.temp_high_f,
    wo.temp_low_f,
    wo.temp_avg_f,
    wo.precipitation_in,
    wo.humidity_pct,
    wo.wind_speed_mph,
    wo.conditions_text,
    -- 7-day rolling averages
    AVG(wo.temp_avg_f) OVER w AS temp_7day_avg_f,
    SUM(wo.precipitation_in) OVER w AS precip_7day_total_in,
    -- Temperature departure from 7-day avg
    wo.temp_avg_f - AVG(wo.temp_avg_f) OVER w AS temp_departure_f,
    -- Data freshness
    NOW() - wo.observation_date::timestamptz AS data_age,
    wo.source,
    wo.quality_flag
FROM silver.weather_observation wo
JOIN public.weather_location wl ON wo.location_id = wl.id
WHERE wl.is_active = TRUE
WINDOW w AS (
    PARTITION BY wo.location_id
    ORDER BY wo.observation_date
    ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
);

COMMENT ON VIEW gold.weather_summary IS 'Weather summary with rolling averages for PowerBI dashboards';

-- -----------------------------------------------------------------------------
-- Weather Latest: Most recent observation per location
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW gold.weather_latest AS
SELECT DISTINCT ON (wo.location_id)
    wo.location_id,
    wl.display_name,
    wl.region,
    wl.country,
    wl.commodities,
    wo.observation_date,
    wo.temp_high_f,
    wo.temp_low_f,
    wo.precipitation_in,
    wo.humidity_pct,
    wo.conditions_text,
    wo.source,
    wo.updated_at
FROM silver.weather_observation wo
JOIN public.weather_location wl ON wo.location_id = wl.id
WHERE wl.is_active = TRUE
ORDER BY wo.location_id, wo.observation_date DESC;

COMMENT ON VIEW gold.weather_latest IS 'Most recent weather observation per location';

-- -----------------------------------------------------------------------------
-- Weather Alerts: Extreme conditions for dashboard highlighting
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW gold.weather_alerts AS
SELECT
    wo.location_id,
    wl.display_name,
    wl.region,
    wl.country,
    wl.commodities,
    wo.observation_date,
    wo.temp_high_f,
    wo.temp_low_f,
    wo.precipitation_in,
    CASE
        WHEN wo.temp_high_f > 100 THEN 'EXTREME_HEAT'
        WHEN wo.temp_high_f > 95 THEN 'HIGH_HEAT'
        WHEN wo.temp_low_f < 28 THEN 'HARD_FREEZE'
        WHEN wo.temp_low_f < 32 THEN 'FROST'
        WHEN wo.precipitation_in > 2 THEN 'HEAVY_RAIN'
        WHEN wo.precipitation_in > 1 THEN 'SIGNIFICANT_RAIN'
        ELSE NULL
    END AS alert_type,
    CASE
        WHEN wo.temp_high_f > 100 THEN 'Temperature exceeds 100F - crop stress risk'
        WHEN wo.temp_high_f > 95 THEN 'Temperature exceeds 95F - heat stress possible'
        WHEN wo.temp_low_f < 28 THEN 'Temperature below 28F - hard freeze risk'
        WHEN wo.temp_low_f < 32 THEN 'Temperature below 32F - frost risk'
        WHEN wo.precipitation_in > 2 THEN 'Heavy rain >2in - flooding risk'
        WHEN wo.precipitation_in > 1 THEN 'Significant rain >1in - field work limited'
        ELSE NULL
    END AS alert_description,
    wo.conditions_text
FROM silver.weather_observation wo
JOIN public.weather_location wl ON wo.location_id = wl.id
WHERE wl.is_active = TRUE
  AND wo.observation_date >= CURRENT_DATE - INTERVAL '7 days'
  AND (
    wo.temp_high_f > 95
    OR wo.temp_low_f < 32
    OR wo.precipitation_in > 1
  );

COMMENT ON VIEW gold.weather_alerts IS 'Weather alerts for extreme conditions affecting agriculture';

-- -----------------------------------------------------------------------------
-- Weather Regional Summary: Aggregated by region
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW gold.weather_regional_summary AS
SELECT
    wl.region,
    wl.country,
    wo.observation_date,
    COUNT(DISTINCT wo.location_id) AS location_count,
    ROUND(AVG(wo.temp_high_f)::numeric, 1) AS avg_temp_high_f,
    ROUND(AVG(wo.temp_low_f)::numeric, 1) AS avg_temp_low_f,
    ROUND(AVG(wo.temp_avg_f)::numeric, 1) AS avg_temp_f,
    ROUND(SUM(wo.precipitation_in)::numeric / COUNT(*), 2) AS avg_precip_in,
    ROUND(AVG(wo.humidity_pct)::numeric, 0) AS avg_humidity_pct,
    MIN(wo.temp_low_f) AS min_temp_f,
    MAX(wo.temp_high_f) AS max_temp_f,
    MAX(wo.precipitation_in) AS max_precip_in
FROM silver.weather_observation wo
JOIN public.weather_location wl ON wo.location_id = wl.id
WHERE wl.is_active = TRUE
GROUP BY wl.region, wl.country, wo.observation_date;

COMMENT ON VIEW gold.weather_regional_summary IS 'Regional weather aggregations by date';

-- =============================================================================
-- DATA QUALITY FUNCTIONS
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Function: Mark weather as processed and transform to silver
-- -----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION bronze.process_weather_to_silver(p_batch_id UUID DEFAULT NULL)
RETURNS TABLE(processed_count INT, error_count INT) AS $$
DECLARE
    v_processed INT := 0;
    v_errors INT := 0;
BEGIN
    -- Process unprocessed bronze records
    INSERT INTO silver.weather_observation (
        location_id,
        observation_date,
        temp_high_c,
        temp_low_c,
        temp_avg_c,
        temp_high_f,
        temp_low_f,
        temp_avg_f,
        precipitation_mm,
        precipitation_in,
        humidity_pct,
        wind_speed_ms,
        wind_speed_mph,
        conditions_code,
        conditions_text,
        source,
        bronze_id
    )
    SELECT
        wr.location_id,
        wr.observation_date,
        -- Extract from JSONB based on source
        CASE wr.source
            WHEN 'openweather' THEN (wr.raw_response->'main'->>'temp')::decimal
            WHEN 'open_meteo' THEN (wr.raw_response->'daily'->'temperature_2m_mean'->>0)::decimal
            ELSE NULL
        END AS temp_high_c,
        CASE wr.source
            WHEN 'openweather' THEN (wr.raw_response->'main'->>'temp_min')::decimal
            WHEN 'open_meteo' THEN (wr.raw_response->'daily'->'temperature_2m_min'->>0)::decimal
            ELSE NULL
        END AS temp_low_c,
        CASE wr.source
            WHEN 'openweather' THEN (wr.raw_response->'main'->>'temp')::decimal
            WHEN 'open_meteo' THEN (wr.raw_response->'daily'->'temperature_2m_mean'->>0)::decimal
            ELSE NULL
        END AS temp_avg_c,
        -- Fahrenheit conversions
        CASE wr.source
            WHEN 'openweather' THEN ((wr.raw_response->'main'->>'temp')::decimal * 9/5 + 32)
            WHEN 'open_meteo' THEN ((wr.raw_response->'daily'->'temperature_2m_mean'->>0)::decimal * 9/5 + 32)
            ELSE NULL
        END AS temp_high_f,
        CASE wr.source
            WHEN 'openweather' THEN ((wr.raw_response->'main'->>'temp_min')::decimal * 9/5 + 32)
            WHEN 'open_meteo' THEN ((wr.raw_response->'daily'->'temperature_2m_min'->>0)::decimal * 9/5 + 32)
            ELSE NULL
        END AS temp_low_f,
        CASE wr.source
            WHEN 'openweather' THEN ((wr.raw_response->'main'->>'temp')::decimal * 9/5 + 32)
            WHEN 'open_meteo' THEN ((wr.raw_response->'daily'->'temperature_2m_mean'->>0)::decimal * 9/5 + 32)
            ELSE NULL
        END AS temp_avg_f,
        -- Precipitation
        CASE wr.source
            WHEN 'open_meteo' THEN (wr.raw_response->'daily'->'precipitation_sum'->>0)::decimal
            ELSE 0
        END AS precipitation_mm,
        CASE wr.source
            WHEN 'open_meteo' THEN ((wr.raw_response->'daily'->'precipitation_sum'->>0)::decimal / 25.4)
            ELSE 0
        END AS precipitation_in,
        -- Humidity
        CASE wr.source
            WHEN 'openweather' THEN (wr.raw_response->'main'->>'humidity')::decimal
            ELSE NULL
        END AS humidity_pct,
        -- Wind
        CASE wr.source
            WHEN 'openweather' THEN (wr.raw_response->'wind'->>'speed')::decimal
            WHEN 'open_meteo' THEN (wr.raw_response->'daily'->'wind_speed_10m_max'->>0)::decimal / 3.6
            ELSE NULL
        END AS wind_speed_ms,
        CASE wr.source
            WHEN 'openweather' THEN ((wr.raw_response->'wind'->>'speed')::decimal * 2.237)
            WHEN 'open_meteo' THEN ((wr.raw_response->'daily'->'wind_speed_10m_max'->>0)::decimal / 3.6 * 2.237)
            ELSE NULL
        END AS wind_speed_mph,
        -- Conditions
        CASE wr.source
            WHEN 'openweather' THEN (wr.raw_response->'weather'->0->>'id')::int
            WHEN 'open_meteo' THEN (wr.raw_response->'daily'->'weather_code'->>0)::int
            ELSE NULL
        END AS conditions_code,
        CASE wr.source
            WHEN 'openweather' THEN wr.raw_response->'weather'->0->>'description'
            ELSE NULL
        END AS conditions_text,
        wr.source,
        wr.id
    FROM bronze.weather_raw wr
    WHERE wr.is_processed = FALSE
      AND (p_batch_id IS NULL OR wr.batch_id = p_batch_id)
    ON CONFLICT (location_id, observation_date, source) DO UPDATE SET
        temp_high_c = EXCLUDED.temp_high_c,
        temp_low_c = EXCLUDED.temp_low_c,
        temp_avg_c = EXCLUDED.temp_avg_c,
        temp_high_f = EXCLUDED.temp_high_f,
        temp_low_f = EXCLUDED.temp_low_f,
        temp_avg_f = EXCLUDED.temp_avg_f,
        precipitation_mm = EXCLUDED.precipitation_mm,
        precipitation_in = EXCLUDED.precipitation_in,
        humidity_pct = EXCLUDED.humidity_pct,
        wind_speed_ms = EXCLUDED.wind_speed_ms,
        wind_speed_mph = EXCLUDED.wind_speed_mph,
        conditions_code = EXCLUDED.conditions_code,
        conditions_text = EXCLUDED.conditions_text,
        bronze_id = EXCLUDED.bronze_id,
        updated_at = NOW();

    GET DIAGNOSTICS v_processed = ROW_COUNT;

    -- Mark bronze records as processed
    UPDATE bronze.weather_raw
    SET is_processed = TRUE, processed_at = NOW()
    WHERE is_processed = FALSE
      AND (p_batch_id IS NULL OR batch_id = p_batch_id);

    RETURN QUERY SELECT v_processed, v_errors;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION bronze.process_weather_to_silver IS 'Transform bronze weather data to silver observations';

-- =============================================================================
-- SEED DATA SOURCE
-- =============================================================================

-- Add weather data sources to the registry
INSERT INTO public.data_source (code, name, description, api_type, update_frequency) VALUES
    ('OPENWEATHER', 'OpenWeatherMap', 'Current weather and forecasts', 'REST', 'DAILY'),
    ('OPEN_METEO', 'Open-Meteo', 'Historical and forecast weather data', 'REST', 'DAILY'),
    ('WEATHER_EMAIL', 'Weather Email Extract', 'LLM-extracted weather from forwarded emails', 'INTERNAL', 'ON_EVENT')
ON CONFLICT (code) DO NOTHING;
