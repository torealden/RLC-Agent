-- ============================================================================
-- Round Lakes Commodities - Silver Layer: Standardized Observations
-- ============================================================================
-- File: 05_silver_observation.sql
-- Purpose: Universal time-series observation store with standardized structure
-- Execute: After 04_bronze_wasde.sql
-- ============================================================================
-- The Silver layer is the primary analytics layer. All observations follow
-- a consistent (series_id, observation_time, value) structure regardless
-- of the original source format.
-- ============================================================================

-- ============================================================================
-- OBSERVATION: Universal time-series fact table
-- ============================================================================
CREATE TABLE IF NOT EXISTS silver.observation (
    id                  BIGSERIAL PRIMARY KEY,

    -- Series reference (what is being measured)
    series_id           INTEGER NOT NULL REFERENCES core.series(id),

    -- Time dimension
    observation_time    TIMESTAMPTZ NOT NULL,   -- When the measurement applies
    observation_date    DATE GENERATED ALWAYS AS (observation_time::DATE) STORED,

    -- Value
    value               DECIMAL(20,6),          -- The measurement value
    value_text          VARCHAR(100),           -- For non-numeric observations

    -- Revision tracking
    revision            INTEGER NOT NULL DEFAULT 0,  -- 0 = original, 1+ = revisions
    is_latest           BOOLEAN NOT NULL DEFAULT TRUE,
    superseded_at       TIMESTAMPTZ,            -- When this was superseded by revision

    -- Source reference (for different source types)
    -- One of these will be populated based on source
    wasde_cell_id       BIGINT,                 -- FK to bronze.wasde_cell
    raw_record_id       BIGINT,                 -- FK to bronze.raw_record

    -- Ingestion tracking
    ingest_run_id       BIGINT REFERENCES audit.ingest_run(id),
    source_timestamp    TIMESTAMPTZ,            -- When source published this

    -- Quality flags
    quality_flag        VARCHAR(20) DEFAULT 'good',
    is_estimated        BOOLEAN DEFAULT FALSE,
    is_forecast         BOOLEAN DEFAULT FALSE,
    is_preliminary      BOOLEAN DEFAULT FALSE,

    -- Metadata
    metadata            JSONB DEFAULT '{}',

    -- Timestamps
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    -- Natural key: series + time + revision ensures idempotency
    CONSTRAINT observation_natural_key UNIQUE (series_id, observation_time, revision),

    CONSTRAINT observation_quality_valid CHECK (
        quality_flag IN ('good', 'suspect', 'missing', 'estimated', 'interpolated')
    )
);

COMMENT ON TABLE silver.observation IS
    'Universal time-series observation store. Every measurement from every source '
    'is normalized to this structure. Natural key (series_id, observation_time, revision) '
    'ensures idempotent upserts.';

COMMENT ON COLUMN silver.observation.series_id IS
    'Foreign key to core.series. Defines what is being measured.';
COMMENT ON COLUMN silver.observation.observation_time IS
    'When this observation applies (not when it was published).';
COMMENT ON COLUMN silver.observation.revision IS
    '0 = first publication, 1+ = subsequent revisions. Higher = more recent.';
COMMENT ON COLUMN silver.observation.is_latest IS
    'TRUE if this is the most recent revision for this series+time.';
COMMENT ON COLUMN silver.observation.wasde_cell_id IS
    'If from WASDE, link to bronze.wasde_cell for traceability.';

-- Primary query indexes
CREATE INDEX IF NOT EXISTS idx_observation_series_time
    ON silver.observation(series_id, observation_time DESC);

CREATE INDEX IF NOT EXISTS idx_observation_latest
    ON silver.observation(series_id, observation_time DESC)
    WHERE is_latest = TRUE;

CREATE INDEX IF NOT EXISTS idx_observation_date
    ON silver.observation(observation_date);

CREATE INDEX IF NOT EXISTS idx_observation_ingest
    ON silver.observation(ingest_run_id);

-- For time-range queries
CREATE INDEX IF NOT EXISTS idx_observation_time_range
    ON silver.observation(observation_time);

-- For revision lookups
CREATE INDEX IF NOT EXISTS idx_observation_revision
    ON silver.observation(series_id, observation_time, revision DESC);


-- ============================================================================
-- OBSERVATION_PARTITIONING: Guidance for large-scale deployments
-- ============================================================================
-- When row count exceeds ~100M rows, partition by observation_time:
--
-- 1. Rename existing table:
--    ALTER TABLE silver.observation RENAME TO observation_unpartitioned;
--
-- 2. Create partitioned table:
--    CREATE TABLE silver.observation (
--        LIKE silver.observation_unpartitioned INCLUDING ALL
--    ) PARTITION BY RANGE (observation_time);
--
-- 3. Create partitions:
--    CREATE TABLE silver.observation_y2020
--        PARTITION OF silver.observation
--        FOR VALUES FROM ('2020-01-01') TO ('2021-01-01');
--    -- Repeat for each year
--
-- 4. Migrate data:
--    INSERT INTO silver.observation SELECT * FROM silver.observation_unpartitioned;
--
-- 5. Create future partitions automatically with pg_partman


-- ============================================================================
-- Trigger: Maintain is_latest flag
-- ============================================================================
CREATE OR REPLACE FUNCTION silver.update_is_latest()
RETURNS TRIGGER AS $$
BEGIN
    -- When a new revision is inserted, mark previous as not latest
    IF TG_OP = 'INSERT' AND NEW.revision > 0 THEN
        UPDATE silver.observation
        SET is_latest = FALSE,
            superseded_at = NEW.created_at
        WHERE series_id = NEW.series_id
          AND observation_time = NEW.observation_time
          AND revision < NEW.revision
          AND is_latest = TRUE;
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_observation_is_latest
    AFTER INSERT ON silver.observation
    FOR EACH ROW EXECUTE FUNCTION silver.update_is_latest();


-- ============================================================================
-- Trigger: Update updated_at
-- ============================================================================
CREATE OR REPLACE FUNCTION silver.set_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_observation_updated
    BEFORE UPDATE ON silver.observation
    FOR EACH ROW EXECUTE FUNCTION silver.set_updated_at();


-- ============================================================================
-- OBSERVATION_DAILY: Optimized view for daily frequency series
-- ============================================================================
CREATE OR REPLACE VIEW silver.observation_daily AS
SELECT
    o.id,
    o.series_id,
    s.series_key,
    s.name AS series_name,
    o.observation_date,
    o.value,
    o.quality_flag,
    o.is_estimated,
    o.is_latest,
    o.ingest_run_id,
    o.created_at
FROM silver.observation o
JOIN core.series s ON o.series_id = s.id
WHERE s.frequency = 'daily'
  AND o.is_latest = TRUE;

COMMENT ON VIEW silver.observation_daily IS
    'Convenience view for daily frequency series with latest values only.';


-- ============================================================================
-- OBSERVATION_MONTHLY: Optimized view for monthly frequency series
-- ============================================================================
CREATE OR REPLACE VIEW silver.observation_monthly AS
SELECT
    o.id,
    o.series_id,
    s.series_key,
    s.name AS series_name,
    DATE_TRUNC('month', o.observation_time)::DATE AS observation_month,
    o.value,
    o.revision,
    o.quality_flag,
    o.is_estimated,
    o.is_forecast,
    o.is_latest,
    o.ingest_run_id,
    o.source_timestamp,
    o.created_at
FROM silver.observation o
JOIN core.series s ON o.series_id = s.id
WHERE s.frequency = 'monthly'
  AND o.is_latest = TRUE;

COMMENT ON VIEW silver.observation_monthly IS
    'Convenience view for monthly frequency series with latest values only.';


-- ============================================================================
-- OBSERVATION_ANNUAL: Optimized view for annual frequency series
-- ============================================================================
CREATE OR REPLACE VIEW silver.observation_annual AS
SELECT
    o.id,
    o.series_id,
    s.series_key,
    s.name AS series_name,
    EXTRACT(YEAR FROM o.observation_time)::INTEGER AS observation_year,
    o.value,
    o.revision,
    o.quality_flag,
    o.is_estimated,
    o.is_forecast,
    o.is_latest,
    o.ingest_run_id,
    o.created_at
FROM silver.observation o
JOIN core.series s ON o.series_id = s.id
WHERE s.frequency = 'annual'
  AND o.is_latest = TRUE;

COMMENT ON VIEW silver.observation_annual IS
    'Convenience view for annual frequency series with latest values only.';


-- ============================================================================
-- SERIES_LATEST: Most recent observation per series
-- ============================================================================
CREATE OR REPLACE VIEW silver.series_latest AS
SELECT DISTINCT ON (o.series_id)
    o.series_id,
    s.series_key,
    s.name AS series_name,
    ds.code AS data_source,
    o.observation_time,
    o.observation_date,
    o.value,
    o.quality_flag,
    o.is_estimated,
    o.is_forecast,
    o.ingest_run_id,
    o.created_at AS value_created_at
FROM silver.observation o
JOIN core.series s ON o.series_id = s.id
JOIN core.data_source ds ON s.data_source_id = ds.id
WHERE o.is_latest = TRUE
ORDER BY o.series_id, o.observation_time DESC;

COMMENT ON VIEW silver.series_latest IS
    'Most recent observation for each series. Useful for dashboards and status checks.';


-- ============================================================================
-- OBSERVATION_WITH_CONTEXT: Denormalized view with all metadata
-- ============================================================================
CREATE OR REPLACE VIEW silver.observation_with_context AS
SELECT
    o.id AS observation_id,
    o.series_id,
    s.series_key,
    s.name AS series_name,
    s.description AS series_description,

    -- Data source
    ds.id AS data_source_id,
    ds.code AS data_source_code,
    ds.name AS data_source_name,

    -- Commodity
    c.id AS commodity_id,
    c.code AS commodity_code,
    c.name AS commodity_name,

    -- Location
    l.id AS location_id,
    l.code AS location_code,
    l.name AS location_name,

    -- Unit
    u.id AS unit_id,
    u.code AS unit_code,
    u.name AS unit_name,

    -- Time
    o.observation_time,
    o.observation_date,
    EXTRACT(YEAR FROM o.observation_time)::INTEGER AS observation_year,
    EXTRACT(MONTH FROM o.observation_time)::INTEGER AS observation_month,

    -- Value and quality
    o.value,
    o.value_text,
    o.revision,
    o.is_latest,
    o.quality_flag,
    o.is_estimated,
    o.is_forecast,
    o.is_preliminary,

    -- Metadata
    o.metadata,
    o.ingest_run_id,
    o.source_timestamp,
    o.created_at,
    o.updated_at

FROM silver.observation o
JOIN core.series s ON o.series_id = s.id
JOIN core.data_source ds ON s.data_source_id = ds.id
LEFT JOIN core.commodity c ON s.commodity_id = c.id
LEFT JOIN core.location l ON s.location_id = l.id
LEFT JOIN core.unit u ON s.unit_id = u.id;

COMMENT ON VIEW silver.observation_with_context IS
    'Fully denormalized observation view with all dimension attributes. '
    'Use for ad-hoc analysis. For performance-critical queries, use base table with joins.';


-- ============================================================================
-- OBSERVATION_TIMESERIES: For time-series charting
-- ============================================================================
CREATE OR REPLACE VIEW silver.observation_timeseries AS
SELECT
    s.series_key,
    s.name AS series_name,
    c.code AS commodity,
    l.code AS location,
    u.code AS unit,
    s.frequency,
    o.observation_time,
    o.observation_date,
    o.value
FROM silver.observation o
JOIN core.series s ON o.series_id = s.id
LEFT JOIN core.commodity c ON s.commodity_id = c.id
LEFT JOIN core.location l ON s.location_id = l.id
LEFT JOIN core.unit u ON s.unit_id = u.id
WHERE o.is_latest = TRUE
  AND o.value IS NOT NULL;

COMMENT ON VIEW silver.observation_timeseries IS
    'Simplified view for time-series visualization. Latest values only.';


-- ============================================================================
-- Statistics table for series health monitoring
-- ============================================================================
CREATE TABLE IF NOT EXISTS silver.series_stats (
    id              SERIAL PRIMARY KEY,
    series_id       INTEGER NOT NULL REFERENCES core.series(id) UNIQUE,
    observation_count INTEGER DEFAULT 0,
    latest_observation_time TIMESTAMPTZ,
    earliest_observation_time TIMESTAMPTZ,
    min_value       DECIMAL(20,6),
    max_value       DECIMAL(20,6),
    avg_value       DECIMAL(20,6),
    last_ingest_run_id BIGINT REFERENCES audit.ingest_run(id),
    last_updated    TIMESTAMPTZ DEFAULT NOW()
);

COMMENT ON TABLE silver.series_stats IS
    'Pre-aggregated statistics per series for monitoring and health checks.';

-- Index
CREATE INDEX IF NOT EXISTS idx_series_stats_updated
    ON silver.series_stats(last_updated);


-- ============================================================================
-- Function: Refresh series statistics
-- ============================================================================
CREATE OR REPLACE FUNCTION silver.refresh_series_stats(p_series_id INTEGER DEFAULT NULL)
RETURNS INTEGER AS $$
DECLARE
    v_count INTEGER := 0;
BEGIN
    INSERT INTO silver.series_stats (
        series_id,
        observation_count,
        latest_observation_time,
        earliest_observation_time,
        min_value,
        max_value,
        avg_value,
        last_updated
    )
    SELECT
        series_id,
        COUNT(*) AS observation_count,
        MAX(observation_time) AS latest_observation_time,
        MIN(observation_time) AS earliest_observation_time,
        MIN(value) AS min_value,
        MAX(value) AS max_value,
        AVG(value) AS avg_value,
        NOW() AS last_updated
    FROM silver.observation
    WHERE is_latest = TRUE
      AND (p_series_id IS NULL OR series_id = p_series_id)
    GROUP BY series_id
    ON CONFLICT (series_id) DO UPDATE SET
        observation_count = EXCLUDED.observation_count,
        latest_observation_time = EXCLUDED.latest_observation_time,
        earliest_observation_time = EXCLUDED.earliest_observation_time,
        min_value = EXCLUDED.min_value,
        max_value = EXCLUDED.max_value,
        avg_value = EXCLUDED.avg_value,
        last_updated = EXCLUDED.last_updated;

    GET DIAGNOSTICS v_count = ROW_COUNT;
    RETURN v_count;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION silver.refresh_series_stats IS
    'Refresh statistics for one or all series. Call after bulk ingestion.';


-- ============================================================================
-- Verification
-- ============================================================================
DO $$
BEGIN
    RAISE NOTICE 'Silver layer created:';
    RAISE NOTICE '  - silver.observation (main fact table)';
    RAISE NOTICE '  - silver.observation_daily (view)';
    RAISE NOTICE '  - silver.observation_monthly (view)';
    RAISE NOTICE '  - silver.observation_annual (view)';
    RAISE NOTICE '  - silver.series_latest (view)';
    RAISE NOTICE '  - silver.observation_with_context (view)';
    RAISE NOTICE '  - silver.observation_timeseries (view)';
    RAISE NOTICE '  - silver.series_stats (monitoring table)';
END $$;
