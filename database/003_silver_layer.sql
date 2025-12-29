-- =============================================================================
-- RLC Commodities Database Schema - Silver Layer
-- Version: 1.0.0
-- =============================================================================
--
-- SILVER LAYER PHILOSOPHY
-- -----------------------
-- Silver is the canonical, standardized representation:
-- - Universal schema: (series_id, observation_time, value)
-- - Consistent units and naming
-- - Quality flags and validation status
-- - Traceable back to bronze via ingest_run_id
--
-- All analytical queries should use Silver, not Bronze.
--
-- =============================================================================

-- =============================================================================
-- CORE OBSERVATION TABLE
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Observation: Universal time-series fact table
-- -----------------------------------------------------------------------------
CREATE TABLE silver.observation (
    id BIGSERIAL PRIMARY KEY,

    -- Core dimensions
    series_id INT NOT NULL REFERENCES public.series(id),
    observation_time TIMESTAMPTZ NOT NULL,
    observation_date DATE NOT NULL,       -- Extracted for easier querying

    -- The value
    value NUMERIC(20, 6) NOT NULL,

    -- Revision tracking (for sources like WASDE that revise values)
    revision_number INT DEFAULT 0,        -- 0 = original, 1+ = revisions
    is_preliminary BOOLEAN DEFAULT FALSE,
    is_final BOOLEAN DEFAULT TRUE,

    -- Quality flags
    quality_flag VARCHAR(20) DEFAULT 'OK',  -- 'OK', 'SUSPECT', 'MISSING_INTERPOLATED', 'ESTIMATED'
    quality_notes TEXT,

    -- Validation
    is_validated BOOLEAN DEFAULT FALSE,
    validated_at TIMESTAMPTZ,
    validated_by VARCHAR(100),

    -- Lineage tracking
    ingest_run_id UUID REFERENCES audit.ingest_run(id),
    source_table VARCHAR(100),            -- Bronze table name
    source_id BIGINT,                     -- ID in bronze table

    -- Timestamps
    effective_from TIMESTAMPTZ DEFAULT NOW(),  -- For SCD Type 2 if needed
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),

    -- Natural key: one value per series per time per revision
    UNIQUE (series_id, observation_time, revision_number)
);

-- Critical indexes for time-series queries
CREATE INDEX idx_obs_series_time ON silver.observation(series_id, observation_time DESC);
CREATE INDEX idx_obs_date ON silver.observation(observation_date);
CREATE INDEX idx_obs_series ON silver.observation(series_id);
CREATE INDEX idx_obs_ingest ON silver.observation(ingest_run_id);
CREATE INDEX idx_obs_quality ON silver.observation(quality_flag) WHERE quality_flag != 'OK';
CREATE INDEX idx_obs_validated ON silver.observation(is_validated) WHERE is_validated = FALSE;

-- Partial index for current (non-revised) values only
CREATE INDEX idx_obs_current ON silver.observation(series_id, observation_time)
    WHERE revision_number = 0;

COMMENT ON TABLE silver.observation IS 'Universal time-series store. One row = one measurement at one point in time.';

-- =============================================================================
-- AGGREGATED VIEWS FOR COMMON QUERIES
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Latest observation per series (useful for dashboards)
-- -----------------------------------------------------------------------------
CREATE VIEW silver.latest_observation AS
SELECT DISTINCT ON (series_id)
    o.series_id,
    s.name AS series_name,
    s.commodity_code,
    s.location_code,
    o.observation_time,
    o.observation_date,
    o.value,
    o.quality_flag,
    o.is_validated,
    u.code AS unit_code
FROM silver.observation o
JOIN public.series s ON o.series_id = s.id
LEFT JOIN public.unit u ON s.unit_code = u.code
WHERE o.revision_number = 0
ORDER BY series_id, observation_time DESC;

COMMENT ON VIEW silver.latest_observation IS 'Most recent observation for each series';

-- =============================================================================
-- SPECIALIZED TABLES FOR COMPLEX DATA
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Trade Flow: Aggregated trade data by commodity/country/period
-- -----------------------------------------------------------------------------
CREATE TABLE silver.trade_flow (
    id BIGSERIAL PRIMARY KEY,

    -- Dimensions
    commodity_code VARCHAR(30) NOT NULL REFERENCES public.commodity(code),
    location_code VARCHAR(50) NOT NULL REFERENCES public.location(code),
    flow_direction VARCHAR(10) NOT NULL,  -- 'EXPORT', 'IMPORT'

    -- Time
    period_start DATE NOT NULL,
    period_end DATE NOT NULL,
    period_type VARCHAR(20) NOT NULL,     -- 'MONTH', 'WEEK', 'MARKETING_YEAR'
    marketing_year VARCHAR(10),

    -- Values (standardized to metric tons and USD)
    quantity_mt NUMERIC(18, 3),
    value_usd NUMERIC(18, 2),

    -- Derived
    unit_value_usd_mt NUMERIC(12, 2),     -- value_usd / quantity_mt

    -- Quality
    is_complete BOOLEAN DEFAULT TRUE,      -- All sub-periods present
    data_source VARCHAR(50),               -- 'CENSUS', 'FGIS', 'CUSTOMS'

    -- Tracking
    ingest_run_id UUID REFERENCES audit.ingest_run(id),
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),

    -- Natural key
    UNIQUE (commodity_code, location_code, flow_direction, period_start, period_type, data_source)
);

CREATE INDEX idx_trade_flow_commodity ON silver.trade_flow(commodity_code);
CREATE INDEX idx_trade_flow_location ON silver.trade_flow(location_code);
CREATE INDEX idx_trade_flow_period ON silver.trade_flow(period_start, period_end);
CREATE INDEX idx_trade_flow_my ON silver.trade_flow(marketing_year);

-- -----------------------------------------------------------------------------
-- Balance Sheet Item: S&D balance sheet components
-- -----------------------------------------------------------------------------
CREATE TABLE silver.balance_sheet_item (
    id BIGSERIAL PRIMARY KEY,

    -- Dimensions
    commodity_code VARCHAR(30) NOT NULL REFERENCES public.commodity(code),
    location_code VARCHAR(50) NOT NULL REFERENCES public.location(code),
    item_type VARCHAR(50) NOT NULL,       -- 'PRODUCTION', 'IMPORTS', 'CRUSH', 'EXPORTS', 'ENDING_STOCKS', etc.

    -- Time
    marketing_year VARCHAR(10) NOT NULL,
    report_date DATE NOT NULL,            -- WASDE release date (for revision tracking)

    -- Value (standardized units - million bushels or MMT depending on commodity)
    value NUMERIC(18, 4),
    unit_code VARCHAR(30) REFERENCES public.unit(code),

    -- Projection vs actual
    is_projection BOOLEAN DEFAULT FALSE,
    projection_month VARCHAR(20),         -- 'Jan', 'Feb', etc. for WASDE projections

    -- Change tracking
    change_from_previous NUMERIC(18, 4),
    previous_report_date DATE,

    -- Source
    data_source VARCHAR(50) DEFAULT 'WASDE',
    ingest_run_id UUID REFERENCES audit.ingest_run(id),

    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),

    -- Natural key
    UNIQUE (commodity_code, location_code, item_type, marketing_year, report_date)
);

CREATE INDEX idx_bs_commodity ON silver.balance_sheet_item(commodity_code);
CREATE INDEX idx_bs_location ON silver.balance_sheet_item(location_code);
CREATE INDEX idx_bs_my ON silver.balance_sheet_item(marketing_year);
CREATE INDEX idx_bs_report ON silver.balance_sheet_item(report_date);
CREATE INDEX idx_bs_type ON silver.balance_sheet_item(item_type);

-- =============================================================================
-- SERIES MANAGEMENT HELPERS
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Function: Get or create series (prevents duplicate series definitions)
-- -----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION public.get_or_create_series(
    p_data_source_code VARCHAR,
    p_series_key VARCHAR,
    p_name VARCHAR,
    p_commodity_code VARCHAR DEFAULT NULL,
    p_location_code VARCHAR DEFAULT NULL,
    p_unit_code VARCHAR DEFAULT NULL,
    p_frequency VARCHAR DEFAULT 'DAILY',
    p_series_type VARCHAR DEFAULT NULL
) RETURNS INT AS $$
DECLARE
    v_data_source_id INT;
    v_series_id INT;
BEGIN
    -- Get data source ID
    SELECT id INTO v_data_source_id
    FROM public.data_source
    WHERE code = p_data_source_code;

    IF v_data_source_id IS NULL THEN
        RAISE EXCEPTION 'Unknown data source: %', p_data_source_code;
    END IF;

    -- Try to find existing series
    SELECT id INTO v_series_id
    FROM public.series
    WHERE data_source_id = v_data_source_id
      AND series_key = p_series_key;

    -- Create if not exists
    IF v_series_id IS NULL THEN
        INSERT INTO public.series (
            data_source_id, series_key, name,
            commodity_code, location_code, unit_code,
            frequency, series_type
        ) VALUES (
            v_data_source_id, p_series_key, p_name,
            p_commodity_code, p_location_code, p_unit_code,
            p_frequency, p_series_type
        )
        RETURNING id INTO v_series_id;
    END IF;

    RETURN v_series_id;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION public.get_or_create_series IS 'Safely get or create a series. Prevents duplicates across agents.';

-- -----------------------------------------------------------------------------
-- Function: Insert observation (idempotent)
-- -----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION silver.upsert_observation(
    p_series_id INT,
    p_observation_time TIMESTAMPTZ,
    p_value NUMERIC,
    p_ingest_run_id UUID,
    p_revision_number INT DEFAULT 0,
    p_quality_flag VARCHAR DEFAULT 'OK',
    p_source_table VARCHAR DEFAULT NULL,
    p_source_id BIGINT DEFAULT NULL
) RETURNS BIGINT AS $$
DECLARE
    v_obs_id BIGINT;
BEGIN
    INSERT INTO silver.observation (
        series_id, observation_time, observation_date, value,
        revision_number, quality_flag,
        ingest_run_id, source_table, source_id
    ) VALUES (
        p_series_id, p_observation_time, p_observation_time::DATE, p_value,
        p_revision_number, p_quality_flag,
        p_ingest_run_id, p_source_table, p_source_id
    )
    ON CONFLICT (series_id, observation_time, revision_number)
    DO UPDATE SET
        value = EXCLUDED.value,
        quality_flag = EXCLUDED.quality_flag,
        ingest_run_id = EXCLUDED.ingest_run_id,
        source_table = EXCLUDED.source_table,
        source_id = EXCLUDED.source_id,
        updated_at = NOW()
    RETURNING id INTO v_obs_id;

    RETURN v_obs_id;
END;
$$ LANGUAGE plpgsql;

-- =============================================================================
-- INGEST RUN LIFECYCLE HELPERS
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Function: Start an ingest run
-- -----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION audit.start_ingest_run(
    p_data_source_code VARCHAR,
    p_job_type VARCHAR DEFAULT 'INCREMENTAL',
    p_job_name VARCHAR DEFAULT NULL,
    p_agent_id VARCHAR DEFAULT NULL,
    p_request_params JSONB DEFAULT NULL
) RETURNS UUID AS $$
DECLARE
    v_data_source_id INT;
    v_run_id UUID;
BEGIN
    SELECT id INTO v_data_source_id
    FROM public.data_source WHERE code = p_data_source_code;

    IF v_data_source_id IS NULL THEN
        RAISE EXCEPTION 'Unknown data source: %', p_data_source_code;
    END IF;

    INSERT INTO audit.ingest_run (
        data_source_id, job_type, job_name,
        agent_id, request_params, status
    ) VALUES (
        v_data_source_id, p_job_type, p_job_name,
        p_agent_id, p_request_params, 'RUNNING'
    )
    RETURNING id INTO v_run_id;

    RETURN v_run_id;
END;
$$ LANGUAGE plpgsql;

-- -----------------------------------------------------------------------------
-- Function: Complete an ingest run
-- -----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION audit.complete_ingest_run(
    p_run_id UUID,
    p_status VARCHAR DEFAULT 'SUCCESS',
    p_records_fetched INT DEFAULT 0,
    p_records_inserted INT DEFAULT 0,
    p_records_updated INT DEFAULT 0,
    p_records_failed INT DEFAULT 0,
    p_error_message TEXT DEFAULT NULL
) RETURNS VOID AS $$
BEGIN
    UPDATE audit.ingest_run
    SET
        status = p_status,
        completed_at = NOW(),
        records_fetched = p_records_fetched,
        records_inserted = p_records_inserted,
        records_updated = p_records_updated,
        records_failed = p_records_failed,
        error_message = p_error_message
    WHERE id = p_run_id;
END;
$$ LANGUAGE plpgsql;

-- =============================================================================
-- PARTITIONING GUIDANCE
-- =============================================================================

-- For very large observation tables (millions of rows), consider:
--
-- 1. Range partitioning by observation_date:
--
--    CREATE TABLE silver.observation_partitioned (
--        LIKE silver.observation INCLUDING ALL
--    ) PARTITION BY RANGE (observation_date);
--
--    CREATE TABLE silver.observation_y2024
--        PARTITION OF silver.observation_partitioned
--        FOR VALUES FROM ('2024-01-01') TO ('2025-01-01');
--
-- 2. Benefits:
--    - Faster queries when filtering by date range
--    - Easier archival of old data
--    - Parallel query execution
--
-- 3. When to implement:
--    - When observation table exceeds 10M rows
--    - When date-range queries become slow
--    - When storage management becomes important
--
-- For now, indexes are sufficient for expected data volumes.

-- =============================================================================
-- END OF SILVER LAYER SCRIPT
-- =============================================================================
