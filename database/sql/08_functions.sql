-- ============================================================================
-- Round Lakes Commodities - Helper Functions
-- ============================================================================
-- File: 08_functions.sql
-- Purpose: Utility functions for series lookup, ingest management, etc.
-- Execute: After 07_roles_grants.sql
-- ============================================================================

-- ============================================================================
-- SERIES GOVERNANCE: Get or create series (atomic)
-- ============================================================================
CREATE OR REPLACE FUNCTION core.get_or_create_series(
    p_data_source_code VARCHAR(50),
    p_series_key VARCHAR(255),
    p_name VARCHAR(500),
    p_commodity_code VARCHAR(30) DEFAULT NULL,
    p_location_code VARCHAR(50) DEFAULT NULL,
    p_unit_code VARCHAR(30) DEFAULT NULL,
    p_frequency VARCHAR(20) DEFAULT 'monthly',
    p_metadata JSONB DEFAULT '{}'
)
RETURNS INTEGER AS $$
DECLARE
    v_series_id INTEGER;
    v_data_source_id INTEGER;
    v_commodity_id INTEGER;
    v_location_id INTEGER;
    v_unit_id INTEGER;
BEGIN
    -- Look up data source
    SELECT id INTO v_data_source_id FROM core.data_source WHERE code = p_data_source_code;
    IF v_data_source_id IS NULL THEN
        RAISE EXCEPTION 'Data source not found: %', p_data_source_code;
    END IF;

    -- Look up optional dimensions
    IF p_commodity_code IS NOT NULL THEN
        SELECT id INTO v_commodity_id FROM core.commodity WHERE code = p_commodity_code;
    END IF;

    IF p_location_code IS NOT NULL THEN
        SELECT id INTO v_location_id FROM core.location WHERE code = p_location_code;
    END IF;

    IF p_unit_code IS NOT NULL THEN
        SELECT id INTO v_unit_id FROM core.unit WHERE code = p_unit_code;
    END IF;

    -- Try to find existing series
    SELECT id INTO v_series_id
    FROM core.series
    WHERE data_source_id = v_data_source_id AND series_key = p_series_key;

    -- Create if not exists
    IF v_series_id IS NULL THEN
        INSERT INTO core.series (
            data_source_id, series_key, name,
            commodity_id, location_id, unit_id,
            frequency, metadata
        ) VALUES (
            v_data_source_id, p_series_key, p_name,
            v_commodity_id, v_location_id, v_unit_id,
            p_frequency, p_metadata
        )
        RETURNING id INTO v_series_id;
    END IF;

    RETURN v_series_id;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION core.get_or_create_series IS
    'Atomically get or create a series. Returns series_id. Prevents duplicate series.';


-- ============================================================================
-- SERIES LOOKUP: Get series ID by key
-- ============================================================================
CREATE OR REPLACE FUNCTION core.get_series_id(
    p_data_source_code VARCHAR(50),
    p_series_key VARCHAR(255)
)
RETURNS INTEGER AS $$
    SELECT s.id
    FROM core.series s
    JOIN core.data_source ds ON s.data_source_id = ds.id
    WHERE ds.code = p_data_source_code AND s.series_key = p_series_key;
$$ LANGUAGE SQL STABLE;

COMMENT ON FUNCTION core.get_series_id IS
    'Look up series ID by data source code and series key. Returns NULL if not found.';


-- ============================================================================
-- INGEST RUN LIFECYCLE: Open a new ingest run
-- ============================================================================
CREATE OR REPLACE FUNCTION audit.open_ingest_run(
    p_data_source_code VARCHAR(50),
    p_job_name VARCHAR(255),
    p_agent_id VARCHAR(100) DEFAULT NULL,
    p_agent_type VARCHAR(100) DEFAULT 'collector',
    p_parameters JSONB DEFAULT '{}'
)
RETURNS BIGINT AS $$
DECLARE
    v_run_id BIGINT;
    v_data_source_id INTEGER;
BEGIN
    -- Look up data source
    SELECT id INTO v_data_source_id FROM core.data_source WHERE code = p_data_source_code;
    IF v_data_source_id IS NULL THEN
        RAISE EXCEPTION 'Data source not found: %', p_data_source_code;
    END IF;

    -- Create the ingest run
    INSERT INTO audit.ingest_run (
        data_source_id, job_name, agent_id, agent_type,
        status, parameters, started_at
    ) VALUES (
        v_data_source_id, p_job_name, p_agent_id, p_agent_type,
        'running', p_parameters, NOW()
    )
    RETURNING id INTO v_run_id;

    RETURN v_run_id;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION audit.open_ingest_run IS
    'Open a new ingest run. Returns the ingest_run_id. Call at start of ingestion.';


-- ============================================================================
-- INGEST RUN LIFECYCLE: Update counts during ingestion
-- ============================================================================
CREATE OR REPLACE FUNCTION audit.update_ingest_counts(
    p_run_id BIGINT,
    p_fetched INTEGER DEFAULT 0,
    p_inserted INTEGER DEFAULT 0,
    p_updated INTEGER DEFAULT 0,
    p_skipped INTEGER DEFAULT 0,
    p_failed INTEGER DEFAULT 0
)
RETURNS VOID AS $$
BEGIN
    UPDATE audit.ingest_run
    SET
        records_fetched = records_fetched + p_fetched,
        records_inserted = records_inserted + p_inserted,
        records_updated = records_updated + p_updated,
        records_skipped = records_skipped + p_skipped,
        records_failed = records_failed + p_failed,
        updated_at = NOW()
    WHERE id = p_run_id;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION audit.update_ingest_counts IS
    'Increment record counts during ingestion. Call periodically during processing.';


-- ============================================================================
-- INGEST RUN LIFECYCLE: Close ingest run
-- ============================================================================
CREATE OR REPLACE FUNCTION audit.close_ingest_run(
    p_run_id BIGINT,
    p_status VARCHAR(20) DEFAULT 'success',
    p_error_message TEXT DEFAULT NULL,
    p_error_details JSONB DEFAULT NULL
)
RETURNS VOID AS $$
BEGIN
    UPDATE audit.ingest_run
    SET
        status = p_status,
        completed_at = NOW(),
        error_message = p_error_message,
        error_details = p_error_details,
        updated_at = NOW()
    WHERE id = p_run_id;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION audit.close_ingest_run IS
    'Close an ingest run. Call at end of ingestion with final status.';


-- ============================================================================
-- INGEST ERROR LOGGING
-- ============================================================================
CREATE OR REPLACE FUNCTION audit.log_ingest_error(
    p_run_id BIGINT,
    p_error_type VARCHAR(100),
    p_error_message TEXT,
    p_record_key VARCHAR(500) DEFAULT NULL,
    p_record_data JSONB DEFAULT NULL,
    p_error_code VARCHAR(50) DEFAULT NULL
)
RETURNS BIGINT AS $$
DECLARE
    v_error_id BIGINT;
BEGIN
    INSERT INTO audit.ingest_error (
        ingest_run_id, error_type, error_message,
        record_key, record_data, error_code
    ) VALUES (
        p_run_id, p_error_type, p_error_message,
        p_record_key, p_record_data, p_error_code
    )
    RETURNING id INTO v_error_id;

    -- Increment failed count
    PERFORM audit.update_ingest_counts(p_run_id, p_failed := 1);

    RETURN v_error_id;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION audit.log_ingest_error IS
    'Log an ingestion error. Automatically increments failed count.';


-- ============================================================================
-- WASDE HELPER: Get or create release
-- ============================================================================
CREATE OR REPLACE FUNCTION bronze.get_or_create_wasde_release(
    p_report_date DATE,
    p_ingest_run_id BIGINT,
    p_release_number INTEGER DEFAULT NULL,
    p_source_url VARCHAR(2000) DEFAULT NULL
)
RETURNS INTEGER AS $$
DECLARE
    v_release_id INTEGER;
BEGIN
    -- Try to find existing release
    SELECT id INTO v_release_id
    FROM bronze.wasde_release
    WHERE report_date = p_report_date;

    -- Create if not exists
    IF v_release_id IS NULL THEN
        INSERT INTO bronze.wasde_release (
            report_date, release_number, source_url, ingest_run_id
        ) VALUES (
            p_report_date, p_release_number, p_source_url, p_ingest_run_id
        )
        RETURNING id INTO v_release_id;
    ELSE
        -- Update with latest ingest run
        UPDATE bronze.wasde_release
        SET
            ingest_run_id = p_ingest_run_id,
            release_number = COALESCE(p_release_number, release_number),
            source_url = COALESCE(p_source_url, source_url),
            updated_at = NOW()
        WHERE id = v_release_id;
    END IF;

    RETURN v_release_id;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION bronze.get_or_create_wasde_release IS
    'Get or create a WASDE release record. Returns release_id.';


-- ============================================================================
-- WASDE HELPER: Upsert cell (idempotent)
-- ============================================================================
CREATE OR REPLACE FUNCTION bronze.upsert_wasde_cell(
    p_release_id INTEGER,
    p_table_id VARCHAR(20),
    p_row_id VARCHAR(100),
    p_column_id VARCHAR(100),
    p_value_text TEXT,
    p_row_label VARCHAR(500) DEFAULT NULL,
    p_row_category VARCHAR(100) DEFAULT NULL,
    p_marketing_year VARCHAR(20) DEFAULT NULL,
    p_ingest_run_id BIGINT DEFAULT NULL
)
RETURNS BIGINT AS $$
DECLARE
    v_cell_id BIGINT;
    v_numeric DECIMAL(20,4);
    v_is_numeric BOOLEAN;
BEGIN
    -- Try to parse numeric value
    BEGIN
        -- Remove commas and parse
        v_numeric := REPLACE(p_value_text, ',', '')::DECIMAL(20,4);
        v_is_numeric := TRUE;
    EXCEPTION WHEN OTHERS THEN
        v_numeric := NULL;
        v_is_numeric := FALSE;
    END;

    -- Upsert the cell
    INSERT INTO bronze.wasde_cell (
        release_id, table_id, row_id, column_id,
        value_text, value_numeric, is_numeric,
        row_label, row_category, marketing_year,
        ingest_run_id
    ) VALUES (
        p_release_id, p_table_id, p_row_id, p_column_id,
        p_value_text, v_numeric, v_is_numeric,
        p_row_label, p_row_category, p_marketing_year,
        p_ingest_run_id
    )
    ON CONFLICT (release_id, table_id, row_id, column_id)
    DO UPDATE SET
        value_text = EXCLUDED.value_text,
        value_numeric = EXCLUDED.value_numeric,
        is_numeric = EXCLUDED.is_numeric,
        row_label = COALESCE(EXCLUDED.row_label, bronze.wasde_cell.row_label),
        row_category = COALESCE(EXCLUDED.row_category, bronze.wasde_cell.row_category),
        marketing_year = COALESCE(EXCLUDED.marketing_year, bronze.wasde_cell.marketing_year),
        ingest_run_id = EXCLUDED.ingest_run_id,
        updated_at = NOW()
    WHERE bronze.wasde_cell.value_text IS DISTINCT FROM EXCLUDED.value_text
    RETURNING id INTO v_cell_id;

    -- If no rows affected (no change), get existing id
    IF v_cell_id IS NULL THEN
        SELECT id INTO v_cell_id
        FROM bronze.wasde_cell
        WHERE release_id = p_release_id
          AND table_id = p_table_id
          AND row_id = p_row_id
          AND column_id = p_column_id;
    END IF;

    RETURN v_cell_id;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION bronze.upsert_wasde_cell IS
    'Idempotent upsert for WASDE cells. Parses numeric values automatically.';


-- ============================================================================
-- SILVER HELPER: Upsert observation (idempotent)
-- ============================================================================
CREATE OR REPLACE FUNCTION silver.upsert_observation(
    p_series_id INTEGER,
    p_observation_time TIMESTAMPTZ,
    p_value DECIMAL(20,6),
    p_ingest_run_id BIGINT,
    p_revision INTEGER DEFAULT 0,
    p_quality_flag VARCHAR(20) DEFAULT 'good',
    p_is_estimated BOOLEAN DEFAULT FALSE,
    p_is_forecast BOOLEAN DEFAULT FALSE,
    p_wasde_cell_id BIGINT DEFAULT NULL,
    p_metadata JSONB DEFAULT '{}'
)
RETURNS BIGINT AS $$
DECLARE
    v_obs_id BIGINT;
BEGIN
    INSERT INTO silver.observation (
        series_id, observation_time, value,
        revision, quality_flag, is_estimated, is_forecast,
        wasde_cell_id, ingest_run_id, metadata
    ) VALUES (
        p_series_id, p_observation_time, p_value,
        p_revision, p_quality_flag, p_is_estimated, p_is_forecast,
        p_wasde_cell_id, p_ingest_run_id, p_metadata
    )
    ON CONFLICT (series_id, observation_time, revision)
    DO UPDATE SET
        value = EXCLUDED.value,
        quality_flag = EXCLUDED.quality_flag,
        is_estimated = EXCLUDED.is_estimated,
        is_forecast = EXCLUDED.is_forecast,
        wasde_cell_id = EXCLUDED.wasde_cell_id,
        ingest_run_id = EXCLUDED.ingest_run_id,
        metadata = EXCLUDED.metadata,
        updated_at = NOW()
    WHERE silver.observation.value IS DISTINCT FROM EXCLUDED.value
       OR silver.observation.quality_flag IS DISTINCT FROM EXCLUDED.quality_flag
    RETURNING id INTO v_obs_id;

    -- If no rows affected (no change), get existing id
    IF v_obs_id IS NULL THEN
        SELECT id INTO v_obs_id
        FROM silver.observation
        WHERE series_id = p_series_id
          AND observation_time = p_observation_time
          AND revision = p_revision;
    END IF;

    RETURN v_obs_id;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION silver.upsert_observation IS
    'Idempotent upsert for observations. Updates only if value or flags changed.';


-- ============================================================================
-- VALIDATION HELPER: Create or update validation status
-- ============================================================================
CREATE OR REPLACE FUNCTION audit.set_validation_status(
    p_entity_type VARCHAR(50),
    p_entity_id VARCHAR(100),
    p_data_source_code VARCHAR(50),
    p_status VARCHAR(30),
    p_checker_agent_id VARCHAR(100) DEFAULT NULL,
    p_check_results JSONB DEFAULT '[]',
    p_discrepancies JSONB DEFAULT '[]',
    p_notes TEXT DEFAULT NULL
)
RETURNS BIGINT AS $$
DECLARE
    v_validation_id BIGINT;
    v_data_source_id INTEGER;
    v_passed INTEGER;
    v_failed INTEGER;
BEGIN
    -- Look up data source
    SELECT id INTO v_data_source_id FROM core.data_source WHERE code = p_data_source_code;

    -- Count passed/failed from check_results
    SELECT
        COUNT(*) FILTER (WHERE (item->>'passed')::BOOLEAN = TRUE),
        COUNT(*) FILTER (WHERE (item->>'passed')::BOOLEAN = FALSE)
    INTO v_passed, v_failed
    FROM jsonb_array_elements(p_check_results) AS item;

    -- Upsert validation status
    INSERT INTO audit.validation_status (
        entity_type, entity_id, data_source_id,
        status, checker_agent_id,
        checks_passed, checks_failed, check_results,
        discrepancies, discrepancy_notes,
        validated_at
    ) VALUES (
        p_entity_type, p_entity_id, v_data_source_id,
        p_status, p_checker_agent_id,
        v_passed, v_failed, p_check_results,
        p_discrepancies, p_notes,
        CASE WHEN p_status IN ('passed', 'failed', 'passed_with_warnings') THEN NOW() ELSE NULL END
    )
    ON CONFLICT (entity_type, entity_id, data_source_id)
    DO UPDATE SET
        status = EXCLUDED.status,
        checker_agent_id = EXCLUDED.checker_agent_id,
        checks_passed = EXCLUDED.checks_passed,
        checks_failed = EXCLUDED.checks_failed,
        check_results = EXCLUDED.check_results,
        discrepancies = EXCLUDED.discrepancies,
        discrepancy_notes = EXCLUDED.discrepancy_notes,
        validated_at = EXCLUDED.validated_at,
        updated_at = NOW()
    RETURNING id INTO v_validation_id;

    RETURN v_validation_id;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION audit.set_validation_status IS
    'Set validation status for a release/entity. Automatically counts passed/failed checks.';


-- ============================================================================
-- UNIT CONVERSION HELPER
-- ============================================================================
CREATE OR REPLACE FUNCTION core.convert_units(
    p_value DECIMAL(20,6),
    p_from_unit_code VARCHAR(30),
    p_to_unit_code VARCHAR(30)
)
RETURNS DECIMAL(20,6) AS $$
DECLARE
    v_from_factor DECIMAL(20,10);
    v_to_factor DECIMAL(20,10);
    v_from_base INTEGER;
    v_to_base INTEGER;
BEGIN
    -- Get from unit's conversion
    SELECT conversion_factor, base_unit_id
    INTO v_from_factor, v_from_base
    FROM core.unit WHERE code = p_from_unit_code;

    -- Get to unit's conversion
    SELECT conversion_factor, base_unit_id
    INTO v_to_factor, v_to_base
    FROM core.unit WHERE code = p_to_unit_code;

    -- Check if convertible (same base unit)
    IF v_from_base IS NULL OR v_to_base IS NULL THEN
        RAISE EXCEPTION 'Units % and % do not have base unit conversion defined',
            p_from_unit_code, p_to_unit_code;
    END IF;

    IF v_from_base != v_to_base THEN
        RAISE EXCEPTION 'Units % and % have different base units and cannot be converted',
            p_from_unit_code, p_to_unit_code;
    END IF;

    -- Convert: value * from_factor / to_factor
    RETURN p_value * v_from_factor / v_to_factor;
END;
$$ LANGUAGE plpgsql STABLE;

COMMENT ON FUNCTION core.convert_units IS
    'Convert a value between units. Both units must have same base unit.';


-- ============================================================================
-- AGENT HEARTBEAT
-- ============================================================================
CREATE OR REPLACE FUNCTION audit.agent_heartbeat(
    p_agent_id VARCHAR(100),
    p_agent_type VARCHAR(100),
    p_status VARCHAR(20) DEFAULT 'alive',
    p_current_task VARCHAR(500) DEFAULT NULL
)
RETURNS VOID AS $$
BEGIN
    INSERT INTO audit.agent_heartbeat (
        agent_id, agent_type, status, current_task,
        hostname, pid, last_heartbeat
    ) VALUES (
        p_agent_id, p_agent_type, p_status, p_current_task,
        inet_server_addr()::TEXT, pg_backend_pid(), NOW()
    )
    ON CONFLICT (agent_id)
    DO UPDATE SET
        status = EXCLUDED.status,
        current_task = EXCLUDED.current_task,
        last_heartbeat = NOW();
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION audit.agent_heartbeat IS
    'Update agent heartbeat. Call periodically from agents to indicate they are alive.';


-- ============================================================================
-- MARKETING YEAR HELPER
-- ============================================================================
CREATE OR REPLACE FUNCTION core.get_marketing_year(
    p_date DATE,
    p_commodity VARCHAR(30) DEFAULT 'corn'
)
RETURNS VARCHAR(7) AS $$
DECLARE
    v_year INTEGER;
    v_month INTEGER;
    v_start_month INTEGER;
BEGIN
    v_year := EXTRACT(YEAR FROM p_date);
    v_month := EXTRACT(MONTH FROM p_date);

    -- Marketing year start months by commodity
    v_start_month := CASE p_commodity
        WHEN 'corn' THEN 9       -- Sep 1
        WHEN 'soybeans' THEN 9   -- Sep 1
        WHEN 'wheat' THEN 6      -- Jun 1
        WHEN 'cotton' THEN 8     -- Aug 1
        ELSE 9                   -- Default to Sep
    END;

    -- Determine marketing year
    IF v_month >= v_start_month THEN
        RETURN v_year::TEXT || '/' || (v_year - 1999)::TEXT;  -- e.g., "2024/25"
    ELSE
        RETURN (v_year - 1)::TEXT || '/' || (v_year - 2000)::TEXT;  -- e.g., "2023/24"
    END IF;
END;
$$ LANGUAGE plpgsql IMMUTABLE;

COMMENT ON FUNCTION core.get_marketing_year IS
    'Get marketing year string for a date and commodity. Returns format "2024/25".';


-- ============================================================================
-- Verification
-- ============================================================================
DO $$
BEGIN
    RAISE NOTICE 'Helper functions created:';
    RAISE NOTICE '  - core.get_or_create_series()';
    RAISE NOTICE '  - core.get_series_id()';
    RAISE NOTICE '  - core.convert_units()';
    RAISE NOTICE '  - core.get_marketing_year()';
    RAISE NOTICE '  - audit.open_ingest_run()';
    RAISE NOTICE '  - audit.update_ingest_counts()';
    RAISE NOTICE '  - audit.close_ingest_run()';
    RAISE NOTICE '  - audit.log_ingest_error()';
    RAISE NOTICE '  - audit.set_validation_status()';
    RAISE NOTICE '  - audit.agent_heartbeat()';
    RAISE NOTICE '  - bronze.get_or_create_wasde_release()';
    RAISE NOTICE '  - bronze.upsert_wasde_cell()';
    RAISE NOTICE '  - silver.upsert_observation()';
END $$;
