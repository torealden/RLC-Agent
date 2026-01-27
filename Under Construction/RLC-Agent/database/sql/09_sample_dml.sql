-- ============================================================================
-- Round Lakes Commodities - Sample DML Patterns
-- ============================================================================
-- File: 09_sample_dml.sql
-- Purpose: Working examples of ingestion, querying, and validation patterns
-- Execute: After all DDL scripts, for reference and testing
-- ============================================================================

-- ============================================================================
-- EXAMPLE 1: Complete WASDE Ingestion Workflow
-- ============================================================================
-- This shows the full lifecycle of ingesting a WASDE release

-- Step 1: Open an ingest run
DO $$
DECLARE
    v_run_id BIGINT;
    v_release_id INTEGER;
    v_series_id INTEGER;
    v_cell_id BIGINT;
    v_obs_id BIGINT;
BEGIN
    -- Open the ingest run
    v_run_id := audit.open_ingest_run(
        p_data_source_code := 'wasde',
        p_job_name := 'wasde_monthly_ingest',
        p_agent_id := 'collector_agent_01',
        p_agent_type := 'collector',
        p_parameters := '{"report_month": "2024-01"}'::JSONB
    );

    RAISE NOTICE 'Opened ingest run: %', v_run_id;

    -- Step 2: Get or create the WASDE release
    v_release_id := bronze.get_or_create_wasde_release(
        p_report_date := '2024-01-01'::DATE,
        p_ingest_run_id := v_run_id,
        p_release_number := 647,
        p_source_url := 'https://usda.gov/wasde/january-2024'
    );

    RAISE NOTICE 'Created/found release: %', v_release_id;

    -- Step 3: Ingest cells (example for US Corn ending stocks)
    v_cell_id := bronze.upsert_wasde_cell(
        p_release_id := v_release_id,
        p_table_id := '04',
        p_row_id := 'ending_stocks',
        p_column_id := '2023/24',
        p_value_text := '2,131',
        p_row_label := 'Ending Stocks',
        p_row_category := 'ending_stocks',
        p_marketing_year := '2023/24',
        p_ingest_run_id := v_run_id
    );

    -- Update counts
    PERFORM audit.update_ingest_counts(v_run_id, p_fetched := 1, p_inserted := 1);

    RAISE NOTICE 'Upserted cell: %', v_cell_id;

    -- Step 4: Get or create the series definition
    v_series_id := core.get_or_create_series(
        p_data_source_code := 'wasde',
        p_series_key := 'supply_demand.corn.ending_stocks.us.monthly',
        p_name := 'US Corn Ending Stocks (WASDE)',
        p_commodity_code := 'corn',
        p_location_code := 'US',
        p_unit_code := 'mil_bu',
        p_frequency := 'monthly',
        p_metadata := '{"wasde_table": "04", "wasde_row": "ending_stocks"}'::JSONB
    );

    RAISE NOTICE 'Created/found series: %', v_series_id;

    -- Step 5: Create the silver observation
    v_obs_id := silver.upsert_observation(
        p_series_id := v_series_id,
        p_observation_time := '2023-09-01'::TIMESTAMPTZ,  -- Start of 2023/24 MY
        p_value := 2131,
        p_ingest_run_id := v_run_id,
        p_revision := 0,
        p_quality_flag := 'good',
        p_is_forecast := TRUE,
        p_wasde_cell_id := v_cell_id
    );

    RAISE NOTICE 'Created observation: %', v_obs_id;

    -- Step 6: Close the ingest run
    PERFORM audit.close_ingest_run(
        p_run_id := v_run_id,
        p_status := 'success'
    );

    RAISE NOTICE 'Closed ingest run successfully';

    -- Rollback for demo (comment out to persist)
    -- ROLLBACK;
END $$;


-- ============================================================================
-- EXAMPLE 2: Direct Upsert Patterns (without helper functions)
-- ============================================================================

-- Idempotent upsert into bronze.wasde_cell
INSERT INTO bronze.wasde_cell (
    release_id, table_id, row_id, column_id,
    value_text, value_numeric, is_numeric,
    row_label, row_category, marketing_year
)
SELECT
    r.id, '04', 'production', '2024/25',
    '14,900', 14900, TRUE,
    'Production', 'production', '2024/25'
FROM bronze.wasde_release r
WHERE r.report_date = '2024-01-01'
ON CONFLICT (release_id, table_id, row_id, column_id)
DO UPDATE SET
    value_text = EXCLUDED.value_text,
    value_numeric = EXCLUDED.value_numeric,
    updated_at = NOW()
WHERE bronze.wasde_cell.value_text IS DISTINCT FROM EXCLUDED.value_text;


-- Idempotent upsert into silver.observation
INSERT INTO silver.observation (
    series_id, observation_time, value, revision,
    is_latest, quality_flag, is_forecast, ingest_run_id
)
SELECT
    s.id, '2024-09-01'::TIMESTAMPTZ, 14900, 0,
    TRUE, 'good', TRUE, NULL
FROM core.series s
JOIN core.data_source ds ON s.data_source_id = ds.id
WHERE ds.code = 'wasde'
  AND s.series_key = 'supply_demand.corn.production.us.monthly'
ON CONFLICT (series_id, observation_time, revision)
DO UPDATE SET
    value = EXCLUDED.value,
    is_forecast = EXCLUDED.is_forecast,
    updated_at = NOW()
WHERE silver.observation.value IS DISTINCT FROM EXCLUDED.value;


-- ============================================================================
-- EXAMPLE 3: Series Creation and Lookup
-- ============================================================================

-- Create a series with all attributes
INSERT INTO core.series (
    data_source_id, series_key, name, description,
    commodity_id, location_id, unit_id,
    frequency, is_revised, is_calculated, metadata
)
SELECT
    ds.id,
    'supply_demand.corn.yield.us.monthly',
    'US Corn Yield per Harvested Acre',
    'Average yield in bushels per acre from WASDE',
    c.id, l.id, u.id,
    'monthly', TRUE, FALSE,
    '{"wasde_table": "04", "wasde_row": "yield"}'::JSONB
FROM core.data_source ds
CROSS JOIN core.commodity c
CROSS JOIN core.location l
CROSS JOIN core.unit u
WHERE ds.code = 'wasde'
  AND c.code = 'corn'
  AND l.code = 'US'
  AND u.code = 'bu_per_acre'
ON CONFLICT (data_source_id, series_key) DO NOTHING;


-- Lookup series by composite key
SELECT
    s.id,
    s.series_key,
    s.name,
    ds.code AS source,
    c.code AS commodity,
    u.code AS unit
FROM core.series s
JOIN core.data_source ds ON s.data_source_id = ds.id
LEFT JOIN core.commodity c ON s.commodity_id = c.id
LEFT JOIN core.unit u ON s.unit_id = u.id
WHERE ds.code = 'wasde'
  AND s.series_key LIKE '%corn%'
ORDER BY s.series_key;


-- ============================================================================
-- EXAMPLE 4: Ingest Run Lifecycle
-- ============================================================================

-- Open a run
INSERT INTO audit.ingest_run (
    data_source_id, job_name, agent_id, agent_type, status, parameters
)
SELECT
    id, 'daily_price_ingest', 'price_collector_01', 'collector', 'running',
    '{"date": "2024-01-15"}'::JSONB
FROM core.data_source
WHERE code = 'ams'
RETURNING id;

-- Update counts during processing (assume run_id = 1)
UPDATE audit.ingest_run
SET
    records_fetched = records_fetched + 100,
    records_inserted = records_inserted + 95,
    records_skipped = records_skipped + 5,
    updated_at = NOW()
WHERE id = 1;

-- Close successfully
UPDATE audit.ingest_run
SET
    status = 'success',
    completed_at = NOW(),
    updated_at = NOW()
WHERE id = 1;

-- Close with error
UPDATE audit.ingest_run
SET
    status = 'failed',
    completed_at = NOW(),
    error_message = 'API returned 503 Service Unavailable',
    error_details = '{"http_code": 503, "retry_count": 3}'::JSONB,
    updated_at = NOW()
WHERE id = 1;


-- ============================================================================
-- EXAMPLE 5: Validation Status Updates
-- ============================================================================

-- Mark a release as validated
INSERT INTO audit.validation_status (
    entity_type, entity_id, data_source_id, status,
    checker_agent_id, checks_passed, checks_failed,
    check_results, validated_at
)
SELECT
    'release', '2024-01', ds.id, 'passed',
    'checker_agent_01', 15, 0,
    '[
        {"check": "row_count", "passed": true, "expected": 150, "actual": 150},
        {"check": "ending_stocks_range", "passed": true, "min": 0, "max": 3000}
    ]'::JSONB,
    NOW()
FROM core.data_source ds
WHERE ds.code = 'wasde'
ON CONFLICT (entity_type, entity_id, data_source_id)
DO UPDATE SET
    status = EXCLUDED.status,
    checks_passed = EXCLUDED.checks_passed,
    checks_failed = EXCLUDED.checks_failed,
    check_results = EXCLUDED.check_results,
    validated_at = EXCLUDED.validated_at,
    updated_at = NOW();


-- ============================================================================
-- EXAMPLE 6: Common Queries
-- ============================================================================

-- Get latest value for a series
SELECT value, observation_time, quality_flag
FROM silver.observation
WHERE series_id = (SELECT id FROM core.series WHERE series_key = 'supply_demand.corn.ending_stocks.us.monthly')
  AND is_latest = TRUE
ORDER BY observation_time DESC
LIMIT 1;


-- Get full revision history for a data point
SELECT
    revision,
    value,
    created_at AS published_at,
    ingest_run_id
FROM silver.observation
WHERE series_id = 123
  AND observation_time = '2024-09-01'
ORDER BY revision DESC;


-- Compare current vs previous WASDE for all corn metrics
WITH releases AS (
    SELECT
        id,
        report_date,
        LAG(id) OVER (ORDER BY report_date) AS prev_id
    FROM bronze.wasde_release
    WHERE report_date >= '2023-01-01'
)
SELECT
    r.report_date,
    c.row_label,
    c.marketing_year,
    c.value_numeric AS current,
    prev.value_numeric AS previous,
    c.value_numeric - prev.value_numeric AS change
FROM bronze.wasde_cell c
JOIN releases r ON c.release_id = r.id
LEFT JOIN bronze.wasde_cell prev ON
    prev.release_id = r.prev_id
    AND prev.table_id = c.table_id
    AND prev.row_id = c.row_id
    AND prev.column_id = c.column_id
WHERE c.table_id = '04'
  AND c.value_numeric IS NOT NULL
  AND r.report_date = (SELECT MAX(report_date) FROM bronze.wasde_release)
ORDER BY c.row_order, c.marketing_year;


-- Get series statistics
SELECT
    s.series_key,
    COUNT(*) AS observations,
    MIN(o.observation_time) AS first_obs,
    MAX(o.observation_time) AS last_obs,
    AVG(o.value) AS avg_value,
    STDDEV(o.value) AS stddev_value
FROM silver.observation o
JOIN core.series s ON o.series_id = s.id
WHERE o.is_latest = TRUE
GROUP BY s.id, s.series_key
ORDER BY s.series_key;


-- ============================================================================
-- EXAMPLE 7: Reconciliation Queries
-- ============================================================================

-- Compare database totals with expected Excel totals
WITH db_totals AS (
    SELECT
        c.table_id,
        c.marketing_year,
        c.row_category,
        SUM(c.value_numeric) AS db_total,
        COUNT(*) AS cell_count
    FROM bronze.wasde_cell c
    JOIN bronze.wasde_release r ON c.release_id = r.id
    WHERE r.report_date = '2024-01-01'
      AND c.table_id = '04'
    GROUP BY c.table_id, c.marketing_year, c.row_category
)
SELECT
    table_id,
    marketing_year,
    row_category,
    db_total,
    cell_count,
    -- Expected values would come from a reconciliation table
    NULL AS excel_total,
    NULL AS difference
FROM db_totals
ORDER BY table_id, marketing_year, row_category;


-- Find cells that failed to parse
SELECT
    r.report_date,
    c.table_id,
    c.row_label,
    c.column_id,
    c.value_text,
    c.parse_warning
FROM bronze.wasde_cell c
JOIN bronze.wasde_release r ON c.release_id = r.id
WHERE c.is_numeric = FALSE
  AND c.value_text NOT IN ('NA', '-', '--', '')
ORDER BY r.report_date DESC, c.table_id, c.row_order;


-- ============================================================================
-- EXAMPLE 8: Agent Health Monitoring
-- ============================================================================

-- Register agent heartbeat
SELECT audit.agent_heartbeat(
    'collector_wasde_01',
    'collector',
    'alive',
    'Processing January 2024 WASDE'
);

-- Find stale agents (no heartbeat in 5 minutes)
SELECT
    agent_id,
    agent_type,
    current_task,
    last_heartbeat,
    EXTRACT(EPOCH FROM NOW() - last_heartbeat) / 60 AS minutes_since_heartbeat
FROM audit.agent_heartbeat
WHERE last_heartbeat < NOW() - INTERVAL '5 minutes'
ORDER BY last_heartbeat;


-- ============================================================================
-- EXAMPLE 9: Data Freshness Check
-- ============================================================================

-- Check if we have the latest WASDE
SELECT
    report_date,
    release_number,
    is_complete,
    is_validated,
    created_at AS ingested_at,
    EXTRACT(DAY FROM NOW() - created_at) AS days_since_ingest
FROM bronze.wasde_release
ORDER BY report_date DESC
LIMIT 5;


-- Check for missing months
WITH expected_months AS (
    SELECT generate_series(
        '2020-01-01'::DATE,
        DATE_TRUNC('month', CURRENT_DATE),
        '1 month'
    )::DATE AS expected_date
)
SELECT expected_date
FROM expected_months
WHERE expected_date NOT IN (
    SELECT report_date FROM bronze.wasde_release
)
ORDER BY expected_date DESC;


-- ============================================================================
-- EXAMPLE 10: Bulk Insert Pattern (for CSV/API data)
-- ============================================================================

-- Example: Bulk insert from a VALUES list
WITH source_data AS (
    SELECT * FROM (VALUES
        ('2024-01-01'::TIMESTAMPTZ, 185.6, 'good'),
        ('2024-01-02'::TIMESTAMPTZ, 186.2, 'good'),
        ('2024-01-03'::TIMESTAMPTZ, 184.9, 'good'),
        ('2024-01-04'::TIMESTAMPTZ, NULL, 'missing'),
        ('2024-01-05'::TIMESTAMPTZ, 185.0, 'good')
    ) AS t(obs_time, value, quality)
)
INSERT INTO silver.observation (
    series_id, observation_time, value, revision,
    is_latest, quality_flag, ingest_run_id
)
SELECT
    123,  -- series_id
    obs_time,
    value,
    0,
    TRUE,
    quality,
    456   -- ingest_run_id
FROM source_data
ON CONFLICT (series_id, observation_time, revision)
DO UPDATE SET
    value = EXCLUDED.value,
    quality_flag = EXCLUDED.quality_flag,
    updated_at = NOW()
WHERE silver.observation.value IS DISTINCT FROM EXCLUDED.value;


-- ============================================================================
-- Verification
-- ============================================================================
DO $$
BEGIN
    RAISE NOTICE 'Sample DML patterns available for reference:';
    RAISE NOTICE '  1. Complete WASDE ingestion workflow';
    RAISE NOTICE '  2. Direct upsert patterns';
    RAISE NOTICE '  3. Series creation and lookup';
    RAISE NOTICE '  4. Ingest run lifecycle';
    RAISE NOTICE '  5. Validation status updates';
    RAISE NOTICE '  6. Common queries';
    RAISE NOTICE '  7. Reconciliation queries';
    RAISE NOTICE '  8. Agent health monitoring';
    RAISE NOTICE '  9. Data freshness checks';
    RAISE NOTICE '  10. Bulk insert patterns';
END $$;
