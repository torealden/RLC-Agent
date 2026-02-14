-- =============================================================================
-- RLC Commodities Database Schema - Transformation Logging
-- Version: 1.0.0
-- =============================================================================
--
-- PURPOSE
-- -------
-- This schema extends the audit namespace to track agent interactions with data:
-- - Session tracking (checkout/checkin pattern)
-- - Transformation operation logging
-- - Output artifact registration
-- - Data lineage relationships
--
-- DESIGN PRINCIPLES
-- -----------------
-- 1. Log metadata, not data values
-- 2. Bronze layer remains the source of truth
-- 3. Optimize for troubleshooting queries
-- 4. Include retention/archival from day one
--
-- =============================================================================

-- =============================================================================
-- TRANSFORMATION SESSION TABLE
-- "Checkout" - Records when an agent starts working with data
-- =============================================================================

CREATE TABLE IF NOT EXISTS audit.transformation_session (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),

    -- Session identification
    session_type VARCHAR(50) NOT NULL,     -- Session type (see CHECK constraint)

    -- Agent information
    agent_id VARCHAR(100) NOT NULL,        -- Which agent initiated the session
    agent_type VARCHAR(50),                -- 'COLLECTOR', 'TRANSFORMER', 'ANALYST', 'VISUALIZATION'
    agent_version VARCHAR(50),

    -- What data is being accessed
    source_layer VARCHAR(10) NOT NULL,     -- 'BRONZE', 'SILVER', 'GOLD'
    source_tables TEXT[] NOT NULL,         -- Array of table names accessed
    source_filters JSONB,                  -- Any WHERE clauses/filters applied

    -- Time range of SOURCE data (not transformation time)
    data_start_date DATE,
    data_end_date DATE,

    -- Session lifecycle
    status VARCHAR(20) NOT NULL DEFAULT 'ACTIVE',
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMPTZ,

    -- Context
    purpose TEXT,                          -- Human-readable description
    ticket_id VARCHAR(50),                 -- Optional: link to Jira/issue tracker
    parent_session_id UUID,                -- For chained transformations

    -- Summary (populated on completion)
    operations_count INT DEFAULT 0,
    outputs_count INT DEFAULT 0,
    total_rows_processed BIGINT DEFAULT 0,

    -- Error tracking
    error_message TEXT,
    error_details JSONB,

    created_at TIMESTAMPTZ DEFAULT NOW(),

    -- Constraints
    FOREIGN KEY (parent_session_id) REFERENCES audit.transformation_session(id),

    CONSTRAINT chk_session_type CHECK (session_type IN (
        'BRONZE_TO_SILVER',      -- Raw → Standardized transformation
        'SILVER_AGGREGATE',      -- Creating aggregated Silver tables
        'SILVER_TO_GOLD',        -- Creating Gold views/tables
        'GOLD_VISUALIZATION',    -- Creating charts/reports from Gold
        'CROSS_LAYER_ANALYSIS',  -- Reading from multiple layers
        'AD_HOC_QUERY',          -- One-off analysis
        'DATA_EXPORT',           -- Exporting data
        'DATA_CORRECTION'        -- Fixing data issues
    )),

    CONSTRAINT chk_source_layer CHECK (source_layer IN ('BRONZE', 'SILVER', 'GOLD')),

    CONSTRAINT chk_session_status CHECK (status IN (
        'ACTIVE',      -- Session in progress
        'COMPLETED',   -- Successfully finished
        'FAILED',      -- Ended with error
        'ABANDONED',   -- Started but never completed (timeout)
        'ROLLED_BACK'  -- Changes were reverted
    ))
);

-- Indexes for common query patterns
CREATE INDEX IF NOT EXISTS idx_trans_session_agent ON audit.transformation_session(agent_id);
CREATE INDEX IF NOT EXISTS idx_trans_session_type ON audit.transformation_session(session_type);
CREATE INDEX IF NOT EXISTS idx_trans_session_status ON audit.transformation_session(status);
CREATE INDEX IF NOT EXISTS idx_trans_session_started ON audit.transformation_session(started_at DESC);
CREATE INDEX IF NOT EXISTS idx_trans_session_source ON audit.transformation_session USING GIN(source_tables);
CREATE INDEX IF NOT EXISTS idx_trans_session_active ON audit.transformation_session(status)
    WHERE status = 'ACTIVE';

COMMENT ON TABLE audit.transformation_session IS
    'Tracks agent sessions that interact with data. The "checkout" pattern.';

-- =============================================================================
-- TRANSFORMATION OPERATION TABLE
-- Records individual operations within a session
-- =============================================================================

CREATE TABLE IF NOT EXISTS audit.transformation_operation (
    id BIGSERIAL PRIMARY KEY,
    session_id UUID NOT NULL REFERENCES audit.transformation_session(id) ON DELETE CASCADE,

    -- Operation details
    operation_type VARCHAR(50) NOT NULL,   -- Operation type (see CHECK constraint)
    operation_order INT NOT NULL,          -- Sequence within session (1, 2, 3...)

    -- What was the input
    input_tables TEXT[],                   -- Tables/views used as input
    input_columns TEXT[],                  -- Columns accessed (optional for simplicity)
    input_row_count BIGINT,                -- Approximate row count processed

    -- What was the transformation
    transformation_logic TEXT,             -- SQL, formula, or description of operation
    transformation_type VARCHAR(30),       -- 'SQL', 'PYTHON', 'FORMULA', 'PANDAS', 'MANUAL'

    -- Parameters used (not the data, just configuration)
    parameters JSONB,                      -- e.g., {"aggregation": "SUM", "group_by": ["commodity"]}

    -- What was the output
    output_table VARCHAR(200),             -- Target table/view name
    output_columns TEXT[],
    output_row_count BIGINT,

    -- Quality and performance
    warnings TEXT[],                       -- Any issues encountered during operation
    execution_time_ms INT,                 -- How long the operation took

    created_at TIMESTAMPTZ DEFAULT NOW(),

    -- Constraints
    CONSTRAINT chk_operation_type CHECK (operation_type IN (
        'SELECT',        -- Reading data
        'INSERT',        -- Adding new rows
        'UPDATE',        -- Modifying existing rows
        'DELETE',        -- Removing rows
        'UPSERT',        -- Insert or update
        'AGGREGATE',     -- SUM, AVG, COUNT, etc.
        'JOIN',          -- Combining tables
        'FILTER',        -- WHERE clause filtering
        'CALCULATE',     -- Derived column calculation
        'PIVOT',         -- Reshaping data
        'UNPIVOT',       -- Normalizing wide data
        'NORMALIZE',     -- Standardizing formats/units
        'CLEAN',         -- Data cleaning operations
        'DEDUPLICATE',   -- Removing duplicates
        'VALIDATE',      -- Quality checks
        'TRANSFORM',     -- Generic transformation
        'EXPORT',        -- Writing to external format
        'REFRESH'        -- Materialized view refresh
    )),

    CONSTRAINT chk_transformation_type CHECK (transformation_type IS NULL OR transformation_type IN (
        'SQL',
        'PYTHON',
        'PANDAS',
        'FORMULA',
        'SPARK',
        'MANUAL',
        'SYSTEM'
    ))
);

-- Indexes for querying operations
CREATE INDEX IF NOT EXISTS idx_trans_op_session ON audit.transformation_operation(session_id);
CREATE INDEX IF NOT EXISTS idx_trans_op_type ON audit.transformation_operation(operation_type);
CREATE INDEX IF NOT EXISTS idx_trans_op_output ON audit.transformation_operation(output_table);
CREATE INDEX IF NOT EXISTS idx_trans_op_created ON audit.transformation_operation(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_trans_op_input ON audit.transformation_operation USING GIN(input_tables);

COMMENT ON TABLE audit.transformation_operation IS
    'Records individual operations within a transformation session.';

-- =============================================================================
-- OUTPUT ARTIFACT TABLE
-- Records outputs created from transformations (tables, views, charts, exports)
-- =============================================================================

CREATE TABLE IF NOT EXISTS audit.output_artifact (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    session_id UUID NOT NULL REFERENCES audit.transformation_session(id) ON DELETE CASCADE,

    -- Artifact identification
    artifact_type VARCHAR(50) NOT NULL,    -- Artifact type (see CHECK constraint)
    artifact_name VARCHAR(300) NOT NULL,
    artifact_location VARCHAR(500),        -- Schema.table, file path, S3 URL, etc.

    -- What data does it contain
    source_tables TEXT[],                  -- Which tables feed this artifact
    source_columns TEXT[],                 -- Which columns are included
    row_count BIGINT,
    column_count INT,

    -- Temporal scope of the data
    data_as_of TIMESTAMPTZ,                -- Point-in-time snapshot timestamp
    data_start_date DATE,                  -- Earliest data in artifact
    data_end_date DATE,                    -- Latest data in artifact

    -- Lifecycle
    is_current BOOLEAN DEFAULT TRUE,       -- Is this the current version?
    superseded_by_id UUID,                 -- ID of artifact that replaced this
    created_at TIMESTAMPTZ DEFAULT NOW(),
    expires_at TIMESTAMPTZ,                -- Optional TTL for temporary artifacts

    -- Metadata
    description TEXT,
    metadata JSONB,                        -- Flexible additional attributes

    -- Constraints
    FOREIGN KEY (superseded_by_id) REFERENCES audit.output_artifact(id),

    CONSTRAINT chk_artifact_type CHECK (artifact_type IN (
        'TABLE',               -- Database table
        'VIEW',                -- Database view
        'MATERIALIZED_VIEW',   -- Materialized view
        'TEMP_TABLE',          -- Temporary table
        'DATAFRAME',           -- In-memory pandas/spark dataframe
        'CSV_FILE',            -- CSV export
        'EXCEL_FILE',          -- Excel export
        'JSON_FILE',           -- JSON export
        'PARQUET_FILE',        -- Parquet export
        'CHART',               -- Visualization
        'DASHBOARD',           -- Dashboard widget
        'REPORT',              -- Generated report
        'API_RESPONSE',        -- Data served via API
        'CACHED_QUERY'         -- Cached query result
    ))
);

-- Indexes for artifact queries
CREATE INDEX IF NOT EXISTS idx_artifact_session ON audit.output_artifact(session_id);
CREATE INDEX IF NOT EXISTS idx_artifact_type ON audit.output_artifact(artifact_type);
CREATE INDEX IF NOT EXISTS idx_artifact_name ON audit.output_artifact(artifact_name);
CREATE INDEX IF NOT EXISTS idx_artifact_current ON audit.output_artifact(is_current) WHERE is_current = TRUE;
CREATE INDEX IF NOT EXISTS idx_artifact_created ON audit.output_artifact(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_artifact_sources ON audit.output_artifact USING GIN(source_tables);

COMMENT ON TABLE audit.output_artifact IS
    'Registry of outputs created from transformation sessions.';

-- =============================================================================
-- LINEAGE EDGE TABLE
-- Records relationships between data entities for lineage tracking
-- =============================================================================

CREATE TABLE IF NOT EXISTS audit.lineage_edge (
    id BIGSERIAL PRIMARY KEY,

    -- Source node (where data comes from)
    source_type VARCHAR(30) NOT NULL,      -- 'TABLE', 'VIEW', 'COLUMN', 'FILE', 'API'
    source_schema VARCHAR(50),             -- Database schema (bronze, silver, gold)
    source_name VARCHAR(200) NOT NULL,     -- Table/view/file name
    source_column VARCHAR(100),            -- Optional: for column-level lineage

    -- Target node (where data goes to)
    target_type VARCHAR(30) NOT NULL,
    target_schema VARCHAR(50),
    target_name VARCHAR(200) NOT NULL,
    target_column VARCHAR(100),

    -- Relationship type
    relationship_type VARCHAR(30) NOT NULL,  -- Relationship type (see CHECK constraint)

    -- Context
    session_id UUID REFERENCES audit.transformation_session(id) ON DELETE SET NULL,
    transformation_description TEXT,       -- Human-readable description of the transformation

    -- Validity (for temporal lineage)
    is_current BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    invalidated_at TIMESTAMPTZ,

    -- Constraints
    CONSTRAINT chk_source_type CHECK (source_type IN (
        'TABLE', 'VIEW', 'MATERIALIZED_VIEW', 'COLUMN', 'FILE', 'API', 'STREAM'
    )),

    CONSTRAINT chk_target_type CHECK (target_type IN (
        'TABLE', 'VIEW', 'MATERIALIZED_VIEW', 'COLUMN', 'FILE', 'API', 'STREAM', 'ARTIFACT'
    )),

    CONSTRAINT chk_relationship_type CHECK (relationship_type IN (
        'DERIVES_FROM',    -- Target is derived from source
        'COPIES',          -- Direct copy with no transformation
        'AGGREGATES',      -- Target aggregates source data
        'JOINS',           -- Target joins source with other data
        'FILTERS',         -- Target is filtered subset of source
        'TRANSFORMS',      -- Generic transformation
        'ENRICHES',        -- Target enriches source with additional data
        'VALIDATES',       -- Target validates source data
        'REFERENCES'       -- Target references source (e.g., FK relationship)
    )),

    -- Unique constraint to prevent duplicate edges
    CONSTRAINT uq_lineage_edge UNIQUE NULLS NOT DISTINCT (
        source_type, source_schema, source_name, source_column,
        target_type, target_schema, target_name, target_column,
        relationship_type
    )
);

-- Indexes for lineage queries
CREATE INDEX IF NOT EXISTS idx_lineage_source ON audit.lineage_edge(source_schema, source_name);
CREATE INDEX IF NOT EXISTS idx_lineage_target ON audit.lineage_edge(target_schema, target_name);
CREATE INDEX IF NOT EXISTS idx_lineage_type ON audit.lineage_edge(relationship_type);
CREATE INDEX IF NOT EXISTS idx_lineage_current ON audit.lineage_edge(is_current) WHERE is_current = TRUE;
CREATE INDEX IF NOT EXISTS idx_lineage_session ON audit.lineage_edge(session_id);

COMMENT ON TABLE audit.lineage_edge IS
    'Data lineage graph edges. Tracks data flow between entities.';

-- =============================================================================
-- HELPER FUNCTIONS
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Function: Start a transformation session (checkout)
-- -----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION audit.start_transformation_session(
    p_session_type VARCHAR,
    p_agent_id VARCHAR,
    p_source_layer VARCHAR,
    p_source_tables TEXT[],
    p_purpose TEXT DEFAULT NULL,
    p_agent_type VARCHAR DEFAULT NULL,
    p_agent_version VARCHAR DEFAULT NULL,
    p_source_filters JSONB DEFAULT NULL,
    p_data_start_date DATE DEFAULT NULL,
    p_data_end_date DATE DEFAULT NULL,
    p_parent_session_id UUID DEFAULT NULL,
    p_ticket_id VARCHAR DEFAULT NULL
) RETURNS UUID AS $$
DECLARE
    v_session_id UUID;
BEGIN
    INSERT INTO audit.transformation_session (
        session_type, agent_id, agent_type, agent_version,
        source_layer, source_tables, source_filters,
        data_start_date, data_end_date,
        purpose, ticket_id, parent_session_id,
        status
    ) VALUES (
        p_session_type, p_agent_id, p_agent_type, p_agent_version,
        p_source_layer, p_source_tables, p_source_filters,
        p_data_start_date, p_data_end_date,
        p_purpose, p_ticket_id, p_parent_session_id,
        'ACTIVE'
    )
    RETURNING id INTO v_session_id;

    RETURN v_session_id;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION audit.start_transformation_session IS
    'Start a new transformation session. Returns the session UUID.';

-- -----------------------------------------------------------------------------
-- Function: Log an operation within a session
-- -----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION audit.log_transformation_operation(
    p_session_id UUID,
    p_operation_type VARCHAR,
    p_input_tables TEXT[] DEFAULT NULL,
    p_transformation_logic TEXT DEFAULT NULL,
    p_output_table VARCHAR DEFAULT NULL,
    p_input_row_count BIGINT DEFAULT NULL,
    p_output_row_count BIGINT DEFAULT NULL,
    p_transformation_type VARCHAR DEFAULT 'SQL',
    p_parameters JSONB DEFAULT NULL,
    p_input_columns TEXT[] DEFAULT NULL,
    p_output_columns TEXT[] DEFAULT NULL,
    p_warnings TEXT[] DEFAULT NULL,
    p_execution_time_ms INT DEFAULT NULL
) RETURNS BIGINT AS $$
DECLARE
    v_operation_id BIGINT;
    v_next_order INT;
BEGIN
    -- Get next operation order
    SELECT COALESCE(MAX(operation_order), 0) + 1 INTO v_next_order
    FROM audit.transformation_operation
    WHERE session_id = p_session_id;

    -- Insert operation
    INSERT INTO audit.transformation_operation (
        session_id, operation_type, operation_order,
        input_tables, input_columns, input_row_count,
        transformation_logic, transformation_type, parameters,
        output_table, output_columns, output_row_count,
        warnings, execution_time_ms
    ) VALUES (
        p_session_id, p_operation_type, v_next_order,
        p_input_tables, p_input_columns, p_input_row_count,
        p_transformation_logic, p_transformation_type, p_parameters,
        p_output_table, p_output_columns, p_output_row_count,
        p_warnings, p_execution_time_ms
    )
    RETURNING id INTO v_operation_id;

    -- Update session counters
    UPDATE audit.transformation_session
    SET
        operations_count = operations_count + 1,
        total_rows_processed = total_rows_processed + COALESCE(p_input_row_count, 0)
    WHERE id = p_session_id;

    RETURN v_operation_id;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION audit.log_transformation_operation IS
    'Log an operation within a transformation session.';

-- -----------------------------------------------------------------------------
-- Function: Register an output artifact
-- -----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION audit.register_output_artifact(
    p_session_id UUID,
    p_artifact_type VARCHAR,
    p_artifact_name VARCHAR,
    p_artifact_location VARCHAR DEFAULT NULL,
    p_source_tables TEXT[] DEFAULT NULL,
    p_row_count BIGINT DEFAULT NULL,
    p_column_count INT DEFAULT NULL,
    p_data_as_of TIMESTAMPTZ DEFAULT NULL,
    p_data_start_date DATE DEFAULT NULL,
    p_data_end_date DATE DEFAULT NULL,
    p_description TEXT DEFAULT NULL,
    p_expires_at TIMESTAMPTZ DEFAULT NULL,
    p_metadata JSONB DEFAULT NULL
) RETURNS UUID AS $$
DECLARE
    v_artifact_id UUID;
BEGIN
    INSERT INTO audit.output_artifact (
        session_id, artifact_type, artifact_name, artifact_location,
        source_tables, row_count, column_count,
        data_as_of, data_start_date, data_end_date,
        description, expires_at, metadata,
        is_current
    ) VALUES (
        p_session_id, p_artifact_type, p_artifact_name, p_artifact_location,
        p_source_tables, p_row_count, p_column_count,
        COALESCE(p_data_as_of, NOW()), p_data_start_date, p_data_end_date,
        p_description, p_expires_at, p_metadata,
        TRUE
    )
    RETURNING id INTO v_artifact_id;

    -- Update session counter
    UPDATE audit.transformation_session
    SET outputs_count = outputs_count + 1
    WHERE id = p_session_id;

    RETURN v_artifact_id;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION audit.register_output_artifact IS
    'Register an output artifact created during a session.';

-- -----------------------------------------------------------------------------
-- Function: Add a lineage edge
-- -----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION audit.add_lineage_edge(
    p_source_type VARCHAR,
    p_source_schema VARCHAR,
    p_source_name VARCHAR,
    p_target_type VARCHAR,
    p_target_schema VARCHAR,
    p_target_name VARCHAR,
    p_relationship_type VARCHAR,
    p_session_id UUID DEFAULT NULL,
    p_source_column VARCHAR DEFAULT NULL,
    p_target_column VARCHAR DEFAULT NULL,
    p_transformation_description TEXT DEFAULT NULL
) RETURNS BIGINT AS $$
DECLARE
    v_edge_id BIGINT;
BEGIN
    INSERT INTO audit.lineage_edge (
        source_type, source_schema, source_name, source_column,
        target_type, target_schema, target_name, target_column,
        relationship_type, session_id, transformation_description,
        is_current
    ) VALUES (
        p_source_type, p_source_schema, p_source_name, p_source_column,
        p_target_type, p_target_schema, p_target_name, p_target_column,
        p_relationship_type, p_session_id, p_transformation_description,
        TRUE
    )
    ON CONFLICT (source_type, source_schema, source_name, source_column,
                 target_type, target_schema, target_name, target_column,
                 relationship_type)
    DO UPDATE SET
        session_id = EXCLUDED.session_id,
        transformation_description = COALESCE(EXCLUDED.transformation_description, audit.lineage_edge.transformation_description),
        is_current = TRUE,
        invalidated_at = NULL
    RETURNING id INTO v_edge_id;

    RETURN v_edge_id;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION audit.add_lineage_edge IS
    'Add or update a lineage edge between two data entities.';

-- -----------------------------------------------------------------------------
-- Function: Complete a transformation session (checkin)
-- -----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION audit.complete_transformation_session(
    p_session_id UUID,
    p_status VARCHAR DEFAULT 'COMPLETED',
    p_error_message TEXT DEFAULT NULL,
    p_error_details JSONB DEFAULT NULL
) RETURNS VOID AS $$
BEGIN
    UPDATE audit.transformation_session
    SET
        status = p_status,
        completed_at = NOW(),
        error_message = p_error_message,
        error_details = p_error_details
    WHERE id = p_session_id;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION audit.complete_transformation_session IS
    'Complete a transformation session (checkin).';

-- -----------------------------------------------------------------------------
-- Function: Abandon stale sessions (cleanup)
-- -----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION audit.abandon_stale_sessions(
    p_max_age_hours INT DEFAULT 24
) RETURNS INT AS $$
DECLARE
    v_count INT;
BEGIN
    UPDATE audit.transformation_session
    SET
        status = 'ABANDONED',
        completed_at = NOW(),
        error_message = 'Session abandoned due to inactivity'
    WHERE status = 'ACTIVE'
      AND started_at < NOW() - (p_max_age_hours || ' hours')::INTERVAL;

    GET DIAGNOSTICS v_count = ROW_COUNT;
    RETURN v_count;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION audit.abandon_stale_sessions IS
    'Mark stale ACTIVE sessions as ABANDONED. Run periodically for cleanup.';

-- =============================================================================
-- MONITORING VIEWS
-- =============================================================================

-- -----------------------------------------------------------------------------
-- View: Recent transformation activity
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW audit.recent_transformation_activity AS
SELECT
    ts.id AS session_id,
    ts.session_type,
    ts.agent_id,
    ts.agent_type,
    ts.source_layer,
    ts.source_tables,
    ts.status,
    ts.started_at,
    ts.completed_at,
    EXTRACT(EPOCH FROM (COALESCE(ts.completed_at, NOW()) - ts.started_at))::INT AS duration_seconds,
    ts.operations_count,
    ts.outputs_count,
    ts.total_rows_processed,
    ts.purpose,
    ts.error_message
FROM audit.transformation_session ts
WHERE ts.started_at >= NOW() - INTERVAL '7 days'
ORDER BY ts.started_at DESC;

COMMENT ON VIEW audit.recent_transformation_activity IS
    'Summary of transformation sessions in the last 7 days.';

-- -----------------------------------------------------------------------------
-- View: Active sessions
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW audit.active_sessions AS
SELECT
    ts.id AS session_id,
    ts.agent_id,
    ts.session_type,
    ts.source_tables,
    ts.started_at,
    EXTRACT(EPOCH FROM (NOW() - ts.started_at))::INT AS active_seconds,
    ts.operations_count,
    ts.purpose
FROM audit.transformation_session ts
WHERE ts.status = 'ACTIVE'
ORDER BY ts.started_at;

COMMENT ON VIEW audit.active_sessions IS
    'Currently active transformation sessions.';

-- -----------------------------------------------------------------------------
-- View: Transformation metrics by agent
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW audit.agent_transformation_metrics AS
SELECT
    agent_id,
    agent_type,
    COUNT(*) AS total_sessions,
    COUNT(*) FILTER (WHERE status = 'COMPLETED') AS completed_sessions,
    COUNT(*) FILTER (WHERE status = 'FAILED') AS failed_sessions,
    SUM(operations_count) AS total_operations,
    SUM(outputs_count) AS total_outputs,
    SUM(total_rows_processed) AS total_rows_processed,
    AVG(EXTRACT(EPOCH FROM (completed_at - started_at)))::INT AS avg_duration_seconds,
    MAX(started_at) AS last_activity
FROM audit.transformation_session
WHERE started_at >= NOW() - INTERVAL '30 days'
GROUP BY agent_id, agent_type
ORDER BY total_sessions DESC;

COMMENT ON VIEW audit.agent_transformation_metrics IS
    'Aggregated transformation metrics by agent (last 30 days).';

-- -----------------------------------------------------------------------------
-- View: Data lineage summary
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW audit.lineage_summary AS
SELECT
    source_schema || '.' || source_name AS source,
    target_schema || '.' || target_name AS target,
    relationship_type,
    transformation_description,
    created_at
FROM audit.lineage_edge
WHERE is_current = TRUE
ORDER BY source_schema, source_name, target_schema, target_name;

COMMENT ON VIEW audit.lineage_summary IS
    'Summary of current data lineage relationships.';

-- =============================================================================
-- ARCHIVAL SUPPORT
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Function: Archive old sessions
-- Moves session data older than specified days to archive
-- -----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION audit.archive_old_sessions(
    p_days_to_keep INT DEFAULT 30
) RETURNS TABLE (
    sessions_archived BIGINT,
    operations_deleted BIGINT,
    artifacts_deleted BIGINT
) AS $$
DECLARE
    v_sessions BIGINT := 0;
    v_operations BIGINT := 0;
    v_artifacts BIGINT := 0;
    v_cutoff_date TIMESTAMPTZ;
BEGIN
    v_cutoff_date := NOW() - (p_days_to_keep || ' days')::INTERVAL;

    -- Count before delete
    SELECT COUNT(*) INTO v_sessions
    FROM audit.transformation_session
    WHERE completed_at < v_cutoff_date
      AND status IN ('COMPLETED', 'FAILED', 'ABANDONED');

    -- Delete operations (will cascade from sessions)
    DELETE FROM audit.transformation_operation
    WHERE session_id IN (
        SELECT id FROM audit.transformation_session
        WHERE completed_at < v_cutoff_date
          AND status IN ('COMPLETED', 'FAILED', 'ABANDONED')
    );
    GET DIAGNOSTICS v_operations = ROW_COUNT;

    -- Delete artifacts (will cascade from sessions)
    DELETE FROM audit.output_artifact
    WHERE session_id IN (
        SELECT id FROM audit.transformation_session
        WHERE completed_at < v_cutoff_date
          AND status IN ('COMPLETED', 'FAILED', 'ABANDONED')
    );
    GET DIAGNOSTICS v_artifacts = ROW_COUNT;

    -- Delete sessions
    DELETE FROM audit.transformation_session
    WHERE completed_at < v_cutoff_date
      AND status IN ('COMPLETED', 'FAILED', 'ABANDONED');

    RETURN QUERY SELECT v_sessions, v_operations, v_artifacts;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION audit.archive_old_sessions IS
    'Delete old completed sessions and their operations/artifacts. Keeps lineage edges.';

-- =============================================================================
-- SEED INITIAL LINEAGE (existing tables)
-- =============================================================================

-- Document existing Bronze → Silver relationships
INSERT INTO audit.lineage_edge (
    source_type, source_schema, source_name,
    target_type, target_schema, target_name,
    relationship_type, transformation_description
) VALUES
    -- WASDE flow
    ('TABLE', 'bronze', 'wasde_cell', 'TABLE', 'silver', 'observation', 'TRANSFORMS', 'WASDE cells parsed to standardized observations'),
    ('TABLE', 'bronze', 'wasde_cell', 'TABLE', 'silver', 'balance_sheet_item', 'TRANSFORMS', 'WASDE balance sheet items extracted'),
    ('TABLE', 'bronze', 'wasde_release', 'TABLE', 'silver', 'observation', 'REFERENCES', 'Release metadata linked to observations'),

    -- Census trade flow
    ('TABLE', 'bronze', 'census_trade_raw', 'TABLE', 'silver', 'trade_flow', 'AGGREGATES', 'Census trade aggregated by period'),
    ('TABLE', 'bronze', 'census_trade_raw', 'TABLE', 'silver', 'observation', 'TRANSFORMS', 'Census trade to time-series observations'),

    -- FGIS inspections flow
    ('TABLE', 'bronze', 'fgis_inspection_raw', 'TABLE', 'silver', 'trade_flow', 'AGGREGATES', 'FGIS inspections aggregated by period'),
    ('TABLE', 'bronze', 'fgis_inspection_raw', 'TABLE', 'silver', 'observation', 'TRANSFORMS', 'FGIS inspections to observations'),

    -- Silver → Gold views
    ('TABLE', 'bronze', 'wasde_cell', 'VIEW', 'gold', 'us_corn_balance_sheet', 'TRANSFORMS', 'Pivoted corn balance sheet view'),
    ('TABLE', 'bronze', 'wasde_cell', 'VIEW', 'gold', 'us_soybeans_balance_sheet', 'TRANSFORMS', 'Pivoted soybean balance sheet view'),
    ('TABLE', 'bronze', 'wasde_release', 'VIEW', 'gold', 'wasde_changes', 'TRANSFORMS', 'WASDE month-over-month changes'),
    ('TABLE', 'bronze', 'fgis_inspection_raw', 'VIEW', 'gold', 'soybean_inspections_by_destination', 'AGGREGATES', 'Monthly soybean inspection summary'),
    ('TABLE', 'silver', 'trade_flow', 'VIEW', 'gold', 'marketing_year_trade_summary', 'AGGREGATES', 'Marketing year trade totals'),

    -- Reconciliation views
    ('TABLE', 'bronze', 'census_trade_raw', 'VIEW', 'gold', 'reconcile_census_monthly_totals', 'AGGREGATES', 'Census reconciliation totals'),
    ('TABLE', 'bronze', 'fgis_inspection_raw', 'VIEW', 'gold', 'reconcile_fgis_weekly_totals', 'AGGREGATES', 'FGIS reconciliation totals'),

    -- Quality dashboard
    ('TABLE', 'audit', 'ingest_run', 'VIEW', 'gold', 'data_quality_dashboard', 'DERIVES_FROM', 'Ingest status dashboard'),
    ('TABLE', 'audit', 'validation_status', 'VIEW', 'gold', 'data_quality_dashboard', 'DERIVES_FROM', 'Validation status included')
ON CONFLICT DO NOTHING;

-- =============================================================================
-- END OF TRANSFORMATION LOGGING SCRIPT
-- =============================================================================
