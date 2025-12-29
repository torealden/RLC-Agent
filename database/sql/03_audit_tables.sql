-- ============================================================================
-- Round Lakes Commodities - Audit and Ingestion Tables
-- ============================================================================
-- File: 03_audit_tables.sql
-- Purpose: Track all data ingestion runs, validation status, and error logs
-- Execute: After 02_core_dimensions.sql
-- ============================================================================

-- ============================================================================
-- INGEST_RUN: Tracks each data ingestion job execution
-- ============================================================================
CREATE TABLE IF NOT EXISTS audit.ingest_run (
    id                  BIGSERIAL PRIMARY KEY,
    run_uuid            UUID NOT NULL DEFAULT gen_random_uuid(),

    -- What was being ingested
    data_source_id      INTEGER NOT NULL REFERENCES core.data_source(id),
    job_name            VARCHAR(255) NOT NULL,  -- e.g., 'wasde_monthly_ingest'
    job_version         VARCHAR(50),            -- Agent/script version

    -- Agent identification
    agent_id            VARCHAR(100),           -- Which agent instance
    agent_type          VARCHAR(100),           -- collector, checker, etc.

    -- Timing
    started_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at        TIMESTAMPTZ,
    duration_seconds    DECIMAL(10,2) GENERATED ALWAYS AS (
        EXTRACT(EPOCH FROM (completed_at - started_at))
    ) STORED,

    -- Status
    status              VARCHAR(20) NOT NULL DEFAULT 'running',
    error_message       TEXT,
    error_details       JSONB,

    -- Statistics
    records_fetched     INTEGER DEFAULT 0,
    records_inserted    INTEGER DEFAULT 0,
    records_updated     INTEGER DEFAULT 0,
    records_skipped     INTEGER DEFAULT 0,
    records_failed      INTEGER DEFAULT 0,

    -- Source tracking
    source_url          VARCHAR(2000),
    source_timestamp    TIMESTAMPTZ,    -- When source data was published
    source_checksum     VARCHAR(128),   -- SHA-256 of raw response
    api_response_code   INTEGER,

    -- Optional: store raw payload reference or inline (for small payloads)
    raw_payload_id      BIGINT,         -- FK to raw_payload if stored separately
    raw_payload_inline  JSONB,          -- For small payloads (< 1MB)

    -- Metadata
    parameters          JSONB DEFAULT '{}',  -- Job parameters used
    metadata            JSONB DEFAULT '{}',

    -- Timestamps
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT ingest_run_status_valid CHECK (
        status IN ('pending', 'running', 'success', 'partial', 'failed', 'cancelled')
    ),
    CONSTRAINT ingest_run_uuid_unique UNIQUE (run_uuid)
);

COMMENT ON TABLE audit.ingest_run IS
    'Tracks each data ingestion job execution. Every observation links back to '
    'the ingest_run that created it for full traceability.';

COMMENT ON COLUMN audit.ingest_run.run_uuid IS
    'Globally unique identifier for cross-system reference';
COMMENT ON COLUMN audit.ingest_run.status IS
    'pending, running, success, partial (some errors), failed, cancelled';
COMMENT ON COLUMN audit.ingest_run.source_checksum IS
    'SHA-256 hash of raw API response for duplicate detection';
COMMENT ON COLUMN audit.ingest_run.parameters IS
    'Job parameters: date_range, filters, options used for this run';

-- Indexes for common queries
CREATE INDEX IF NOT EXISTS idx_ingest_run_source ON audit.ingest_run(data_source_id);
CREATE INDEX IF NOT EXISTS idx_ingest_run_status ON audit.ingest_run(status);
CREATE INDEX IF NOT EXISTS idx_ingest_run_started ON audit.ingest_run(started_at DESC);
CREATE INDEX IF NOT EXISTS idx_ingest_run_job ON audit.ingest_run(job_name, started_at DESC);
CREATE INDEX IF NOT EXISTS idx_ingest_run_agent ON audit.ingest_run(agent_id);


-- ============================================================================
-- RAW_PAYLOAD: Optional storage for large raw API responses
-- ============================================================================
CREATE TABLE IF NOT EXISTS audit.raw_payload (
    id              BIGSERIAL PRIMARY KEY,
    ingest_run_id   BIGINT REFERENCES audit.ingest_run(id),
    payload_type    VARCHAR(50) NOT NULL,  -- json, xml, csv, binary
    payload_size    INTEGER,               -- Size in bytes
    checksum        VARCHAR(128),          -- SHA-256

    -- Storage: either inline or external reference
    payload_data    BYTEA,                 -- For binary or compressed
    payload_json    JSONB,                 -- For JSON payloads
    external_path   VARCHAR(1000),         -- Path to external storage

    compression     VARCHAR(20),           -- gzip, zstd, none
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT raw_payload_type_valid CHECK (
        payload_type IN ('json', 'xml', 'csv', 'binary', 'html', 'text')
    )
);

COMMENT ON TABLE audit.raw_payload IS
    'Optional storage for large raw API responses. Use for payloads > 1MB '
    'or when binary/compressed storage is needed.';

-- Add FK back to ingest_run
ALTER TABLE audit.ingest_run
    ADD CONSTRAINT ingest_run_raw_payload_fk
    FOREIGN KEY (raw_payload_id) REFERENCES audit.raw_payload(id);


-- ============================================================================
-- INGEST_ERROR: Detailed error logging per record
-- ============================================================================
CREATE TABLE IF NOT EXISTS audit.ingest_error (
    id              BIGSERIAL PRIMARY KEY,
    ingest_run_id   BIGINT NOT NULL REFERENCES audit.ingest_run(id),

    -- Error context
    error_time      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    error_type      VARCHAR(100) NOT NULL,  -- parse_error, validation_error, etc.
    error_message   TEXT NOT NULL,
    error_code      VARCHAR(50),

    -- Record context
    record_index    INTEGER,                -- Position in source data
    record_key      VARCHAR(500),           -- Natural key of failed record
    record_data     JSONB,                  -- The problematic record (for debugging)

    -- Stack trace (for debugging)
    stack_trace     TEXT,

    -- Resolution
    is_resolved     BOOLEAN DEFAULT FALSE,
    resolved_at     TIMESTAMPTZ,
    resolved_by     VARCHAR(100),
    resolution_note TEXT
);

COMMENT ON TABLE audit.ingest_error IS
    'Detailed per-record error logging for ingestion failures.';

-- Indexes
CREATE INDEX IF NOT EXISTS idx_ingest_error_run ON audit.ingest_error(ingest_run_id);
CREATE INDEX IF NOT EXISTS idx_ingest_error_type ON audit.ingest_error(error_type);
CREATE INDEX IF NOT EXISTS idx_ingest_error_unresolved ON audit.ingest_error(is_resolved)
    WHERE is_resolved = FALSE;


-- ============================================================================
-- VALIDATION_STATUS: Tracks validation state for releases/datasets
-- ============================================================================
CREATE TABLE IF NOT EXISTS audit.validation_status (
    id              BIGSERIAL PRIMARY KEY,

    -- What is being validated
    entity_type     VARCHAR(50) NOT NULL,   -- release, ingest_run, dataset
    entity_id       VARCHAR(100) NOT NULL,  -- e.g., wasde release date
    data_source_id  INTEGER REFERENCES core.data_source(id),
    ingest_run_id   BIGINT REFERENCES audit.ingest_run(id),

    -- Validation state
    status          VARCHAR(30) NOT NULL DEFAULT 'pending',
    validation_type VARCHAR(50),            -- automated, manual, reconciliation

    -- Checker details
    checker_agent_id VARCHAR(100),
    checker_run_id   BIGINT REFERENCES audit.ingest_run(id),

    -- Results
    checks_passed   INTEGER DEFAULT 0,
    checks_failed   INTEGER DEFAULT 0,
    checks_skipped  INTEGER DEFAULT 0,
    check_results   JSONB DEFAULT '[]',     -- Array of individual check results

    -- Discrepancies found
    discrepancies   JSONB DEFAULT '[]',
    discrepancy_notes TEXT,

    -- Approval
    approved_by     VARCHAR(100),
    approved_at     TIMESTAMPTZ,
    approval_notes  TEXT,

    -- Timestamps
    validated_at    TIMESTAMPTZ,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT validation_status_valid CHECK (
        status IN ('pending', 'in_progress', 'passed', 'failed',
                   'passed_with_warnings', 'requires_review', 'superseded')
    ),
    CONSTRAINT validation_entity_unique UNIQUE (entity_type, entity_id, data_source_id)
);

COMMENT ON TABLE audit.validation_status IS
    'Tracks validation state for data releases. A release must pass validation '
    'before it can be published to Gold layer.';

COMMENT ON COLUMN audit.validation_status.entity_type IS
    'Type of entity: release (e.g., WASDE month), ingest_run, dataset';
COMMENT ON COLUMN audit.validation_status.entity_id IS
    'Identifier for the entity (e.g., "2024-01" for January WASDE)';
COMMENT ON COLUMN audit.validation_status.check_results IS
    'Array of {check_name, passed, message, details} objects';

-- Indexes
CREATE INDEX IF NOT EXISTS idx_validation_status_entity
    ON audit.validation_status(entity_type, entity_id);
CREATE INDEX IF NOT EXISTS idx_validation_status_status
    ON audit.validation_status(status);
CREATE INDEX IF NOT EXISTS idx_validation_status_pending
    ON audit.validation_status(status) WHERE status IN ('pending', 'in_progress');


-- ============================================================================
-- RECONCILIATION_CHECK: Individual reconciliation check results
-- ============================================================================
CREATE TABLE IF NOT EXISTS audit.reconciliation_check (
    id                  BIGSERIAL PRIMARY KEY,
    validation_id       BIGINT REFERENCES audit.validation_status(id),
    ingest_run_id       BIGINT REFERENCES audit.ingest_run(id),

    -- Check definition
    check_name          VARCHAR(255) NOT NULL,
    check_type          VARCHAR(50) NOT NULL,   -- row_count, sum, value_match, etc.
    check_description   TEXT,

    -- What was checked
    source_type         VARCHAR(50),            -- bronze, silver, gold, excel
    source_identifier   VARCHAR(500),           -- Table/view/file name
    target_type         VARCHAR(50),
    target_identifier   VARCHAR(500),

    -- Expected vs Actual
    expected_value      TEXT,
    actual_value        TEXT,
    tolerance           DECIMAL(10,4),
    variance            DECIMAL(15,4),
    variance_pct        DECIMAL(10,4),

    -- Result
    passed              BOOLEAN NOT NULL,
    severity            VARCHAR(20) NOT NULL DEFAULT 'error',
    message             TEXT,
    details             JSONB,

    -- Timestamps
    checked_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT reconciliation_check_severity_valid CHECK (
        severity IN ('info', 'warning', 'error', 'critical')
    )
);

COMMENT ON TABLE audit.reconciliation_check IS
    'Individual reconciliation check results. Used to verify database values '
    'match source data and Excel spreadsheet outputs.';

-- Indexes
CREATE INDEX IF NOT EXISTS idx_reconciliation_validation
    ON audit.reconciliation_check(validation_id);
CREATE INDEX IF NOT EXISTS idx_reconciliation_failed
    ON audit.reconciliation_check(passed) WHERE passed = FALSE;


-- ============================================================================
-- DATA_LINEAGE: Track data transformations (optional)
-- ============================================================================
CREATE TABLE IF NOT EXISTS audit.data_lineage (
    id              BIGSERIAL PRIMARY KEY,

    -- Target (where data was written)
    target_schema   VARCHAR(63) NOT NULL,
    target_table    VARCHAR(63) NOT NULL,
    target_key      JSONB NOT NULL,         -- Natural key of the record

    -- Source (where data came from)
    source_schema   VARCHAR(63) NOT NULL,
    source_table    VARCHAR(63) NOT NULL,
    source_key      JSONB NOT NULL,

    -- Transformation
    transformation  VARCHAR(255),           -- Name of the transformation
    ingest_run_id   BIGINT REFERENCES audit.ingest_run(id),

    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

COMMENT ON TABLE audit.data_lineage IS
    'Optional: Track data lineage from Bronze to Silver to Gold.';

-- Indexes
CREATE INDEX IF NOT EXISTS idx_lineage_target
    ON audit.data_lineage(target_schema, target_table);
CREATE INDEX IF NOT EXISTS idx_lineage_source
    ON audit.data_lineage(source_schema, source_table);


-- ============================================================================
-- AGENT_HEARTBEAT: Track agent health (for monitoring)
-- ============================================================================
CREATE TABLE IF NOT EXISTS audit.agent_heartbeat (
    id              BIGSERIAL PRIMARY KEY,
    agent_id        VARCHAR(100) NOT NULL,
    agent_type      VARCHAR(100) NOT NULL,
    hostname        VARCHAR(255),
    pid             INTEGER,
    status          VARCHAR(20) NOT NULL DEFAULT 'alive',
    current_task    VARCHAR(500),
    last_heartbeat  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    metadata        JSONB DEFAULT '{}',

    CONSTRAINT agent_heartbeat_unique UNIQUE (agent_id)
);

COMMENT ON TABLE audit.agent_heartbeat IS
    'Agent health monitoring. Agents update this periodically to indicate they are alive.';

-- Index for stale agents
CREATE INDEX IF NOT EXISTS idx_agent_heartbeat_time
    ON audit.agent_heartbeat(last_heartbeat);


-- ============================================================================
-- Updated_at trigger function
-- ============================================================================
CREATE OR REPLACE FUNCTION audit.set_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Apply to tables with updated_at
CREATE TRIGGER trg_ingest_run_updated
    BEFORE UPDATE ON audit.ingest_run
    FOR EACH ROW EXECUTE FUNCTION audit.set_updated_at();

CREATE TRIGGER trg_validation_status_updated
    BEFORE UPDATE ON audit.validation_status
    FOR EACH ROW EXECUTE FUNCTION audit.set_updated_at();


-- ============================================================================
-- Verification
-- ============================================================================
DO $$
BEGIN
    RAISE NOTICE 'Audit tables created:';
    RAISE NOTICE '  - audit.ingest_run';
    RAISE NOTICE '  - audit.raw_payload';
    RAISE NOTICE '  - audit.ingest_error';
    RAISE NOTICE '  - audit.validation_status';
    RAISE NOTICE '  - audit.reconciliation_check';
    RAISE NOTICE '  - audit.data_lineage';
    RAISE NOTICE '  - audit.agent_heartbeat';
END $$;
