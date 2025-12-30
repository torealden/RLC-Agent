-- ============================================================================
-- Round Lakes Commodities - Role-Based Access Control
-- ============================================================================
-- File: 07_roles_grants.sql
-- Purpose: Create roles and grant appropriate permissions for multi-agent access
-- Execute: After 06_gold_views.sql (as superuser)
-- ============================================================================
-- Roles follow principle of least privilege:
-- - ingest_writer: Write bronze/silver, read core/audit
-- - checker: Read all, write audit.validation_status
-- - analyst_reader: Read silver/gold only
-- - admin: Full access
-- ============================================================================

-- ============================================================================
-- CREATE ROLES (if not exist)
-- ============================================================================

-- Admin role: Full database access
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'rlc_admin') THEN
        CREATE ROLE rlc_admin WITH LOGIN CREATEROLE;
    END IF;
END $$;

-- Ingest writer role: For collector agents
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'ingest_writer') THEN
        CREATE ROLE ingest_writer WITH LOGIN;
    END IF;
END $$;

-- Checker role: For validation agents
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'checker') THEN
        CREATE ROLE checker WITH LOGIN;
    END IF;
END $$;

-- Analyst reader role: For report/trading agents
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'analyst_reader') THEN
        CREATE ROLE analyst_reader WITH LOGIN;
    END IF;
END $$;

-- Application role: For general application access (read-heavy)
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'rlc_app') THEN
        CREATE ROLE rlc_app WITH LOGIN;
    END IF;
END $$;

-- ============================================================================
-- GRANT SCHEMA USAGE
-- ============================================================================

-- All roles need schema usage to see objects
GRANT USAGE ON SCHEMA core TO ingest_writer, checker, analyst_reader, rlc_app;
GRANT USAGE ON SCHEMA audit TO ingest_writer, checker, analyst_reader, rlc_app;
GRANT USAGE ON SCHEMA bronze TO ingest_writer, checker, analyst_reader, rlc_app;
GRANT USAGE ON SCHEMA silver TO ingest_writer, checker, analyst_reader, rlc_app;
GRANT USAGE ON SCHEMA gold TO checker, analyst_reader, rlc_app;

-- Admin gets all schemas
GRANT ALL ON SCHEMA core, audit, bronze, silver, gold TO rlc_admin;

-- ============================================================================
-- INGEST_WRITER PERMISSIONS
-- Collector agents that ingest data
-- ============================================================================

-- Core schema: READ ONLY (lookup dimensions)
GRANT SELECT ON ALL TABLES IN SCHEMA core TO ingest_writer;
GRANT SELECT ON ALL SEQUENCES IN SCHEMA core TO ingest_writer;

-- Audit schema: WRITE (create ingest runs, log errors)
GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA audit TO ingest_writer;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA audit TO ingest_writer;

-- Bronze schema: WRITE (store raw data)
GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA bronze TO ingest_writer;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA bronze TO ingest_writer;

-- Silver schema: WRITE (store observations)
GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA silver TO ingest_writer;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA silver TO ingest_writer;

-- Gold schema: READ ONLY
GRANT SELECT ON ALL TABLES IN SCHEMA gold TO ingest_writer;

-- Future tables in these schemas
ALTER DEFAULT PRIVILEGES IN SCHEMA core GRANT SELECT ON TABLES TO ingest_writer;
ALTER DEFAULT PRIVILEGES IN SCHEMA audit GRANT SELECT, INSERT, UPDATE ON TABLES TO ingest_writer;
ALTER DEFAULT PRIVILEGES IN SCHEMA audit GRANT USAGE, SELECT ON SEQUENCES TO ingest_writer;
ALTER DEFAULT PRIVILEGES IN SCHEMA bronze GRANT SELECT, INSERT, UPDATE ON TABLES TO ingest_writer;
ALTER DEFAULT PRIVILEGES IN SCHEMA bronze GRANT USAGE, SELECT ON SEQUENCES TO ingest_writer;
ALTER DEFAULT PRIVILEGES IN SCHEMA silver GRANT SELECT, INSERT, UPDATE ON TABLES TO ingest_writer;
ALTER DEFAULT PRIVILEGES IN SCHEMA silver GRANT USAGE, SELECT ON SEQUENCES TO ingest_writer;
ALTER DEFAULT PRIVILEGES IN SCHEMA gold GRANT SELECT ON TABLES TO ingest_writer;

COMMENT ON ROLE ingest_writer IS
    'Collector agents: Write to bronze/silver/audit, read core dimensions';

-- ============================================================================
-- CHECKER PERMISSIONS
-- Validation agents that verify data quality
-- ============================================================================

-- Core schema: READ ONLY
GRANT SELECT ON ALL TABLES IN SCHEMA core TO checker;

-- Audit schema: READ + WRITE validation tables
GRANT SELECT ON ALL TABLES IN SCHEMA audit TO checker;
GRANT INSERT, UPDATE ON audit.validation_status TO checker;
GRANT INSERT ON audit.reconciliation_check TO checker;
GRANT UPDATE ON audit.ingest_run TO checker;  -- To mark as validated
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA audit TO checker;

-- Bronze schema: READ ONLY (inspect raw data)
GRANT SELECT ON ALL TABLES IN SCHEMA bronze TO checker;

-- Silver schema: READ ONLY
GRANT SELECT ON ALL TABLES IN SCHEMA silver TO checker;

-- Gold schema: READ ONLY
GRANT SELECT ON ALL TABLES IN SCHEMA gold TO checker;

-- Future tables
ALTER DEFAULT PRIVILEGES IN SCHEMA core GRANT SELECT ON TABLES TO checker;
ALTER DEFAULT PRIVILEGES IN SCHEMA bronze GRANT SELECT ON TABLES TO checker;
ALTER DEFAULT PRIVILEGES IN SCHEMA silver GRANT SELECT ON TABLES TO checker;
ALTER DEFAULT PRIVILEGES IN SCHEMA gold GRANT SELECT ON TABLES TO checker;

COMMENT ON ROLE checker IS
    'Validation agents: Read all schemas, write validation status';

-- ============================================================================
-- ANALYST_READER PERMISSIONS
-- Report-writing and trading agents (read-only)
-- ============================================================================

-- Core schema: READ ONLY (for dimension lookups)
GRANT SELECT ON ALL TABLES IN SCHEMA core TO analyst_reader;

-- Audit schema: READ ONLY (for traceability)
GRANT SELECT ON ALL TABLES IN SCHEMA audit TO analyst_reader;

-- Bronze schema: NO ACCESS (don't need raw data)
-- If needed for forensics, uncomment:
-- GRANT SELECT ON ALL TABLES IN SCHEMA bronze TO analyst_reader;

-- Silver schema: READ ONLY
GRANT SELECT ON ALL TABLES IN SCHEMA silver TO analyst_reader;

-- Gold schema: READ ONLY
GRANT SELECT ON ALL TABLES IN SCHEMA gold TO analyst_reader;

-- Future tables
ALTER DEFAULT PRIVILEGES IN SCHEMA core GRANT SELECT ON TABLES TO analyst_reader;
ALTER DEFAULT PRIVILEGES IN SCHEMA silver GRANT SELECT ON TABLES TO analyst_reader;
ALTER DEFAULT PRIVILEGES IN SCHEMA gold GRANT SELECT ON TABLES TO analyst_reader;

COMMENT ON ROLE analyst_reader IS
    'Analyst agents: Read-only access to silver, gold, and core dimensions';

-- ============================================================================
-- RLC_APP PERMISSIONS
-- General application access
-- ============================================================================

-- Similar to analyst_reader but with bronze access for debugging
GRANT SELECT ON ALL TABLES IN SCHEMA core TO rlc_app;
GRANT SELECT ON ALL TABLES IN SCHEMA audit TO rlc_app;
GRANT SELECT ON ALL TABLES IN SCHEMA bronze TO rlc_app;
GRANT SELECT ON ALL TABLES IN SCHEMA silver TO rlc_app;
GRANT SELECT ON ALL TABLES IN SCHEMA gold TO rlc_app;

-- Future tables
ALTER DEFAULT PRIVILEGES IN SCHEMA core GRANT SELECT ON TABLES TO rlc_app;
ALTER DEFAULT PRIVILEGES IN SCHEMA audit GRANT SELECT ON TABLES TO rlc_app;
ALTER DEFAULT PRIVILEGES IN SCHEMA bronze GRANT SELECT ON TABLES TO rlc_app;
ALTER DEFAULT PRIVILEGES IN SCHEMA silver GRANT SELECT ON TABLES TO rlc_app;
ALTER DEFAULT PRIVILEGES IN SCHEMA gold GRANT SELECT ON TABLES TO rlc_app;

COMMENT ON ROLE rlc_app IS
    'Application role: Read-only access to all schemas for general queries';

-- ============================================================================
-- RLC_ADMIN PERMISSIONS
-- Full administrative access
-- ============================================================================

GRANT ALL ON ALL TABLES IN SCHEMA core TO rlc_admin;
GRANT ALL ON ALL TABLES IN SCHEMA audit TO rlc_admin;
GRANT ALL ON ALL TABLES IN SCHEMA bronze TO rlc_admin;
GRANT ALL ON ALL TABLES IN SCHEMA silver TO rlc_admin;
GRANT ALL ON ALL TABLES IN SCHEMA gold TO rlc_admin;
GRANT ALL ON ALL SEQUENCES IN SCHEMA core TO rlc_admin;
GRANT ALL ON ALL SEQUENCES IN SCHEMA audit TO rlc_admin;
GRANT ALL ON ALL SEQUENCES IN SCHEMA bronze TO rlc_admin;
GRANT ALL ON ALL SEQUENCES IN SCHEMA silver TO rlc_admin;
GRANT ALL ON ALL SEQUENCES IN SCHEMA gold TO rlc_admin;
GRANT ALL ON ALL FUNCTIONS IN SCHEMA core TO rlc_admin;
GRANT ALL ON ALL FUNCTIONS IN SCHEMA audit TO rlc_admin;
GRANT ALL ON ALL FUNCTIONS IN SCHEMA bronze TO rlc_admin;
GRANT ALL ON ALL FUNCTIONS IN SCHEMA silver TO rlc_admin;
GRANT ALL ON ALL FUNCTIONS IN SCHEMA gold TO rlc_admin;

-- Future objects
ALTER DEFAULT PRIVILEGES IN SCHEMA core GRANT ALL ON TABLES TO rlc_admin;
ALTER DEFAULT PRIVILEGES IN SCHEMA audit GRANT ALL ON TABLES TO rlc_admin;
ALTER DEFAULT PRIVILEGES IN SCHEMA bronze GRANT ALL ON TABLES TO rlc_admin;
ALTER DEFAULT PRIVILEGES IN SCHEMA silver GRANT ALL ON TABLES TO rlc_admin;
ALTER DEFAULT PRIVILEGES IN SCHEMA gold GRANT ALL ON TABLES TO rlc_admin;

COMMENT ON ROLE rlc_admin IS
    'Admin role: Full access to all schemas for maintenance and debugging';

-- ============================================================================
-- FUNCTION EXECUTION PERMISSIONS
-- ============================================================================

-- Allow ingest_writer to use helper functions
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA core TO ingest_writer;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA silver TO ingest_writer;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA bronze TO ingest_writer;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA audit TO ingest_writer;

-- Allow checker to use functions
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA core TO checker;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA silver TO checker;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA audit TO checker;

-- Allow analyst to use read-only functions
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA core TO analyst_reader;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA silver TO analyst_reader;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA gold TO analyst_reader;

-- ============================================================================
-- ROW-LEVEL SECURITY (Optional - for multi-tenant scenarios)
-- ============================================================================
-- Uncomment if you need to restrict access to specific data by agent

-- Example: Restrict by data_source
-- ALTER TABLE silver.observation ENABLE ROW LEVEL SECURITY;
--
-- CREATE POLICY observation_by_source ON silver.observation
--     FOR SELECT
--     USING (
--         series_id IN (
--             SELECT id FROM core.series
--             WHERE data_source_id IN (
--                 SELECT ds.id FROM core.data_source ds
--                 JOIN allowed_sources_for_role(current_user) asr ON ds.code = asr.source_code
--             )
--         )
--     );

-- ============================================================================
-- CONNECTION LIMITS (apply via ALTER ROLE)
-- ============================================================================
-- Limit concurrent connections per role to prevent resource exhaustion

-- ALTER ROLE ingest_writer CONNECTION LIMIT 5;
-- ALTER ROLE checker CONNECTION LIMIT 3;
-- ALTER ROLE analyst_reader CONNECTION LIMIT 10;
-- ALTER ROLE rlc_app CONNECTION LIMIT 5;

-- ============================================================================
-- STATEMENT TIMEOUTS (apply via ALTER ROLE)
-- ============================================================================
-- Prevent long-running queries from blocking resources

-- ALTER ROLE analyst_reader SET statement_timeout = '30s';
-- ALTER ROLE checker SET statement_timeout = '60s';
-- ALTER ROLE ingest_writer SET statement_timeout = '5min';

-- ============================================================================
-- SAMPLE USERS (create specific login users inheriting from roles)
-- ============================================================================

-- Collector agent user
-- CREATE USER wasde_collector WITH PASSWORD 'secure_password_here';
-- GRANT ingest_writer TO wasde_collector;

-- Validation agent user
-- CREATE USER validator_agent WITH PASSWORD 'secure_password_here';
-- GRANT checker TO validator_agent;

-- Report agent user
-- CREATE USER report_writer WITH PASSWORD 'secure_password_here';
-- GRANT analyst_reader TO report_writer;

-- Trading agent user
-- CREATE USER trading_analyst WITH PASSWORD 'secure_password_here';
-- GRANT analyst_reader TO trading_analyst;

-- Application service user
-- CREATE USER app_service WITH PASSWORD 'secure_password_here';
-- GRANT rlc_app TO app_service;

-- ============================================================================
-- Verification
-- ============================================================================
DO $$
BEGIN
    RAISE NOTICE 'Roles and grants configured:';
    RAISE NOTICE '  - rlc_admin: Full administrative access';
    RAISE NOTICE '  - ingest_writer: Write bronze/silver, read core';
    RAISE NOTICE '  - checker: Read all, write validation';
    RAISE NOTICE '  - analyst_reader: Read silver/gold only';
    RAISE NOTICE '  - rlc_app: Read all schemas';
    RAISE NOTICE '';
    RAISE NOTICE 'To create users, run:';
    RAISE NOTICE '  CREATE USER myuser WITH PASSWORD ''...'';';
    RAISE NOTICE '  GRANT <role> TO myuser;';
END $$;
