-- =============================================================================
-- RLC Commodities Database Schema - Roles and Security
-- Version: 1.0.0
-- =============================================================================
--
-- ROLE-BASED ACCESS CONTROL
-- -------------------------
-- Implements principle of least privilege:
--
-- ingest_writer:  Collector agents - write bronze, call silver procedures
-- checker:        Validation agents - read bronze/silver, write validation status
-- analyst_reader: Report/trading agents - read silver/gold only
-- admin:          Full access for maintenance
--
-- =============================================================================

-- =============================================================================
-- CREATE ROLES
-- =============================================================================

-- Drop existing roles if recreating (comment out in production)
-- DROP ROLE IF EXISTS ingest_writer;
-- DROP ROLE IF EXISTS checker;
-- DROP ROLE IF EXISTS analyst_reader;
-- DROP ROLE IF EXISTS rlc_admin;

-- -----------------------------------------------------------------------------
-- Ingest Writer: For data collection agents
-- -----------------------------------------------------------------------------
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'ingest_writer') THEN
        CREATE ROLE ingest_writer WITH LOGIN PASSWORD 'change_me_ingest_2025';
    END IF;
END
$$;

COMMENT ON ROLE ingest_writer IS 'Data collection agents: write bronze, audit, call silver functions';

-- -----------------------------------------------------------------------------
-- Checker: For validation agents
-- -----------------------------------------------------------------------------
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'checker') THEN
        CREATE ROLE checker WITH LOGIN PASSWORD 'change_me_checker_2025';
    END IF;
END
$$;

COMMENT ON ROLE checker IS 'Validation agents: read bronze/silver, write validation_status';

-- -----------------------------------------------------------------------------
-- Analyst Reader: For report-writing and trading agents
-- -----------------------------------------------------------------------------
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'analyst_reader') THEN
        CREATE ROLE analyst_reader WITH LOGIN PASSWORD 'change_me_analyst_2025';
    END IF;
END
$$;

COMMENT ON ROLE analyst_reader IS 'Report and trading agents: read-only access to silver/gold';

-- -----------------------------------------------------------------------------
-- Admin: Full access for maintenance
-- -----------------------------------------------------------------------------
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'rlc_admin') THEN
        CREATE ROLE rlc_admin WITH LOGIN PASSWORD 'change_me_admin_2025' SUPERUSER;
    END IF;
END
$$;

-- =============================================================================
-- SCHEMA PERMISSIONS
-- =============================================================================

-- Revoke default public access
REVOKE ALL ON SCHEMA public FROM PUBLIC;
REVOKE ALL ON SCHEMA bronze FROM PUBLIC;
REVOKE ALL ON SCHEMA silver FROM PUBLIC;
REVOKE ALL ON SCHEMA gold FROM PUBLIC;
REVOKE ALL ON SCHEMA audit FROM PUBLIC;

-- -----------------------------------------------------------------------------
-- Ingest Writer Permissions
-- -----------------------------------------------------------------------------
GRANT USAGE ON SCHEMA public TO ingest_writer;
GRANT USAGE ON SCHEMA bronze TO ingest_writer;
GRANT USAGE ON SCHEMA silver TO ingest_writer;
GRANT USAGE ON SCHEMA audit TO ingest_writer;

-- Public schema: read dimensions, manage series
GRANT SELECT ON ALL TABLES IN SCHEMA public TO ingest_writer;
GRANT INSERT, UPDATE ON public.series TO ingest_writer;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO ingest_writer;

-- Bronze: full write access
GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA bronze TO ingest_writer;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA bronze TO ingest_writer;

-- Silver: write observations and trade data
GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA silver TO ingest_writer;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA silver TO ingest_writer;

-- Audit: write ingest_run
GRANT SELECT, INSERT, UPDATE ON audit.ingest_run TO ingest_writer;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA audit TO ingest_writer;

-- Functions
GRANT EXECUTE ON FUNCTION public.get_or_create_series TO ingest_writer;
GRANT EXECUTE ON FUNCTION silver.upsert_observation TO ingest_writer;
GRANT EXECUTE ON FUNCTION audit.start_ingest_run TO ingest_writer;
GRANT EXECUTE ON FUNCTION audit.complete_ingest_run TO ingest_writer;

-- -----------------------------------------------------------------------------
-- Checker Permissions
-- -----------------------------------------------------------------------------
GRANT USAGE ON SCHEMA public TO checker;
GRANT USAGE ON SCHEMA bronze TO checker;
GRANT USAGE ON SCHEMA silver TO checker;
GRANT USAGE ON SCHEMA audit TO checker;

-- Read access to all data
GRANT SELECT ON ALL TABLES IN SCHEMA public TO checker;
GRANT SELECT ON ALL TABLES IN SCHEMA bronze TO checker;
GRANT SELECT ON ALL TABLES IN SCHEMA silver TO checker;
GRANT SELECT ON audit.ingest_run TO checker;

-- Write validation status only
GRANT SELECT, INSERT, UPDATE ON audit.validation_status TO checker;
GRANT USAGE, SELECT ON SEQUENCE audit.validation_status_id_seq TO checker;

-- -----------------------------------------------------------------------------
-- Analyst Reader Permissions (most restrictive)
-- -----------------------------------------------------------------------------
GRANT USAGE ON SCHEMA public TO analyst_reader;
GRANT USAGE ON SCHEMA silver TO analyst_reader;
GRANT USAGE ON SCHEMA gold TO analyst_reader;

-- Read-only access to dimensions and curated data
GRANT SELECT ON ALL TABLES IN SCHEMA public TO analyst_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA silver TO analyst_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA gold TO analyst_reader;

-- No access to bronze (raw data) or audit (internal)

-- =============================================================================
-- DEFAULT PRIVILEGES FOR FUTURE OBJECTS
-- =============================================================================

-- Ensure new tables get appropriate permissions

ALTER DEFAULT PRIVILEGES IN SCHEMA bronze
    GRANT SELECT, INSERT, UPDATE ON TABLES TO ingest_writer;

ALTER DEFAULT PRIVILEGES IN SCHEMA silver
    GRANT SELECT, INSERT, UPDATE ON TABLES TO ingest_writer;

ALTER DEFAULT PRIVILEGES IN SCHEMA silver
    GRANT SELECT ON TABLES TO analyst_reader;

ALTER DEFAULT PRIVILEGES IN SCHEMA gold
    GRANT SELECT ON TABLES TO analyst_reader;

ALTER DEFAULT PRIVILEGES IN SCHEMA bronze
    GRANT USAGE, SELECT ON SEQUENCES TO ingest_writer;

ALTER DEFAULT PRIVILEGES IN SCHEMA silver
    GRANT USAGE, SELECT ON SEQUENCES TO ingest_writer;

-- =============================================================================
-- ROW-LEVEL SECURITY (Optional - for multi-tenant scenarios)
-- =============================================================================

-- If you need to restrict access by commodity or data source:
--
-- ALTER TABLE silver.observation ENABLE ROW LEVEL SECURITY;
--
-- CREATE POLICY analyst_commodity_access ON silver.observation
--     FOR SELECT TO analyst_reader
--     USING (
--         series_id IN (
--             SELECT id FROM public.series
--             WHERE commodity_code IN ('SOYBEANS', 'CORN', 'WHEAT')
--         )
--     );
--
-- This is optional and typically not needed for a single-company setup.

-- =============================================================================
-- CONNECTION LIMITS
-- =============================================================================

-- Limit connections per role to prevent runaway agents
ALTER ROLE ingest_writer CONNECTION LIMIT 5;
ALTER ROLE checker CONNECTION LIMIT 3;
ALTER ROLE analyst_reader CONNECTION LIMIT 10;

-- =============================================================================
-- PASSWORD POLICY REMINDER
-- =============================================================================

-- In production, use strong passwords and consider:
-- 1. Store passwords in environment variables or secrets manager
-- 2. Use SSL/TLS for connections (ssl = on in postgresql.conf)
-- 3. Rotate passwords periodically
-- 4. Use certificate-based authentication for agents

-- Example connection string format:
-- postgresql://ingest_writer:password@localhost:5432/rlc_commodities?sslmode=require

-- =============================================================================
-- VERIFICATION QUERIES
-- =============================================================================

-- Check role permissions:
--
-- SELECT
--     grantee,
--     table_schema,
--     table_name,
--     privilege_type
-- FROM information_schema.table_privileges
-- WHERE grantee IN ('ingest_writer', 'checker', 'analyst_reader')
-- ORDER BY grantee, table_schema, table_name;

-- Check role attributes:
--
-- SELECT
--     rolname,
--     rolsuper,
--     rolinherit,
--     rolcreaterole,
--     rolcreatedb,
--     rolcanlogin,
--     rolconnlimit
-- FROM pg_roles
-- WHERE rolname IN ('ingest_writer', 'checker', 'analyst_reader', 'rlc_admin');

-- =============================================================================
-- END OF ROLES AND SECURITY SCRIPT
-- =============================================================================
