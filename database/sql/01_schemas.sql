-- ============================================================================
-- Round Lakes Commodities - Schema Creation
-- ============================================================================
-- File: 01_schemas.sql
-- Purpose: Create the layered schema architecture
-- Execute: After 00_init.sql
-- ============================================================================

-- ----------------------------------------------------------------------------
-- Drop existing schemas (CAUTION: destructive - comment out in production)
-- ----------------------------------------------------------------------------
-- Uncomment these lines only for fresh installs:
-- DROP SCHEMA IF EXISTS gold CASCADE;
-- DROP SCHEMA IF EXISTS silver CASCADE;
-- DROP SCHEMA IF EXISTS bronze CASCADE;
-- DROP SCHEMA IF EXISTS audit CASCADE;
-- DROP SCHEMA IF EXISTS core CASCADE;

-- ----------------------------------------------------------------------------
-- Core Schema: Shared dimension tables
-- ----------------------------------------------------------------------------
CREATE SCHEMA IF NOT EXISTS core;
COMMENT ON SCHEMA core IS
    'Core dimension tables: data sources, series definitions, units, locations. '
    'Shared across all layers. Read by all agents, written by admin/ingest.';

-- ----------------------------------------------------------------------------
-- Audit Schema: Ingestion tracking and validation
-- ----------------------------------------------------------------------------
CREATE SCHEMA IF NOT EXISTS audit;
COMMENT ON SCHEMA audit IS
    'Audit and operational tables: ingest runs, validation status, error logs. '
    'Written by ingest/checker agents, read by all for traceability.';

-- ----------------------------------------------------------------------------
-- Bronze Schema: Source-faithful raw data
-- ----------------------------------------------------------------------------
CREATE SCHEMA IF NOT EXISTS bronze;
COMMENT ON SCHEMA bronze IS
    'Bronze layer: Raw, source-faithful data. Preserves original structure, '
    'text values, and units exactly as received. Immutable after initial load. '
    'Used for forensics and reconciliation, not analytics.';

-- ----------------------------------------------------------------------------
-- Silver Schema: Standardized observations
-- ----------------------------------------------------------------------------
CREATE SCHEMA IF NOT EXISTS silver;
COMMENT ON SCHEMA silver IS
    'Silver layer: Cleaned, standardized time-series observations. '
    'Universal (series_id, time, value) format. Primary analytics layer. '
    'Supports revisions and quality flags.';

-- ----------------------------------------------------------------------------
-- Gold Schema: Business-ready views and materialized tables
-- ----------------------------------------------------------------------------
CREATE SCHEMA IF NOT EXISTS gold;
COMMENT ON SCHEMA gold IS
    'Gold layer: Curated business views and materialized aggregates. '
    'Excel-compatible layouts, calculated metrics, pivoted outputs. '
    'Derived from Silver. Read-only for analysts.';

-- ----------------------------------------------------------------------------
-- Set default search path
-- ----------------------------------------------------------------------------
-- This ensures unqualified table references check these schemas in order
ALTER DATABASE current_database() SET search_path TO core, silver, gold, public;

-- ----------------------------------------------------------------------------
-- Verification
-- ----------------------------------------------------------------------------
DO $$
BEGIN
    RAISE NOTICE 'Schemas created successfully:';
    RAISE NOTICE '  - core: Dimension tables';
    RAISE NOTICE '  - audit: Ingestion tracking';
    RAISE NOTICE '  - bronze: Raw source data';
    RAISE NOTICE '  - silver: Standardized observations';
    RAISE NOTICE '  - gold: Business views';
END $$;
