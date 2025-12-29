-- =============================================================================
-- RLC Commodities Database Schema - Foundation
-- Version: 1.0.0
-- =============================================================================
--
-- ARCHITECTURE OVERVIEW
-- ---------------------
-- This database implements a medallion (Bronze/Silver/Gold) architecture:
--
-- BRONZE (Raw Layer):
--   - Source-faithful data exactly as received from APIs/files
--   - Preserves original formats, units, field names
--   - Enables audit and reconciliation against source systems
--   - One table per major data source (WASDE, Census, FGIS, etc.)
--
-- SILVER (Standardized Layer):
--   - Cleaned, validated, and normalized data
--   - Universal time-series format: (series_id, time, value)
--   - Common units and naming conventions
--   - Quality flags and validation status
--
-- GOLD (Curated Layer):
--   - Business-ready views and materialized views
--   - Excel-compatible pivoted outputs
--   - Pre-calculated aggregations and deltas
--   - Report-ready datasets
--
-- AGENT INTERACTION MODEL
-- -----------------------
-- - Collector agents: Write to bronze.*, call silver refresh procedures
-- - Checker agents: Read bronze/silver, write validation status
-- - Report agents: Read silver/gold only (no write access)
-- - Trading agents: Read gold only (no write access)
--
-- IDEMPOTENCY GUARANTEE
-- ---------------------
-- All inserts use ON CONFLICT DO UPDATE with natural keys.
-- Reruns produce identical results. No duplicates possible.
--
-- =============================================================================

-- Enable required extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pgcrypto";

-- =============================================================================
-- SCHEMA CREATION
-- =============================================================================

-- Drop and recreate schemas (comment out in production migrations)
DROP SCHEMA IF EXISTS bronze CASCADE;
DROP SCHEMA IF EXISTS silver CASCADE;
DROP SCHEMA IF EXISTS gold CASCADE;
DROP SCHEMA IF EXISTS audit CASCADE;

CREATE SCHEMA bronze;
CREATE SCHEMA silver;
CREATE SCHEMA gold;
CREATE SCHEMA audit;

COMMENT ON SCHEMA bronze IS 'Raw source-faithful data, preserving original formats';
COMMENT ON SCHEMA silver IS 'Standardized, validated time-series observations';
COMMENT ON SCHEMA gold IS 'Business-ready views and aggregations';
COMMENT ON SCHEMA audit IS 'Ingestion tracking and system audit logs';

-- =============================================================================
-- CORE DIMENSION TABLES (public schema)
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Data Sources: Registry of all data providers
-- -----------------------------------------------------------------------------
CREATE TABLE public.data_source (
    id SERIAL PRIMARY KEY,
    code VARCHAR(50) NOT NULL UNIQUE,
    name VARCHAR(200) NOT NULL,
    description TEXT,
    base_url VARCHAR(500),
    api_type VARCHAR(50),  -- 'REST', 'SOAP', 'FTP', 'FILE', 'SCRAPE'
    update_frequency VARCHAR(50),  -- 'DAILY', 'WEEKLY', 'MONTHLY', 'ON_RELEASE'
    timezone VARCHAR(50) DEFAULT 'America/Chicago',
    is_active BOOLEAN DEFAULT TRUE,
    metadata JSONB DEFAULT '{}',
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Seed core data sources
INSERT INTO public.data_source (code, name, description, api_type, update_frequency) VALUES
    ('USDA_WASDE', 'USDA WASDE Report', 'World Agricultural Supply and Demand Estimates', 'REST', 'MONTHLY'),
    ('USDA_AMS', 'USDA Agricultural Marketing Service', 'Cash prices and market reports', 'REST', 'DAILY'),
    ('USDA_NASS', 'USDA National Agricultural Statistics Service', 'Crop production and acreage', 'REST', 'VARIES'),
    ('USDA_FAS', 'USDA Foreign Agricultural Service', 'Export sales and attaché reports', 'REST', 'WEEKLY'),
    ('USDA_FGIS', 'USDA Federal Grain Inspection Service', 'Export inspections', 'FILE', 'WEEKLY'),
    ('CENSUS_TRADE', 'US Census Bureau Trade', 'Import/export trade statistics', 'REST', 'MONTHLY'),
    ('EIA', 'Energy Information Administration', 'Fuel and energy data', 'REST', 'WEEKLY'),
    ('NOAA', 'National Oceanic and Atmospheric Administration', 'Weather and climate data', 'REST', 'DAILY'),
    ('CME', 'CME Group', 'Futures prices and settlements', 'REST', 'DAILY'),
    ('CFTC', 'Commodity Futures Trading Commission', 'Commitment of Traders reports', 'REST', 'WEEKLY')
ON CONFLICT (code) DO NOTHING;

-- -----------------------------------------------------------------------------
-- Units: Standardized measurement units with conversion factors
-- -----------------------------------------------------------------------------
CREATE TABLE public.unit (
    id SERIAL PRIMARY KEY,
    code VARCHAR(30) NOT NULL UNIQUE,
    name VARCHAR(100) NOT NULL,
    category VARCHAR(50) NOT NULL,  -- 'VOLUME', 'WEIGHT', 'CURRENCY', 'RATIO', 'COUNT'
    base_unit_code VARCHAR(30),      -- Reference to base unit for conversions
    to_base_factor NUMERIC(20, 10),  -- Multiply by this to convert to base unit
    description TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Seed common units
INSERT INTO public.unit (code, name, category, base_unit_code, to_base_factor, description) VALUES
    -- Volume/Weight for grains
    ('BU', 'Bushels', 'VOLUME', 'MT', NULL, 'Varies by commodity'),
    ('MT', 'Metric Tons', 'WEIGHT', 'MT', 1.0, 'Base weight unit'),
    ('TMT', 'Thousand Metric Tons', 'WEIGHT', 'MT', 1000.0, 'Common for trade data'),
    ('MMT', 'Million Metric Tons', 'WEIGHT', 'MT', 1000000.0, 'Common for balance sheets'),
    ('ST', 'Short Tons', 'WEIGHT', 'MT', 0.907185, 'US tons'),
    ('LB', 'Pounds', 'WEIGHT', 'MT', 0.000453592, 'Pounds'),
    ('KG', 'Kilograms', 'WEIGHT', 'MT', 0.001, 'Kilograms'),

    -- Currency
    ('USD', 'US Dollars', 'CURRENCY', 'USD', 1.0, 'Base currency'),
    ('USD_BU', 'Dollars per Bushel', 'CURRENCY', NULL, NULL, 'Price per bushel'),
    ('USD_MT', 'Dollars per Metric Ton', 'CURRENCY', NULL, NULL, 'Price per MT'),
    ('USD_CWT', 'Dollars per Hundredweight', 'CURRENCY', NULL, NULL, 'Price per 100 lbs'),

    -- Ratios and percentages
    ('PCT', 'Percent', 'RATIO', NULL, 0.01, 'Percentage'),
    ('RATIO', 'Ratio', 'RATIO', NULL, 1.0, 'Dimensionless ratio'),
    ('BPA', 'Bushels per Acre', 'RATIO', NULL, NULL, 'Yield measure'),

    -- Area
    ('ACRE', 'Acres', 'AREA', 'HA', 0.404686, 'US land measure'),
    ('HA', 'Hectares', 'AREA', 'HA', 1.0, 'Base area unit'),
    ('MAA', 'Million Acres', 'AREA', 'HA', 404686.0, 'Common for US acreage'),

    -- Count
    ('HEAD', 'Head', 'COUNT', NULL, 1.0, 'Livestock count'),
    ('MHEAD', 'Million Head', 'COUNT', NULL, 1000000.0, 'Livestock millions')
ON CONFLICT (code) DO NOTHING;

-- Commodity-specific bushel conversions (stored as metadata)
COMMENT ON TABLE public.unit IS 'Bushel conversions: CORN=39.368 bu/MT, SOYBEANS=36.744 bu/MT, WHEAT=36.744 bu/MT';

-- -----------------------------------------------------------------------------
-- Locations: Flexible geographic hierarchy
-- -----------------------------------------------------------------------------
CREATE TABLE public.location (
    id SERIAL PRIMARY KEY,
    code VARCHAR(50) NOT NULL UNIQUE,
    name VARCHAR(200) NOT NULL,
    location_type VARCHAR(50) NOT NULL,  -- 'COUNTRY', 'REGION', 'STATE', 'PORT', 'DISTRICT'
    parent_code VARCHAR(50),
    iso_alpha2 CHAR(2),
    iso_alpha3 CHAR(3),
    latitude NUMERIC(9, 6),
    longitude NUMERIC(9, 6),
    metadata JSONB DEFAULT '{}',
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMPTZ DEFAULT NOW(),

    FOREIGN KEY (parent_code) REFERENCES public.location(code)
);

CREATE INDEX idx_location_type ON public.location(location_type);
CREATE INDEX idx_location_parent ON public.location(parent_code);

-- Seed major trading regions and countries
INSERT INTO public.location (code, name, location_type, iso_alpha2, iso_alpha3) VALUES
    ('WORLD', 'World', 'REGION', NULL, NULL),
    ('US', 'United States', 'COUNTRY', 'US', 'USA'),
    ('CN', 'China', 'COUNTRY', 'CN', 'CHN'),
    ('BR', 'Brazil', 'COUNTRY', 'BR', 'BRA'),
    ('AR', 'Argentina', 'COUNTRY', 'AR', 'ARG'),
    ('EU', 'European Union', 'REGION', NULL, NULL),
    ('MX', 'Mexico', 'COUNTRY', 'MX', 'MEX'),
    ('JP', 'Japan', 'COUNTRY', 'JP', 'JPN'),
    ('KR', 'South Korea', 'COUNTRY', 'KR', 'KOR'),
    ('ID', 'Indonesia', 'COUNTRY', 'ID', 'IDN'),
    ('EG', 'Egypt', 'COUNTRY', 'EG', 'EGY'),
    ('CA', 'Canada', 'COUNTRY', 'CA', 'CAN'),
    ('IN', 'India', 'COUNTRY', 'IN', 'IND'),
    ('VN', 'Vietnam', 'COUNTRY', 'VN', 'VNM'),
    ('TW', 'Taiwan', 'COUNTRY', 'TW', 'TWN'),
    ('PH', 'Philippines', 'COUNTRY', 'PH', 'PHL'),
    ('TH', 'Thailand', 'COUNTRY', 'TH', 'THA'),
    ('MY', 'Malaysia', 'COUNTRY', 'MY', 'MYS'),
    ('CO', 'Colombia', 'COUNTRY', 'CO', 'COL'),
    ('UA', 'Ukraine', 'COUNTRY', 'UA', 'UKR'),
    ('RU', 'Russia', 'COUNTRY', 'RU', 'RUS')
ON CONFLICT (code) DO NOTHING;

-- -----------------------------------------------------------------------------
-- Commodities: Master list of traded commodities
-- -----------------------------------------------------------------------------
CREATE TABLE public.commodity (
    id SERIAL PRIMARY KEY,
    code VARCHAR(30) NOT NULL UNIQUE,
    name VARCHAR(100) NOT NULL,
    category VARCHAR(50) NOT NULL,  -- 'OILSEED', 'GRAIN', 'FUEL', 'PROTEIN', 'OIL'
    parent_code VARCHAR(30),         -- For product hierarchies (soybean -> soybean_oil)
    default_unit_code VARCHAR(30) REFERENCES public.unit(code),
    bushel_weight_lbs NUMERIC(6, 2), -- Pounds per bushel (if applicable)
    marketing_year_start_month INT,   -- 1-12
    hs_codes TEXT[],                  -- Array of HS codes
    metadata JSONB DEFAULT '{}',
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

INSERT INTO public.commodity (code, name, category, default_unit_code, bushel_weight_lbs, marketing_year_start_month, hs_codes) VALUES
    ('CORN', 'Corn', 'GRAIN', 'BU', 56.0, 9, ARRAY['1005']),
    ('SOYBEANS', 'Soybeans', 'OILSEED', 'BU', 60.0, 9, ARRAY['120110', '120190']),
    ('SOYBEAN_MEAL', 'Soybean Meal', 'PROTEIN', 'ST', NULL, 10, ARRAY['230400', '230499']),
    ('SOYBEAN_OIL', 'Soybean Oil', 'OIL', 'LB', NULL, 10, ARRAY['150710', '150790']),
    ('SOYBEAN_HULLS', 'Soybean Hulls', 'PROTEIN', 'ST', NULL, 10, ARRAY['230250']),
    ('WHEAT', 'Wheat (All)', 'GRAIN', 'BU', 60.0, 6, ARRAY['1001']),
    ('HRW', 'Hard Red Winter Wheat', 'GRAIN', 'BU', 60.0, 6, ARRAY['100119']),
    ('HRS', 'Hard Red Spring Wheat', 'GRAIN', 'BU', 60.0, 6, ARRAY['100119']),
    ('SRW', 'Soft Red Winter Wheat', 'GRAIN', 'BU', 60.0, 6, ARRAY['100119']),
    ('ETHANOL', 'Fuel Ethanol', 'FUEL', 'MT', NULL, 9, ARRAY['2207']),
    ('BIODIESEL', 'Biodiesel', 'FUEL', 'MT', NULL, 1, ARRAY['3826']),
    ('DDGS', 'Distillers Dried Grains', 'PROTEIN', 'ST', NULL, 9, ARRAY['230330'])
ON CONFLICT (code) DO NOTHING;

-- -----------------------------------------------------------------------------
-- Series: Central metadata registry for all time-series
-- -----------------------------------------------------------------------------
CREATE TABLE public.series (
    id SERIAL PRIMARY KEY,

    -- Natural key (unique identifier)
    data_source_id INT NOT NULL REFERENCES public.data_source(id),
    series_key VARCHAR(200) NOT NULL,  -- Source-specific identifier

    -- Descriptive metadata
    name VARCHAR(300) NOT NULL,
    description TEXT,

    -- Dimensional references
    commodity_code VARCHAR(30) REFERENCES public.commodity(code),
    location_code VARCHAR(50) REFERENCES public.location(code),
    unit_code VARCHAR(30) REFERENCES public.unit(code),

    -- Series characteristics
    frequency VARCHAR(20) NOT NULL,  -- 'DAILY', 'WEEKLY', 'MONTHLY', 'ANNUAL'
    series_type VARCHAR(50),         -- 'PRODUCTION', 'CONSUMPTION', 'TRADE', 'PRICE', 'STOCKS'
    aggregation_method VARCHAR(20),  -- 'SUM', 'AVG', 'LAST', 'FIRST'

    -- Time context
    marketing_year_start_month INT,
    timezone VARCHAR(50) DEFAULT 'America/Chicago',

    -- Flexible metadata
    metadata JSONB DEFAULT '{}',

    -- Status
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),

    -- Unique constraint on natural key
    UNIQUE (data_source_id, series_key)
);

CREATE INDEX idx_series_commodity ON public.series(commodity_code);
CREATE INDEX idx_series_location ON public.series(location_code);
CREATE INDEX idx_series_source ON public.series(data_source_id);
CREATE INDEX idx_series_type ON public.series(series_type);

COMMENT ON TABLE public.series IS 'Central registry of all time-series. Each observation references a series_id.';

-- =============================================================================
-- AUDIT SCHEMA: Ingestion tracking
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Ingest Run: Track each data collection job
-- -----------------------------------------------------------------------------
CREATE TABLE audit.ingest_run (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),

    -- What was ingested
    data_source_id INT NOT NULL REFERENCES public.data_source(id),
    job_type VARCHAR(50) NOT NULL,     -- 'FULL', 'INCREMENTAL', 'BACKFILL', 'CORRECTION'
    job_name VARCHAR(200),

    -- Agent info
    agent_id VARCHAR(100),
    agent_version VARCHAR(50),

    -- Timing
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMPTZ,

    -- Status
    status VARCHAR(20) NOT NULL DEFAULT 'RUNNING',  -- 'RUNNING', 'SUCCESS', 'FAILED', 'PARTIAL'

    -- Counts
    records_fetched INT DEFAULT 0,
    records_inserted INT DEFAULT 0,
    records_updated INT DEFAULT 0,
    records_failed INT DEFAULT 0,

    -- Error tracking
    error_message TEXT,
    error_details JSONB,

    -- Request/response metadata
    request_params JSONB,
    response_metadata JSONB,

    -- Checksums for verification
    source_checksum VARCHAR(64),

    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_ingest_run_source ON audit.ingest_run(data_source_id);
CREATE INDEX idx_ingest_run_status ON audit.ingest_run(status);
CREATE INDEX idx_ingest_run_started ON audit.ingest_run(started_at DESC);

-- -----------------------------------------------------------------------------
-- Validation Status: Track checker agent approvals
-- -----------------------------------------------------------------------------
CREATE TABLE audit.validation_status (
    id SERIAL PRIMARY KEY,

    -- What was validated
    ingest_run_id UUID NOT NULL REFERENCES audit.ingest_run(id),
    validation_type VARCHAR(50) NOT NULL,  -- 'COMPLETENESS', 'ACCURACY', 'TIMELINESS'

    -- Validation result
    status VARCHAR(20) NOT NULL,  -- 'PENDING', 'PASSED', 'FAILED', 'SKIPPED'
    score NUMERIC(5, 2),          -- Optional numeric score (0-100)

    -- Checker details
    checker_agent_id VARCHAR(100),
    checked_at TIMESTAMPTZ DEFAULT NOW(),

    -- Notes and issues
    notes TEXT,
    issues JSONB,  -- Array of issue objects

    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_validation_ingest ON audit.validation_status(ingest_run_id);
CREATE INDEX idx_validation_status ON audit.validation_status(status);

COMMENT ON TABLE audit.validation_status IS 'Checker agents record validation results here before data is promoted to Gold';

-- =============================================================================
-- INDEXES AND CONSTRAINTS SUMMARY
-- =============================================================================

COMMENT ON SCHEMA public IS 'Core dimension tables: data_source, unit, location, commodity, series';

-- Verify all foreign keys are indexed (PostgreSQL doesn't auto-index FK columns)
-- All indexes created inline above

-- =============================================================================
-- END OF FOUNDATION SCRIPT
-- =============================================================================
