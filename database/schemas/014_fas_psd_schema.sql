-- =============================================================================
-- FAS PSD & Export Sales Schema
-- USDA Foreign Agricultural Service Data Tables
-- =============================================================================
-- Created: 2026-01-29
-- Purpose: Store FAS Export Sales Report (ESR) and Production, Supply,
--          Distribution (PSD) data for global commodity S&D tracking
-- =============================================================================

-- Ensure bronze schema exists
CREATE SCHEMA IF NOT EXISTS bronze;

-- =============================================================================
-- FAS Export Sales Report (Weekly)
-- Source: https://api.fas.usda.gov/api/esr/exports/
-- =============================================================================
CREATE TABLE IF NOT EXISTS bronze.fas_export_sales (
    id BIGSERIAL PRIMARY KEY,

    -- Commodity identification
    commodity VARCHAR(50) NOT NULL,          -- corn, soybeans, wheat, etc.
    commodity_code INT NOT NULL,             -- ESR code (401=corn, 801=soybeans)

    -- Destination
    country VARCHAR(100),
    country_code VARCHAR(10),
    region VARCHAR(100),

    -- Time period
    marketing_year INT NOT NULL,
    week_ending DATE NOT NULL,

    -- Export sales data (MT)
    weekly_exports NUMERIC(18,2),            -- Exports shipped this week
    accumulated_exports NUMERIC(18,2),       -- Cumulative MY exports
    outstanding_sales NUMERIC(18,2),         -- Unshipped commitments
    gross_new_sales NUMERIC(18,2),           -- New sales booked
    net_sales NUMERIC(18,2),                 -- Net after cancellations

    -- Previous year comparison
    prev_my_accumulated NUMERIC(18,2),       -- Prior MY accumulated exports

    -- Metadata
    unit VARCHAR(20) DEFAULT 'MT',
    collected_at TIMESTAMPTZ DEFAULT NOW(),
    ingest_run_id UUID,

    -- Natural key for upserts
    UNIQUE (commodity_code, country_code, marketing_year, week_ending)
);

-- Indexes for common query patterns
CREATE INDEX IF NOT EXISTS idx_fas_esr_commodity
    ON bronze.fas_export_sales(commodity, marketing_year, week_ending DESC);
CREATE INDEX IF NOT EXISTS idx_fas_esr_country
    ON bronze.fas_export_sales(country_code, marketing_year);
CREATE INDEX IF NOT EXISTS idx_fas_esr_collected
    ON bronze.fas_export_sales(collected_at DESC);

COMMENT ON TABLE bronze.fas_export_sales IS
    'Weekly USDA FAS Export Sales Report data by commodity and destination country';


-- =============================================================================
-- FAS PSD Balance Sheet (Production, Supply, Distribution)
-- Source: https://api.fas.usda.gov/api/psd/
-- =============================================================================
CREATE TABLE IF NOT EXISTS bronze.fas_psd (
    id BIGSERIAL PRIMARY KEY,

    -- Commodity identification
    commodity VARCHAR(50) NOT NULL,          -- corn, soybeans, wheat, etc.
    commodity_code VARCHAR(20) NOT NULL,     -- PSD code (0440000=corn)

    -- Location
    country VARCHAR(100) NOT NULL,
    country_code VARCHAR(10) NOT NULL,       -- US, BR, AR, CH, etc.

    -- Time period
    marketing_year INT NOT NULL,             -- e.g., 2025 for MY 2024/25
    calendar_year INT,                       -- Calendar year of data
    month INT,                               -- Month of estimate (1-12)
    report_date DATE DEFAULT CURRENT_DATE,   -- Date of USDA report

    -- SUPPLY SIDE (1000 MT unless noted)
    area_planted NUMERIC(18,2),              -- 1000 HA
    area_harvested NUMERIC(18,2),            -- 1000 HA
    yield NUMERIC(18,4),                     -- MT/HA
    beginning_stocks NUMERIC(18,2),
    production NUMERIC(18,2),
    imports NUMERIC(18,2),
    total_supply NUMERIC(18,2),

    -- DEMAND SIDE (1000 MT)
    feed_dom_consumption NUMERIC(18,2),      -- Feed & residual use
    fsi_consumption NUMERIC(18,2),           -- Food/Seed/Industrial
    crush NUMERIC(18,2),                     -- Oilseed crush
    domestic_consumption NUMERIC(18,2),      -- Total domestic use
    exports NUMERIC(18,2),
    total_distribution NUMERIC(18,2),        -- Total use/disappearance

    -- ENDING POSITION
    ending_stocks NUMERIC(18,2),

    -- Trade year data (if different from MY)
    ty_imports NUMERIC(18,2),
    ty_exports NUMERIC(18,2),

    -- Metadata
    unit VARCHAR(20) DEFAULT '1000 MT',
    collected_at TIMESTAMPTZ DEFAULT NOW(),
    ingest_run_id UUID,

    -- Natural key: one record per commodity/country/MY/month/report
    UNIQUE (commodity_code, country_code, marketing_year, month, report_date)
);

-- Indexes for common query patterns
CREATE INDEX IF NOT EXISTS idx_fas_psd_commodity
    ON bronze.fas_psd(commodity, country_code, marketing_year DESC);
CREATE INDEX IF NOT EXISTS idx_fas_psd_country
    ON bronze.fas_psd(country_code, marketing_year DESC);
CREATE INDEX IF NOT EXISTS idx_fas_psd_report_date
    ON bronze.fas_psd(report_date DESC);
CREATE INDEX IF NOT EXISTS idx_fas_psd_collected
    ON bronze.fas_psd(collected_at DESC);

COMMENT ON TABLE bronze.fas_psd IS
    'USDA FAS Production, Supply & Distribution balance sheet data by commodity and country';


-- =============================================================================
-- Reference Tables for FAS Codes
-- =============================================================================

-- ESR Commodity Codes
CREATE TABLE IF NOT EXISTS bronze.fas_esr_commodity_ref (
    code INT PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    unit VARCHAR(20) DEFAULT 'MT'
);

INSERT INTO bronze.fas_esr_commodity_ref (code, name, unit) VALUES
    (401, 'Corn', 'MT'),
    (801, 'Soybeans', 'MT'),
    (107, 'All Wheat', 'MT'),
    (101, 'Wheat HRW', 'MT'),
    (102, 'Wheat SRW', 'MT'),
    (103, 'Wheat HRS', 'MT'),
    (104, 'Wheat White', 'MT'),
    (901, 'Soybean Meal', 'MT'),
    (902, 'Soybean Oil', 'MT'),
    (701, 'Sorghum', 'MT'),
    (1404, 'All Upland Cotton', '480LB BALES'),
    (1505, 'All Rice', 'MT')
ON CONFLICT (code) DO NOTHING;

-- PSD Commodity Codes
CREATE TABLE IF NOT EXISTS bronze.fas_psd_commodity_ref (
    code VARCHAR(20) PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    unit VARCHAR(20) DEFAULT '1000 MT'
);

INSERT INTO bronze.fas_psd_commodity_ref (code, name, unit) VALUES
    ('0440000', 'Corn', '1000 MT'),
    ('2222000', 'Soybeans', '1000 MT'),
    ('0410000', 'Wheat', '1000 MT'),
    ('0813100', 'Soybean Meal', '1000 MT'),
    ('4232000', 'Soybean Oil', '1000 MT'),
    ('0459200', 'Sorghum', '1000 MT'),
    ('2631000', 'Cotton', '1000 480LB BALES'),
    ('0422110', 'Rice Milled', '1000 MT')
ON CONFLICT (code) DO NOTHING;

-- Country Codes
CREATE TABLE IF NOT EXISTS bronze.fas_country_ref (
    code VARCHAR(10) PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    region VARCHAR(50)
);

INSERT INTO bronze.fas_country_ref (code, name, region) VALUES
    ('US', 'United States', 'North America'),
    ('BR', 'Brazil', 'South America'),
    ('AR', 'Argentina', 'South America'),
    ('CH', 'China', 'Asia'),
    ('IN', 'India', 'Asia'),
    ('E4', 'European Union', 'Europe'),
    ('RS', 'Russia', 'Europe'),
    ('UP', 'Ukraine', 'Europe'),
    ('AS', 'Australia', 'Oceania'),
    ('CA', 'Canada', 'North America'),
    ('MX', 'Mexico', 'North America'),
    ('JA', 'Japan', 'Asia'),
    ('KS', 'Korea, South', 'Asia'),
    ('MY', 'Malaysia', 'Asia'),
    ('ID', 'Indonesia', 'Asia')
ON CONFLICT (code) DO NOTHING;


-- =============================================================================
-- Grants
-- =============================================================================
GRANT SELECT ON bronze.fas_export_sales TO readonly_role;
GRANT SELECT ON bronze.fas_psd TO readonly_role;
GRANT SELECT ON bronze.fas_esr_commodity_ref TO readonly_role;
GRANT SELECT ON bronze.fas_psd_commodity_ref TO readonly_role;
GRANT SELECT ON bronze.fas_country_ref TO readonly_role;

GRANT ALL ON bronze.fas_export_sales TO collector_role;
GRANT ALL ON bronze.fas_psd TO collector_role;
