-- =============================================================================
-- EPA RFS (Renewable Fuel Standard) Schema
-- =============================================================================
--
-- Create reference schema if not exists
CREATE SCHEMA IF NOT EXISTS reference;
--
-- Complete schema for EPA EMTS (EPA Moderated Transaction System) data:
-- - RIN Generation by D-code (annual and monthly)
-- - Fuel Production by fuel type
-- - RIN Separation by reason
-- - RIN Retirement by reason
-- - Available RINs inventory
--
-- Data sources: https://www.epa.gov/fuels-registration-reporting-and-compliance-help/public-data-renewable-fuel-standard
-- Data starts: July 2010 (EMTS launch)
-- =============================================================================

-- =============================================================================
-- BRONZE LAYER - Raw EPA RFS Data
-- =============================================================================

-- -----------------------------------------------------------------------------
-- EPA RFS Generation Breakout (Annual)
-- Source: generationbreakout_[mon][year].csv
-- Annual RIN generation by D-code and producer type
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS bronze.epa_rfs_generation (
    id BIGSERIAL PRIMARY KEY,

    -- Natural key
    rin_year INT NOT NULL,
    d_code VARCHAR(2) NOT NULL,  -- '3', '4', '5', '6', '7' (D-codes)

    -- Generation volumes (in RINs, not gallons)
    domestic_rins BIGINT,          -- RINs from domestic producers
    importer_rins BIGINT,          -- RINs from importers
    foreign_generation_rins BIGINT, -- RINs from foreign generators
    total_rins BIGINT,             -- Total RINs generated

    -- Metadata
    source_file VARCHAR(200),      -- Original filename
    file_month VARCHAR(20),        -- Month of the file (e.g., 'dec2025')
    collected_at TIMESTAMPTZ DEFAULT NOW(),

    -- Natural key constraint
    UNIQUE (rin_year, d_code)
);

CREATE INDEX IF NOT EXISTS idx_epa_rfs_gen_year ON bronze.epa_rfs_generation(rin_year);
CREATE INDEX IF NOT EXISTS idx_epa_rfs_gen_dcode ON bronze.epa_rfs_generation(d_code);

COMMENT ON TABLE bronze.epa_rfs_generation IS
    'EPA RFS RIN generation by year and D-code. Source: EPA EMTS generationbreakout CSV files.';

-- -----------------------------------------------------------------------------
-- EPA RFS Monthly RIN Data
-- Source: rindata_[mon][year].csv
-- Monthly RIN generation - essential for time series analysis
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS bronze.epa_rfs_rin_monthly (
    id BIGSERIAL PRIMARY KEY,

    -- Natural key
    rin_year INT NOT NULL,
    production_month INT NOT NULL,  -- 1-12
    d_code VARCHAR(2) NOT NULL,

    -- Volumes
    rin_quantity BIGINT,           -- RINs generated
    batch_volume BIGINT,           -- Fuel volume in gallons

    -- Metadata
    source_file VARCHAR(200),
    collected_at TIMESTAMPTZ DEFAULT NOW(),

    UNIQUE (rin_year, production_month, d_code)
);

CREATE INDEX IF NOT EXISTS idx_epa_rfs_monthly_ym ON bronze.epa_rfs_rin_monthly(rin_year, production_month);
CREATE INDEX IF NOT EXISTS idx_epa_rfs_monthly_dcode ON bronze.epa_rfs_rin_monthly(d_code);

COMMENT ON TABLE bronze.epa_rfs_rin_monthly IS
    'Monthly RIN generation by D-code. Source: EPA EMTS rindata CSV files. Key for seasonal analysis.';

-- -----------------------------------------------------------------------------
-- EPA RFS Fuel Production by Type
-- Source: fuelproduction_[mon][year].csv
-- Detailed breakdown by specific fuel category (biodiesel, RD, SAF, etc.)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS bronze.epa_rfs_fuel_production (
    id BIGSERIAL PRIMARY KEY,

    -- Natural key
    rin_year INT NOT NULL,
    d_code VARCHAR(2) NOT NULL,
    fuel_category_code VARCHAR(20) NOT NULL,

    -- Fuel details
    fuel_name VARCHAR(100),        -- e.g., 'Biomass-Based Diesel'
    fuel_category VARCHAR(200),    -- e.g., 'Non-ester Renewable Diesel (EV 1.7)'

    -- Volumes
    rin_quantity BIGINT,
    batch_volume BIGINT,

    -- Metadata
    source_file VARCHAR(200),
    collected_at TIMESTAMPTZ DEFAULT NOW(),

    UNIQUE (rin_year, d_code, fuel_category_code)
);

CREATE INDEX IF NOT EXISTS idx_epa_rfs_fuel_year ON bronze.epa_rfs_fuel_production(rin_year);
CREATE INDEX IF NOT EXISTS idx_epa_rfs_fuel_dcode ON bronze.epa_rfs_fuel_production(d_code);
CREATE INDEX IF NOT EXISTS idx_epa_rfs_fuel_cat ON bronze.epa_rfs_fuel_production(fuel_category_code);

COMMENT ON TABLE bronze.epa_rfs_fuel_production IS
    'Fuel production by type. Shows breakdown of D4 into biodiesel vs renewable diesel vs SAF.';

-- -----------------------------------------------------------------------------
-- EPA RFS Available RINs
-- Source: availablerins_[mon][year].csv
-- RIN inventory by year, D-code, and assignment status
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS bronze.epa_rfs_available (
    id BIGSERIAL PRIMARY KEY,

    -- Natural key
    rin_year INT NOT NULL,
    d_code VARCHAR(2) NOT NULL,
    assignment VARCHAR(20) NOT NULL,  -- 'Separated' or 'Assigned'

    -- Volumes
    total_generated BIGINT,
    total_retired BIGINT,
    total_locked BIGINT,
    total_available BIGINT,

    -- Metadata
    source_file VARCHAR(200),
    collected_at TIMESTAMPTZ DEFAULT NOW(),

    UNIQUE (rin_year, d_code, assignment)
);

CREATE INDEX IF NOT EXISTS idx_epa_rfs_avail_year ON bronze.epa_rfs_available(rin_year);
CREATE INDEX IF NOT EXISTS idx_epa_rfs_avail_dcode ON bronze.epa_rfs_available(d_code);

COMMENT ON TABLE bronze.epa_rfs_available IS
    'Available RIN inventory. Separated RINs can be traded; Assigned RINs attached to fuel batches.';

-- -----------------------------------------------------------------------------
-- EPA RFS Retirement Transactions
-- Source: retiretransaction_[mon][year].csv
-- RIN retirements by reason code
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS bronze.epa_rfs_retirement (
    id BIGSERIAL PRIMARY KEY,

    -- Natural key
    rin_year INT NOT NULL,
    d_code VARCHAR(2) NOT NULL,
    retire_reason_code VARCHAR(10) NOT NULL,

    -- Details
    d_code_description VARCHAR(100),
    retire_reason_description VARCHAR(500),

    -- Volumes
    rin_quantity BIGINT,

    -- Metadata
    source_file VARCHAR(200),
    collected_at TIMESTAMPTZ DEFAULT NOW(),

    UNIQUE (rin_year, d_code, retire_reason_code)
);

CREATE INDEX IF NOT EXISTS idx_epa_rfs_retire_year ON bronze.epa_rfs_retirement(rin_year);
CREATE INDEX IF NOT EXISTS idx_epa_rfs_retire_dcode ON bronze.epa_rfs_retirement(d_code);
CREATE INDEX IF NOT EXISTS idx_epa_rfs_retire_reason ON bronze.epa_rfs_retirement(retire_reason_code);

COMMENT ON TABLE bronze.epa_rfs_retirement IS
    'RIN retirements by reason. Code 80 = compliance; 50 = invalid; 70 = enforcement.';

-- -----------------------------------------------------------------------------
-- EPA RFS Separation Transactions
-- Source: separatetransaction_[mon][year].csv
-- RIN separations by reason code
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS bronze.epa_rfs_separation (
    id BIGSERIAL PRIMARY KEY,

    -- Natural key
    rin_year INT NOT NULL,
    d_code VARCHAR(2) NOT NULL,
    separation_reason_code VARCHAR(10) NOT NULL,

    -- Details
    d_code_description VARCHAR(100),
    separation_reason_description VARCHAR(500),

    -- Volumes
    rin_quantity BIGINT,

    -- Metadata
    source_file VARCHAR(200),
    collected_at TIMESTAMPTZ DEFAULT NOW(),

    UNIQUE (rin_year, d_code, separation_reason_code)
);

CREATE INDEX IF NOT EXISTS idx_epa_rfs_sep_year ON bronze.epa_rfs_separation(rin_year);
CREATE INDEX IF NOT EXISTS idx_epa_rfs_sep_dcode ON bronze.epa_rfs_separation(d_code);
CREATE INDEX IF NOT EXISTS idx_epa_rfs_sep_reason ON bronze.epa_rfs_separation(separation_reason_code);

COMMENT ON TABLE bronze.epa_rfs_separation IS
    'RIN separations by reason. Code 10 = obligated party; 20 = blending; 50 = export.';

-- =============================================================================
-- SILVER LAYER - Cleaned EPA RFS Data
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Silver RIN Generation Summary (Annual)
-- Cleaned and validated generation data with YoY calculations
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS silver.epa_rfs_generation (
    id BIGSERIAL PRIMARY KEY,

    -- Dimensions
    year INT NOT NULL,
    d_code VARCHAR(2) NOT NULL,
    d_code_name VARCHAR(50),  -- 'Cellulosic Biofuel', 'Biomass-Based Diesel', etc.

    -- Volumes
    total_rins BIGINT NOT NULL,
    domestic_rins BIGINT,
    import_rins BIGINT,
    foreign_rins BIGINT,

    -- Calculated fields
    equivalent_gallons NUMERIC(18, 2),  -- total_rins / equivalence_value
    equivalence_value NUMERIC(4, 2),     -- EV used for calculation

    -- Year-over-year change
    yoy_change_rins BIGINT,
    yoy_change_pct NUMERIC(8, 2),

    -- Metadata
    source_bronze_id BIGINT,
    transformed_at TIMESTAMPTZ DEFAULT NOW(),

    UNIQUE (year, d_code)
);

COMMENT ON TABLE silver.epa_rfs_generation IS
    'Cleaned EPA RFS generation data with D-code names and calculated equivalent gallons.';

-- -----------------------------------------------------------------------------
-- Silver Monthly RIN Generation
-- Monthly time series with rolling calculations
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS silver.epa_rfs_monthly (
    id BIGSERIAL PRIMARY KEY,

    -- Dimensions
    year INT NOT NULL,
    month INT NOT NULL,
    d_code VARCHAR(2) NOT NULL,
    d_code_name VARCHAR(50),

    -- Volumes
    rin_quantity BIGINT NOT NULL,
    batch_volume BIGINT,
    equivalent_gallons NUMERIC(18, 2),

    -- Rolling calculations
    rin_quantity_ytd BIGINT,           -- Year-to-date
    rin_quantity_rolling_12m BIGINT,   -- Trailing 12 months
    mom_change_pct NUMERIC(8, 2),      -- Month-over-month
    yoy_change_pct NUMERIC(8, 2),      -- Year-over-year (same month)

    -- Metadata
    source_bronze_id BIGINT,
    transformed_at TIMESTAMPTZ DEFAULT NOW(),

    UNIQUE (year, month, d_code)
);

CREATE INDEX IF NOT EXISTS idx_silver_rfs_monthly_ym ON silver.epa_rfs_monthly(year, month);

COMMENT ON TABLE silver.epa_rfs_monthly IS
    'Monthly RIN generation with rolling calculations for trend analysis.';

-- =============================================================================
-- GOLD LAYER - Analytics Views
-- =============================================================================

-- -----------------------------------------------------------------------------
-- RIN Generation Summary by Category
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW gold.rin_generation_summary AS
SELECT
    rin_year as year,
    SUM(CASE WHEN d_code = '3' THEN total_rins ELSE 0 END) as d3_cellulosic,
    SUM(CASE WHEN d_code = '4' THEN total_rins ELSE 0 END) as d4_biodiesel,
    SUM(CASE WHEN d_code = '5' THEN total_rins ELSE 0 END) as d5_advanced,
    SUM(CASE WHEN d_code = '6' THEN total_rins ELSE 0 END) as d6_renewable,
    SUM(CASE WHEN d_code = '7' THEN total_rins ELSE 0 END) as d7_cellulosic_diesel,
    SUM(total_rins) as total_all_rins,
    -- Advanced = D3 + D4 + D5 + D7
    SUM(CASE WHEN d_code IN ('3', '4', '5', '7') THEN total_rins ELSE 0 END) as total_advanced
FROM bronze.epa_rfs_generation
GROUP BY rin_year
ORDER BY rin_year DESC;

COMMENT ON VIEW gold.rin_generation_summary IS
    'Annual RIN generation totals by D-code category with advanced biofuel subtotal.';

-- -----------------------------------------------------------------------------
-- D4 BBD Trend - Critical for soybean oil demand
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW gold.d4_bbd_trend AS
SELECT
    rin_year as year,
    total_rins as d4_rins,
    ROUND(total_rins / 1.6, 0) as approx_gallons,
    domestic_rins,
    importer_rins + foreign_generation_rins as import_and_foreign,
    ROUND(100.0 * domestic_rins / NULLIF(total_rins, 0), 1) as domestic_pct,
    LAG(total_rins) OVER (ORDER BY rin_year) as prev_year_rins,
    ROUND(100.0 * (total_rins - LAG(total_rins) OVER (ORDER BY rin_year))
          / NULLIF(LAG(total_rins) OVER (ORDER BY rin_year), 0), 1) as yoy_growth_pct
FROM bronze.epa_rfs_generation
WHERE d_code = '4'
ORDER BY rin_year DESC;

COMMENT ON VIEW gold.d4_bbd_trend IS
    'D4 Biomass-Based Diesel trend - key driver of soybean oil demand.';

-- -----------------------------------------------------------------------------
-- D6 Conventional Ethanol Trend - Corn demand indicator
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW gold.d6_ethanol_trend AS
SELECT
    rin_year as year,
    total_rins as d6_rins,
    total_rins as approx_gallons,  -- D6 EV = 1.0
    domestic_rins,
    ROUND(100.0 * domestic_rins / NULLIF(total_rins, 0), 1) as domestic_pct,
    LAG(total_rins) OVER (ORDER BY rin_year) as prev_year_rins,
    ROUND(100.0 * (total_rins - LAG(total_rins) OVER (ORDER BY rin_year))
          / NULLIF(LAG(total_rins) OVER (ORDER BY rin_year), 0), 1) as yoy_growth_pct
FROM bronze.epa_rfs_generation
WHERE d_code = '6'
ORDER BY rin_year DESC;

COMMENT ON VIEW gold.d6_ethanol_trend IS
    'D6 Renewable Fuel (corn ethanol) trend - key driver of corn demand.';

-- -----------------------------------------------------------------------------
-- Monthly RIN Generation Trend
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW gold.rin_monthly_trend AS
SELECT
    rin_year as year,
    production_month as month,
    TO_CHAR(TO_DATE(production_month::text, 'MM'), 'Mon') as month_name,
    SUM(CASE WHEN d_code = '3' THEN rin_quantity ELSE 0 END) as d3_rins,
    SUM(CASE WHEN d_code = '4' THEN rin_quantity ELSE 0 END) as d4_rins,
    SUM(CASE WHEN d_code = '5' THEN rin_quantity ELSE 0 END) as d5_rins,
    SUM(CASE WHEN d_code = '6' THEN rin_quantity ELSE 0 END) as d6_rins,
    SUM(rin_quantity) as total_rins
FROM bronze.epa_rfs_rin_monthly
GROUP BY rin_year, production_month
ORDER BY rin_year DESC, production_month DESC;

COMMENT ON VIEW gold.rin_monthly_trend IS
    'Monthly RIN generation by D-code for seasonal and trend analysis.';

-- -----------------------------------------------------------------------------
-- D4 Fuel Type Breakdown - Biodiesel vs Renewable Diesel vs SAF
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW gold.d4_fuel_breakdown AS
SELECT
    rin_year as year,
    fuel_category,
    fuel_category_code,
    rin_quantity,
    batch_volume,
    ROUND(100.0 * rin_quantity / SUM(rin_quantity) OVER (PARTITION BY rin_year), 1) as pct_of_d4
FROM bronze.epa_rfs_fuel_production
WHERE d_code = '4'
ORDER BY rin_year DESC, rin_quantity DESC;

COMMENT ON VIEW gold.d4_fuel_breakdown IS
    'D4 breakdown by fuel type showing shift from biodiesel to renewable diesel.';

-- -----------------------------------------------------------------------------
-- RIN Compliance Summary
-- Shows compliance retirements vs other retirement reasons
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW gold.rin_compliance_summary AS
SELECT
    rin_year as year,
    d_code,
    SUM(CASE WHEN retire_reason_code = '80' THEN rin_quantity ELSE 0 END) as compliance_retirements,
    SUM(CASE WHEN retire_reason_code = '50' THEN rin_quantity ELSE 0 END) as invalid_rins,
    SUM(CASE WHEN retire_reason_code = '70' THEN rin_quantity ELSE 0 END) as enforcement,
    SUM(CASE WHEN retire_reason_code NOT IN ('80', '50', '70') THEN rin_quantity ELSE 0 END) as other,
    SUM(rin_quantity) as total_retired
FROM bronze.epa_rfs_retirement
GROUP BY rin_year, d_code
ORDER BY rin_year DESC, d_code;

COMMENT ON VIEW gold.rin_compliance_summary IS
    'Retirement summary showing compliance vs invalid vs enforcement retirements.';

-- -----------------------------------------------------------------------------
-- Available RINs Supply
-- Current RIN inventory for supply analysis
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW gold.rin_supply AS
SELECT
    rin_year as year,
    d_code,
    SUM(CASE WHEN assignment = 'Separated' THEN total_available ELSE 0 END) as separated_available,
    SUM(CASE WHEN assignment = 'Assigned' THEN total_available ELSE 0 END) as assigned_available,
    SUM(total_available) as total_available,
    SUM(total_locked) as total_locked,
    SUM(total_retired) as total_retired
FROM bronze.epa_rfs_available
GROUP BY rin_year, d_code
ORDER BY rin_year DESC, d_code;

COMMENT ON VIEW gold.rin_supply IS
    'Available RIN supply. Separated RINs are tradeable; Assigned attached to fuel.';

-- =============================================================================
-- D-CODE REFERENCE TABLE
-- =============================================================================
CREATE TABLE IF NOT EXISTS reference.epa_d_codes (
    d_code VARCHAR(2) PRIMARY KEY,
    name VARCHAR(50) NOT NULL,
    description TEXT,
    ghg_threshold VARCHAR(20),
    typical_equivalence_value NUMERIC(4, 2),
    satisfies_rvos TEXT[],
    common_fuels TEXT[],
    common_feedstocks TEXT[]
);

INSERT INTO reference.epa_d_codes (d_code, name, ghg_threshold, typical_equivalence_value, satisfies_rvos, common_fuels)
VALUES
    ('3', 'Cellulosic Biofuel', '>=60%', 1.0,
     ARRAY['cellulosic', 'advanced', 'total'],
     ARRAY['Cellulosic ethanol', 'RNG from landfills', 'Biogas']),
    ('4', 'Biomass-Based Diesel', '>=50%', 1.6,
     ARRAY['bbd', 'advanced', 'total'],
     ARRAY['Biodiesel (FAME)', 'Renewable diesel', 'Renewable jet fuel']),
    ('5', 'Advanced Biofuel', '>=50%', 1.0,
     ARRAY['advanced', 'total'],
     ARRAY['Sugarcane ethanol', 'Non-corn starch ethanol', 'Renewable naphtha']),
    ('6', 'Renewable Fuel (Conventional)', '>=20%', 1.0,
     ARRAY['total'],
     ARRAY['Corn ethanol', 'Grain sorghum ethanol']),
    ('7', 'Cellulosic Diesel', '>=60%', 1.7,
     ARRAY['cellulosic_or_bbd', 'advanced', 'total'],
     ARRAY['Renewable diesel from cellulosic', 'Cellulosic SAF'])
ON CONFLICT (d_code) DO UPDATE SET
    name = EXCLUDED.name,
    ghg_threshold = EXCLUDED.ghg_threshold,
    typical_equivalence_value = EXCLUDED.typical_equivalence_value,
    satisfies_rvos = EXCLUDED.satisfies_rvos,
    common_fuels = EXCLUDED.common_fuels;

COMMENT ON TABLE reference.epa_d_codes IS
    'EPA RFS D-code reference with GHG thresholds, equivalence values, and nested RVO structure.';

-- =============================================================================
-- RETIREMENT REASON CODES REFERENCE
-- =============================================================================
CREATE TABLE IF NOT EXISTS reference.epa_retire_reasons (
    reason_code VARCHAR(10) PRIMARY KEY,
    description VARCHAR(500) NOT NULL,
    category VARCHAR(50)
);

INSERT INTO reference.epa_retire_reasons (reason_code, description, category) VALUES
    ('10', 'Reported spill', 'loss'),
    ('20', 'Contaminated or spoiled fuel', 'loss'),
    ('30', 'Import volume correction', 'correction'),
    ('40', 'Renewable fuel used in ocean-going vessel', 'non_transport'),
    ('50', 'Invalid RIN', 'invalid'),
    ('60', 'Volume error correction', 'correction'),
    ('70', 'Enforcement Obligation', 'enforcement'),
    ('80', 'Demonstrate annual compliance', 'compliance'),
    ('90', 'Renewable fuel used in non-transportation application', 'non_transport'),
    ('100', 'Delayed RIN Retire per 40CFR 80.1426(g)(3)', 'delayed'),
    ('110', 'Remedial action - Retirement pursuant to 80.1431(c)', 'remedial'),
    ('120', 'Remedial Action - Retire for Compliance', 'remedial'),
    ('130', 'Remediation of Invalid RIN Use for Compliance', 'remedial')
ON CONFLICT (reason_code) DO UPDATE SET
    description = EXCLUDED.description,
    category = EXCLUDED.category;

-- =============================================================================
-- SEPARATION REASON CODES REFERENCE
-- =============================================================================
CREATE TABLE IF NOT EXISTS reference.epa_separation_reasons (
    reason_code VARCHAR(10) PRIMARY KEY,
    description VARCHAR(500) NOT NULL,
    category VARCHAR(50)
);

INSERT INTO reference.epa_separation_reasons (reason_code, description, category) VALUES
    ('10', 'Receipt of Renewable Fuel by Obligated Party', 'obligated_party'),
    ('20', 'Blending to Produce Transportation Fuel', 'blending'),
    ('30', 'Designation as Transportation Fuel without blending', 'designation'),
    ('40', 'Upstream Delegation for Blending', 'delegation'),
    ('50', 'Export of Renewable Fuel', 'export'),
    ('60', 'Use as Heating Oil or Jet Fuel', 'heating_jet'),
    ('70', 'Use in non-road engine or vehicle', 'non_road'),
    ('80', 'Designation as Heating Oil or Jet Fuel without blending', 'designation'),
    ('90', 'Delayed RIN Separation per 40 CFR 80.1426(g)(8)', 'delayed')
ON CONFLICT (reason_code) DO UPDATE SET
    description = EXCLUDED.description,
    category = EXCLUDED.category;
