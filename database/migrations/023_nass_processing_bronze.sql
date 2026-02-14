-- ============================================================================
-- Migration: 023_nass_processing_bronze.sql
-- Date: 2026-02-10
-- Description: Create bronze layer for NASS processing data (Fats & Oils,
--              Grain Crushings, etc.) and crush attribute reference table
--              for mapping NASS data to Excel spreadsheet columns.
--
-- Previously, nass_processing_collector.py saved directly to
-- silver.monthly_realized (no bronze layer). This migration adds:
--   1. bronze.nass_processing — raw API response storage
--   2. silver.crush_attribute_reference — maps NASS fields to spreadsheet cols
--   3. Seed data for soybean crush spreadsheet mapping
--   4. Lineage edges
--
-- Target spreadsheet: us_soy_crush.xlsx ("NASS Crush" sheet)
-- ============================================================================

-- ============================================================================
-- 1. BRONZE TABLE: Raw NASS processing data
-- ============================================================================

CREATE TABLE IF NOT EXISTS bronze.nass_processing (
    id SERIAL PRIMARY KEY,

    -- Raw NASS API fields (preserved as-is from response)
    commodity_desc VARCHAR(100) NOT NULL,      -- 'OIL', 'SOYBEANS', 'CAKE & MEAL'
    class_desc VARCHAR(100),                   -- 'SOYBEAN', 'COTTONSEED', etc.
    statisticcat_desc VARCHAR(100) NOT NULL,   -- 'PRODUCTION', 'STOCKS', 'CRUSHED'
    short_desc TEXT NOT NULL,                  -- Full NASS description string
    unit_desc VARCHAR(100),                    -- 'TONS', 'LB', '1,000 SHORT TONS'
    domaincat_desc TEXT,                       -- Disambiguates oil stocks by location

    -- Time
    year INTEGER NOT NULL,
    reference_period_desc VARCHAR(50),         -- 'JAN', 'FEB', etc.
    month INTEGER,                             -- Parsed month number (1-12)

    -- Value
    value DECIMAL(18,4),

    -- Context
    freq_desc VARCHAR(20) DEFAULT 'MONTHLY',
    agg_level_desc VARCHAR(20) DEFAULT 'NATIONAL',
    source_desc VARCHAR(20) DEFAULT 'SURVEY',

    -- Our metadata
    report_type VARCHAR(50),                   -- 'fats_oils', 'grain_crushings', etc.
    source VARCHAR(50),                        -- 'NASS_FATS_OILS', 'NASS_GRAIN_CRUSH'
    collected_at TIMESTAMPTZ DEFAULT NOW(),
    ingest_run_id UUID
);

-- Unique index with COALESCE to handle NULL domaincat_desc
CREATE UNIQUE INDEX IF NOT EXISTS uq_nass_processing_natural_key
    ON bronze.nass_processing(
        commodity_desc,
        COALESCE(class_desc, ''),
        statisticcat_desc,
        short_desc,
        year,
        COALESCE(month, 0),
        COALESCE(domaincat_desc, '')
    );

CREATE INDEX IF NOT EXISTS idx_nass_proc_commodity
    ON bronze.nass_processing(commodity_desc, class_desc);
CREATE INDEX IF NOT EXISTS idx_nass_proc_year_month
    ON bronze.nass_processing(year, month);
CREATE INDEX IF NOT EXISTS idx_nass_proc_report_type
    ON bronze.nass_processing(report_type);
CREATE INDEX IF NOT EXISTS idx_nass_proc_source
    ON bronze.nass_processing(source);

COMMENT ON TABLE bronze.nass_processing IS
    'Raw NASS processing report data (Fats & Oils, Grain Crushings, Flour Milling, Peanut Processing). Preserved as-is from NASS QuickStats API.';


-- ============================================================================
-- 2. SILVER TABLE: Crush attribute reference
--    Maps NASS API fields to spreadsheet columns
-- ============================================================================

CREATE TABLE IF NOT EXISTS silver.crush_attribute_reference (
    id SERIAL PRIMARY KEY,

    -- Commodity grouping
    commodity VARCHAR(50) NOT NULL,            -- 'soybeans', 'corn', etc.

    -- Attribute identification
    attribute_code VARCHAR(80) NOT NULL,       -- 'soybeans_crushed', 'crude_oil_production'
    display_name VARCHAR(200) NOT NULL,        -- Column header in spreadsheet

    -- Unit conversion
    source_unit VARCHAR(50),                   -- NASS unit: 'TONS', 'LB'
    display_unit VARCHAR(50),                  -- Spreadsheet unit: '000 tons', 'mil lbs'
    conversion_factor NUMERIC(18,10) NOT NULL DEFAULT 1.0,

    -- Spreadsheet mapping
    spreadsheet_column INTEGER,                -- Column number in crush spreadsheet (A=1)
    is_formula BOOLEAN DEFAULT FALSE,          -- TRUE = formula column, VBA skips

    -- NASS API field mapping (for joining bronze -> gold)
    nass_commodity_desc VARCHAR(100),
    nass_class_desc VARCHAR(100),
    nass_statisticcat_desc VARCHAR(100),
    nass_short_desc_filter VARCHAR(300),        -- Substring match for short_desc
    nass_domaincat_filter VARCHAR(300),         -- Substring match for domaincat_desc

    is_active BOOLEAN DEFAULT TRUE,

    UNIQUE(commodity, attribute_code)
);

CREATE INDEX IF NOT EXISTS idx_crush_attr_commodity
    ON silver.crush_attribute_reference(commodity);
CREATE INDEX IF NOT EXISTS idx_crush_attr_active
    ON silver.crush_attribute_reference(commodity, is_active)
    WHERE is_active = TRUE;

COMMENT ON TABLE silver.crush_attribute_reference IS
    'Maps NASS processing data to Excel crush spreadsheet columns. Used by gold views and VBA updater to place values in correct cells.';


-- ============================================================================
-- 3. SEED DATA: Soybean crush attribute mapping
--
-- Spreadsheet: us_soy_crush.xlsx, "NASS Crush" sheet
-- Layout: Row 3 = headers, Row 4 = units, Column A = dates, data starts row 5
--
-- NASS API units:
--   SOYBEANS CRUSHED: TONS (short tons)
--   CAKE & MEAL: TONS (short tons)
--   OIL: LB (pounds)
--
-- Spreadsheet display units:
--   000 tons = thousands of short tons
--   mil lbs = million pounds
--   lbs/bu, % = derived (formula columns)
-- ============================================================================

INSERT INTO silver.crush_attribute_reference (
    commodity, attribute_code, display_name,
    source_unit, display_unit, conversion_factor,
    spreadsheet_column, is_formula,
    nass_commodity_desc, nass_class_desc, nass_statisticcat_desc,
    nass_short_desc_filter, nass_domaincat_filter
) VALUES
    -- ========================================
    -- CRUSH SECTION (Columns B-H)
    -- ========================================

    -- Col C (3): Soybeans Crushed — NASS: SOYBEANS/CRUSHED, TONS -> 000 tons
    ('soybeans', 'soybeans_crushed', 'Soybeans Crushed',
     'TONS', '000 tons', 0.001,
     3, FALSE,
     'SOYBEANS', 'ALL CLASSES', 'CRUSHED',
     'SOYBEANS - CRUSHED, MEASURED IN TONS', NULL),

    -- Col D (4): Soybeans Crushed (mil bu) — FORMULA =C*36.7437/1000
    ('soybeans', 'soybeans_crushed_bu', 'Soybeans Crushed (bu)',
     NULL, 'mil bu', 1.0,
     4, TRUE,
     NULL, NULL, NULL, NULL, NULL),

    -- ========================================
    -- MEAL SECTION (Columns J-R)
    -- ========================================

    -- Col J (10): Meal Production — NASS: CAKE & MEAL,SOYBEAN/PRODUCTION total
    ('soybeans', 'meal_production', 'Meal Production',
     'TONS', '000 tons', 0.001,
     10, FALSE,
     'CAKE & MEAL', 'SOYBEAN', 'PRODUCTION',
     'CAKE & MEAL, SOYBEAN - PRODUCTION, MEASURED IN TONS', NULL),

    -- Col K (11): Meal Animal Feed Use
    ('soybeans', 'meal_animal_feed', 'Meal Animal Feed Use',
     'TONS', '000 tons', 0.001,
     11, FALSE,
     'CAKE & MEAL', 'SOYBEAN', 'PRODUCTION',
     'CAKE & MEAL, SOYBEAN, ANIMAL FEED - PRODUCTION, MEASURED IN TONS', NULL),

    -- Col L (12): Meal Edible Protein Use
    ('soybeans', 'meal_edible_protein', 'Meal Edible Protein Use',
     'TONS', '000 tons', 0.001,
     12, FALSE,
     'CAKE & MEAL', 'SOYBEAN', 'PRODUCTION',
     'CAKE & MEAL, SOYBEAN, EDIBLE PROTEIN PRODUCTS - PRODUCTION, MEASURED IN TONS', NULL),

    -- Col O (15): Meal Yield — FORMULA
    ('soybeans', 'meal_yield', 'Meal Yield',
     NULL, 'lbs/bu', 1.0,
     15, TRUE,
     NULL, NULL, NULL, NULL, NULL),

    -- Col P (16): Meal Yield with Millfeed/Hull — FORMULA
    ('soybeans', 'meal_yield_with_millfeed', 'Meal Yield with Millfeed/Hull',
     NULL, 'lbs/bu', 1.0,
     16, TRUE,
     NULL, NULL, NULL, NULL, NULL),

    -- ========================================
    -- OIL PRODUCTION SECTION (Columns T-X)
    -- ========================================

    -- Col T (20): Crude Oil Production — NASS: OIL,SOYBEAN,CRUDE/PRODUCTION, LB -> mil lbs
    ('soybeans', 'crude_oil_production', 'Crude Oil Production',
     'LB', 'mil lbs', 0.000001,
     20, FALSE,
     'OIL', 'SOYBEAN', 'PRODUCTION',
     'OIL, SOYBEAN, CRUDE - PRODUCTION, MEASURED IN LB', NULL),

    -- Col V (22): Crude Oil Refined — NASS: REMOVAL FOR PROCESSING, LB -> mil lbs
    ('soybeans', 'crude_oil_refined', 'Crude Oil Refined',
     'LB', 'mil lbs', 0.000001,
     22, FALSE,
     'OIL', 'SOYBEAN', 'REMOVAL FOR PROCESSING',
     'OIL, SOYBEAN, CRUDE, PROCESSED IN REFINING - REMOVAL FOR PROCESSING, MEASURED IN LB', NULL),

    -- Col W (23): Refined Oil Production — NASS: OIL,SOYBEAN,ONCE REFINED/PRODUCTION
    ('soybeans', 'refined_oil_production', 'Refined Oil Production',
     'LB', 'mil lbs', 0.000001,
     23, FALSE,
     'OIL', 'SOYBEAN', 'PRODUCTION',
     'OIL, SOYBEAN, ONCE REFINED - PRODUCTION, MEASURED IN LB', NULL),

    -- Col AH (34): Oil Yield — FORMULA
    ('soybeans', 'oil_yield', 'Oil Yield',
     NULL, 'lbs/bu', 1.0,
     34, TRUE,
     NULL, NULL, NULL, NULL, NULL),

    -- ========================================
    -- OIL STOCKS SECTION (Columns AB-AG)
    -- ========================================

    -- Col AB (28): Crude Oil Stocks (total) — NASS: ONSITE & OFFSITE, CRUDE/STOCKS
    ('soybeans', 'crude_oil_stocks_total', 'Crude Oil Stocks',
     'LB', 'mil lbs', 0.000001,
     28, FALSE,
     'OIL', 'SOYBEAN', 'STOCKS',
     'OIL, SOYBEAN, ONSITE & OFFSITE, CRUDE - STOCKS, MEASURED IN LB', NULL),

    -- Col AC (29): Crude Oil Crusher Stocks — disambiguated by domaincat_desc
    ('soybeans', 'crude_oil_crusher_stocks', 'Crude Oil Crusher Stocks',
     'LB', 'mil lbs', 0.000001,
     29, FALSE,
     'OIL', 'SOYBEAN', 'STOCKS',
     'OIL, SOYBEAN, CRUDE - STOCKS, MEASURED IN LB', 'CRUSHER'),

    -- Col AD (30): Refined Oil Refiner Stocks — NASS: ONSITE & OFFSITE, ONCE REFINED/STOCKS
    ('soybeans', 'refined_oil_stocks', 'Refined Oil Refiner Stocks',
     'LB', 'mil lbs', 0.000001,
     30, FALSE,
     'OIL', 'SOYBEAN', 'STOCKS',
     'OIL, SOYBEAN, ONSITE & OFFSITE, ONCE REFINED - STOCKS, MEASURED IN LB', NULL),

    -- Col AE (31): All Oil Offsite Stocks — NASS: OFFSITE, CRUDE/STOCKS
    ('soybeans', 'oil_offsite_stocks', 'All Oil Offsite Stocks',
     'LB', 'mil lbs', 0.000001,
     31, FALSE,
     'OIL', 'SOYBEAN', 'STOCKS',
     'OIL, SOYBEAN, OFFSITE, CRUDE - STOCKS, MEASURED IN LB', NULL)

ON CONFLICT (commodity, attribute_code) DO UPDATE SET
    display_name = EXCLUDED.display_name,
    source_unit = EXCLUDED.source_unit,
    display_unit = EXCLUDED.display_unit,
    conversion_factor = EXCLUDED.conversion_factor,
    spreadsheet_column = EXCLUDED.spreadsheet_column,
    is_formula = EXCLUDED.is_formula,
    nass_commodity_desc = EXCLUDED.nass_commodity_desc,
    nass_class_desc = EXCLUDED.nass_class_desc,
    nass_statisticcat_desc = EXCLUDED.nass_statisticcat_desc,
    nass_short_desc_filter = EXCLUDED.nass_short_desc_filter,
    nass_domaincat_filter = EXCLUDED.nass_domaincat_filter;


-- ============================================================================
-- 4. LINEAGE EDGES
-- ============================================================================

INSERT INTO audit.lineage_edge (
    source_type, source_schema, source_name,
    target_type, target_schema, target_name,
    relationship_type, transformation_description
) VALUES
    -- API -> Bronze
    ('API', NULL, 'NASS_QuickStats_API',
     'TABLE', 'bronze', 'nass_processing',
     'COPIES', 'Raw NASS Fats & Oils / Grain Crushings data stored in bronze'),

    -- Bronze -> Silver (monthly_realized)
    ('TABLE', 'bronze', 'nass_processing',
     'TABLE', 'silver', 'monthly_realized',
     'TRANSFORMS', 'NASS processing data mapped to standard attributes and units'),

    -- Bronze -> Gold (crush views)
    ('TABLE', 'bronze', 'nass_processing',
     'VIEW', 'gold', 'nass_crush_mapped',
     'TRANSFORMS', 'Bronze NASS data joined with crush_attribute_reference for unit conversion'),

    -- Reference -> Gold
    ('TABLE', 'silver', 'crush_attribute_reference',
     'VIEW', 'gold', 'nass_crush_mapped',
     'REFERENCES', 'Attribute mapping and conversion factors for crush spreadsheet'),

    -- Gold mapped -> Gold matrix
    ('VIEW', 'gold', 'nass_crush_mapped',
     'VIEW', 'gold', 'nass_soy_crush_matrix',
     'DERIVES_FROM', 'Soybean-filtered view of crush data for VBA consumption')

ON CONFLICT DO NOTHING;


-- ============================================================================
-- VERIFICATION
-- ============================================================================
-- After running this migration:
--
-- 1. Verify bronze table:
--    SELECT COUNT(*) FROM bronze.nass_processing;  -- Should be 0 (not yet populated)
--
-- 2. Verify attribute reference:
--    SELECT attribute_code, display_name, spreadsheet_column, is_formula,
--           conversion_factor, nass_short_desc_filter
--    FROM silver.crush_attribute_reference
--    WHERE commodity = 'soybeans'
--    ORDER BY spreadsheet_column;
--
-- 3. Verify lineage:
--    SELECT source_name, target_name, relationship_type
--    FROM audit.lineage_edge
--    WHERE target_name LIKE '%crush%' OR source_name = 'nass_processing';
-- ============================================================================
