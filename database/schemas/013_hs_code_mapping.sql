-- =============================================================================
-- HS Code Mapping and History Schema
-- =============================================================================
-- Tracks HS codes across time, including retirements, replacements, and splits
-- Enables accurate historical trade data aggregation across code changes
-- =============================================================================

-- -----------------------------------------------------------------------------
-- HS CODE MASTER TABLE
-- -----------------------------------------------------------------------------
-- Central reference for all HS codes we track
CREATE TABLE IF NOT EXISTS reference.hs_codes (
    id SERIAL PRIMARY KEY,
    hs_code VARCHAR(10) NOT NULL UNIQUE,
    description VARCHAR(255) NOT NULL,
    chapter VARCHAR(4),                    -- First 2-4 digits (e.g., '2304' for soybean meal)

    -- Commodity grouping for aggregation
    commodity_group VARCHAR(50),           -- e.g., 'SOYBEAN_MEAL', 'SOYBEAN_OIL', 'SOYBEANS'
    commodity_subgroup VARCHAR(50),        -- e.g., 'ORGANIC', 'CONVENTIONAL', 'HULLS'

    -- Unit information
    census_unit VARCHAR(10) NOT NULL,      -- Unit Census reports (KG, T, etc.)
    standard_unit VARCHAR(20) NOT NULL,    -- Our standard unit (SHORT_TONS, 1000_LBS, etc.)
    conversion_factor DECIMAL(12,6),       -- Multiply census_unit by this to get standard_unit

    -- Validity period
    valid_from DATE NOT NULL,              -- When this code started being used
    valid_to DATE,                         -- NULL if still active, date if retired
    is_active BOOLEAN DEFAULT TRUE,

    -- Succession tracking
    replaced_by VARCHAR(10)[],             -- Array of HS codes that replaced this one
    replaces VARCHAR(10)[],                -- Array of HS codes this one replaced

    -- Trade direction applicability
    applies_to_imports BOOLEAN DEFAULT TRUE,
    applies_to_exports BOOLEAN DEFAULT TRUE,

    -- Metadata
    notes TEXT,
    source VARCHAR(100),                   -- Where we got this info (USITC, Census, etc.)
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- -----------------------------------------------------------------------------
-- HS CODE CONCORDANCE TABLE
-- -----------------------------------------------------------------------------
-- Maps old codes to new codes with effective dates and split ratios
CREATE TABLE IF NOT EXISTS reference.hs_code_concordance (
    id SERIAL PRIMARY KEY,
    old_code VARCHAR(10) NOT NULL,
    new_code VARCHAR(10) NOT NULL,
    effective_date DATE NOT NULL,          -- When the change took effect

    -- For splits: what percentage of old code volume goes to new code
    -- e.g., 2304000000 split to 2304000010 (organic ~40%) and 2304000090 (conventional ~60%)
    estimated_share DECIMAL(5,2),          -- Percentage (0-100)

    -- Relationship type
    relationship_type VARCHAR(20) NOT NULL, -- 'REPLACED', 'SPLIT', 'MERGED', 'RENAMED'

    notes TEXT,
    source VARCHAR(100),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- -----------------------------------------------------------------------------
-- COMMODITY GROUP DEFINITIONS
-- -----------------------------------------------------------------------------
-- Defines what HS codes belong to each commodity group for aggregation
CREATE TABLE IF NOT EXISTS reference.commodity_groups (
    id SERIAL PRIMARY KEY,
    group_code VARCHAR(50) NOT NULL UNIQUE,  -- e.g., 'SOYBEAN_MEAL_ALL'
    group_name VARCHAR(100) NOT NULL,        -- e.g., 'All Soybean Meal Products'
    description TEXT,

    -- Unit for aggregated reporting
    report_unit VARCHAR(20) NOT NULL,        -- 'SHORT_TONS', '1000_LBS', 'BUSHELS'
    report_unit_label VARCHAR(50),           -- 'Short Tons', '1,000 Lbs', 'Bushels'

    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- -----------------------------------------------------------------------------
-- INDEXES
-- -----------------------------------------------------------------------------
CREATE INDEX IF NOT EXISTS idx_hs_codes_commodity_group ON reference.hs_codes(commodity_group);
CREATE INDEX IF NOT EXISTS idx_hs_codes_valid_dates ON reference.hs_codes(valid_from, valid_to);
CREATE INDEX IF NOT EXISTS idx_hs_codes_active ON reference.hs_codes(is_active);
CREATE INDEX IF NOT EXISTS idx_concordance_old_code ON reference.hs_code_concordance(old_code);
CREATE INDEX IF NOT EXISTS idx_concordance_new_code ON reference.hs_code_concordance(new_code);
CREATE INDEX IF NOT EXISTS idx_concordance_effective ON reference.hs_code_concordance(effective_date);

-- -----------------------------------------------------------------------------
-- INITIAL DATA: Soybean Meal HS Codes
-- -----------------------------------------------------------------------------

-- Commodity Groups
INSERT INTO reference.commodity_groups (group_code, group_name, description, report_unit, report_unit_label)
VALUES
    ('SOYBEAN_MEAL_ALL', 'All Soybean Meal Products', 'Includes oilcake, flour, hulls - all soybean protein products', 'SHORT_TONS', 'Short Tons'),
    ('SOYBEAN_OIL_ALL', 'All Soybean Oil Products', 'Includes crude and refined soybean oil', '1000_LBS', '1,000 Lbs'),
    ('SOYBEANS_ALL', 'All Soybeans', 'Includes bulk, seed, oilstock soybeans', 'BUSHELS', 'Bushels')
ON CONFLICT (group_code) DO NOTHING;

-- Soybean Meal HS Codes
-- Conversion: KG → Short Tons = KG / 1000 * 1.10231
INSERT INTO reference.hs_codes (hs_code, description, chapter, commodity_group, commodity_subgroup,
    census_unit, standard_unit, conversion_factor, valid_from, valid_to, is_active,
    replaced_by, replaces, applies_to_imports, applies_to_exports, notes, source)
VALUES
    -- Main soybean meal (RETIRED for imports in 2023)
    ('2304000000', 'Soybean Oilcake and Meal', '2304', 'SOYBEAN_MEAL_ALL', 'CONVENTIONAL',
     'KG', 'SHORT_TONS', 0.00110231, '2013-01-01', '2022-12-31', FALSE,
     ARRAY['2304000010', '2304000090'], NULL, TRUE, TRUE,
     'Replaced by organic (2304000010) and NES (2304000090) codes for imports starting 2023. Still used for exports.',
     'Census Bureau'),

    -- Organic soybean meal (NEW in 2023)
    ('2304000010', 'Organic Soybean Oilcake and Meal', '2304', 'SOYBEAN_MEAL_ALL', 'ORGANIC',
     'KG', 'SHORT_TONS', 0.00110231, '2023-01-01', NULL, TRUE,
     NULL, ARRAY['2304000000'], TRUE, TRUE,
     'Split from 2304000000 starting 2023 for organic tracking',
     'Census Bureau'),

    -- Soybean meal NES (NEW in 2023)
    ('2304000090', 'Soybean Oilcake and Meal NES', '2304', 'SOYBEAN_MEAL_ALL', 'CONVENTIONAL',
     'KG', 'SHORT_TONS', 0.00110231, '2023-01-01', NULL, TRUE,
     NULL, ARRAY['2304000000'], TRUE, TRUE,
     'Split from 2304000000 starting 2023 - conventional (non-organic)',
     'Census Bureau'),

    -- Soybean hulls/bran
    ('2302500000', 'Bran, Sharps, Residues from Legumes (Soybean Hulls)', '2302', 'SOYBEAN_MEAL_ALL', 'HULLS',
     'KG', 'SHORT_TONS', 0.00110231, '2013-01-01', NULL, TRUE,
     NULL, NULL, TRUE, TRUE,
     'Soybean hulls - byproduct of soybean processing',
     'Census Bureau'),

    -- Soy flour/meal NES
    ('1208100090', 'Flour and Meal of Soybeans NES', '1208', 'SOYBEAN_MEAL_ALL', 'FLOUR',
     'KG', 'SHORT_TONS', 0.00110231, '2013-01-01', NULL, TRUE,
     NULL, NULL, TRUE, TRUE,
     'Soy flour and meal - food grade',
     'Census Bureau'),

    -- Soy flour/meal organic
    ('1208100010', 'Flour and Meal of Soybeans Organic', '1208', 'SOYBEAN_MEAL_ALL', 'FLOUR_ORGANIC',
     'KG', 'SHORT_TONS', 0.00110231, '2013-01-01', NULL, TRUE,
     NULL, NULL, TRUE, TRUE,
     'Organic soy flour and meal',
     'Census Bureau')
ON CONFLICT (hs_code) DO UPDATE SET
    description = EXCLUDED.description,
    valid_to = EXCLUDED.valid_to,
    is_active = EXCLUDED.is_active,
    replaced_by = EXCLUDED.replaced_by,
    updated_at = NOW();

-- Concordance: 2304000000 split in 2023
INSERT INTO reference.hs_code_concordance (old_code, new_code, effective_date, estimated_share, relationship_type, notes, source)
VALUES
    ('2304000000', '2304000010', '2023-01-01', 45, 'SPLIT', 'Organic portion of soybean meal imports', 'Estimated from 2023-2024 import data'),
    ('2304000000', '2304000090', '2023-01-01', 55, 'SPLIT', 'Conventional (NES) portion of soybean meal imports', 'Estimated from 2023-2024 import data')
ON CONFLICT DO NOTHING;

-- -----------------------------------------------------------------------------
-- HELPER VIEWS
-- -----------------------------------------------------------------------------

-- View: All active HS codes for a commodity group
CREATE OR REPLACE VIEW reference.active_hs_codes AS
SELECT
    hs_code,
    description,
    commodity_group,
    commodity_subgroup,
    census_unit,
    standard_unit,
    conversion_factor,
    applies_to_imports,
    applies_to_exports
FROM reference.hs_codes
WHERE is_active = TRUE
ORDER BY commodity_group, hs_code;

-- View: HS codes valid for a specific date (for historical queries)
CREATE OR REPLACE VIEW reference.hs_codes_with_validity AS
SELECT
    hs_code,
    description,
    commodity_group,
    valid_from,
    COALESCE(valid_to, '9999-12-31'::DATE) as valid_to,
    is_active,
    replaced_by,
    replaces
FROM reference.hs_codes
ORDER BY commodity_group, valid_from;

-- Function: Get all HS codes valid for a commodity group on a specific date
CREATE OR REPLACE FUNCTION reference.get_valid_hs_codes(
    p_commodity_group VARCHAR,
    p_date DATE DEFAULT CURRENT_DATE
)
RETURNS TABLE (
    hs_code VARCHAR(10),
    description VARCHAR(255),
    conversion_factor DECIMAL(12,6)
) AS $$
BEGIN
    RETURN QUERY
    SELECT
        h.hs_code,
        h.description,
        h.conversion_factor
    FROM reference.hs_codes h
    WHERE h.commodity_group = p_commodity_group
      AND h.valid_from <= p_date
      AND (h.valid_to IS NULL OR h.valid_to >= p_date);
END;
$$ LANGUAGE plpgsql;

-- -----------------------------------------------------------------------------
-- COMMENTS
-- -----------------------------------------------------------------------------
COMMENT ON TABLE reference.hs_codes IS 'Master table of HS codes with validity periods and commodity groupings';
COMMENT ON TABLE reference.hs_code_concordance IS 'Maps old HS codes to new codes when codes are split, merged, or renamed';
COMMENT ON TABLE reference.commodity_groups IS 'Defines commodity groupings for aggregating multiple HS codes';
COMMENT ON FUNCTION reference.get_valid_hs_codes IS 'Returns all HS codes valid for a commodity group on a specific date';
