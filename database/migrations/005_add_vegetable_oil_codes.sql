-- ============================================================================
-- Migration 005: Add Vegetable Oil HS Codes
-- ============================================================================
-- This migration adds missing soybean oil codes and other vegetable oil codes
-- to the trade_commodity_reference table.
--
-- Run with: python scripts/run_migration.py database/migrations/005_add_vegetable_oil_codes.sql
-- ============================================================================

-- ============================================================================
-- FIX SOYBEAN OIL DISPLAY UNIT (should say Thousand Pounds, not Million)
-- ============================================================================
-- The conversion formula is: quantity * conversion_factor * 1000
-- With factor 2.20462E-6 and KG input, output is in THOUSAND pounds
UPDATE silver.trade_commodity_reference
SET display_unit = '1,000 Pounds',
    notes = 'KG to thousand lbs - verified vs Census totals'
WHERE commodity_group = 'SOYBEAN_OIL';

-- ============================================================================
-- ADD MISSING SOYBEAN OIL CODES
-- ============================================================================
-- 1507904020 - Soybean oil once refined (primarily exports)
-- 1507904040 - Soybean oil fully refined (imports)

INSERT INTO silver.trade_commodity_reference
(hs_code_10, hs_code_6, commodity_group, commodity_name, flow_type, source_unit, display_unit, conversion_factor, notes)
VALUES
-- Once refined soybean oil (exports)
('1507904020', '150790', 'SOYBEAN_OIL', 'Soybean oil, once refined', 'EXPORTS', 'KG', '1,000 Pounds', 0.0000022046, 'KG to thousand lbs'),
-- Once refined soybean oil (imports) - may have small volumes
('1507904020', '150790', 'SOYBEAN_OIL', 'Soybean oil, once refined', 'IMPORTS', 'KG', '1,000 Pounds', 0.0000022046, 'KG to thousand lbs'),
-- Fully refined soybean oil (imports) - this is the main import code
('1507904040', '150790', 'SOYBEAN_OIL', 'Soybean oil, fully refined', 'IMPORTS', 'KG', '1,000 Pounds', 0.0000022046, 'KG to thousand lbs - main import code'),
-- Fully refined soybean oil (exports) - may have small volumes
('1507904040', '150790', 'SOYBEAN_OIL', 'Soybean oil, fully refined', 'EXPORTS', 'KG', '1,000 Pounds', 0.0000022046, 'KG to thousand lbs')

ON CONFLICT (hs_code_10, flow_type) DO UPDATE SET
    commodity_name = EXCLUDED.commodity_name,
    source_unit = EXCLUDED.source_unit,
    display_unit = EXCLUDED.display_unit,
    conversion_factor = EXCLUDED.conversion_factor,
    notes = EXCLUDED.notes;

-- ============================================================================
-- ADD PALM OIL CODES
-- ============================================================================
INSERT INTO silver.trade_commodity_reference
(hs_code_10, hs_code_6, commodity_group, commodity_name, flow_type, source_unit, display_unit, conversion_factor, notes)
VALUES
-- Palm oil crude
('1511100000', '151110', 'PALM_OIL', 'Palm oil, crude', 'EXPORTS', 'KG', '1,000 Pounds', 0.0000022046, 'KG to thousand lbs'),
('1511100000', '151110', 'PALM_OIL', 'Palm oil, crude', 'IMPORTS', 'KG', '1,000 Pounds', 0.0000022046, 'KG to thousand lbs'),
-- Palm oil refined
('1511900000', '151190', 'PALM_OIL', 'Palm oil, refined', 'EXPORTS', 'KG', '1,000 Pounds', 0.0000022046, 'KG to thousand lbs'),
('1511900000', '151190', 'PALM_OIL', 'Palm oil, refined', 'IMPORTS', 'KG', '1,000 Pounds', 0.0000022046, 'KG to thousand lbs - main US import')

ON CONFLICT (hs_code_10, flow_type) DO UPDATE SET
    commodity_name = EXCLUDED.commodity_name,
    source_unit = EXCLUDED.source_unit,
    display_unit = EXCLUDED.display_unit,
    conversion_factor = EXCLUDED.conversion_factor,
    notes = EXCLUDED.notes;

-- ============================================================================
-- ADD CANOLA/RAPESEED OIL CODES
-- ============================================================================
INSERT INTO silver.trade_commodity_reference
(hs_code_10, hs_code_6, commodity_group, commodity_name, flow_type, source_unit, display_unit, conversion_factor, notes)
VALUES
-- Canola oil crude
('1514110000', '151411', 'CANOLA_OIL', 'Canola/rapeseed oil, crude', 'EXPORTS', 'KG', '1,000 Pounds', 0.0000022046, 'KG to thousand lbs'),
('1514110000', '151411', 'CANOLA_OIL', 'Canola/rapeseed oil, crude', 'IMPORTS', 'KG', '1,000 Pounds', 0.0000022046, 'KG to thousand lbs'),
-- Canola oil NESOI (refined)
('1514190000', '151419', 'CANOLA_OIL', 'Canola/rapeseed oil, NESOI', 'EXPORTS', 'KG', '1,000 Pounds', 0.0000022046, 'KG to thousand lbs'),
('1514190000', '151419', 'CANOLA_OIL', 'Canola/rapeseed oil, NESOI', 'IMPORTS', 'KG', '1,000 Pounds', 0.0000022046, 'KG to thousand lbs')

ON CONFLICT (hs_code_10, flow_type) DO UPDATE SET
    commodity_name = EXCLUDED.commodity_name,
    source_unit = EXCLUDED.source_unit,
    display_unit = EXCLUDED.display_unit,
    conversion_factor = EXCLUDED.conversion_factor,
    notes = EXCLUDED.notes;

-- ============================================================================
-- ADD SUNFLOWER OIL CODES
-- ============================================================================
INSERT INTO silver.trade_commodity_reference
(hs_code_10, hs_code_6, commodity_group, commodity_name, flow_type, source_unit, display_unit, conversion_factor, notes)
VALUES
('1512190020', '151219', 'SUNFLOWER_OIL', 'Sunflower oil, refined', 'EXPORTS', 'KG', '1,000 Pounds', 0.0000022046, 'KG to thousand lbs'),
('1512190020', '151219', 'SUNFLOWER_OIL', 'Sunflower oil, refined', 'IMPORTS', 'KG', '1,000 Pounds', 0.0000022046, 'KG to thousand lbs')

ON CONFLICT (hs_code_10, flow_type) DO UPDATE SET
    commodity_name = EXCLUDED.commodity_name,
    source_unit = EXCLUDED.source_unit,
    display_unit = EXCLUDED.display_unit,
    conversion_factor = EXCLUDED.conversion_factor,
    notes = EXCLUDED.notes;

-- ============================================================================
-- ADD CORN OIL CODES
-- ============================================================================
INSERT INTO silver.trade_commodity_reference
(hs_code_10, hs_code_6, commodity_group, commodity_name, flow_type, source_unit, display_unit, conversion_factor, notes)
VALUES
('1515290040', '151529', 'CORN_OIL', 'Corn oil, fully refined', 'EXPORTS', 'KG', '1,000 Pounds', 0.0000022046, 'KG to thousand lbs'),
('1515290040', '151529', 'CORN_OIL', 'Corn oil, fully refined', 'IMPORTS', 'KG', '1,000 Pounds', 0.0000022046, 'KG to thousand lbs')

ON CONFLICT (hs_code_10, flow_type) DO UPDATE SET
    commodity_name = EXCLUDED.commodity_name,
    source_unit = EXCLUDED.source_unit,
    display_unit = EXCLUDED.display_unit,
    conversion_factor = EXCLUDED.conversion_factor,
    notes = EXCLUDED.notes;

-- ============================================================================
-- ADD PALM KERNEL OIL CODES
-- ============================================================================
INSERT INTO silver.trade_commodity_reference
(hs_code_10, hs_code_6, commodity_group, commodity_name, flow_type, source_unit, display_unit, conversion_factor, notes)
VALUES
('1513290000', '151329', 'PALM_KERNEL_OIL', 'Palm kernel oil, refined', 'EXPORTS', 'KG', '1,000 Pounds', 0.0000022046, 'KG to thousand lbs'),
('1513290000', '151329', 'PALM_KERNEL_OIL', 'Palm kernel oil, refined', 'IMPORTS', 'KG', '1,000 Pounds', 0.0000022046, 'KG to thousand lbs')

ON CONFLICT (hs_code_10, flow_type) DO UPDATE SET
    commodity_name = EXCLUDED.commodity_name,
    source_unit = EXCLUDED.source_unit,
    display_unit = EXCLUDED.display_unit,
    conversion_factor = EXCLUDED.conversion_factor,
    notes = EXCLUDED.notes;

-- ============================================================================
-- UPDATE WORLD TOTAL COUNTRY REFERENCE
-- ============================================================================
-- Ensure TOTAL FOR ALL COUNTRIES maps to WORLD TOTAL
UPDATE silver.trade_country_reference
SET country_name_alt = 'TOTAL FOR ALL COUNTRIES'
WHERE country_name = 'WORLD TOTAL' AND country_name_alt IS NULL;

-- ============================================================================
-- VERIFICATION
-- ============================================================================
SELECT commodity_group, flow_type, COUNT(*) as hs_code_count, display_unit
FROM silver.trade_commodity_reference
GROUP BY commodity_group, flow_type, display_unit
ORDER BY commodity_group, flow_type;
