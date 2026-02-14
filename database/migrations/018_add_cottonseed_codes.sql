-- ============================================================================
-- Migration: Add Cottonseed Complex HS Codes
-- ============================================================================
-- Adds cottonseed (seed), cottonseed oil, and cottonseed meal to the
-- trade_commodity_reference table for Census Bureau trade data collection.
--
-- HS Codes Added:
--   Cottonseed (Seed):
--     1207210000 - Cotton seeds, for sowing
--     1207290000 - Cotton seeds, other
--   Cottonseed Oil:
--     1512210000 - Crude cottonseed oil
--     1512290020 - Refined cottonseed oil, once refined (edible)
--     1512290040 - Refined cottonseed oil, fully refined (other)
--   Cottonseed Meal:
--     2306100000 - Cottonseed oilcake and meal
--
-- US TRADE VOLUME UNIT STANDARDS (monthly columns):
--   - Cottonseed (seed): Short Tons (balance sheet uses 000 Short Tons)
--   - Cottonseed Oil: 000 Pounds (balance sheet uses Million Pounds)
--   - Cottonseed Meal: Short Tons (balance sheet uses 000 Short Tons)
--
-- Note: Census API returns data in MT (Metric Tonnes)
--   MT to Short Tons: 1.10231
--   MT to 000 Pounds: 2.20462
--
-- Created: 2026-02-07
-- Updated: 2026-02-08 (Fixed cottonseed units to Short Tons)
-- ============================================================================

-- ============================================================================
-- COTTONSEED (SEED) - Trade in Short Tons
-- ============================================================================

INSERT INTO silver.trade_commodity_reference
(hs_code_10, hs_code_6, commodity_group, commodity_name, flow_type, source_unit, display_unit, conversion_factor, is_active, notes)
VALUES
-- Exports
('1207210000', '120721', 'COTTONSEED', 'Cotton seeds, for sowing', 'EXPORT', 'MT', 'Short Tons', 1.10231, TRUE, 'MT to Short Tons'),
('1207290000', '120729', 'COTTONSEED', 'Cotton seeds, other', 'EXPORT', 'MT', 'Short Tons', 1.10231, TRUE, 'MT to Short Tons - primary export code'),
-- Imports
('1207210000', '120721', 'COTTONSEED', 'Cotton seeds, for sowing', 'IMPORT', 'MT', 'Short Tons', 1.10231, TRUE, 'MT to Short Tons'),
('1207290000', '120729', 'COTTONSEED', 'Cotton seeds, other', 'IMPORT', 'MT', 'Short Tons', 1.10231, TRUE, 'MT to Short Tons')
ON CONFLICT (hs_code_10, flow_type) DO UPDATE SET
    commodity_group = EXCLUDED.commodity_group,
    commodity_name = EXCLUDED.commodity_name,
    source_unit = EXCLUDED.source_unit,
    display_unit = EXCLUDED.display_unit,
    conversion_factor = EXCLUDED.conversion_factor,
    notes = EXCLUDED.notes;

-- ============================================================================
-- COTTONSEED OIL - Trade in 000 Pounds
-- ============================================================================

INSERT INTO silver.trade_commodity_reference
(hs_code_10, hs_code_6, commodity_group, commodity_name, flow_type, source_unit, display_unit, conversion_factor, is_active, notes)
VALUES
-- Exports
('1512210000', '151221', 'COTTONSEED_OIL', 'Crude cottonseed oil', 'EXPORT', 'MT', '000 Pounds', 2.20462, TRUE, 'MT to 000 lbs'),
('1512290020', '151229', 'COTTONSEED_OIL', 'Refined cottonseed oil, once refined', 'EXPORT', 'MT', '000 Pounds', 2.20462, TRUE, 'MT to 000 lbs - edible grade'),
('1512290040', '151229', 'COTTONSEED_OIL', 'Refined cottonseed oil, fully refined', 'EXPORT', 'MT', '000 Pounds', 2.20462, TRUE, 'MT to 000 lbs - other uses'),
-- Imports
('1512210000', '151221', 'COTTONSEED_OIL', 'Crude cottonseed oil', 'IMPORT', 'MT', '000 Pounds', 2.20462, TRUE, 'MT to 000 lbs'),
('1512290020', '151229', 'COTTONSEED_OIL', 'Refined cottonseed oil, once refined', 'IMPORT', 'MT', '000 Pounds', 2.20462, TRUE, 'MT to 000 lbs - edible grade'),
('1512290040', '151229', 'COTTONSEED_OIL', 'Refined cottonseed oil, fully refined', 'IMPORT', 'MT', '000 Pounds', 2.20462, TRUE, 'MT to 000 lbs - other uses')
ON CONFLICT (hs_code_10, flow_type) DO UPDATE SET
    commodity_group = EXCLUDED.commodity_group,
    commodity_name = EXCLUDED.commodity_name,
    source_unit = EXCLUDED.source_unit,
    display_unit = EXCLUDED.display_unit,
    conversion_factor = EXCLUDED.conversion_factor,
    notes = EXCLUDED.notes;

-- ============================================================================
-- COTTONSEED MEAL - Trade in Short Tons
-- ============================================================================

INSERT INTO silver.trade_commodity_reference
(hs_code_10, hs_code_6, commodity_group, commodity_name, flow_type, source_unit, display_unit, conversion_factor, is_active, notes)
VALUES
-- Exports
('2306100000', '230610', 'COTTONSEED_MEAL', 'Cottonseed oilcake and meal', 'EXPORT', 'MT', 'Short Tons', 1.10231, TRUE, 'MT to Short Tons'),
-- Imports
('2306100000', '230610', 'COTTONSEED_MEAL', 'Cottonseed oilcake and meal', 'IMPORT', 'MT', 'Short Tons', 1.10231, TRUE, 'MT to Short Tons')
ON CONFLICT (hs_code_10, flow_type) DO UPDATE SET
    commodity_group = EXCLUDED.commodity_group,
    commodity_name = EXCLUDED.commodity_name,
    source_unit = EXCLUDED.source_unit,
    display_unit = EXCLUDED.display_unit,
    conversion_factor = EXCLUDED.conversion_factor,
    notes = EXCLUDED.notes;

-- ============================================================================
-- VERIFICATION
-- ============================================================================
-- Run to verify the migration:
-- SELECT commodity_group, hs_code_10, commodity_name, flow_type, display_unit, conversion_factor
-- FROM silver.trade_commodity_reference
-- WHERE commodity_group LIKE 'COTTONSEED%'
-- ORDER BY commodity_group, flow_type, hs_code_10;
