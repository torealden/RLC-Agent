-- ============================================================================
-- Migration 004: Add Soybean Meal Import HS Codes
-- ============================================================================
-- This migration adds the missing import HS codes for soybean meal to the
-- trade_commodity_reference table, including the 2023 split codes.
--
-- Run with: psql -d rlc_commodities -f database/migrations/004_add_soybean_meal_import_codes.sql
-- ============================================================================

-- ============================================================================
-- ADD SOYBEAN MEAL IMPORT CODES
-- ============================================================================
-- Note: Soybean meal imports split in 2023:
--   Pre-2023: 2304000000 (all soybean meal)
--   2023+:    2304000010 (organic) + 2304000090 (NES/conventional)
--
-- Census reports most soybean meal in KG, EXCEPT 2302500000 (bran/hulls) which is in MT

INSERT INTO silver.trade_commodity_reference
(hs_code_10, hs_code_6, commodity_group, commodity_name, flow_type, source_unit, display_unit, conversion_factor, notes)
VALUES

-- Soybean meal - imports (pre-2023 and exports continue using 2304000000)
('2304000000', '230400', 'SOYBEAN_MEAL', 'Soybean oilcake and meal', 'IMPORT', 'KG', '1,000 MT', 0.000001, 'KG to 1000 MT - legacy import code, still used for exports'),

-- Soybean meal - imports 2023+ (split codes)
('2304000010', '230400', 'SOYBEAN_MEAL', 'Soybean oilcake and meal, organic', 'IMPORT', 'KG', '1,000 MT', 0.000001, 'KG to 1000 MT - imports 2023+ organic'),
('2304000090', '230400', 'SOYBEAN_MEAL', 'Soybean oilcake and meal, NES', 'IMPORT', 'KG', '1,000 MT', 0.000001, 'KG to 1000 MT - imports 2023+ conventional/NES'),

-- Soybean bran/hulls - IMPORTANT: Census reports this in MT, not KG!
('2302500000', '230250', 'SOYBEAN_MEAL', 'Bran from legumes (soybean hulls)', 'EXPORT', 'MT', '1,000 MT', 0.001, 'MT to 1000 MT - Census reports in MT'),
('2302500000', '230250', 'SOYBEAN_MEAL', 'Bran from legumes (soybean hulls)', 'IMPORT', 'MT', '1,000 MT', 0.001, 'MT to 1000 MT - Census reports in MT'),

-- Soybean flour/meal codes
('1208100000', '120810', 'SOYBEAN_MEAL', 'Soy flour and meal', 'EXPORT', 'KG', '1,000 MT', 0.000001, 'KG to 1000 MT - legacy code'),
('1208100000', '120810', 'SOYBEAN_MEAL', 'Soy flour and meal', 'IMPORT', 'KG', '1,000 MT', 0.000001, 'KG to 1000 MT - legacy code'),
('1208100010', '120810', 'SOYBEAN_MEAL', 'Soy flour and meal, organic', 'EXPORT', 'KG', '1,000 MT', 0.000001, 'KG to 1000 MT'),
('1208100010', '120810', 'SOYBEAN_MEAL', 'Soy flour and meal, organic', 'IMPORT', 'KG', '1,000 MT', 0.000001, 'KG to 1000 MT'),
('1208100090', '120810', 'SOYBEAN_MEAL', 'Soy flour and meal, NES', 'EXPORT', 'KG', '1,000 MT', 0.000001, 'KG to 1000 MT'),
('1208100090', '120810', 'SOYBEAN_MEAL', 'Soy flour and meal, NES', 'IMPORT', 'KG', '1,000 MT', 0.000001, 'KG to 1000 MT')

ON CONFLICT (hs_code_10, flow_type) DO UPDATE SET
    commodity_name = EXCLUDED.commodity_name,
    source_unit = EXCLUDED.source_unit,
    conversion_factor = EXCLUDED.conversion_factor,
    notes = EXCLUDED.notes;

-- ============================================================================
-- ADD SOYBEAN OIL IMPORT CODES
-- ============================================================================
INSERT INTO silver.trade_commodity_reference
(hs_code_10, hs_code_6, commodity_group, commodity_name, flow_type, source_unit, display_unit, conversion_factor, notes)
VALUES
('1507100000', '150710', 'SOYBEAN_OIL', 'Crude soybean oil', 'IMPORT', 'KG', 'Million Pounds', 0.0000022046, 'KG to million lbs'),
('1507904050', '150790', 'SOYBEAN_OIL', 'Soybean oil, refined', 'IMPORT', 'KG', 'Million Pounds', 0.0000022046, 'KG to million lbs')
ON CONFLICT (hs_code_10, flow_type) DO UPDATE SET
    commodity_name = EXCLUDED.commodity_name,
    source_unit = EXCLUDED.source_unit,
    conversion_factor = EXCLUDED.conversion_factor,
    notes = EXCLUDED.notes;

-- ============================================================================
-- ADD CORN IMPORT CODES
-- ============================================================================
INSERT INTO silver.trade_commodity_reference
(hs_code_10, hs_code_6, commodity_group, commodity_name, flow_type, source_unit, display_unit, conversion_factor, notes)
VALUES
('1005902030', '100590', 'CORN', 'Corn, other than seed', 'IMPORT', 'KG', 'Million Bushels', 0.0000393683, 'KG to million bushels: /1000/25.4'),
('1005100000', '100510', 'CORN', 'Corn seed', 'IMPORT', 'KG', 'Million Bushels', 0.0000393683, 'KG to million bushels')
ON CONFLICT (hs_code_10, flow_type) DO UPDATE SET
    commodity_name = EXCLUDED.commodity_name,
    source_unit = EXCLUDED.source_unit,
    conversion_factor = EXCLUDED.conversion_factor,
    notes = EXCLUDED.notes;

-- ============================================================================
-- ADD SOYBEANS IMPORT CODES
-- ============================================================================
INSERT INTO silver.trade_commodity_reference
(hs_code_10, hs_code_6, commodity_group, commodity_name, flow_type, source_unit, display_unit, conversion_factor, notes)
VALUES
('1201900095', '120190', 'SOYBEANS', 'Soybeans, other than seed', 'IMPORT', 'KG', 'Million Bushels', 0.0000367437, 'KG to million bushels: /1000/27.2155'),
('1201100000', '120110', 'SOYBEANS', 'Soybeans, seed', 'IMPORT', 'KG', 'Million Bushels', 0.0000367437, 'KG to million bushels')
ON CONFLICT (hs_code_10, flow_type) DO UPDATE SET
    commodity_name = EXCLUDED.commodity_name,
    source_unit = EXCLUDED.source_unit,
    conversion_factor = EXCLUDED.conversion_factor,
    notes = EXCLUDED.notes;

-- ============================================================================
-- VERIFICATION
-- ============================================================================
-- Check what's in the reference table now:
SELECT commodity_group, flow_type, COUNT(*) as hs_code_count
FROM silver.trade_commodity_reference
GROUP BY commodity_group, flow_type
ORDER BY commodity_group, flow_type;
