-- ============================================================================
-- Migration: Update All Oilseed Commodity Units for US Trade
-- ============================================================================
-- Updates commodity reference table with correct US trade volume units
--
-- US TRADE VOLUME STANDARDS (monthly columns):
--   VEGETABLE OILS/FATS: 000 Pounds (Census source: MT)
--   PROTEIN MEALS: Short Tons (Census source: MT)
--   OILSEEDS:
--     - Soybeans, Flaxseed: 000 Bushels
--     - Cottonseed: Short Tons
--     - Canola, Sunflower, Peanuts, Safflower: 000 Pounds
--
-- Conversion Factors (from MT):
--   MT to Short Tons: 1.10231
--   MT to 000 Pounds: 2.20462
--   MT to 000 Bushels (Soybeans): 36.744
--   MT to 000 Bushels (Flaxseed): 39.368
--
-- Created: 2026-02-08
-- ============================================================================

-- ============================================================================
-- SOYBEANS - 000 Bushels
-- ============================================================================

INSERT INTO silver.trade_commodity_reference
(hs_code_10, hs_code_6, commodity_group, commodity_name, flow_type, source_unit, display_unit, conversion_factor, is_active, notes)
VALUES
-- Exports
('1201900095', '120190', 'SOYBEANS', 'Soybeans, bulk', 'EXPORT', 'MT', '000 Bushels', 36.744, TRUE, 'MT to 000 bu (36.744 bu/MT)'),
('1201900005', '120190', 'SOYBEANS', 'Soybeans, for oil stock', 'EXPORT', 'MT', '000 Bushels', 36.744, TRUE, 'MT to 000 bu'),
('1201100000', '120110', 'SOYBEANS', 'Soybean seeds for sowing', 'EXPORT', 'MT', '000 Bushels', 36.744, TRUE, 'MT to 000 bu'),
-- Imports
('1201900095', '120190', 'SOYBEANS', 'Soybeans, bulk', 'IMPORT', 'MT', '000 Bushels', 36.744, TRUE, 'MT to 000 bu'),
('1201900005', '120190', 'SOYBEANS', 'Soybeans, for oil stock', 'IMPORT', 'MT', '000 Bushels', 36.744, TRUE, 'MT to 000 bu'),
('1201100000', '120110', 'SOYBEANS', 'Soybean seeds for sowing', 'IMPORT', 'MT', '000 Bushels', 36.744, TRUE, 'MT to 000 bu')
ON CONFLICT (hs_code_10, flow_type) DO UPDATE SET
    commodity_group = EXCLUDED.commodity_group,
    commodity_name = EXCLUDED.commodity_name,
    source_unit = EXCLUDED.source_unit,
    display_unit = EXCLUDED.display_unit,
    conversion_factor = EXCLUDED.conversion_factor,
    notes = EXCLUDED.notes;

-- ============================================================================
-- SOYBEAN OIL - 000 Pounds
-- ============================================================================

INSERT INTO silver.trade_commodity_reference
(hs_code_10, hs_code_6, commodity_group, commodity_name, flow_type, source_unit, display_unit, conversion_factor, is_active, notes)
VALUES
-- Exports
('1507100000', '150710', 'SOYBEAN_OIL', 'Soybean oil, crude', 'EXPORT', 'MT', '000 Pounds', 2.20462, TRUE, 'MT to 000 lbs'),
('1507904050', '150790', 'SOYBEAN_OIL', 'Soybean oil, fully refined', 'EXPORT', 'MT', '000 Pounds', 2.20462, TRUE, 'MT to 000 lbs'),
-- Imports
('1507100000', '150710', 'SOYBEAN_OIL', 'Soybean oil, crude', 'IMPORT', 'MT', '000 Pounds', 2.20462, TRUE, 'MT to 000 lbs'),
('1507904050', '150790', 'SOYBEAN_OIL', 'Soybean oil, fully refined', 'IMPORT', 'MT', '000 Pounds', 2.20462, TRUE, 'MT to 000 lbs')
ON CONFLICT (hs_code_10, flow_type) DO UPDATE SET
    commodity_group = EXCLUDED.commodity_group,
    commodity_name = EXCLUDED.commodity_name,
    source_unit = EXCLUDED.source_unit,
    display_unit = EXCLUDED.display_unit,
    conversion_factor = EXCLUDED.conversion_factor,
    notes = EXCLUDED.notes;

-- ============================================================================
-- SOYBEAN MEAL - Short Tons
-- ============================================================================

INSERT INTO silver.trade_commodity_reference
(hs_code_10, hs_code_6, commodity_group, commodity_name, flow_type, source_unit, display_unit, conversion_factor, is_active, notes)
VALUES
-- Exports
('2304000000', '230400', 'SOYBEAN_MEAL', 'Soybean oilcake and meal', 'EXPORT', 'MT', 'Short Tons', 1.10231, TRUE, 'MT to Short Tons'),
-- Imports
('2304000000', '230400', 'SOYBEAN_MEAL', 'Soybean oilcake and meal', 'IMPORT', 'MT', 'Short Tons', 1.10231, TRUE, 'MT to Short Tons')
ON CONFLICT (hs_code_10, flow_type) DO UPDATE SET
    commodity_group = EXCLUDED.commodity_group,
    commodity_name = EXCLUDED.commodity_name,
    source_unit = EXCLUDED.source_unit,
    display_unit = EXCLUDED.display_unit,
    conversion_factor = EXCLUDED.conversion_factor,
    notes = EXCLUDED.notes;

-- ============================================================================
-- CANOLA/RAPESEED - 000 Pounds
-- ============================================================================

INSERT INTO silver.trade_commodity_reference
(hs_code_10, hs_code_6, commodity_group, commodity_name, flow_type, source_unit, display_unit, conversion_factor, is_active, notes)
VALUES
-- Exports
('1205100000', '120510', 'CANOLA', 'Low erucic acid rapeseed/canola', 'EXPORT', 'MT', '000 Pounds', 2.20462, TRUE, 'MT to 000 lbs'),
-- Imports
('1205100000', '120510', 'CANOLA', 'Low erucic acid rapeseed/canola', 'IMPORT', 'MT', '000 Pounds', 2.20462, TRUE, 'MT to 000 lbs')
ON CONFLICT (hs_code_10, flow_type) DO UPDATE SET
    commodity_group = EXCLUDED.commodity_group,
    commodity_name = EXCLUDED.commodity_name,
    source_unit = EXCLUDED.source_unit,
    display_unit = EXCLUDED.display_unit,
    conversion_factor = EXCLUDED.conversion_factor,
    notes = EXCLUDED.notes;

-- ============================================================================
-- CANOLA OIL - 000 Pounds
-- ============================================================================

INSERT INTO silver.trade_commodity_reference
(hs_code_10, hs_code_6, commodity_group, commodity_name, flow_type, source_unit, display_unit, conversion_factor, is_active, notes)
VALUES
-- Exports
('1514110000', '151411', 'CANOLA_OIL', 'Rapeseed/canola oil, crude', 'EXPORT', 'MT', '000 Pounds', 2.20462, TRUE, 'MT to 000 lbs'),
('1514190000', '151419', 'CANOLA_OIL', 'Rapeseed/canola oil, refined', 'EXPORT', 'MT', '000 Pounds', 2.20462, TRUE, 'MT to 000 lbs'),
-- Imports
('1514110000', '151411', 'CANOLA_OIL', 'Rapeseed/canola oil, crude', 'IMPORT', 'MT', '000 Pounds', 2.20462, TRUE, 'MT to 000 lbs'),
('1514190000', '151419', 'CANOLA_OIL', 'Rapeseed/canola oil, refined', 'IMPORT', 'MT', '000 Pounds', 2.20462, TRUE, 'MT to 000 lbs')
ON CONFLICT (hs_code_10, flow_type) DO UPDATE SET
    commodity_group = EXCLUDED.commodity_group,
    commodity_name = EXCLUDED.commodity_name,
    source_unit = EXCLUDED.source_unit,
    display_unit = EXCLUDED.display_unit,
    conversion_factor = EXCLUDED.conversion_factor,
    notes = EXCLUDED.notes;

-- ============================================================================
-- CANOLA MEAL - Short Tons
-- ============================================================================

INSERT INTO silver.trade_commodity_reference
(hs_code_10, hs_code_6, commodity_group, commodity_name, flow_type, source_unit, display_unit, conversion_factor, is_active, notes)
VALUES
-- Exports
('2306490000', '230649', 'CANOLA_MEAL', 'Rapeseed/canola meal', 'EXPORT', 'MT', 'Short Tons', 1.10231, TRUE, 'MT to Short Tons'),
-- Imports
('2306490000', '230649', 'CANOLA_MEAL', 'Rapeseed/canola meal', 'IMPORT', 'MT', 'Short Tons', 1.10231, TRUE, 'MT to Short Tons')
ON CONFLICT (hs_code_10, flow_type) DO UPDATE SET
    commodity_group = EXCLUDED.commodity_group,
    commodity_name = EXCLUDED.commodity_name,
    source_unit = EXCLUDED.source_unit,
    display_unit = EXCLUDED.display_unit,
    conversion_factor = EXCLUDED.conversion_factor,
    notes = EXCLUDED.notes;

-- ============================================================================
-- SUNFLOWERSEED - 000 Pounds
-- ============================================================================

INSERT INTO silver.trade_commodity_reference
(hs_code_10, hs_code_6, commodity_group, commodity_name, flow_type, source_unit, display_unit, conversion_factor, is_active, notes)
VALUES
-- Exports
('1206000020', '120600', 'SUNFLOWERSEED', 'Sunflower seeds for oil', 'EXPORT', 'MT', '000 Pounds', 2.20462, TRUE, 'MT to 000 lbs'),
('1206000090', '120600', 'SUNFLOWERSEED', 'Sunflower seeds NESOI', 'EXPORT', 'MT', '000 Pounds', 2.20462, TRUE, 'MT to 000 lbs'),
-- Imports
('1206000020', '120600', 'SUNFLOWERSEED', 'Sunflower seeds for oil', 'IMPORT', 'MT', '000 Pounds', 2.20462, TRUE, 'MT to 000 lbs'),
('1206000090', '120600', 'SUNFLOWERSEED', 'Sunflower seeds NESOI', 'IMPORT', 'MT', '000 Pounds', 2.20462, TRUE, 'MT to 000 lbs')
ON CONFLICT (hs_code_10, flow_type) DO UPDATE SET
    commodity_group = EXCLUDED.commodity_group,
    commodity_name = EXCLUDED.commodity_name,
    source_unit = EXCLUDED.source_unit,
    display_unit = EXCLUDED.display_unit,
    conversion_factor = EXCLUDED.conversion_factor,
    notes = EXCLUDED.notes;

-- ============================================================================
-- SUNFLOWER OIL - 000 Pounds
-- ============================================================================

INSERT INTO silver.trade_commodity_reference
(hs_code_10, hs_code_6, commodity_group, commodity_name, flow_type, source_unit, display_unit, conversion_factor, is_active, notes)
VALUES
-- Exports
('1512110020', '151211', 'SUNFLOWER_OIL', 'Sunflower oil, crude', 'EXPORT', 'MT', '000 Pounds', 2.20462, TRUE, 'MT to 000 lbs'),
('1512190020', '151219', 'SUNFLOWER_OIL', 'Sunflower oil, refined', 'EXPORT', 'MT', '000 Pounds', 2.20462, TRUE, 'MT to 000 lbs'),
-- Imports
('1512110020', '151211', 'SUNFLOWER_OIL', 'Sunflower oil, crude', 'IMPORT', 'MT', '000 Pounds', 2.20462, TRUE, 'MT to 000 lbs'),
('1512190020', '151219', 'SUNFLOWER_OIL', 'Sunflower oil, refined', 'IMPORT', 'MT', '000 Pounds', 2.20462, TRUE, 'MT to 000 lbs')
ON CONFLICT (hs_code_10, flow_type) DO UPDATE SET
    commodity_group = EXCLUDED.commodity_group,
    commodity_name = EXCLUDED.commodity_name,
    source_unit = EXCLUDED.source_unit,
    display_unit = EXCLUDED.display_unit,
    conversion_factor = EXCLUDED.conversion_factor,
    notes = EXCLUDED.notes;

-- ============================================================================
-- SUNFLOWER MEAL - Short Tons
-- ============================================================================

INSERT INTO silver.trade_commodity_reference
(hs_code_10, hs_code_6, commodity_group, commodity_name, flow_type, source_unit, display_unit, conversion_factor, is_active, notes)
VALUES
-- Exports
('2306300000', '230630', 'SUNFLOWER_MEAL', 'Sunflower seed meal', 'EXPORT', 'MT', 'Short Tons', 1.10231, TRUE, 'MT to Short Tons'),
-- Imports
('2306300000', '230630', 'SUNFLOWER_MEAL', 'Sunflower seed meal', 'IMPORT', 'MT', 'Short Tons', 1.10231, TRUE, 'MT to Short Tons')
ON CONFLICT (hs_code_10, flow_type) DO UPDATE SET
    commodity_group = EXCLUDED.commodity_group,
    commodity_name = EXCLUDED.commodity_name,
    source_unit = EXCLUDED.source_unit,
    display_unit = EXCLUDED.display_unit,
    conversion_factor = EXCLUDED.conversion_factor,
    notes = EXCLUDED.notes;

-- ============================================================================
-- PEANUTS/GROUNDNUTS - 000 Pounds
-- ============================================================================

INSERT INTO silver.trade_commodity_reference
(hs_code_10, hs_code_6, commodity_group, commodity_name, flow_type, source_unit, display_unit, conversion_factor, is_active, notes)
VALUES
-- Exports
('1202410000', '120241', 'PEANUTS', 'Groundnuts in shell', 'EXPORT', 'MT', '000 Pounds', 2.20462, TRUE, 'MT to 000 lbs'),
('1202420000', '120242', 'PEANUTS', 'Groundnuts shelled', 'EXPORT', 'MT', '000 Pounds', 2.20462, TRUE, 'MT to 000 lbs'),
-- Imports
('1202410000', '120241', 'PEANUTS', 'Groundnuts in shell', 'IMPORT', 'MT', '000 Pounds', 2.20462, TRUE, 'MT to 000 lbs'),
('1202420000', '120242', 'PEANUTS', 'Groundnuts shelled', 'IMPORT', 'MT', '000 Pounds', 2.20462, TRUE, 'MT to 000 lbs')
ON CONFLICT (hs_code_10, flow_type) DO UPDATE SET
    commodity_group = EXCLUDED.commodity_group,
    commodity_name = EXCLUDED.commodity_name,
    source_unit = EXCLUDED.source_unit,
    display_unit = EXCLUDED.display_unit,
    conversion_factor = EXCLUDED.conversion_factor,
    notes = EXCLUDED.notes;

-- ============================================================================
-- PEANUT OIL - 000 Pounds
-- ============================================================================

INSERT INTO silver.trade_commodity_reference
(hs_code_10, hs_code_6, commodity_group, commodity_name, flow_type, source_unit, display_unit, conversion_factor, is_active, notes)
VALUES
-- Exports
('1508100000', '150810', 'PEANUT_OIL', 'Peanut oil, crude', 'EXPORT', 'MT', '000 Pounds', 2.20462, TRUE, 'MT to 000 lbs'),
('1508900000', '150890', 'PEANUT_OIL', 'Peanut oil, refined', 'EXPORT', 'MT', '000 Pounds', 2.20462, TRUE, 'MT to 000 lbs'),
-- Imports
('1508100000', '150810', 'PEANUT_OIL', 'Peanut oil, crude', 'IMPORT', 'MT', '000 Pounds', 2.20462, TRUE, 'MT to 000 lbs'),
('1508900000', '150890', 'PEANUT_OIL', 'Peanut oil, refined', 'IMPORT', 'MT', '000 Pounds', 2.20462, TRUE, 'MT to 000 lbs')
ON CONFLICT (hs_code_10, flow_type) DO UPDATE SET
    commodity_group = EXCLUDED.commodity_group,
    commodity_name = EXCLUDED.commodity_name,
    source_unit = EXCLUDED.source_unit,
    display_unit = EXCLUDED.display_unit,
    conversion_factor = EXCLUDED.conversion_factor,
    notes = EXCLUDED.notes;

-- ============================================================================
-- DDGS - Short Tons
-- ============================================================================

INSERT INTO silver.trade_commodity_reference
(hs_code_10, hs_code_6, commodity_group, commodity_name, flow_type, source_unit, display_unit, conversion_factor, is_active, notes)
VALUES
-- Exports
('2303300000', '230330', 'DDGS', 'Distillers dried grains with solubles', 'EXPORT', 'MT', 'Short Tons', 1.10231, TRUE, 'MT to Short Tons'),
-- Imports
('2303300000', '230330', 'DDGS', 'Distillers dried grains with solubles', 'IMPORT', 'MT', 'Short Tons', 1.10231, TRUE, 'MT to Short Tons')
ON CONFLICT (hs_code_10, flow_type) DO UPDATE SET
    commodity_group = EXCLUDED.commodity_group,
    commodity_name = EXCLUDED.commodity_name,
    source_unit = EXCLUDED.source_unit,
    display_unit = EXCLUDED.display_unit,
    conversion_factor = EXCLUDED.conversion_factor,
    notes = EXCLUDED.notes;

-- ============================================================================
-- CORN GLUTEN - Short Tons
-- ============================================================================

INSERT INTO silver.trade_commodity_reference
(hs_code_10, hs_code_6, commodity_group, commodity_name, flow_type, source_unit, display_unit, conversion_factor, is_active, notes)
VALUES
-- Exports
('2303100010', '230310', 'CORN_GLUTEN_FEED', 'Corn gluten feed', 'EXPORT', 'MT', 'Short Tons', 1.10231, TRUE, 'MT to Short Tons'),
('2303100020', '230310', 'CORN_GLUTEN_MEAL', 'Corn gluten meal', 'EXPORT', 'MT', 'Short Tons', 1.10231, TRUE, 'MT to Short Tons'),
-- Imports
('2303100010', '230310', 'CORN_GLUTEN_FEED', 'Corn gluten feed', 'IMPORT', 'MT', 'Short Tons', 1.10231, TRUE, 'MT to Short Tons'),
('2303100020', '230310', 'CORN_GLUTEN_MEAL', 'Corn gluten meal', 'IMPORT', 'MT', 'Short Tons', 1.10231, TRUE, 'MT to Short Tons')
ON CONFLICT (hs_code_10, flow_type) DO UPDATE SET
    commodity_group = EXCLUDED.commodity_group,
    commodity_name = EXCLUDED.commodity_name,
    source_unit = EXCLUDED.source_unit,
    display_unit = EXCLUDED.display_unit,
    conversion_factor = EXCLUDED.conversion_factor,
    notes = EXCLUDED.notes;

-- ============================================================================
-- ANIMAL FATS - 000 Pounds
-- ============================================================================

INSERT INTO silver.trade_commodity_reference
(hs_code_10, hs_code_6, commodity_group, commodity_name, flow_type, source_unit, display_unit, conversion_factor, is_active, notes)
VALUES
-- Tallow
('1502100020', '150210', 'TALLOW', 'Edible tallow', 'EXPORT', 'MT', '000 Pounds', 2.20462, TRUE, 'MT to 000 lbs'),
('1502100040', '150210', 'TALLOW', 'Inedible tallow', 'EXPORT', 'MT', '000 Pounds', 2.20462, TRUE, 'MT to 000 lbs'),
('1502100020', '150210', 'TALLOW', 'Edible tallow', 'IMPORT', 'MT', '000 Pounds', 2.20462, TRUE, 'MT to 000 lbs'),
('1502100040', '150210', 'TALLOW', 'Inedible tallow', 'IMPORT', 'MT', '000 Pounds', 2.20462, TRUE, 'MT to 000 lbs'),
-- Lard
('1501100000', '150110', 'LARD', 'Lard', 'EXPORT', 'MT', '000 Pounds', 2.20462, TRUE, 'MT to 000 lbs'),
('1501100000', '150110', 'LARD', 'Lard', 'IMPORT', 'MT', '000 Pounds', 2.20462, TRUE, 'MT to 000 lbs'),
-- Yellow Grease
('1501200060', '150120', 'YELLOW_GREASE', 'Yellow grease', 'EXPORT', 'MT', '000 Pounds', 2.20462, TRUE, 'MT to 000 lbs'),
('1501200060', '150120', 'YELLOW_GREASE', 'Yellow grease', 'IMPORT', 'MT', '000 Pounds', 2.20462, TRUE, 'MT to 000 lbs')
ON CONFLICT (hs_code_10, flow_type) DO UPDATE SET
    commodity_group = EXCLUDED.commodity_group,
    commodity_name = EXCLUDED.commodity_name,
    source_unit = EXCLUDED.source_unit,
    display_unit = EXCLUDED.display_unit,
    conversion_factor = EXCLUDED.conversion_factor,
    notes = EXCLUDED.notes;

-- ============================================================================
-- PALM OIL - 000 Pounds
-- ============================================================================

INSERT INTO silver.trade_commodity_reference
(hs_code_10, hs_code_6, commodity_group, commodity_name, flow_type, source_unit, display_unit, conversion_factor, is_active, notes)
VALUES
-- Palm Oil
('1511100000', '151110', 'PALM_OIL', 'Palm oil, crude', 'EXPORT', 'MT', '000 Pounds', 2.20462, TRUE, 'MT to 000 lbs'),
('1511900000', '151190', 'PALM_OIL', 'Palm oil, refined', 'EXPORT', 'MT', '000 Pounds', 2.20462, TRUE, 'MT to 000 lbs'),
('1511100000', '151110', 'PALM_OIL', 'Palm oil, crude', 'IMPORT', 'MT', '000 Pounds', 2.20462, TRUE, 'MT to 000 lbs'),
('1511900000', '151190', 'PALM_OIL', 'Palm oil, refined', 'IMPORT', 'MT', '000 Pounds', 2.20462, TRUE, 'MT to 000 lbs'),
-- Palm Kernel Oil
('1513210000', '151321', 'PALM_KERNEL_OIL', 'Palm kernel oil, crude', 'EXPORT', 'MT', '000 Pounds', 2.20462, TRUE, 'MT to 000 lbs'),
('1513290000', '151329', 'PALM_KERNEL_OIL', 'Palm kernel oil, refined', 'EXPORT', 'MT', '000 Pounds', 2.20462, TRUE, 'MT to 000 lbs'),
('1513210000', '151321', 'PALM_KERNEL_OIL', 'Palm kernel oil, crude', 'IMPORT', 'MT', '000 Pounds', 2.20462, TRUE, 'MT to 000 lbs'),
('1513290000', '151329', 'PALM_KERNEL_OIL', 'Palm kernel oil, refined', 'IMPORT', 'MT', '000 Pounds', 2.20462, TRUE, 'MT to 000 lbs')
ON CONFLICT (hs_code_10, flow_type) DO UPDATE SET
    commodity_group = EXCLUDED.commodity_group,
    commodity_name = EXCLUDED.commodity_name,
    source_unit = EXCLUDED.source_unit,
    display_unit = EXCLUDED.display_unit,
    conversion_factor = EXCLUDED.conversion_factor,
    notes = EXCLUDED.notes;

-- ============================================================================
-- CORN OIL - 000 Pounds
-- ============================================================================

INSERT INTO silver.trade_commodity_reference
(hs_code_10, hs_code_6, commodity_group, commodity_name, flow_type, source_unit, display_unit, conversion_factor, is_active, notes)
VALUES
-- Exports
('1515210000', '151521', 'CORN_OIL', 'Corn oil, crude', 'EXPORT', 'MT', '000 Pounds', 2.20462, TRUE, 'MT to 000 lbs'),
('1515290040', '151529', 'CORN_OIL', 'Corn oil, fully refined', 'EXPORT', 'MT', '000 Pounds', 2.20462, TRUE, 'MT to 000 lbs'),
-- Imports
('1515210000', '151521', 'CORN_OIL', 'Corn oil, crude', 'IMPORT', 'MT', '000 Pounds', 2.20462, TRUE, 'MT to 000 lbs'),
('1515290040', '151529', 'CORN_OIL', 'Corn oil, fully refined', 'IMPORT', 'MT', '000 Pounds', 2.20462, TRUE, 'MT to 000 lbs')
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
-- Run to verify:
-- SELECT commodity_group, display_unit, COUNT(*) as cnt
-- FROM silver.trade_commodity_reference
-- GROUP BY commodity_group, display_unit
-- ORDER BY commodity_group;
