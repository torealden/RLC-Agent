-- ============================================================================
-- Migration 006: Add All Trade HS Codes
-- ============================================================================
-- This migration adds all missing HS codes from the bronze table to the
-- trade_commodity_reference table for proper conversion and display.
--
-- Conversion Factors:
-- - Grains (wheat, corn, barley, sorghum, rice): KG to Million Bushels
-- - Oilseeds (soybeans, canola, sunflower, cotton, peanuts): KG to Million Bushels or 1000 MT
-- - Vegetable Oils: KG to 1,000 Pounds
-- - Protein Meals: KG to Short Tons
-- - Cotton: KG to 1,000 Bales (480 lbs/bale)
--
-- Run with: python scripts/run_migration.py database/migrations/006_add_all_trade_hs_codes.sql
-- ============================================================================

-- ============================================================================
-- WHEAT ADDITIONAL CODES
-- ============================================================================
INSERT INTO silver.trade_commodity_reference
(hs_code_10, hs_code_6, commodity_group, commodity_name, flow_type, source_unit, display_unit, conversion_factor, notes)
VALUES
-- Wheat seed
('1001110000', '100111', 'WHEAT', 'Wheat seed, durum', 'EXPORTS', 'KG', 'Million Bushels', 0.0000367437, 'KG to million bu'),
('1001110000', '100111', 'WHEAT', 'Wheat seed, durum', 'IMPORTS', 'KG', 'Million Bushels', 0.0000367437, 'KG to million bu'),
('1001190000', '100119', 'WHEAT', 'Wheat seed, other', 'EXPORTS', 'KG', 'Million Bushels', 0.0000367437, 'KG to million bu'),
('1001190000', '100119', 'WHEAT', 'Wheat seed, other', 'IMPORTS', 'KG', 'Million Bushels', 0.0000367437, 'KG to million bu'),
-- White wheat
('1001992015', '100199', 'WHEAT', 'White wheat', 'EXPORTS', 'KG', 'Million Bushels', 0.0000367437, 'KG to million bu'),
('1001992015', '100199', 'WHEAT', 'White wheat', 'IMPORTS', 'KG', 'Million Bushels', 0.0000367437, 'KG to million bu')
ON CONFLICT (hs_code_10, flow_type) DO UPDATE SET
    commodity_name = EXCLUDED.commodity_name,
    conversion_factor = EXCLUDED.conversion_factor;

-- ============================================================================
-- BARLEY
-- ============================================================================
INSERT INTO silver.trade_commodity_reference
(hs_code_10, hs_code_6, commodity_group, commodity_name, flow_type, source_unit, display_unit, conversion_factor, notes)
VALUES
('1003100000', '100310', 'BARLEY', 'Barley seed', 'EXPORTS', 'KG', 'Million Bushels', 0.0000416667, 'KG to million bu (48 lbs/bu)'),
('1003100000', '100310', 'BARLEY', 'Barley seed', 'IMPORTS', 'KG', 'Million Bushels', 0.0000416667, 'KG to million bu'),
('1003900000', '100390', 'BARLEY', 'Barley other than seed', 'EXPORTS', 'KG', 'Million Bushels', 0.0000416667, 'KG to million bu'),
('1003900000', '100390', 'BARLEY', 'Barley other than seed', 'IMPORTS', 'KG', 'Million Bushels', 0.0000416667, 'KG to million bu')
ON CONFLICT (hs_code_10, flow_type) DO UPDATE SET
    commodity_name = EXCLUDED.commodity_name,
    conversion_factor = EXCLUDED.conversion_factor;

-- ============================================================================
-- CORN ADDITIONAL CODES
-- ============================================================================
INSERT INTO silver.trade_commodity_reference
(hs_code_10, hs_code_6, commodity_group, commodity_name, flow_type, source_unit, display_unit, conversion_factor, notes)
VALUES
-- Corn seed
('1005100010', '100510', 'CORN', 'Yellow corn seed', 'EXPORTS', 'KG', 'Million Bushels', 0.0000393683, 'KG to million bu (56 lbs/bu)'),
('1005100010', '100510', 'CORN', 'Yellow corn seed', 'IMPORTS', 'KG', 'Million Bushels', 0.0000393683, 'KG to million bu'),
-- Corn other grades
('1005902020', '100590', 'CORN', 'Yellow dent corn No. 1', 'EXPORTS', 'KG', 'Million Bushels', 0.0000393683, 'KG to million bu'),
('1005902020', '100590', 'CORN', 'Yellow dent corn No. 1', 'IMPORTS', 'KG', 'Million Bushels', 0.0000393683, 'KG to million bu'),
('1005902035', '100590', 'CORN', 'Yellow dent corn No. 3', 'EXPORTS', 'KG', 'Million Bushels', 0.0000393683, 'KG to million bu'),
('1005902035', '100590', 'CORN', 'Yellow dent corn No. 3', 'IMPORTS', 'KG', 'Million Bushels', 0.0000393683, 'KG to million bu'),
('1005904065', '100590', 'CORN', 'Corn NESOI', 'EXPORTS', 'KG', 'Million Bushels', 0.0000393683, 'KG to million bu'),
('1005904065', '100590', 'CORN', 'Corn NESOI', 'IMPORTS', 'KG', 'Million Bushels', 0.0000393683, 'KG to million bu')
ON CONFLICT (hs_code_10, flow_type) DO UPDATE SET
    commodity_name = EXCLUDED.commodity_name,
    conversion_factor = EXCLUDED.conversion_factor;

-- ============================================================================
-- RICE
-- ============================================================================
INSERT INTO silver.trade_commodity_reference
(hs_code_10, hs_code_6, commodity_group, commodity_name, flow_type, source_unit, display_unit, conversion_factor, notes)
VALUES
('1006100000', '100610', 'RICE', 'Rice in husk (paddy)', 'EXPORTS', 'KG', '1,000 CWT', 0.0000220462, 'KG to 1000 cwt'),
('1006100000', '100610', 'RICE', 'Rice in husk (paddy)', 'IMPORTS', 'KG', '1,000 CWT', 0.0000220462, 'KG to 1000 cwt'),
('1006400000', '100640', 'RICE', 'Broken rice', 'EXPORTS', 'KG', '1,000 CWT', 0.0000220462, 'KG to 1000 cwt'),
('1006400000', '100640', 'RICE', 'Broken rice', 'IMPORTS', 'KG', '1,000 CWT', 0.0000220462, 'KG to 1000 cwt')
ON CONFLICT (hs_code_10, flow_type) DO UPDATE SET
    commodity_name = EXCLUDED.commodity_name,
    conversion_factor = EXCLUDED.conversion_factor;

-- ============================================================================
-- SORGHUM
-- ============================================================================
INSERT INTO silver.trade_commodity_reference
(hs_code_10, hs_code_6, commodity_group, commodity_name, flow_type, source_unit, display_unit, conversion_factor, notes)
VALUES
('1007100000', '100710', 'SORGHUM', 'Grain sorghum seed', 'EXPORTS', 'KG', 'Million Bushels', 0.0000393683, 'KG to million bu (56 lbs/bu)'),
('1007100000', '100710', 'SORGHUM', 'Grain sorghum seed', 'IMPORTS', 'KG', 'Million Bushels', 0.0000393683, 'KG to million bu'),
('1007900000', '100790', 'SORGHUM', 'Grain sorghum other', 'EXPORTS', 'KG', 'Million Bushels', 0.0000393683, 'KG to million bu'),
('1007900000', '100790', 'SORGHUM', 'Grain sorghum other', 'IMPORTS', 'KG', 'Million Bushels', 0.0000393683, 'KG to million bu')
ON CONFLICT (hs_code_10, flow_type) DO UPDATE SET
    commodity_name = EXCLUDED.commodity_name,
    conversion_factor = EXCLUDED.conversion_factor;

-- ============================================================================
-- PEANUTS
-- ============================================================================
INSERT INTO silver.trade_commodity_reference
(hs_code_10, hs_code_6, commodity_group, commodity_name, flow_type, source_unit, display_unit, conversion_factor, notes)
VALUES
('1202410000', '120241', 'PEANUTS', 'Peanuts in shell', 'EXPORTS', 'KG', '1,000 MT', 0.000001, 'KG to 1000 MT'),
('1202410000', '120241', 'PEANUTS', 'Peanuts in shell', 'IMPORTS', 'KG', '1,000 MT', 0.000001, 'KG to 1000 MT')
ON CONFLICT (hs_code_10, flow_type) DO UPDATE SET
    commodity_name = EXCLUDED.commodity_name,
    conversion_factor = EXCLUDED.conversion_factor;

-- ============================================================================
-- FLAXSEED
-- ============================================================================
INSERT INTO silver.trade_commodity_reference
(hs_code_10, hs_code_6, commodity_group, commodity_name, flow_type, source_unit, display_unit, conversion_factor, notes)
VALUES
('1204000000', '120400', 'FLAXSEED', 'Flaxseed/linseed', 'EXPORTS', 'KG', '1,000 MT', 0.000001, 'KG to 1000 MT'),
('1204000000', '120400', 'FLAXSEED', 'Flaxseed/linseed', 'IMPORTS', 'KG', '1,000 MT', 0.000001, 'KG to 1000 MT')
ON CONFLICT (hs_code_10, flow_type) DO UPDATE SET
    commodity_name = EXCLUDED.commodity_name,
    conversion_factor = EXCLUDED.conversion_factor;

-- ============================================================================
-- CANOLA/RAPESEED ADDITIONAL
-- ============================================================================
INSERT INTO silver.trade_commodity_reference
(hs_code_10, hs_code_6, commodity_group, commodity_name, flow_type, source_unit, display_unit, conversion_factor, notes)
VALUES
('1205900000', '120590', 'CANOLA', 'Rapeseed/canola other', 'EXPORTS', 'KG', '1,000 MT', 0.000001, 'KG to 1000 MT'),
('1205900000', '120590', 'CANOLA', 'Rapeseed/canola other', 'IMPORTS', 'KG', '1,000 MT', 0.000001, 'KG to 1000 MT')
ON CONFLICT (hs_code_10, flow_type) DO UPDATE SET
    commodity_name = EXCLUDED.commodity_name,
    conversion_factor = EXCLUDED.conversion_factor;

-- ============================================================================
-- SUNFLOWER SEEDS
-- ============================================================================
INSERT INTO silver.trade_commodity_reference
(hs_code_10, hs_code_6, commodity_group, commodity_name, flow_type, source_unit, display_unit, conversion_factor, notes)
VALUES
('1206000020', '120600', 'SUNFLOWER', 'Sunflower seed for oil', 'EXPORTS', 'KG', '1,000 MT', 0.000001, 'KG to 1000 MT'),
('1206000020', '120600', 'SUNFLOWER', 'Sunflower seed for oil', 'IMPORTS', 'KG', '1,000 MT', 0.000001, 'KG to 1000 MT'),
('1206000090', '120600', 'SUNFLOWER', 'Sunflower seed NESOI', 'EXPORTS', 'KG', '1,000 MT', 0.000001, 'KG to 1000 MT'),
('1206000090', '120600', 'SUNFLOWER', 'Sunflower seed NESOI', 'IMPORTS', 'KG', '1,000 MT', 0.000001, 'KG to 1000 MT')
ON CONFLICT (hs_code_10, flow_type) DO UPDATE SET
    commodity_name = EXCLUDED.commodity_name,
    conversion_factor = EXCLUDED.conversion_factor;

-- ============================================================================
-- COTTON SEEDS
-- ============================================================================
INSERT INTO silver.trade_commodity_reference
(hs_code_10, hs_code_6, commodity_group, commodity_name, flow_type, source_unit, display_unit, conversion_factor, notes)
VALUES
('1207290000', '120729', 'COTTONSEED', 'Cotton seeds', 'EXPORTS', 'KG', '1,000 MT', 0.000001, 'KG to 1000 MT'),
('1207290000', '120729', 'COTTONSEED', 'Cotton seeds', 'IMPORTS', 'KG', '1,000 MT', 0.000001, 'KG to 1000 MT')
ON CONFLICT (hs_code_10, flow_type) DO UPDATE SET
    commodity_name = EXCLUDED.commodity_name,
    conversion_factor = EXCLUDED.conversion_factor;

-- ============================================================================
-- OTHER OILSEEDS (SESAME, MUSTARD, SAFFLOWER)
-- ============================================================================
INSERT INTO silver.trade_commodity_reference
(hs_code_10, hs_code_6, commodity_group, commodity_name, flow_type, source_unit, display_unit, conversion_factor, notes)
VALUES
('1207400000', '120740', 'SESAME', 'Sesame seeds', 'EXPORTS', 'KG', '1,000 MT', 0.000001, 'KG to 1000 MT'),
('1207400000', '120740', 'SESAME', 'Sesame seeds', 'IMPORTS', 'KG', '1,000 MT', 0.000001, 'KG to 1000 MT'),
('1207500000', '120750', 'MUSTARD', 'Mustard seeds', 'EXPORTS', 'KG', '1,000 MT', 0.000001, 'KG to 1000 MT'),
('1207500000', '120750', 'MUSTARD', 'Mustard seeds', 'IMPORTS', 'KG', '1,000 MT', 0.000001, 'KG to 1000 MT'),
('1207600000', '120760', 'SAFFLOWER', 'Safflower seeds', 'EXPORTS', 'KG', '1,000 MT', 0.000001, 'KG to 1000 MT'),
('1207600000', '120760', 'SAFFLOWER', 'Safflower seeds', 'IMPORTS', 'KG', '1,000 MT', 0.000001, 'KG to 1000 MT')
ON CONFLICT (hs_code_10, flow_type) DO UPDATE SET
    commodity_name = EXCLUDED.commodity_name,
    conversion_factor = EXCLUDED.conversion_factor;

-- ============================================================================
-- SUNFLOWER OIL ADDITIONAL
-- ============================================================================
INSERT INTO silver.trade_commodity_reference
(hs_code_10, hs_code_6, commodity_group, commodity_name, flow_type, source_unit, display_unit, conversion_factor, notes)
VALUES
('1512210000', '151221', 'SUNFLOWER_OIL', 'Sunflower oil, crude', 'EXPORTS', 'KG', '1,000 Pounds', 0.0000022046, 'KG to thousand lbs'),
('1512210000', '151221', 'SUNFLOWER_OIL', 'Sunflower oil, crude', 'IMPORTS', 'KG', '1,000 Pounds', 0.0000022046, 'KG to thousand lbs')
ON CONFLICT (hs_code_10, flow_type) DO UPDATE SET
    commodity_name = EXCLUDED.commodity_name,
    conversion_factor = EXCLUDED.conversion_factor;

-- ============================================================================
-- LINSEED OIL
-- ============================================================================
INSERT INTO silver.trade_commodity_reference
(hs_code_10, hs_code_6, commodity_group, commodity_name, flow_type, source_unit, display_unit, conversion_factor, notes)
VALUES
('1515110000', '151511', 'LINSEED_OIL', 'Linseed oil, crude', 'EXPORTS', 'KG', '1,000 Pounds', 0.0000022046, 'KG to thousand lbs'),
('1515110000', '151511', 'LINSEED_OIL', 'Linseed oil, crude', 'IMPORTS', 'KG', '1,000 Pounds', 0.0000022046, 'KG to thousand lbs'),
('1515190000', '151519', 'LINSEED_OIL', 'Linseed oil, other', 'EXPORTS', 'KG', '1,000 Pounds', 0.0000022046, 'KG to thousand lbs'),
('1515190000', '151519', 'LINSEED_OIL', 'Linseed oil, other', 'IMPORTS', 'KG', '1,000 Pounds', 0.0000022046, 'KG to thousand lbs')
ON CONFLICT (hs_code_10, flow_type) DO UPDATE SET
    commodity_name = EXCLUDED.commodity_name,
    conversion_factor = EXCLUDED.conversion_factor;

-- ============================================================================
-- CORN OIL ADDITIONAL
-- ============================================================================
INSERT INTO silver.trade_commodity_reference
(hs_code_10, hs_code_6, commodity_group, commodity_name, flow_type, source_unit, display_unit, conversion_factor, notes)
VALUES
('1515210000', '151521', 'CORN_OIL', 'Corn oil, crude', 'EXPORTS', 'KG', '1,000 Pounds', 0.0000022046, 'KG to thousand lbs'),
('1515210000', '151521', 'CORN_OIL', 'Corn oil, crude', 'IMPORTS', 'KG', '1,000 Pounds', 0.0000022046, 'KG to thousand lbs')
ON CONFLICT (hs_code_10, flow_type) DO UPDATE SET
    commodity_name = EXCLUDED.commodity_name,
    conversion_factor = EXCLUDED.conversion_factor;

-- ============================================================================
-- OTHER VEGETABLE OILS
-- ============================================================================
INSERT INTO silver.trade_commodity_reference
(hs_code_10, hs_code_6, commodity_group, commodity_name, flow_type, source_unit, display_unit, conversion_factor, notes)
VALUES
('1515500000', '151550', 'SESAME_OIL', 'Sesame oil', 'EXPORTS', 'KG', '1,000 Pounds', 0.0000022046, 'KG to thousand lbs'),
('1515500000', '151550', 'SESAME_OIL', 'Sesame oil', 'IMPORTS', 'KG', '1,000 Pounds', 0.0000022046, 'KG to thousand lbs'),
('1515906000', '151590', 'OTHER_VEG_OIL', 'Other vegetable oils NESOI', 'EXPORTS', 'KG', '1,000 Pounds', 0.0000022046, 'KG to thousand lbs'),
('1515906000', '151590', 'OTHER_VEG_OIL', 'Other vegetable oils NESOI', 'IMPORTS', 'KG', '1,000 Pounds', 0.0000022046, 'KG to thousand lbs')
ON CONFLICT (hs_code_10, flow_type) DO UPDATE SET
    commodity_name = EXCLUDED.commodity_name,
    conversion_factor = EXCLUDED.conversion_factor;

-- ============================================================================
-- CORN GLUTEN FEED AND MEAL
-- ============================================================================
INSERT INTO silver.trade_commodity_reference
(hs_code_10, hs_code_6, commodity_group, commodity_name, flow_type, source_unit, display_unit, conversion_factor, notes)
VALUES
('2303100010', '230310', 'CORN_GLUTEN', 'Corn gluten feed', 'EXPORTS', 'KG', 'Short Tons', 0.0000011023, 'KG to short tons'),
('2303100010', '230310', 'CORN_GLUTEN', 'Corn gluten feed', 'IMPORTS', 'KG', 'Short Tons', 0.0000011023, 'KG to short tons'),
('2303100020', '230310', 'CORN_GLUTEN', 'Corn gluten meal', 'EXPORTS', 'KG', 'Short Tons', 0.0000011023, 'KG to short tons'),
('2303100020', '230310', 'CORN_GLUTEN', 'Corn gluten meal', 'IMPORTS', 'KG', 'Short Tons', 0.0000011023, 'KG to short tons')
ON CONFLICT (hs_code_10, flow_type) DO UPDATE SET
    commodity_name = EXCLUDED.commodity_name,
    conversion_factor = EXCLUDED.conversion_factor;

-- ============================================================================
-- PEANUT MEAL
-- ============================================================================
INSERT INTO silver.trade_commodity_reference
(hs_code_10, hs_code_6, commodity_group, commodity_name, flow_type, source_unit, display_unit, conversion_factor, notes)
VALUES
('2305000000', '230500', 'PEANUT_MEAL', 'Peanut oilcake/meal', 'EXPORTS', 'KG', 'Short Tons', 0.0000011023, 'KG to short tons'),
('2305000000', '230500', 'PEANUT_MEAL', 'Peanut oilcake/meal', 'IMPORTS', 'KG', 'Short Tons', 0.0000011023, 'KG to short tons')
ON CONFLICT (hs_code_10, flow_type) DO UPDATE SET
    commodity_name = EXCLUDED.commodity_name,
    conversion_factor = EXCLUDED.conversion_factor;

-- ============================================================================
-- COTTONSEED MEAL
-- ============================================================================
INSERT INTO silver.trade_commodity_reference
(hs_code_10, hs_code_6, commodity_group, commodity_name, flow_type, source_unit, display_unit, conversion_factor, notes)
VALUES
('2306100000', '230610', 'COTTONSEED_MEAL', 'Cottonseed oilcake/meal', 'EXPORTS', 'KG', 'Short Tons', 0.0000011023, 'KG to short tons'),
('2306100000', '230610', 'COTTONSEED_MEAL', 'Cottonseed oilcake/meal', 'IMPORTS', 'KG', 'Short Tons', 0.0000011023, 'KG to short tons')
ON CONFLICT (hs_code_10, flow_type) DO UPDATE SET
    commodity_name = EXCLUDED.commodity_name,
    conversion_factor = EXCLUDED.conversion_factor;

-- ============================================================================
-- SUNFLOWER MEAL
-- ============================================================================
INSERT INTO silver.trade_commodity_reference
(hs_code_10, hs_code_6, commodity_group, commodity_name, flow_type, source_unit, display_unit, conversion_factor, notes)
VALUES
('2306300000', '230630', 'SUNFLOWER_MEAL', 'Sunflower oilcake/meal', 'EXPORTS', 'KG', 'Short Tons', 0.0000011023, 'KG to short tons'),
('2306300000', '230630', 'SUNFLOWER_MEAL', 'Sunflower oilcake/meal', 'IMPORTS', 'KG', 'Short Tons', 0.0000011023, 'KG to short tons')
ON CONFLICT (hs_code_10, flow_type) DO UPDATE SET
    commodity_name = EXCLUDED.commodity_name,
    conversion_factor = EXCLUDED.conversion_factor;

-- ============================================================================
-- RAPESEED/CANOLA MEAL ADDITIONAL
-- ============================================================================
INSERT INTO silver.trade_commodity_reference
(hs_code_10, hs_code_6, commodity_group, commodity_name, flow_type, source_unit, display_unit, conversion_factor, notes)
VALUES
('2306410000', '230641', 'CANOLA_MEAL', 'Rapeseed meal, low erucic', 'EXPORTS', 'KG', 'Short Tons', 0.0000011023, 'KG to short tons'),
('2306410000', '230641', 'CANOLA_MEAL', 'Rapeseed meal, low erucic', 'IMPORTS', 'KG', 'Short Tons', 0.0000011023, 'KG to short tons'),
('2306490000', '230649', 'CANOLA_MEAL', 'Rapeseed meal, other', 'EXPORTS', 'KG', 'Short Tons', 0.0000011023, 'KG to short tons'),
('2306490000', '230649', 'CANOLA_MEAL', 'Rapeseed meal, other', 'IMPORTS', 'KG', 'Short Tons', 0.0000011023, 'KG to short tons')
ON CONFLICT (hs_code_10, flow_type) DO UPDATE SET
    commodity_name = EXCLUDED.commodity_name,
    conversion_factor = EXCLUDED.conversion_factor;

-- ============================================================================
-- OTHER OILCAKE/MEAL
-- ============================================================================
INSERT INTO silver.trade_commodity_reference
(hs_code_10, hs_code_6, commodity_group, commodity_name, flow_type, source_unit, display_unit, conversion_factor, notes)
VALUES
('2306500000', '230650', 'SAFFLOWER_MEAL', 'Safflower oilcake/meal', 'EXPORTS', 'KG', 'Short Tons', 0.0000011023, 'KG to short tons'),
('2306500000', '230650', 'SAFFLOWER_MEAL', 'Safflower oilcake/meal', 'IMPORTS', 'KG', 'Short Tons', 0.0000011023, 'KG to short tons'),
('2306600000', '230660', 'PALM_KERNEL_MEAL', 'Palm kernel oilcake/meal', 'EXPORTS', 'KG', 'Short Tons', 0.0000011023, 'KG to short tons'),
('2306600000', '230660', 'PALM_KERNEL_MEAL', 'Palm kernel oilcake/meal', 'IMPORTS', 'KG', 'Short Tons', 0.0000011023, 'KG to short tons')
ON CONFLICT (hs_code_10, flow_type) DO UPDATE SET
    commodity_name = EXCLUDED.commodity_name,
    conversion_factor = EXCLUDED.conversion_factor;

-- ============================================================================
-- COTTON (RAW FIBER)
-- ============================================================================
-- Cotton uses special conversion: 1 bale = 480 lbs = 217.72 kg
-- Conversion from KG to 1000 bales: KG / 217.72 / 1000 = KG * 0.0000045931
INSERT INTO silver.trade_commodity_reference
(hs_code_10, hs_code_6, commodity_group, commodity_name, flow_type, source_unit, display_unit, conversion_factor, notes)
VALUES
('5201001090', '520100', 'COTTON', 'Cotton staple 25.4-28.575mm', 'EXPORTS', 'KG', '1,000 Bales', 0.0000045931, 'KG to thousand bales (480 lbs/bale)'),
('5201001090', '520100', 'COTTON', 'Cotton staple 25.4-28.575mm', 'IMPORTS', 'KG', '1,000 Bales', 0.0000045931, 'KG to thousand bales'),
('5201002030', '520100', 'COTTON', 'American Pima cotton', 'EXPORTS', 'KG', '1,000 Bales', 0.0000045931, 'KG to thousand bales'),
('5201002030', '520100', 'COTTON', 'American Pima cotton', 'IMPORTS', 'KG', '1,000 Bales', 0.0000045931, 'KG to thousand bales'),
('5201009000', '520100', 'COTTON', 'Cotton staple >28.575mm', 'EXPORTS', 'KG', '1,000 Bales', 0.0000045931, 'KG to thousand bales'),
('5201009000', '520100', 'COTTON', 'Cotton staple >28.575mm', 'IMPORTS', 'KG', '1,000 Bales', 0.0000045931, 'KG to thousand bales')
ON CONFLICT (hs_code_10, flow_type) DO UPDATE SET
    commodity_name = EXCLUDED.commodity_name,
    conversion_factor = EXCLUDED.conversion_factor;

-- ============================================================================
-- VERIFICATION
-- ============================================================================
SELECT commodity_group, flow_type, COUNT(*) as codes, display_unit
FROM silver.trade_commodity_reference
GROUP BY commodity_group, flow_type, display_unit
ORDER BY commodity_group, flow_type;
