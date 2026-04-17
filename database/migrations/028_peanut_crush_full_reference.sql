-- =============================================================================
-- Migration 028: Peanut crush full reference table expansion
-- =============================================================================
-- Adds 14 new rows to silver.crush_attribute_reference for the peanut_crush tab
-- of us_oilseed_crush.xlsm.
--
-- Sources:
--   - NASS Peanut Stocks and Processing report (CRUSHED, USAGE, PRODUCTION, STOCKS)
--   - All series confirmed via NASS Quickstats API probe 2026-04-16
--
-- Note: cols M, N, O (Page 2 Farmer Stock inventory, FSE Total, Roasting Stock)
-- are NOT available in NASS Quickstats API. They appear only in the PDF report.
-- These will require a separate data path (manual entry or PDF scraping).
--
-- Existing wired cols (migration 027): V, W, X, Y (Fats & Oils oil series)
-- =============================================================================

-- ── Peanuts block ──

-- Col B: Shelled peanuts crushed (Page 5)
INSERT INTO silver.crush_attribute_reference
    (commodity, attribute_code, display_name, source_unit, display_unit, conversion_factor,
     spreadsheet_column, is_formula, nass_commodity_desc, nass_class_desc,
     nass_statisticcat_desc, nass_short_desc_filter, is_active, header_pattern)
VALUES
    ('peanut', 'shelled_peanuts_crushed', 'Shelled Peanuts Crushed',
     'LB', 'thousand pounds', 0.001,
     0, FALSE, 'PEANUTS', NULL,
     'CRUSHED', 'PEANUTS, SHELLED - CRUSHED, MEASURED IN LB',
     TRUE, 'Shelled peanuts crushed');

-- Col D: Total edible grade shelled peanuts used in products (Page 7)
INSERT INTO silver.crush_attribute_reference
    (commodity, attribute_code, display_name, source_unit, display_unit, conversion_factor,
     spreadsheet_column, is_formula, nass_commodity_desc, nass_class_desc,
     nass_statisticcat_desc, nass_short_desc_filter, is_active, header_pattern)
VALUES
    ('peanut', 'edible_usage_total', 'Total Edible Grade Shelled Peanuts',
     'LB', 'thousand pounds', 0.001,
     0, FALSE, 'PEANUTS', NULL,
     'USAGE', 'PEANUTS, SHELLED, EDIBLE - USAGE, MEASURED IN LB, RAW BASIS',
     TRUE, 'Total edible grade shelled peanuts');

-- Col E: Edible peanuts used in peanut candy (Page 7)
INSERT INTO silver.crush_attribute_reference
    (commodity, attribute_code, display_name, source_unit, display_unit, conversion_factor,
     spreadsheet_column, is_formula, nass_commodity_desc, nass_class_desc,
     nass_statisticcat_desc, nass_short_desc_filter, is_active, header_pattern)
VALUES
    ('peanut', 'edible_usage_candy', 'Edible Peanuts Used in Peanut Candy',
     'LB', 'thousand pounds', 0.001,
     0, FALSE, 'PEANUTS', NULL,
     'USAGE', 'PEANUTS, SHELLED, EDIBLE, CANDY - USAGE, MEASURED IN LB, RAW BASIS',
     TRUE, 'Edible peanuts used in peanut candy');

-- Col F: Edible peanuts used in peanut snacks (Page 7)
INSERT INTO silver.crush_attribute_reference
    (commodity, attribute_code, display_name, source_unit, display_unit, conversion_factor,
     spreadsheet_column, is_formula, nass_commodity_desc, nass_class_desc,
     nass_statisticcat_desc, nass_short_desc_filter, is_active, header_pattern)
VALUES
    ('peanut', 'edible_usage_snacks', 'Edible Peanuts Used in Peanut Snacks',
     'LB', 'thousand pounds', 0.001,
     0, FALSE, 'PEANUTS', NULL,
     'USAGE', 'PEANUTS, SHELLED, EDIBLE, SNACKS - USAGE, MEASURED IN LB, RAW BASIS',
     TRUE, 'Edible peanuts used in peanut snacks');

-- Col G: Edible peanuts used in peanut butter (Page 7)
INSERT INTO silver.crush_attribute_reference
    (commodity, attribute_code, display_name, source_unit, display_unit, conversion_factor,
     spreadsheet_column, is_formula, nass_commodity_desc, nass_class_desc,
     nass_statisticcat_desc, nass_short_desc_filter, is_active, header_pattern)
VALUES
    ('peanut', 'edible_usage_peanut_butter', 'Edible Peanuts Used in Peanut Butter',
     'LB', 'thousand pounds', 0.001,
     0, FALSE, 'PEANUTS', NULL,
     'USAGE', 'PEANUTS, SHELLED, EDIBLE, PEANUT BUTTER - USAGE, MEASURED IN LB, RAW BASIS',
     TRUE, 'Edible peanuts used in peanut butter');

-- Col H: Edible peanuts used in other products (Page 7)
INSERT INTO silver.crush_attribute_reference
    (commodity, attribute_code, display_name, source_unit, display_unit, conversion_factor,
     spreadsheet_column, is_formula, nass_commodity_desc, nass_class_desc,
     nass_statisticcat_desc, nass_short_desc_filter, is_active, header_pattern)
VALUES
    ('peanut', 'edible_usage_other', 'Edible Peanuts Used in Other Products',
     'LB', 'thousand pounds', 0.001,
     0, FALSE, 'PEANUTS', NULL,
     'USAGE', 'PEANUTS, SHELLED, EDIBLE, OTHER USES - USAGE, MEASURED IN LB, RAW BASIS',
     TRUE, 'Edible peanuts used in other products');

-- Col I: Total shelled peanuts of all grades (Page 7)
INSERT INTO silver.crush_attribute_reference
    (commodity, attribute_code, display_name, source_unit, display_unit, conversion_factor,
     spreadsheet_column, is_formula, nass_commodity_desc, nass_class_desc,
     nass_statisticcat_desc, nass_short_desc_filter, is_active, header_pattern)
VALUES
    ('peanut', 'shelled_usage_all_grades', 'Total Shelled Peanuts of All Grades',
     'LB', 'thousand pounds', 0.001,
     0, FALSE, 'PEANUTS', NULL,
     'USAGE', 'PEANUTS, SHELLED - USAGE, MEASURED IN LB, RAW BASIS',
     TRUE, 'Total shelled peanuts of all grades');

-- Col J: In shell peanuts (Page 7)
INSERT INTO silver.crush_attribute_reference
    (commodity, attribute_code, display_name, source_unit, display_unit, conversion_factor,
     spreadsheet_column, is_formula, nass_commodity_desc, nass_class_desc,
     nass_statisticcat_desc, nass_short_desc_filter, is_active, header_pattern)
VALUES
    ('peanut', 'in_shell_usage', 'In Shell Peanuts',
     'LB', 'thousand pounds', 0.001,
     0, FALSE, 'PEANUTS', NULL,
     'USAGE', 'PEANUTS, IN SHELL - USAGE, MEASURED IN LB, RAW BASIS',
     TRUE, 'In shell peanuts');

-- Col K: Roasting stock (in shell) peanut production (Page 4)
INSERT INTO silver.crush_attribute_reference
    (commodity, attribute_code, display_name, source_unit, display_unit, conversion_factor,
     spreadsheet_column, is_formula, nass_commodity_desc, nass_class_desc,
     nass_statisticcat_desc, nass_short_desc_filter, is_active, header_pattern)
VALUES
    ('peanut', 'roasting_stock_production', 'Roasting Stock (in Shell) Peanut Production',
     'LB', 'thousand pounds', 0.001,
     0, FALSE, 'PEANUTS', NULL,
     'PRODUCTION', 'PEANUTS, IN SHELL, ROASTING - PRODUCTION, MEASURED IN LB',
     TRUE, 'Roasting stock (in shell) peanut production');

-- Col L: Shelled oil stocks production (Page 4)
INSERT INTO silver.crush_attribute_reference
    (commodity, attribute_code, display_name, source_unit, display_unit, conversion_factor,
     spreadsheet_column, is_formula, nass_commodity_desc, nass_class_desc,
     nass_statisticcat_desc, nass_short_desc_filter, is_active, header_pattern)
VALUES
    ('peanut', 'shelled_oil_stocks_production', 'Shelled Oil Stocks',
     'LB', 'thousand pounds', 0.001,
     0, FALSE, 'PEANUTS', NULL,
     'PRODUCTION', 'PEANUTS, SHELLED, OIL STOCKS - PRODUCTION, MEASURED IN LB',
     TRUE, 'Shelled oil stocks');

-- ── Peanut Meal block ──

-- Col R: Cake and meal production (Page 5)
INSERT INTO silver.crush_attribute_reference
    (commodity, attribute_code, display_name, source_unit, display_unit, conversion_factor,
     spreadsheet_column, is_formula, nass_commodity_desc, nass_class_desc,
     nass_statisticcat_desc, nass_short_desc_filter, is_active, header_pattern)
VALUES
    ('peanut', 'cake_meal_production', 'Cake and Meal Production',
     'LB', 'thousand pounds', 0.001,
     0, FALSE, 'PEANUTS', NULL,
     'PRODUCTION', 'PEANUTS, SHELLED, CRUSHED, CAKE & MEAL - PRODUCTION, MEASURED IN LB',
     TRUE, 'Cake and meal production');

-- Col S: Cake and meal stocks (Page 5) — display in 000 Short Tons
INSERT INTO silver.crush_attribute_reference
    (commodity, attribute_code, display_name, source_unit, display_unit, conversion_factor,
     spreadsheet_column, is_formula, nass_commodity_desc, nass_class_desc,
     nass_statisticcat_desc, nass_short_desc_filter, is_active, header_pattern)
VALUES
    ('peanut', 'cake_meal_stocks', 'Cake and Meal Stocks',
     'LB', '000 ST', 0.0000005,
     0, FALSE, 'PEANUTS', NULL,
     'STOCKS', 'PEANUTS, SHELLED, CRUSHED, CAKE & MEAL - STOCKS, MEASURED IN LB',
     TRUE, 'Cake and meal stocks');

-- ── Peanut Oil block ──

-- Col U: Crude oil production at oil mills (Page 5)
INSERT INTO silver.crush_attribute_reference
    (commodity, attribute_code, display_name, source_unit, display_unit, conversion_factor,
     spreadsheet_column, is_formula, nass_commodity_desc, nass_class_desc,
     nass_statisticcat_desc, nass_short_desc_filter, is_active, header_pattern)
VALUES
    ('peanut', 'crude_oil_production_mills', 'Crude Oil Production',
     'LB', 'thousand pounds', 0.001,
     0, FALSE, 'PEANUTS', NULL,
     'PRODUCTION', 'PEANUTS, SHELLED, CRUSHED, OIL - PRODUCTION, MEASURED IN LB',
     TRUE, 'Crude oil production');

-- Col Z: Crude oil stocks at oil mills (Page 5) — distinct from col Y (refinery)
INSERT INTO silver.crush_attribute_reference
    (commodity, attribute_code, display_name, source_unit, display_unit, conversion_factor,
     spreadsheet_column, is_formula, nass_commodity_desc, nass_class_desc,
     nass_statisticcat_desc, nass_short_desc_filter, is_active, header_pattern)
VALUES
    ('peanut', 'crude_oil_stocks_mills', 'Crude Oil Stocks',
     'LB', 'thousand pounds', 0.001,
     0, FALSE, 'PEANUTS', NULL,
     'STOCKS', 'PEANUTS, SHELLED, CRUSHED, OIL - STOCKS, MEASURED IN LB',
     TRUE, 'Crude oil stocks');
