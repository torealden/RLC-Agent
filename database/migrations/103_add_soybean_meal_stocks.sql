-- Migration 103: Add missing soybean meal_stocks reference row
--
-- Bug: column O ("Meal Stocks") in soy_crush tab was unfilled because
-- silver.crush_attribute_reference had no row mapping it to the
-- 'CAKE & MEAL, SOYBEAN - STOCKS, MEASURED IN TONS' NASS short_desc.
-- The data has been in bronze.nass_processing since 2015 — it just
-- wasn't wired into the gold matrix.

INSERT INTO silver.crush_attribute_reference (
    commodity, attribute_code, display_name,
    source_unit, display_unit, conversion_factor,
    spreadsheet_column, is_formula,
    nass_commodity_desc, nass_class_desc, nass_statisticcat_desc,
    nass_short_desc_filter, nass_domaincat_filter,
    is_active, header_pattern
) VALUES (
    'soybeans', 'meal_stocks', 'Meal Stocks',
    'TONS', '000 tons', 0.001,
    15, FALSE,
    'CAKE & MEAL', 'SOYBEAN', 'STOCKS',
    'CAKE & MEAL, SOYBEAN - STOCKS, MEASURED IN TONS', NULL,
    TRUE, 'Meal Stocks'
)
ON CONFLICT DO NOTHING;

-- Cross-check other oilseeds for the same gap.
-- Canola has 'CAKE & MEAL, CANOLA - STOCKS, MEASURED IN TONS' (66 rows in bronze).
INSERT INTO silver.crush_attribute_reference (
    commodity, attribute_code, display_name,
    source_unit, display_unit, conversion_factor,
    spreadsheet_column, is_formula,
    nass_commodity_desc, nass_class_desc, nass_statisticcat_desc,
    nass_short_desc_filter, nass_domaincat_filter,
    is_active, header_pattern
) VALUES (
    'canola', 'meal_stocks', 'Meal Stocks',
    'TONS', '000 tons', 0.001,
    15, FALSE,
    'CAKE & MEAL', 'CANOLA', 'STOCKS',
    'CAKE & MEAL, CANOLA - STOCKS, MEASURED IN TONS', NULL,
    TRUE, 'Meal Stocks'
)
ON CONFLICT DO NOTHING;

-- Cottonseed has 'CAKE & MEAL, COTTONSEED - STOCKS, MEASURED IN TONS' (131 rows in bronze).
INSERT INTO silver.crush_attribute_reference (
    commodity, attribute_code, display_name,
    source_unit, display_unit, conversion_factor,
    spreadsheet_column, is_formula,
    nass_commodity_desc, nass_class_desc, nass_statisticcat_desc,
    nass_short_desc_filter, nass_domaincat_filter,
    is_active, header_pattern
) VALUES (
    'cottonseed', 'meal_stocks', 'Meal Stocks',
    'TONS', '000 tons', 0.001,
    15, FALSE,
    'CAKE & MEAL', 'COTTONSEED', 'STOCKS',
    'CAKE & MEAL, COTTONSEED - STOCKS, MEASURED IN TONS', NULL,
    TRUE, 'Meal Stocks'
)
ON CONFLICT DO NOTHING;
