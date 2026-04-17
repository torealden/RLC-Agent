-- =============================================================================
-- Migration 029: Peanut derived formula attributes
-- =============================================================================
-- Col C: "Shelled peanut crush - Farmer stock basis" = col B × 1.33
--
-- Added as is_formula=TRUE row in reference table. The gold view computes
-- the display_value from the base attribute (shelled_peanuts_crushed).
-- =============================================================================

-- Reference row for the formula attribute
INSERT INTO silver.crush_attribute_reference
    (commodity, attribute_code, display_name, source_unit, display_unit, conversion_factor,
     spreadsheet_column, is_formula, nass_commodity_desc, nass_class_desc,
     nass_statisticcat_desc, nass_short_desc_filter, is_active, header_pattern)
VALUES
    ('peanut', 'shelled_crush_farmer_stock_basis', 'Shelled Peanut Crush - Farmer Stock Basis',
     'LB', 'thousand pounds', 0.001,
     0, TRUE, NULL, NULL,
     NULL, NULL,
     TRUE, 'Shelled peanut crush - Farmer stock basis');


-- =============================================================================
-- Extend gold.fats_oils_crush_matrix to include computed formula rows
-- =============================================================================
-- Add a view that computes formula attributes from base attributes,
-- then UNION into the main matrix.

CREATE OR REPLACE VIEW gold.peanut_formula_values AS
SELECT
    base.commodity,
    base.year,
    base.month,
    'shelled_crush_farmer_stock_basis'::varchar(80) AS attribute_code,
    'Shelled peanut crush - Farmer stock basis'::varchar(100) AS header_pattern,
    'Shelled Peanut Crush - Farmer Stock Basis'::varchar(200) AS display_name,
    'thousand pounds'::varchar(50) AS display_unit,
    base.display_value * 1.33 AS display_value
FROM gold.nass_crush_mapped base
WHERE base.attribute_code = 'shelled_peanuts_crushed';


-- Replace fats_oils_crush_matrix to include formula rows
CREATE OR REPLACE VIEW gold.fats_oils_crush_matrix AS
-- Original: non-formula attributes from nass_crush_mapped
SELECT
    m.commodity,
    m.year,
    m.month,
    make_date(m.year, m.month, 1) AS month_date,
    a.attribute_code,
    a.header_pattern,
    a.display_name,
    a.display_unit,
    d.display_value
FROM (
    SELECT DISTINCT commodity, year, month
    FROM gold.nass_crush_mapped
) m
CROSS JOIN (
    SELECT commodity, attribute_code, header_pattern, display_name, display_unit
    FROM silver.crush_attribute_reference
    WHERE is_formula = FALSE AND is_active = TRUE AND header_pattern IS NOT NULL
) a
LEFT JOIN gold.nass_crush_mapped d
    ON d.commodity = a.commodity
    AND d.year = m.year
    AND d.month = m.month
    AND d.attribute_code = a.attribute_code
WHERE m.commodity = a.commodity

UNION ALL

-- Formula attributes (computed from base attributes)
SELECT
    f.commodity,
    f.year,
    f.month,
    make_date(f.year, f.month, 1) AS month_date,
    f.attribute_code,
    f.header_pattern,
    f.display_name,
    f.display_unit,
    f.display_value
FROM gold.peanut_formula_values f

ORDER BY 1, 2, 3, 5;
