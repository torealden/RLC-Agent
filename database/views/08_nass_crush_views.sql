-- ============================================================================
-- NASS CRUSH VIEWS - Gold Layer
-- ============================================================================
-- These views prepare NASS Fats & Oils / crushing data for Excel spreadsheet
-- export via VBA (CrushUpdaterSQL.bas).
--
-- Target spreadsheet: us_soy_crush.xlsx ("NASS Crush" sheet)
-- Layout: Row 3 = headers, Row 4 = units, Column A = dates (1st of month)
-- ============================================================================

-- ============================================================================
-- 1. NASS CRUSH MAPPED
-- ============================================================================
-- Joins bronze.nass_processing with silver.crush_attribute_reference to:
--   - Map raw NASS fields to spreadsheet attributes
--   - Apply unit conversion factors
--   - Assign spreadsheet column numbers
--
-- Uses exact short_desc matching for reliable joins.
-- For oil stocks, domaincat_desc disambiguates location-specific records.

CREATE OR REPLACE VIEW gold.nass_crush_mapped AS
SELECT
    bp.year,
    bp.month,
    make_date(bp.year, bp.month, 1) AS month_date,
    car.attribute_code,
    car.display_name,
    car.display_unit,
    car.spreadsheet_column,
    bp.value * car.conversion_factor AS display_value,
    bp.value AS raw_value,
    car.source_unit AS raw_unit,
    bp.source,
    bp.collected_at
FROM bronze.nass_processing bp
JOIN silver.crush_attribute_reference car
    ON bp.commodity_desc = car.nass_commodity_desc
    AND (car.nass_class_desc IS NULL
         OR bp.class_desc = car.nass_class_desc)
    AND bp.statisticcat_desc = car.nass_statisticcat_desc
    AND bp.short_desc = car.nass_short_desc_filter
    AND (car.nass_domaincat_filter IS NULL
         OR bp.domaincat_desc ILIKE '%' || car.nass_domaincat_filter || '%')
WHERE car.is_active = TRUE
  AND car.is_formula = FALSE
  AND bp.month IS NOT NULL;


-- ============================================================================
-- 2. NASS SOY CRUSH MATRIX
-- ============================================================================
-- VBA-ready view filtered to soybeans only.
-- Cross-joins all available months with all active attributes so that
-- suppressed/missing data appears as NULL display_value (VBA writes "D").
-- Returns one row per (year, month, attribute) combination.

CREATE OR REPLACE VIEW gold.nass_soy_crush_matrix AS
SELECT
    m.year,
    m.month,
    m.month_date,
    a.attribute_code,
    a.display_name,
    a.spreadsheet_column,
    d.display_value,
    a.display_unit
FROM (
    -- All months that have ANY soybean data
    SELECT DISTINCT year, month, month_date
    FROM gold.nass_crush_mapped
    WHERE attribute_code IN (
        SELECT attribute_code FROM silver.crush_attribute_reference
        WHERE commodity = 'soybeans' AND is_formula = FALSE AND is_active = TRUE
    )
) m
CROSS JOIN (
    -- All active soybean attributes
    SELECT attribute_code, display_name, spreadsheet_column, display_unit
    FROM silver.crush_attribute_reference
    WHERE commodity = 'soybeans'
      AND is_formula = FALSE
      AND is_active = TRUE
) a
LEFT JOIN gold.nass_crush_mapped d
    ON d.year = m.year
    AND d.month = m.month
    AND d.attribute_code = a.attribute_code
ORDER BY m.year, m.month, a.spreadsheet_column;


-- ============================================================================
-- 3. AVAILABLE MONTHS
-- ============================================================================
-- Helper view showing which months have data

CREATE OR REPLACE VIEW gold.nass_crush_available_months AS
SELECT DISTINCT
    year,
    month,
    month_date
FROM gold.nass_crush_mapped
ORDER BY year, month;


-- ============================================================================
-- GRANT PERMISSIONS
-- ============================================================================
GRANT SELECT ON gold.nass_crush_mapped TO PUBLIC;
GRANT SELECT ON gold.nass_soy_crush_matrix TO PUBLIC;
GRANT SELECT ON gold.nass_crush_available_months TO PUBLIC;
