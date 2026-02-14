-- ============================================================================
-- TRADE EXPORT VIEWS - Gold Layer
-- ============================================================================
-- These views prepare trade data for Excel spreadsheet export
-- ============================================================================

-- ============================================================================
-- 1. TRADE DATA WITH COUNTRY MAPPING
-- ============================================================================
-- Joins census trade data with country reference for proper naming/ordering

CREATE OR REPLACE VIEW gold.trade_export_mapped AS
SELECT
    ct.year,
    ct.month,
    ct.flow,
    ct.hs_code,
    cr.commodity_group,
    cr.commodity_name,
    cr.display_unit,
    ct.country_code,
    ct.country_name AS census_country_name,
    COALESCE(tcr.country_name, ct.country_name) AS standard_country_name,
    tcr.region,
    tcr.region_sort_order,
    tcr.country_sort_order,
    tcr.spreadsheet_row,
    tcr.is_regional_total,
    ct.value_usd,
    ct.quantity AS quantity_raw,
    -- Apply conversion factor (result in 1,000 bushels)
    -- conversion_factor gives million bushels, multiply by 1000 for 1000 bu
    ct.quantity * cr.conversion_factor * 1000 AS quantity_converted,
    cr.conversion_factor,
    -- Calculate marketing year (Sep-Aug for grains/oilseeds)
    CASE
        WHEN ct.month >= 9 THEN ct.year || '/' || RIGHT((ct.year + 1)::TEXT, 2)
        ELSE (ct.year - 1) || '/' || RIGHT(ct.year::TEXT, 2)
    END AS marketing_year,
    -- Marketing year for sorting
    CASE
        WHEN ct.month >= 9 THEN ct.year + 1
        ELSE ct.year
    END AS marketing_year_end
FROM bronze.census_trade ct
LEFT JOIN silver.trade_commodity_reference cr
    ON ct.hs_code = cr.hs_code_10
    AND UPPER(ct.flow) = cr.flow_type
LEFT JOIN silver.trade_country_reference tcr
    ON UPPER(ct.country_name) = UPPER(tcr.country_name)
    OR UPPER(ct.country_name) = UPPER(tcr.country_name_alt)
WHERE cr.is_active = TRUE;

-- ============================================================================
-- 2. MONTHLY TRADE BY COUNTRY
-- ============================================================================
-- Aggregated monthly data ready for spreadsheet columns

CREATE OR REPLACE VIEW gold.trade_monthly_by_country AS
SELECT
    commodity_group,
    flow,
    standard_country_name AS country_name,
    region,
    region_sort_order,
    country_sort_order,
    spreadsheet_row,
    is_regional_total,
    year,
    month,
    marketing_year,
    marketing_year_end,
    display_unit,
    SUM(quantity_converted) AS quantity,
    SUM(value_usd) AS value_usd
FROM gold.trade_export_mapped
WHERE standard_country_name IS NOT NULL
GROUP BY
    commodity_group, flow, standard_country_name, region,
    region_sort_order, country_sort_order, spreadsheet_row, is_regional_total,
    year, month, marketing_year, marketing_year_end, display_unit
ORDER BY region_sort_order, country_sort_order, year, month;

-- ============================================================================
-- 3. MARKETING YEAR TOTALS BY COUNTRY
-- ============================================================================
-- Accumulated totals for marketing year columns in spreadsheet

CREATE OR REPLACE VIEW gold.trade_my_totals_by_country AS
SELECT
    commodity_group,
    flow,
    standard_country_name AS country_name,
    region,
    region_sort_order,
    country_sort_order,
    spreadsheet_row,
    is_regional_total,
    marketing_year,
    marketing_year_end,
    display_unit,
    SUM(quantity_converted) AS quantity,
    SUM(value_usd) AS value_usd
FROM gold.trade_export_mapped
WHERE standard_country_name IS NOT NULL
GROUP BY
    commodity_group, flow, standard_country_name, region,
    region_sort_order, country_sort_order, spreadsheet_row, is_regional_total,
    marketing_year, marketing_year_end, display_unit
ORDER BY region_sort_order, country_sort_order, marketing_year_end;

-- ============================================================================
-- 4. REGIONAL TOTALS - Monthly
-- ============================================================================
-- Calculate regional subtotals for each month

CREATE OR REPLACE VIEW gold.trade_regional_monthly AS
SELECT
    commodity_group,
    flow,
    region,
    region_sort_order,
    year,
    month,
    marketing_year,
    marketing_year_end,
    display_unit,
    SUM(quantity_converted) AS quantity,
    SUM(value_usd) AS value_usd,
    COUNT(DISTINCT standard_country_name) AS country_count
FROM gold.trade_export_mapped
WHERE is_regional_total = FALSE  -- Don't double-count
  AND standard_country_name IS NOT NULL
GROUP BY
    commodity_group, flow, region, region_sort_order,
    year, month, marketing_year, marketing_year_end, display_unit
ORDER BY region_sort_order, year, month;

-- ============================================================================
-- 5. REGIONAL TOTALS - Marketing Year
-- ============================================================================

CREATE OR REPLACE VIEW gold.trade_regional_my AS
SELECT
    commodity_group,
    flow,
    region,
    region_sort_order,
    marketing_year,
    marketing_year_end,
    display_unit,
    SUM(quantity_converted) AS quantity,
    SUM(value_usd) AS value_usd,
    COUNT(DISTINCT standard_country_name) AS country_count
FROM gold.trade_export_mapped
WHERE is_regional_total = FALSE
  AND standard_country_name IS NOT NULL
GROUP BY
    commodity_group, flow, region, region_sort_order,
    marketing_year, marketing_year_end, display_unit
ORDER BY region_sort_order, marketing_year_end;

-- ============================================================================
-- 6. WORLD TOTALS - Monthly
-- ============================================================================

CREATE OR REPLACE VIEW gold.trade_world_monthly AS
SELECT
    commodity_group,
    flow,
    year,
    month,
    marketing_year,
    marketing_year_end,
    display_unit,
    SUM(quantity_converted) AS quantity,
    SUM(value_usd) AS value_usd,
    COUNT(DISTINCT standard_country_name) AS country_count
FROM gold.trade_export_mapped
WHERE is_regional_total = FALSE
  AND standard_country_name IS NOT NULL
GROUP BY
    commodity_group, flow, year, month,
    marketing_year, marketing_year_end, display_unit
ORDER BY year, month;

-- ============================================================================
-- 7. COMPLETE TRADE MATRIX
-- ============================================================================
-- Full matrix with countries in correct order, including regional totals
-- This is what the Python script will query

CREATE OR REPLACE VIEW gold.trade_export_matrix AS
WITH country_data AS (
    SELECT
        commodity_group,
        flow,
        country_name,
        region,
        region_sort_order,
        country_sort_order,
        spreadsheet_row,
        FALSE AS is_regional_total,
        year,
        month,
        marketing_year,
        marketing_year_end,
        display_unit,
        quantity,
        value_usd
    FROM gold.trade_monthly_by_country
    WHERE is_regional_total = FALSE
),
regional_totals AS (
    SELECT
        commodity_group,
        flow,
        rr.region_name AS country_name,
        region,
        region_sort_order,
        0 AS country_sort_order,  -- Regional totals come first
        -- Get spreadsheet row for regional total
        (SELECT spreadsheet_row FROM silver.trade_country_reference
         WHERE region = rm.region AND is_regional_total = TRUE LIMIT 1) AS spreadsheet_row,
        TRUE AS is_regional_total,
        year,
        month,
        marketing_year,
        marketing_year_end,
        display_unit,
        quantity,
        value_usd
    FROM gold.trade_regional_monthly rm
    JOIN silver.trade_region_reference rr ON rm.region = rr.region_code
),
world_total AS (
    -- Use the actual WORLD TOTAL from Census data (via monthly_by_country)
    -- instead of summing individual countries (which may miss some)
    SELECT
        commodity_group,
        flow,
        country_name,
        'WORLD' AS region,
        99 AS region_sort_order,
        0 AS country_sort_order,
        spreadsheet_row,
        is_regional_total,
        year,
        month,
        marketing_year,
        marketing_year_end,
        display_unit,
        quantity,
        value_usd
    FROM gold.trade_monthly_by_country
    WHERE country_name = 'WORLD TOTAL'
)
SELECT * FROM country_data
UNION ALL
SELECT * FROM regional_totals
UNION ALL
SELECT * FROM world_total
ORDER BY region_sort_order, country_sort_order, year, month;

-- ============================================================================
-- 8. PIVOT HELPER - Get all available months
-- ============================================================================

CREATE OR REPLACE VIEW gold.trade_available_months AS
SELECT DISTINCT
    commodity_group,
    flow,
    year,
    month,
    marketing_year,
    marketing_year_end,
    make_date(year, month, 1) AS month_date
FROM gold.trade_export_mapped
ORDER BY year, month;

-- ============================================================================
-- 9. PIVOT HELPER - Get all available marketing years
-- ============================================================================

CREATE OR REPLACE VIEW gold.trade_available_my AS
SELECT DISTINCT
    commodity_group,
    flow,
    marketing_year,
    marketing_year_end
FROM gold.trade_export_mapped
ORDER BY marketing_year_end;

-- ============================================================================
-- GRANT PERMISSIONS
-- ============================================================================
GRANT SELECT ON gold.trade_export_mapped TO PUBLIC;
GRANT SELECT ON gold.trade_monthly_by_country TO PUBLIC;
GRANT SELECT ON gold.trade_my_totals_by_country TO PUBLIC;
GRANT SELECT ON gold.trade_regional_monthly TO PUBLIC;
GRANT SELECT ON gold.trade_regional_my TO PUBLIC;
GRANT SELECT ON gold.trade_world_monthly TO PUBLIC;
GRANT SELECT ON gold.trade_export_matrix TO PUBLIC;
GRANT SELECT ON gold.trade_available_months TO PUBLIC;
GRANT SELECT ON gold.trade_available_my TO PUBLIC;
