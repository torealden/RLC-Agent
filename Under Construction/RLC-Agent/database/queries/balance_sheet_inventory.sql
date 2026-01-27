-- ============================================================================
-- BALANCE SHEET DATA INVENTORY QUERIES
-- Run these in pgAdmin or Power BI to understand what data you have
-- ============================================================================

-- 1. ALL COMMODITIES
SELECT DISTINCT commodity, COUNT(*) as records
FROM bronze.sqlite_commodity_balance_sheets
GROUP BY commodity
ORDER BY records DESC;

-- 2. ALL COUNTRIES
SELECT DISTINCT country, COUNT(*) as records
FROM bronze.sqlite_commodity_balance_sheets
GROUP BY country
ORDER BY records DESC;

-- 3. ALL METRICS (Balance Sheet Line Items) - THIS IS KEY
SELECT DISTINCT metric, COUNT(*) as records
FROM bronze.sqlite_commodity_balance_sheets
GROUP BY metric
ORDER BY records DESC;

-- 4. ALL SECTIONS
SELECT DISTINCT section, COUNT(*) as records
FROM bronze.sqlite_commodity_balance_sheets
GROUP BY section
ORDER BY records DESC;

-- 5. MARKETING YEARS AVAILABLE
SELECT DISTINCT marketing_year, COUNT(*) as records
FROM bronze.sqlite_commodity_balance_sheets
GROUP BY marketing_year
ORDER BY marketing_year DESC;

-- 6. DATA COVERAGE SUMMARY (Commodity + Country combinations)
SELECT
    commodity,
    country,
    COUNT(DISTINCT metric) as unique_metrics,
    COUNT(DISTINCT marketing_year) as year_coverage,
    MIN(marketing_year) as earliest_year,
    MAX(marketing_year) as latest_year,
    COUNT(*) as total_records
FROM bronze.sqlite_commodity_balance_sheets
GROUP BY commodity, country
ORDER BY total_records DESC;

-- 7. US SOYBEANS - All metrics available
SELECT DISTINCT metric
FROM bronze.sqlite_commodity_balance_sheets
WHERE LOWER(commodity) LIKE '%soy%'
  AND LOWER(country) LIKE '%united states%'
ORDER BY metric;

-- 8. US SOYBEANS - Sample balance sheet for latest year
SELECT metric, marketing_year, value, unit
FROM bronze.sqlite_commodity_balance_sheets
WHERE LOWER(commodity) LIKE '%soy%'
  AND LOWER(country) LIKE '%united states%'
ORDER BY marketing_year DESC, metric
LIMIT 50;

-- 9. WORLD CORN - All metrics available
SELECT DISTINCT metric
FROM bronze.sqlite_commodity_balance_sheets
WHERE LOWER(commodity) LIKE '%corn%'
ORDER BY metric;

-- 10. CHECK FOR TRADITIONAL BALANCE SHEET COMPONENTS
-- This shows which commodities have the key S&D metrics
SELECT
    commodity,
    country,
    MAX(CASE WHEN LOWER(metric) LIKE '%beginning%stock%' THEN 1 ELSE 0 END) as has_beg_stocks,
    MAX(CASE WHEN LOWER(metric) LIKE '%production%' THEN 1 ELSE 0 END) as has_production,
    MAX(CASE WHEN LOWER(metric) LIKE '%import%' THEN 1 ELSE 0 END) as has_imports,
    MAX(CASE WHEN LOWER(metric) LIKE '%export%' THEN 1 ELSE 0 END) as has_exports,
    MAX(CASE WHEN LOWER(metric) LIKE '%ending%stock%' THEN 1 ELSE 0 END) as has_end_stocks,
    MAX(CASE WHEN LOWER(metric) LIKE '%crush%' OR LOWER(metric) LIKE '%domestic%' THEN 1 ELSE 0 END) as has_domestic_use,
    COUNT(DISTINCT metric) as total_metrics
FROM bronze.sqlite_commodity_balance_sheets
GROUP BY commodity, country
HAVING
    MAX(CASE WHEN LOWER(metric) LIKE '%beginning%stock%' THEN 1 ELSE 0 END) = 1
    OR MAX(CASE WHEN LOWER(metric) LIKE '%ending%stock%' THEN 1 ELSE 0 END) = 1
ORDER BY total_metrics DESC;
