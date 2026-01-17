-- ============================================================================
-- GOLD VIEWS FOR ERS DATA
-- ============================================================================
-- Creates Power BI-ready views from bronze.ers_oilcrops_raw and bronze.ers_wheat_raw
-- Run: psql -h localhost -U postgres -d rlc_commodities -f database/views/04_ers_gold_views.sql
-- ============================================================================

-- Ensure gold schema exists
CREATE SCHEMA IF NOT EXISTS gold;

-- ============================================================================
-- VIEW 1: US SOYBEAN BALANCE SHEET
-- Classic supply/demand table format for Power BI
-- ============================================================================
DROP VIEW IF EXISTS gold.us_soybean_balance_sheet CASCADE;
CREATE VIEW gold.us_soybean_balance_sheet AS
SELECT
    marketing_year,
    -- Supply
    MAX(CASE WHEN attribute_desc = 'Beginning stocks' AND unit_desc = 'Million bushels' THEN amount END) AS beginning_stocks_mil_bu,
    MAX(CASE WHEN attribute_desc = 'Production' AND unit_desc = 'Million bushels' THEN amount END) AS production_mil_bu,
    MAX(CASE WHEN attribute_desc = 'Imports' AND unit_desc = 'Million bushels' THEN amount END) AS imports_mil_bu,
    MAX(CASE WHEN attribute_desc = 'Total supply' AND unit_desc = 'Million bushels' THEN amount END) AS total_supply_mil_bu,
    -- Demand
    MAX(CASE WHEN attribute_desc = 'Crush' AND unit_desc = 'Million bushels' THEN amount END) AS crush_mil_bu,
    MAX(CASE WHEN attribute_desc = 'Exports' AND unit_desc = 'Million bushels' THEN amount END) AS exports_mil_bu,
    MAX(CASE WHEN attribute_desc = 'Seed' AND unit_desc = 'Million bushels' THEN amount END) AS seed_mil_bu,
    MAX(CASE WHEN attribute_desc = 'Residual' AND unit_desc = 'Million bushels' THEN amount END) AS residual_mil_bu,
    MAX(CASE WHEN attribute_desc = 'Total disappearance' AND unit_desc = 'Million bushels' THEN amount END) AS total_use_mil_bu,
    -- Ending Position
    MAX(CASE WHEN attribute_desc = 'Ending stocks' AND unit_desc = 'Million bushels' THEN amount END) AS ending_stocks_mil_bu,
    -- Prices
    MAX(CASE WHEN attribute_desc = 'Season-average price received by farmers' THEN amount END) AS farm_price_usd_bu,
    -- Calculated metrics
    CASE
        WHEN MAX(CASE WHEN attribute_desc = 'Total disappearance' AND unit_desc = 'Million bushels' THEN amount END) > 0
        THEN ROUND(
            MAX(CASE WHEN attribute_desc = 'Ending stocks' AND unit_desc = 'Million bushels' THEN amount END) /
            MAX(CASE WHEN attribute_desc = 'Total disappearance' AND unit_desc = 'Million bushels' THEN amount END) * 100, 1
        )
    END AS stocks_to_use_pct
FROM bronze.ers_oilcrops_raw
WHERE commodity = 'Soybeans'
  AND geography_desc = 'United States'
  AND marketing_year IS NOT NULL
  AND marketing_year ~ '^\d{4}/\d{2}$'
GROUP BY marketing_year
ORDER BY marketing_year DESC;

COMMENT ON VIEW gold.us_soybean_balance_sheet IS
'US Soybean supply and demand balance sheet by marketing year. Source: USDA ERS Oil Crops Outlook.';

-- ============================================================================
-- VIEW 2: US SOYBEAN MEAL BALANCE SHEET
-- ============================================================================
DROP VIEW IF EXISTS gold.us_soybean_meal_balance_sheet CASCADE;
CREATE VIEW gold.us_soybean_meal_balance_sheet AS
SELECT
    marketing_year,
    MAX(CASE WHEN attribute_desc = 'Beginning stocks' THEN amount END) AS beginning_stocks_thou_st,
    MAX(CASE WHEN attribute_desc = 'Production' THEN amount END) AS production_thou_st,
    MAX(CASE WHEN attribute_desc = 'Imports' THEN amount END) AS imports_thou_st,
    MAX(CASE WHEN attribute_desc = 'Total supply' THEN amount END) AS total_supply_thou_st,
    MAX(CASE WHEN attribute_desc = 'Domestic disappearance' THEN amount END) AS domestic_use_thou_st,
    MAX(CASE WHEN attribute_desc = 'Exports' THEN amount END) AS exports_thou_st,
    MAX(CASE WHEN attribute_desc = 'Ending stocks' THEN amount END) AS ending_stocks_thou_st,
    MAX(CASE WHEN attribute_desc LIKE '%price%Decatur%' THEN amount END) AS decatur_price_usd_st
FROM bronze.ers_oilcrops_raw
WHERE commodity = 'Soybean meal'
  AND geography_desc = 'United States'
  AND marketing_year IS NOT NULL
  AND marketing_year ~ '^\d{4}/\d{2}$'
  AND unit_desc = 'Thousand short tons'
GROUP BY marketing_year
ORDER BY marketing_year DESC;

COMMENT ON VIEW gold.us_soybean_meal_balance_sheet IS
'US Soybean Meal supply and demand. Units: Thousand short tons.';

-- ============================================================================
-- VIEW 3: US SOYBEAN OIL BALANCE SHEET
-- ============================================================================
DROP VIEW IF EXISTS gold.us_soybean_oil_balance_sheet CASCADE;
CREATE VIEW gold.us_soybean_oil_balance_sheet AS
SELECT
    marketing_year,
    MAX(CASE WHEN attribute_desc = 'Beginning stocks' THEN amount END) AS beginning_stocks_mil_lbs,
    MAX(CASE WHEN attribute_desc = 'Production' THEN amount END) AS production_mil_lbs,
    MAX(CASE WHEN attribute_desc = 'Imports' THEN amount END) AS imports_mil_lbs,
    MAX(CASE WHEN attribute_desc = 'Total supply' THEN amount END) AS total_supply_mil_lbs,
    MAX(CASE WHEN attribute_desc LIKE '%Biodiesel%' OR attribute_desc LIKE '%biofuel%' THEN amount END) AS biodiesel_use_mil_lbs,
    MAX(CASE WHEN attribute_desc LIKE '%Food%' OR attribute_desc LIKE '%feed%industrial%' THEN amount END) AS food_use_mil_lbs,
    MAX(CASE WHEN attribute_desc = 'Exports' THEN amount END) AS exports_mil_lbs,
    MAX(CASE WHEN attribute_desc = 'Ending stocks' THEN amount END) AS ending_stocks_mil_lbs,
    MAX(CASE WHEN attribute_desc LIKE '%price%Decatur%' THEN amount END) AS decatur_price_cents_lb
FROM bronze.ers_oilcrops_raw
WHERE commodity = 'Soybean oil'
  AND geography_desc = 'United States'
  AND marketing_year IS NOT NULL
  AND marketing_year ~ '^\d{4}/\d{2}$'
  AND unit_desc = 'Million pounds'
GROUP BY marketing_year
ORDER BY marketing_year DESC;

COMMENT ON VIEW gold.us_soybean_oil_balance_sheet IS
'US Soybean Oil supply and demand. Units: Million pounds.';

-- ============================================================================
-- VIEW 4: US WHEAT BALANCE SHEET (All Classes)
-- ============================================================================
DROP VIEW IF EXISTS gold.us_wheat_balance_sheet CASCADE;
CREATE VIEW gold.us_wheat_balance_sheet AS
SELECT
    marketing_year,
    -- Sort column: extract start year as integer for proper chronological sorting
    CAST(SUBSTRING(marketing_year FROM 1 FOR 4) AS INTEGER) AS marketing_year_sort,
    MAX(CASE WHEN attribute_desc = 'Beginning stocks' THEN amount END) AS beginning_stocks_mil_bu,
    MAX(CASE WHEN attribute_desc = 'Production' THEN amount END) AS production_mil_bu,
    MAX(CASE WHEN attribute_desc = 'Imports' THEN amount END) AS imports_mil_bu,
    MAX(CASE WHEN attribute_desc = 'Total supply' THEN amount END) AS total_supply_mil_bu,
    MAX(CASE WHEN attribute_desc = 'Food use' THEN amount END) AS food_use_mil_bu,
    MAX(CASE WHEN attribute_desc = 'Feed and residual use' THEN amount END) AS feed_residual_mil_bu,
    MAX(CASE WHEN attribute_desc = 'Exports' THEN amount END) AS exports_mil_bu,
    MAX(CASE WHEN attribute_desc = 'Total disappearance' THEN amount END) AS total_use_mil_bu,
    MAX(CASE WHEN attribute_desc = 'Ending stocks' THEN amount END) AS ending_stocks_mil_bu,
    -- Calculated
    CASE
        WHEN MAX(CASE WHEN attribute_desc = 'Total disappearance' THEN amount END) > 0
        THEN ROUND(
            MAX(CASE WHEN attribute_desc = 'Ending stocks' THEN amount END) /
            MAX(CASE WHEN attribute_desc = 'Total disappearance' THEN amount END) * 100, 1
        )
    END AS stocks_to_use_pct
FROM bronze.ers_wheat_raw
WHERE commodity_desc = 'Wheat'
  AND commodity_desc2 = 'All wheat'
  AND geography_desc = 'United States'
  AND marketing_year IS NOT NULL
  AND marketing_year ~ '^\d{4}/\d{2}$'
  AND unit_desc = 'Million bushels'
GROUP BY marketing_year
ORDER BY CAST(SUBSTRING(marketing_year FROM 1 FOR 4) AS INTEGER) DESC;

COMMENT ON VIEW gold.us_wheat_balance_sheet IS
'US All Wheat supply and demand. Units: Million bushels.';

-- ============================================================================
-- VIEW 5: OILSEED SUPPLY COMPARISON (Multi-commodity)
-- ============================================================================
DROP VIEW IF EXISTS gold.us_oilseed_comparison CASCADE;
CREATE VIEW gold.us_oilseed_comparison AS
SELECT
    marketing_year,
    commodity,
    MAX(CASE WHEN attribute_desc = 'Production' THEN amount END) AS production,
    MAX(CASE WHEN attribute_desc = 'Crush' THEN amount END) AS crush,
    MAX(CASE WHEN attribute_desc = 'Exports' THEN amount END) AS exports,
    MAX(CASE WHEN attribute_desc = 'Ending stocks' THEN amount END) AS ending_stocks,
    MAX(unit_desc) AS unit
FROM bronze.ers_oilcrops_raw
WHERE commodity IN ('Soybeans', 'Canola', 'Sunflowerseed', 'Cottonseed', 'Peanuts', 'Flaxseed')
  AND geography_desc = 'United States'
  AND marketing_year IS NOT NULL
  AND marketing_year ~ '^\d{4}/\d{2}$'
  AND attribute_desc IN ('Production', 'Crush', 'Exports', 'Ending stocks')
GROUP BY marketing_year, commodity
ORDER BY marketing_year DESC, commodity;

COMMENT ON VIEW gold.us_oilseed_comparison IS
'Side-by-side comparison of major US oilseeds.';

-- ============================================================================
-- VIEW 6: VEGETABLE OIL COMPARISON
-- ============================================================================
DROP VIEW IF EXISTS gold.us_vegetable_oil_comparison CASCADE;
CREATE VIEW gold.us_vegetable_oil_comparison AS
SELECT
    marketing_year,
    commodity,
    MAX(CASE WHEN attribute_desc = 'Production' THEN amount END) AS production_mil_lbs,
    MAX(CASE WHEN attribute_desc = 'Imports' THEN amount END) AS imports_mil_lbs,
    MAX(CASE WHEN attribute_desc = 'Exports' THEN amount END) AS exports_mil_lbs,
    MAX(CASE WHEN attribute_desc = 'Ending stocks' THEN amount END) AS ending_stocks_mil_lbs
FROM bronze.ers_oilcrops_raw
WHERE commodity IN ('Soybean oil', 'Corn oil', 'Canola oil', 'Sunflowerseed oil', 'Cottonseed oil')
  AND geography_desc = 'United States'
  AND marketing_year IS NOT NULL
  AND marketing_year ~ '^\d{4}/\d{2}$'
  AND unit_desc = 'Million pounds'
GROUP BY marketing_year, commodity
ORDER BY marketing_year DESC, commodity;

COMMENT ON VIEW gold.us_vegetable_oil_comparison IS
'Comparison of major US vegetable oils. Units: Million pounds.';

-- ============================================================================
-- VIEW 7: QUARTERLY SOYBEAN STOCKS
-- ============================================================================
DROP VIEW IF EXISTS gold.us_soybean_quarterly_stocks CASCADE;
CREATE VIEW gold.us_soybean_quarterly_stocks AS
SELECT
    marketing_year,
    timeperiod_desc AS quarter_date,
    MAX(CASE WHEN attribute_desc = 'On-farm storage' THEN amount END) AS on_farm_thou_bu,
    MAX(CASE WHEN attribute_desc = 'Off-farm storage' THEN amount END) AS off_farm_thou_bu,
    MAX(CASE WHEN attribute_desc = 'Total storage' THEN amount END) AS total_thou_bu
FROM bronze.ers_oilcrops_raw
WHERE commodity = 'Soybeans'
  AND geography_desc = 'United States'
  AND table_number = '1'
  AND marketing_year IS NOT NULL
GROUP BY marketing_year, timeperiod_desc
ORDER BY marketing_year DESC, timeperiod_desc;

COMMENT ON VIEW gold.us_soybean_quarterly_stocks IS
'US Soybean quarterly stocks position. Units: Thousand bushels.';

-- ============================================================================
-- VIEW 8: HISTORICAL PRICES
-- ============================================================================
DROP VIEW IF EXISTS gold.us_commodity_prices CASCADE;
CREATE VIEW gold.us_commodity_prices AS
SELECT
    marketing_year,
    'Soybeans' AS commodity,
    amount AS price,
    'USD/bu' AS unit
FROM bronze.ers_oilcrops_raw
WHERE commodity = 'Soybeans'
  AND attribute_desc = 'Season-average price received by farmers'
  AND geography_desc = 'United States'
  AND marketing_year ~ '^\d{4}/\d{2}$'

UNION ALL

SELECT
    marketing_year,
    'Soybean Meal' AS commodity,
    amount AS price,
    'USD/short ton' AS unit
FROM bronze.ers_oilcrops_raw
WHERE commodity = 'Soybean meal'
  AND attribute_desc LIKE '%price%Decatur%'
  AND marketing_year ~ '^\d{4}/\d{2}$'

UNION ALL

SELECT
    marketing_year,
    'Soybean Oil' AS commodity,
    amount AS price,
    'cents/lb' AS unit
FROM bronze.ers_oilcrops_raw
WHERE commodity = 'Soybean oil'
  AND attribute_desc LIKE '%price%Decatur%'
  AND marketing_year ~ '^\d{4}/\d{2}$'

ORDER BY marketing_year DESC, commodity;

COMMENT ON VIEW gold.us_commodity_prices IS
'Historical commodity prices by marketing year.';

-- ============================================================================
-- VIEW 9: WHEAT BY CLASS
-- ============================================================================
DROP VIEW IF EXISTS gold.us_wheat_by_class CASCADE;
CREATE VIEW gold.us_wheat_by_class AS
SELECT
    marketing_year,
    -- Sort column: extract start year as integer for proper chronological sorting
    CAST(SUBSTRING(marketing_year FROM 1 FOR 4) AS INTEGER) AS marketing_year_sort,
    commodity_desc2 AS wheat_class,
    MAX(CASE WHEN attribute_desc = 'Production' THEN amount END) AS production_mil_bu,
    MAX(CASE WHEN attribute_desc = 'Exports' THEN amount END) AS exports_mil_bu
FROM bronze.ers_wheat_raw
WHERE commodity_desc = 'Wheat'
  AND commodity_desc2 IN ('Hard red winter', 'Hard red spring', 'Soft red winter', 'White', 'Durum')
  AND geography_desc = 'United States'
  AND marketing_year IS NOT NULL
  AND marketing_year ~ '^\d{4}/\d{2}$'
  AND unit_desc = 'Million bushels'
GROUP BY marketing_year, commodity_desc2
ORDER BY CAST(SUBSTRING(marketing_year FROM 1 FOR 4) AS INTEGER) DESC, commodity_desc2;

COMMENT ON VIEW gold.us_wheat_by_class IS
'US Wheat production and exports by class.';

-- ============================================================================
-- VIEW 10: DASHBOARD SUMMARY STATS
-- ============================================================================
DROP VIEW IF EXISTS gold.commodity_dashboard_stats CASCADE;
CREATE VIEW gold.commodity_dashboard_stats AS
WITH latest_year AS (
    SELECT MAX(marketing_year) as my FROM bronze.ers_oilcrops_raw
    WHERE marketing_year ~ '^\d{4}/\d{2}$'
)
SELECT
    'Soybeans' AS commodity,
    (SELECT my FROM latest_year) AS marketing_year,
    MAX(CASE WHEN attribute_desc = 'Production' AND unit_desc = 'Million bushels' THEN amount END) AS production,
    MAX(CASE WHEN attribute_desc = 'Exports' AND unit_desc = 'Million bushels' THEN amount END) AS exports,
    MAX(CASE WHEN attribute_desc = 'Crush' AND unit_desc = 'Million bushels' THEN amount END) AS domestic_use,
    MAX(CASE WHEN attribute_desc = 'Ending stocks' AND unit_desc = 'Million bushels' THEN amount END) AS ending_stocks,
    'Million bushels' AS unit
FROM bronze.ers_oilcrops_raw, latest_year
WHERE commodity = 'Soybeans'
  AND geography_desc = 'United States'
  AND marketing_year = latest_year.my

UNION ALL

SELECT
    'Soybean Meal' AS commodity,
    (SELECT my FROM latest_year) AS marketing_year,
    MAX(CASE WHEN attribute_desc = 'Production' THEN amount END) AS production,
    MAX(CASE WHEN attribute_desc = 'Exports' THEN amount END) AS exports,
    MAX(CASE WHEN attribute_desc = 'Domestic disappearance' THEN amount END) AS domestic_use,
    MAX(CASE WHEN attribute_desc = 'Ending stocks' THEN amount END) AS ending_stocks,
    'Thousand short tons' AS unit
FROM bronze.ers_oilcrops_raw, latest_year
WHERE commodity = 'Soybean meal'
  AND geography_desc = 'United States'
  AND marketing_year = latest_year.my
  AND unit_desc = 'Thousand short tons'

UNION ALL

SELECT
    'Soybean Oil' AS commodity,
    (SELECT my FROM latest_year) AS marketing_year,
    MAX(CASE WHEN attribute_desc = 'Production' THEN amount END) AS production,
    MAX(CASE WHEN attribute_desc = 'Exports' THEN amount END) AS exports,
    MAX(CASE WHEN attribute_desc LIKE '%Biodiesel%' THEN amount END) AS domestic_use,
    MAX(CASE WHEN attribute_desc = 'Ending stocks' THEN amount END) AS ending_stocks,
    'Million pounds' AS unit
FROM bronze.ers_oilcrops_raw, latest_year
WHERE commodity = 'Soybean oil'
  AND geography_desc = 'United States'
  AND marketing_year = latest_year.my
  AND unit_desc = 'Million pounds';

COMMENT ON VIEW gold.commodity_dashboard_stats IS
'Summary statistics for dashboard cards showing latest marketing year data.';

-- ============================================================================
-- VIEW 11: YEAR-OVER-YEAR CHANGES
-- ============================================================================
DROP VIEW IF EXISTS gold.soybean_yoy_changes CASCADE;
CREATE VIEW gold.soybean_yoy_changes AS
WITH yearly_data AS (
    SELECT
        marketing_year,
        MAX(CASE WHEN attribute_desc = 'Production' AND unit_desc = 'Million bushels' THEN amount END) AS production,
        MAX(CASE WHEN attribute_desc = 'Exports' AND unit_desc = 'Million bushels' THEN amount END) AS exports,
        MAX(CASE WHEN attribute_desc = 'Crush' AND unit_desc = 'Million bushels' THEN amount END) AS crush,
        MAX(CASE WHEN attribute_desc = 'Ending stocks' AND unit_desc = 'Million bushels' THEN amount END) AS ending_stocks
    FROM bronze.ers_oilcrops_raw
    WHERE commodity = 'Soybeans'
      AND geography_desc = 'United States'
      AND marketing_year ~ '^\d{4}/\d{2}$'
    GROUP BY marketing_year
)
SELECT
    y.marketing_year,
    y.production,
    y.exports,
    y.crush,
    y.ending_stocks,
    -- Year-over-year changes
    y.production - LAG(y.production) OVER (ORDER BY y.marketing_year) AS production_change,
    y.exports - LAG(y.exports) OVER (ORDER BY y.marketing_year) AS exports_change,
    y.crush - LAG(y.crush) OVER (ORDER BY y.marketing_year) AS crush_change,
    y.ending_stocks - LAG(y.ending_stocks) OVER (ORDER BY y.marketing_year) AS stocks_change,
    -- Percent changes
    ROUND((y.production - LAG(y.production) OVER (ORDER BY y.marketing_year)) /
          NULLIF(LAG(y.production) OVER (ORDER BY y.marketing_year), 0) * 100, 1) AS production_pct_change,
    ROUND((y.exports - LAG(y.exports) OVER (ORDER BY y.marketing_year)) /
          NULLIF(LAG(y.exports) OVER (ORDER BY y.marketing_year), 0) * 100, 1) AS exports_pct_change
FROM yearly_data y
ORDER BY y.marketing_year DESC;

COMMENT ON VIEW gold.soybean_yoy_changes IS
'US Soybean year-over-year changes for trend analysis.';

-- ============================================================================
-- GRANT PERMISSIONS
-- ============================================================================
GRANT SELECT ON ALL TABLES IN SCHEMA gold TO PUBLIC;

-- ============================================================================
-- VERIFICATION
-- ============================================================================
\echo ''
\echo '=== GOLD VIEWS CREATED ==='
\echo 'Available views:'
SELECT schemaname, viewname FROM pg_views WHERE schemaname = 'gold' ORDER BY viewname;

\echo ''
\echo '=== SAMPLE: US Soybean Balance Sheet (last 5 years) ==='
SELECT marketing_year, production_mil_bu, crush_mil_bu, exports_mil_bu, ending_stocks_mil_bu, stocks_to_use_pct
FROM gold.us_soybean_balance_sheet
LIMIT 5;

\echo ''
\echo '=== SAMPLE: Dashboard Stats ==='
SELECT * FROM gold.commodity_dashboard_stats;
