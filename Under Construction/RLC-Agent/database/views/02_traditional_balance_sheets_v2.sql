-- ============================================================================
-- TRADITIONAL BALANCE SHEET VIEWS - V2
-- Based on actual metric names from bronze.sqlite_commodity_balance_sheets
-- ============================================================================
-- Run this in pgAdmin or psql to create the views
-- ============================================================================

-- ============================================================================
-- VIEW 1: US SOYBEANS TRADITIONAL BALANCE SHEET
-- Marketing Year: September 1 - August 31 (same as corn)
-- ============================================================================
DROP VIEW IF EXISTS gold.us_soybeans_balance_sheet_v2;
CREATE VIEW gold.us_soybeans_balance_sheet_v2 AS
SELECT
    marketing_year,

    -- SUPPLY (Sep 1 - Aug 31 marketing year)
    MAX(CASE WHEN metric IN ('Beginning Stocks (September 1)', 'Carryin (September 1)',
                             'Beginning Stocks (October 1)', 'Carryin (October 1)') THEN value END) as beginning_stocks,
    MAX(CASE WHEN metric = 'Production' THEN value END) as production,
    MAX(CASE WHEN metric = 'Imports' THEN value END) as imports,
    MAX(CASE WHEN metric = 'Total Supply' THEN value END) as total_supply,

    -- DEMAND - Domestic
    MAX(CASE WHEN metric IN ('Crush', 'Crush (October - September)') THEN value END) as crush,
    MAX(CASE WHEN metric = 'Seed' THEN value END) as seed,
    MAX(CASE WHEN metric = 'Residual' THEN value END) as residual,
    MAX(CASE WHEN metric IN ('Domestic Usage', 'Domestic Use') THEN value END) as domestic_total,

    -- DEMAND - Exports
    MAX(CASE WHEN metric = 'Exports' THEN value END) as exports,

    -- TOTAL USE
    MAX(CASE WHEN metric = 'Total Use' THEN value END) as total_use,

    -- ENDING STOCKS (Sep 1 - Aug 31 marketing year)
    MAX(CASE WHEN metric IN ('Ending Stocks (August 31)', 'Carryout (August 31)',
                             'Ending Stocks (September 30)', 'Carryout (September 30)') THEN value END) as ending_stocks,

    -- RATIOS
    MAX(CASE WHEN metric IN ('Stocks/Use', 'Stocks-to-Usage') THEN value END) as stocks_to_use_pct,

    -- AREA & YIELD
    MAX(CASE WHEN metric = 'Planted Area' THEN value END) as planted_area,
    MAX(CASE WHEN metric = 'Harvested Area' THEN value END) as harvested_area,
    MAX(CASE WHEN metric = 'Yield' THEN value END) as yield,

    -- Metadata
    MAX(unit) as unit,
    'US Soybeans' as commodity,
    'United States' as country,
    'Sep 1 - Aug 31' as marketing_year_period

FROM bronze.sqlite_commodity_balance_sheets
WHERE LOWER(commodity) LIKE '%soybean%'
  AND LOWER(commodity) NOT LIKE '%soybean meal%'
  AND LOWER(commodity) NOT LIKE '%soybean oil%'
  AND (LOWER(country) LIKE '%united states%' OR LOWER(country) LIKE '%u.s.%' OR country = 'US')
  AND value IS NOT NULL
GROUP BY marketing_year
ORDER BY marketing_year DESC;


-- ============================================================================
-- VIEW 2: US CORN TRADITIONAL BALANCE SHEET
-- Marketing Year: September 1 - August 31
-- ============================================================================
DROP VIEW IF EXISTS gold.us_corn_balance_sheet_v2;
CREATE VIEW gold.us_corn_balance_sheet_v2 AS
SELECT
    marketing_year,

    -- SUPPLY
    MAX(CASE WHEN metric IN ('Beginning Stocks (September 1)', 'Carryin (September 1)') THEN value END) as beginning_stocks,
    MAX(CASE WHEN metric = 'Production' THEN value END) as production,
    MAX(CASE WHEN metric = 'Imports' THEN value END) as imports,
    MAX(CASE WHEN metric = 'Total Supply' THEN value END) as total_supply,

    -- DEMAND - Domestic
    MAX(CASE WHEN metric = 'Feed Usage' THEN value END) as feed_usage,
    MAX(CASE WHEN metric = 'Food Usage' THEN value END) as food_usage,
    MAX(CASE WHEN metric IN ('Industrail Usage', 'Industrial Usage') THEN value END) as industrial_usage,
    MAX(CASE WHEN metric = 'Seed' THEN value END) as seed,
    MAX(CASE WHEN metric IN ('Domestic Usage', 'Domestic Use') THEN value END) as domestic_total,

    -- DEMAND - Exports
    MAX(CASE WHEN metric = 'Exports' THEN value END) as exports,

    -- TOTAL USE
    MAX(CASE WHEN metric = 'Total Use' THEN value END) as total_use,

    -- ENDING STOCKS
    MAX(CASE WHEN metric IN ('Ending Stocks (August 31)', 'Carryout (August 31)') THEN value END) as ending_stocks,

    -- RATIOS
    MAX(CASE WHEN metric IN ('Stocks/Use', 'Stocks-to-Usage') THEN value END) as stocks_to_use_pct,

    -- AREA & YIELD
    MAX(CASE WHEN metric = 'Planted Area' THEN value END) as planted_area,
    MAX(CASE WHEN metric = 'Harvested Area' THEN value END) as harvested_area,
    MAX(CASE WHEN metric = 'Yield' THEN value END) as yield,

    -- Metadata
    MAX(unit) as unit,
    'US Corn' as commodity,
    'United States' as country

FROM bronze.sqlite_commodity_balance_sheets
WHERE LOWER(commodity) LIKE '%corn%'
  AND (LOWER(country) LIKE '%united states%' OR LOWER(country) LIKE '%u.s.%' OR country = 'US')
  AND value IS NOT NULL
GROUP BY marketing_year
ORDER BY marketing_year DESC;


-- ============================================================================
-- VIEW 3: WORLD SOYBEANS BALANCE SHEET (All Countries)
-- ============================================================================
DROP VIEW IF EXISTS gold.world_soybeans_balance_sheet;
CREATE VIEW gold.world_soybeans_balance_sheet AS
SELECT
    country,
    marketing_year,

    -- SUPPLY
    MAX(CASE WHEN metric IN ('Beginning Stocks (October 1)', 'Carryin (October 1)', 'Beginning Stocks') THEN value END) as beginning_stocks,
    MAX(CASE WHEN metric = 'Production' THEN value END) as production,
    MAX(CASE WHEN metric = 'Imports' THEN value END) as imports,
    MAX(CASE WHEN metric = 'Total Supply' THEN value END) as total_supply,

    -- DEMAND
    MAX(CASE WHEN metric IN ('Crush', 'Crush (October - September)') THEN value END) as crush,
    MAX(CASE WHEN metric IN ('Domestic Usage', 'Domestic Use') THEN value END) as domestic_total,
    MAX(CASE WHEN metric = 'Exports' THEN value END) as exports,
    MAX(CASE WHEN metric = 'Total Use' THEN value END) as total_use,

    -- ENDING STOCKS
    MAX(CASE WHEN metric IN ('Ending Stocks (September 30)', 'Carryout (September 30)', 'Ending Stocks') THEN value END) as ending_stocks,

    -- RATIOS
    MAX(CASE WHEN metric IN ('Stocks/Use', 'Stocks-to-Usage') THEN value END) as stocks_to_use_pct,

    MAX(unit) as unit

FROM bronze.sqlite_commodity_balance_sheets
WHERE LOWER(commodity) LIKE '%soybean%'
  AND value IS NOT NULL
GROUP BY country, marketing_year
ORDER BY country, marketing_year DESC;


-- ============================================================================
-- VIEW 4: GENERIC BALANCE SHEET PIVOT (All Commodities/Countries)
-- Use slicers in Power BI to filter
-- ============================================================================
DROP VIEW IF EXISTS gold.balance_sheet_universal;
CREATE VIEW gold.balance_sheet_universal AS
SELECT
    commodity,
    country,
    marketing_year,

    -- SUPPLY (handle different naming conventions)
    MAX(CASE WHEN metric LIKE 'Beginning Stocks%' OR metric LIKE 'Carryin%' THEN value END) as beginning_stocks,
    MAX(CASE WHEN metric = 'Production' THEN value END) as production,
    MAX(CASE WHEN metric = 'Imports' THEN value END) as imports,
    MAX(CASE WHEN metric = 'Total Supply' THEN value END) as total_supply,

    -- DEMAND
    MAX(CASE WHEN metric LIKE 'Crush%' AND metric NOT LIKE '%Domestic%' THEN value END) as crush,
    MAX(CASE WHEN metric IN ('Feed Usage', 'Feed Use') THEN value END) as feed,
    MAX(CASE WHEN metric IN ('Food Usage', 'Food Use') THEN value END) as food,
    MAX(CASE WHEN metric = 'Seed' THEN value END) as seed,
    MAX(CASE WHEN metric = 'Residual' THEN value END) as residual,
    MAX(CASE WHEN metric IN ('Domestic Usage', 'Domestic Use') THEN value END) as domestic_total,
    MAX(CASE WHEN metric = 'Exports' THEN value END) as exports,
    MAX(CASE WHEN metric = 'Total Use' THEN value END) as total_use,

    -- ENDING STOCKS
    MAX(CASE WHEN metric LIKE 'Ending Stocks%' OR metric LIKE 'Carryout%' THEN value END) as ending_stocks,

    -- RATIOS
    MAX(CASE WHEN metric IN ('Stocks/Use', 'Stocks-to-Usage') THEN value END) as stocks_to_use_pct,

    -- AREA & YIELD
    MAX(CASE WHEN metric = 'Planted Area' OR metric = 'Planted Acres' THEN value END) as planted_area,
    MAX(CASE WHEN metric = 'Harvested Area' OR metric = 'Harvested Acres' THEN value END) as harvested_area,
    MAX(CASE WHEN metric = 'Yield' OR metric LIKE 'Yield (%' THEN value END) as yield,

    MAX(unit) as unit

FROM bronze.sqlite_commodity_balance_sheets
WHERE value IS NOT NULL
  AND metric NOT IN ('January', 'February', 'March', 'April', 'May', 'June',
                     'July', 'August', 'September', 'October', 'November', 'December',
                     'Total', 'Marketing-Year Average', 'Average')
GROUP BY commodity, country, marketing_year
HAVING MAX(CASE WHEN metric = 'Production' THEN value END) IS NOT NULL
    OR MAX(CASE WHEN metric LIKE 'Ending Stocks%' OR metric LIKE 'Carryout%' THEN value END) IS NOT NULL
ORDER BY commodity, country, marketing_year DESC;


-- ============================================================================
-- VIEW 5: BALANCE SHEET WITH YOY CHANGES (For Power BI trend visuals)
-- ============================================================================
DROP VIEW IF EXISTS gold.balance_sheet_with_yoy;
CREATE VIEW gold.balance_sheet_with_yoy AS
WITH base AS (
    SELECT * FROM gold.balance_sheet_universal
),
with_lag AS (
    SELECT
        b.*,
        LAG(b.production) OVER (PARTITION BY b.commodity, b.country ORDER BY b.marketing_year) as prior_production,
        LAG(b.exports) OVER (PARTITION BY b.commodity, b.country ORDER BY b.marketing_year) as prior_exports,
        LAG(b.ending_stocks) OVER (PARTITION BY b.commodity, b.country ORDER BY b.marketing_year) as prior_ending_stocks,
        LAG(b.crush) OVER (PARTITION BY b.commodity, b.country ORDER BY b.marketing_year) as prior_crush
    FROM base b
)
SELECT
    commodity,
    country,
    marketing_year,

    -- Current Values
    beginning_stocks,
    production,
    imports,
    total_supply,
    crush,
    feed,
    food,
    domestic_total,
    exports,
    total_use,
    ending_stocks,
    stocks_to_use_pct,

    -- YoY Changes
    ROUND((production - prior_production)::numeric, 1) as production_yoy_change,
    ROUND((exports - prior_exports)::numeric, 1) as exports_yoy_change,
    ROUND((ending_stocks - prior_ending_stocks)::numeric, 1) as stocks_yoy_change,
    ROUND((crush - prior_crush)::numeric, 1) as crush_yoy_change,

    -- YoY % Changes
    CASE WHEN prior_production > 0
         THEN ROUND(((production - prior_production) / prior_production * 100)::numeric, 1)
    END as production_yoy_pct,
    CASE WHEN prior_exports > 0
         THEN ROUND(((exports - prior_exports) / prior_exports * 100)::numeric, 1)
    END as exports_yoy_pct,
    CASE WHEN prior_ending_stocks > 0
         THEN ROUND(((ending_stocks - prior_ending_stocks) / prior_ending_stocks * 100)::numeric, 1)
    END as stocks_yoy_pct,

    unit
FROM with_lag
ORDER BY commodity, country, marketing_year DESC;


-- ============================================================================
-- VIEW 6: US SOYBEAN MEAL BALANCE SHEET
-- Marketing Year: October 1 - September 30
-- ============================================================================
DROP VIEW IF EXISTS gold.us_soybean_meal_balance_sheet;
CREATE VIEW gold.us_soybean_meal_balance_sheet AS
SELECT
    marketing_year,

    -- SUPPLY (Oct 1 - Sep 30 marketing year)
    MAX(CASE WHEN metric IN ('Beginning Stocks (October 1)', 'Carryin (October 1)') THEN value END) as beginning_stocks,
    MAX(CASE WHEN metric = 'Production' THEN value END) as production,
    MAX(CASE WHEN metric = 'Imports' THEN value END) as imports,
    MAX(CASE WHEN metric = 'Total Supply' THEN value END) as total_supply,

    -- DEMAND
    MAX(CASE WHEN metric IN ('Domestic Usage', 'Domestic Use') THEN value END) as domestic_use,
    MAX(CASE WHEN metric = 'Exports' THEN value END) as exports,
    MAX(CASE WHEN metric = 'Total Use' THEN value END) as total_use,

    -- ENDING STOCKS
    MAX(CASE WHEN metric IN ('Ending Stocks (September 30)', 'Carryout (September 30)') THEN value END) as ending_stocks,

    -- RATIOS
    MAX(CASE WHEN metric IN ('Stocks/Use', 'Stocks-to-Usage', 'Meal Stocks-to-Use') THEN value END) as stocks_to_use_pct,

    MAX(unit) as unit,
    'US Soybean Meal' as commodity,
    'United States' as country,
    'Oct 1 - Sep 30' as marketing_year_period

FROM bronze.sqlite_commodity_balance_sheets
WHERE LOWER(commodity) LIKE '%soybean meal%'
  AND (LOWER(country) LIKE '%united states%' OR LOWER(country) LIKE '%u.s.%' OR country = 'US')
  AND value IS NOT NULL
GROUP BY marketing_year
ORDER BY marketing_year DESC;


-- ============================================================================
-- VIEW 7: US SOYBEAN OIL BALANCE SHEET
-- Marketing Year: October 1 - September 30
-- ============================================================================
DROP VIEW IF EXISTS gold.us_soybean_oil_balance_sheet;
CREATE VIEW gold.us_soybean_oil_balance_sheet AS
SELECT
    marketing_year,

    -- SUPPLY (Oct 1 - Sep 30 marketing year)
    MAX(CASE WHEN metric IN ('Beginning Stocks (October 1)', 'Carryin (October 1)') THEN value END) as beginning_stocks,
    MAX(CASE WHEN metric = 'Production' THEN value END) as production,
    MAX(CASE WHEN metric = 'Imports' THEN value END) as imports,
    MAX(CASE WHEN metric = 'Total Supply' THEN value END) as total_supply,

    -- DEMAND - Biofuels focus
    MAX(CASE WHEN metric LIKE '%Biodiesel%Use%' THEN value END) as biodiesel_use,
    MAX(CASE WHEN metric LIKE '%Renewable Diesel%' THEN value END) as renewable_diesel_use,
    MAX(CASE WHEN metric LIKE '%Biomass-Based%' THEN value END) as biomass_diesel_use,
    MAX(CASE WHEN metric LIKE '%Non-Biodiesel%' OR metric LIKE '%Non-Biofuel%' THEN value END) as non_biofuel_use,
    MAX(CASE WHEN metric IN ('Domestic Usage', 'Domestic Use') THEN value END) as domestic_use,
    MAX(CASE WHEN metric = 'Exports' THEN value END) as exports,
    MAX(CASE WHEN metric = 'Total Use' THEN value END) as total_use,

    -- ENDING STOCKS
    MAX(CASE WHEN metric IN ('Ending Stocks (September 30)', 'Carryout (September 30)') THEN value END) as ending_stocks,
    MAX(CASE WHEN metric LIKE '%Days of Coverage%' OR metric LIKE '%Days of Usage%' THEN value END) as days_of_coverage,

    MAX(unit) as unit,
    'US Soybean Oil' as commodity,
    'United States' as country,
    'Oct 1 - Sep 30' as marketing_year_period

FROM bronze.sqlite_commodity_balance_sheets
WHERE LOWER(commodity) LIKE '%soybean oil%'
  AND (LOWER(country) LIKE '%united states%' OR LOWER(country) LIKE '%u.s.%' OR country = 'US')
  AND value IS NOT NULL
GROUP BY marketing_year
ORDER BY marketing_year DESC;


-- ============================================================================
-- VIEW 8: SOYBEAN OIL & MEAL BALANCE SHEET (Combined - Biofuels focus)
-- ============================================================================
DROP VIEW IF EXISTS gold.soybean_products_balance_sheet;
CREATE VIEW gold.soybean_products_balance_sheet AS
SELECT
    commodity,
    country,
    marketing_year,

    MAX(CASE WHEN metric = 'Production' THEN value END) as production,
    MAX(CASE WHEN metric = 'Imports' THEN value END) as imports,
    MAX(CASE WHEN metric = 'Exports' THEN value END) as exports,
    MAX(CASE WHEN metric IN ('Domestic Usage', 'Domestic Use') THEN value END) as domestic_use,
    MAX(CASE WHEN metric LIKE '%Biodiesel%' THEN value END) as biodiesel_use,
    MAX(CASE WHEN metric LIKE '%Renewable Diesel%' THEN value END) as renewable_diesel_use,
    MAX(CASE WHEN metric LIKE '%Biomass-Based%' THEN value END) as biomass_diesel_use,
    MAX(CASE WHEN metric LIKE '%Non-Biodiesel%' OR metric LIKE '%Non-Biofuel%' THEN value END) as non_biofuel_use,
    MAX(CASE WHEN metric LIKE 'Ending Stocks%' OR metric LIKE '%END-OF-MONTH STOCKS%' THEN value END) as ending_stocks,

    MAX(unit) as unit

FROM bronze.sqlite_commodity_balance_sheets
WHERE (LOWER(commodity) LIKE '%soybean oil%'
    OR LOWER(commodity) LIKE '%soybean meal%'
    OR LOWER(commodity) LIKE '%soy oil%'
    OR LOWER(commodity) LIKE '%canola oil%')
  AND value IS NOT NULL
GROUP BY commodity, country, marketing_year
ORDER BY commodity, country, marketing_year DESC;


-- ============================================================================
-- GRANT SELECT ACCESS
-- ============================================================================
-- GRANT SELECT ON ALL TABLES IN SCHEMA gold TO readonly_user;

-- ============================================================================
-- VERIFY VIEWS CREATED
-- ============================================================================
SELECT
    schemaname,
    viewname
FROM pg_views
WHERE schemaname = 'gold'
  AND viewname LIKE '%balance%'
ORDER BY viewname;
