-- ============================================================================
-- TRADITIONAL BALANCE SHEET VIEWS
-- ============================================================================
-- These views transform the raw data into traditional S&D balance sheet format
--
-- Balance Sheet Structure:
--   SUPPLY:
--     Beginning Stocks (= Prior Year Ending Stocks)
--     + Production
--     + Imports
--     = Total Supply
--
--   DEMAND:
--     Crush / Food / Seed / Feed / Residual (Domestic Use components)
--     + Exports
--     = Total Demand (Total Disappearance)
--
--   STOCKS:
--     Total Supply - Total Demand = Ending Stocks
--
-- ============================================================================

-- ============================================================================
-- VIEW: US Soybeans Traditional Balance Sheet
-- ============================================================================
CREATE OR REPLACE VIEW gold.us_soybeans_traditional AS
WITH pivoted AS (
    SELECT
        marketing_year,
        -- SUPPLY SIDE
        MAX(CASE WHEN LOWER(metric) LIKE '%beginning%stock%' THEN value END) as beginning_stocks,
        MAX(CASE WHEN LOWER(metric) = 'production'
                  OR (LOWER(metric) LIKE '%production%' AND LOWER(metric) NOT LIKE '%crush%') THEN value END) as production,
        MAX(CASE WHEN LOWER(metric) LIKE '%import%' AND LOWER(metric) NOT LIKE '%export%' THEN value END) as imports,
        MAX(CASE WHEN LOWER(metric) LIKE '%total supply%' OR LOWER(metric) = 'supply, total' THEN value END) as total_supply,

        -- DEMAND SIDE - Domestic Use Components
        MAX(CASE WHEN LOWER(metric) LIKE '%crush%' AND LOWER(metric) NOT LIKE '%residual%' THEN value END) as crush,
        MAX(CASE WHEN LOWER(metric) LIKE '%food%' OR LOWER(metric) LIKE '%food, seed%' THEN value END) as food_seed_residual,
        MAX(CASE WHEN LOWER(metric) LIKE '%seed%' AND LOWER(metric) NOT LIKE '%food%' THEN value END) as seed,
        MAX(CASE WHEN LOWER(metric) LIKE '%residual%' AND LOWER(metric) NOT LIKE '%feed%' THEN value END) as residual,
        MAX(CASE WHEN LOWER(metric) LIKE '%domestic%total%' OR LOWER(metric) LIKE '%total domestic%' THEN value END) as domestic_total,

        -- Exports
        MAX(CASE WHEN LOWER(metric) LIKE '%export%' AND LOWER(metric) NOT LIKE '%import%' THEN value END) as exports,

        -- Total Use
        MAX(CASE WHEN LOWER(metric) LIKE '%total use%'
                  OR LOWER(metric) LIKE '%total disappearance%'
                  OR LOWER(metric) = 'use, total' THEN value END) as total_use,

        -- ENDING STOCKS
        MAX(CASE WHEN LOWER(metric) LIKE '%ending%stock%' THEN value END) as ending_stocks,

        -- RATIOS
        MAX(CASE WHEN LOWER(metric) LIKE '%stocks%use%' OR LOWER(metric) LIKE '%stock-to-use%' THEN value END) as stocks_to_use_ratio,

        -- Get unit
        MAX(unit) as unit
    FROM bronze.sqlite_commodity_balance_sheets
    WHERE LOWER(commodity) LIKE '%soybean%'
      AND LOWER(country) LIKE '%united states%'
      AND value IS NOT NULL
    GROUP BY marketing_year
)
SELECT
    marketing_year,

    -- SUPPLY
    beginning_stocks,
    production,
    imports,
    COALESCE(total_supply,
             COALESCE(beginning_stocks, 0) + COALESCE(production, 0) + COALESCE(imports, 0)) as total_supply,

    -- DEMAND
    crush,
    food_seed_residual,
    COALESCE(domestic_total,
             COALESCE(crush, 0) + COALESCE(food_seed_residual, 0)) as domestic_total,
    exports,
    COALESCE(total_use,
             COALESCE(domestic_total, COALESCE(crush, 0) + COALESCE(food_seed_residual, 0)) + COALESCE(exports, 0)) as total_use,

    -- ENDING STOCKS
    ending_stocks,

    -- CALCULATED STOCKS-TO-USE (%)
    CASE
        WHEN COALESCE(total_use, 1) > 0
        THEN ROUND((ending_stocks / COALESCE(total_use, 1) * 100)::NUMERIC, 1)
    END as calc_stocks_to_use_pct,

    unit,
    'US Soybeans' as commodity,
    'United States' as country
FROM pivoted
WHERE marketing_year IS NOT NULL
ORDER BY marketing_year DESC;

COMMENT ON VIEW gold.us_soybeans_traditional IS
'US Soybeans S&D in traditional balance sheet format. Beginning + Production + Imports = Supply; Crush + FSR + Exports = Use; Supply - Use = Ending';


-- ============================================================================
-- VIEW: US Corn Traditional Balance Sheet
-- ============================================================================
CREATE OR REPLACE VIEW gold.us_corn_traditional AS
WITH pivoted AS (
    SELECT
        marketing_year,
        -- SUPPLY SIDE
        MAX(CASE WHEN LOWER(metric) LIKE '%beginning%stock%' THEN value END) as beginning_stocks,
        MAX(CASE WHEN LOWER(metric) = 'production'
                  OR (LOWER(metric) LIKE '%production%' AND LOWER(metric) NOT LIKE '%ethanol%') THEN value END) as production,
        MAX(CASE WHEN LOWER(metric) LIKE '%import%' AND LOWER(metric) NOT LIKE '%export%' THEN value END) as imports,
        MAX(CASE WHEN LOWER(metric) LIKE '%total supply%' OR LOWER(metric) = 'supply, total' THEN value END) as total_supply,

        -- DEMAND SIDE
        MAX(CASE WHEN LOWER(metric) LIKE '%feed%residual%' OR LOWER(metric) LIKE '%feed and residual%' THEN value END) as feed_residual,
        MAX(CASE WHEN LOWER(metric) LIKE '%ethanol%' AND LOWER(metric) LIKE '%fsi%' THEN value END) as ethanol,
        MAX(CASE WHEN LOWER(metric) LIKE '%food%seed%' AND LOWER(metric) NOT LIKE '%ethanol%' THEN value END) as food_seed_industrial,
        MAX(CASE WHEN LOWER(metric) LIKE '%domestic%total%' OR LOWER(metric) LIKE '%total domestic%' THEN value END) as domestic_total,
        MAX(CASE WHEN LOWER(metric) LIKE '%export%' AND LOWER(metric) NOT LIKE '%import%' THEN value END) as exports,
        MAX(CASE WHEN LOWER(metric) LIKE '%total use%'
                  OR LOWER(metric) LIKE '%total disappearance%' THEN value END) as total_use,

        -- ENDING STOCKS
        MAX(CASE WHEN LOWER(metric) LIKE '%ending%stock%' THEN value END) as ending_stocks,

        MAX(unit) as unit
    FROM bronze.sqlite_commodity_balance_sheets
    WHERE LOWER(commodity) LIKE '%corn%'
      AND LOWER(country) LIKE '%united states%'
      AND value IS NOT NULL
    GROUP BY marketing_year
)
SELECT
    marketing_year,

    -- SUPPLY
    beginning_stocks,
    production,
    imports,
    COALESCE(total_supply,
             COALESCE(beginning_stocks, 0) + COALESCE(production, 0) + COALESCE(imports, 0)) as total_supply,

    -- DEMAND
    feed_residual,
    ethanol,
    food_seed_industrial,
    COALESCE(domestic_total,
             COALESCE(feed_residual, 0) + COALESCE(ethanol, 0) + COALESCE(food_seed_industrial, 0)) as domestic_total,
    exports,
    COALESCE(total_use,
             COALESCE(domestic_total, 0) + COALESCE(exports, 0)) as total_use,

    -- ENDING STOCKS
    ending_stocks,

    -- CALCULATED STOCKS-TO-USE (%)
    CASE
        WHEN COALESCE(total_use, 1) > 0
        THEN ROUND((ending_stocks / COALESCE(total_use, 1) * 100)::NUMERIC, 1)
    END as calc_stocks_to_use_pct,

    unit,
    'US Corn' as commodity,
    'United States' as country
FROM pivoted
WHERE marketing_year IS NOT NULL
ORDER BY marketing_year DESC;


-- ============================================================================
-- VIEW: Generic Balance Sheet Query (works for any commodity/country)
-- Use this as a template in Power BI with parameters
-- ============================================================================
CREATE OR REPLACE VIEW gold.balance_sheet_pivot AS
SELECT
    commodity,
    country,
    marketing_year,
    MAX(CASE WHEN LOWER(metric) LIKE '%beginning%stock%' THEN value END) as beginning_stocks,
    MAX(CASE WHEN LOWER(metric) = 'production'
              OR (LOWER(metric) LIKE '%production%' AND LOWER(metric) NOT LIKE '%ethanol%' AND LOWER(metric) NOT LIKE '%crush%') THEN value END) as production,
    MAX(CASE WHEN LOWER(metric) LIKE '%import%' AND LOWER(metric) NOT LIKE '%export%' THEN value END) as imports,
    MAX(CASE WHEN LOWER(metric) LIKE '%total supply%' OR LOWER(metric) = 'supply, total' THEN value END) as total_supply,
    MAX(CASE WHEN LOWER(metric) LIKE '%crush%' AND LOWER(metric) NOT LIKE '%residual%' THEN value END) as crush,
    MAX(CASE WHEN LOWER(metric) LIKE '%feed%' OR LOWER(metric) LIKE '%food%' OR LOWER(metric) LIKE '%seed%' THEN value END) as domestic_use,
    MAX(CASE WHEN LOWER(metric) LIKE '%export%' AND LOWER(metric) NOT LIKE '%import%' THEN value END) as exports,
    MAX(CASE WHEN LOWER(metric) LIKE '%total use%' OR LOWER(metric) LIKE '%total disappearance%' THEN value END) as total_use,
    MAX(CASE WHEN LOWER(metric) LIKE '%ending%stock%' THEN value END) as ending_stocks,
    MAX(unit) as unit
FROM bronze.sqlite_commodity_balance_sheets
WHERE value IS NOT NULL
GROUP BY commodity, country, marketing_year
ORDER BY commodity, country, marketing_year DESC;

COMMENT ON VIEW gold.balance_sheet_pivot IS
'Generic balance sheet pivot - filter by commodity and country in Power BI';


-- ============================================================================
-- VIEW: Balance Sheet with YoY Changes (Great for Power BI)
-- ============================================================================
CREATE OR REPLACE VIEW gold.balance_sheet_with_changes AS
WITH base AS (
    SELECT * FROM gold.balance_sheet_pivot
),
with_prior AS (
    SELECT
        b.*,
        LAG(b.beginning_stocks) OVER (PARTITION BY b.commodity, b.country ORDER BY b.marketing_year) as prior_beg_stocks,
        LAG(b.production) OVER (PARTITION BY b.commodity, b.country ORDER BY b.marketing_year) as prior_production,
        LAG(b.exports) OVER (PARTITION BY b.commodity, b.country ORDER BY b.marketing_year) as prior_exports,
        LAG(b.ending_stocks) OVER (PARTITION BY b.commodity, b.country ORDER BY b.marketing_year) as prior_ending_stocks
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
    domestic_use,
    exports,
    total_use,
    ending_stocks,

    -- YoY Changes
    production - COALESCE(prior_production, production) as production_change,
    exports - COALESCE(prior_exports, exports) as exports_change,
    ending_stocks - COALESCE(prior_ending_stocks, ending_stocks) as stocks_change,

    -- YoY % Changes
    CASE WHEN COALESCE(prior_production, 0) > 0
         THEN ROUND(((production - prior_production) / prior_production * 100)::NUMERIC, 1)
    END as production_pct_change,
    CASE WHEN COALESCE(prior_exports, 0) > 0
         THEN ROUND(((exports - prior_exports) / prior_exports * 100)::NUMERIC, 1)
    END as exports_pct_change,
    CASE WHEN COALESCE(prior_ending_stocks, 0) > 0
         THEN ROUND(((ending_stocks - prior_ending_stocks) / prior_ending_stocks * 100)::NUMERIC, 1)
    END as stocks_pct_change,

    -- Stocks-to-Use Ratio
    CASE WHEN COALESCE(total_use, 1) > 0
         THEN ROUND((ending_stocks / total_use * 100)::NUMERIC, 1)
    END as stocks_to_use_pct,

    unit
FROM with_prior
ORDER BY commodity, country, marketing_year DESC;

COMMENT ON VIEW gold.balance_sheet_with_changes IS
'Balance sheet with YoY changes and % changes - ideal for Power BI trend analysis';


-- ============================================================================
-- VIEW: Multi-Year Balance Sheet Matrix (for Excel-style view in Power BI)
-- ============================================================================
CREATE OR REPLACE VIEW gold.balance_sheet_matrix AS
SELECT
    commodity,
    country,
    'Beginning Stocks' as line_item,
    1 as sort_order,
    'SUPPLY' as category,
    MAX(CASE WHEN marketing_year LIKE '%2020%' THEN value END) as "2020/21",
    MAX(CASE WHEN marketing_year LIKE '%2021%' THEN value END) as "2021/22",
    MAX(CASE WHEN marketing_year LIKE '%2022%' THEN value END) as "2022/23",
    MAX(CASE WHEN marketing_year LIKE '%2023%' THEN value END) as "2023/24",
    MAX(CASE WHEN marketing_year LIKE '%2024%' THEN value END) as "2024/25"
FROM bronze.sqlite_commodity_balance_sheets
WHERE LOWER(metric) LIKE '%beginning%stock%'
GROUP BY commodity, country

UNION ALL

SELECT
    commodity,
    country,
    'Production' as line_item,
    2 as sort_order,
    'SUPPLY' as category,
    MAX(CASE WHEN marketing_year LIKE '%2020%' THEN value END) as "2020/21",
    MAX(CASE WHEN marketing_year LIKE '%2021%' THEN value END) as "2021/22",
    MAX(CASE WHEN marketing_year LIKE '%2022%' THEN value END) as "2022/23",
    MAX(CASE WHEN marketing_year LIKE '%2023%' THEN value END) as "2023/24",
    MAX(CASE WHEN marketing_year LIKE '%2024%' THEN value END) as "2024/25"
FROM bronze.sqlite_commodity_balance_sheets
WHERE LOWER(metric) = 'production'
   OR (LOWER(metric) LIKE '%production%' AND LOWER(metric) NOT LIKE '%ethanol%')
GROUP BY commodity, country

UNION ALL

SELECT
    commodity,
    country,
    'Imports' as line_item,
    3 as sort_order,
    'SUPPLY' as category,
    MAX(CASE WHEN marketing_year LIKE '%2020%' THEN value END) as "2020/21",
    MAX(CASE WHEN marketing_year LIKE '%2021%' THEN value END) as "2021/22",
    MAX(CASE WHEN marketing_year LIKE '%2022%' THEN value END) as "2022/23",
    MAX(CASE WHEN marketing_year LIKE '%2023%' THEN value END) as "2023/24",
    MAX(CASE WHEN marketing_year LIKE '%2024%' THEN value END) as "2024/25"
FROM bronze.sqlite_commodity_balance_sheets
WHERE LOWER(metric) LIKE '%import%' AND LOWER(metric) NOT LIKE '%export%'
GROUP BY commodity, country

UNION ALL

SELECT
    commodity,
    country,
    'Exports' as line_item,
    5 as sort_order,
    'DEMAND' as category,
    MAX(CASE WHEN marketing_year LIKE '%2020%' THEN value END) as "2020/21",
    MAX(CASE WHEN marketing_year LIKE '%2021%' THEN value END) as "2021/22",
    MAX(CASE WHEN marketing_year LIKE '%2022%' THEN value END) as "2022/23",
    MAX(CASE WHEN marketing_year LIKE '%2023%' THEN value END) as "2023/24",
    MAX(CASE WHEN marketing_year LIKE '%2024%' THEN value END) as "2024/25"
FROM bronze.sqlite_commodity_balance_sheets
WHERE LOWER(metric) LIKE '%export%' AND LOWER(metric) NOT LIKE '%import%'
GROUP BY commodity, country

UNION ALL

SELECT
    commodity,
    country,
    'Ending Stocks' as line_item,
    10 as sort_order,
    'STOCKS' as category,
    MAX(CASE WHEN marketing_year LIKE '%2020%' THEN value END) as "2020/21",
    MAX(CASE WHEN marketing_year LIKE '%2021%' THEN value END) as "2021/22",
    MAX(CASE WHEN marketing_year LIKE '%2022%' THEN value END) as "2022/23",
    MAX(CASE WHEN marketing_year LIKE '%2023%' THEN value END) as "2023/24",
    MAX(CASE WHEN marketing_year LIKE '%2024%' THEN value END) as "2024/25"
FROM bronze.sqlite_commodity_balance_sheets
WHERE LOWER(metric) LIKE '%ending%stock%'
GROUP BY commodity, country

ORDER BY commodity, country, sort_order;

COMMENT ON VIEW gold.balance_sheet_matrix IS
'Excel-style balance sheet matrix with years as columns - use Matrix visual in Power BI';
