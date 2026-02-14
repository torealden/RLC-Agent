-- ============================================================================
-- US SOYBEANS DASHBOARD - SQL VIEWS
-- ============================================================================
-- These views prepare data for the Power BI dashboard
-- Run this file against the rlc_commodities database
-- ============================================================================

-- ============================================================================
-- 1. USDA COMP VIEW - Formatted for WASDE Comparison Matrix
-- ============================================================================
-- Creates the exact format: Category | MY USDA | Change | RLC for 3 marketing years

CREATE OR REPLACE VIEW gold.usda_comp_soybeans AS
WITH current_my AS (
    -- Determine current marketing year (Sep-Aug)
    SELECT CASE
        WHEN EXTRACT(MONTH FROM CURRENT_DATE) >= 9 THEN EXTRACT(YEAR FROM CURRENT_DATE) + 1
        ELSE EXTRACT(YEAR FROM CURRENT_DATE)
    END AS current_marketing_year
),
latest_reports AS (
    -- Get the most recent report for each marketing year
    SELECT DISTINCT ON (marketing_year)
        marketing_year,
        report_date,
        area_planted,
        area_harvested,
        yield,
        beginning_stocks,
        production,
        imports,
        total_supply,
        feed_dom_consumption AS crush,  -- For soybeans, this is crush
        exports,
        ending_stocks,
        CASE WHEN total_distribution > 0
             THEN ROUND((ending_stocks / total_distribution * 100)::numeric, 1)
             ELSE NULL END AS stocks_use_pct
    FROM bronze.fas_psd
    WHERE commodity = 'soybeans'
    AND country_code = 'US'
    ORDER BY marketing_year DESC, report_date DESC
),
prior_reports AS (
    -- Get the prior month's report for calculating changes
    SELECT
        lr.marketing_year,
        lr.report_date AS current_report_date,
        pr.report_date AS prior_report_date,
        lr.area_planted - COALESCE(pr.area_planted, lr.area_planted) AS area_planted_chg,
        lr.area_harvested - COALESCE(pr.area_harvested, lr.area_harvested) AS area_harvested_chg,
        lr.yield - COALESCE(pr.yield, lr.yield) AS yield_chg,
        lr.beginning_stocks - COALESCE(pr.beginning_stocks, lr.beginning_stocks) AS beg_stocks_chg,
        lr.production - COALESCE(pr.production, lr.production) AS production_chg,
        lr.imports - COALESCE(pr.imports, lr.imports) AS imports_chg,
        lr.total_supply - COALESCE(pr.total_supply, lr.total_supply) AS total_supply_chg,
        lr.crush - COALESCE(pr.feed_dom_consumption, lr.crush) AS crush_chg,
        lr.exports - COALESCE(pr.exports, lr.exports) AS exports_chg,
        lr.ending_stocks - COALESCE(pr.ending_stocks, lr.ending_stocks) AS ending_stocks_chg
    FROM latest_reports lr
    LEFT JOIN bronze.fas_psd pr
        ON pr.commodity = 'soybeans'
        AND pr.country_code = 'US'
        AND pr.marketing_year = lr.marketing_year
        AND pr.report_date < lr.report_date
        AND pr.report_date = (
            SELECT MAX(report_date)
            FROM bronze.fas_psd
            WHERE commodity = 'soybeans'
            AND country_code = 'US'
            AND marketing_year = lr.marketing_year
            AND report_date < lr.report_date
        )
),
user_estimates AS (
    -- Get RLC estimates
    SELECT
        marketing_year,
        beginning_stocks AS rlc_beg_stocks,
        production AS rlc_production,
        imports AS rlc_imports,
        total_supply AS rlc_total_supply,
        crush AS rlc_crush,
        exports AS rlc_exports,
        ending_stocks AS rlc_ending_stocks,
        stocks_use_ratio AS rlc_su_ratio
    FROM silver.user_sd_estimate
    WHERE commodity = 'soybeans'
    AND country = 'US'
    AND is_current = TRUE
)
SELECT
    lr.marketing_year,
    CASE
        WHEN lr.marketing_year = (SELECT current_marketing_year FROM current_my) THEN 'Current'
        WHEN lr.marketing_year = (SELECT current_marketing_year FROM current_my) - 1 THEN 'Prior'
        WHEN lr.marketing_year = (SELECT current_marketing_year FROM current_my) - 2 THEN 'Two Years Ago'
        ELSE 'Historical'
    END AS year_type,
    (lr.marketing_year - 1)::text || '/' || RIGHT(lr.marketing_year::text, 2) AS my_label,
    lr.report_date,

    -- USDA Values
    lr.area_planted AS usda_area_planted,
    lr.area_harvested AS usda_area_harvested,
    lr.yield AS usda_yield,
    lr.beginning_stocks AS usda_beg_stocks,
    lr.production AS usda_production,
    lr.imports AS usda_imports,
    lr.total_supply AS usda_total_supply,
    lr.crush AS usda_crush,
    lr.exports AS usda_exports,
    lr.ending_stocks AS usda_ending_stocks,
    lr.stocks_use_pct AS usda_su_pct,

    -- Changes from Prior Report
    pr.area_planted_chg,
    pr.area_harvested_chg,
    pr.yield_chg,
    pr.beg_stocks_chg,
    pr.production_chg,
    pr.imports_chg,
    pr.total_supply_chg,
    pr.crush_chg,
    pr.exports_chg,
    pr.ending_stocks_chg,

    -- RLC Estimates
    ue.rlc_beg_stocks,
    ue.rlc_production,
    ue.rlc_imports,
    ue.rlc_total_supply,
    ue.rlc_crush,
    ue.rlc_exports,
    ue.rlc_ending_stocks,
    ue.rlc_su_ratio

FROM latest_reports lr
LEFT JOIN prior_reports pr ON lr.marketing_year = pr.marketing_year
LEFT JOIN user_estimates ue ON lr.marketing_year = ue.marketing_year
WHERE lr.marketing_year >= (SELECT current_marketing_year FROM current_my) - 2
ORDER BY lr.marketing_year DESC;


-- ============================================================================
-- 2. CRUSH PACE TRACKING - Monthly vs Annual Target
-- ============================================================================

CREATE OR REPLACE VIEW gold.soybean_crush_pace AS
WITH current_my AS (
    SELECT CASE
        WHEN EXTRACT(MONTH FROM CURRENT_DATE) >= 9 THEN EXTRACT(YEAR FROM CURRENT_DATE) + 1
        ELSE EXTRACT(YEAR FROM CURRENT_DATE)
    END AS current_marketing_year
),
annual_target AS (
    -- Get USDA annual crush estimate
    SELECT
        marketing_year,
        feed_dom_consumption AS annual_crush_target
    FROM bronze.fas_psd
    WHERE commodity = 'soybeans'
    AND country_code = 'US'
    AND marketing_year = (SELECT current_marketing_year FROM current_my)
    ORDER BY report_date DESC
    LIMIT 1
),
monthly_crush AS (
    SELECT
        marketing_year,
        month,
        calendar_year,
        realized_value AS monthly_crush,
        source,
        report_date
    FROM silver.monthly_realized
    WHERE commodity = 'soybeans'
    AND attribute = 'crush'
)
SELECT
    mc.marketing_year,
    mc.month,
    mc.calendar_year,
    mc.monthly_crush,
    at.annual_crush_target,
    at.annual_crush_target / 12 AS monthly_target,
    SUM(mc.monthly_crush) OVER (
        PARTITION BY mc.marketing_year
        ORDER BY mc.calendar_year, mc.month
    ) AS ytd_crush,
    ROUND(
        SUM(mc.monthly_crush) OVER (
            PARTITION BY mc.marketing_year
            ORDER BY mc.calendar_year, mc.month
        ) / NULLIF(at.annual_crush_target, 0) * 100, 1
    ) AS ytd_pct_of_annual,
    -- Marketing year month (1=Sep, 12=Aug)
    CASE
        WHEN mc.month >= 9 THEN mc.month - 8
        ELSE mc.month + 4
    END AS my_month
FROM monthly_crush mc
LEFT JOIN annual_target at ON mc.marketing_year = at.marketing_year
WHERE mc.marketing_year >= (SELECT current_marketing_year FROM current_my) - 1
ORDER BY mc.marketing_year DESC, mc.calendar_year, mc.month;


-- ============================================================================
-- 3. EXPORT PACE COMPARISON - Current vs Prior Year vs 5-Year Avg
-- ============================================================================

CREATE OR REPLACE VIEW gold.soybean_export_pace AS
WITH current_my AS (
    SELECT CASE
        WHEN EXTRACT(MONTH FROM CURRENT_DATE) >= 9 THEN EXTRACT(YEAR FROM CURRENT_DATE) + 1
        ELSE EXTRACT(YEAR FROM CURRENT_DATE)
    END AS current_marketing_year
),
weekly_exports AS (
    SELECT
        marketing_year,
        week_ending,
        accumulated_exports,
        -- Calculate week of marketing year
        EXTRACT(WEEK FROM week_ending) -
            EXTRACT(WEEK FROM make_date(
                CASE WHEN EXTRACT(MONTH FROM week_ending) >= 9
                     THEN EXTRACT(YEAR FROM week_ending)::int
                     ELSE EXTRACT(YEAR FROM week_ending)::int - 1
                END, 9, 1
            )) + 1 AS my_week
    FROM bronze.fas_export_sales
    WHERE commodity = 'soybeans'
),
five_year_avg AS (
    SELECT
        my_week,
        AVG(accumulated_exports) AS avg_5yr_exports
    FROM weekly_exports
    WHERE marketing_year BETWEEN
        (SELECT current_marketing_year FROM current_my) - 5
        AND (SELECT current_marketing_year FROM current_my) - 1
    GROUP BY my_week
)
SELECT
    we.marketing_year,
    we.week_ending,
    we.my_week,
    we.accumulated_exports,
    fya.avg_5yr_exports,
    we.accumulated_exports - fya.avg_5yr_exports AS vs_5yr_avg,
    ROUND((we.accumulated_exports / NULLIF(fya.avg_5yr_exports, 0) - 1) * 100, 1) AS vs_5yr_avg_pct,
    CASE
        WHEN we.marketing_year = (SELECT current_marketing_year FROM current_my) THEN 'Current'
        WHEN we.marketing_year = (SELECT current_marketing_year FROM current_my) - 1 THEN 'Prior'
        ELSE 'Historical'
    END AS year_type
FROM weekly_exports we
LEFT JOIN five_year_avg fya ON we.my_week = fya.my_week
WHERE we.marketing_year >= (SELECT current_marketing_year FROM current_my) - 1
ORDER BY we.marketing_year DESC, we.week_ending DESC;


-- ============================================================================
-- 4. CRUSH MARGIN CALCULATION
-- ============================================================================
-- Board Crush = (Meal Price × Meal Yield) + (Oil Price × Oil Yield) - Soybean Price

CREATE OR REPLACE VIEW gold.soybean_crush_margin AS
WITH yields AS (
    -- Get latest yields from Fats & Oils report
    SELECT
        calendar_year,
        month,
        MAX(CASE WHEN attribute = 'meal_yield' THEN realized_value END) AS meal_yield,
        MAX(CASE WHEN attribute = 'oil_yield' THEN realized_value END) AS oil_yield
    FROM silver.monthly_realized
    WHERE commodity = 'soybeans'
    AND attribute IN ('meal_yield', 'oil_yield')
    GROUP BY calendar_year, month
),
prices AS (
    -- Get prices (assuming we have a futures_price table)
    SELECT
        date,
        commodity,
        settlement_price
    FROM silver.futures_price
    WHERE commodity IN ('soybeans', 'soybean_meal', 'soybean_oil')
)
SELECT
    p_soy.date,
    p_soy.settlement_price AS soybean_price,
    p_meal.settlement_price AS meal_price,
    p_oil.settlement_price AS oil_price,
    COALESCE(y.meal_yield, 47.84) AS meal_yield,  -- Default from your screenshot
    COALESCE(y.oil_yield, 11.79) AS oil_yield,    -- Default from your screenshot
    -- Board Crush Calculation
    -- Meal is $/ton, need to convert: ($/ton × lbs/bu) / 2000 = $/bu from meal
    -- Oil is cents/lb, need to convert: (cents/lb × lbs/bu) / 100 = $/bu from oil
    ROUND((
        (p_meal.settlement_price * COALESCE(y.meal_yield, 47.84) / 2000) +
        (p_oil.settlement_price * COALESCE(y.oil_yield, 11.79) / 100) -
        p_soy.settlement_price
    )::numeric, 4) AS gross_crush_margin
FROM prices p_soy
LEFT JOIN prices p_meal
    ON p_meal.date = p_soy.date
    AND p_meal.commodity = 'soybean_meal'
LEFT JOIN prices p_oil
    ON p_oil.date = p_soy.date
    AND p_oil.commodity = 'soybean_oil'
LEFT JOIN yields y
    ON EXTRACT(YEAR FROM p_soy.date) = y.calendar_year
    AND EXTRACT(MONTH FROM p_soy.date) = y.month
WHERE p_soy.commodity = 'soybeans'
ORDER BY p_soy.date DESC;


-- ============================================================================
-- 5. STALE DATA TRACKING - For Grey Overlay Logic
-- ============================================================================
-- Tracks when each data source was last updated vs expected update frequency

CREATE OR REPLACE VIEW gold.data_freshness AS
SELECT
    'fas_psd' AS data_source,
    'Monthly' AS expected_frequency,
    MAX(collected_at) AS last_updated,
    -- Expected update: 12th of each month (WASDE day)
    CASE
        WHEN EXTRACT(DAY FROM CURRENT_DATE) >= 12
        THEN make_date(EXTRACT(YEAR FROM CURRENT_DATE)::int, EXTRACT(MONTH FROM CURRENT_DATE)::int, 12)
        ELSE make_date(
            EXTRACT(YEAR FROM CURRENT_DATE - INTERVAL '1 month')::int,
            EXTRACT(MONTH FROM CURRENT_DATE - INTERVAL '1 month')::int, 12
        )
    END AS expected_update_date,
    CASE
        WHEN MAX(collected_at)::date >= CASE
            WHEN EXTRACT(DAY FROM CURRENT_DATE) >= 12
            THEN make_date(EXTRACT(YEAR FROM CURRENT_DATE)::int, EXTRACT(MONTH FROM CURRENT_DATE)::int, 12)
            ELSE make_date(
                EXTRACT(YEAR FROM CURRENT_DATE - INTERVAL '1 month')::int,
                EXTRACT(MONTH FROM CURRENT_DATE - INTERVAL '1 month')::int, 12
            )
        END
        THEN FALSE
        ELSE TRUE
    END AS is_stale
FROM bronze.fas_psd
WHERE commodity = 'soybeans'

UNION ALL

SELECT
    'monthly_realized' AS data_source,
    'Monthly' AS expected_frequency,
    MAX(collected_at) AS last_updated,
    -- Expected: 15th-16th of month
    make_date(EXTRACT(YEAR FROM CURRENT_DATE)::int, EXTRACT(MONTH FROM CURRENT_DATE)::int, 15) AS expected_update_date,
    CASE
        WHEN MAX(collected_at)::date >= CURRENT_DATE - INTERVAL '45 days'
        THEN FALSE
        ELSE TRUE
    END AS is_stale
FROM silver.monthly_realized
WHERE commodity = 'soybeans'

UNION ALL

SELECT
    'fas_export_sales' AS data_source,
    'Weekly' AS expected_frequency,
    MAX(collected_at) AS last_updated,
    -- Expected: Every Friday
    CURRENT_DATE - EXTRACT(DOW FROM CURRENT_DATE)::int + 5 AS expected_update_date,
    CASE
        WHEN MAX(collected_at)::date >= CURRENT_DATE - INTERVAL '10 days'
        THEN FALSE
        ELSE TRUE
    END AS is_stale
FROM bronze.fas_export_sales
WHERE commodity = 'soybeans';


-- ============================================================================
-- 6. KPI SUMMARY - All key metrics in one view
-- ============================================================================

CREATE OR REPLACE VIEW gold.soybean_kpi_summary AS
WITH current_my AS (
    SELECT CASE
        WHEN EXTRACT(MONTH FROM CURRENT_DATE) >= 9 THEN EXTRACT(YEAR FROM CURRENT_DATE) + 1
        ELSE EXTRACT(YEAR FROM CURRENT_DATE)
    END AS current_marketing_year
),
psd_current AS (
    SELECT *
    FROM bronze.fas_psd
    WHERE commodity = 'soybeans'
    AND country_code = 'US'
    AND marketing_year = (SELECT current_marketing_year FROM current_my)
    ORDER BY report_date DESC
    LIMIT 1
),
psd_prior AS (
    SELECT *
    FROM bronze.fas_psd
    WHERE commodity = 'soybeans'
    AND country_code = 'US'
    AND marketing_year = (SELECT current_marketing_year FROM current_my) - 1
    ORDER BY report_date DESC
    LIMIT 1
)
SELECT
    (SELECT current_marketing_year FROM current_my) AS marketing_year,
    pc.production AS us_production,
    pp.production AS prior_production,
    ROUND(((pc.production - pp.production) / NULLIF(pp.production, 0) * 100)::numeric, 1) AS production_yoy_pct,

    pc.feed_dom_consumption AS crush,
    pp.feed_dom_consumption AS prior_crush,
    ROUND(((pc.feed_dom_consumption - pp.feed_dom_consumption) / NULLIF(pp.feed_dom_consumption, 0) * 100)::numeric, 1) AS crush_yoy_pct,

    pc.exports,
    pp.exports AS prior_exports,
    ROUND(((pc.exports - pp.exports) / NULLIF(pp.exports, 0) * 100)::numeric, 1) AS exports_yoy_pct,

    pc.ending_stocks,
    pp.ending_stocks AS prior_ending_stocks,
    ROUND(((pc.ending_stocks - pp.ending_stocks) / NULLIF(pp.ending_stocks, 0) * 100)::numeric, 1) AS stocks_yoy_pct,

    ROUND((pc.ending_stocks / NULLIF(pc.total_distribution, 0) * 100)::numeric, 1) AS stocks_use_pct,
    CASE
        WHEN pc.ending_stocks / NULLIF(pc.total_distribution, 0) < 0.08 THEN 'CRITICAL'
        WHEN pc.ending_stocks / NULLIF(pc.total_distribution, 0) < 0.12 THEN 'TIGHT'
        WHEN pc.ending_stocks / NULLIF(pc.total_distribution, 0) < 0.18 THEN 'BALANCED'
        ELSE 'COMFORTABLE'
    END AS su_label

FROM psd_current pc, psd_prior pp;


-- ============================================================================
-- Grant permissions
-- ============================================================================
GRANT SELECT ON gold.usda_comp_soybeans TO PUBLIC;
GRANT SELECT ON gold.soybean_crush_pace TO PUBLIC;
GRANT SELECT ON gold.soybean_export_pace TO PUBLIC;
GRANT SELECT ON gold.soybean_crush_margin TO PUBLIC;
GRANT SELECT ON gold.data_freshness TO PUBLIC;
GRANT SELECT ON gold.soybean_kpi_summary TO PUBLIC;
