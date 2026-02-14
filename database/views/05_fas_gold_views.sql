-- =============================================================================
-- FAS Gold Views
-- S&D Balance Sheets, Variance Tracking, and Export Analysis
-- =============================================================================
-- Created: 2026-01-29
-- Purpose: Provide Excel-compatible balance sheet views and variance analysis
--          for tracking realized vs expected S&D data throughout the MY
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS gold;

-- =============================================================================
-- VARIANCE TRACKING: Realized vs Expected Monthly Data
-- =============================================================================
CREATE OR REPLACE VIEW gold.sd_variance_tracker AS
WITH monthly_summary AS (
    SELECT
        r.commodity,
        r.country,
        r.marketing_year,
        r.month,
        r.calendar_year,
        r.attribute,
        r.realized_value,
        r.unit,
        r.source AS realized_source,
        r.report_date AS realized_report_date,
        r.is_preliminary,
        e.expected_value,
        e.confidence AS expected_confidence,
        r.realized_value - COALESCE(e.expected_value, 0) AS variance,
        CASE
            WHEN COALESCE(e.expected_value, 0) != 0
            THEN ROUND(((r.realized_value - e.expected_value) / e.expected_value * 100)::numeric, 2)
            ELSE NULL
        END AS variance_pct
    FROM silver.monthly_realized r
    LEFT JOIN silver.monthly_expectation e
        ON r.commodity = e.commodity
        AND r.country = e.country
        AND r.marketing_year = e.marketing_year
        AND r.month = e.month
        AND r.attribute = e.attribute
        AND e.is_current = TRUE
)
SELECT * FROM monthly_summary
ORDER BY commodity, country, marketing_year, month, attribute;

COMMENT ON VIEW gold.sd_variance_tracker IS
    'Tracks variance between realized monthly S&D data and user expectations. Positive variance = actuals exceeded forecast.';


-- =============================================================================
-- ANNUAL TRACKER: YTD Realized + Remaining Projection
-- =============================================================================
CREATE OR REPLACE VIEW gold.sd_annual_tracker AS
WITH realized_ytd AS (
    SELECT
        commodity,
        country,
        marketing_year,
        attribute,
        SUM(realized_value) AS realized_ytd,
        COUNT(DISTINCT month) AS months_realized,
        MAX(calendar_year || '-' || LPAD(month::text, 2, '0')) AS latest_month
    FROM silver.monthly_realized
    GROUP BY commodity, country, marketing_year, attribute
),
remaining_projection AS (
    SELECT
        e.commodity,
        e.country,
        e.marketing_year,
        e.attribute,
        SUM(e.expected_value) AS remaining_expected,
        COUNT(*) AS months_projected
    FROM silver.monthly_expectation e
    WHERE e.is_current = TRUE
    AND NOT EXISTS (
        SELECT 1 FROM silver.monthly_realized r
        WHERE r.commodity = e.commodity
        AND r.country = e.country
        AND r.marketing_year = e.marketing_year
        AND r.month = e.month
        AND r.attribute = e.attribute
    )
    GROUP BY e.commodity, e.country, e.marketing_year, e.attribute
),
user_annual AS (
    SELECT commodity, country, marketing_year,
           crush, feed_residual, ethanol, fsi, exports, ending_stocks
    FROM silver.user_sd_estimate
    WHERE is_current = TRUE
)
SELECT
    r.commodity,
    r.country,
    r.marketing_year,
    r.attribute,
    r.realized_ytd,
    r.months_realized,
    r.latest_month,
    COALESCE(p.remaining_expected, 0) AS remaining_expected,
    COALESCE(p.months_projected, 0) AS months_projected,
    r.realized_ytd + COALESCE(p.remaining_expected, 0) AS projected_annual_total,
    -- Compare to user's annual estimate
    CASE r.attribute
        WHEN 'crush' THEN u.crush
        WHEN 'feed_residual' THEN u.feed_residual
        WHEN 'ethanol' THEN u.ethanol
        WHEN 'fsi' THEN u.fsi
        WHEN 'exports' THEN u.exports
        ELSE NULL
    END AS user_annual_estimate,
    12 - r.months_realized AS months_remaining
FROM realized_ytd r
LEFT JOIN remaining_projection p
    ON r.commodity = p.commodity
    AND r.country = p.country
    AND r.marketing_year = p.marketing_year
    AND r.attribute = p.attribute
LEFT JOIN user_annual u
    ON r.commodity = u.commodity
    AND r.country = u.country
    AND r.marketing_year = u.marketing_year
ORDER BY r.commodity, r.country, r.marketing_year, r.attribute;

COMMENT ON VIEW gold.sd_annual_tracker IS
    'Tracks YTD realized totals plus remaining monthly projections vs user annual estimates. Shows implied annual totals.';


-- =============================================================================
-- PSD BALANCE SHEET VIEWS BY COMMODITY
-- =============================================================================

-- US Corn Balance Sheet (from PSD)
DROP VIEW IF EXISTS gold.fas_us_corn_balance_sheet;
CREATE VIEW gold.fas_us_corn_balance_sheet AS
SELECT
    marketing_year,
    report_date,
    'USDA PSD' AS source,

    -- Supply
    area_harvested,
    yield,
    beginning_stocks,
    production,
    imports,
    total_supply,

    -- Demand
    feed_dom_consumption,
    fsi_consumption,
    domestic_consumption,
    exports,
    total_distribution,

    -- Ending
    ending_stocks,
    CASE WHEN total_distribution > 0
         THEN ROUND((ending_stocks / total_distribution * 100)::numeric, 1)
         ELSE NULL
    END AS stocks_use_pct,

    unit
FROM bronze.fas_psd
WHERE commodity = 'corn'
  AND country_code = 'US'
ORDER BY marketing_year DESC, report_date DESC;


-- US Soybeans Balance Sheet (from PSD)
DROP VIEW IF EXISTS gold.fas_us_soybeans_balance_sheet;
CREATE VIEW gold.fas_us_soybeans_balance_sheet AS
SELECT
    marketing_year,
    report_date,
    'USDA PSD' AS source,

    -- Supply
    area_harvested,
    yield,
    beginning_stocks,
    production,
    imports,
    total_supply,

    -- Demand
    crush,
    fsi_consumption,
    domestic_consumption,
    exports,
    total_distribution,

    -- Ending
    ending_stocks,
    CASE WHEN total_distribution > 0
         THEN ROUND((ending_stocks / total_distribution * 100)::numeric, 1)
         ELSE NULL
    END AS stocks_use_pct,

    unit
FROM bronze.fas_psd
WHERE commodity = 'soybeans'
  AND country_code = 'US'
ORDER BY marketing_year DESC, report_date DESC;


-- US Wheat Balance Sheet (from PSD)
DROP VIEW IF EXISTS gold.fas_us_wheat_balance_sheet;
CREATE VIEW gold.fas_us_wheat_balance_sheet AS
SELECT
    marketing_year,
    report_date,
    'USDA PSD' AS source,

    area_harvested,
    yield,
    beginning_stocks,
    production,
    imports,
    total_supply,

    feed_dom_consumption,
    fsi_consumption,
    domestic_consumption,
    exports,
    total_distribution,

    ending_stocks,
    CASE WHEN total_distribution > 0
         THEN ROUND((ending_stocks / total_distribution * 100)::numeric, 1)
         ELSE NULL
    END AS stocks_use_pct,

    unit
FROM bronze.fas_psd
WHERE commodity = 'wheat'
  AND country_code = 'US'
ORDER BY marketing_year DESC, report_date DESC;


-- =============================================================================
-- GLOBAL S&D COMPARISON VIEW
-- =============================================================================
CREATE OR REPLACE VIEW gold.global_sd_comparison AS
SELECT
    p.commodity,
    c.name AS country,
    p.marketing_year,
    p.report_date,
    p.production,
    p.domestic_consumption,
    p.exports,
    p.imports,
    p.ending_stocks,
    CASE WHEN p.domestic_consumption + p.exports > 0
         THEN ROUND((p.ending_stocks / (p.domestic_consumption + p.exports) * 100)::numeric, 1)
         ELSE NULL
    END AS stocks_use_pct,
    p.unit
FROM bronze.fas_psd p
LEFT JOIN bronze.fas_country_ref c ON p.country_code = c.code
WHERE p.country_code IN ('US', 'BR', 'AR', 'CH', 'E4', 'RS', 'UP', 'AS', 'CA')
ORDER BY p.commodity, p.marketing_year DESC, p.report_date DESC, p.country_code;

COMMENT ON VIEW gold.global_sd_comparison IS
    'Compares S&D balances across major producing/consuming countries for key commodities.';


-- =============================================================================
-- EXPORT SALES ANALYSIS
-- =============================================================================

-- Weekly Export Sales Summary
CREATE OR REPLACE VIEW gold.export_sales_weekly AS
SELECT
    e.commodity,
    e.marketing_year,
    e.week_ending,
    COUNT(DISTINCT e.country_code) AS num_destinations,
    SUM(e.weekly_exports) AS total_weekly_exports,
    SUM(e.accumulated_exports) AS total_accumulated,
    SUM(e.outstanding_sales) AS total_outstanding,
    SUM(e.net_sales) AS total_net_sales
FROM bronze.fas_export_sales e
GROUP BY e.commodity, e.marketing_year, e.week_ending
ORDER BY e.commodity, e.marketing_year, e.week_ending DESC;


-- Top Export Destinations by Commodity
CREATE OR REPLACE VIEW gold.export_top_destinations AS
WITH latest_week AS (
    SELECT commodity, marketing_year, MAX(week_ending) AS latest
    FROM bronze.fas_export_sales
    GROUP BY commodity, marketing_year
),
ranked AS (
    SELECT
        e.commodity,
        e.marketing_year,
        e.country,
        e.country_code,
        e.accumulated_exports,
        e.outstanding_sales,
        e.accumulated_exports + e.outstanding_sales AS total_commitment,
        ROW_NUMBER() OVER (
            PARTITION BY e.commodity, e.marketing_year
            ORDER BY e.accumulated_exports DESC
        ) AS rank
    FROM bronze.fas_export_sales e
    INNER JOIN latest_week lw
        ON e.commodity = lw.commodity
        AND e.marketing_year = lw.marketing_year
        AND e.week_ending = lw.latest
)
SELECT
    commodity,
    marketing_year,
    country,
    country_code,
    accumulated_exports,
    outstanding_sales,
    total_commitment,
    rank
FROM ranked
WHERE rank <= 10
ORDER BY commodity, marketing_year, rank;

COMMENT ON VIEW gold.export_top_destinations IS
    'Top 10 export destinations by accumulated exports for each commodity and marketing year.';


-- =============================================================================
-- USER ESTIMATE vs PSD COMPARISON
-- =============================================================================
CREATE OR REPLACE VIEW gold.user_vs_psd_comparison AS
SELECT
    u.commodity,
    u.country,
    u.marketing_year,
    u.estimate_date AS user_estimate_date,
    p.report_date AS psd_report_date,

    -- User estimates (need to convert units if different)
    u.production AS user_production,
    u.ending_stocks AS user_ending_stocks,
    u.exports AS user_exports,
    u.unit AS user_unit,

    -- PSD estimates
    p.production AS psd_production,
    p.ending_stocks AS psd_ending_stocks,
    p.exports AS psd_exports,
    p.unit AS psd_unit,

    -- Differences (same-unit comparison)
    u.ending_stocks - p.ending_stocks AS ending_stocks_diff

FROM silver.user_sd_estimate u
LEFT JOIN bronze.fas_psd p
    ON LOWER(u.commodity) = p.commodity
    AND u.marketing_year = p.marketing_year
    AND p.country_code = CASE
        WHEN u.country = 'United States' THEN 'US'
        WHEN u.country = 'Brazil' THEN 'BR'
        WHEN u.country = 'Argentina' THEN 'AR'
        ELSE u.country
    END
WHERE u.is_current = TRUE
ORDER BY u.commodity, u.marketing_year DESC;

COMMENT ON VIEW gold.user_vs_psd_comparison IS
    'Compares user S&D estimates to official USDA PSD data. Note: units may differ (mil bu vs 1000 MT).';


-- =============================================================================
-- Grants
-- =============================================================================
GRANT SELECT ON gold.sd_variance_tracker TO readonly_role;
GRANT SELECT ON gold.sd_annual_tracker TO readonly_role;
GRANT SELECT ON gold.us_corn_balance_sheet TO readonly_role;
GRANT SELECT ON gold.us_soybeans_balance_sheet TO readonly_role;
GRANT SELECT ON gold.us_wheat_balance_sheet TO readonly_role;
GRANT SELECT ON gold.global_sd_comparison TO readonly_role;
GRANT SELECT ON gold.export_sales_weekly TO readonly_role;
GRANT SELECT ON gold.export_top_destinations TO readonly_role;
GRANT SELECT ON gold.user_vs_psd_comparison TO readonly_role;
