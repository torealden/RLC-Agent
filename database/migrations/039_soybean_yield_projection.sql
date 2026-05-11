-- ============================================================================
-- Migration 039: Soybean yield trend × seasonal projection
-- ============================================================================
-- Per Tore design (2026-04-27):
--   Replace fixed 11.6 lb oil/bu assumption with marketing-year projection
--   based on (a) trend regression of last 5-10 MY annual oil yields,
--   (b) within-MY allocation using 5-year seasonal pattern, (c) blend
--   already-observed months with future-month projections.
--
-- Rationale: oil yield depends on growing-season conditions (drier+warmer
--   pod-fill = more oil per seed). Yields shift YoY, so a flat assumption
--   misses real economic signal. Future enhancement: weather-based summer-
--   growing-season yield model (kg_callable).
--
-- Architecture:
--   silver.soybean_yield_my_annual    — per-MY annual oil + meal yield
--   silver.soybean_yield_seasonal     — 5-yr avg monthly share of annual
--   gold.soybean_yield_my_trend       — trend regression forecast for current/next MY
--   gold.soybean_yield_monthly_projection — combined: observed-where-known + projected-future
-- ============================================================================

-- ----------------------------------------------------------------------------
-- 1. silver.soybean_yield_my_annual — per-MY annual averages
-- ----------------------------------------------------------------------------
CREATE OR REPLACE VIEW silver.soybean_yield_my_annual AS
SELECT
    marketing_year,
    COUNT(*) AS months_observed,
    AVG(oil_yield_lbs_per_bu)  AS oil_yield_lbs_per_bu,
    AVG(meal_yield_lbs_per_bu) AS meal_yield_lbs_per_bu,
    -- Implied hulls per bushel (60 - oil - meal)
    60.0 - AVG(oil_yield_lbs_per_bu) - AVG(meal_yield_lbs_per_bu) AS implied_hulls_lbs_per_bu,
    SUM(national_crush_mil_bu) AS my_total_crush_mil_bu,
    MIN(period_month) AS first_month,
    MAX(period_month) AS last_month,
    -- A MY is "complete" once it has 12 months of data
    (COUNT(*) = 12) AS is_complete
FROM silver.nopa_yield_history
WHERE oil_yield_lbs_per_bu IS NOT NULL
GROUP BY marketing_year
ORDER BY marketing_year;

COMMENT ON VIEW silver.soybean_yield_my_annual IS
'Per-marketing-year (Sep-Aug) average soybean oil + meal yield per bushel from NOPA. is_complete flag distinguishes finished MYs from in-progress.';


-- ----------------------------------------------------------------------------
-- 2. silver.soybean_yield_seasonal — 5-year-avg monthly share of MY total
-- ----------------------------------------------------------------------------
-- For each calendar month, compute that month's average yield as a fraction
-- of its MY total. This captures the within-year seasonal pattern.
CREATE OR REPLACE VIEW silver.soybean_yield_seasonal AS
WITH recent_completed_mys AS (
    SELECT marketing_year FROM silver.soybean_yield_my_annual
    WHERE is_complete = TRUE
    ORDER BY marketing_year DESC
    LIMIT 5
),
month_in_my AS (
    SELECT
        h.marketing_year,
        h.month,
        h.oil_yield_lbs_per_bu,
        h.meal_yield_lbs_per_bu,
        a.oil_yield_lbs_per_bu  AS my_avg_oil,
        a.meal_yield_lbs_per_bu AS my_avg_meal
    FROM silver.nopa_yield_history h
    JOIN silver.soybean_yield_my_annual a USING (marketing_year)
    WHERE h.marketing_year IN (SELECT marketing_year FROM recent_completed_mys)
      AND h.oil_yield_lbs_per_bu IS NOT NULL
)
SELECT
    month,
    -- MY-relative position (Sep=1, Aug=12)
    CASE WHEN month >= 9 THEN month - 8 ELSE month + 4 END AS my_month_position,
    AVG(oil_yield_lbs_per_bu)               AS avg_oil_yield_lbs_per_bu,
    AVG(meal_yield_lbs_per_bu)              AS avg_meal_yield_lbs_per_bu,
    -- Seasonal index: month's avg yield divided by MY avg (1.0 = average month)
    AVG(oil_yield_lbs_per_bu / NULLIF(my_avg_oil, 0))  AS oil_seasonal_index,
    AVG(meal_yield_lbs_per_bu / NULLIF(my_avg_meal, 0)) AS meal_seasonal_index,
    COUNT(*) AS n_observations
FROM month_in_my
GROUP BY month;

COMMENT ON VIEW silver.soybean_yield_seasonal IS
'Within-marketing-year seasonal pattern of oil + meal yield per bushel. seasonal_index of 1.0 means the month''s yield is the MY average; >1 means above-average month (typically late-MY months when newer-crop beans dominate). Computed from last 5 completed MYs.';


-- ----------------------------------------------------------------------------
-- 3. gold.soybean_yield_my_trend — trend regression forecast
-- ----------------------------------------------------------------------------
-- Linear regression on last 10 completed MYs to project annual yield for the
-- next MY. Simple slope+intercept; no weather adjustment in v1.
CREATE OR REPLACE VIEW gold.soybean_yield_my_trend AS
WITH recent AS (
    SELECT marketing_year, oil_yield_lbs_per_bu, meal_yield_lbs_per_bu
    FROM silver.soybean_yield_my_annual
    WHERE is_complete = TRUE
    ORDER BY marketing_year DESC LIMIT 10
),
stats AS (
    SELECT
        regr_slope(oil_yield_lbs_per_bu,  marketing_year)     AS oil_slope,
        regr_intercept(oil_yield_lbs_per_bu,  marketing_year) AS oil_intercept,
        regr_r2(oil_yield_lbs_per_bu,  marketing_year)        AS oil_r2,
        regr_slope(meal_yield_lbs_per_bu, marketing_year)     AS meal_slope,
        regr_intercept(meal_yield_lbs_per_bu, marketing_year) AS meal_intercept,
        regr_r2(meal_yield_lbs_per_bu, marketing_year)        AS meal_r2,
        MAX(marketing_year)                                    AS last_complete_my,
        MIN(marketing_year)                                    AS first_my_used
    FROM recent
)
SELECT
    target_my,
    oil_slope * target_my + oil_intercept   AS projected_oil_yield_lbs_per_bu,
    meal_slope * target_my + meal_intercept AS projected_meal_yield_lbs_per_bu,
    -- Hulls assumption: 3% of bushel weight per Tore convention
    1.8                                       AS assumed_hulls_lbs_per_bu,
    60.0 - (oil_slope * target_my + oil_intercept) - (meal_slope * target_my + meal_intercept)
                                              AS implied_other_lbs_per_bu,
    oil_slope, oil_intercept, oil_r2,
    meal_slope, meal_intercept, meal_r2,
    last_complete_my,
    first_my_used,
    target_my - first_my_used + 1            AS trend_window_my
FROM stats CROSS JOIN (
    -- Project for last_complete_my + 1 through + 7 to cover the current MY
    -- even when the underlying NOPA data is several MYs stale.
    VALUES (1), (2), (3), (4), (5), (6), (7)
) AS offsets(o)
CROSS JOIN LATERAL (SELECT MAX(marketing_year) + offsets.o AS target_my FROM recent) t;

COMMENT ON VIEW gold.soybean_yield_my_trend IS
'Linear trend regression of last 10 completed MYs (slope × MY + intercept) to project annual oil + meal yield for current MY (target_my = last_complete + 1) and next MY (+ 2). r2 reports trend strength. v1: no weather adjustment.';


-- ----------------------------------------------------------------------------
-- 4. gold.soybean_yield_monthly_projection — observed + projected
-- ----------------------------------------------------------------------------
-- For a given target MY:
--   - For months ALREADY OBSERVED in the MY → return actual NOPA yield
--   - For FUTURE months in the MY → project using trend × seasonal index,
--     adjusted to make the in-progress MY mean consistent with observed-to-date
CREATE OR REPLACE VIEW gold.soybean_yield_monthly_projection AS
WITH target_mys AS (
    SELECT target_my, projected_oil_yield_lbs_per_bu, projected_meal_yield_lbs_per_bu
    FROM gold.soybean_yield_my_trend
),
observed AS (
    SELECT
        marketing_year AS target_my,
        month,
        oil_yield_lbs_per_bu  AS observed_oil_yield,
        meal_yield_lbs_per_bu AS observed_meal_yield,
        TRUE  AS is_observed
    FROM silver.nopa_yield_history
    WHERE marketing_year IN (SELECT target_my FROM target_mys)
),
all_months AS (
    SELECT t.target_my, m.month
    FROM target_mys t
    CROSS JOIN generate_series(1, 12) AS m(month)
)
SELECT
    am.target_my,
    am.month,
    -- Convert MY + month to calendar period (months 9-12 = MY year, 1-8 = MY+1 year)
    make_date(
        CASE WHEN am.month >= 9 THEN am.target_my ELSE am.target_my + 1 END,
        am.month, 1
    ) AS period_month,
    COALESCE(o.observed_oil_yield,
             tm.projected_oil_yield_lbs_per_bu * COALESCE(s.oil_seasonal_index, 1.0)
    ) AS oil_yield_lbs_per_bu,
    COALESCE(o.observed_meal_yield,
             tm.projected_meal_yield_lbs_per_bu * COALESCE(s.meal_seasonal_index, 1.0)
    ) AS meal_yield_lbs_per_bu,
    COALESCE(o.is_observed, FALSE) AS is_observed,
    s.oil_seasonal_index,
    tm.projected_oil_yield_lbs_per_bu  AS my_avg_projected_oil,
    tm.projected_meal_yield_lbs_per_bu AS my_avg_projected_meal
FROM all_months am
JOIN target_mys tm ON tm.target_my = am.target_my
LEFT JOIN observed o ON o.target_my = am.target_my AND o.month = am.month
LEFT JOIN silver.soybean_yield_seasonal s ON s.month = am.month;

COMMENT ON VIEW gold.soybean_yield_monthly_projection IS
'Per-month soybean oil/meal yield projection for current and next MY. is_observed=TRUE rows are actual NOPA values; FALSE rows are trend × seasonal projection. The default consumed by the IFV kg_callable when no facility-specific override is supplied.';

GRANT SELECT ON silver.soybean_yield_my_annual                TO PUBLIC;
GRANT SELECT ON silver.soybean_yield_seasonal                  TO PUBLIC;
GRANT SELECT ON gold.soybean_yield_my_trend                    TO PUBLIC;
GRANT SELECT ON gold.soybean_yield_monthly_projection          TO PUBLIC;
