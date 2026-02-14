-- =============================================================================
-- Populate Calculated Columns in Silver Tables
-- =============================================================================
-- This script populates YoY changes, 5-year averages, ratios, and other
-- calculated columns that were left empty during initial data load.
--
-- Run this after data loads to ensure calculated columns are populated.
-- Can be re-run safely - uses self-joins to compute values.
-- =============================================================================

-- =============================================================================
-- 1. CONAB PRODUCTION - YoY Changes
-- =============================================================================
-- Calculate year-over-year changes for production, area, and yield

WITH prior_year_data AS (
    SELECT
        state,
        commodity,
        crop_type,
        crop_year,
        production_mmt,
        planted_area_ha,
        yield_mt_ha,
        -- Get the prior year's crop_year string (e.g., '2024/25' -> '2023/24')
        CASE
            WHEN crop_year ~ '^\d{4}/\d{2}$' THEN
                (SPLIT_PART(crop_year, '/', 1)::int - 1)::text || '/' ||
                LPAD(((SPLIT_PART(crop_year, '/', 2)::int - 1) % 100)::text, 2, '0')
            ELSE NULL
        END AS prior_crop_year
    FROM silver.conab_production
),
yoy_calc AS (
    SELECT
        c.state,
        c.commodity,
        c.crop_type,
        c.crop_year,
        -- Production YoY
        c.production_mmt - p.production_mmt AS production_yoy_change,
        CASE WHEN p.production_mmt > 0
             THEN ROUND(((c.production_mmt - p.production_mmt) / p.production_mmt * 100)::numeric, 2)
             ELSE NULL END AS production_yoy_pct,
        -- Area YoY
        c.planted_area_ha - p.planted_area_ha AS area_yoy_change,
        CASE WHEN p.planted_area_ha > 0
             THEN ROUND(((c.planted_area_ha - p.planted_area_ha) / p.planted_area_ha * 100)::numeric, 2)
             ELSE NULL END AS area_yoy_pct,
        -- Yield YoY
        c.yield_mt_ha - p.yield_mt_ha AS yield_yoy_change,
        CASE WHEN p.yield_mt_ha > 0
             THEN ROUND(((c.yield_mt_ha - p.yield_mt_ha) / p.yield_mt_ha * 100)::numeric, 2)
             ELSE NULL END AS yield_yoy_pct
    FROM prior_year_data c
    LEFT JOIN silver.conab_production p ON
        c.state = p.state
        AND c.commodity = p.commodity
        AND COALESCE(c.crop_type, '') = COALESCE(p.crop_type, '')
        AND c.prior_crop_year = p.crop_year
    WHERE c.prior_crop_year IS NOT NULL
)
UPDATE silver.conab_production cp
SET
    production_yoy_change = yoy.production_yoy_change,
    production_yoy_pct = yoy.production_yoy_pct,
    area_yoy_change = yoy.area_yoy_change,
    area_yoy_pct = yoy.area_yoy_pct,
    yield_yoy_change = yoy.yield_yoy_change,
    yield_yoy_pct = yoy.yield_yoy_pct,
    updated_at = NOW()
FROM yoy_calc yoy
WHERE cp.state = yoy.state
  AND cp.commodity = yoy.commodity
  AND COALESCE(cp.crop_type, '') = COALESCE(yoy.crop_type, '')
  AND cp.crop_year = yoy.crop_year;

-- Report results
DO $$
DECLARE
    updated_count INT;
BEGIN
    SELECT COUNT(*) INTO updated_count
    FROM silver.conab_production
    WHERE production_yoy_pct IS NOT NULL;
    RAISE NOTICE 'CONAB Production YoY: Updated % rows with YoY calculations', updated_count;
END $$;


-- =============================================================================
-- 2. CONAB PRODUCTION - 5-Year Averages
-- =============================================================================
-- Calculate departure from 5-year trailing average

WITH five_year_avg AS (
    SELECT
        state,
        commodity,
        crop_type,
        crop_year,
        production_mmt,
        yield_mt_ha,
        -- Calculate 5-year average using LAG
        AVG(production_mmt) OVER (
            PARTITION BY state, commodity, crop_type
            ORDER BY crop_year
            ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
        ) AS avg_5yr_production,
        AVG(yield_mt_ha) OVER (
            PARTITION BY state, commodity, crop_type
            ORDER BY crop_year
            ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
        ) AS avg_5yr_yield,
        COUNT(*) OVER (
            PARTITION BY state, commodity, crop_type
            ORDER BY crop_year
            ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
        ) AS years_in_avg
    FROM silver.conab_production
    WHERE crop_year NOT LIKE '%Previs%'  -- Exclude forecasts from average calculation
)
UPDATE silver.conab_production cp
SET
    production_vs_5yr_avg = CASE
        WHEN fya.avg_5yr_production > 0 AND fya.years_in_avg >= 3
        THEN ROUND(((cp.production_mmt - fya.avg_5yr_production) / fya.avg_5yr_production * 100)::numeric, 2)
        ELSE NULL END,
    yield_vs_5yr_avg = CASE
        WHEN fya.avg_5yr_yield > 0 AND fya.years_in_avg >= 3
        THEN ROUND(((cp.yield_mt_ha - fya.avg_5yr_yield) / fya.avg_5yr_yield * 100)::numeric, 2)
        ELSE NULL END,
    updated_at = NOW()
FROM five_year_avg fya
WHERE cp.state = fya.state
  AND cp.commodity = fya.commodity
  AND COALESCE(cp.crop_type, '') = COALESCE(fya.crop_type, '')
  AND cp.crop_year = fya.crop_year;

-- Report results
DO $$
DECLARE
    updated_count INT;
BEGIN
    SELECT COUNT(*) INTO updated_count
    FROM silver.conab_production
    WHERE production_vs_5yr_avg IS NOT NULL;
    RAISE NOTICE 'CONAB Production 5yr Avg: Updated % rows with 5-year average calculations', updated_count;
END $$;


-- =============================================================================
-- 3. CONAB BALANCE SHEET - Ratios and YoY Changes
-- =============================================================================

-- First, calculate the simple ratios
UPDATE silver.conab_balance_sheet
SET
    stocks_to_use_ratio = CASE
        WHEN total_use_mt > 0
        THEN ROUND((ending_stocks_mt / total_use_mt * 100)::numeric, 2)
        ELSE NULL END,
    export_share_pct = CASE
        WHEN total_use_mt > 0
        THEN ROUND((exports_mt / total_use_mt * 100)::numeric, 2)
        ELSE NULL END,
    crush_share_pct = CASE
        WHEN domestic_consumption_mt > 0
        THEN ROUND((crush_mt / domestic_consumption_mt * 100)::numeric, 2)
        ELSE NULL END,
    feed_share_pct = CASE
        WHEN domestic_consumption_mt > 0
        THEN ROUND((feed_use_mt / domestic_consumption_mt * 100)::numeric, 2)
        ELSE NULL END,
    updated_at = NOW()
WHERE total_use_mt IS NOT NULL OR domestic_consumption_mt IS NOT NULL;

-- Now calculate YoY changes for balance sheet
WITH prior_year_bs AS (
    SELECT
        commodity,
        crop_year,
        production_mt,
        exports_mt,
        ending_stocks_mt,
        -- Get the prior year's crop_year string
        CASE
            WHEN crop_year ~ '^\d{4}/\d{2}$' THEN
                (SPLIT_PART(crop_year, '/', 1)::int - 1)::text || '/' ||
                LPAD(((SPLIT_PART(crop_year, '/', 2)::int - 1) % 100)::text, 2, '0')
            ELSE NULL
        END AS prior_crop_year
    FROM silver.conab_balance_sheet
),
yoy_bs_calc AS (
    SELECT
        c.commodity,
        c.crop_year,
        CASE WHEN p.production_mt > 0
             THEN ROUND(((c.production_mt - p.production_mt) / p.production_mt * 100)::numeric, 2)
             ELSE NULL END AS production_yoy_pct,
        CASE WHEN p.exports_mt > 0
             THEN ROUND(((c.exports_mt - p.exports_mt) / p.exports_mt * 100)::numeric, 2)
             ELSE NULL END AS exports_yoy_pct,
        CASE WHEN p.ending_stocks_mt > 0
             THEN ROUND(((c.ending_stocks_mt - p.ending_stocks_mt) / p.ending_stocks_mt * 100)::numeric, 2)
             ELSE NULL END AS ending_stocks_yoy_pct
    FROM prior_year_bs c
    LEFT JOIN silver.conab_balance_sheet p ON
        c.commodity = p.commodity
        AND c.prior_crop_year = p.crop_year
    WHERE c.prior_crop_year IS NOT NULL
)
UPDATE silver.conab_balance_sheet cb
SET
    production_yoy_pct = yoy.production_yoy_pct,
    exports_yoy_pct = yoy.exports_yoy_pct,
    ending_stocks_yoy_pct = yoy.ending_stocks_yoy_pct,
    updated_at = NOW()
FROM yoy_bs_calc yoy
WHERE cb.commodity = yoy.commodity
  AND cb.crop_year = yoy.crop_year;

-- Report results
DO $$
DECLARE
    ratio_count INT;
    yoy_count INT;
BEGIN
    SELECT COUNT(*) INTO ratio_count
    FROM silver.conab_balance_sheet
    WHERE stocks_to_use_ratio IS NOT NULL;

    SELECT COUNT(*) INTO yoy_count
    FROM silver.conab_balance_sheet
    WHERE production_yoy_pct IS NOT NULL;

    RAISE NOTICE 'CONAB Balance Sheet: % rows with ratios, % rows with YoY changes', ratio_count, yoy_count;
END $$;


-- =============================================================================
-- 4. VERIFICATION QUERIES
-- =============================================================================
-- Run these to verify the calculations worked

-- Check CONAB Production
SELECT
    'conab_production' AS table_name,
    COUNT(*) AS total_rows,
    COUNT(production_yoy_pct) AS has_yoy,
    COUNT(production_vs_5yr_avg) AS has_5yr_avg
FROM silver.conab_production;

-- Check CONAB Balance Sheet
SELECT
    'conab_balance_sheet' AS table_name,
    COUNT(*) AS total_rows,
    COUNT(stocks_to_use_ratio) AS has_stu_ratio,
    COUNT(production_yoy_pct) AS has_yoy
FROM silver.conab_balance_sheet;

-- Sample of calculated data - Brazil Soybeans
SELECT
    crop_year,
    state,
    ROUND(production_mmt, 2) AS prod_mmt,
    production_yoy_pct AS yoy_pct,
    production_vs_5yr_avg AS vs_5yr_pct
FROM silver.conab_production
WHERE commodity = 'soybeans' AND state = 'BRASIL'
ORDER BY crop_year DESC
LIMIT 10;
