-- Migration 120: Consolidated feedstock prices view (AMS + legacy fastmarkets)
--
-- The legacy bronze.feedstock_prices feed (fastmarkets sourced 2019-2025) went
-- dark in mid-2025. The new AMS slugs added 2026-05 (2837/2839/3510) supply
-- daily/weekly tallow, choice white grease, yellow grease, lard, MBM, blood
-- meal, and feathermeal — covering most of the gap left by fastmarkets.
--
-- This view unifies the two so the feedstock-report data pack can read a
-- single shape regardless of source, and so client-facing material can be
-- driven from AMS (public domain) when proprietary fastmarkets data must
-- not be exposed (per memory:feedback_fastmarkets_keep_dont_show).
--
-- Output shape matches bronze.feedstock_prices: feedstock_code, region,
-- price_per_lb. Multi-grade / multi-freight variants for the same
-- (product, location, date) are averaged. Units normalized to $/lb:
--   "Cents Per Lb" -> /100
--   "$ Per CWT"    -> /100      (CWT = 100 lb)
--   "$ Per Ton"    -> /2000

BEGIN;

CREATE OR REPLACE VIEW silver.feedstock_prices_consolidated AS

WITH ams_byproducts AS (
    SELECT
        report_date  AS price_date,
        'daily'      AS frequency,
        CASE
            WHEN commodity ILIKE '%Yellow Grease%'                       THEN 'YG'
            WHEN commodity ILIKE '%Choice White Grease%'                 THEN 'CWG'
            WHEN commodity ILIKE 'Packer Bleachable Tallow%'             THEN 'BFT'
            WHEN commodity ILIKE 'Renderer Bleachable Tallow <.15%%'     THEN 'BFT'
            WHEN commodity = 'Tallow'                                    THEN 'BFT'
            WHEN commodity ILIKE 'Loose Lard%'                           THEN 'LARD'
            WHEN commodity ILIKE 'Meat%Bone Meal%'                       THEN 'MBM'
            WHEN commodity ILIKE 'Commodity Blood Meal%'                 THEN 'BM'
            WHEN commodity ILIKE 'Blood Meal,%'                          THEN 'BM'
            WHEN commodity = 'Feathermeal'                               THEN 'FM'
            -- Technical Tallow excluded — industrial-only grade
            -- "Renderer Bleachable Tallow" (no FFA spec) excluded — stale stub
            ELSE NULL
        END AS feedstock_code,
        CASE
            WHEN location IN ('Chicago', 'Chicago, IL')           THEN 'chicago'
            WHEN location IN ('Central U.S.', 'Central US')       THEN 'central_us'
            WHEN location = 'Minnesota'                            THEN 'minnesota'
            WHEN location = 'Pacific Northwest'                    THEN 'us_pnw'
            WHEN location IN ('Eastern Cornbelt','Eastern Corn-Belt') THEN 'eastern_corn_belt'
            WHEN location = 'Panhandle'                            THEN 'panhandle'
            WHEN location = 'Southern Plains'                      THEN 'southern_plains'
            WHEN location = 'CA-San Joaquin Valley'                THEN 'ca_sjv'
            WHEN location = 'CA-South'                             THEN 'ca_south'
            WHEN location = 'CA-Central Coast'                     THEN 'ca_central_coast'
            WHEN location = 'KC Region'                            THEN 'kc'
            WHEN location = 'Arkansas'                             THEN 'arkansas'
            WHEN location = 'Mississippi'                          THEN 'mississippi'
            WHEN location = 'Gulf'                                 THEN 'us_gulf'
            ELSE LOWER(REPLACE(REPLACE(location, ' ', '_'), '.', ''))
        END AS region,
        -- Unit-normalized price ($/lb)
        AVG(
            CASE
                WHEN unit ILIKE 'Cents Per Lb' THEN price_avg / 100.0
                WHEN unit ILIKE '$ Per CWT'    THEN price_avg / 100.0
                WHEN unit ILIKE '$ Per Ton'    THEN price_avg / 2000.0
                ELSE NULL
            END
        )::numeric AS price_per_lb,
        NULL::numeric AS price_per_gal,
        'USDA AMS ' || slug_id AS source,
        FALSE AS is_proprietary
    FROM silver.specialty_price
    WHERE category = 'byproducts'
      AND price_avg IS NOT NULL
      AND price_avg > 0
    GROUP BY 1, 2, 3, 4, 7, 8
)

-- Legacy fastmarkets path (historical baseline)
SELECT
    price_date,
    frequency,
    feedstock_code,
    region,
    price_per_lb,
    price_per_gal,
    source,
    COALESCE(is_proprietary, FALSE) AS is_proprietary
FROM bronze.feedstock_prices
WHERE feedstock_code IS NOT NULL

UNION ALL

-- AMS byproducts (current source going forward)
SELECT
    price_date,
    frequency,
    feedstock_code,
    region,
    price_per_lb,
    price_per_gal,
    source,
    is_proprietary
FROM ams_byproducts
WHERE feedstock_code IS NOT NULL
  AND price_per_lb IS NOT NULL
  AND price_per_lb > 0;

COMMENT ON VIEW silver.feedstock_prices_consolidated IS
'Unified feedstock price feed. Combines legacy fastmarkets (bronze.feedstock_prices, frozen ~2025-04) with USDA AMS tallow/grease/protein reports (slugs 2837/2839/3510, live since 2022). Use this view for any client-facing rendering; the AMS-sourced rows have is_proprietary=FALSE.';

COMMIT;

-- Verification:
-- SELECT feedstock_code, region, COUNT(*), MIN(price_date), MAX(price_date)
-- FROM silver.feedstock_prices_consolidated
-- WHERE source LIKE 'USDA AMS%'
-- GROUP BY 1,2 ORDER BY 1,2;
