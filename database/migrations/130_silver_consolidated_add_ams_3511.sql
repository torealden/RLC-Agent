-- Migration 130: Extend silver.feedstock_prices_consolidated to include AMS 3511
--
-- AMS slug 3511 (National Grain & Oilseed Processor Feedstuff Report,
-- weekly via Minneapolis) is already ingested into bronze.ams_price_record
-- and routes to silver.cash_price (664 rows, since 2025-02-17). But the
-- existing consolidated view (mig 120) only pulls 2837/2839/3510, so the
-- 3511 prices aren't reaching the workbook builder.
--
-- This migration extends the view with a third UNION arm that pivots
-- slug 3511 commodities into feedstock_code form, with unit normalization
-- to $/lb. Commodity mapping:
--   Safflower Meal     -> SAFM        ($/Ton -> /2000 = $/lb)
--   Linseed Meal       -> LSM
--   Sunflower Meal     -> SFM
--   Canola Meal        -> CM
--   Cottonseed Meal    -> CSM
--   Soybean Meal       -> SBM
--   Soybean Oil        -> SBO         (cents/lb -> /100 = $/lb)
--   Corn Distillers    -> DDGS
--   Corn Gluten Meal   -> CGM
--   Corn Gluten Feed   -> CGF
--
-- Location mapping (slug 3511's "trade Loc" field):
--   Minneapolis            -> minneapolis
--   CA-San Joaquin Valley  -> ca_sjv
--   CA-South               -> ca_south
--   Iowa                   -> iowa
--   Illinois               -> illinois
--   Indiana-Ohio           -> indiana_ohio
--   Minnesota              -> minnesota
--   Kansas / Nebraska / etc -> direct lowercase
--   Pacific Northwest      -> us_pnw
--   Portland, OR           -> portland_or
--   Min-Dak                -> min_dak

BEGIN;

CREATE OR REPLACE VIEW silver.feedstock_prices_consolidated AS

WITH ams_byproducts AS (
    -- Existing arm: slugs 2837 / 2839 / 3510 (mig 120)
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
),

ams_3511 AS (
    -- NEW arm: slug 3511 grain & oilseed processor feedstuffs (weekly Minneapolis)
    SELECT
        report_date AS price_date,
        'weekly'    AS frequency,
        CASE LOWER(commodity)
            WHEN 'safflower meal'   THEN 'SAFM'
            WHEN 'linseed meal'     THEN 'LSM'
            WHEN 'sunflower meal'   THEN 'SFM'
            WHEN 'canola meal'      THEN 'CM'
            WHEN 'cottonseed meal'  THEN 'CSM'
            WHEN 'cottonseed (whole)' THEN 'WCS'
            WHEN 'cottonseed hulls' THEN 'CSH'
            WHEN 'soybean_meal'     THEN 'SBM'
            WHEN 'soybean meal'     THEN 'SBM'
            WHEN 'soybean_oil'      THEN 'SBO'
            WHEN 'soybean oil'      THEN 'SBO'
            WHEN 'soybean hulls'    THEN 'SBH'
            WHEN 'corn distillers'  THEN 'DDGS'
            WHEN 'corn gluten meal' THEN 'CGM'
            WHEN 'corn gluten feed' THEN 'CGF'
            WHEN 'yellow corn hominy feed' THEN 'YCHF'
            ELSE NULL
        END AS feedstock_code,
        CASE LOWER(location_name)
            WHEN 'minneapolis'              THEN 'minneapolis'
            WHEN 'ca-san joaquin valley'    THEN 'ca_sjv'
            WHEN 'ca-south'                 THEN 'ca_south'
            WHEN 'iowa'                     THEN 'iowa'
            WHEN 'illinois'                 THEN 'illinois'
            WHEN 'indiana-ohio'             THEN 'indiana_ohio'
            WHEN 'minnesota'                THEN 'minnesota'
            WHEN 'kansas'                   THEN 'kansas'
            WHEN 'nebraska'                 THEN 'nebraska'
            WHEN 'south dakota'             THEN 'south_dakota'
            WHEN 'wisconsin'                THEN 'wisconsin'
            WHEN 'missouri'                 THEN 'missouri'
            WHEN 'michigan'                 THEN 'michigan'
            WHEN 'pacific northwest'        THEN 'us_pnw'
            WHEN 'portland, or'             THEN 'portland_or'
            WHEN 'min-dak'                  THEN 'min_dak'
            WHEN 'california'               THEN 'california'
            WHEN 'central us'               THEN 'central_us'
            WHEN 'kc region'                THEN 'kc'
            WHEN 'st louis, mo'             THEN 'st_louis_mo'
            WHEN 'alabama'                  THEN 'alabama'
            ELSE LOWER(REPLACE(REPLACE(location_name, ' ', '_'), ',', ''))
        END AS region,
        AVG(
            CASE
                WHEN unit ILIKE 'Cents Per Lb' THEN price_cash / 100.0
                WHEN unit ILIKE '$ Per CWT'    THEN price_cash / 100.0
                WHEN unit ILIKE '$ Per Ton'    THEN price_cash / 2000.0
                ELSE NULL
            END
        )::numeric AS price_per_lb,
        NULL::numeric AS price_per_gal,
        'USDA AMS 3511' AS source,
        FALSE AS is_proprietary
    FROM silver.cash_price
    WHERE slug_id = '3511'
      AND price_cash IS NOT NULL
      AND price_cash > 0
    GROUP BY 1, 2, 3, 4, 7, 8
)

-- Legacy fastmarkets arm
SELECT
    price_date, frequency, feedstock_code, region,
    price_per_lb, price_per_gal, source,
    COALESCE(is_proprietary, FALSE) AS is_proprietary
FROM bronze.feedstock_prices
WHERE feedstock_code IS NOT NULL

UNION ALL

-- AMS byproducts (2837/2839/3510)
SELECT
    price_date, frequency, feedstock_code, region,
    price_per_lb, price_per_gal, source, is_proprietary
FROM ams_byproducts
WHERE feedstock_code IS NOT NULL
  AND price_per_lb IS NOT NULL AND price_per_lb > 0

UNION ALL

-- AMS 3511 (oilseed processor feedstuffs)
SELECT
    price_date, frequency, feedstock_code, region,
    price_per_lb, price_per_gal, source, is_proprietary
FROM ams_3511
WHERE feedstock_code IS NOT NULL
  AND price_per_lb IS NOT NULL AND price_per_lb > 0;

COMMENT ON VIEW silver.feedstock_prices_consolidated IS
'Unified feedstock price feed. Combines fastmarkets (frozen ~2025) + AMS 2837/2839/3510 (animal byproducts) + AMS 3511 (oilseed processor feedstuffs incl. safflower/linseed/sunflower/canola/cottonseed/soybean meals + SBO regional). Use for any client-facing rendering; AMS rows are non-proprietary.';

COMMIT;

-- Verification:
-- SELECT feedstock_code, region, COUNT(*) AS n, ROUND(AVG(price_per_lb)::numeric, 4) AS avg_usd_lb,
--        MIN(price_date), MAX(price_date)
-- FROM silver.feedstock_prices_consolidated
-- WHERE source = 'USDA AMS 3511'
-- GROUP BY feedstock_code, region ORDER BY feedstock_code, region;
