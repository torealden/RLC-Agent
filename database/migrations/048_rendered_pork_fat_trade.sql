-- Migration 048: Gold view for US rendered pork fat trade
--
-- Date: 2026-05-04
--
-- Why:
--   bronze.census_trade tracks 1501200040 (Choice White Grease) and
--   1501200080 (Other Pig Fat, Rendered) as separate rows. Both fall
--   under HS-6 150120 ("Lard; other pig fat, rendered"). The CWG split
--   is by quality grade (NRA spec: ≤4% FFA, color ≤13R, pork-only) vs
--   the residual "other rendered pig fat" that doesn't meet that spec.
--
--   For BBD feedstock analysis we want both:
--     (a) each grade addressable separately (different CI scores,
--         different end-use markets — CWG into food/feed/biofuel,
--         Other Pig Fat mostly biofuel/industrial)
--     (b) an aggregate "total US rendered pork fat trade" series
--         since renderers can flex between qualities and the residual
--         category occasionally spikes (2023 imports ran 9x the
--         surrounding-year average — worth tracking as an arbitrage
--         indicator)
--
-- Convention notes:
--   - bronze stores quantity in KG (Census source unit per
--     silver.trade_commodity_reference)
--   - country_code='-' is the World Total convention (matches the
--     "World Total" row in Census UATO CSVs)
--   - DB exports include re-exports; UATO CSVs are domestic-only,
--     so DB export figures may run 1-7% higher in some years
--     (per feedback_census_trade_verification.md)
--   - Display units: MT (matches CSV), 000 lbs and mil lbs (match
--     the spreadsheet display convention for oils/fats per memory
--     "Bushels for oilseeds & grains rule")

DROP VIEW IF EXISTS gold.us_rendered_pork_fat_trade;

CREATE VIEW gold.us_rendered_pork_fat_trade AS
WITH world_total AS (
    SELECT
        year,
        month,
        flow,
        hs_code,
        SUM(quantity)  AS quantity_kg,
        SUM(value_usd) AS value_usd
    FROM bronze.census_trade
    WHERE hs_code IN ('1501200040', '1501200080')
      AND country_code = '-'
    GROUP BY year, month, flow, hs_code
)
SELECT
    year,
    month,
    flow,
    -- CWG (Choice White Grease)
    SUM(CASE WHEN hs_code = '1501200040'
             THEN quantity_kg ELSE 0 END) / 1000.0
        AS cwg_mt,
    SUM(CASE WHEN hs_code = '1501200040'
             THEN quantity_kg ELSE 0 END) * 0.002204622
        AS cwg_lbs,
    SUM(CASE WHEN hs_code = '1501200040'
             THEN quantity_kg ELSE 0 END) * 0.002204622 / 1000.0
        AS cwg_000_lbs,
    SUM(CASE WHEN hs_code = '1501200040'
             THEN quantity_kg ELSE 0 END) * 0.002204622 / 1000000.0
        AS cwg_mil_lbs,
    SUM(CASE WHEN hs_code = '1501200040'
             THEN value_usd ELSE 0 END)
        AS cwg_value_usd,
    -- Other Pig Fat, Rendered (residual of HS-6 150120)
    SUM(CASE WHEN hs_code = '1501200080'
             THEN quantity_kg ELSE 0 END) / 1000.0
        AS other_pig_fat_mt,
    SUM(CASE WHEN hs_code = '1501200080'
             THEN quantity_kg ELSE 0 END) * 0.002204622
        AS other_pig_fat_lbs,
    SUM(CASE WHEN hs_code = '1501200080'
             THEN quantity_kg ELSE 0 END) * 0.002204622 / 1000.0
        AS other_pig_fat_000_lbs,
    SUM(CASE WHEN hs_code = '1501200080'
             THEN quantity_kg ELSE 0 END) * 0.002204622 / 1000000.0
        AS other_pig_fat_mil_lbs,
    SUM(CASE WHEN hs_code = '1501200080'
             THEN value_usd ELSE 0 END)
        AS other_pig_fat_value_usd,
    -- Total rendered pork fat (sum of both HS codes)
    SUM(quantity_kg) / 1000.0
        AS total_mt,
    SUM(quantity_kg) * 0.002204622 / 1000.0
        AS total_000_lbs,
    SUM(quantity_kg) * 0.002204622 / 1000000.0
        AS total_mil_lbs,
    SUM(value_usd)
        AS total_value_usd,
    -- Mix indicators (helpful for spotting reclassification anomalies)
    CASE WHEN SUM(quantity_kg) > 0
         THEN SUM(CASE WHEN hs_code = '1501200080'
                       THEN quantity_kg ELSE 0 END)
              / NULLIF(SUM(quantity_kg), 0) * 100
         ELSE NULL
    END AS other_pig_fat_share_pct
FROM world_total
GROUP BY year, month, flow
ORDER BY year, month, flow;

COMMENT ON VIEW gold.us_rendered_pork_fat_trade IS
'US rendered pork fat trade — World Total monthly, HS 1501200040 (Choice White Grease) + HS 1501200080 (Other Pig Fat, Rendered). Provides each grade separately plus an aggregate total. Source: bronze.census_trade where country_code=''-''. Note: DB exports include re-exports; differ from Census UATO domestic-export figures by 1-7% in some years.';
