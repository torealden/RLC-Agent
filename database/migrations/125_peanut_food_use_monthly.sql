-- Migration 125: silver.peanut_trade_unit_price + silver.peanut_food_use_monthly_net
--
-- Two views:
--   1. silver.peanut_trade_unit_price (VIEW): annual $/kg per HS code derived
--      from rows where we have BOTH qty + value. Used to impute kg for
--      the value-only series (1202.42, 2008.11 6-digit exports).
--
--   2. silver.peanut_food_use_monthly_net (VIEW): the headline food balance
--      sheet with trade adjustments. Monthly, shelled-equivalent (000 lb).
--      DISTINCT from silver.peanut_food_use_monthly (which is domestic NASS
--      breakdown only). The _net view adds:
--        - NASS domestic edible + in-shell usage (already in bronze.nass_processing)
--        - Net trade for in-shell (1202.41), shelled raw (1202.42), peanut butter
--          (2008.11.05), other prep (2008.11.15), with imputed qty where needed.
--
-- Unit conversion: kg -> 000 lb via /453.592. Peanut butter weight -> peanut
-- equivalent via /0.93.

BEGIN;

-- =============================================================
-- Unit price reference (VIEW)
-- Per (hs_code, flow, year), gives $/kg from real qty data.
-- Used to impute kg for the 6-digit value-only series.
-- =============================================================
CREATE OR REPLACE VIEW silver.peanut_trade_unit_price AS
SELECT
    hs_code,
    flow,
    year,
    SUM(value_usd) AS total_value_usd,
    SUM(quantity)  AS total_qty_kg,
    CASE WHEN SUM(quantity) > 0
         THEN SUM(value_usd) / SUM(quantity)
         ELSE NULL
    END AS price_usd_per_kg
FROM bronze.census_trade
WHERE hs_code IN ('1202410000', '1202300000', '2008110500', '2008111500')
  AND country_name = 'TOTAL FOR ALL COUNTRIES'
  AND quantity > 0
  AND value_usd > 0
GROUP BY hs_code, flow, year;

COMMENT ON VIEW silver.peanut_trade_unit_price IS
'Annual $/kg per (HS code, flow). Derived only from rows with real qty (>0). Used to impute kg for the value-only 6-digit series (1202.42 / 200811 / 1202.30 imports / 1202.41 imports).';


-- =============================================================
-- Monthly peanut food use balance sheet (VIEW)
-- One row per (year, month). All values in 000 lb shelled-equivalent
-- unless noted. Derived = TRUE for the imputed shelled-export and
-- prepared-export quantities.
-- =============================================================
CREATE OR REPLACE VIEW silver.peanut_food_use_monthly_net AS
WITH

-- NASS domestic side: edible + in-shell usage
nass_food AS (
    SELECT
        year, month,
        MAX(CASE WHEN short_desc = 'PEANUTS, SHELLED, EDIBLE, CANDY - USAGE, MEASURED IN LB, RAW BASIS'
                 THEN value END) / 1000.0 AS candy_000lb,
        MAX(CASE WHEN short_desc = 'PEANUTS, SHELLED, EDIBLE, SNACKS - USAGE, MEASURED IN LB, RAW BASIS'
                 THEN value END) / 1000.0 AS snacks_000lb,
        MAX(CASE WHEN short_desc = 'PEANUTS, SHELLED, EDIBLE, PEANUT BUTTER - USAGE, MEASURED IN LB, RAW BASIS'
                 THEN value END) / 1000.0 AS peanut_butter_000lb,
        MAX(CASE WHEN short_desc = 'PEANUTS, SHELLED, EDIBLE, OTHER USES - USAGE, MEASURED IN LB, RAW BASIS'
                 THEN value END) / 1000.0 AS other_000lb,
        MAX(CASE WHEN short_desc = 'PEANUTS, SHELLED, EDIBLE - USAGE, MEASURED IN LB, RAW BASIS'
                 THEN value END) / 1000.0 AS edible_total_000lb,
        MAX(CASE WHEN short_desc = 'PEANUTS, IN SHELL - USAGE, MEASURED IN LB, RAW BASIS'
                 THEN value END) / 1000.0 AS in_shell_food_usage_000lb
    FROM bronze.nass_processing
    WHERE LOWER(commodity_desc) = 'peanuts'
      AND year >= 2013
      AND month IS NOT NULL
    GROUP BY year, month
),

-- Trade side: monthly totals by HS code from bronze.census_trade.
-- For value-only 6-digit codes, qty=0, so we'll impute from unit_price.
trade AS (
    SELECT
        year, month, hs_code, flow,
        SUM(value_usd) AS value_usd_total,
        SUM(quantity)  AS qty_kg_total
    FROM bronze.census_trade
    WHERE hs_code IN ('1202410000','1202420000','1202300000',
                      '2008110500','2008111500',
                      '120241','120242','120230','200811')
      AND country_name = 'TOTAL FOR ALL COUNTRIES'
    GROUP BY year, month, hs_code, flow
),

-- Pivot: one row per (year, month), columns for each (hs, flow, metric).
-- kg -> 000 lb via /453.592 (1 kg = 2.20462 lb).
-- Peanut butter weight -> peanut equivalent via /0.93.
trade_pivot AS (
    SELECT
        year, month,
        -- 1202.41 in-shell (have 10-digit qty for exports; imports are 6-digit value only)
        MAX(CASE WHEN hs_code = '1202410000' AND flow = 'exports'
                 THEN qty_kg_total / 453.592 END) AS in_shell_exports_000lb,
        MAX(CASE WHEN hs_code = '120241'     AND flow = 'imports'
                 THEN value_usd_total END)        AS in_shell_imports_value_usd,
        -- 1202.42 shelled (both flows are 6-digit value only)
        MAX(CASE WHEN hs_code = '120242'     AND flow = 'exports'
                 THEN value_usd_total END)        AS shelled_exports_value_usd,
        MAX(CASE WHEN hs_code = '120242'     AND flow = 'imports'
                 THEN value_usd_total END)        AS shelled_imports_value_usd,
        -- 1202.30 seed (exports 10-digit qty; imports 6-digit value)
        MAX(CASE WHEN hs_code = '1202300000' AND flow = 'exports'
                 THEN qty_kg_total / 453.592 END) AS seed_exports_000lb,
        MAX(CASE WHEN hs_code = '120230'     AND flow = 'imports'
                 THEN value_usd_total END)        AS seed_imports_value_usd,
        -- 2008.11.05 peanut butter imports (qty / 0.93 -> peanut equivalent)
        MAX(CASE WHEN hs_code = '2008110500' AND flow = 'imports'
                 THEN qty_kg_total / 453.592 / 0.93 END) AS pb_imports_000lb,
        -- 2008.11.15 other prep imports
        MAX(CASE WHEN hs_code = '2008111500' AND flow = 'imports'
                 THEN qty_kg_total / 453.592 END)         AS otherprep_imports_000lb,
        -- 2008.11 prepared exports (6-digit value only)
        MAX(CASE WHEN hs_code = '200811'     AND flow = 'exports'
                 THEN value_usd_total END)        AS prep_exports_value_usd
    FROM trade
    GROUP BY year, month
),

-- Unit price proxies (annual avg). For 1202.42 we use 1202.41 export $/kg.
-- For 2008.11 exports we use 2008.11.05 import $/kg.
unit_price_annual AS (
    SELECT
        year,
        MAX(CASE WHEN hs_code='1202410000' AND flow='exports'
                 THEN price_usd_per_kg END) AS in_shell_export_usd_per_kg,
        MAX(CASE WHEN hs_code='2008110500' AND flow='imports'
                 THEN price_usd_per_kg END) AS pb_import_usd_per_kg
    FROM silver.peanut_trade_unit_price
    GROUP BY year
)

SELECT
    nf.year, nf.month,
    -- NASS domestic
    nf.candy_000lb,
    nf.snacks_000lb,
    nf.peanut_butter_000lb,
    nf.other_000lb,
    nf.edible_total_000lb,
    nf.in_shell_food_usage_000lb,

    -- Trade with real qty
    COALESCE(tp.in_shell_exports_000lb, 0) AS in_shell_exports_000lb,
    COALESCE(tp.seed_exports_000lb, 0)      AS seed_exports_000lb,
    COALESCE(tp.pb_imports_000lb, 0)        AS pb_imports_peanut_eq_000lb,
    COALESCE(tp.otherprep_imports_000lb, 0) AS otherprep_imports_000lb,

    -- Imputed quantities (DERIVED from value / annual unit price)
    -- Flagged via *_derived columns so analysts know.
    CASE
        WHEN tp.shelled_exports_value_usd IS NOT NULL AND up.in_shell_export_usd_per_kg > 0
        THEN tp.shelled_exports_value_usd / up.in_shell_export_usd_per_kg / 453.592
        ELSE NULL
    END AS shelled_exports_derived_000lb,

    CASE
        WHEN tp.shelled_imports_value_usd IS NOT NULL AND up.in_shell_export_usd_per_kg > 0
        THEN tp.shelled_imports_value_usd / up.in_shell_export_usd_per_kg / 453.592
        ELSE NULL
    END AS shelled_imports_derived_000lb,

    CASE
        WHEN tp.in_shell_imports_value_usd IS NOT NULL AND up.in_shell_export_usd_per_kg > 0
        THEN tp.in_shell_imports_value_usd / up.in_shell_export_usd_per_kg / 453.592
        ELSE NULL
    END AS in_shell_imports_derived_000lb,

    CASE
        WHEN tp.prep_exports_value_usd IS NOT NULL AND up.pb_import_usd_per_kg > 0
        THEN tp.prep_exports_value_usd / up.pb_import_usd_per_kg / 453.592
        ELSE NULL
    END AS prep_exports_derived_peanut_eq_000lb,

    -- Headline: peanuts used in US food = domestic edible + in-shell food
    --   + net food-stream imports (peanut butter + other prep + in-shell + shelled raw)
    --   - net food-stream exports (in-shell + shelled raw + prepared)
    -- Seed exports/imports are EXCLUDED from food use.
    (
        COALESCE(nf.edible_total_000lb, 0)
      + COALESCE(nf.in_shell_food_usage_000lb, 0)
      + COALESCE(tp.pb_imports_000lb, 0)
      + COALESCE(tp.otherprep_imports_000lb, 0)
      + COALESCE(CASE
            WHEN tp.in_shell_imports_value_usd IS NOT NULL AND up.in_shell_export_usd_per_kg > 0
            THEN tp.in_shell_imports_value_usd / up.in_shell_export_usd_per_kg / 453.592
            ELSE 0 END, 0)
      + COALESCE(CASE
            WHEN tp.shelled_imports_value_usd IS NOT NULL AND up.in_shell_export_usd_per_kg > 0
            THEN tp.shelled_imports_value_usd / up.in_shell_export_usd_per_kg / 453.592
            ELSE 0 END, 0)
      - COALESCE(tp.in_shell_exports_000lb, 0)
      - COALESCE(CASE
            WHEN tp.shelled_exports_value_usd IS NOT NULL AND up.in_shell_export_usd_per_kg > 0
            THEN tp.shelled_exports_value_usd / up.in_shell_export_usd_per_kg / 453.592
            ELSE 0 END, 0)
      - COALESCE(CASE
            WHEN tp.prep_exports_value_usd IS NOT NULL AND up.pb_import_usd_per_kg > 0
            THEN tp.prep_exports_value_usd / up.pb_import_usd_per_kg / 453.592
            ELSE 0 END, 0)
    ) AS us_peanut_food_use_000lb,

    -- Raw value totals for the 6-digit value-only series (transparency)
    tp.shelled_exports_value_usd,
    tp.shelled_imports_value_usd,
    tp.in_shell_imports_value_usd,
    tp.prep_exports_value_usd,
    tp.seed_imports_value_usd,

    -- Flags
    (tp.shelled_exports_value_usd IS NOT NULL OR
     tp.shelled_imports_value_usd IS NOT NULL OR
     tp.prep_exports_value_usd    IS NOT NULL)        AS has_derived_qty
FROM nass_food nf
LEFT JOIN trade_pivot tp ON nf.year = tp.year AND nf.month = tp.month
LEFT JOIN unit_price_annual up ON nf.year = up.year;

COMMENT ON VIEW silver.peanut_food_use_monthly_net IS
'Monthly US peanut food use balance sheet WITH TRADE ADJUSTMENTS, shelled-equivalent (000 lb). Combines NASS domestic edible/in-shell usage with monthly trade flows to give US-only peanut consumption. *_derived_* columns mean the quantity was imputed from value via silver.peanut_trade_unit_price because Census suppresses quantity at the 6-digit aggregate level for the affected HS codes (1202.42, 2008.11, 1202.41 imports, 1202.30 imports). Distinct from silver.peanut_food_use_monthly, which is the domestic-NASS-only breakdown.';

COMMIT;

-- Verification:
-- SELECT * FROM silver.peanut_trade_unit_price ORDER BY year DESC, hs_code, flow;
-- SELECT year, month, us_peanut_food_use_000lb, has_derived_qty
-- FROM silver.peanut_food_use_monthly_net
-- WHERE year >= 2024 ORDER BY year DESC, month DESC LIMIT 12;
