-- Migration 112: silver.peanut_balance_sheet_master (Tier 1)
--
-- Pivots ERS Oil Crops Yearbook Table 11 (farmer's stock basis) from
-- (marketing_year, attribute, value) long form into wide form: one
-- row per marketing year with all line items as columns.
--
-- Units (per ERS): all million pounds farmer's-stock basis, EXCEPT
--   season_avg_price_cents_per_lb (Cents/pound)
--   food_use_per_capita_lb        (Pounds)
--
-- Adds derived columns:
--   ending_stocks_calc            = total_supply - total_disappearance
--   stocks_to_use_pct             = ending_stocks_calc / total_disappearance * 100
--   reconciliation_supply_check   = beg + production + imports - total_supply
--                                   (should be 0, flags ERS data drift)
--   reconciliation_disap_check    = crush + food_use + exports + seed_loss
--                                   - total_disappearance (should be 0)

CREATE OR REPLACE VIEW silver.peanut_balance_sheet_master AS
WITH pivoted AS (
    SELECT
        marketing_year,
        MAX(CASE WHEN attribute_desc = 'Beginning stocks'                       THEN amount END) AS beginning_stocks_mil_lbs,
        MAX(CASE WHEN attribute_desc = 'Production'                              THEN amount END) AS production_mil_lbs,
        MAX(CASE WHEN attribute_desc = 'Imports'                                 THEN amount END) AS imports_mil_lbs,
        MAX(CASE WHEN attribute_desc = 'Total supply'                            THEN amount END) AS total_supply_mil_lbs,
        MAX(CASE WHEN attribute_desc = 'Crush'                                   THEN amount END) AS crush_mil_lbs,
        MAX(CASE WHEN attribute_desc = 'Food use'                                THEN amount END) AS food_use_mil_lbs,
        MAX(CASE WHEN attribute_desc = 'Exports'                                 THEN amount END) AS exports_mil_lbs,
        MAX(CASE WHEN attribute_desc = 'Seed, loss, shrinkage, and residual'     THEN amount END) AS seed_loss_residual_mil_lbs,
        MAX(CASE WHEN attribute_desc = 'Total disappearance'                     THEN amount END) AS total_disappearance_mil_lbs,
        MAX(CASE WHEN attribute_desc = 'Season-average price received by farmers' THEN amount END) AS season_avg_price_cents_per_lb,
        MAX(CASE WHEN attribute_desc = 'Food use per capita (shelled basis)'     THEN amount END) AS food_use_per_capita_lb
    FROM bronze.ers_oilcrops_raw
    WHERE commodity ILIKE 'peanut%'
      AND table_number = '11'
      AND geography_desc2 = 'United States'
      AND timeperiod_desc = 'MY Total'
    GROUP BY marketing_year
)
SELECT
    marketing_year,
    'August-July'::text AS marketing_year_definition,
    beginning_stocks_mil_lbs,
    production_mil_lbs,
    imports_mil_lbs,
    total_supply_mil_lbs,
    crush_mil_lbs,
    food_use_mil_lbs,
    exports_mil_lbs,
    seed_loss_residual_mil_lbs,
    total_disappearance_mil_lbs,
    -- Derived
    (total_supply_mil_lbs - total_disappearance_mil_lbs) AS ending_stocks_calc_mil_lbs,
    CASE WHEN total_disappearance_mil_lbs > 0
         THEN ROUND((total_supply_mil_lbs - total_disappearance_mil_lbs) / total_disappearance_mil_lbs * 100, 1)
    END AS stocks_to_use_pct,
    season_avg_price_cents_per_lb,
    food_use_per_capita_lb,
    -- Reconciliation
    (COALESCE(beginning_stocks_mil_lbs, 0) + COALESCE(production_mil_lbs, 0) + COALESCE(imports_mil_lbs, 0)
     - COALESCE(total_supply_mil_lbs, 0)) AS reconciliation_supply_check,
    (COALESCE(crush_mil_lbs, 0) + COALESCE(food_use_mil_lbs, 0) + COALESCE(exports_mil_lbs, 0)
     + COALESCE(seed_loss_residual_mil_lbs, 0) - COALESCE(total_disappearance_mil_lbs, 0)) AS reconciliation_disap_check,
    'ERS Oil Crops Yearbook Table 11'::text AS source
FROM pivoted
ORDER BY marketing_year;

COMMENT ON VIEW silver.peanut_balance_sheet_master IS
'Tier 1 master peanut balance sheet, farmer''s stock basis. Source = ERS Oil Crops Yearbook Table 11. Wide-pivot per marketing year. Annual cadence (MY Aug-Jul). Includes derived ending_stocks and reconciliation_*_check columns that should be ~0; non-zero values flag ERS data drift.';
