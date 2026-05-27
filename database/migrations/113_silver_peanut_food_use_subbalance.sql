-- Migration 113: silver.peanut_food_use_subbalance (Tier 2B)
--
-- Two views, both shelled basis (million pounds):
--   silver.peanut_food_use_annual   — ERS Yearbook Table 12 canon
--   silver.peanut_food_use_monthly  — NASS Peanut Stocks & Processing
--
-- Annual sums of the monthly view should reconcile to the annual canon
-- within ±5% (NASS marketing-year aggregation, August-July).

-- ─────────────────────────────────────────────────────────────────
-- Annual (ERS Yearbook Table 12)
-- ─────────────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW silver.peanut_food_use_annual AS
WITH pivoted AS (
    SELECT
        marketing_year,
        MAX(CASE WHEN attribute_desc = 'Peanut butter food use'      THEN amount END) AS peanut_butter_mil_lbs,
        MAX(CASE WHEN attribute_desc = 'Peanut candy food use'       THEN amount END) AS peanut_candy_mil_lbs,
        MAX(CASE WHEN attribute_desc = 'Snack peanuts food use'      THEN amount END) AS snack_peanuts_mil_lbs,
        MAX(CASE WHEN attribute_desc = 'Other food use'              THEN amount END) AS other_food_mil_lbs,
        MAX(CASE WHEN attribute_desc = 'Clean in-shell food use'     THEN amount END) AS clean_in_shell_mil_lbs,
        MAX(CASE WHEN attribute_desc = 'Total food use'              THEN amount END) AS total_food_use_mil_lbs
    FROM bronze.ers_oilcrops_raw
    WHERE commodity ILIKE 'peanut%'
      AND table_number = '12'
      AND geography_desc2 = 'United States'
      AND timeperiod_desc = 'MY Total'
    GROUP BY marketing_year
)
SELECT
    marketing_year,
    'August-July'::text AS marketing_year_definition,
    peanut_butter_mil_lbs,
    peanut_candy_mil_lbs,
    snack_peanuts_mil_lbs,
    other_food_mil_lbs,
    clean_in_shell_mil_lbs,
    total_food_use_mil_lbs,
    -- Share breakdown
    CASE WHEN total_food_use_mil_lbs > 0
         THEN ROUND(peanut_butter_mil_lbs / total_food_use_mil_lbs * 100, 1) END AS pct_butter,
    CASE WHEN total_food_use_mil_lbs > 0
         THEN ROUND(peanut_candy_mil_lbs / total_food_use_mil_lbs * 100, 1) END AS pct_candy,
    CASE WHEN total_food_use_mil_lbs > 0
         THEN ROUND(snack_peanuts_mil_lbs / total_food_use_mil_lbs * 100, 1) END AS pct_snack,
    CASE WHEN total_food_use_mil_lbs > 0
         THEN ROUND(other_food_mil_lbs / total_food_use_mil_lbs * 100, 1) END AS pct_other,
    CASE WHEN total_food_use_mil_lbs > 0
         THEN ROUND(clean_in_shell_mil_lbs / total_food_use_mil_lbs * 100, 1) END AS pct_in_shell,
    -- Reconciliation
    (COALESCE(peanut_butter_mil_lbs, 0) + COALESCE(peanut_candy_mil_lbs, 0)
     + COALESCE(snack_peanuts_mil_lbs, 0) + COALESCE(other_food_mil_lbs, 0)
     + COALESCE(clean_in_shell_mil_lbs, 0) - COALESCE(total_food_use_mil_lbs, 0)) AS reconciliation_sum_check,
    'ERS Oil Crops Yearbook Table 12'::text AS source
FROM pivoted
ORDER BY marketing_year;

COMMENT ON VIEW silver.peanut_food_use_annual IS
'Tier 2B annual food use sub-balance (shelled basis, million pounds). Source = ERS Oil Crops Yearbook Table 12. Five sub-flows: peanut butter, candy, snacks, other edible, clean in-shell. Share percentages and reconciliation check included.';

-- ─────────────────────────────────────────────────────────────────
-- Monthly (NASS Peanut Stocks & Processing)
-- ─────────────────────────────────────────────────────────────────
-- All NASS values are in LB; ÷1000 converts to thousand-pounds, which
-- matches the spreadsheet display unit. Multiply by 0.001 again to
-- reach million pounds (used here for direct comparison to ERS).
CREATE OR REPLACE VIEW silver.peanut_food_use_monthly AS
WITH base AS (
    SELECT
        year,
        month,
        DATE(year || '-' || LPAD(month::text, 2, '0') || '-01') AS period,
        attribute_code,
        -- All NASS values are LB; convert to mil lbs
        value / 1000000.0 AS value_mil_lbs
    FROM bronze.nass_processing np
    JOIN silver.crush_attribute_reference car
      ON car.commodity = 'peanut'
     AND car.attribute_code IN (
        'edible_usage_peanut_butter', 'edible_usage_candy',
        'edible_usage_snacks', 'edible_usage_other',
        'edible_usage_total', 'in_shell_usage'
     )
     AND np.short_desc = car.nass_short_desc_filter
    WHERE np.source = 'NASS_PEANUT'
      AND np.month IS NOT NULL
      AND np.value IS NOT NULL
)
SELECT
    period,
    year,
    month,
    -- Calendar marketing year (Aug-Jul) — for reconciliation with annual canon
    CASE WHEN month >= 8 THEN year ELSE year - 1 END AS marketing_year_start,
    MAX(CASE WHEN attribute_code = 'edible_usage_peanut_butter' THEN value_mil_lbs END) AS peanut_butter_mil_lbs,
    MAX(CASE WHEN attribute_code = 'edible_usage_candy'         THEN value_mil_lbs END) AS peanut_candy_mil_lbs,
    MAX(CASE WHEN attribute_code = 'edible_usage_snacks'        THEN value_mil_lbs END) AS snack_peanuts_mil_lbs,
    MAX(CASE WHEN attribute_code = 'edible_usage_other'         THEN value_mil_lbs END) AS other_food_mil_lbs,
    MAX(CASE WHEN attribute_code = 'in_shell_usage'             THEN value_mil_lbs END) AS clean_in_shell_mil_lbs,
    MAX(CASE WHEN attribute_code = 'edible_usage_total'         THEN value_mil_lbs END) AS total_food_use_mil_lbs,
    'NASS Peanut Stocks & Processing'::text AS source
FROM base
GROUP BY period, year, month
ORDER BY period;

COMMENT ON VIEW silver.peanut_food_use_monthly IS
'Tier 2B monthly food use sub-balance (shelled basis, million pounds). Source = NASS Peanut Stocks & Processing via gold.fats_oils_crush_matrix attribute mapping. Coverage typically from May 2015 forward. marketing_year_start column is the Aug-Jul MY containing the period (used to reconcile to peanut_food_use_annual).';
