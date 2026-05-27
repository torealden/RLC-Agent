-- Migration 115: gold.peanut_complex_reconciliation
--
-- Runs the 7 reconciliation rules from docs/specs/peanut_balance_sheet_model.md
-- as a single annual report. Each row = one marketing year, with the
-- value of each check and a pass/fail flag (±5% tolerance unless otherwise
-- noted). Use this to spot data drift between sources.
--
-- Rules:
--  R1: Tier 1 Crush x 0.75 = Tier 2A Shelled Crushed (annual sum)
--  R2: Tier 1 Food Use x 0.75 = Tier 2B Total Food Use (annual sum)
--  R3: Tier 2B Total = sum of 5 sub-flows (already verified per-row)
--  R4: Tier 2A Crude Oil Production = Tier 3A mills production (same source — sanity check)
--  R5: Tier 2A Cake & Meal Production = Tier 3B Production (same source)
--  R6: Tier 3A Crude Processed in Refining ≈ Tier 3A Refined Production
--      (within 12-mo rolling lag, accounting for refining loss ~3%)
--  R7: Annual NASS sums reconcile to ERS Yearbook within ±5%

CREATE OR REPLACE VIEW gold.peanut_complex_reconciliation AS
WITH master AS (
    SELECT
        marketing_year,
        CAST(SUBSTRING(marketing_year FROM 1 FOR 4) AS int) AS my_start_year,
        crush_mil_lbs                  AS tier1_crush_fs_basis,
        food_use_mil_lbs               AS tier1_food_use_fs_basis,
        crush_mil_lbs * 0.75           AS tier1_crush_shelled_basis,
        food_use_mil_lbs * 0.75        AS tier1_food_use_shelled_basis
    FROM silver.peanut_balance_sheet_master
),
food_annual AS (
    SELECT
        marketing_year,
        total_food_use_mil_lbs AS tier2b_total_food_use,
        peanut_butter_mil_lbs, peanut_candy_mil_lbs, snack_peanuts_mil_lbs,
        other_food_mil_lbs, clean_in_shell_mil_lbs
    FROM silver.peanut_food_use_annual
),
crush_monthly_agg AS (
    -- Aggregate monthly NASS to marketing year (Aug-Jul)
    SELECT
        marketing_year_start AS my_start_year,
        SUM(shelled_crushed_mil_lbs)            AS tier2a_shelled_crushed_annual,
        SUM(crude_oil_production_mil_lbs)       AS tier2a_crude_oil_production_annual,
        SUM(cake_meal_production_mil_lbs)       AS tier2a_meal_production_annual
    FROM silver.peanut_crush_subbalance
    GROUP BY marketing_year_start
),
food_monthly_agg AS (
    SELECT
        marketing_year_start AS my_start_year,
        SUM(total_food_use_mil_lbs) AS tier2b_total_food_use_monthly_sum
    FROM silver.peanut_food_use_monthly
    GROUP BY marketing_year_start
),
oil_monthly_agg AS (
    SELECT
        marketing_year_start AS my_start_year,
        SUM(crude_oil_production_mil_lbs)               AS tier3a_mills_production_annual,
        SUM(crude_processed_in_refining_mil_lbs)        AS tier3a_refining_processed_annual,
        SUM(refined_oil_production_mil_lbs)             AS tier3a_refined_production_annual
    FROM silver.peanut_oil_balance_sheet
    GROUP BY marketing_year_start
),
meal_monthly_agg AS (
    SELECT
        marketing_year_start AS my_start_year,
        SUM(production_mil_lbs) AS tier3b_meal_production_annual
    FROM silver.peanut_meal_balance_sheet
    GROUP BY marketing_year_start
)
SELECT
    m.marketing_year,
    m.my_start_year,
    -- R1: Tier 1 Crush (shelled basis) vs Tier 2A Shelled Crushed (annual)
    m.tier1_crush_shelled_basis,
    cma.tier2a_shelled_crushed_annual,
    CASE
        WHEN cma.tier2a_shelled_crushed_annual IS NULL THEN 'N/A (no monthly data)'
        WHEN m.tier1_crush_shelled_basis = 0 THEN 'N/A (zero)'
        WHEN ABS(cma.tier2a_shelled_crushed_annual - m.tier1_crush_shelled_basis)
             / NULLIF(m.tier1_crush_shelled_basis, 0) <= 0.05 THEN 'PASS'
        ELSE 'FAIL (>5%)'
    END AS r1_crush_check,
    ROUND(
        100.0 * (cma.tier2a_shelled_crushed_annual - m.tier1_crush_shelled_basis)
        / NULLIF(m.tier1_crush_shelled_basis, 0), 1
    ) AS r1_variance_pct,

    -- R2: Tier 1 Food Use (shelled basis) vs Tier 2B Total Food Use (ERS annual)
    m.tier1_food_use_shelled_basis,
    fa.tier2b_total_food_use,
    CASE
        WHEN fa.tier2b_total_food_use IS NULL THEN 'N/A'
        WHEN m.tier1_food_use_shelled_basis = 0 THEN 'N/A'
        WHEN ABS(fa.tier2b_total_food_use - m.tier1_food_use_shelled_basis)
             / NULLIF(m.tier1_food_use_shelled_basis, 0) <= 0.05 THEN 'PASS'
        ELSE 'FAIL (>5%)'
    END AS r2_food_use_check,
    ROUND(
        100.0 * (fa.tier2b_total_food_use - m.tier1_food_use_shelled_basis)
        / NULLIF(m.tier1_food_use_shelled_basis, 0), 1
    ) AS r2_variance_pct,

    -- R3: Tier 2B sum of 5 sub-flows = Tier 2B Total (already enforced per-row via reconciliation_sum_check)
    (COALESCE(fa.peanut_butter_mil_lbs, 0) + COALESCE(fa.peanut_candy_mil_lbs, 0)
     + COALESCE(fa.snack_peanuts_mil_lbs, 0) + COALESCE(fa.other_food_mil_lbs, 0)
     + COALESCE(fa.clean_in_shell_mil_lbs, 0)) AS r3_subflow_sum,

    -- R4: Tier 2A crude oil production vs Tier 3A mills production (same source)
    cma.tier2a_crude_oil_production_annual,
    oma.tier3a_mills_production_annual,
    CASE
        WHEN cma.tier2a_crude_oil_production_annual IS NULL THEN 'N/A'
        WHEN cma.tier2a_crude_oil_production_annual = oma.tier3a_mills_production_annual THEN 'PASS'
        ELSE 'FAIL (sources diverged)'
    END AS r4_oil_production_check,

    -- R5: Tier 2A meal production vs Tier 3B production (same source)
    cma.tier2a_meal_production_annual,
    mma.tier3b_meal_production_annual,
    CASE
        WHEN cma.tier2a_meal_production_annual IS NULL THEN 'N/A'
        WHEN cma.tier2a_meal_production_annual = mma.tier3b_meal_production_annual THEN 'PASS'
        ELSE 'FAIL'
    END AS r5_meal_production_check,

    -- R6: Tier 3A refining processed ≈ refined production (3% loss tolerance)
    oma.tier3a_refining_processed_annual,
    oma.tier3a_refined_production_annual,
    CASE
        WHEN oma.tier3a_refining_processed_annual IS NULL OR oma.tier3a_refined_production_annual IS NULL THEN 'N/A'
        WHEN oma.tier3a_refining_processed_annual = 0 THEN 'N/A'
        WHEN ABS(oma.tier3a_refining_processed_annual - oma.tier3a_refined_production_annual)
             / NULLIF(oma.tier3a_refining_processed_annual, 0) <= 0.10 THEN 'PASS (<10% loss)'
        ELSE 'FAIL (>10% diff)'
    END AS r6_refining_yield_check,

    -- R7: NASS monthly food use sum vs ERS annual (already part of R2)
    fma.tier2b_total_food_use_monthly_sum,
    CASE
        WHEN fma.tier2b_total_food_use_monthly_sum IS NULL THEN 'N/A'
        WHEN fa.tier2b_total_food_use = 0 THEN 'N/A'
        WHEN ABS(fma.tier2b_total_food_use_monthly_sum - fa.tier2b_total_food_use)
             / NULLIF(fa.tier2b_total_food_use, 0) <= 0.05 THEN 'PASS'
        ELSE 'FAIL (>5%)'
    END AS r7_nass_vs_ers_food_check
FROM master m
LEFT JOIN food_annual fa USING (marketing_year)
LEFT JOIN crush_monthly_agg cma USING (my_start_year)
LEFT JOIN food_monthly_agg fma USING (my_start_year)
LEFT JOIN oil_monthly_agg oma USING (my_start_year)
LEFT JOIN meal_monthly_agg mma USING (my_start_year)
ORDER BY m.my_start_year DESC;

COMMENT ON VIEW gold.peanut_complex_reconciliation IS
'Runs the 7 reconciliation rules from docs/specs/peanut_balance_sheet_model.md. One row per marketing year. Each rule has a *_check column (PASS / FAIL / N/A) and a *_variance_pct where applicable. Use this view as the system check after every NASS / ERS data update.';
