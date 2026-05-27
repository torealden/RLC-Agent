-- Migration 114: peanut Tier 2A (crush) + Tier 3A (oil) + Tier 3B (meal)
--
-- Three views, all sourced from NASS Peanut Stocks & Processing
-- and NASS Fats and Oils, via existing gold.fats_oils_crush_matrix
-- attribute mapping. Monthly cadence (shelled basis where applicable).
--
-- All NASS values in bronze.nass_processing are in raw LB; this
-- migration converts to MIL LBS (÷1,000,000) for consistency with
-- the annual ERS Yearbook layer.

-- ─────────────────────────────────────────────────────────────────
-- Tier 2A: Peanut Crush Sub-Balance (monthly, shelled basis)
-- ─────────────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW silver.peanut_crush_subbalance AS
WITH base AS (
    SELECT
        np.year, np.month,
        DATE(np.year || '-' || LPAD(np.month::text, 2, '0') || '-01') AS period,
        car.attribute_code,
        np.value / 1000000.0 AS value_mil_lbs
    FROM bronze.nass_processing np
    JOIN silver.crush_attribute_reference car
      ON car.commodity = 'peanut'
     AND car.attribute_code IN (
        'shelled_peanuts_crushed',
        'shelled_crush_farmer_stock_basis',
        'crude_oil_production_mills',
        'cake_meal_production',
        'cake_meal_stocks',
        'shelled_oil_stocks_production'
     )
     AND np.short_desc = car.nass_short_desc_filter
    WHERE np.source = 'NASS_PEANUT'
      AND np.month IS NOT NULL
      AND np.value IS NOT NULL
),
formula_derived AS (
    -- shelled_crush_farmer_stock_basis is a derived formula (mig 029);
    -- pull pre-computed values from gold.peanut_formula_values.
    -- display_value here is in thousand pounds, divide by 1000 -> mil lbs.
    SELECT year, month,
           DATE(year || '-' || LPAD(month::text, 2, '0') || '-01') AS period,
           display_value / 1000.0 AS farmer_stock_basis_mil_lbs
    FROM gold.peanut_formula_values
    WHERE attribute_code = 'shelled_crush_farmer_stock_basis'
      AND commodity = 'peanut'
)
SELECT
    b.period,
    b.year,
    b.month,
    CASE WHEN b.month >= 8 THEN b.year ELSE b.year - 1 END AS marketing_year_start,
    MAX(CASE WHEN b.attribute_code = 'shelled_peanuts_crushed'      THEN b.value_mil_lbs END) AS shelled_crushed_mil_lbs,
    MAX(fd.farmer_stock_basis_mil_lbs)                                                          AS farmer_stock_basis_mil_lbs,
    MAX(CASE WHEN b.attribute_code = 'crude_oil_production_mills'   THEN b.value_mil_lbs END) AS crude_oil_production_mil_lbs,
    MAX(CASE WHEN b.attribute_code = 'cake_meal_production'         THEN b.value_mil_lbs END) AS cake_meal_production_mil_lbs,
    MAX(CASE WHEN b.attribute_code = 'cake_meal_stocks'             THEN b.value_mil_lbs END) AS cake_meal_stocks_mil_lbs,
    MAX(CASE WHEN b.attribute_code = 'shelled_oil_stocks_production' THEN b.value_mil_lbs END) AS shelled_oil_stocks_mil_lbs,
    -- Derived yields
    CASE
        WHEN MAX(CASE WHEN b.attribute_code = 'shelled_peanuts_crushed' THEN b.value_mil_lbs END) > 0
        THEN ROUND(
            MAX(CASE WHEN b.attribute_code = 'crude_oil_production_mills' THEN b.value_mil_lbs END)
            / NULLIF(MAX(CASE WHEN b.attribute_code = 'shelled_peanuts_crushed' THEN b.value_mil_lbs END), 0) * 100, 1)
    END AS implied_oil_yield_pct,
    CASE
        WHEN MAX(CASE WHEN b.attribute_code = 'shelled_peanuts_crushed' THEN b.value_mil_lbs END) > 0
        THEN ROUND(
            MAX(CASE WHEN b.attribute_code = 'cake_meal_production' THEN b.value_mil_lbs END)
            / NULLIF(MAX(CASE WHEN b.attribute_code = 'shelled_peanuts_crushed' THEN b.value_mil_lbs END), 0) * 100, 1)
    END AS implied_meal_yield_pct,
    'NASS Peanut Stocks & Processing'::text AS source
FROM base b
LEFT JOIN formula_derived fd USING (year, month)
GROUP BY b.period, b.year, b.month
ORDER BY b.period;

COMMENT ON VIEW silver.peanut_crush_subbalance IS
'Tier 2A peanut crush sub-balance (monthly, shelled basis except farmer_stock_basis column which is farmer-stock basis = shelled x 1.33). Derived oil/meal yield percentages included. Source: NASS Peanut Stocks & Processing + derived formulas from mig 029.';

-- ─────────────────────────────────────────────────────────────────
-- Tier 3A: Peanut Oil Balance Sheet (monthly, million pounds)
-- ─────────────────────────────────────────────────────────────────
-- Combines NASS Peanut mills-side (crude production + stocks) with
-- NASS Fats & Oils refiners-side (crude processed, refined produced,
-- refined removed for processing, crude stocks).
CREATE OR REPLACE VIEW silver.peanut_oil_balance_sheet AS
WITH peanut_mills AS (
    SELECT np.year, np.month,
           DATE(np.year || '-' || LPAD(np.month::text, 2, '0') || '-01') AS period,
           MAX(CASE WHEN car.attribute_code = 'crude_oil_production_mills' THEN np.value / 1000000.0 END) AS crude_oil_production_mil_lbs,
           MAX(CASE WHEN car.attribute_code = 'crude_oil_stocks_mills'     THEN np.value / 1000000.0 END) AS crude_oil_stocks_mills_mil_lbs
    FROM bronze.nass_processing np
    JOIN silver.crush_attribute_reference car
      ON car.commodity = 'peanut'
     AND car.attribute_code IN ('crude_oil_production_mills', 'crude_oil_stocks_mills')
     AND np.short_desc = car.nass_short_desc_filter
    WHERE np.source = 'NASS_PEANUT' AND np.month IS NOT NULL AND np.value IS NOT NULL
    GROUP BY np.year, np.month
),
fats_oils AS (
    SELECT np.year, np.month,
           DATE(np.year || '-' || LPAD(np.month::text, 2, '0') || '-01') AS period,
           MAX(CASE WHEN car.attribute_code = 'crude_oil_refined'              THEN np.value / 1000000.0 END) AS crude_processed_in_refining_mil_lbs,
           MAX(CASE WHEN car.attribute_code = 'refined_oil_production'         THEN np.value / 1000000.0 END) AS refined_oil_production_mil_lbs,
           MAX(CASE WHEN car.attribute_code = 'refined_oil_further_processing' THEN np.value / 1000000.0 END) AS refined_oil_removed_mil_lbs,
           MAX(CASE WHEN car.attribute_code = 'crude_oil_stocks'               THEN np.value / 1000000.0 END) AS crude_oil_stocks_refiners_mil_lbs,
           MAX(CASE WHEN car.attribute_code = 'refined_oil_stocks'             THEN np.value / 1000000.0 END) AS refined_oil_stocks_mil_lbs
    FROM bronze.nass_processing np
    JOIN silver.crush_attribute_reference car
      ON car.commodity = 'peanut'
     AND car.attribute_code IN ('crude_oil_refined', 'refined_oil_production', 'refined_oil_further_processing', 'crude_oil_stocks', 'refined_oil_stocks')
     AND np.short_desc = car.nass_short_desc_filter
    WHERE np.source = 'NASS_FATS_OILS' AND np.month IS NOT NULL AND np.value IS NOT NULL
    GROUP BY np.year, np.month
)
SELECT
    COALESCE(pm.period, fo.period) AS period,
    COALESCE(pm.year, fo.year) AS year,
    COALESCE(pm.month, fo.month) AS month,
    CASE WHEN COALESCE(pm.month, fo.month) >= 8 THEN COALESCE(pm.year, fo.year)
         ELSE COALESCE(pm.year, fo.year) - 1 END AS marketing_year_start,
    -- Mills (production) side
    pm.crude_oil_production_mil_lbs,
    pm.crude_oil_stocks_mills_mil_lbs,
    -- Refiners side
    fo.crude_processed_in_refining_mil_lbs,
    fo.refined_oil_production_mil_lbs,
    fo.refined_oil_removed_mil_lbs,
    fo.crude_oil_stocks_refiners_mil_lbs,
    fo.refined_oil_stocks_mil_lbs,
    -- Cross-source reconciliation: mills production vs refiners processing
    -- They won't equal in any given month due to stock-bridge lag, but the
    -- 12-mo rolling avg should be close.
    (COALESCE(pm.crude_oil_production_mil_lbs, 0) - COALESCE(fo.crude_processed_in_refining_mil_lbs, 0)) AS production_minus_processed_mil_lbs,
    'NASS Peanut + NASS Fats & Oils'::text AS source
FROM peanut_mills pm
FULL OUTER JOIN fats_oils fo USING (year, month)
ORDER BY COALESCE(pm.year, fo.year), COALESCE(pm.month, fo.month);

COMMENT ON VIEW silver.peanut_oil_balance_sheet IS
'Tier 3A peanut oil balance sheet (monthly, million pounds). Combines mills-side data (crude oil production + stocks from NASS Peanut Stocks & Processing) with refiners-side data (crude processed, refined produced, refined removed, stocks from NASS Fats & Oils). production_minus_processed_mil_lbs is the cross-source variance; should be close to 0 in a 12-month rolling avg.';

-- ─────────────────────────────────────────────────────────────────
-- Tier 3B: Peanut Cake & Meal Balance Sheet (monthly, million pounds)
-- ─────────────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW silver.peanut_meal_balance_sheet AS
WITH nass AS (
    SELECT np.year, np.month,
           DATE(np.year || '-' || LPAD(np.month::text, 2, '0') || '-01') AS period,
           MAX(CASE WHEN car.attribute_code = 'cake_meal_production' THEN np.value / 1000000.0 END) AS production_mil_lbs,
           MAX(CASE WHEN car.attribute_code = 'cake_meal_stocks'     THEN np.value / 1000000.0 END) AS ending_stocks_mil_lbs
    FROM bronze.nass_processing np
    JOIN silver.crush_attribute_reference car
      ON car.commodity = 'peanut'
     AND car.attribute_code IN ('cake_meal_production', 'cake_meal_stocks')
     AND np.short_desc = car.nass_short_desc_filter
    WHERE np.source = 'NASS_PEANUT' AND np.month IS NOT NULL AND np.value IS NOT NULL
    GROUP BY np.year, np.month
)
SELECT
    period, year, month,
    CASE WHEN month >= 8 THEN year ELSE year - 1 END AS marketing_year_start,
    production_mil_lbs,
    ending_stocks_mil_lbs,
    -- Implied domestic disappearance derived as residual:
    --   prev_stocks + production - exports - stocks = domestic_use
    -- Census trade exports for peanut meal (HS code TBD) would close the loop;
    -- placeholder for now, populate when trade ingestion is wired here.
    NULL::numeric AS exports_mil_lbs_placeholder,
    NULL::numeric AS implied_domestic_use_mil_lbs,
    'NASS Peanut Stocks & Processing'::text AS source
FROM nass
ORDER BY period;

COMMENT ON VIEW silver.peanut_meal_balance_sheet IS
'Tier 3B peanut cake/meal balance sheet (monthly, million pounds). Production and ending stocks from NASS. Exports and implied domestic use are placeholders until Census trade HS code mapping is wired in (peanut oilcake HS 2305.00 / oil-cake/meal HS 2305).';
