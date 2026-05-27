-- Migration 116: silver lauric oils balance sheets (coconut + palm kernel)
--
-- Implements the 4-tier balance sheet model from
-- docs/specs/peanut_balance_sheet_model.md applied to lauric oils.
--
-- Key differences from peanut:
--   - No domestic production (US doesn't crush copra at scale)
--   - Tier 1 supply = Beg Stocks + Imports
--   - Marketing year = Oct-Sep (per ERS Table 32 convention)
--   - Food sub-flows are MODELED with explicit assumptions
--     (USDA doesn't break out lauric oil food use)
--
-- Per Tore (2026-05-27): lauric food sub-flows modeled with stated
-- assumptions now; per-facility data when we have it later.
--
-- Source: ERS Oil Crops Yearbook Table 32 (Edible fats and oils
-- supply and disappearance), 2006/07 - 2024/25.

-- ─────────────────────────────────────────────────────────────────
-- Tier 1: Coconut Oil Balance Sheet (annual, Oct-Sep MY, million pounds)
-- ─────────────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW silver.coconut_oil_balance_sheet AS
WITH pivoted AS (
    SELECT
        marketing_year,
        MAX(CASE WHEN attribute_desc = 'Stocks beginning October 1' THEN amount END) AS beginning_stocks_mil_lbs,
        MAX(CASE WHEN attribute_desc = 'Imports'                    THEN amount END) AS imports_mil_lbs,
        MAX(CASE WHEN attribute_desc = 'Domestic disappearance'     THEN amount END) AS domestic_disappearance_mil_lbs,
        MAX(CASE WHEN attribute_desc = 'Exports'                    THEN amount END) AS exports_mil_lbs
    FROM bronze.ers_oilcrops_raw
    WHERE commodity = 'Coconut oil'
      AND table_number = '32'
      AND geography_desc2 = 'United States'
      AND timeperiod_desc = 'MY Total'
    GROUP BY marketing_year
)
SELECT
    marketing_year,
    'October-September'::text AS marketing_year_definition,
    beginning_stocks_mil_lbs,
    imports_mil_lbs,
    -- Coconut oil: US production negligible (small Hawaiian production rounds to 0)
    0::numeric AS production_mil_lbs,
    (COALESCE(beginning_stocks_mil_lbs, 0) + COALESCE(imports_mil_lbs, 0)) AS total_supply_mil_lbs,
    domestic_disappearance_mil_lbs,
    exports_mil_lbs,
    (COALESCE(domestic_disappearance_mil_lbs, 0) + COALESCE(exports_mil_lbs, 0)) AS total_disappearance_mil_lbs,
    (COALESCE(beginning_stocks_mil_lbs, 0) + COALESCE(imports_mil_lbs, 0)
     - COALESCE(domestic_disappearance_mil_lbs, 0) - COALESCE(exports_mil_lbs, 0)) AS ending_stocks_calc_mil_lbs,
    CASE WHEN domestic_disappearance_mil_lbs > 0
         THEN ROUND(imports_mil_lbs / domestic_disappearance_mil_lbs * 100, 1)
    END AS imports_dependency_pct,
    'ERS Oil Crops Yearbook Table 32'::text AS source
FROM pivoted
ORDER BY marketing_year;

COMMENT ON VIEW silver.coconut_oil_balance_sheet IS
'Tier 1 coconut oil balance sheet (annual, Oct-Sep MY, million pounds). Source = ERS Oil Crops Yearbook Table 32. No domestic production (US is import-dependent). imports_dependency_pct surfaces this — typically >95% for coconut oil.';

-- ─────────────────────────────────────────────────────────────────
-- Tier 1: Palm Kernel Oil Balance Sheet (annual, Oct-Sep MY)
-- ─────────────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW silver.palm_kernel_oil_balance_sheet AS
WITH pivoted AS (
    SELECT
        marketing_year,
        MAX(CASE WHEN attribute_desc = 'Stocks beginning October 1' THEN amount END) AS beginning_stocks_mil_lbs,
        MAX(CASE WHEN attribute_desc = 'Imports'                    THEN amount END) AS imports_mil_lbs,
        MAX(CASE WHEN attribute_desc = 'Domestic disappearance'     THEN amount END) AS domestic_disappearance_mil_lbs,
        MAX(CASE WHEN attribute_desc = 'Exports'                    THEN amount END) AS exports_mil_lbs
    FROM bronze.ers_oilcrops_raw
    WHERE commodity = 'Palm kernel oil'
      AND table_number = '32'
      AND geography_desc2 = 'United States'
      AND timeperiod_desc = 'MY Total'
    GROUP BY marketing_year
)
SELECT
    marketing_year,
    'October-September'::text AS marketing_year_definition,
    beginning_stocks_mil_lbs,
    imports_mil_lbs,
    0::numeric AS production_mil_lbs,
    (COALESCE(beginning_stocks_mil_lbs, 0) + COALESCE(imports_mil_lbs, 0)) AS total_supply_mil_lbs,
    domestic_disappearance_mil_lbs,
    exports_mil_lbs,
    (COALESCE(domestic_disappearance_mil_lbs, 0) + COALESCE(exports_mil_lbs, 0)) AS total_disappearance_mil_lbs,
    (COALESCE(beginning_stocks_mil_lbs, 0) + COALESCE(imports_mil_lbs, 0)
     - COALESCE(domestic_disappearance_mil_lbs, 0) - COALESCE(exports_mil_lbs, 0)) AS ending_stocks_calc_mil_lbs,
    CASE WHEN domestic_disappearance_mil_lbs > 0
         THEN ROUND(imports_mil_lbs / domestic_disappearance_mil_lbs * 100, 1)
    END AS imports_dependency_pct,
    'ERS Oil Crops Yearbook Table 32'::text AS source
FROM pivoted
ORDER BY marketing_year;

COMMENT ON VIEW silver.palm_kernel_oil_balance_sheet IS
'Tier 1 palm kernel oil balance sheet (annual, Oct-Sep MY, million pounds). Source = ERS Oil Crops Yearbook Table 32. No US production. Typically used heavily in non-food industrial applications (oleochemicals, soaps) so non-food sub-flows dominate.';

-- ─────────────────────────────────────────────────────────────────
-- Tier 2B: Modeled food use sub-flows
-- ─────────────────────────────────────────────────────────────────
-- USDA does NOT break out lauric oil end-use. Per Tore's guidance
-- (2026-05-27), we MODEL the allocation with explicit assumptions and
-- update them as we learn more about the industry.
--
-- Sub-flow shares — coconut oil:
--   Confectionery:           25%
--   Baking / food service:   35%
--   Food industrial
--   (margarine, shortening): 10%
--   Non-food industrial
--   (soap, cosmetics, etc):  30%
--
-- Sub-flow shares — palm kernel oil:
--   Confectionery:           15%
--   Baking / food service:   15%
--   Food industrial:          5%
--   Non-food industrial:     65%
--
-- Assumptions are documented in
-- reference_peanut_conversion_and_modeling.md / lauric oils modeling
-- stance. Revisit when:
--  - We onboard a lauric-buying client
--  - Industry data (NOPA-equivalent for laurics, USAFL reports) becomes available
--  - We have facility-level data on major US lauric processors

CREATE TABLE IF NOT EXISTS reference.lauric_food_use_assumptions (
    commodity        varchar(32) NOT NULL,
    sub_flow         varchar(32) NOT NULL,
    share_pct        numeric NOT NULL,
    assumption_basis text,
    effective_from   date NOT NULL DEFAULT '2006-10-01',
    effective_to     date,
    PRIMARY KEY (commodity, sub_flow, effective_from)
);

INSERT INTO reference.lauric_food_use_assumptions
    (commodity, sub_flow, share_pct, assumption_basis)
VALUES
    ('coconut_oil',     'confectionery',       0.25, 'Industry estimate; coconut oil widely used in chocolate coatings and similar'),
    ('coconut_oil',     'baking_food_service', 0.35, 'Bakery shortenings and food service frying; largest food channel'),
    ('coconut_oil',     'food_industrial',     0.10, 'Margarine and shortening blends'),
    ('coconut_oil',     'non_food_industrial', 0.30, 'Soap, cosmetics, personal care'),
    ('palm_kernel_oil', 'confectionery',       0.15, 'Coating fats, similar to coconut oil applications'),
    ('palm_kernel_oil', 'baking_food_service', 0.15, 'Limited food use vs coconut oil'),
    ('palm_kernel_oil', 'food_industrial',     0.05, 'Specialty fats only'),
    ('palm_kernel_oil', 'non_food_industrial', 0.65, 'Oleochemicals, surfactants, soap — dominant end-use')
ON CONFLICT (commodity, sub_flow, effective_from) DO UPDATE SET
    share_pct = EXCLUDED.share_pct,
    assumption_basis = EXCLUDED.assumption_basis;

CREATE OR REPLACE VIEW silver.lauric_food_use_modeled AS
SELECT
    'coconut_oil'::text AS commodity,
    bs.marketing_year,
    a.sub_flow,
    a.share_pct,
    bs.domestic_disappearance_mil_lbs,
    ROUND((bs.domestic_disappearance_mil_lbs * a.share_pct)::numeric, 1) AS allocated_mil_lbs,
    a.assumption_basis,
    'MODELED — ERS T32 domestic disappearance x reference.lauric_food_use_assumptions'::text AS source
FROM silver.coconut_oil_balance_sheet bs
CROSS JOIN reference.lauric_food_use_assumptions a
WHERE a.commodity = 'coconut_oil'
UNION ALL
SELECT
    'palm_kernel_oil'::text,
    bs.marketing_year,
    a.sub_flow,
    a.share_pct,
    bs.domestic_disappearance_mil_lbs,
    ROUND((bs.domestic_disappearance_mil_lbs * a.share_pct)::numeric, 1),
    a.assumption_basis,
    'MODELED — ERS T32 domestic disappearance x reference.lauric_food_use_assumptions'::text
FROM silver.palm_kernel_oil_balance_sheet bs
CROSS JOIN reference.lauric_food_use_assumptions a
WHERE a.commodity = 'palm_kernel_oil'
ORDER BY commodity, marketing_year, sub_flow;

COMMENT ON VIEW silver.lauric_food_use_modeled IS
'Tier 2B lauric oil food use sub-balance (MODELED — not from USDA). Allocates ERS T32 Domestic disappearance across 4 sub-flows (confectionery, baking_food_service, food_industrial, non_food_industrial) using shares in reference.lauric_food_use_assumptions. Per Tore (2026-05-27), revisit assumptions as industry data improves or facility-level data is built.';
