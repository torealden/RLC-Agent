-- ============================================================================
-- Migration 044: Fix HS 2306.50/60 oilcake commodity_group mapping
-- ============================================================================
-- Bug from migration 006: HS 2306500000 was labeled SAFFLOWER_MEAL.
-- Per WCO HS Harmonized System:
--   2306.50 = oilcake/meal "Of coconut or copra"  (= COPRA_MEAL)
--   2306.60 = oilcake/meal "Of palm nuts or kernels"  (= PALM_KERNEL_MEAL)
--   2306.90 = oilcake/meal "Other" (where safflower would actually classify)
--
-- Confirmed by US import origins for HS 230650: Papua New Guinea, Samoa,
-- Australia/Oceania — all coconut/copra producers, never safflower producers.
--
-- Fix: re-label the reference table and silver.census_trade_monthly rows.
-- Backfill of HS 2306600000 IMPORTS (currently 0 rows in bronze) is handled
-- by scripts/backfill_palm_kernel_meal_imports.py — must be run separately.
-- ============================================================================

BEGIN;

-- ----------------------------------------------------------------------------
-- 1. Fix reference table: HS 2306500000 → COPRA_MEAL
-- ----------------------------------------------------------------------------
UPDATE silver.trade_commodity_reference
   SET commodity_group = 'COPRA_MEAL',
       commodity_name  = 'Coconut/copra oilcake/meal'
 WHERE hs_code_10 = '2306500000'
   AND commodity_group = 'SAFFLOWER_MEAL';

-- ----------------------------------------------------------------------------
-- 2. Re-label existing silver rows for HS 2306500000
-- ----------------------------------------------------------------------------
UPDATE silver.census_trade_monthly
   SET commodity_group = 'COPRA_MEAL',
       commodity_name  = 'Coconut/copra oilcake/meal',
       transformed_at  = NOW()
 WHERE hs_code = '2306500000'
   AND commodity_group = 'SAFFLOWER_MEAL';

-- ----------------------------------------------------------------------------
-- 3. (Optional) Add HS 2306900000 as the legitimate home for SAFFLOWER_MEAL
--    and other minor oilcakes. We do NOT seed bronze data here — only the
--    reference mapping so future ingests classify correctly.
-- ----------------------------------------------------------------------------
INSERT INTO silver.trade_commodity_reference
    (hs_code_10, hs_code_6, commodity_group, commodity_name, flow_type,
     source_unit, display_unit, conversion_factor, is_active, notes)
VALUES
    ('2306900000', '230690', 'OILCAKE_OTHER', 'Other oilcakes/meals (HS 2306.90)', 'EXPORTS',
     'KG', 'Short Tons', 0.001102311000, TRUE,
     'HS 2306.90 = "Other" oilcakes — includes safflower, sesame, etc. Added migration 044'),
    ('2306900000', '230690', 'OILCAKE_OTHER', 'Other oilcakes/meals (HS 2306.90)', 'IMPORTS',
     'KG', 'Short Tons', 0.001102311000, TRUE,
     'HS 2306.90 = "Other" oilcakes — includes safflower, sesame, etc. Added migration 044')
ON CONFLICT DO NOTHING;

COMMIT;

-- ----------------------------------------------------------------------------
-- Verification
-- ----------------------------------------------------------------------------
-- After this migration:
--   SELECT hs_code_6, commodity_group, flow_type FROM silver.trade_commodity_reference
--   WHERE hs_code_6 IN ('230650','230660','230690') ORDER BY hs_code_6, flow_type;
-- Should return:
--   230650  COPRA_MEAL         EXPORTS
--   230650  COPRA_MEAL         IMPORTS
--   230660  PALM_KERNEL_MEAL   EXPORTS
--   230660  PALM_KERNEL_MEAL   IMPORTS
--   230690  OILCAKE_OTHER      EXPORTS
--   230690  OILCAKE_OTHER      IMPORTS
