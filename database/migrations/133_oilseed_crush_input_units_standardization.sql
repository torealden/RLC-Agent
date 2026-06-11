-- ============================================================================
-- Migration 133: Standardize oilseed-crush input-sheet units across commodities
-- ============================================================================
-- Establishes a single rule for what unit lands in us_oilseed_crush.xlsm cells.
--
-- US balance-sheet display units (the ground truth, set by Tore 2026-06-11):
--   - meal: always '000 short tons' (every commodity)
--   - oil:  always 'million pounds' (every commodity)
--   - seed: per-commodity:
--       soybeans, flaxseed         -> million bushels
--       canola, sunflower, peanut, safflower -> million pounds
--       cottonseed                 -> '000 short tons' (yield in lbs/acre via 480-lb bales)
--
-- Input-sheet rule = balance-sheet unit / 1000:
--   - meal:   tons     (BS 000 ST / 1000)
--   - oil:    000 lbs  (BS mil lbs / 1000)
--   - seed:
--       canola / sunflower / peanut / safflower: 000 lbs  (BS mil lbs / 1000)
--       cottonseed: tons (BS 000 ST / 1000)
--       soybeans: SKIP this migration -- the soy crush row has a parallel bu
--         column (soybeans_crushed_bu, mil bu) that the BS reads; leaving
--         soybeans_crushed at '000 tons' for now. Revisit if/when bu column
--         needs realignment.
--
-- Source-unit math:
--   - LB source  -> display_unit '000 lbs', conversion_factor 0.001
--   - LB source  -> display_unit 'tons'   , conversion_factor 0.0005  (1 lb = 0.0005 short ton)
--   - TONS source-> display_unit 'tons'   , conversion_factor 1.0
--   - TONS source-> display_unit '000 lbs', conversion_factor 2.0     (1 ton = 2000 lbs)
--
-- BS workbooks (us_*_balance_sheets.xlsx) reference the input sheet cells with
-- their own scaling formulas. After this migration the values landing in the
-- input cells change scale; Tore will rescale the BS formulas by hand (his
-- preference -- per AskUserQuestion 2026-06-11). The .bas updater R4 unit
-- labels on us_oilseed_crush.xlsm also need a manual pass.
--
-- This continues the work begun in migrations 031 (corn oils mil lbs -> 000 lbs)
-- and 032 (palm/palm_kernel/safflower/coconut/peanut oils same).
-- ============================================================================

BEGIN;

-- ----------------------------------------------------------------------------
-- 1. Oils: LB source -> 000 lbs (factor 0.001)
--    Covers all 'mil lbs' rows (soybeans, canola, cottonseed, sunflower).
--    Coconut/corn/palm/palm_kernel/peanut/safflower oils were fixed by 031/032.
-- ----------------------------------------------------------------------------
UPDATE silver.crush_attribute_reference
SET display_unit = '000 lbs',
    conversion_factor = 0.001
WHERE source_unit = 'LB'
  AND display_unit = 'mil lbs'
  AND commodity IN ('soybeans', 'canola', 'cottonseed', 'sunflower');

-- ----------------------------------------------------------------------------
-- 2. Oils: normalize 'thousand pounds' -> '000 lbs' (cosmetic, factor unchanged)
--    Peanut block had inconsistent display_unit text; align it.
-- ----------------------------------------------------------------------------
UPDATE silver.crush_attribute_reference
SET display_unit = '000 lbs'
WHERE source_unit = 'LB'
  AND display_unit = 'thousand pounds'
  AND conversion_factor = 0.001;

-- ----------------------------------------------------------------------------
-- 3. Meals: TONS source -> 'tons' (factor 1.0)
--    Soybeans (meal + millfeed), canola, cottonseed.
-- ----------------------------------------------------------------------------
UPDATE silver.crush_attribute_reference
SET display_unit = 'tons',
    conversion_factor = 1.0
WHERE source_unit = 'TONS'
  AND display_unit = '000 tons'
  AND attribute_code IN (
      'meal_production', 'meal_stocks',
      'meal_animal_feed', 'meal_edible_protein',
      'millfeed_production', 'millfeed_stocks'
  )
  AND commodity IN ('soybeans', 'canola', 'cottonseed');

-- ----------------------------------------------------------------------------
-- 4. Meals: LB source -> 'tons' (factor 0.0005)
--    Peanut cake-meal (production + stocks), sunflower meal estimate.
-- ----------------------------------------------------------------------------
UPDATE silver.crush_attribute_reference
SET display_unit = 'tons',
    conversion_factor = 0.0005
WHERE source_unit = 'LB'
  AND commodity = 'peanut'
  AND attribute_code IN ('cake_meal_production', 'cake_meal_stocks');

UPDATE silver.crush_attribute_reference
SET display_unit = 'tons',
    conversion_factor = 0.0005
WHERE source_unit = 'LB'
  AND commodity = 'sunflower'
  AND attribute_code = 'meal_production_est';

-- ----------------------------------------------------------------------------
-- 5. Seeds: TONS source -> '000 lbs' (factor 2.0) for canola
--    BS shows canola seed in million pounds, so input is thousand pounds.
-- ----------------------------------------------------------------------------
UPDATE silver.crush_attribute_reference
SET display_unit = '000 lbs',
    conversion_factor = 2.0
WHERE commodity = 'canola'
  AND attribute_code = 'seeds_crushed'
  AND source_unit = 'TONS';

-- ----------------------------------------------------------------------------
-- 6. Seeds: TONS source -> 'tons' (factor 1.0) for cottonseed
--    BS shows cottonseed in '000 short tons' so input is tons.
--    (This is the specific change Tore requested 2026-06-11.)
-- ----------------------------------------------------------------------------
UPDATE silver.crush_attribute_reference
SET display_unit = 'tons',
    conversion_factor = 1.0
WHERE commodity = 'cottonseed'
  AND attribute_code = 'seeds_crushed'
  AND source_unit = 'TONS';

-- ----------------------------------------------------------------------------
-- 7. Seeds: LB source -> '000 lbs' (factor 0.001) for sunflower est row
--    BS shows sunflower seed in mil lbs, so input is 000 lbs.
--    Currently stored as 5e-7 (which produced '000 tons'). Realign.
-- ----------------------------------------------------------------------------
UPDATE silver.crush_attribute_reference
SET display_unit = '000 lbs',
    conversion_factor = 0.001
WHERE commodity = 'sunflower'
  AND attribute_code = 'seeds_crushed_est'
  AND source_unit = 'LB';

-- ----------------------------------------------------------------------------
-- Verification: dump every commodity x attribute that survives in the table.
-- (Run as a separate SELECT after the BEGIN/COMMIT to inspect.)
-- ----------------------------------------------------------------------------

COMMIT;
