-- 154_fix_soy_crush_matrix_dedup_and_units.sql
--
-- Fix why us_soy_crush.xlsm meal + oil columns never machine-fill (only crush did). Two bugs, both in
-- the soy crush matrix path; verified against the sheet's own declared units (row 4) and its known-good
-- Dec-2025 values. Modeled on gold.trade_export_matrix, which scopes to ONE series per cell (the reason
-- the trade updater "just works").
--
-- BUG 1 -- DEDUP. gold.nass_soy_crush_matrix joins gold.nass_crush_mapped on (year, month,
--   attribute_code) but NOT commodity. attribute_code 'meal_production' / 'crude_oil_production' are
--   GENERIC codes shared by soybeans, canola, cottonseed, so canola's and cottonseed's values fan into
--   the soybean column -> 3 meal rows / 4 oil rows per cell. Any consumer (the Ctrl+U macro, a script)
--   grabs an arbitrary one. Fix: add `AND d.commodity = 'soybeans'` to the join. Crush was never
--   affected because 'soybeans_crushed' is a soy-only code.
--
-- BUG 2 -- UNITS. display_value = raw_value * conversion_factor. Crush was configured correctly
--   (cf 0.001 -> '000 tons'), but:
--     meal cols 11-16 : cf = 1.0     (raw TONS)  -> matrix 5,107,259 vs sheet 5,107.26  (1000x high)
--     oil  cols 22-33 : cf = 0.001   ('000 lbs') -> matrix 2,657,399 vs sheet 2,657.4   (1000x high)
--   The sheet's row-4 units are '000 tons' (meal) and 'mil lbs' (oil), so:
--     meal -> cf 0.001,     display_unit '000 tons'
--     oil  -> cf 0.000001,  display_unit 'mil lbs'
--   Tie-out after fix: meal 5,107,259*0.001 = 5,107.26 ✓ ; oil raw*1e-6 = 2,657.4 ✓ ; crush unchanged.
--
-- Reversible (see rollback block at end, commented). Only touches commodity='soybeans' rows.

BEGIN;

-- BUG 2a: meal / millfeed columns -> thousand short tons
UPDATE silver.crush_attribute_reference
   SET conversion_factor = 0.001, display_unit = '000 tons'
 WHERE commodity = 'soybeans'
   AND attribute_code IN ('meal_production', 'millfeed_production', 'meal_animal_feed',
                          'meal_edible_protein', 'meal_stocks', 'millfeed_stocks')
   AND conversion_factor = 1.0;               -- guard: only the mis-set ones

-- BUG 2b: crude/refined oil columns (production, use, stocks) -> million pounds
UPDATE silver.crush_attribute_reference
   SET conversion_factor = 0.000001, display_unit = 'mil lbs'
 WHERE commodity = 'soybeans'
   AND source_unit = 'LB'
   AND attribute_code IN ('crude_oil_production', 'crude_oil_inedible_use', 'crude_oil_refined',
                          'refined_oil_production', 'refined_oil_further_processing',
                          'refined_oil_inedible_use', 'refined_oil_edible_use',
                          'crude_oil_stocks_total', 'crude_oil_crusher_stocks',
                          'refined_oil_stocks', 'oil_offsite_stocks')
   AND conversion_factor = 0.001;             -- guard

-- BUG 1: dedup the matrix by scoping the data join to soybeans (mirrors gold.trade_export_matrix).
CREATE OR REPLACE VIEW gold.nass_soy_crush_matrix AS
SELECT m.year, m.month, m.month_date,
       a.attribute_code, a.display_name, a.spreadsheet_column,
       d.display_value, a.display_unit
  FROM ( SELECT DISTINCT ncm.year, ncm.month, ncm.month_date
           FROM gold.nass_crush_mapped ncm
          WHERE ncm.attribute_code::text IN (
                    SELECT car.attribute_code FROM silver.crush_attribute_reference car
                     WHERE car.commodity::text = 'soybeans' AND car.is_formula = false AND car.is_active = true)
       ) m
  CROSS JOIN ( SELECT car.attribute_code, car.display_name, car.spreadsheet_column, car.display_unit
                 FROM silver.crush_attribute_reference car
                WHERE car.commodity::text = 'soybeans' AND car.is_formula = false AND car.is_active = true
             ) a
  LEFT JOIN gold.nass_crush_mapped d
         ON d.year = m.year AND d.month = m.month
        AND d.attribute_code::text = a.attribute_code::text
        AND d.commodity::text = 'soybeans'          -- <<< THE FIX: scope data side to soybeans
 ORDER BY m.year, m.month, a.spreadsheet_column;

COMMIT;

-- Rollback (if needed):
--   UPDATE silver.crush_attribute_reference SET conversion_factor=1.0, display_unit='tons'
--     WHERE commodity='soybeans' AND attribute_code IN ('meal_production','millfeed_production',
--       'meal_animal_feed','meal_edible_protein','meal_stocks','millfeed_stocks');
--   UPDATE silver.crush_attribute_reference SET conversion_factor=0.001, display_unit='000 lbs'
--     WHERE commodity='soybeans' AND source_unit='LB' AND attribute_code IN ('crude_oil_production',
--       'crude_oil_inedible_use','crude_oil_refined','refined_oil_production','refined_oil_further_processing',
--       'refined_oil_inedible_use','refined_oil_edible_use','crude_oil_stocks_total','crude_oil_crusher_stocks',
--       'refined_oil_stocks','oil_offsite_stocks');
--   (and restore the prior view definition without the commodity predicate)
