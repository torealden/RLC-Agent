-- Migration 068: refine is_optional flag to mean "expected in air permit"
-- Date: 2026-05-09
--
-- The is_optional flag is used by the coverage metric. A function flagged
-- TRUE is excluded from "required for full coverage."
--
-- Functions that EXIST on every plant but typically don't appear in
-- Title V air permit data (because they're not air emission sources)
-- should be marked optional so they don't drag the coverage score:
--
--   - compressed_air: not an emission source
--   - storage_crude_oil: closed tanks, no VOC vents
--   - wastewater: water permit, not air permit
--
-- Functions that ARE expected in a Title V should remain required.
-- The coverage gap then becomes a real signal of LLM extractor misses.

BEGIN;

UPDATE reference.equipment_function_canonical
SET is_optional = TRUE,
    diagnostic_notes = COALESCE(diagnostic_notes || ' ', '') ||
                       '[2026-05-09] Marked optional: typically not in Title V air permit.'
WHERE function_id IN (
    'oilseed_crush.compressed_air',
    'oilseed_crush.storage_crude_oil',
    'oilseed_crush.wastewater'
);

COMMIT;
