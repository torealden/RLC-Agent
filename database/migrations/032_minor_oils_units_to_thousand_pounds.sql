-- ============================================================================
-- Migration 032: Minor oils flat-file units mil lbs -> 000 lbs
-- ============================================================================
-- Continuation of mig 031 (which fixed corn). User established convention:
-- flat files store at 1,000x SMALLER unit than balance sheet displays.
-- Balance sheet uses mil lbs => flat file stores in 000 lbs.
--
-- Affected: palm, palm_kernel, safflower, coconut, peanut (oil-only fields).
-- Peanut grain/cake fields are already in 'thousand pounds' — left untouched.
--
-- After this migration the values written by FatsOilsUpdater will match the
-- row-3 unit text the user already corrected to 'thousand pounds'.
-- ============================================================================

BEGIN;

UPDATE silver.crush_attribute_reference
SET display_unit = '000 lbs',
    conversion_factor = 0.001
WHERE source_unit = 'LB'
  AND display_unit = 'mil lbs'
  AND commodity IN ('palm', 'palm_kernel', 'safflower', 'coconut', 'peanut');

COMMIT;
