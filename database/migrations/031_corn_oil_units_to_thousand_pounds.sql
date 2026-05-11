-- ============================================================================
-- Migration 031: Convert corn oil flat-file units from 'mil lbs' to '000 lbs'
-- ============================================================================
-- USER CONVENTION (from project memory + Apr 2026 guidance):
--   Flat files store data in units 1,000x SMALLER than the balance sheet
--   display unit. Balance sheet shows mil lbs => flat file stores 000 lbs.
--   This matches the trade-flow convention (Census kg -> 000 pounds in flat
--   file -> mil pounds in balance sheet).
--
-- Before this migration:
--   corn rows: source_unit=LB, display_unit='mil lbs', factor=0.000001
--   gold view returned 152.317 (mil lbs) for ~152M lbs raw NASS value
--
-- After:
--   corn rows: source_unit=LB, display_unit='000 lbs', factor=0.001
--   gold view returns 152,317 (000 lbs)
--
-- WORKBOOK ACTION REQUIRED (manual, per user):
--   On NASS Other Veg Oils tab, change row-3 unit text in the corn block
--   (cols V-AA) from 'mil lbs' to '000 lbs'. Then re-run Ctrl+U.
-- ============================================================================

BEGIN;

UPDATE silver.crush_attribute_reference
SET display_unit = '000 lbs',
    conversion_factor = 0.001
WHERE commodity = 'corn'
  AND source_unit = 'LB'
  AND display_unit = 'mil lbs';

-- Verification:
-- After migration, ALL 11 corn rows should show factor=0.001 and unit='000 lbs'
-- gold.fats_oils_crush_matrix should return display_value ~150,000 not ~150
COMMIT;
