-- ============================================================================
-- Migration 030: Fix corn oil _alt rows in silver.crush_attribute_reference
-- ============================================================================
-- PROBLEM:
-- The NASS Other Veg Oils tab in us_oilseed_crush.xlsm uses SHORT-form headers
-- for corn oil columns ("Crude Stocks", "Refined Produced", "Refined Stocks")
-- but the corresponding `_alt` reference rows pointed to NASS short_desc
-- filters that don't exist in bronze.nass_processing, so `gold.fats_oils_crush_matrix`
-- never returned data under those header_patterns.
--
-- Broken rows (verified against bronze.nass_processing):
--   crude_oil_stocks_alt        -> 'OIL, CORN, ONSITE & OFFSITE, CRUDE - STOCKS, MEASURED IN LB'  (does not exist)
--   refined_oil_production_alt  -> 'OIL, CORN, REFINED - PRODUCTION, MEASURED IN LB'              (NASS uses 'ONCE REFINED')
--   refined_oil_stocks_alt      -> 'OIL, CORN, ONSITE & OFFSITE, REFINED - STOCKS, MEASURED IN LB' (NASS uses 'ONCE REFINED')
--
-- FIX:
-- Point each _alt row at its PRIMARY counterpart's NASS filter. This way
-- both primary and alt header_patterns get populated with the same data,
-- so whichever form the Excel column uses, the VBA updater matches.
-- ============================================================================

BEGIN;

-- crude_oil_stocks_alt -> match crude_oil_stocks
UPDATE silver.crush_attribute_reference
SET nass_short_desc_filter = 'OIL, CORN, CRUDE - STOCKS, MEASURED IN LB'
WHERE commodity = 'corn'
  AND attribute_code = 'crude_oil_stocks_alt';

-- refined_oil_production_alt -> match refined_oil_production
UPDATE silver.crush_attribute_reference
SET nass_short_desc_filter = 'OIL, CORN, ONCE REFINED - PRODUCTION, MEASURED IN LB'
WHERE commodity = 'corn'
  AND attribute_code = 'refined_oil_production_alt';

-- refined_oil_stocks_alt -> match refined_oil_stocks
UPDATE silver.crush_attribute_reference
SET nass_short_desc_filter = 'OIL, CORN, ONSITE & OFFSITE, ONCE REFINED - STOCKS, MEASURED IN LB'
WHERE commodity = 'corn'
  AND attribute_code = 'refined_oil_stocks_alt';

COMMIT;

-- Verification
-- After this migration the following should each return > 0 rows:
--   SELECT COUNT(*) FROM gold.fats_oils_crush_matrix
--   WHERE commodity='corn' AND header_pattern='Crude Stocks' AND display_value IS NOT NULL;
--   SELECT COUNT(*) FROM gold.fats_oils_crush_matrix
--   WHERE commodity='corn' AND header_pattern='Refined Produced' AND display_value IS NOT NULL;
--   SELECT COUNT(*) FROM gold.fats_oils_crush_matrix
--   WHERE commodity='corn' AND header_pattern='Refined Stocks' AND display_value IS NOT NULL;
