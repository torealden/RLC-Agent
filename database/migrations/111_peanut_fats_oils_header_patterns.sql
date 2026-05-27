-- Migration 111: Fix peanut Fats & Oils header_patterns
--
-- The peanut_crush sheet in us_oilseed_crush.xlsm has four columns
-- (V/W/X/Y) sourced from the NASS Fats and Oils monthly report's
-- "Selected Oilseed Crushing, Production, Consumption and Stocks"
-- Peanut Refine/Consumption/Stocks block. The header_pattern values
-- in silver.crush_attribute_reference for these attributes were the
-- short canonical names ("Crude Oil Refined", etc.) which did not
-- match (substring or otherwise) the descriptive sheet headers
-- ending in "(Fats and Oils)".
--
-- Without a match, the FatsOilsUpdaterSQL VBA macro left those four
-- columns empty after Ctrl+U.
--
-- This migration updates the header_pattern values to exact matches.
-- Side benefit: col Z "Crude oil stocks" now only matches
-- crude_oil_stocks_mills (Peanut Stocks & Processing report) instead
-- of being overwritten by the Fats & Oils refining attr.

UPDATE silver.crush_attribute_reference
   SET header_pattern = 'Crude oil processed in refining (Fats and Oils)'
 WHERE commodity = 'peanut'
   AND attribute_code = 'crude_oil_refined';

UPDATE silver.crush_attribute_reference
   SET header_pattern = 'Once refined oil produced (Fats and Oils)'
 WHERE commodity = 'peanut'
   AND attribute_code = 'refined_oil_production';

UPDATE silver.crush_attribute_reference
   SET header_pattern = 'Once refined oil removed for use in processing (Fats and Oils)'
 WHERE commodity = 'peanut'
   AND attribute_code = 'refined_oil_further_processing';

UPDATE silver.crush_attribute_reference
   SET header_pattern = 'Crude oil on hand end of month (Fats and Oils)'
 WHERE commodity = 'peanut'
   AND attribute_code = 'crude_oil_stocks';

-- Verification query (run manually to confirm):
--   SELECT attribute_code, header_pattern, nass_short_desc_filter
--   FROM silver.crush_attribute_reference
--   WHERE commodity = 'peanut'
--     AND attribute_code IN (
--       'crude_oil_refined', 'refined_oil_production',
--       'refined_oil_further_processing', 'crude_oil_stocks'
--     )
--   ORDER BY attribute_code;
