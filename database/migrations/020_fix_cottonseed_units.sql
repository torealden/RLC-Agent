-- ============================================================================
-- Migration: 020_fix_cottonseed_units.sql
-- Date: 2026-02-08
-- Description: Fix cottonseed seed and meal display units and conversion factors
--
-- Issue: All cottonseed entries were set to '000 Pounds' with factor 0.002204620.
--        Cottonseed seed and meal should be in 'Short Tons' per US trade standards.
--        Cottonseed oil was already correct at '000 Pounds'.
--
-- Current gold.trade_export_mapped formula: quantity_KG * conversion_factor
-- (The view does NOT use the * 1000 multiplier shown in the SQL file)
--
-- Correct factors for KG input (matching current view formula):
--   KG -> Short Tons:  1 / 907.185 = 0.001102311
--   KG -> 000 Pounds:  2.20462 / 1000 = 0.002204620 (unchanged for oil)
--
-- NOTE: source_unit is metadata only - does not affect gold view calculation.
--       Bronze data is stored in KG for all cottonseed codes.
-- ============================================================================

-- ============================================================================
-- COTTONSEED (SEED) - Change from 000 Pounds to Short Tons
-- ============================================================================

UPDATE silver.trade_commodity_reference SET
    display_unit = 'Short Tons',
    conversion_factor = 0.001102311,
    source_unit = 'KG',
    notes = 'KG to Short Tons (1/907.185)'
WHERE commodity_group = 'COTTONSEED'
  AND hs_code_10 IN ('1207210000', '1207290000');

-- ============================================================================
-- COTTONSEED MEAL - Change from 000 Pounds to Short Tons
-- ============================================================================

UPDATE silver.trade_commodity_reference SET
    display_unit = 'Short Tons',
    conversion_factor = 0.001102311,
    source_unit = 'KG',
    notes = 'KG to Short Tons (1/907.185)'
WHERE commodity_group = 'COTTONSEED_MEAL'
  AND hs_code_10 = '2306100000';

-- ============================================================================
-- COTTONSEED OIL - Fix source_unit metadata (factor already correct)
-- ============================================================================

UPDATE silver.trade_commodity_reference SET
    source_unit = 'KG',
    notes = 'KG to 000 Pounds (2.20462/1000)'
WHERE commodity_group = 'COTTONSEED_OIL'
  AND hs_code_10 IN ('1512210000', '1512290020', '1512290040');

-- ============================================================================
-- VERIFICATION
-- ============================================================================
-- Run to verify:
-- SELECT commodity_group, hs_code_10, flow_type, source_unit, display_unit,
--        conversion_factor::text
-- FROM silver.trade_commodity_reference
-- WHERE commodity_group LIKE 'COTTONSEED%'
-- ORDER BY commodity_group, flow_type, hs_code_10;
--
-- Expected:
--   COTTONSEED       1207210000  EXPORTS  KG  Short Tons  0.001102311
--   COTTONSEED       1207290000  EXPORTS  KG  Short Tons  0.001102311
--   COTTONSEED       1207210000  IMPORTS  KG  Short Tons  0.001102311
--   COTTONSEED       1207290000  IMPORTS  KG  Short Tons  0.001102311
--   COTTONSEED_MEAL  2306100000  EXPORTS  KG  Short Tons  0.001102311
--   COTTONSEED_MEAL  2306100000  IMPORTS  KG  Short Tons  0.001102311
--   COTTONSEED_OIL   1512210000  EXPORTS  KG  000 Pounds  0.002204620
--   COTTONSEED_OIL   1512290020  EXPORTS  KG  000 Pounds  0.002204620
--   COTTONSEED_OIL   1512290040  EXPORTS  KG  000 Pounds  0.002204620
--   COTTONSEED_OIL   1512210000  IMPORTS  KG  000 Pounds  0.002204620
--   COTTONSEED_OIL   1512290020  IMPORTS  KG  000 Pounds  0.002204620
--   COTTONSEED_OIL   1512290040  IMPORTS  KG  000 Pounds  0.002204620
