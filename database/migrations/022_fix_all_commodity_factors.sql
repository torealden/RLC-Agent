-- ============================================================================
-- Migration: 022_fix_all_commodity_factors.sql
-- Date: 2026-02-10
-- Description: Comprehensive fix of ALL conversion factors in
--              silver.trade_commodity_reference
--
-- ROOT CAUSE: The gold view formula is qty * factor (NO * 1000 multiplier).
--   But most factors were designed for qty * factor * 1000, making them
--   1000x too small. Additionally, some HS codes report in METRIC TONS
--   from the Census API (not KG), requiring different factors.
--
-- Bronze data units (from silver.census_trade_monthly CASE logic):
--   METRIC TONS: 1005902030, 1005902035, 1001992055, 1001992015,
--                1003900000, 1201900095, 2303100010, 2303100020, 2303300000
--   KG: All other HS codes
--
-- Gold view formula: quantity * conversion_factor = display_unit_value
--
-- Standard bushel weights (USDA):
--   Soybeans: 60 lbs = 27.2155 kg
--   Corn:     56 lbs = 25.4012 kg
--   Wheat:    60 lbs = 27.2155 kg
--   Barley:   48 lbs = 21.7724 kg
--   Sorghum:  56 lbs = 25.4012 kg
--   Flaxseed: 56 lbs = 25.4012 kg
--
-- ALREADY CORRECT (skip these):
--   COTTONSEED      (migration 020)
--   COTTONSEED_MEAL (migration 020)
--   COTTONSEED_OIL  (migration 020)
--   SOYBEANS        (migration 021)
--   SORGHUM         (0.0000393683 KG->000 Bu correct)
--   COTTON          (0.000004593 KG->000 Bales correct)
--   RICE            (0.0000220462 KG->1,000 CWT correct)
-- ============================================================================

-- ============================================================================
-- SECTION 1: KG-based OILS — factor 1000x too small
-- Correct: KG * 2.20462 / 1000 = 000 Pounds → factor = 0.002204620
-- ============================================================================

-- CANOLA (seed) — display_unit '000 Pounds'
UPDATE silver.trade_commodity_reference SET
    conversion_factor = 0.002204622,
    source_unit = 'KG',
    notes = 'KG to 000 Pounds (2.20462/1000) - fixed migration 022'
WHERE commodity_group = 'CANOLA'
  AND source_unit = 'KG';

-- CANOLA_OIL
UPDATE silver.trade_commodity_reference SET
    conversion_factor = 0.002204620,
    source_unit = 'KG',
    notes = 'KG to 000 Pounds (2.20462/1000) - fixed migration 022'
WHERE commodity_group = 'CANOLA_OIL';

-- CORN_OIL
UPDATE silver.trade_commodity_reference SET
    conversion_factor = 0.002204620,
    source_unit = 'KG',
    notes = 'KG to 000 Pounds (2.20462/1000) - fixed migration 022'
WHERE commodity_group = 'CORN_OIL';

-- LINSEED_OIL
UPDATE silver.trade_commodity_reference SET
    conversion_factor = 0.002204620,
    source_unit = 'KG',
    notes = 'KG to 000 Pounds (2.20462/1000) - fixed migration 022'
WHERE commodity_group = 'LINSEED_OIL';

-- OTHER_VEG_OIL
UPDATE silver.trade_commodity_reference SET
    conversion_factor = 0.002204620,
    source_unit = 'KG',
    notes = 'KG to 000 Pounds (2.20462/1000) - fixed migration 022'
WHERE commodity_group = 'OTHER_VEG_OIL';

-- PALM_KERNEL_OIL
UPDATE silver.trade_commodity_reference SET
    conversion_factor = 0.002204620,
    source_unit = 'KG',
    notes = 'KG to 000 Pounds (2.20462/1000) - fixed migration 022'
WHERE commodity_group = 'PALM_KERNEL_OIL';

-- PALM_OIL
UPDATE silver.trade_commodity_reference SET
    conversion_factor = 0.002204620,
    source_unit = 'KG',
    notes = 'KG to 000 Pounds (2.20462/1000) - fixed migration 022'
WHERE commodity_group = 'PALM_OIL';

-- SESAME_OIL
UPDATE silver.trade_commodity_reference SET
    conversion_factor = 0.002204620,
    source_unit = 'KG',
    notes = 'KG to 000 Pounds (2.20462/1000) - fixed migration 022'
WHERE commodity_group = 'SESAME_OIL';

-- SOYBEAN_OIL
UPDATE silver.trade_commodity_reference SET
    conversion_factor = 0.002204620,
    source_unit = 'KG',
    notes = 'KG to 000 Pounds (2.20462/1000) - fixed migration 022'
WHERE commodity_group = 'SOYBEAN_OIL';

-- SUNFLOWER (seed) — display_unit '000 Pounds'
UPDATE silver.trade_commodity_reference SET
    conversion_factor = 0.002204622,
    source_unit = 'KG',
    notes = 'KG to 000 Pounds (2.20462/1000) - fixed migration 022'
WHERE commodity_group = 'SUNFLOWER';

-- SUNFLOWER_OIL
UPDATE silver.trade_commodity_reference SET
    conversion_factor = 0.002204622,
    source_unit = 'KG',
    notes = 'KG to 000 Pounds (2.20462/1000) - fixed migration 022'
WHERE commodity_group = 'SUNFLOWER_OIL';

-- ============================================================================
-- SECTION 2: KG-based MEALS — factor 1000x too small
-- Correct: KG / 907.185 = Short Tons → factor = 0.001102311
-- ============================================================================

-- PALM_KERNEL_MEAL
UPDATE silver.trade_commodity_reference SET
    conversion_factor = 0.001102311,
    source_unit = 'KG',
    notes = 'KG to Short Tons (1/907.185) - fixed migration 022'
WHERE commodity_group = 'PALM_KERNEL_MEAL';

-- PEANUT_MEAL
UPDATE silver.trade_commodity_reference SET
    conversion_factor = 0.001102311,
    source_unit = 'KG',
    notes = 'KG to Short Tons (1/907.185) - fixed migration 022'
WHERE commodity_group = 'PEANUT_MEAL';

-- SAFFLOWER_MEAL
UPDATE silver.trade_commodity_reference SET
    conversion_factor = 0.001102311,
    source_unit = 'KG',
    notes = 'KG to Short Tons (1/907.185) - fixed migration 022'
WHERE commodity_group = 'SAFFLOWER_MEAL';

-- SOYBEAN_MEAL (KG codes only — exclude 2302500000 which already has correct factor)
UPDATE silver.trade_commodity_reference SET
    conversion_factor = 0.001102311,
    source_unit = 'KG',
    notes = 'KG to Short Tons (1/907.185) - fixed migration 022'
WHERE commodity_group = 'SOYBEAN_MEAL'
  AND hs_code_10 != '2302500000'
  AND conversion_factor < 0.001;

-- SOYBEAN_MEAL 2302500000 — fix source_unit label (factor was accidentally correct)
UPDATE silver.trade_commodity_reference SET
    source_unit = 'KG',
    notes = 'KG to Short Tons (1/907.185) - source_unit label fixed migration 022'
WHERE commodity_group = 'SOYBEAN_MEAL'
  AND hs_code_10 = '2302500000';

-- SUNFLOWER_MEAL
UPDATE silver.trade_commodity_reference SET
    conversion_factor = 0.001102311,
    source_unit = 'KG',
    notes = 'KG to Short Tons (1/907.185) - fixed migration 022'
WHERE commodity_group = 'SUNFLOWER_MEAL';

-- ============================================================================
-- SECTION 3: KG-based codes that are 1000x too LARGE
-- These had the raw conversion (e.g. 2.20462 lbs/KG) without /1000 for display
-- ============================================================================

-- CANOLA_MEAL — had MT→Short Tons factor (1.10231) but data is KG
UPDATE silver.trade_commodity_reference SET
    conversion_factor = 0.001102311,
    source_unit = 'KG',
    notes = 'KG to Short Tons (1/907.185) - fixed migration 022'
WHERE commodity_group = 'CANOLA_MEAL';

-- MUSTARD — had 2.20462 (lbs/KG) instead of 0.002204620 (000 Pounds/KG)
UPDATE silver.trade_commodity_reference SET
    conversion_factor = 0.002204620,
    source_unit = 'KG',
    notes = 'KG to 000 Pounds (2.20462/1000) - fixed migration 022'
WHERE commodity_group = 'MUSTARD';

-- PEANUTS — had 2.20462 instead of 0.002204620
UPDATE silver.trade_commodity_reference SET
    conversion_factor = 0.002204620,
    source_unit = 'KG',
    notes = 'KG to 000 Pounds (2.20462/1000) - fixed migration 022'
WHERE commodity_group = 'PEANUTS';

-- SAFFLOWER (seed) — had 2.20462 instead of 0.002204620
UPDATE silver.trade_commodity_reference SET
    conversion_factor = 0.002204620,
    source_unit = 'KG',
    notes = 'KG to 000 Pounds (2.20462/1000) - fixed migration 022'
WHERE commodity_group = 'SAFFLOWER';

-- SESAME (seed) — had 2.20462 instead of 0.002204620
UPDATE silver.trade_commodity_reference SET
    conversion_factor = 0.002204620,
    source_unit = 'KG',
    notes = 'KG to 000 Pounds (2.20462/1000) - fixed migration 022'
WHERE commodity_group = 'SESAME';

-- ============================================================================
-- SECTION 4: FLAXSEED — factor 1,000,000x too large
-- Had 39.368 (bushels per MT) instead of KG→000 Bushels
-- Flaxseed: 56 lbs/bu = 25.4012 kg/bu
-- KG → 000 Bushels: 1/(25.4012 * 1000) = 0.0000393683
-- ============================================================================

UPDATE silver.trade_commodity_reference SET
    conversion_factor = 0.0000393683,
    source_unit = 'KG',
    notes = 'KG to 000 Bushels (56 lbs/bu, 1/25401.2) - fixed migration 022'
WHERE commodity_group = 'FLAXSEED';

-- ============================================================================
-- SECTION 5: METRIC TON codes — need MT-based factors
-- These HS codes return quantity in metric tons from Census API
-- ============================================================================

-- CORN MT codes (MT → 000 Bushels)
-- 56 lbs/bu = 25.4012 kg/bu; 1 MT = 39.3683 bu; 000 Bu = 0.0393683
-- Confirmed via Census API UNIT_QY1 field: 1005902020, 1005902030, 1005902035, 1005904065 = 'T'
UPDATE silver.trade_commodity_reference SET
    conversion_factor = 0.0393683,
    source_unit = 'MT',
    notes = 'MT to 000 Bushels (39.3683 bu/MT / 1000) - fixed migration 022'
WHERE commodity_group = 'CORN'
  AND hs_code_10 IN ('1005902020', '1005902030', '1005902035', '1005904065');

-- CORN KG codes that were 1000x too small
-- 1005100000: had 3.937E-8, correct is 0.0000393683
UPDATE silver.trade_commodity_reference SET
    conversion_factor = 0.0000393683,
    source_unit = 'KG',
    notes = 'KG to 000 Bushels (56 lbs/bu, 1/25401.2) - fixed migration 022'
WHERE commodity_group = 'CORN'
  AND hs_code_10 = '1005100000';

-- WHEAT MT codes (MT → 000 Bushels)
-- 60 lbs/bu = 27.2155 kg/bu; 1 MT = 36.7437 bu; 000 Bu = 0.0367437
-- Confirmed via Census API UNIT_QY1 field: 1001190000, 1001992015, 1001992055 = 'T'
UPDATE silver.trade_commodity_reference SET
    conversion_factor = 0.0367437,
    source_unit = 'MT',
    notes = 'MT to 000 Bushels (36.7437 bu/MT / 1000) - fixed migration 022'
WHERE commodity_group = 'WHEAT'
  AND hs_code_10 IN ('1001190000', '1001992015', '1001992055');

-- WHEAT KG code that was 1000x too small
-- 1001910000: had 3.6744E-8, correct is 0.0000367437
UPDATE silver.trade_commodity_reference SET
    conversion_factor = 0.0000367437,
    source_unit = 'KG',
    notes = 'KG to 000 Bushels (60 lbs/bu, 1/27215.5) - fixed migration 022'
WHERE commodity_group = 'WHEAT'
  AND hs_code_10 = '1001910000';

-- BARLEY MT code (MT → 000 Bushels)
-- 48 lbs/bu = 21.7724 kg/bu; 1 MT = 45.9296 bu; 000 Bu = 0.0459296
UPDATE silver.trade_commodity_reference SET
    conversion_factor = 0.0459296,
    source_unit = 'MT',
    notes = 'MT to 000 Bushels (48 lbs/bu, 45.9296 bu/MT / 1000) - fixed migration 022'
WHERE commodity_group = 'BARLEY'
  AND hs_code_10 = '1003900000';

-- BARLEY KG code — also had wrong bushel weight (used 24 kg/bu instead of 21.7724)
-- 48 lbs/bu = 21.7724 kg/bu; KG → 000 Bu = 1/(21.7724 * 1000) = 0.0000459296
UPDATE silver.trade_commodity_reference SET
    conversion_factor = 0.0000459296,
    source_unit = 'KG',
    notes = 'KG to 000 Bushels (48 lbs/bu, 1/21772.4) - fixed migration 022'
WHERE commodity_group = 'BARLEY'
  AND hs_code_10 = '1003100000';

-- CORN_GLUTEN (MT → Short Tons)
-- 1 MT = 1.10231 Short Tons
UPDATE silver.trade_commodity_reference SET
    conversion_factor = 1.10231,
    source_unit = 'MT',
    notes = 'MT to Short Tons (1.10231) - fixed migration 022'
WHERE commodity_group = 'CORN_GLUTEN'
  AND hs_code_10 IN ('2303100010', '2303100020');

-- DDGS (MT → Short Tons)
-- 1 MT = 1.10231 Short Tons
UPDATE silver.trade_commodity_reference SET
    conversion_factor = 1.10231,
    source_unit = 'MT',
    notes = 'MT to Short Tons (1.10231) - fixed migration 022'
WHERE commodity_group = 'DDGS'
  AND hs_code_10 = '2303300000';

-- ============================================================================
-- VERIFICATION QUERY
-- ============================================================================
-- Run after migration to verify all factors:
--
-- SELECT commodity_group, hs_code_10, flow_type, source_unit, display_unit,
--        conversion_factor::text, notes
-- FROM silver.trade_commodity_reference
-- ORDER BY commodity_group, flow_type, hs_code_10;
--
-- Quick sanity checks (using Nov 2025 data):
--
-- Soybean Meal exports (should be ~800-1000K short tons/month):
-- SELECT SUM(ct.quantity * cr.conversion_factor) as total_short_tons
-- FROM bronze.census_trade ct
-- JOIN silver.trade_commodity_reference cr
--   ON ct.hs_code = cr.hs_code_10 AND ct.flow = LOWER(cr.flow_type)
-- WHERE cr.commodity_group = 'SOYBEAN_MEAL' AND ct.year=2025 AND ct.month=11
--   AND ct.flow='exports' AND ct.country_code NOT IN ('0000','-');
--
-- DDGS exports (should be ~900-1100K short tons/month):
-- SELECT SUM(ct.quantity * cr.conversion_factor) as total_short_tons
-- FROM bronze.census_trade ct
-- JOIN silver.trade_commodity_reference cr
--   ON ct.hs_code = cr.hs_code_10 AND ct.flow = LOWER(cr.flow_type)
-- WHERE cr.commodity_group = 'DDGS' AND ct.year=2025 AND ct.month=11
--   AND ct.flow='exports' AND ct.country_code NOT IN ('0000','-');
