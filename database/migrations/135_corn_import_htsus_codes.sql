-- Migration 135: Fix CORN import HS codes (HTSUS, not Schedule B)
-- =============================================================================
-- Problem: corn IMPORTS in silver.trade_commodity_reference used Schedule B
-- (export) codes — 1005902020/902030/902035/904065 — which do not exist in
-- HTSUS. Census returns NOTHING for them on the imports endpoint, so all
-- grain-corn imports (the bulk, ~24 mil bu/yr from Canada) were never fetched.
-- Only seed corn (1005100010) came through (~0.4 mil bu/yr), making the corn
-- balance sheet imports line ~40x too low.
--
-- Census imports HS10 enumeration (2024) shows the real corn import codes:
--   1005100010  YELLOW CORN, SEED                       11.0M kg  (already had)
--   1005100090  CORN SEED, OTHER THAN YELLOW             6.2M kg
--   1005902015  CERTIFIED ORGANIC YELLOW DENT CORN     123.3M kg
--   1005902025  YELLOW DENT CORN, EXCEPT SEED          397.0M kg  <- dominant
--   1005904040  POPCORN, UNPOPPED                        3.5M kg  (specialty — excluded)
--   1005904060  CORN, EXCEPT SEED, YELLOW DENT          87.8M kg
-- Field-corn total (excl popcorn) = 625.3M kg = 24.6 mil bu, matches USDA.
--
-- Popcorn (1005904040) is a distinct specialty crop, not in the field-corn
-- S&D, so it is intentionally excluded. Add it later if a popcorn line is wanted.
-- =============================================================================

-- 1. Deactivate the dead Schedule-B import rows (no HTSUS import data exists).
UPDATE silver.trade_commodity_reference
SET is_active = false,
    notes = COALESCE(notes,'') || ' [mig135: deactivated — Schedule B code, not valid HTSUS import]'
WHERE commodity_group = 'CORN'
  AND flow_type = 'IMPORTS'
  AND hs_code_10 IN ('1005902020','1005902030','1005902035','1005904065');

-- 2. Add the correct HTSUS corn import codes (KG -> 000 Bushels).
INSERT INTO silver.trade_commodity_reference
    (hs_code_10, hs_code_6, commodity_group, commodity_name, flow_type,
     source_unit, display_unit, conversion_factor, is_active, notes,
     price_unit_label, price_unit_factor)
VALUES
    ('1005100090','100510','CORN','Corn seed, other than yellow','IMPORTS',
     'KG','000 Bushels',0.000039368250, true,
     'KG to 000 Bushels (56 lbs/bu). HTSUS import code (mig135)','usd_per_bushel',25.4011773),
    ('1005902015','100590','CORN','Organic yellow dent corn, except seed','IMPORTS',
     'KG','000 Bushels',0.000039368250, true,
     'KG to 000 Bushels (56 lbs/bu). HTSUS import code (mig135)','usd_per_bushel',25.4011773),
    ('1005902025','100590','CORN','Yellow dent corn, except seed','IMPORTS',
     'KG','000 Bushels',0.000039368250, true,
     'KG to 000 Bushels (56 lbs/bu). HTSUS import code, dominant import line (mig135)','usd_per_bushel',25.4011773),
    ('1005904060','100590','CORN','Corn, except seed, other than yellow dent','IMPORTS',
     'KG','000 Bushels',0.000039368250, true,
     'KG to 000 Bushels (56 lbs/bu). HTSUS import code (mig135)','usd_per_bushel',25.4011773);
