-- Migration 123: Peanut trade-flow HS codes for the food balance sheet
--
-- Tore's spec (2026-05-29): split US peanut trade into three buckets to
-- feed a small balance sheet that gives "peanuts used in food":
--   - Unshelled (in-shell, not for sowing)         HS 1202.41
--   - Raw shelled (not for sowing)                 HS 1202.42
--   - Prepared & preserved                         HS 2008.11
--      split into peanut butter vs other prep
--   + seed peanuts (1202.30) for completeness
--
-- HS depth chosen by hybrid: 6-digit for 1202.41/42/30 (10-digit returns
-- 204 No Content from Census API for these — see scripts probing notes),
-- 10-digit for 2008.11 since peanut butter (2008.11.05) is a distinct
-- consumer market vs other prep peanuts (2008.11.15).
--
-- Census API confirmed (2024 data probe):
--   1202.41 imports = 0 kg                  exports = 185M kg
--   1202.42 imports = 0 kg                  exports = qty suppressed (value $394M)
--   1202.30 imports = 0 kg                  exports = 17M kg
--   2008.11.05 imports = 17.3M kg           exports = 0
--   2008.11.15 imports = 2.9M kg            exports = 0
--
-- For zero-import series, downstream updater should write 0 (not blank).

BEGIN;

-- New peanut HS code entries (5 codes x 2 flows = 10 rows)
INSERT INTO silver.trade_commodity_reference (
    hs_code_10, hs_code_6,
    commodity_group, commodity_name,
    flow_type, source_unit, display_unit, conversion_factor,
    is_active, notes
) VALUES
    -- ------------ Shelled raw (1202.42) ------------
    ('1202420000', '120242', 'PEANUTS', 'Peanuts, shelled (raw)',
     'EXPORTS', 'KG', '000 Pounds', 2.20462,
     TRUE, 'Census API quirk: exports report value but qty=0 at 6-digit. Tracked anyway for the value-side.'),
    ('1202420000', '120242', 'PEANUTS', 'Peanuts, shelled (raw)',
     'IMPORTS', 'KG', '000 Pounds', 2.20462,
     TRUE, 'Confirmed 2024: zero imports. Updater should write 0 not blank.'),

    -- ------------ Seed peanuts (1202.30) ------------
    ('1202300000', '120230', 'PEANUTS', 'Peanuts, seed',
     'EXPORTS', 'KG', '000 Pounds', 2.20462,
     TRUE, 'Treated/untreated peanut seed for sowing.'),
    ('1202300000', '120230', 'PEANUTS', 'Peanuts, seed',
     'IMPORTS', 'KG', '000 Pounds', 2.20462,
     TRUE, 'Confirmed 2024: zero imports. Updater should write 0 not blank.'),

    -- ------------ Peanut butter (2008.11.05) ------------
    ('2008110500', '200811', 'PEANUTS', 'Peanut butter',
     'EXPORTS', 'KG', '000 Pounds', 2.20462,
     TRUE, 'Prepared peanut paste/butter.'),
    ('2008110500', '200811', 'PEANUTS', 'Peanut butter',
     'IMPORTS', 'KG', '000 Pounds', 2.20462,
     TRUE, 'Largest US peanut import line.'),

    -- ------------ Other prep/preserved (2008.11.15) ------------
    ('2008111500', '200811', 'PEANUTS', 'Peanuts, prepared or preserved (other)',
     'EXPORTS', 'KG', '000 Pounds', 2.20462,
     TRUE, 'Roasted, blanched, dry-roasted, etc. NOT peanut butter.'),
    ('2008111500', '200811', 'PEANUTS', 'Peanuts, prepared or preserved (other)',
     'IMPORTS', 'KG', '000 Pounds', 2.20462,
     TRUE, 'Roasted, blanched, dry-roasted, etc. NOT peanut butter.')

ON CONFLICT DO NOTHING;

COMMIT;

-- Verification:
-- SELECT hs_code_10, flow_type, commodity_name
-- FROM silver.trade_commodity_reference
-- WHERE commodity_group = 'PEANUTS' ORDER BY hs_code_10, flow_type;
