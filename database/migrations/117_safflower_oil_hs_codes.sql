-- Migration 117: Add safflower oil HS codes + verify safflower complex mappings
--
-- HS code situation for safflower:
--   1207.60.00.00  — Safflower seed              -> SAFFLOWER       (already in DB, 2,398 rows)
--   1512.11.00.40  — Safflower oil, crude        -> SAFFLOWER_OIL   (THIS MIGRATION adds)
--   1512.19.00.40  — Safflower oil, refined      -> SAFFLOWER_OIL   (THIS MIGRATION adds)
--   2306.90.00.00  — Other oilcake/meal          -> OILCAKE_OTHER   (already mapped via mig 044)
--                    PROXY for safflower meal — HS doesn't break out
--                    safflower meal at any granularity; bucket mixes
--                    safflower, sesame, mustard, and other minor oilseed
--                    meals.
--
-- US HTS Schedule B uses 10-digit codes .0040 for safflower variants of
-- HS 1512.11 (crude) and HS 1512.19 (refined). Currently no rows of
-- these codes in bronze.census_trade — adding the reference entries here
-- will get them picked up by the next census trade collection run
-- (collector reads HS codes from silver.trade_commodity_reference).
--
-- Historical backfill: run scripts/backfill_safflower_oil_trade.py after
-- this migration to fetch the 2013+ history from Census API.

BEGIN;

INSERT INTO silver.trade_commodity_reference
    (hs_code_10, hs_code_6, commodity_group, commodity_name, flow_type,
     source_unit, display_unit, conversion_factor, is_active, notes)
VALUES
    -- Crude safflower oil
    ('1512110040', '151211', 'SAFFLOWER_OIL', 'Safflower oil, crude', 'EXPORTS',
     'KG', 'Million Pounds', 0.00000220462, TRUE,
     'HS 1512.11.00.40 — Safflower oil, crude. Added migration 117 (2026-05-28).'),
    ('1512110040', '151211', 'SAFFLOWER_OIL', 'Safflower oil, crude', 'IMPORTS',
     'KG', 'Million Pounds', 0.00000220462, TRUE,
     'HS 1512.11.00.40 — Safflower oil, crude. Added migration 117 (2026-05-28).'),
    -- Refined safflower oil
    ('1512190040', '151219', 'SAFFLOWER_OIL', 'Safflower oil, refined', 'EXPORTS',
     'KG', 'Million Pounds', 0.00000220462, TRUE,
     'HS 1512.19.00.40 — Safflower oil, refined. Added migration 117 (2026-05-28).'),
    ('1512190040', '151219', 'SAFFLOWER_OIL', 'Safflower oil, refined', 'IMPORTS',
     'KG', 'Million Pounds', 0.00000220462, TRUE,
     'HS 1512.19.00.40 — Safflower oil, refined. Added migration 117 (2026-05-28).')
ON CONFLICT (hs_code_10, flow_type) DO UPDATE SET
    commodity_group   = EXCLUDED.commodity_group,
    commodity_name    = EXCLUDED.commodity_name,
    source_unit       = EXCLUDED.source_unit,
    display_unit      = EXCLUDED.display_unit,
    conversion_factor = EXCLUDED.conversion_factor,
    is_active         = EXCLUDED.is_active,
    notes             = EXCLUDED.notes;

COMMIT;

-- Verification:
-- SELECT hs_code_10, commodity_group, commodity_name, flow_type
-- FROM silver.trade_commodity_reference
-- WHERE commodity_group IN ('SAFFLOWER', 'SAFFLOWER_OIL', 'OILCAKE_OTHER')
-- ORDER BY hs_code_10, flow_type;
