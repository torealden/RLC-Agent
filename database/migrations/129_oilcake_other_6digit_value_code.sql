-- Migration 129: 6-digit reference row for HS 2306.90 (other oilcake)
--
-- Same Census API quirk as peanut shelled exports (mig 124) and 2008.11
-- prepared peanuts: HS 2306900000 returns HTTP 204 at the 10-digit level;
-- the 6-digit 230690 publishes value-only ($ but no kg, qty suppressed).
--
-- HS 2306.90 is the generic "other oilcake / meal" bucket. Safflower meal
-- has no dedicated HS subheading and falls here along with sesame meal,
-- mustard meal, and other minor oilseed meals. The data is a PROXY for
-- safflower meal — value/quantity reflect the whole bucket, not just
-- safflower.
--
-- Storage convention (same as mig 124 for peanut 230690): we store the
-- 6-digit string in the hs_code_10 column. bronze.census_trade.hs_code
-- is TEXT, so mixed-length codes coexist.

BEGIN;

INSERT INTO silver.trade_commodity_reference (
    hs_code_10, hs_code_6,
    commodity_group, commodity_name,
    flow_type, source_unit, display_unit, conversion_factor,
    is_active, notes
) VALUES
    ('230690', '230690', 'OILCAKE_OTHER',
     'Other oilcakes/meals (6-digit value-only)',
     'EXPORTS', 'KG', '000 Pounds', 2.20462,
     TRUE,
     'Census API quirk: 10-digit 2306900000 returns 204; 6-digit publishes value-only (qty suppressed). Generic bucket — includes safflower meal as proxy along with sesame, mustard, and other minor-oilseed meals.'),
    ('230690', '230690', 'OILCAKE_OTHER',
     'Other oilcakes/meals (6-digit value-only)',
     'IMPORTS', 'KG', '000 Pounds', 2.20462,
     TRUE,
     'Census API quirk: 10-digit returns 204; 6-digit publishes value-only (qty suppressed). Generic bucket — includes safflower meal as proxy along with sesame, mustard, and other minor-oilseed meals.')
ON CONFLICT DO NOTHING;

COMMIT;

-- Verification:
-- SELECT hs_code_10, flow_type, commodity_name, notes
-- FROM silver.trade_commodity_reference
-- WHERE commodity_group = 'OILCAKE_OTHER' ORDER BY hs_code_10, flow_type;
