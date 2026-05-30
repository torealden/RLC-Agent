-- Migration 124: Add 6-digit peanut HS codes for value-only Census series
--
-- The Census API publishes some peanut trade categories ONLY at the
-- 6-digit aggregate level, with value-in-USD but quantity suppressed.
-- This affects two MAJOR flows that the food balance sheet needs:
--   - 1202.42 shelled exports — $394M/yr (the BIG one, main US export)
--   - 2008.11 prepared exports — $257M/yr (peanut butter + other prep)
--
-- Plus two minor flows we'd otherwise miss:
--   - 1202.41 imports — $319K/yr (boutique trickle)
--   - 1202.30 imports — $45K/yr (essentially zero)
--   - 1202.42 imports — $3.2M/yr
--
-- Tore's decision (2026-05-30): derive implied quantity from value x
-- reference $/kg, with a `derived` flag in the food balance output. The
-- $/kg reference is built from years/codes where we have BOTH qty and
-- value, scaled to the value-only series.
--
-- Storage convention: we store the 6-digit string ('120242' etc.) in the
-- hs_code_10 column. Bronze.census_trade.hs_code is TEXT so mixed lengths
-- are fine; downstream code should not assume hs_code is always 10 chars.

BEGIN;

INSERT INTO silver.trade_commodity_reference (
    hs_code_10, hs_code_6,
    commodity_group, commodity_name,
    flow_type, source_unit, display_unit, conversion_factor,
    is_active, notes
) VALUES
    -- 1202.42 6-digit (value-only at this level, qty suppressed)
    ('120242', '120242', 'PEANUTS', 'Peanuts, shelled (raw) — 6-digit value only',
     'EXPORTS', 'KG', '000 Pounds', 2.20462,
     TRUE, 'Census API quirk: 10-digit returns 204; 6-digit publishes $394M/yr value with qty suppressed. The main US peanut export channel. Quantity is derived in silver.peanut_unit_price * value.'),
    ('120242', '120242', 'PEANUTS', 'Peanuts, shelled (raw) — 6-digit value only',
     'IMPORTS', 'KG', '000 Pounds', 2.20462,
     TRUE, '~$3.2M/yr imports. Qty suppressed.'),

    -- 1202.41 6-digit imports (very small)
    ('120241', '120241', 'PEANUTS', 'Peanuts in shell — 6-digit value only',
     'IMPORTS', 'KG', '000 Pounds', 2.20462,
     TRUE, '~$319K/yr imports (boutique trickle). Qty suppressed.'),

    -- 1202.30 6-digit imports (effectively zero)
    ('120230', '120230', 'PEANUTS', 'Peanuts, seed — 6-digit value only',
     'IMPORTS', 'KG', '000 Pounds', 2.20462,
     TRUE, '~$45K/yr imports (essentially zero). Qty suppressed.'),

    -- 2008.11 6-digit exports (value-only $257M/yr)
    ('200811', '200811', 'PEANUTS', 'Peanuts, prepared/preserved — 6-digit value only',
     'EXPORTS', 'KG', '000 Pounds', 2.20462,
     TRUE, '$257M/yr value-only exports. All 10-digit sub-codes (2008.11.05, 2008.11.15, etc.) return 204 for exports. Qty derived from PB-import unit value.')

ON CONFLICT DO NOTHING;

COMMIT;

-- Verification:
-- SELECT hs_code_10, flow_type, commodity_name, notes
-- FROM silver.trade_commodity_reference
-- WHERE commodity_group = 'PEANUTS' AND LENGTH(hs_code_10) = 6
-- ORDER BY hs_code_10, flow_type;
