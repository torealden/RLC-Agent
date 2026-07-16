-- 144_reference_nonbio_enduse_shares.sql
-- Single source of truth for splitting each feedstock's NON-BIOFUEL demand lump into
-- end-use component lines on the balance sheets. Shared by Claude Code (flat-file writers)
-- and Claude Desktop (workbooks) so both use the SAME weights.
--
-- Provenance: USDA/Census "Fats & Oils: consumption by end-use" blocks, mirrored in the
-- Census Crush tab of us_oilseed_crush.xlsm. NASS discontinued the end-use survey after 2011,
-- so shares are computed over the 2006-2011 window and HELD FORWARD ("the portioning
-- survives over time", Q4 ruling; docs/reference/non_biofuel_disposition_shares.md).
--
-- Application: each commodity's existing non_biofuel_use TOTAL (disappearance/residual, as
-- the writers already derive it) is split by these shares. Components sum to the total by
-- construction, so every sheet closes exactly as before. "Methyl esters" = biodiesel and is
-- EXCLUDED here (it is the biofuel slice, handled by the allocator/rake).
--
-- measured=TRUE  -> the split is a real 2006-2011 Census end-use measurement.
-- measured=FALSE -> analog/assumption (flag as assumption on the sheet, not as measured).

CREATE TABLE IF NOT EXISTS reference.nonbio_enduse_shares (
    commodity       varchar(32) NOT NULL,   -- writer commodity key (soybean_oil, tallow, ...)
    end_use         varchar(48) NOT NULL,   -- component slug (salad_cooking_oil, feed, ...)
    share_pct       numeric      NOT NULL,   -- fraction of the non-biofuel total (sums to 1.0)
    measured        boolean      NOT NULL DEFAULT false,
    basis           text,
    effective_from  date         NOT NULL DEFAULT '2006-01-01',
    effective_to    date,
    PRIMARY KEY (commodity, end_use, effective_from)
);

DELETE FROM reference.nonbio_enduse_shares;  -- idempotent reseed

INSERT INTO reference.nonbio_enduse_shares (commodity, end_use, share_pct, measured, basis) VALUES
-- ---- Soybean oil: measured 2006-2011 Census end-use (~95% edible / ~5% industrial) ----
('soybean_oil', 'salad_cooking_oil',  0.642, true,  'Census end-use 2006-2011'),
('soybean_oil', 'baking_frying_fats', 0.307, true,  'Census end-use 2006-2011'),
('soybean_oil', 'margarine',          0.027, true,  'Census end-use 2006-2011'),
('soybean_oil', 'other_inedible',     0.015, true,  'Census end-use 2006-2011'),
('soybean_oil', 'resins_plastics',    0.004, true,  'Census end-use 2006-2011'),
('soybean_oil', 'other_edible',       0.003, true,  'Census end-use 2006-2011'),
('soybean_oil', 'paint_varnish',      0.002, true,  'Census end-use 2006-2011'),
-- ---- Canola oil: ANALOG of soybean-oil EDIBLE shares (3 cats renormalized to 1.0) ----
('canola_oil', 'salad_cooking_oil',  0.6578, false, 'Analog: soybean-oil edible shares renormalized'),
('canola_oil', 'baking_frying_fats', 0.3145, false, 'Analog: soybean-oil edible shares renormalized'),
('canola_oil', 'margarine',          0.0277, false, 'Analog: soybean-oil edible shares renormalized'),
-- ---- Inedible tallow & grease: measured, feed-dominated (73.4/17.4/9.3 renorm to 1.0) ----
('tallow', 'feed',           0.733, true, 'Census inedible T&G end-use 2006-2011'),
('tallow', 'fatty_acids',    0.174, true, 'Census inedible T&G end-use 2006-2011'),
('tallow', 'other_inedible', 0.093, true, 'Census inedible T&G end-use 2006-2011'),
-- ---- Poultry fat / white grease / yellow grease: ANALOG of tallow & grease pool ----
('poultry_fat', 'feed',           0.733, false, 'Analog: rendered inedible fats pool (tallow split)'),
('poultry_fat', 'fatty_acids',    0.174, false, 'Analog: rendered inedible fats pool (tallow split)'),
('poultry_fat', 'other_inedible', 0.093, false, 'Analog: rendered inedible fats pool (tallow split)'),
('white_grease', 'feed',           0.733, false, 'Analog: rendered inedible fats pool (tallow split)'),
('white_grease', 'fatty_acids',    0.174, false, 'Analog: rendered inedible fats pool (tallow split)'),
('white_grease', 'other_inedible', 0.093, false, 'Analog: rendered inedible fats pool (tallow split)'),
('yellow_grease', 'feed',           0.733, false, 'Analog: rendered inedible fats pool (tallow split)'),
('yellow_grease', 'fatty_acids',    0.174, false, 'Analog: rendered inedible fats pool (tallow split)'),
('yellow_grease', 'other_inedible', 0.093, false, 'Analog: rendered inedible fats pool (tallow split)'),
-- ---- DCO: ~all biofuel; small non-bio residual = animal feed ----
('dco', 'feed', 1.0, false, 'Analog: DCO non-bio residual is animal feed (no edible split)'),
-- ---- UCO/YG combined: post-consumer; residual non-bio = oleochemical/feed ----
('uco_yg', 'oleochemical_feed', 1.0, false, 'Analog: UCO/YG non-bio residual is oleochemical/feed'),
-- ---- Cottonseed oil: measured, essentially all edible (81/17/1 renorm to 1.0); no CSO flat file yet ----
('cottonseed_oil', 'salad_cooking_oil',  0.818, true, 'Census end-use 2006-2011 (renormalized)'),
('cottonseed_oil', 'baking_frying_fats', 0.172, true, 'Census end-use 2006-2011 (renormalized)'),
('cottonseed_oil', 'other_edible',       0.010, true, 'Census end-use 2006-2011 (renormalized)');

COMMENT ON TABLE reference.nonbio_enduse_shares IS
'Fixed 2006-2011 Census end-use shares for splitting each feedstock non-biofuel demand total '
'into component lines on the balance sheets. Held forward (survey discontinued 2011). '
'measured=TRUE is a real Census split; FALSE is an analog/assumption. Consumed by '
'scripts/write_oils_supply_flat_files.py and scripts/write_fats_supply_flat_files.py. '
'Seed source: docs/reference/non_biofuel_disposition_shares.md.';
