-- =============================================================================
-- Migration 089: BD/RD trade-split rules v2 — calibrated to import unit prices
-- =============================================================================
-- Unit-price analysis of 2024 HS 3826 imports showed virtually ALL major
-- source countries priced at biodiesel levels ($4.29-4.59/gal):
--
--   Canada $4.35, Germany $4.52, Spain $4.57, Italy $4.59, Belgium $4.58,
--   Korea $4.46, Malaysia $4.29, Brazil $4.36, China $4.48
--
-- Industry benchmark RD wholesale is $5-6/gal in 2024 — none of these match.
-- Conclusion: US RD imports (Tidewater/Braya/Neste-Singapore/Neste-Rotterdam)
-- flow primarily under HS 2710.20.x (other refined petroleum products), NOT
-- HS 3826. The HS 3826 channel is dominated by BD.
--
-- This migration replaces the previous country-share rules (which assumed
-- Tidewater/Braya/Singapore Neste RD flowed through HS 3826) with BD-dominant
-- shares across the board. Small RD percentages preserved to capture the
-- minority RD cargoes and SAF-priced shipments observed in the data
-- (France $6-9/gal small lots, Belgium Dec 2024 $12.60/gal SAF cargoes).
--
-- Note: HS 2710.20.x ingestion is a separate follow-up sprint to capture
-- the actual RD import flows that are currently invisible to this view.
-- =============================================================================

DELETE FROM reference.biofuel_trade_split WHERE flow = 'imports';

-- Helper bulk insert. Time-period stratification collapsed — price evidence
-- doesn't support different shares across 2013-2024.
INSERT INTO reference.biofuel_trade_split (hs_code, flow, origin, year_from, year_to, bd_share, rd_share, confidence, source, notes) VALUES

-- Canada (1220): Tidewater + Braya RD flows under HS 2710, not 3826. HS 3826 from Canada is BD.
  ('3826001000','imports','1220',2013,2099, 0.90, 0.10, 'high', 'mig 089 price-calibrated', 'Canadian BD producers. Tidewater/Braya RD via HS 2710'),
  ('3826003000','imports','1220',2013,2099, 0.90, 0.10, 'high', 'mig 089 price-calibrated', NULL),

-- Argentina (3570): Soy methyl ester biodiesel
  ('3826001000','imports','3570',2013,2099, 0.95, 0.05, 'high', 'mig 089 price-calibrated', 'Soy BD'),
  ('3826003000','imports','3570',2013,2099, 0.95, 0.05, 'high', 'mig 089 price-calibrated', NULL),

-- Brazil (3510): Soy / tallow BD
  ('3826001000','imports','3510',2013,2099, 0.95, 0.05, 'high', 'mig 089 price-calibrated', 'Brazilian soy/tallow BD'),
  ('3826003000','imports','3510',2013,2099, 0.95, 0.05, 'high', 'mig 089 price-calibrated', NULL),

-- Indonesia (5600): Palm BD
  ('3826001000','imports','5600',2013,2099, 0.95, 0.05, 'high', 'mig 089 price-calibrated', 'Palm BD'),
  ('3826003000','imports','5600',2013,2099, 0.95, 0.05, 'high', 'mig 089 price-calibrated', NULL),

-- Malaysia (5570): Palm BD
  ('3826001000','imports','5570',2013,2099, 0.90, 0.10, 'high', 'mig 089 price-calibrated', 'Palm BD; small Petronas RD'),
  ('3826003000','imports','5570',2013,2099, 0.90, 0.10, 'high', 'mig 089 price-calibrated', NULL),

-- Spain (4700): UCO BD
  ('3826001000','imports','4700',2013,2099, 0.90, 0.10, 'high', 'mig 089 price-calibrated', 'Spanish UCO BD'),
  ('3826003000','imports','4700',2013,2099, 0.90, 0.10, 'high', 'mig 089 price-calibrated', NULL),

-- Germany (4280): Mixed BD (rapeseed) — price shows BD-dominant
  ('3826001000','imports','4280',2013,2099, 0.90, 0.10, 'high', 'mig 089 price-calibrated', 'German BD; price says no significant RD share'),
  ('3826003000','imports','4280',2013,2099, 0.90, 0.10, 'high', 'mig 089 price-calibrated', NULL),

-- France (4279): Some premium shipments (SAF/RD candidates) at $6-9/gal
  ('3826001000','imports','4279',2013,2099, 0.80, 0.20, 'medium', 'mig 089 price-calibrated', 'French rapeseed BD; some premium shipments suggest minor SAF/RD'),
  ('3826003000','imports','4279',2013,2099, 0.80, 0.20, 'medium', 'mig 089 price-calibrated', NULL),

-- Belgium (4231): Dec 2024 $12.60/gal cargoes flagged as likely SAF
  ('3826001000','imports','4231',2013,2099, 0.85, 0.15, 'medium', 'mig 089 price-calibrated', 'Mostly BD; Dec 2024 $12.60/gal cargoes likely SAF'),
  ('3826003000','imports','4231',2013,2099, 0.85, 0.15, 'medium', 'mig 089 price-calibrated', NULL),

-- Italy (4759): BD (price-confirmed)
  ('3826001000','imports','4759',2013,2099, 0.90, 0.10, 'high', 'mig 089 price-calibrated', 'Italian BD'),
  ('3826003000','imports','4759',2013,2099, 0.90, 0.10, 'high', 'mig 089 price-calibrated', NULL),

-- Korea (5800): UCO BD
  ('3826001000','imports','5800',2013,2099, 0.90, 0.10, 'high', 'mig 089 price-calibrated', 'Korean UCO BD'),
  ('3826003000','imports','5800',2013,2099, 0.90, 0.10, 'high', 'mig 089 price-calibrated', NULL),

-- China (5700): BD
  ('3826001000','imports','5700',2013,2099, 0.95, 0.05, 'high', 'mig 089 price-calibrated', 'Chinese BD'),
  ('3826003000','imports','5700',2013,2099, 0.95, 0.05, 'high', 'mig 089 price-calibrated', NULL),

-- Netherlands (4210): Was 25/75 assuming Neste Rotterdam RD; price evidence
-- doesn't support. Neste Rotterdam RD probably flows under HS 2710 to US (or stays in EU).
  ('3826001000','imports','4210',2013,2099, 0.90, 0.10, 'medium', 'mig 089 price-calibrated', 'BD-dominant per price; Neste Rotterdam RD via HS 2710'),
  ('3826003000','imports','4210',2013,2099, 0.90, 0.10, 'medium', 'mig 089 price-calibrated', NULL),

-- Singapore (5590): Was 5/95 assuming Neste Singapore RD. Same reasoning — Neste RD via HS 2710.
  ('3826001000','imports','5590',2013,2099, 0.85, 0.15, 'medium', 'mig 089 price-calibrated', 'Mostly BD per price; Neste Singapore RD likely via HS 2710'),
  ('3826003000','imports','5590',2013,2099, 0.85, 0.15, 'medium', 'mig 089 price-calibrated', NULL),

-- Hong Kong (5820): UCO BD only
  ('3826001000','imports','5820',2013,2099, 1.00, 0.00, 'high', 'mig 089 price-calibrated', 'ASB UCO BD only'),
  ('3826003000','imports','5820',2013,2099, 1.00, 0.00, 'high', 'mig 089 price-calibrated', NULL),

-- India (5330): UCO BD
  ('3826001000','imports','5330',2013,2099, 1.00, 0.00, 'high', 'mig 089 price-calibrated', 'Universal Biofuels UCO BD'),
  ('3826003000','imports','5330',2013,2099, 1.00, 0.00, 'high', 'mig 089 price-calibrated', NULL),

-- Australia (6021): Just Biodiesel only
  ('3826001000','imports','6021',2013,2099, 1.00, 0.00, 'high', 'mig 089 price-calibrated', 'Just Biodiesel'),
  ('3826003000','imports','6021',2013,2099, 1.00, 0.00, 'high', 'mig 089 price-calibrated', NULL),

-- Default IMPORTS for any origin not explicitly listed
  ('3826001000','imports',NULL,2013,2099, 0.90, 0.10, 'low', 'mig 089 price-calibrated', 'BD-dominant default for unmapped origins'),
  ('3826003000','imports',NULL,2013,2099, 0.90, 0.10, 'low', 'mig 089 price-calibrated', NULL);

-- Exports unchanged from mig 082 (US export 95/5 BD/RD; revisit when HS 2710 data is in).
-- Note: this migration only touched flow='imports'.
