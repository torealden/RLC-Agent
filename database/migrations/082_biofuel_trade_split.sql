-- =============================================================================
-- Migration 082: reference.biofuel_trade_split + gold.biofuel_trade_split
-- =============================================================================
-- Heuristic allocation of HS 3826 Census trade between biodiesel (BD) and
-- renewable diesel (RD).
--
-- Architecture: see docs/specs/biodiesel_rd_trade_split.md (approved 2026-05-13).
-- - Country-origin rules below distribute import volumes by producer mix.
-- - Time-period adjustment baked in via separate rows per year_from/year_to.
-- - US export default: 95% BD / 5% RD (spec; Tore expects higher RD on
--   EU-bound exports; validation against EIA monthly will revise).
-- - Confidence levels: high/medium/low.
-- - HS codes: 3826000000 (exports Schedule B), 3826001000 + 3826003000 (imports).
-- - Country codes are Census Schedule B numeric (4-digit), NOT ISO 2-letter.
-- - Regional aggregates (codes like '6XXX', '-') are excluded by the gold view.
-- =============================================================================

CREATE TABLE IF NOT EXISTS reference.biofuel_trade_split (
    rule_id        SERIAL PRIMARY KEY,
    hs_code        TEXT NOT NULL,
    flow           TEXT NOT NULL CHECK (flow IN ('imports', 'exports')),
    origin         TEXT,                          -- Census 4-digit code; NULL = default
    year_from      INT  NOT NULL,
    year_to        INT  NOT NULL,
    bd_share       NUMERIC NOT NULL CHECK (bd_share BETWEEN 0 AND 1),
    rd_share       NUMERIC NOT NULL CHECK (rd_share BETWEEN 0 AND 1),
    confidence     TEXT NOT NULL CHECK (confidence IN ('high','medium','low')),
    source         TEXT,
    notes          TEXT,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    CHECK (bd_share + rd_share = 1.0),
    CHECK (year_from <= year_to)
);

CREATE INDEX IF NOT EXISTS biofuel_trade_split_lookup_idx
    ON reference.biofuel_trade_split (hs_code, flow, origin, year_from, year_to);

-- Clear any prior seed so this migration is idempotent.
DELETE FROM reference.biofuel_trade_split;

-- =============================================================================
-- IMPORTS — country-origin profile per spec
-- =============================================================================
-- Time periods per spec:
--   2013-2017: RD industry was small (~250 Bgy global) → RD share × 0.3
--   2018-2020: base table
--   2021-2023: base table
--   2024+:     RD industry mature (~3000 Bgy) → RD share × 1.2, cap at 1.0
--
-- HS imports: 3826001000 (B100, "biodiesel and mixtures, containing 50% or
-- more biodiesel") + 3826003000 ("other biodiesel and mixtures").
-- Both codes get the same rule per origin.

-- Helper format: (hs, flow, origin, yr_from, yr_to, bd, rd, conf, notes)

-- ---- Singapore (5590): Neste HEFA RD plant ----
INSERT INTO reference.biofuel_trade_split (hs_code, flow, origin, year_from, year_to, bd_share, rd_share, confidence, source, notes) VALUES
  ('3826001000','imports','5590',2013,2017, 0.985, 0.015, 'medium', 'spec table × 0.3 RD weight', 'Neste Singapore HEFA — early period'),
  ('3826001000','imports','5590',2018,2023, 0.05,  0.95,  'high',   'spec table',                  'Neste Singapore ~1.7 Bgy HEFA RD'),
  ('3826001000','imports','5590',2024,2099, 0.00,  1.00,  'high',   'spec × 1.2 RD weight (cap 1.0)','Neste Singapore mature'),
  ('3826003000','imports','5590',2013,2017, 0.985, 0.015, 'medium', 'spec table × 0.3 RD weight', 'Neste Singapore HEFA — early period'),
  ('3826003000','imports','5590',2018,2023, 0.05,  0.95,  'high',   'spec table',                  'Neste Singapore'),
  ('3826003000','imports','5590',2024,2099, 0.00,  1.00,  'high',   'spec × 1.2 RD weight (cap 1.0)','Neste Singapore mature');

-- ---- Netherlands (4210): Neste Rotterdam RD + small BD ----
INSERT INTO reference.biofuel_trade_split (hs_code, flow, origin, year_from, year_to, bd_share, rd_share, confidence, source, notes) VALUES
  ('3826001000','imports','4210',2013,2017, 0.925, 0.075, 'medium', 'spec table × 0.3 RD weight', 'Pre-Rotterdam-expansion'),
  ('3826001000','imports','4210',2018,2023, 0.25,  0.75,  'medium', 'spec table',                  'Neste Rotterdam RD + small BD'),
  ('3826001000','imports','4210',2024,2099, 0.10,  0.90,  'medium', 'spec × 1.2 RD (cap 1.0)',     'Rotterdam mature'),
  ('3826003000','imports','4210',2013,2017, 0.925, 0.075, 'medium', 'spec table × 0.3 RD weight', NULL),
  ('3826003000','imports','4210',2018,2023, 0.25,  0.75,  'medium', 'spec table',                  'Neste Rotterdam'),
  ('3826003000','imports','4210',2024,2099, 0.10,  0.90,  'medium', 'spec × 1.2 RD (cap 1.0)',     NULL);

-- ---- Canada (1220): Tidewater Prince George + Braya Come By Chance RD ----
INSERT INTO reference.biofuel_trade_split (hs_code, flow, origin, year_from, year_to, bd_share, rd_share, confidence, source, notes) VALUES
  ('3826001000','imports','1220',2013,2017, 0.97,  0.03,  'medium', 'spec × 0.3 RD',               'Pre-Tidewater/Braya'),
  ('3826001000','imports','1220',2018,2023, 0.10,  0.90,  'medium', 'spec table',                  'Tidewater+Braya RD dominant'),
  ('3826001000','imports','1220',2024,2099, 0.00,  1.00,  'medium', 'spec × 1.2 RD (cap)',         NULL),
  ('3826003000','imports','1220',2013,2017, 0.97,  0.03,  'medium', 'spec × 0.3 RD',               NULL),
  ('3826003000','imports','1220',2018,2023, 0.10,  0.90,  'medium', 'spec table',                  'Tidewater+Braya'),
  ('3826003000','imports','1220',2024,2099, 0.00,  1.00,  'medium', 'spec × 1.2 RD (cap)',         NULL);

-- ---- Argentina (3570): Renova/Vicentin/Cresta soy-meth-ester ----
INSERT INTO reference.biofuel_trade_split (hs_code, flow, origin, year_from, year_to, bd_share, rd_share, confidence, source, notes) VALUES
  ('3826001000','imports','3570',2013,2099, 0.95,  0.05,  'high',   'spec table',                  'Argentine soy BD'),
  ('3826003000','imports','3570',2013,2099, 0.95,  0.05,  'high',   'spec table',                  'Argentine soy BD');

-- ---- Indonesia (5600): Wilmar/SinarMas palm BD ----
INSERT INTO reference.biofuel_trade_split (hs_code, flow, origin, year_from, year_to, bd_share, rd_share, confidence, source, notes) VALUES
  ('3826001000','imports','5600',2013,2099, 0.95,  0.05,  'high',   'spec table',                  'Indonesian palm BD'),
  ('3826003000','imports','5600',2013,2099, 0.95,  0.05,  'high',   'spec table',                  NULL);

-- ---- Malaysia (5570): mostly palm BD + small Petronas RD ----
INSERT INTO reference.biofuel_trade_split (hs_code, flow, origin, year_from, year_to, bd_share, rd_share, confidence, source, notes) VALUES
  ('3826001000','imports','5570',2013,2099, 0.90,  0.10,  'medium', 'spec table',                  'Palm BD + Petronas RD'),
  ('3826003000','imports','5570',2013,2099, 0.90,  0.10,  'medium', 'spec table',                  NULL);

-- ---- Brazil (3510): BE8/Petrobras soy/tallow BD ----
INSERT INTO reference.biofuel_trade_split (hs_code, flow, origin, year_from, year_to, bd_share, rd_share, confidence, source, notes) VALUES
  ('3826001000','imports','3510',2013,2099, 0.95,  0.05,  'high',   'spec table',                  'Brazilian soy/tallow BD'),
  ('3826003000','imports','3510',2013,2099, 0.95,  0.05,  'high',   'spec table',                  NULL);

-- ---- Spain (4700): Biocom + others UCO BD ----
INSERT INTO reference.biofuel_trade_split (hs_code, flow, origin, year_from, year_to, bd_share, rd_share, confidence, source, notes) VALUES
  ('3826001000','imports','4700',2013,2099, 0.90,  0.10,  'medium', 'spec table',                  'Spanish UCO BD'),
  ('3826003000','imports','4700',2013,2099, 0.90,  0.10,  'medium', 'spec table',                  NULL);

-- ---- Germany (4280): mixed BD/RD producers ----
INSERT INTO reference.biofuel_trade_split (hs_code, flow, origin, year_from, year_to, bd_share, rd_share, confidence, source, notes) VALUES
  ('3826001000','imports','4280',2013,2099, 0.70,  0.30,  'medium', 'spec table',                  'German mixed BD/RD'),
  ('3826003000','imports','4280',2013,2099, 0.70,  0.30,  'medium', 'spec table',                  NULL);

-- ---- France (4279): mostly rapeseed BD ----
INSERT INTO reference.biofuel_trade_split (hs_code, flow, origin, year_from, year_to, bd_share, rd_share, confidence, source, notes) VALUES
  ('3826001000','imports','4279',2013,2099, 0.85,  0.15,  'medium', 'spec table',                  'French rapeseed BD'),
  ('3826003000','imports','4279',2013,2099, 0.85,  0.15,  'medium', 'spec table',                  NULL);

-- ---- Hong Kong (5820): ASB UCO BD ----
INSERT INTO reference.biofuel_trade_split (hs_code, flow, origin, year_from, year_to, bd_share, rd_share, confidence, source, notes) VALUES
  ('3826001000','imports','5820',2013,2099, 1.00,  0.00,  'high',   'spec table',                  'ASB UCO BD only'),
  ('3826003000','imports','5820',2013,2099, 1.00,  0.00,  'high',   'spec table',                  NULL);

-- ---- India (5330): Universal Biofuels UCO BD ----
INSERT INTO reference.biofuel_trade_split (hs_code, flow, origin, year_from, year_to, bd_share, rd_share, confidence, source, notes) VALUES
  ('3826001000','imports','5330',2013,2099, 1.00,  0.00,  'high',   'spec table',                  'Universal Biofuels UCO BD'),
  ('3826003000','imports','5330',2013,2099, 1.00,  0.00,  'high',   'spec table',                  NULL);

-- ---- Australia (6021): Just Biodiesel ----
INSERT INTO reference.biofuel_trade_split (hs_code, flow, origin, year_from, year_to, bd_share, rd_share, confidence, source, notes) VALUES
  ('3826001000','imports','6021',2013,2099, 1.00,  0.00,  'high',   'spec table',                  'Just Biodiesel only'),
  ('3826003000','imports','6021',2013,2099, 1.00,  0.00,  'high',   'spec table',                  NULL);

-- ---- Default IMPORTS (any origin not explicitly listed) ----
INSERT INTO reference.biofuel_trade_split (hs_code, flow, origin, year_from, year_to, bd_share, rd_share, confidence, source, notes) VALUES
  ('3826001000','imports',NULL,2013,2099, 0.90, 0.10, 'low', 'spec default', 'Conservative BD default for unmapped origins'),
  ('3826003000','imports',NULL,2013,2099, 0.90, 0.10, 'low', 'spec default', 'Conservative BD default for unmapped origins');

-- =============================================================================
-- EXPORTS — single Schedule B code 3826000000
-- =============================================================================
-- Spec defaults: 5% RD on US exports. Tore's prior is higher (EU-bound RD
-- from DGD Norco / Port Arthur). Initial rules per spec; revise after
-- validation against EIA monthly BD-exports series.

INSERT INTO reference.biofuel_trade_split (hs_code, flow, origin, year_from, year_to, bd_share, rd_share, confidence, source, notes) VALUES
  ('3826000000','exports',NULL,2013,2099, 0.95, 0.05, 'low', 'spec default', 'US exports default — revisit after EIA BD-exports validation; Tore expects higher RD share');

-- =============================================================================
-- gold.biofuel_trade_split — view emitting BD and RD components
-- =============================================================================
-- Pattern follows gold.corn_oil_trade_split: one source row → two output rows.
-- Joins to the rule table by (hs_code, flow, origin, year between year_from/to).
-- If no specific origin rule matches, falls back to the NULL-origin default rule.

DROP VIEW IF EXISTS gold.biofuel_trade_split;

CREATE VIEW gold.biofuel_trade_split AS
WITH base AS (
    SELECT
        ct.id,
        ct.year,
        ct.month,
        ct.flow,
        ct.hs_code,
        ct.country_code,
        ct.country_name,
        ct.quantity,
        ct.value_usd,
        -- Pick the most-specific rule: prefer origin-match, else NULL-origin default.
        -- Done by ranking matches with origin-match scoring higher.
        (
            SELECT btj.rule_id
            FROM reference.biofuel_trade_split btj
            WHERE btj.hs_code = ct.hs_code
              AND btj.flow    = ct.flow
              AND ct.year BETWEEN btj.year_from AND btj.year_to
              AND (btj.origin = ct.country_code OR btj.origin IS NULL)
            ORDER BY (btj.origin IS NOT NULL) DESC, btj.year_from DESC
            LIMIT 1
        ) AS rule_id
    FROM bronze.census_trade ct
    WHERE ct.hs_code LIKE '3826%'
      AND ct.country_code <> '-'
      AND ct.country_code NOT LIKE '%XXX'        -- exclude regional aggregates
)
SELECT
    b.year,
    b.month,
    make_date(b.year, b.month, 1)  AS period_date,
    b.flow,
    b.hs_code,
    b.country_code,
    b.country_name,
    'BIODIESEL'::TEXT              AS commodity_split,
    b.quantity * COALESCE(r.bd_share, 1.0)              AS quantity_kg,
    b.quantity * COALESCE(r.bd_share, 1.0) * 0.301      AS quantity_gal,   -- ≈ density 7.5 lb/gal (305 gal/MT for FAME-ish)
    b.value_usd * COALESCE(r.bd_share, 1.0)             AS value_usd,
    COALESCE(r.confidence, 'low')  AS confidence,
    COALESCE(r.bd_share, 1.0)      AS share_used
FROM base b
LEFT JOIN reference.biofuel_trade_split r ON r.rule_id = b.rule_id

UNION ALL

SELECT
    b.year,
    b.month,
    make_date(b.year, b.month, 1)  AS period_date,
    b.flow,
    b.hs_code,
    b.country_code,
    b.country_name,
    'RENEWABLE_DIESEL'::TEXT       AS commodity_split,
    b.quantity * COALESCE(r.rd_share, 0.0)              AS quantity_kg,
    b.quantity * COALESCE(r.rd_share, 0.0) * 0.301      AS quantity_gal,
    b.value_usd * COALESCE(r.rd_share, 0.0)             AS value_usd,
    COALESCE(r.confidence, 'low')  AS confidence,
    COALESCE(r.rd_share, 0.0)      AS share_used
FROM base b
LEFT JOIN reference.biofuel_trade_split r ON r.rule_id = b.rule_id
WHERE COALESCE(r.rd_share, 0.0) > 0;

COMMENT ON VIEW gold.biofuel_trade_split IS
'Heuristic split of HS 3826 Census trade between biodiesel and renewable diesel. '
'Each source row emits one BD row and (if rd_share>0) one RD row. '
'Allocation governed by reference.biofuel_trade_split (country profile + time-period rules). '
'quantity_gal uses 0.301 gal/kg ≈ FAME-ish density; refine if needed. '
'See docs/specs/biodiesel_rd_trade_split.md for design.';
