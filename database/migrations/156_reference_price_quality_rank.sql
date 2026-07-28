-- 156_reference_price_quality_rank.sql
--
-- Guidance Report price-feed layer, storage migration 1 of 3 (brief:
-- clients/Contracts/Helios/CLAUDE_CODE_BRIEF_price_feeds.md §"Storage contract").
--
-- The provenance vocabulary for every price the Guidance Report consumes. The brief fixes the
-- ordering and says "extend, don't invent" -- so this is a reference TABLE, not a Postgres ENUM:
-- a new rank is an INSERT (and its ordinal slots between existing ones), never an ALTER TYPE that
-- has to be coordinated with a deploy. It is a table and not a CHECK-list-of-strings for two
-- reasons:
--
--   1. Every report cell "inherits the min rank of its inputs" (brief). "min rank" is a numeric
--      operation -- MIN(ordinal) across the cell's input marks -- so the ordinal has to live
--      somewhere queryable. A bare text CHECK gives you the vocabulary but not the order.
--   2. price_mark.quality_rank and curve_term.quality_rank FK to this table, so the vocabulary is
--      enforced at write time and a typo ('SETTLE_OFICIAL') fails loud instead of silently ranking
--      itself out of every min().
--
-- HIGHER ordinal = BETTER provenance, consistent with the vintage-ladder convention everywhere else
-- in the estate (higher wins; migration 149). To compute the rank a derived cell inherits, take the
-- MIN(ordinal) of its inputs and map back to rank_name. Ordinals are spaced by 10 so future ranks
-- (e.g. an assessed-but-unlicensed tier between DERIVED_PARITY and ASSESSED_LICENSED) drop in
-- without a renumber.
--
-- Ordering rationale (brief §"Storage contract", quality-rank vocabulary):
--   SETTLE_OFFICIAL   an exchange's own official daily settlement (AMS block, CME, Bursa EOD). Best.
--   ASSESSED_LICENSED a paid assessor's mark we are licensed to republish (none yet; can_republish
--                     gates the *rights*, this gates the *quality*). Above gov because an assessed
--                     physical mark is closer to a transactable price than a monthly gov average.
--   OFFICIAL_GOV      a government-published price (Argentina FOB oficial, EU DG AGRI, EPA EMTS).
--   DERIVED_PARITY    our import-parity chain: parent settle + freight + duty + VAT + FX + basis.
--   DERIVED_CARRY     a single mark / short strip extended by cost of carry.
--   PROXY_SPREAD      a statistical spread to a liquid parent where no structural chain exists.
--   NEWS_INDICATIVE   a directional read from wire/news or a delayed/derived quote (yfinance, etc.).
--                     The floor: the existing 2000-> yfinance strips backfill here (brief history
--                     decision, Tore 2026-07-28: keep the depth, flag the provenance).
--
-- NEW objects only; nothing to backfill.

BEGIN;

CREATE TABLE reference.price_quality_rank (
    rank_name    text     PRIMARY KEY,
    rank_ordinal integer  NOT NULL UNIQUE,          -- HIGHER = better provenance; min() picks worst input
    description  text     NOT NULL,
    CONSTRAINT price_quality_rank_ordinal_positive_ck CHECK (rank_ordinal > 0)
);

INSERT INTO reference.price_quality_rank (rank_name, rank_ordinal, description) VALUES
    ('SETTLE_OFFICIAL',   70, 'Exchange official daily settlement (USDA AMS settlement block, CME, Bursa EOD).'),
    ('ASSESSED_LICENSED', 60, 'Paid assessor mark we are licensed to republish. can_republish still gates the right.'),
    ('OFFICIAL_GOV',      50, 'Government-published price (Argentina FOB oficial, EU DG AGRI dashboard, EPA EMTS).'),
    ('DERIVED_PARITY',    40, 'RLC import-parity chain: parent settle + freight + duty/levy + VAT + FX + basis residual.'),
    ('DERIVED_CARRY',     30, 'Single mark or short strip extended by explicit cost of carry.'),
    ('PROXY_SPREAD',      20, 'Statistical spread to a liquid parent where no structural chain exists.'),
    ('NEWS_INDICATIVE',   10, 'Wire/news directional read or delayed/derived quote (e.g. backfilled yfinance/ibkr strips).');

COMMENT ON TABLE reference.price_quality_rank IS
'Provenance vocabulary for the Guidance Report price layer (silver.price_mark, gold.curve_term). '
'HIGHER rank_ordinal = better provenance (higher-wins, per the vintage ladder). A report cell inherits '
'MIN(rank_ordinal) across its input marks. Extend by INSERT (ordinals spaced by 10); never invent a '
'rank name not in this table -- the FK on price_mark/curve_term rejects it. See migration 156 and the '
'Helios price-feeds brief.';
COMMENT ON COLUMN reference.price_quality_rank.rank_ordinal IS
'Sort key for min-rank inheritance. HIGHER = better. Spaced by 10 so new tiers slot in without renumber.';

COMMIT;
