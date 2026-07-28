-- 160_gold_price_mark_best.sql
--
-- Guidance Report price-feed layer: the consumer-facing "best mark per cell" view + the source-
-- preference vocabulary it needs (brief: clients/Contracts/Helios/CLAUDE_CODE_BRIEF_price_feeds.md,
-- "Every report cell inherits the min rank of its inputs").
--
-- WHY NOW: adding the FRED H.10 FX collector (source 'fred_h10') alongside the interim ECB collector
-- ('ecb_ref'/'ecb_ref_xrate') created ~40k (series_key, obs_date, tenor) cells that carry TWO marks at
-- the SAME quality_rank (OFFICIAL_GOV). A consumer reading silver.price_mark directly now gets an
-- ambiguous pair per FX cell and has to invent a tie-break. This view writes that tie-break down ONCE
-- so no consumer re-derives it (and gets it wrong).
--
-- RESOLUTION ORDER (per Tore 2026-07-28, with two amendments below):
--   1. quality_rank.rank_ordinal DESC     -- the KIND of measurement dominates. An exchange settle
--                                            beats a gov survey beats a derived-parity beats news,
--                                            regardless of who published it. This is the brief's axis
--                                            and it must sit on top: otherwise a licensed assessor's
--                                            physical mark (rank 60) would lose to a foreign government
--                                            number it should beat.
--   2. price_source.tier_ordinal ASC      -- Tore's source hierarchy, applied only WITHIN an equal
--                                            quality_rank. Tore's original list: gov-of-series-country
--                                            > gov-of-another > assoc-in-country > assoc-anywhere >
--                                            aggregator. AMENDMENT 1: prepend EXCHANGE_SETTLE (the
--                                            exchange's own settlement of its own contract) as the top
--                                            tier -- it was missing and is the authority for anything
--                                            exchange-traded.
--   3. collected_at DESC, source ASC      -- most-recently-collected, then a stable alphabetical
--                                            final tiebreak so the view is fully deterministic.
--
-- AMENDMENT 2 (documented, not yet enforced): "gov of the series' OWN country vs another country" is a
--   property of the (source, series) pair, not of the source alone -- fred_h10 is series-country-gov
--   for a US series (DCO, WTI) but foreign-gov for USD/MYR (whose home authority is Bank Negara). A
--   full model needs a per-series "home authority" tag. No current data hits that conflict EXCEPT FX
--   pairs, which have two countries and no single home; those are resolved by making FRED the register-
--   designated PRIMARY uniformly (fred_h10 tier < ecb_ref tier), so we don't get FRED-for-USDMYR /
--   ECB-for-EURUSD seams. Add the home-authority tag if a foreign-gov series ever competes with a
--   US-gov mark on the same cell. For now the static source->tier table below is exactly right for all
--   data present.
--
-- The tier table is DATA, not a CASE baked into the view -- same discipline as reference.price_quality_
-- rank (migration 156): re-tiering a source, or adding one, is an INSERT/UPDATE, not a view rebuild.
--
-- NEW objects only; nothing to backfill.

BEGIN;

CREATE TABLE reference.price_source (
    source_name   text     PRIMARY KEY,               -- matches silver.price_mark.source
    source_tier   text     NOT NULL,                  -- taxonomy label (below)
    tier_ordinal  integer  NOT NULL,                  -- LOWER = preferred; tie-break WITHIN equal quality_rank
    description   text     NOT NULL,
    CONSTRAINT price_source_tier_ck CHECK (source_tier IN
        ('EXCHANGE_SETTLE','GOV_SERIES','GOV_OTHER','ASSOC_SERIES','ASSOC_ANY','AGGREGATOR'))
);

-- Tiers (LOWER ordinal = preferred), spaced by 10 for future insertion:
--   10 EXCHANGE_SETTLE  the exchange's own official settlement of its own contract        [amendment 1]
--   20 GOV_SERIES       government of the series' own country                             [Tore #1]
--   30 GOV_OTHER        government of another country                                     [Tore #2]
--   40 ASSOC_SERIES     trade/producer association in the series' country                 [Tore #3]
--   50 ASSOC_ANY        trade association anywhere                                        [Tore #4]
--   60 AGGREGATOR       data aggregator / delayed/derived vendor                          [Tore #5]
INSERT INTO reference.price_source (source_name, source_tier, tier_ordinal, description) VALUES
    ('usda_ams_settle_3192','EXCHANGE_SETTLE',10,'CBOT/KCBT/MGEX official settlement, relayed via USDA AMS grain PDF slug 3192.'),
    ('usda_ams_settle_2850','EXCHANGE_SETTLE',10,'Same exchange settlement block via AMS slug 2850 (identical, fallback source).'),
    ('usda_ams_settle_2771','EXCHANGE_SETTLE',10,'Same exchange settlement block via AMS slug 2771 (identical, fallback source).'),
    ('eia_spot',            'GOV_SERIES',     20,'US EIA official daily crude spot (WTI/Brent) -- US government, US series.'),
    ('usda_ams_3618',       'GOV_SERIES',     20,'USDA AMS weekly Distillers Corn Oil regional cash survey -- US government, US series.'),
    ('fred_h10',            'GOV_SERIES',     20,'US Federal Reserve H.10 daily FX via FRED -- US gov; register-primary FX source (see amendment 2).'),
    ('ecb_ref',            'GOV_OTHER',      30,'ECB euro reference rate (EURUSD) -- EU government; interim FX fallback.'),
    ('ecb_ref_xrate',      'GOV_OTHER',      30,'ECB euro reference rates triangulated to USD pairs -- EU government; interim FX fallback.'),
    ('yfinance',           'AGGREGATOR',     60,'Delayed/derived retail aggregator (Yahoo Finance) -- indicative history only.'),
    ('ibkr_tws',           'AGGREGATOR',     60,'Interactive Brokers TWS delayed feed -- indicative history only.');

COMMENT ON TABLE reference.price_source IS
'Source-preference vocabulary for gold.price_mark_best (Helios price-feeds brief). tier_ordinal is the '
'tie-break used WITHIN an equal quality_rank; LOWER = preferred. Order: EXCHANGE_SETTLE > gov-of-series-'
'country > gov-of-another > assoc-in-country > assoc-anywhere > aggregator. Extend by INSERT. See '
'migration 160.';

-- ---------------------------------------------------------------------------------------------------
-- gold.price_mark_best: exactly one row per (series_key, obs_date, tenor_type, tenor) -- the single
-- best available mark for that cell. Consumers read THIS, not silver.price_mark, when they want "the"
-- price for a series/date/tenor.
-- ---------------------------------------------------------------------------------------------------
CREATE VIEW gold.price_mark_best AS
SELECT DISTINCT ON (pm.series_key, pm.obs_date, pm.tenor_type, pm.tenor)
    pm.series_key,
    pm.obs_date,
    pm.tenor_type,
    pm.tenor,
    pm.value,
    pm.unit,
    pm.currency,
    pm.source,
    pm.quality_rank,
    qr.rank_ordinal,
    pm.can_republish,
    COALESCE(ps.source_tier, 'UNRANKED') AS source_tier,
    pm.collected_at
FROM silver.price_mark pm
JOIN reference.price_quality_rank qr ON qr.rank_name = pm.quality_rank
LEFT JOIN reference.price_source     ps ON ps.source_name = pm.source
ORDER BY
    pm.series_key, pm.obs_date, pm.tenor_type, pm.tenor,   -- DISTINCT ON key
    qr.rank_ordinal DESC,                                  -- 1) best measurement quality wins
    COALESCE(ps.tier_ordinal, 9999) ASC,                   -- 2) source hierarchy within equal rank; unknown source = worst
    pm.collected_at DESC,                                  -- 3) freshest collection
    pm.source ASC;                                         -- 4) stable final determinism

COMMENT ON VIEW gold.price_mark_best IS
'The single best mark per (series_key, obs_date, tenor_type, tenor): DISTINCT ON that cell ordered by '
'quality_rank ordinal DESC, then reference.price_source.tier_ordinal ASC (source hierarchy within equal '
'rank; unknown source ranks worst but is NOT dropped), then collected_at DESC, then source. Consumers '
'read this instead of silver.price_mark to get one unambiguous price. See migration 160.';

COMMIT;
