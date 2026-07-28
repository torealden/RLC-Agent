-- 162_placeholder_rank.sql
--
-- Guidance Report price-feed layer: register the PLACEHOLDER quality rank (brief v1.1 addendum §D.1,
-- "register so they are not re-litigated") and exclude it from gold.price_mark_best.
--
-- §D.1 dead-contract guard: a contract whose ingest signature is zero volume AND zero OI AND uniform
-- change across consecutive tenors (proven: FCPO Jan '28+, the whole NCDEX sunflower board) is tagged
-- rank PLACEHOLDER and "excluded from every curve read and chart". This migration does the two
-- reference-layer pieces of that ruling:
--   1. add PLACEHOLDER to reference.price_quality_rank as the new FLOOR (ordinal 5, below
--      NEWS_INDICATIVE 10) -- a dead print is worse than a delayed/news quote, and min-rank inheritance
--      makes any cell touching a PLACEHOLDER input inherit the floor;
--   2. exclude PLACEHOLDER from gold.price_mark_best -- the "best mark per cell" view is a read, and a
--      dead contract must never surface as a cell's best price even when it is the only row present
--      (better to return no row than a stale dead print).
--
-- The ingest-time DETECTION of the dead signature lives in the collector / curve module (§D, later);
-- this migration only lands the vocabulary + the consumer-view exclusion so those are ready and the
-- ruling is recorded.
--
-- Additive/reversible. price_mark.quality_rank already FKs price_quality_rank, so the new value is
-- immediately writable; no existing rows use it yet.

BEGIN;

INSERT INTO reference.price_quality_rank (rank_name, rank_ordinal, description) VALUES
    ('PLACEHOLDER', 5, 'Dead contract (zero vol + zero OI + uniform change across tenors, brief v1.1 §D.1). '
                       'Floor rank; excluded from gold.price_mark_best and from every curve read/chart.');

-- Re-create the best-mark view with the PLACEHOLDER exclusion (column list unchanged from migration 160).
CREATE OR REPLACE VIEW gold.price_mark_best AS
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
WHERE pm.quality_rank <> 'PLACEHOLDER'                     -- dead contracts never surface as a best mark
ORDER BY
    pm.series_key, pm.obs_date, pm.tenor_type, pm.tenor,   -- DISTINCT ON key
    qr.rank_ordinal DESC,                                  -- 1) best measurement quality wins
    COALESCE(ps.tier_ordinal, 9999) ASC,                   -- 2) source hierarchy within equal rank
    pm.collected_at DESC,                                  -- 3) freshest collection
    pm.source ASC;                                         -- 4) stable final determinism

COMMIT;
