-- 157_silver_price_layer.sql
--
-- Guidance Report price-feed layer, storage migration 2 of 3 (brief:
-- clients/Contracts/Helios/CLAUDE_CODE_BRIEF_price_feeds.md §"Storage contract").
--
-- Two tables:
--
--   silver.price_mark      -- the canonical per-observation price table the report consumes. One row
--                             per (series_key, obs_date, tenor_type, tenor, source). Every price the
--                             report shows -- spot, a named futures contract, a delivery window --
--                             is a row here with provenance (source, quality_rank) and a redistribution
--                             flag (can_republish).
--
--   silver.curve_snapshot  -- full forward strips as pulled: one row per (series_key, obs_date,
--                             contract, source), carrying settle + volume + open_interest. This is
--                             what the thin-OI curve guard reads (refuse a tenor where OI < threshold)
--                             and it closes the standing request to store the FCPO strip history so the
--                             Oct/Mar spread becomes a chart exhibit instead of a one-off pull.
--
-- KEY DESIGN CHOICES
--
-- Natural composite PKs, not surrogate ids. Collectors re-run daily and must be idempotent: a re-pull
-- of the same (series, date, tenor, source) is an UPSERT (ON CONFLICT ... DO UPDATE), not a duplicate.
-- The natural key IS the idempotency key. Same discipline the vintage tables already use.
--
-- tenor is NEVER NULL. A spot mark stores tenor_type='SPOT', tenor='SPOT' -- a literal, not a NULL --
-- so the PK stays clean and "all tenors for a series on a date" is one predicate. tenor_type is a
-- fixed vocabulary (SPOT | CONTRACT | WINDOW) enforced by CHECK; tenor is the free-form label within
-- it ('SPOT', 'ZC_Z26', '2027-03').
--
-- quality_rank FKs to reference.price_quality_rank (migration 156): the vocabulary is enforced at
-- write time. can_republish defaults FALSE -- the §9 rights flag. Assessed/paid values and the
-- backfilled delayed strips ingest as FALSE; only official public-domain data and our own derived
-- math (our terms, our numbers) flip to TRUE, and that is a per-collector decision logged in the
-- collector docstring, never a default.
--
-- curve_snapshot carries source + quality_rank too, but NOT can_republish: a raw strip snapshot is an
-- input to curves, not itself a published cell. What publishes is the price_mark rows and the
-- gold.curve_term stack derived from it.
--
-- NEW tables (verified: neither exists). Backfill of the existing yfinance/ibkr futures strips into
-- both tables at NEWS_INDICATIVE / can_republish=FALSE is a SEPARATE, reviewable data migration
-- (Tore 2026-07-28: keep the 2000-> history, flag the provenance) -- it is not folded into this DDL
-- so the schema review is clean of data movement.

BEGIN;

CREATE TABLE silver.price_mark (
    series_key     text        NOT NULL,                 -- e.g. 'ZC', 'FCPO', 'BR_SBO_PARITY', 'FX_USDMYR'
    obs_date       date        NOT NULL,
    tenor_type     text        NOT NULL,                 -- SPOT | CONTRACT | WINDOW
    tenor          text        NOT NULL,                 -- 'SPOT' | 'ZC_Z26' | '2027-03'
    value          numeric     NOT NULL,
    unit           text        NOT NULL,                 -- e.g. 'USD/bu', 'MYR/t', 'cents/lb'
    currency       text        NOT NULL,                 -- ISO 4217, e.g. 'USD', 'MYR', 'CNY'
    source         text        NOT NULL,                 -- collector-level provenance, e.g. 'usda_ams_3192_settle'
    quality_rank   text        NOT NULL REFERENCES reference.price_quality_rank(rank_name),
    can_republish  boolean     NOT NULL DEFAULT false,   -- §9 redistribution right; FALSE until licensing says otherwise
    collected_at   timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT price_mark_pk PRIMARY KEY (series_key, obs_date, tenor_type, tenor, source),
    CONSTRAINT price_mark_tenor_type_ck CHECK (tenor_type IN ('SPOT','CONTRACT','WINDOW')),
    CONSTRAINT price_mark_spot_tenor_ck CHECK (tenor_type <> 'SPOT' OR tenor = 'SPOT')
);

CREATE INDEX price_mark_series_date_idx ON silver.price_mark (series_key, obs_date DESC);
CREATE INDEX price_mark_republishable_idx ON silver.price_mark (series_key, obs_date DESC) WHERE can_republish;

COMMENT ON TABLE silver.price_mark IS
'Canonical per-observation price table the Guidance Report consumes (Helios price-feeds brief). One row '
'per (series_key, obs_date, tenor_type, tenor, source); natural PK so daily re-pulls UPSERT. tenor is '
'never NULL -- spot uses tenor=''SPOT''. quality_rank FKs reference.price_quality_rank; can_republish '
'is the §9 rights flag (default FALSE). See migration 157.';
COMMENT ON COLUMN silver.price_mark.can_republish IS
'§9 redistribution right. FALSE default. TRUE only for public-domain official data or RLC-derived math; '
'never for an assessed/paid mark or a screen-transited value. Per-collector decision, logged in the '
'collector docstring.';
COMMENT ON COLUMN silver.price_mark.tenor IS
'Tenor label within tenor_type. SPOT->''SPOT''; CONTRACT->exchange contract code (''ZC_Z26''); '
'WINDOW->delivery month (''2027-03'').';

CREATE TABLE silver.curve_snapshot (
    series_key     text        NOT NULL,                 -- the strip's series, e.g. 'FCPO', 'ZC'
    obs_date       date        NOT NULL,
    contract       text        NOT NULL,                 -- exchange contract code for the strip leg
    settle         numeric,                              -- nullable: a leg can quote OI/vol with no trade
    volume         bigint,
    open_interest  bigint,
    unit           text        NOT NULL,
    currency       text        NOT NULL,
    source         text        NOT NULL,
    quality_rank   text        NOT NULL REFERENCES reference.price_quality_rank(rank_name),
    collected_at   timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT curve_snapshot_pk PRIMARY KEY (series_key, obs_date, contract, source)
);

CREATE INDEX curve_snapshot_series_date_idx ON silver.curve_snapshot (series_key, obs_date DESC);

COMMENT ON TABLE silver.curve_snapshot IS
'Full forward strips as pulled: one row per (series_key, obs_date, contract, source) with settle + '
'volume + open_interest. Feeds the curve module''s EXCHANGE_STRIP method and the thin-OI refusal guard '
'(OI < threshold -> hand tenor to carry/parity). Stores FCPO strip history for the Oct/Mar spread '
'exhibit. See migration 157.';
COMMENT ON COLUMN silver.curve_snapshot.open_interest IS
'Open interest for the contract leg. The thin-curve guard refuses EXCHANGE_STRIP reads where this is '
'below the per-series threshold (the canola Nov-27 rule).';

COMMIT;
