-- 159_backfill_delayed_price_history.sql
--
-- Guidance Report price-feed layer: backfill the existing delayed/derived futures history into the
-- new price layer (Tore ruling 2026-07-28: keep the 2000-> depth, flag the provenance).
--
-- SOURCE: bronze.futures_daily_settlement -- 13 symbols from yfinance (2000->) + FCPO from ibkr_tws
-- (2024->). This is delayed/derived data, NOT official settlement, so it lands at NEWS_INDICATIVE
-- with can_republish=FALSE. The official AMS settles (migration + collector, source usda_ams_settle_*)
-- already overlay from 2026-07-28 at SETTLE_OFFICIAL and coexist as separate rows (the PK includes
-- source), so a consumer that takes MAX(rank_ordinal) always gets the official mark where both exist.
--
-- WHY A NEW tenor_type 'NEARBY' (this is the key modeling decision, flagged for review):
--   The 25-year DEPTH lives entirely in the continuous front-month rows (contract_month = 'FRONT'):
--   ~6,500 rows/symbol back to 2000. The dated contracts (U26, Z26, ...) exist only for the recent
--   ~2024+ window (a few hundred rows/symbol). Verified 2026-07-28: e.g. CL has 6,511 FRONT rows and
--   605 dated. So dropping FRONT would discard exactly the history the ruling asks us to keep.
--   A rolled continuous front-month is neither SPOT, a fixed CONTRACT, nor a delivery WINDOW -- the
--   three types migration 157 anticipated. It is a NEARBY series. Rather than mis-file it as a fake
--   contract, this migration extends the tenor_type vocabulary with 'NEARBY' and stores FRONT as
--   (tenor_type='NEARBY', tenor='M1'). Extensible to M2.. if a deferred-nearby series ever appears.
--   The continuous series is UNADJUSTED (carries roll discontinuities) -- fine for a NEWS_INDICATIVE
--   history used for charts/fan-chart calibration, documented here so nobody reads it as a clean panel.
--
-- WHAT LANDS:
--   * FRONT           -> silver.price_mark, tenor_type NEARBY, tenor 'M1'      (the deep history)
--   * dated contracts -> silver.price_mark, tenor_type CONTRACT, tenor '<key>_<Mon>' (recent strip)
--   * dated contracts -> silver.curve_snapshot (strip form; volume kept, OI is NULL in the source)
--   All at NEWS_INDICATIVE / can_republish=FALSE. Negative settles (CL 2020-04-20 = -37.63) are REAL
--   and deliberately not filtered; only NULL settles are skipped (price_mark.value is NOT NULL).
--
-- REVERSIBLE: DELETE FROM silver.price_mark / silver.curve_snapshot WHERE source IN ('yfinance','ibkr_tws').
-- Idempotent: ON CONFLICT DO NOTHING, so a re-run adds only genuinely new (symbol,date,contract) rows.

BEGIN;

-- 1) Extend the tenor_type vocabulary with NEARBY (continuous nearby series).
ALTER TABLE silver.price_mark DROP CONSTRAINT price_mark_tenor_type_ck;
ALTER TABLE silver.price_mark ADD CONSTRAINT price_mark_tenor_type_ck
    CHECK (tenor_type IN ('SPOT','CONTRACT','WINDOW','NEARBY'));

COMMENT ON COLUMN silver.price_mark.tenor IS
'Tenor label within tenor_type. SPOT->''SPOT''; CONTRACT->exchange contract code (''ZC_Z26''); '
'WINDOW->delivery month (''2027-03''); NEARBY->nearby ordinal (''M1'' = continuous front-month).';

-- 2) Symbol -> (series_key, unit, currency) mapping. series_key = symbol, consistent with the AMS
--    settlement collector (ZC/ZW/KE/ZS/... already match). Units verified against 2026-07-27 levels.
WITH sym(symbol, series_key, unit, currency) AS (VALUES
    ('ZC','ZC','cents/bu','USD'),
    ('ZW','ZW','cents/bu','USD'),
    ('KE','KE','cents/bu','USD'),
    ('ZS','ZS','cents/bu','USD'),
    ('ZL','ZL','cents/lb','USD'),
    ('ZM','ZM','USD/short ton','USD'),
    ('ZR','ZR','USD/cwt','USD'),          -- rough rice
    ('CL','CL','USD/bbl','USD'),
    ('HO','HO','USD/gal','USD'),
    ('RB','RB','USD/gal','USD'),
    ('NG','NG','USD/MMBtu','USD'),
    ('DC','DC','USD/cwt','USD'),          -- CME Class III milk
    ('FCPO','FCPO','MYR/t','MYR')
),
-- 3a) Deep history: continuous front-month -> NEARBY / M1.
ins_nearby AS (
    INSERT INTO silver.price_mark
        (series_key, obs_date, tenor_type, tenor, value, unit, currency, source, quality_rank, can_republish)
    SELECT s.series_key, f.trade_date, 'NEARBY', 'M1', f.settlement, s.unit, s.currency,
           f.source, 'NEWS_INDICATIVE', FALSE
    FROM bronze.futures_daily_settlement f
    JOIN sym s ON s.symbol = f.symbol
    WHERE f.contract_month = 'FRONT' AND f.settlement IS NOT NULL
    ON CONFLICT (series_key, obs_date, tenor_type, tenor, source) DO NOTHING
    RETURNING 1
),
-- 3b) Recent strip: dated contracts -> CONTRACT.
ins_contract AS (
    INSERT INTO silver.price_mark
        (series_key, obs_date, tenor_type, tenor, value, unit, currency, source, quality_rank, can_republish)
    SELECT s.series_key, f.trade_date, 'CONTRACT', s.series_key || '_' || f.contract_month,
           f.settlement, s.unit, s.currency, f.source, 'NEWS_INDICATIVE', FALSE
    FROM bronze.futures_daily_settlement f
    JOIN sym s ON s.symbol = f.symbol
    WHERE f.contract_month <> 'FRONT' AND f.settlement IS NOT NULL
    ON CONFLICT (series_key, obs_date, tenor_type, tenor, source) DO NOTHING
    RETURNING 1
),
-- 3c) Strip snapshots (volume kept; OI is NULL in the delayed source).
ins_snapshot AS (
    INSERT INTO silver.curve_snapshot
        (series_key, obs_date, contract, settle, volume, open_interest, unit, currency, source, quality_rank)
    SELECT s.series_key, f.trade_date, s.series_key || '_' || f.contract_month,
           f.settlement, f.total_volume, f.open_interest, s.unit, s.currency, f.source, 'NEWS_INDICATIVE'
    FROM bronze.futures_daily_settlement f
    JOIN sym s ON s.symbol = f.symbol
    WHERE f.contract_month <> 'FRONT' AND f.settlement IS NOT NULL
    ON CONFLICT (series_key, obs_date, contract, source) DO NOTHING
    RETURNING 1
)
SELECT
    (SELECT count(*) FROM ins_nearby)   AS nearby_rows,
    (SELECT count(*) FROM ins_contract) AS contract_rows,
    (SELECT count(*) FROM ins_snapshot) AS snapshot_rows;

COMMIT;
