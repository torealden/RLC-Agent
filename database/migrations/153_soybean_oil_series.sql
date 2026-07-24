-- 153_soybean_oil_series.sql
--
-- Forecast layer BUILD (ledger 6c; design forecast_layer_design_v1.md D1/D4/D5).
--
-- Creates silver.soybean_oil_series -- the SECOND book-(b) home (after silver.wheat_series, the pilot).
-- Until this migration, only wheat_series existed, so no soybean_oil forecast had anywhere to live
-- (design §1 scope note; handoff 2026-07-24 §5 "cheapest first check"). This table is the prerequisite
-- for the first D5 forecast callable (the biofuel-feedstock-use forecast that closes the ~17-month
-- soyoil biofuel gap).
--
-- Schema and constraints MIRROR silver.wheat_series (migration 151) EXACTLY, so the whole family shares
-- one shape:
--   value_low / value_high : the forecast interval (D4). Stored raw LB, same unit as `value`.
--   run_id                 : one-hop provenance to core.forecast_run (row -> run -> assumptions, D5).
--                            NULL for actuals.
--
-- The CHECK is the D4 hard band gate (Tore 2026-07-23: fail loud, never silent). A forecast-band row
-- (rank 1..9) CANNOT be inserted without a band that brackets the point:
--     rank 1..9  =>  value, value_low, value_high all present AND value_low <= value <= value_high
-- Equality is allowed (a deliberately tight/degenerate band is expressible); a NULL band on a forecast
-- never is. Actuals (rank >= 10) leave the band columns NULL and are unconstrained.
--
-- CONVENTION NOTE (differs from wheat_series, matches the US oils flat file it feeds): the oils
-- supply/demand flat files store the CALENDAR YEAR in `marketing_year` and the calendar month in
-- `period` ('M05' = May), and write_oils_supply_flat_files.py::write_wide re-buckets to the Oct-Sep oil
-- marketing year at render time. soybean_oil forecast rows follow that same convention so the writer can
-- merge them straight through. (period_type = 'cal_month'.)
--
-- New table; nothing to backfill. The forecast callable / runner that writes it is 6c+ code, so the
-- CHECK validates against an empty table on apply.

CREATE TABLE silver.soybean_oil_series (
    commodity      text        NOT NULL,          -- 'soybean_oil'
    class          text        NOT NULL,          -- 'ALL'
    series         text        NOT NULL,          -- e.g. 'biofuel_use_biodiesel', 'biofuel_use_total'
    marketing_year integer     NOT NULL,          -- CALENDAR year (oils convention; see header note)
    period_type    text        NOT NULL,          -- 'cal_month'
    period         text        NOT NULL,          -- 'M01'..'M12'
    vintage        text        NOT NULL,          -- e.g. 'MODEL_BASE'
    vintage_rank   integer     NOT NULL,          -- 1..9 forecast band; >=10 actuals
    value          numeric,                       -- raw LB
    unit           text,                          -- 'LB'
    source         text,                          -- e.g. 'biofuel_feedstock_use_forecast'
    release_date   date,
    revision       integer,
    loaded_at      timestamptz NOT NULL DEFAULT now(),
    value_low      numeric,                       -- forecast band lower bound (raw LB); required rank 1..9
    value_high     numeric,                       -- forecast band upper bound (raw LB); required rank 1..9
    run_id         uuid REFERENCES core.forecast_run(run_id),
    CONSTRAINT soybean_oil_series_forecast_band_ck CHECK (
        vintage_rank > 9
        OR (
            value      IS NOT NULL AND
            value_low  IS NOT NULL AND
            value_high IS NOT NULL AND
            value_low <= value AND value <= value_high
        )
    )
);

-- Dedup key mirrors the flat-file writer's DISTINCT ON: within one key, vintage_rank is unique
-- (design D2). Not a PK because a revision reuses (key, rank) with a newer release_date.
CREATE INDEX soybean_oil_series_key_idx
    ON silver.soybean_oil_series (commodity, class, series, marketing_year, period, vintage_rank);
CREATE INDEX soybean_oil_series_run_idx
    ON silver.soybean_oil_series (run_id) WHERE run_id IS NOT NULL;

COMMENT ON TABLE silver.soybean_oil_series IS
'Book-(b) mechanical forecast home for soybean_oil (design forecast_layer_design_v1.md D1). Mirrors '
'silver.wheat_series (mig 151). Forecast rows sit at vintage_rank 1..9 with a mandatory bracketing band '
'(CHECK soybean_oil_series_forecast_band_ck, D4); actuals at rank >=10 are unconstrained. marketing_year '
'holds the CALENDAR year (US oils flat-file convention; write_wide re-buckets to Oct-Sep MY at render). '
'run_id links a published row to its core.forecast_run invocation and assumptions (D5). Never holds '
'book-(a) LLM forecasts -- those live in core.forecasts.';
COMMENT ON COLUMN silver.soybean_oil_series.value_low IS
'Forecast-band lower bound (raw LB). Required (with value_high) for rank 1..9 forecasts; NULL for '
'actuals. Band semantics: a scenario/subjective interval -- see _meta of the flat file, not a p-value.';
COMMENT ON COLUMN silver.soybean_oil_series.run_id IS
'FK to core.forecast_run: one-hop provenance from a published forecast row to its invocation and '
'assumptions (design D5). NULL for actuals.';
COMMENT ON CONSTRAINT soybean_oil_series_forecast_band_ck ON silver.soybean_oil_series IS
'D4 hard band gate (Tore 2026-07-23): a forecast-band row (rank 1..9) must carry value + a band that '
'brackets it; equality permitted, NULL never. Actuals (rank >= 10) unconstrained.';
