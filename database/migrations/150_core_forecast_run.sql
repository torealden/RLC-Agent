-- 150_core_forecast_run.sql
--
-- Forecast layer BUILD (ledger 6b; design forecast_layer_design_v1.md D5).
--
-- Records every mechanical-forecast invocation. A forecast callable is a PURE function of
-- (data, assumptions) -- same inputs -> byte-identical output, forever (D5) -- and this table is
-- the provenance ledger for those invocations. One row per run, whether or not the run is
-- published.
--
-- The `retain` column is THE publish gate:
--   retain = true   -> the run's output rows are written into silver.<commodity>_series and become
--                      the house forecast (book b). They feed the flat files and the balance sheets.
--   retain = false  -> scenario / what-if / preview. Logged here for provenance, but the numbers
--                      NEVER enter the book or a flat file. Tore can run ten what-ifs without
--                      polluting the forecast of record.
--
-- `assumptions jsonb` captures the full assumption set (yields, mix, price deck, ...) so any
-- published forecast row traces back to the exact assumptions that produced it -- free to capture
-- now, expensive to reconstruct later. silver.<commodity>_series rows carry run_id (migration 151)
-- for one-hop row -> run -> assumptions provenance.
--
-- This is a NEW table (verified: core.forecast_run did not exist before this migration). No data
-- migration, nothing to backfill -- the forecast callables that write it are 6b+ code.

CREATE TABLE core.forecast_run (
    run_id            uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    callable          text        NOT NULL,               -- e.g. 'implied_feedstock_value'; joins the src/kg/callables family
    callable_version  text,                               -- version tag of the callable at run time (repro anchor)
    assumptions       jsonb       NOT NULL DEFAULT '{}'::jsonb,  -- full assumption set behind the run
    input_snapshot_ref text,                              -- pointer to the input data snapshot (path / hash / query id)
    produced_vintage  text,                               -- vintage the run emits, e.g. 'MODEL_BASE' / 'MODEL'
    produced_rank     integer,                            -- forecast-band rank the run emits (1..9)
    target_keys       jsonb,                              -- the (commodity,class,series,MY,period) keys the run wrote/would write
    retain            boolean     NOT NULL DEFAULT false,  -- PUBLISH GATE: true => rows enter the book; false => scenario only
    created_at        timestamptz NOT NULL DEFAULT now(),  -- invocation timestamp (provenance metadata, not a forecast input)
    CONSTRAINT forecast_run_produced_rank_band_ck
        CHECK (produced_rank IS NULL OR produced_rank BETWEEN 1 AND 9)
);

CREATE INDEX forecast_run_callable_idx ON core.forecast_run (callable, created_at DESC);
CREATE INDEX forecast_run_retain_idx   ON core.forecast_run (retain) WHERE retain;

COMMENT ON TABLE core.forecast_run IS
'Provenance ledger for mechanical-forecast invocations (design forecast_layer_design_v1.md D5). '
'One row per run of a pure (data, assumptions) forecast callable. retain=true publishes the run into '
'silver.<commodity>_series (book b, feeds the sheets); retain=false is a scenario/what-if kept for '
'provenance only and never enters the book. silver.*_series.run_id links each published forecast row '
'back to its run and thus its exact assumptions. Never holds book-(a) LLM forecasts -- those live in '
'core.forecasts.';
COMMENT ON COLUMN core.forecast_run.retain IS
'Publish gate. true => output rows written into silver.<commodity>_series and flat files. '
'false => scenario/preview only; numbers never enter the book of record.';
COMMENT ON COLUMN core.forecast_run.assumptions IS
'Full assumption set (yields, mix, price deck, ...) behind this run, so a published forecast traces '
'back to the exact assumptions that made it.';
