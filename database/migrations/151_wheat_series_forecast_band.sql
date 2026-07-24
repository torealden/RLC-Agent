-- 151_wheat_series_forecast_band.sql
--
-- Forecast layer BUILD (ledger 6b; design forecast_layer_design_v1.md D4/D5).
--
-- Adds the mandatory-band storage to the forecast pilot table silver.wheat_series and enforces it
-- at the database, not by convention.
--
--   value_low / value_high : the forecast interval. Projected to the flat file as two TRAILING
--                            columns (append-only; keys 1-8 + value keep their positions). Verified
--                            2026-07-24 non-breaking against the real §4 SUMIFS/MAXIFS contract on
--                            the wheat pilot -- a 13->15 column append shifted zero computed values
--                            (D4 gate PASS; see forecast_layer_design_v1.md §9 / SESSION_LEDGER 6b).
--
--   run_id                 : one-hop provenance to core.forecast_run (row -> run -> assumptions, D5).
--                            NULL for actuals. NOT projected to the flat file (stays a DB-only column;
--                            the writer's explicit column list is unchanged for run_id).
--
-- The CHECK is the "hard to skip" part (D4, ruled by Tore 2026-07-23: hard gate, fail loud, never
-- silent). A forecast-band row (rank 1..9) CANNOT be inserted without a band that brackets the point:
--     rank 1..9  =>  value, value_low, value_high all present AND value_low <= value <= value_high
-- Equality is allowed (value_low = value = value_high) so a deliberately tight/degenerate band is
-- EXPRESSIBLE -- but a NULL band on a forecast never is. Actuals (rank >= 10) leave the band columns
-- NULL and are unconstrained. No is_forecast-style boolean escape hatch (that boolean is exactly the
-- silver.fuel_production_forecast failure this replaces).
--
-- Vacuously satisfied on apply: min(vintage_rank) in silver.wheat_series is 10 today -- there are no
-- rank 1..9 rows yet -- so the constraint validates against existing data without a single violation.
-- (If that were not true this ALTER would fail loud, which is the point.)

ALTER TABLE silver.wheat_series
    ADD COLUMN value_low  numeric,
    ADD COLUMN value_high numeric,
    ADD COLUMN run_id     uuid REFERENCES core.forecast_run(run_id);

ALTER TABLE silver.wheat_series
    ADD CONSTRAINT wheat_series_forecast_band_ck CHECK (
        vintage_rank > 9
        OR (
            value      IS NOT NULL AND
            value_low  IS NOT NULL AND
            value_high IS NOT NULL AND
            value_low <= value AND value <= value_high
        )
    );

COMMENT ON COLUMN silver.wheat_series.value_low IS
'Forecast-band lower bound. Required (with value_high) for rank 1..9 forecasts (CHECK '
'wheat_series_forecast_band_ck); NULL for actuals (rank >= 10). Projected to the flat file as a '
'trailing column (flat_file_contract v1.1). Band semantics default to a scenario/subjective interval '
'-- document the exact convention per series in _meta, do not read as a p-value.';
COMMENT ON COLUMN silver.wheat_series.value_high IS
'Forecast-band upper bound. See value_low.';
COMMENT ON COLUMN silver.wheat_series.run_id IS
'FK to core.forecast_run: one-hop provenance from a published forecast row to its invocation and '
'assumptions (design D5). NULL for actuals. DB-only -- not projected to the flat file.';
COMMENT ON CONSTRAINT wheat_series_forecast_band_ck ON silver.wheat_series IS
'D4 hard band gate (Tore 2026-07-23): a forecast-band row (rank 1..9) must carry value + a band that '
'brackets it; equality permitted, NULL never. Actuals (rank >= 10) unconstrained.';
