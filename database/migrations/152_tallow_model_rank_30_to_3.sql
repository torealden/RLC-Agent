-- 152_tallow_model_rank_30_to_3.sql
--
-- Forecast layer BUILD (ledger 6b; design forecast_layer_design_v1.md D7/D8).
--
-- Vocabulary alignment: relocate the tallow mechanical-model vintage MODEL from rank 30 to rank 3,
-- so the "MODEL" word carries the MODEL band rank (D3: MODEL = 3) estate-wide. Behavior-neutral,
-- verified 2026-07-24 against the live table:
--   * silver.tallow_balance has ZERO rows at rank 4..29 -> MODEL is already the floor; moving it
--     from 30 to 3 keeps it the floor. MAXIFS still picks it for gap periods and the actuals ladder
--     (SLAUGHTER_DERIVED 60 / CENSUS_CIR 80 / CIR 85 / CENSUS,NASS 90 / EIA 95) still overrides it,
--     identically at 30 or 3.
--   * No consumer (VBA/py/sql) filters tallow on vintage_rank=30.
--   * 644 MODEL@30 rows relocate; 0 MAXIFS collisions before, and MODEL is a single vintage moving
--     uniformly so still 0 after (re-checked in the same session by the standing guard).
--
-- KNOWN GAP (documented, not silent): silver.tallow_balance is NOT a silver.<commodity>_series table
-- and carries no band CHECK, so this rank-3 MODEL is an UNBANDED forecast. The D4 band guarantee
-- holds today only on silver.wheat_series (the pilot). Tallow gains band enforcement when it migrates
-- to the *_series pattern (separate, pre-existing flat-file migration work). See build doc §not-verified.
--
-- The builder (scripts/build_tallow_biofuel_use.py) is edited in the same commit to write rank 3 and
-- to include vintage_rank in its upsert's DO UPDATE SET, so a future run stays consistent with this
-- migration instead of silently leaving old rows at 30.

UPDATE silver.tallow_balance
   SET vintage_rank = 3
 WHERE vintage = 'MODEL' AND vintage_rank = 30;

-- Post-condition guard, in SQL, fail-loud: no (class,series,period) key may carry more than one
-- distinct vintage at its winning (max) rank. Mirrors the Python standing guard; raises if violated
-- so the migration rolls back rather than leaving a silently double-countable flat file.
DO $$
DECLARE bad int;
BEGIN
    WITH mx AS (
        SELECT class, series, period, max(vintage_rank) mr
        FROM silver.tallow_balance GROUP BY 1,2,3
    )
    SELECT count(*) INTO bad FROM (
        SELECT t.class, t.series, t.period
        FROM silver.tallow_balance t
        JOIN mx ON mx.class=t.class AND mx.series=t.series AND mx.period=t.period AND mx.mr=t.vintage_rank
        GROUP BY t.class, t.series, t.period
        HAVING count(DISTINCT t.vintage) > 1
    ) x;
    IF bad > 0 THEN
        RAISE EXCEPTION 'tallow MAXIFS collision after MODEL 30->3: % key(s) with >1 vintage at max rank', bad;
    END IF;
    RAISE NOTICE 'tallow MODEL 30->3 applied; 0 MAXIFS collisions confirmed';
END $$;
