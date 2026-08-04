# Handoff 2026-08-04 — 'dict' has no attribute 'success' + WinSock 10055 / stuck EPA Echo runs

## Root cause (verified, not inferred)

**The "intermittent" failures were not intermittent and masked no data loss.** Every
scheduler fire of `fred_fx`, `ecb_fx`, `ams_dco_prices` (and, pre-8/3, `ams_grain_settlement`,
`eia_crude_price_bridge`, `claude_md_drift_check`) produced **two** `collection_status` rows
for the **same run**:

1. the collector's own self-logged `SUCCESS/manual` row, written with an ET-naive
   `datetime.now()` start (hence the 4-hour start/finish skew), and
2. the runner's `failed/scheduler` row with `'dict' object has no attribute 'success'`,
   because `collect()` returned a plain dict and `collector_runner.py` accessed
   `result.success` unguarded.

Commit `edd5f663` (8/3) fixed this contract for 5 modules but missed the three
price-layer collectors above. Verified post-8/3: `ams_grain_settlement` and
`eia_crude_price_bridge` run clean via scheduler; the three missed ones still failed.
Data was current the whole time (upserts idempotent; re-runs changed 0 rows).

**The WinSock 10055 is a separate, real failure** — recorded once in `core.event_log`
(2026-06-26, `epa_echo_enrich_by_frs`, psycopg2 *connect to RDS* failing with
"No buffer space available"). Driver: the enrich collector opened **2 new TCP+SSL
connections per facility** (`_get_prior_state` + `_upsert_and_audit`), ~4,000 per
~105-min run. The stuck `running` rows had a second mechanism: when a run died of a
transient network error, the runner's finalize UPDATE hit the same network error,
was swallowed after one attempt, and the row stayed `running` forever. Then
`run_with_retry` fired attempt 2 → a second concurrent `running` row (today's
1538 dead / 1539 live pair, exactly 15 min after attempt 1 died).

## What shipped (commit 855ec2df)

- `fred_fx_collector.py`, `ecb_fx_collector.py`, `ams_dco_collector.py` — `collect()`
  returns `CollectorResult`; self-logging only on the explicit CLI path (tz-aware UTC).
- `collector_runner.py` — normalizes any future dict return (surfaces the collector's
  TRUE error, logs a contract warning); finalize UPDATE retried once after 60s so
  transient network failures can't strand `running` rows.
- `epa_echo_enrich_by_frs_collector.py` — one shared DB connection per run
  (reconnect-on-error), and a hard `max_runtime_min=150` wall-clock cap (normal run
  ~105 min) so robotic-block backoffs can't run unbounded.
- Closed 13 orphaned `status='running'` rows (May–Aug: old SIC-sweep trio ×6,
  enrich ×5 incl. today's dead attempt 1, canada_cgc, usda_ams_cash_prices ×2).

## Verified

- `fred_fx` / `ecb_fx` / `ams_dco_prices` re-run through `CollectorRunner`: success,
  single clean status rows, `silver.price_mark` current (fred_h10 62,276 rows → 7/31;
  ecb 41,838 → 8/3; usda_ams_3618 1,592 → 7/31). 7/31 is correct — Friday close.
- Parser tests 11/11 pass.
- TCP sampled during the live enrich window: TIME_WAIT 11–14, RDS conns 3–5, no
  exhaustion. The ~540 "Bound" sockets are OneDrive (416), not our stack.

## Known-broken / unverified

- **Dispatcher restart pending**: PID 56328 still runs pre-fix code. A background
  job waits for enrich run 1539 to finish (~10:47 UTC), then restarts
  `\RLC\RLC Dispatcher`. If that didn't land, restart the task manually — until then
  tonight's fred_fx/ecb_fx/ams_dco fires would still log the AttributeError.
- The enrich single-connection + runtime-cap change has **not yet run end-to-end**
  (first real exercise = tomorrow's 04:00 ET fire). Check `collection_status`
  tomorrow: expect one row, `success`/`partial`, finished ≤150 min.
- Other registered collectors that still return dicts from `collect()` now work via
  the runner's normalizer (with a warning log) but should be converted opportunistically.
- Zombie-run checker (flag `running` > 6h in daily CNS) remains on the priority queue —
  today's cleanup was manual.
- 10055 recurrence: not reproducible today; single documented occurrence (6/26).
  If it recurs after the churn fix, look beyond this collector (non-paged pool,
  other processes).
