# Handoff 2026-08-05 — RDS outage hardening, NASS quarterly fixes, WAPR characterization

## What shipped (commits bab89954, 9459b6ff, 1dfd8b85 — all pushed)

- **usda_nass_stocks**: quarterly trigger fired the 1st of Jan/Mar/Jun/Sep — 29 days
  BEFORE each last-biz-day release; the 6/30 Grain Stocks report was never collected.
  Fixed with exact `release_dates` (Sep 30 2026 + 2027 calendar; Jan 2027 = Mon-Fri
  11-15 window pending USDA calendar), 16:00 ET pull. **6/30 report backfilled** —
  FIRST OF JUN rows present for corn/sorghum/soybeans/wheat.
- **usda_nass_acreage**: same defect, fatal — day_of_month=31 never fires in Jun/Sep;
  ZERO lifetime runs. Same exact-dates fix; **6/30 Acreage backfilled** (2026 rows,
  all 9 commodities). Quarterly trigger also unions candidate days across ALL years
  so the mid-Jan window survives a dispatcher that doesn't restart after Jan 1.
- **Mig 172**: data_source usda_nass_stocks → quarterly; freshness view gains a
  quarterly rule (>110 days = missed cycle) + 15-min tolerance on the daily rule
  (APScheduler fires ~1s early; 1-second cme run finished before the 17:00:00
  expected instant and re-flagged). Freshness board: 0 overdue, verified both ways.
- **RDS outage hardening**: ~5-min connectivity drop (10060, 09:15-09:20 UTC) killed
  enrich run 1563 mid-run — single-attempt reconnect raised out of fetch_data; lone
  60s finalize retry landed inside the outage → row stranded 'running'. Now:
  `_open_conn_with_retry` (4×60s) + finalize backoff 60/120/300s. Row 1563 closed
  with true error.
- **Orphan dispatcher killed**: restart revealed a second dispatcher (PID 69112,
  spawned 03:46 ET, not tracked by the scheduled task — suspected watchdog spawn).
  Killed; single instance (72468, 05:25:36 ET) runs all current code + schedules.

## Verify next session

- **EPA Echo manual enrich** (detached process, started 09:29 UTC on hardened code,
  1,902 facilities): check today's `epa_echo_enrich_by_frs` row — expect
  success/partial, ~105 min runtime, ONE row. This is the first full exercise of
  single-connection + runtime-cap + reconnect-retry.
- Tonight's quarterly cron does NOT fire (next NASS date Sep 30) — nothing to watch.
- NASS wart (queued): collectors put informational 'rows_persisted=N' in warnings →
  runner maps clean runs to 'partial'. Cosmetic.

## Open items

- **Watchdog audit**: how did an untracked dispatcher spawn at 03:46 ET? If the
  watchdog can start instances the task doesn't own, double-firing recurs.
- **WAPR collector build (next session, Desktop prompt coming)**: see
  memory `reference_wapr_helios_index.md`. Table `bronze.helios_climate_risk` has
  all four risk factors 100% populated (226,736 rows, 88 commodity×country pairs,
  2021-07-01→2028-07-21) but is a ONE-TIME 7/21 load via
  `scripts/collect_helios_climate.py` — no collector, no schedule, no runs.
  Build daily `helios_wapr` collector + backfill 7/22→present + decide
  forecast-vintage handling (no vintage history currently kept).
- RDS outage filed observed-once; correlate with AWS status only if it repeats.
