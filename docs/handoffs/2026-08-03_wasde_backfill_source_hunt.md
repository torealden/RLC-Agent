# Handoff 2026-08-03 (early AM) — Scheduled-job check + WASDE vintage backfill source hunt

Session picked option (a) from `2026-08-03_curve_engine_claude_md_lock.md`.

## 1. Scheduled-job verification: NOT YET POSSIBLE — too early, not a failure

Session ran at ~02:50 ET on 08-03. `core.collection_status` shows only the manual runs
from the build session (all SUCCESS, triggered_by cli/cli_verify/cli_drift_demo). First
scheduled fires are **later today**: claude_md_drift_check 07:30, futures_price_mark_bridge
17:45, curve_builder 18:15. **Next session must actually check these** — the item stays open.

**Side finding, estate-wide, pre-existing**: `core.collection_status` rows come in pairs for
some collectors (eia_crude_price_bridge, ams_grain_settlement, the three new jobs) where one
row's `run_started_at` is written ET-naive and `run_finished_at` UTC → apparent 4-hour
durations. faostat rows are clean. Cosmetic for duration math but will distort any
freshness/duration calc that trusts `run_started_at`. Not chased; worth a look inside the
scheduler's status-writer double-logging path.

## 2. WASDE vintage backfill — source found, secured, verified. Ingestion NOT built.

Full detail: `docs/specs/wasde_vintage_backfill_v1.md`. Short version:

- **USDA OCE publishes exactly the pre-compiled dataset we wanted**: per-release CSVs of
  every WASDE as-published, Apr 2010 → present ("Consolidated Historical WASDE Report Data").
- **All of it is on disk**: `data/raw/wasde_historical/` — 2 ZIPs (2010–2020, committed to
  git because USDA's WAF now blocks .zip re-download; retrieved via Wayback) + 66 monthly
  CSVs 2021-01→2026-07 (gitignored, regenerable via `scripts/download_wasde_historical.py`).
- **Verified**: 193 reports, WASDE #481→#673, zero sequence gaps; 948,901 rows; month gaps
  2013-10 / 2019-01 / 2025-10 are shutdown skips proven by report-number contiguity.
  July 2026 CSV ties to `gold.psd_wasde_vintages` WASDE_JUL_26 US corn at published rounding.
- **Errata gotcha**: reposts rename to uppercase `-V2.csv` (2026-05, 2026-06).

## Open decisions needing Tore

1. **vintage_rank assignment for backfilled rows** (spec §Ingestion plan item 2): reuse
   mig-166 maturity logic (Proj. 63 / Est. 67, FINAL 90 stays on top). Confirm before load.
2. Commodity scope for the v1 transform (spec proposes comp-tab commodities only).
3. Still pending from prior handoff: fob_spread/band rulings (curve engine review points).

## Known-broken / unverified

1. Three scheduled jobs still unverified (fire later today — see §1).
2. Ingestion of the archive: nothing loaded to bronze yet; no schema written.
3. `verify_wasde_archive.py` lives in the session scratchpad only; the spec says "rerun any
   time" — recreate from spec §Verification if needed (or fold its checks into the loader
   as binding assertions, per the CLAUDE.md checks-in-code rule).
4. collection_status timestamp skew (§1) — diagnosed pattern, root cause not located.

## Next-session prompt

> Read docs/handoffs/2026-08-03_wasde_backfill_source_hunt.md. First verify yesterday's
> three scheduled jobs actually fired clean in core.collection_status (drift check 07:30,
> bridge 17:45, curve_builder 18:15 — triggered_by should be the dispatcher, not cli).
> Then build the WASDE historical backfill loader per
> docs/specs/wasde_vintage_backfill_v1.md: bronze.wasde_historical + transform into the
> vintage ladder. Get Tore's ruling on vintage_rank assignment (spec §2) before the
> transform writes anything. Put the archive-contiguity checks (report-number sequence,
> shutdown skips) in the loader as assertions, not in prose.
