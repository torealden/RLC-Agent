# Handoff 2026-08-03 — WASDE historical backfill loader + vintage-ladder union

Follow-on from `2026-08-03_wasde_backfill_source_hunt.md`. Ran ~05:40–06:30 ET.

## Shipped (all verified, all committed)

1. **`bronze.wasde_historical`** (mig 167) + `scripts/load_wasde_historical.py`.
   948,901 rows, 193 reports, WASDE #481 (Apr 2010) → #673 (Jul 2026). The archive-
   contiguity checks are BINDING ASSERTIONS in the loader (A1–A9), not prose:
   header/key uniqueness per file, report-number sequence zero-gap from 481,
   month gaps == the three shutdown skips exactly, rows-per-report band, post-load
   DB totals, and a US-corn tie-out vs the live ladder. Loader is idempotent
   (10-col natural key — the spec's 7-col key was NOT unique; reliability-appendix
   and Proj./Est. rows collide) and stamps `last_touched_at` on every touch.
2. **`silver.wasde_historical_vintage`** (mig 168) + `scripts/transform_wasde_history_to_vintages.py`.
   46,023 PSD-shaped vintage rows from the 7 world tables (corn, wheat, soybeans,
   soybean meal, soybean oil, rice, cotton) × 29 mapped countries incl. World.
   Assertions T1–T6; T5 tie-out vs live PSD on 2,815 shared values: **100.00%
   within ±6 (1000 MT/bales)**, zero gross mismatches.
3. **`gold.psd_wasde_vintages` redefined as the union** (mig 168), per **Tore's
   ruling (2026-08-03): "one ladder, recomputed"**. Live PSD wins shared cycles;
   FINAL stays live newest of a closed MY at 90; everything else 61+dense_rank
   (psd_cycle) per MY capped 79. Live ranks renumbered (rank = order only).
   New `vintage_source` column: 'PSD' (API precision) vs 'WASDE_ARCHIVE'
   (published 2-dp rounding) — don't chase sub-0.01-MMT "revisions" across sources.
   Verified: US corn MY2012 shows the full drought path May 375.7 → Jan 273.8 MMT
   with FINAL 90 on top; MY2025 shows archive 61–67 (May–Dec 25) then live 68–74.
4. **`build_usda_comp_tabs.py` ORDER BY tie-break** (`vintage_rank DESC, psd_cycle
   DESC`) — REQUIRED, not hygiene: active MYs will exceed 19 cycles and tie at 79.
   Comp tabs themselves NOT rebuilt this session (rerun after next WASDE per SOP).
5. Scope ruling #2 (comp-tab commodities): extending later = add REGION_CODE /
   TITLES entries and re-run; no structural cost.

## Scope NOT covered (explicit, not silent)

- **Sorghum**: no WASDE world table (only Coarse Grain aggregate). US sorghum
  exists in the bushel-basis US table in bronze — needs its own conversion path
  if wanted on the ladder.
- **Wheat classes**: US table only, not on the PSD ladder (PSD has no by-class).
- **Pre-2010**: deferred per spec (ESMIS PDF scrape).
- **EU vintage gap Jan–Apr 2021** (WASDE #608–611): world tables printed
  'EU-27+UK' instead of 'European Union' (Brexit transition). Mapping it to E4
  (EU-27) would contaminate the series → whitelisted as aggregate, honest 4-cycle
  hole in E4 history.

## Side findings (pre-existing live-data defects, NOT fixed here)

1. **Live cotton unit labels are wrong**: ALL live cotton values are 1000 480-lb
   bales, but rows before the 2026-03 cycle are labeled '1000 MT' (US 13,918
   identical under both labels; China 34,500 only works as bales). Queue a label
   fix in the PSD collector / bronze cleanup.
2. **ISO-orphan rows are not just orphans — some carry WRONG country names**:
   live rows "South Africa|ZA" hold ZAMBIA values (FIPS ZA=Zambia; real South
   Africa is SF), "Nigeria|NG" is likely NIGER, "Australia|AU" likely AUSTRIA.
   Verified numerically (ZA corn domestic 2.8 MMT ≈ Zambia; SF-scale is ~14).
   Raises the priority of the queued ISO-orphan cleanup: it's a mislabeling bug,
   not cosmetic duplication.
3. **PSD WD (World) domestic_consumption is identity-derived**
   (= beg+prod+imp−exp−end, verified exact), which differs from the WASDE world
   print by the world trade imbalance (~24 MMT for corn). Definitional, both
   internally consistent; transform tie-outs assert WD only on production/stocks.
4. **RealDictCursor gotcha** (cost one 948k-row rollback): `get_connection()`
   sets RealDictCursor; a SELECT with two same-named columns (e.g. two COUNTs)
   silently collapses keys. Alias every aggregate.

## Scheduled-job verification (task from prior handoff) — STILL OPEN, monitored

At 06:11 ET none of the three had fired yet (drift 07:30, bridge 17:45, curve
18:15 — all later today). A **persistent Monitor** is armed in this session
polling `core.collection_status` every 10 min; it reports each fire (status +
triggered_by) or a MISSED line at due+45 min. If the session closes first, next
session runs:
```sql
SELECT collector_name, run_started_at, status, triggered_by
FROM core.collection_status
WHERE collector_name IN ('claude_md_drift_check','futures_price_mark_bridge','curve_builder')
  AND run_started_at > '2026-08-03 10:00+00' ORDER BY run_started_at;
```
`triggered_by` must be the dispatcher, not cli.

## Known-broken / unverified

1. The three scheduled fires (above) — unverified until they happen.
2. Comp-tab workbooks not rebuilt against the union view (query change verified
   by inspection only; ties-at-79 path untested against real books).
3. collection_status ET-naive/UTC start-stamp skew — still diagnosed-not-fixed
   (loader/transform here write both stamps tz-aware UTC and are clean).
4. CLAUDE.md DB inventory regenerated (bronze 108, silver 79 tables now).

## Next-session prompt

> Read docs/handoffs/2026-08-03_wasde_backfill_loader.md. First close out the
> scheduled-job verification (query in §Scheduled-job verification; triggered_by
> must be dispatcher). Then pick up the priority queue: USDACompUpdater.bas
> universal + meta-stamp + usda_comp style-donor formatting (donor = Argentina
> xlsm tab), and rebuild the comp tabs against the union view — first rebuild
> exercises the rank-79 tie-break, check a deep-history tab (US corn) by hand.
