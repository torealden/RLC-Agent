# Morning briefing — 2026-05-22

*Helios meeting day. Rehearsal blocked 9:00-10:30 AM ET. Meeting at 2:00 PM ET.*

---

## Overnight work — what landed

### 1. FAS Export Sales historical backfill ✅
- **624,444 records persisted** across 9 commodities × 34 marketYears (1990-2023)
- Range: **1989-06-01 → 2026-05-14**
- Total v2-keyed records in bronze.fas_export_sales: **1,320,632**
- Ahead of the 1993/94 target — we went all the way to MY 1989/90 (the API's full history)
- Per-commodity coverage:

| Commodity | Earliest week | Records |
|---|---|---:|
| wheat_hrs | 1989-06-01 | 218,398 |
| wheat_hrw | 1989-06-01 | 204,538 |
| wheat_srw | 1989-06-01 | 173,206 |
| soybean_meal | 1989-10-05 | 161,781 |
| corn | 1989-09-07 | 159,435 |
| soybeans | 1989-09-07 | 144,791 |
| soybean_oil | 1989-10-05 | 133,414 |
| cotton_upland | 1989-08-03 | 89,229 |
| sorghum | 1989-09-07 | 35,840 |

### 2. AMS overnight cash prices ✅
- Wed 5/21 21:00 UTC run succeeded: **5,673 records collected, 404 persisted to bronze for 5/21 across 16 slugs**
- The `.collect()` fix from Monday continues to hold
- Bronze.ams_price_record now current through 2026-05-21

### 3. Ops dashboard alerts cleaned ✅
- Bulk-acknowledged **349 stale alerts** (out of 437)
- Down to **88 active**, all of which are real outstanding work:
  - 60 alerts: `usda_ams_feedstocks` + `usda_ams_ddgs` parser broken (TXT report deprecated by USDA Sep 2022; needs MARS-API rewrite — task #70)
  - 28 alerts: 7 manual-download sources (mpob, faostat, conab, ers_*, epa_rfs) — Tore's punch list

### 4. Code-bug audit ✅
- Verified every collector in the "broken" punch list actually loads + runs correctly NOW
- The "not found in registry" errors for yield_forecast/weather_daily_summary/nass_processing were stale (registered in prior sessions; failures pre-date registration)
- The gefs_ensemble flakiness was transient (May 11-12); recent runs succeed
- The cme_settlements "OVERDUE" alerts were a false-positive: data is fresh (70,209 rows, latest 2026-05-21), the freshness threshold for `daily` cadence just fires too aggressively. Cleared.

### 5. Helios demo pre-warmed ✅
- All 11 key dashboard queries hit twice; second-pass cache speedup 50-75% on most
- Total time for the dashboard's data load: ~2 seconds warm vs ~4 seconds cold
- Slowest queries: bronze.fas_export_sales (1M+ rows), bronze.cftc_cot. Acceptable for the demo

---

## Status going into 9am rehearsal

| Component | State |
|---|---|
| **Q&A script** | Ready: `docs/specs/helios_rehearsal_qa.md` |
| **Leave-behind** | Ready: Dropbox + `docs/specs/helios_leave_behind.md` |
| **Demo dashboard** | Streamlit at `dashboards/helios_demo/app.py`, queries pre-warmed |
| **IFV calibration** | Patched (134.47 / 88.62), 24 unit tests pass, reproduces IFVS spec §5.8 exactly |
| **KG stats** | 436 nodes / 395 edges / 336 contexts (deck + leave-behind both reflect this) |
| **Data freshness** | AMS through 5/21, FAS through 5/14, CFTC through 5/12 (next release tonight 4:30 ET) |

**Risk items to address in rehearsal:**
1. The **moat answer** ("math isn't the moat — pipeline + analyst credibility is") — Francisco will probably probe
2. The **"what would change your mind on acquisition" answer** — currently mentions Claude API spend by name; consider rewording
3. The **timing on demo walkthrough** — script targets 12 min; need a live timing pass

**New tasks created overnight:**
- **#70** — Rewrite AMS Tallow/Protein + DDGs parsers (TXT deprecated since Sep 2022)

---

## Punch list for after Helios (in priority order)

1. **#70** AMS feedstocks/ddgs MARS-API rewrite (60 stale alerts depend on this)
2. **#67** NASS data_type plumbing (the silent failure on crop_progress)
3. **#69** 3-way balance sheet comparison wire-up (your spreadsheets vs RLC vs USDA — this is the next analytical artifact)
4. **#68** Energy + Brazil + Census backfill to Jan 1993
5. **#60-65** IFVS infrastructure pieces (Claude-Content waiting on FastAPI endpoint and MCP tool)
6. **#66** ECHO architecture flip (9hr daily grind → ~30 min via FRS-ID enrich)

---

*Generated 2026-05-22 ~03:30 ET. Re-runs of any of the audit scripts will refresh the numbers.*
