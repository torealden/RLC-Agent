# Session handoff — 2026-07-22

**Session:** system graph build (ledger session 2), plus the `models/` cleanup deliverable
(session 4, pulled forward) and a forecast-layer design conversation that produced rulings.

Read this before acting, then **verify what it claims before acting on it.**

---

## 1. What shipped

| Artifact | Commit |
|---|---|
| `sys` schema — migrations 146, 147, 148 | `50ac935a`, `fa650aa4` |
| `src/sysgraph/` — 10 modules: catalog, repo, series, coderefs, workbooks, blocks, trace, checks, store, scan | `50ac935a` |
| `scripts/sysgraph_scan.py`, `scripts/sysgraph_report.py` | `50ac935a` |
| `docs/specs/system_graph_build_v1.md` — the build record, 7 corrections to the design | `50ac935a` |
| `src/tools/WASDECompUpdater.bas` — was untracked production VBA | `fa650aa4` |
| `src/tools/CornGrindWorkbookEvents.bas` — the missing piece of the Ctrl+K wiring | `488fe7ab` |
| `docs/handoffs/2026-07-22_models_workbook_triage.md` + `_open_question_resolution.md` | `fa650aa4`, `c4c91721` |
| 249 rows in `sys.declaration` | (data) |

**Graph state:** 13,651 nodes / 15,241 edges. Full cold scan **170 s**. All 7 §8 checks pass.

**Q1** (`oil_stocks` blast radius) 18 direct consumers, 61 total, sub-second.
**Q2** reproduces the design's hand count exactly — `eia_data.xlsm` feeds four blocks of
`soyoil_balance_sheet` across **2,292 cells**, found without knowing any row number.
**Q3** 323 repo files, 16 SQL scripts, 525 of 653 workbooks with no code reference.

---

## 2. Rulings persisted (`sys.declaration`, 249 rows)

| Predicate | n | Meaning |
|---|---:|---|
| `IS_CANONICAL` | 153 | keep |
| `ARCHIVE_OK` | 91 | move to `models/Archive` |
| `DATA_PATH` | 1 | source → collector → database → flat file → balance sheet |
| `ARCHIVE_RULE` | 1 | not a balance sheet and not a known input → Archive |
| `HORIZON` | 1 | 20 marketing years forward → **2046/47** |
| `HISTORY_START` | 1 | **1993/94** where data allows |

All 237 `models/` workbooks are now ruled. The 23 that were open were resolved by content,
not by tab name — tab-name overlap is worthless in this estate because every production
workbook shares the same five tab names by convention.

---

## 3. State by workstream

**System graph** — sessions 2 and 4 done. Session 3 (the `SERVES` seam, `trace_series` MCP
tool, `flat_file_series`→`data_series` join) is open and not on any critical path.

**Forecast layer** — designed in conversation, not written. Ledger split into **6a design**
(can run now, unblocks Rodney Ndum's model work) and **6b build** (after session 5, because it
writes into the flat files and that path is unproven). Decisions reached:

- Forecasts are **rows in the existing `silver.*_series` tables** at ranks 1–9. No new schema,
  no new pipeline, no formula change — `MAXIFS(vintage_rank)` makes the model → Prospective →
  Acreage → Final → Actual handoff automatic.
- **Two books, structurally separate.** (b) mechanical model forecasts at flat-file grain feed
  the sheets; (a) the LLM balance-sheet book stays in `core.forecasts` at date grain. They must
  not share a table or the "is the LLM beating Tore" question gets contaminated.
- **Integer ranks, not decimals.** Decimals collide (1.10 *is* 1.1) and conflate ladder position
  with revision. Revisions use the existing `release_date` / `revision` columns; two forecasts
  visible side by side means two ladder positions (`MODEL`=3, `MODEL_ADJUSTED`=6).
- **Bands mandatory, not optional** — the structure should make it hard to publish a far-horizon
  point estimate without one.
- **Model callables must be pure functions of `(data, assumptions)`**, with `forecast.run`
  carrying `assumptions jsonb` + a retention flag. Free now, expensive later, and it is what
  makes the eventual client-facing MCP possible.

**Feedstock / balance sheets** — untouched today. Session 5 still open and still gates 6b.

---

## 4. Known broken / unverified

Carried forward plus new. **Do not assume any of these are fixed.**

- [ ] `silver.monthly_realized.oil_stocks` is once-refined alone — consumers understate ~6× *(session 5)*
- [ ] Biofuel has no forecast — MY2026/27 reads 0.0 *(session 5)*
- [ ] `bronze.historical_feedstock_allocation` still feeds `eia_data.xlsm`; **2,292 cells in
      `soyoil_balance_sheet` confirmed by the graph** *(session 5)*
- [ ] `SoyOilRepointToFlatFile.bas` has never been executed — and is embedded in no workbook
- [ ] Tallow gap of 3,133 mil lb vs EIA — uninvestigated
- [ ] PSD attributes 140 / 149 not ingested *(session 8)*
- [ ] **43 `models/` workbooks report null `sheet_count`** after the property-merge fix. Cause
      unknown, not investigated.
- [ ] **83 external links point at bare `models/Oilseeds/*` paths that do not exist.** This is
      what is *stored* in the file; Excel may repair on open. **Not checked** — open one
      workbook, Data → Edit Links, two minutes.
- [ ] **`us_oilseed_crush[sunflower_crush]` last date 2057-06-01, `[NASS Other Veg Oils]`
      2056-10-01.** Intentional forecast placeholders per Tore, but they exceed the 2046/47
      horizon just ruled, and a bad date propagating into a `MAXIFS` sheet does not announce
      itself.
- [ ] **`silver.fuel_production_forecast` uses `is_forecast boolean`** across 902 rows — cannot
      express which forecast, when made, or with what band. Convert during 6b.
- [ ] `us_grain_crush.xlsm` ethanol tabs stale: weekly stops **2017-12-22**, monthly
      **2020-10-01**. Its `_meta` vintage-rule text is copy-pasted crop-production boilerplate.
- [ ] **12 tracked `.bas` modules are embedded in no workbook** (`RINUpdaterSQL`,
      `EthanolUpdater`, `CrushUpdaterSQL`, …). Per Tore these are *unwired*, not dead — written,
      assumed working, never imported into a destination sheet. Do not report them as orphans.
- [ ] Only **2 of 71** oils/fats workbooks use the flat-file SUMIFS contract. This is the
      **starting line of a migration**, not a defect — the retroactive conversion of direct
      external links is planned work.
- [ ] `src/kg/callables/` holds **2 callables**. That is the real gap for the forecast layer,
      not the storage.

---

## 5. Open for Tore

Only one, and it belongs to session 6a: **the rank-ladder reconciliation.**
`silver.tallow_balance` puts `MODEL` at rank 30, above `PROSPECTIVE`(20). Rank 90 is shared by
CENSUS and NASS_FATS_OILS, 95 by CIR and EIA, and `CIR` appears at both 85 and 95. Verified
that nothing currently double-counts — but §7's uniqueness guarantee is holding by luck, not
by construction, and the forecast ranks land right next to this.

---

## 6. Open the next session with this

Session 6a, forecast-layer design. The cheapest way that design is wrong: **do the flat-file
writers currently emit any vintage below rank 10, anywhere?** The measurement found ranks
10–99 in use and `MODEL` at 30, but no writer was read. If some writer already emits a low rank
for something else, the 1–9 forecast band is not free and the vocabulary needs rethinking
before anything is written down.
