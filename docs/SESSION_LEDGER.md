# Session Ledger

**What this is:** the tick-off list. One task per session, `/clear` between. Says *what order* and
*what's done* — not *why*. Strategic direction lives in the roadmap; rulings live in their spec.

**Rules of the ledger** (agreed 2026-07-21):
- A session that produces only conclusions produced nothing — every one ends in a durable artifact.
- Each session must be stoppable: if you halted after it, the value already banked stays banked.
- Every artifact ends with a **not-verified** list. The next session opens by checking the cheapest
  item on it.
- Update this file at the end of the session, not the start of the next one.

Status: `[ ]` open · `[~]` in progress · `[x]` done · `[-]` dropped (say why)

---

## Workstream: System graph (code + database + spreadsheet lineage)

Spec: `docs/specs/system_knowledge_graph_design_v1.md` — design D1–D6 and rulings R1–R4 are locked.

| # | Session | Artifact | Status |
|---|---|---|---|
| 1 | Design the node/edge model | `system_knowledge_graph_design_v1.md` (`e3126a03`, rulings `03b55156`) | `[x]` 2026-07-21 |
| 2 | Extractors + validate pass — spec §9 steps 1–7 | `system_graph_build_v1.md`; migrations 146–148; `src/sysgraph/`; 7/7 checks green | `[x]` 2026-07-22 |
| 3 | Seam + tooling — §9 steps 8–9 | ~81 `SERVES` rulings, `trace_series` MCP tool, `flat_file_series`→`data_series` join | `[ ]` |
| 4 | Cleanup deliverable — §9 step 11 (R1) | `models/` triage: 237 workbooks ruled, 249 declarations persisted | `[x]` 2026-07-22 |

**Session 5 done (2026-07-24)** — 3 artifacts, all verified: (1) `oil_stocks` once-refined collision
fixed (soybean 2026-03 476M→2,603M, +265 totals backfilled, collector patched, tie-out green);
(2) soyoil balance sheet repointed off stale `eia_data.xlsm`/`bronze.historical_feedstock_allocation`
onto the flat file — **period-aware** so history 0-fills and only the forward hole blanks (6,312 cells,
`#VALUE!` history now clean, `#DIV/0!` 612→256); (3) biofuel forecast hole visible (blank, not 0.0).
The macro (`SoyOilRepointToFlatFile.bas`) is embedded + run + committed. **`RepointSoyOilCleanup` must
NOT run** — 3,107 forward cells still depend on the ff_ mirrors + `eia_data.xlsm` (Tore's MY2028–2045
extensions + biofuel-yield line). Residual 495 `#VALUE!` are all forward (MY2025–2045), the honest
signature of the ~17-month biofuel gap (May 2026–Sep 2027) between allocator-end and Tore's extensions;
the correct fix is 6b (forecast biofuel forward), NOT a cosmetic 0-fill (would silently mis-state stocks).

Session 2 also landed §9 step 10 (checks wired into the scan). Q1, Q2 and Q3 are answered —
the `oil_stocks` blast radius is 61 nodes in under a second, and the `eia_data.xlsm` chain
reproduces the design's hand count of 2,292 cells exactly.

**Open the next session with this** (build doc §7, cheapest way the build is wrong): only 19
`flat_file_series` came out of 125,610 mined SUMIFS criteria, from just two workbooks. Either
only those two use the flat-file pattern — or the pattern is present elsewhere and being
missed, in which case Q1 is silently under-answered downstream of the flat files. Sample the
criteria from three other oils/fats workbooks and see which it is. Ten minutes.

---

## Workstream: Feedstock / balance sheets — carried from `docs/handoffs/2026-07-21_session_handoff.md`

| # | Session | Artifact | Status |
|---|---|---|---|
| 5 | Cleanup: run the repoint macro, fix `silver.oil_stocks`, blank the biofuel forecast hole | working macro run + collector fix + visible hole | `[x]` 2026-07-24 |
| 6a | **Forecast layer — DESIGN.** Can run now; it is a doc and it unblocks the Rodney Ndum model work in parallel | `docs/specs/forecast_layer_design_v1.md` — D1–D8 + not-verified list | `[x]` 2026-07-23 |
| 6b | Forecast layer — BUILD | `forecast_layer_build_v1.md`; migrations 150–152; `src/forecast/guards.py`; D4 gate PASS | `[x]` 2026-07-24 |
| 6c | **First D5 forecast callable** — biofuel-feedstock-use forecast | `forecast_layer_build_v2_biofuel_callable.md`; mig 153 `soybean_oil_series`; callable + runner; writer wired; 116 rows published; Excel recalc PASS on biofuel blocks | `[x]` 2026-07-24 |
| 6d | **Soybean-oil SUPPLY forecast** (production via crush×oil-yield + stocks) — the *actual* forward `#VALUE!` source | closes the soyoil sheet forward | `[ ]` |
| 7 | Helios validation — index vs the 2012 drought / 2019 wet commentary archive | validation note with numbers | `[ ]` |
| 8 | Non-bio everywhere — needs the system graph **and** the PSD 140/149 ingest first | collector change + coverage report | `[ ]` |

---

**Session 6b done (2026-07-24)** — forecast storage + vocabulary layer. D4 gate PASSED (13→15 col append
to the real wheat flat file shifted zero values in the §4 SUMIFS/MAXIFS contract; no tables/defined-names,
binding by explicit column letter). Shipped: migrations 150 (`core.forecast_run` + retain gate), 151
(`wheat_series` bands + `run_id` + hard band CHECK), 152 (tallow `MODEL` 30→3); `src/forecast/guards.py`
standing MAXIFS-collision guard (0 clean on both live tables, raises on injected collision); canola non-bio
two-vintage split (`RESIDUAL_ACTUAL`90 + `FORECAST_SEASONAL`40, like soy); wheat flat file → contract v1.1.
All verified live (CHECK gate rejects band-less/inverted/null-point forecasts, accepts valid+actuals).
Full detail + not-verified list: `docs/specs/forecast_layer_build_v1.md`.

**Decisions (2026-07-24):** (1) **Biofuel gap DEFERRED** — the 495 forward `#VALUE!` stay loud; closing
the gap = the first real D5 forecast callable, its own session. (2) **D8-decision-1 roll DEFERRED** — soy/
canola `FORECAST_SEASONAL` stays at 40 (not rolled to 1–9) because oils have no `*_series` table/band CHECK
yet and D4 forbids unbanded 1–9 rows; the roll is cosmetic (MAXIFS-identical) and waits for the oils
`*_series` migration. Gate beats parameter.

**Session 6c done (2026-07-24)** — first D5 forecast callable shipped. `biofuel_feedstock_use_forecast`
(pure `(data, assumptions)`; fuel-prod × trailing-12mo intensity → banded `MODEL_BASE`(1) rows for
biofuel_use biodiesel/RD/SAF/total, May-2026..Sep-2028 co-terminal with non-bio). Migration 153
`silver.soybean_oil_series` (2nd book-(b) home, mirrors wheat_series + D4 band CHECK). Runner logs
`core.forecast_run` (retain gate + assumptions jsonb) and writes 116 banded rows carrying `run_id`;
collision guard clean; retain→series→run_id proved end-to-end. Writer merges retained rows into the
demand flat file. **Excel recalc (win32com):** biofuel monthly blocks (r99–177) and non-bio (r179–305)
now **0 errors**; Total BBD Use annual = 14,004 / 13,099 (2026/27, 2027/28) vs 0 before. Full detail:
`docs/specs/forecast_layer_build_v2_biofuel_callable.md`.

**⚠️ Premise correction (verified by recalc, not assumed):** the "495 forward `#VALUE!` = the biofuel gap"
framing was WRONG. The biofuel forward cells are `IF(…="",0,…)`-guarded → they read **0** (silently
wrong), never `#VALUE!`. The `#VALUE!` come entirely from the **un-forecast SUPPLY side** (production
`AL11=AL49→#DIV/0!`, beginning stocks `AL10=AK29→#VALUE!`) cascading into Total Supply/Demand/Ending
Stocks. 624 supply-side errors remain after the biofuel fix.

**Open the next session with this (6d):** **soybean-oil SUPPLY forecast** — production (crush × oil yield)
+ beginning/ending stocks roll-forward, as `MODEL_BASE`(1) banded rows into `silver.soybean_oil_series`,
same publish path. That is what actually closes the soyoil balance sheet forward (the 624 remaining
`#VALUE!`). Copy the 6c callable/runner/writer-merge pattern exactly; the plumbing is proven.

---

## Blocked on Tore — not sessions, decisions

Resolved 2026-07-22 unless marked open.

- [x] **Wheat scope** — *not* a SOW No. 1 amendment. The requirement is real and documented:
      `Helios POC Requirments Gathering.pdf` lists Corn (US, **High**) and Wheat (US/CAN,
      **Medium**) for forecasting and what-if, plus a GRAIN LATAM section. It never made it into
      SOW No. 1, which is veg-oils only. → **draft SOW No. 2 (Grains) for Dominic.**
- [~] **Reference-series conflict** — the issue is redistribution rights, not citation. SOW §3.2
      obligates delivering *reference-series values* weekly; Agreement §10.2 puts the licence
      obligation on RLC and SOW §9 names no third-party restrictions. Ruled: pursue (1) amend
      §3.2 to deliver guidance not benchmark values, and (4) move to public benchmarks where
      arbitrage of the logistical spread allows. Transformation (daily → monthly average) is
      usually acceptable. Likely supplier ProphetX/DTN; **prices are attacked last.**
- [x] **Helios v1 vs v3** — **proceed on v1.** Asked 2026-07-21; Eden and Dominic did not know.
      Called by Tore, explicitly on him if it changes.
- [x] **Dual non-bio** — yes, but a special case. Build it up from the end-use categories already
      broken out (annual volume each, combine, check against the data), not as a second
      mechanical model. The agentic facility model is what ultimately estimates this.
- [x] **Rank-ladder reconciliation** — resolved 2026-07-23, **no decision needed.** Verified 0 MAXIFS
      collisions across `silver.tallow_balance`; the flagged shared ranks (90 CENSUS/NASS, 95 CIR/EIA,
      CIR at 85+95) are all cross-series, fine by construction. CENSUS(trade) and NASS(production)
      never overlap — Census fat production ended 2011-07, NASS started 2015-05, 0 shared months, and
      no post-2011 Census production report exists. 6b: relocate tallow `MODEL`(30)→`MODEL`(3) and add
      the collision check as a standing guard. See `forecast_layer_design_v1.md` §7.

---

## Standing known-broken

Full list with evidence in `docs/handoffs/2026-07-21_session_handoff.md` §4. Do not assume any of
these are fixed because a later session touched nearby code.

- [x] `silver.monthly_realized.oil_stocks` once-refined collision — **FIXED session 5** (crude + once-refined;
      shared recompute in `oil_stocks_composition.py`, collector patched, 265 totals backfilled).
- [x] Biofuel forecast hole — **CLOSED session 6c.** `biofuel_feedstock_use_forecast` callable now fills
      biofuel_use forward May-2026..Sep-2028 (banded MODEL_BASE rows via `silver.soybean_oil_series`).
      **Correction:** the "495 forward `#VALUE!`" were NOT this gap — biofuel forward was IF-guarded to 0
      (silently wrong), not `#VALUE!`. The forward `#VALUE!` are the un-forecast **supply side** (see 6d).
- [ ] **Soyoil balance sheet still #VALUE! forward (624 cells)** — supply side (production/stocks) is
      un-forecast; `AL11 Production=AL49→#DIV/0!`, `AL10 Beginning Stocks=AK29→#VALUE!` cascade. Needs the
      6d soybean-oil supply forecast to close. *(new, session 6c — verified by Excel recalc)*
- [x] `bronze.historical_feedstock_allocation` → `eia_data.xlsm` → `soyoil_balance_sheet` — **RESOLVED session 5**
      for the historical/near range: 6,312 cells repointed to the flat file, zero eia_data refs in that range.
      (Forward MY2028–2045 still read eia_data by design — Tore's extensions past the flat-file horizon.)
- [ ] Tallow gap of 3,133 mil lb vs EIA is larger than the RLC-canonical exemption explains — uninvestigated
- [x] `SoyOilRepointToFlatFile.bas` — **executed session 5** (embedded, Preview→Apply→BlankBBDForecastHole).
- [ ] **`RepointSoyOilCleanup` must NOT run** — 3,107 forward cells (ff_ mirrors + `eia_data.xlsm`) still
      depend on it; breaking the links `#REF!`s Tore's MY2028–2045 extensions + the biofuel-yield line *(new, session 5)*.
- [ ] PSD attributes 140 / 149 not ingested — no biofuel/non-biofuel split outside the US *(session 8)*
