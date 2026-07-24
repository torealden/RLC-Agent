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
| 6b | Forecast layer — BUILD. **Unblocked** — write path proven: soyoil balance sheet now reads the flat file (6,312 cells), history clean | `forecast.run`, low-rank vintages, bands | `[ ]` |
| 7 | Helios validation — index vs the 2012 drought / 2019 wet commentary archive | validation note with numbers | `[ ]` |
| 8 | Non-bio everywhere — needs the system graph **and** the PSD 140/149 ingest first | collector change + coverage report | `[ ]` |

---

**Open 6b with this** (forecast design §9, cheapest way the design is wrong): **is the flat-file
schema append actually non-breaking — tested on the RIGHT workbook?** D4 adds `value_low`/`value_high`
as trailing columns and asserts Desktop's `MAXIFS`/`SUMIFS` bind only to keys 1–8 + `value`. Verified
this session that soy is the WRONG test target — `us_soybean_complex_bal_sheets.xlsm` reads its flat
file via positional external links (`xlsx_extlink`), not the `vintage_rank`/`MAXIFS` contract. Run
the append test on the wheat pilot (`us_wheat_production.xlsx`, a genuine SUMIFS consumer): add the
two columns, open the consumer, confirm nothing shifts. If it shifts, the band mechanism (all of D4)
needs rethinking before any migration. Gate decision is settled: **hard CHECK gate, fail loud.**

Note for 6b: verified this session that "1–9 confirmed free" was true only at the floor — `MODEL`=30,
`FORECAST_SEASONAL`=40, `RESIDUAL`=50 already exist as forecast/model vintages above 10 (D3/D7/D8).

**Sharpened by session 5 (cheapest 6b opener):** even *after* the repoint, `soyoil_balance_sheet` reads
the flat file via **plain positional cell refs** (`=IF([5]soybean_oil_demand_wide!$AM$3="","",…)`), NOT
SUMIFS/MAXIFS — so appending `value_low`/`value_high` trailing columns is invisible to soy *and* soy gets
no MAXIFS auto-upgrade. So the D4 append test **must** run on the wheat pilot (`us_wheat_production.xlsx`,
a genuine SUMIFS consumer). That is the ten-minute first check of 6b. Second-cheapest: the biofuel gap
(May 2026–Sep 2027) is the concrete thing 6b's forecast must fill to clear the 495 forward `#VALUE!`.

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
- [~] Biofuel forecast hole — **made visible session 5** (blank not 0.0). The ~17-month gap itself
      (May 2026–Sep 2027) is un-forecast; closing it is **6b** work. Currently shows as 495 forward `#VALUE!`.
- [x] `bronze.historical_feedstock_allocation` → `eia_data.xlsm` → `soyoil_balance_sheet` — **RESOLVED session 5**
      for the historical/near range: 6,312 cells repointed to the flat file, zero eia_data refs in that range.
      (Forward MY2028–2045 still read eia_data by design — Tore's extensions past the flat-file horizon.)
- [ ] Tallow gap of 3,133 mil lb vs EIA is larger than the RLC-canonical exemption explains — uninvestigated
- [x] `SoyOilRepointToFlatFile.bas` — **executed session 5** (embedded, Preview→Apply→BlankBBDForecastHole).
- [ ] **`RepointSoyOilCleanup` must NOT run** — 3,107 forward cells (ff_ mirrors + `eia_data.xlsm`) still
      depend on it; breaking the links `#REF!`s Tore's MY2028–2045 extensions + the biofuel-yield line *(new, session 5)*.
- [ ] PSD attributes 140 / 149 not ingested — no biofuel/non-biofuel split outside the US *(session 8)*
