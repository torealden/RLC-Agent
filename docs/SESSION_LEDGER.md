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
| 5 | Cleanup: run the repoint macro, fix `silver.oil_stocks`, blank the biofuel forecast hole | working macro run + collector fix + visible hole | `[ ]` |
| 6a | **Forecast layer — DESIGN.** Can run now; it is a doc and it unblocks the Rodney Ndum model work in parallel | design spec, numbered decisions, not-verified list | `[ ]` **next** |
| 6b | Forecast layer — BUILD. **After 5** — it writes into the flat files, and that write path is still unproven | `forecast.run`, low-rank vintages, bands | `[ ]` |
| 7 | Helios validation — index vs the 2012 drought / 2019 wet commentary archive | validation note with numbers | `[ ]` |
| 8 | Non-bio everywhere — needs the system graph **and** the PSD 140/149 ingest first | collector change + coverage report | `[ ]` |

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
- [ ] **Rank-ladder reconciliation** *(new, for session 6a)* — `silver.tallow_balance` has
      `MODEL` at rank 30, above `PROSPECTIVE`(20). Also rank 90 shared by CENSUS and
      NASS_FATS_OILS, 95 by CIR and EIA, and `CIR` at both 85 and 95. Not currently
      double-counting (verified), but §7's guarantee holds by luck rather than construction.

---

## Standing known-broken

Full list with evidence in `docs/handoffs/2026-07-21_session_handoff.md` §4. Do not assume any of
these are fixed because a later session touched nearby code.

- [ ] `silver.monthly_realized.oil_stocks` is once-refined alone — consumers understate ~6× *(session 5)*
- [ ] Biofuel has no forecast — MY2026/27 reads 0.0 *(session 5)*
- [ ] `bronze.historical_feedstock_allocation` still feeds `eia_data.xlsm`; +647 mil lb vs the rake
      — confirmed live in `soyoil_balance_sheet`, 2,292 cells *(session 5)*
- [ ] Tallow gap of 3,133 mil lb vs EIA is larger than the RLC-canonical exemption explains — uninvestigated
- [ ] `SoyOilRepointToFlatFile.bas` has never been executed *(session 5)*
- [ ] PSD attributes 140 / 149 not ingested — no biofuel/non-biofuel split outside the US *(session 8)*
