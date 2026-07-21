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
| 2 | Extractors + validate pass — spec §9 steps 1–7 | `sys` migration + extractors + green checks §8 | `[ ]` **next** |
| 3 | Seam + tooling — §9 steps 8–10 | `sys.declaration`, ~81 `SERVES` rulings, `trace_series` MCP tool | `[ ]` |
| 4 | Cleanup deliverable — §9 step 11 (R1) | archive-candidate report; Tore rules; rulings persisted | `[ ]` |

**Open the next session with this** (spec §14, cheapest way the design is wrong): do the 26 `.bas`
files in git match the VBA actually embedded in the `.xlsm` workbooks? If they've diverged, the VBA
extractor parses fiction and build steps 3/5 need rethinking. Two minutes to test.

---

## Workstream: Feedstock / balance sheets — carried from `docs/handoffs/2026-07-21_session_handoff.md`

| # | Session | Artifact | Status |
|---|---|---|---|
| 5 | Cleanup: run the repoint macro, fix `silver.oil_stocks`, blank the biofuel forecast hole | working macro run + collector fix + visible hole | `[ ]` |
| 6 | Forecast layer — **after 5**. Tiered by series type; vintages frozen in `core.forecasts`; 50%/90% bands from the start | forecast writer + bands | `[ ]` |
| 7 | Helios validation — index vs the 2012 drought / 2019 wet commentary archive | validation note with numbers | `[ ]` |
| 8 | Non-bio everywhere — needs the system graph **and** the PSD 140/149 ingest first | collector change + coverage report | `[ ]` |

---

## Blocked on Tore — not sessions, decisions

From handoff §3. These gate the sessions above; none are resolved.

- [ ] **Wheat scope** — live workstream, absent from SOW No. 1. Amend, change-order, or stop building.
- [ ] **Reference-series citation conflict** — 3 of 5 contracted series are private assessments,
      unpublishable under SOW §9. Settle during build-out, not after.
- [ ] **Helios v1 vs v3** — docs reference `/v3/daily-risk`; it 404s. We built on v1.
- [ ] **Dual non-bio** — carry the derived residual *and* the independent mechanical forecast?

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
