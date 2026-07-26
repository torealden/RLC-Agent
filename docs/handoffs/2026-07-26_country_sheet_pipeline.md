# Handoff — 2026-07-26: the country balance-sheet pipeline (data → flat → sheet)

Read this, then **verify before acting**. This session turned the "build all the country sheets"
goal into a **working, verified, three-stage pipeline** and proved it end-to-end on Brazil soy.

## ⭐ START HERE — the plan and the pipeline

**The plan (the drawings):** `docs/specs/rlc_model_completion_masterplan_v1.md` — report outline (Part
A), the country-build SOP (Part B), the prioritized matrix (Part C), the P0→P·FINAL sequence (Part D).
Governing ruling: no shortcuts; Helios/Pepsi set first; fundamentals before prices; **prices are the
final pass**.

**The interface:** `docs/specs/flat_file_contract_v1.md` **(v1.1 — read §6, the VERIFIED wiring)**.

**Live coverage tracker:** `docs/specs/rlc_model_coverage_matrix.html` (regen:
`python scripts/build_coverage_matrix_html.py`).

### The pipeline (three scripts, all working, all committed)

```
1. python scripts/write_psd_flat_file.py <commodity> <CODE>     # annual PSD control -> <country>_<commodity>_flat.xlsx
2. python scripts/write_<country>_<complex>_monthly.py           # MERGE monthly national-source rows into the flat file
3. python scripts/write_balance_sheet.py <target-key>            # closed balance sheet (mirror tabs + verified wiring)
```

- Stage 3 **closes on the annual PSD flat file alone** — you do NOT need stage 2 to get a tied-out
  annual sheet. Monthly (stage 2) is enrichment.
- **Always verify stage 3 with an Excel recalc** (win32com): every `_balance_sheet.xlsx` tab must show
  the two tie-out cells = 0, the `CHECK SUM(production)` cell > 0, and **zero `#` errors**. The
  recalc snippet is in this session's transcript; the trap it catches is real (see "MAXIFS" below).

## What shipped + verified this session

| Artifact | State |
|---|---|
| `scripts/write_psd_flat_file.py` | 31 annual flat files (all Tier-A oilseed cells) written + tie-out verified; files are `*_flat.xlsx` (models/ is gitignored — regenerate locally) |
| `scripts/write_brazil_soy_monthly.py` | Brazil soy **monthly block** merged (ABIOVE crush/prod/stocks + comexstat exports + derived pre-2025 prod); MY convention + vintage layering verified |
| `scripts/write_balance_sheet.py` | **Brazil soy complex CLOSED** (soy/meal/oil), Excel-recalc verified: ending computed = reported, TIE=0, nontriv>0, 0 errors. TARGETS wired for all 10 staged cells |
| `flat_file_contract_v1.md` v1.1 | mirror-tab pattern, exact `IF(COUNTIFS=0,"",SUMIFS(.,_xlfn.MAXIFS(.)))` idiom, annual variant, `_flat.xlsx` naming, non-triviality cell |
| Notion "SPRINT — Veg-Oil Balance Sheets" (`3a9ead02…`) | Desktop brief + all 6 bounce answers |

## ▶ Next session — work straight down the list

**Fastest visible progress: annual-close the other 9 staged cells.** Stage 3 needs only their
`*_flat.xlsx` (already written). For each key, generate + recalc-verify:

```
argentina-soy · eu-rape · canada-rape · australia-rape · russia-rape
ukraine-sun · russia-sun · argentina-sun · malaysia-palm · indonesia-palm
   python scripts/write_balance_sheet.py <key>     # then win32com recalc: TIE=0, nontriv>0, 0 errors
```
When a cell verifies, add its `(complex, country)` to `VERIFIED_CLOSED` in
`build_coverage_matrix_html.py` and regen — the coverage cell flips green.

**Then monthly enrichment, per country (this is the real per-source work):**
- Each country needs its monthly national source wired (like `write_brazil_soy_monthly.py`).
  Availability varies wildly — **verify per source, do not assume**:
  - Brazil soy: DONE (ABIOVE in DB).
  - **MPOB (Malaysia palm): a REBUILD.** `bronze.mpob_industry_overview` is annual-only (docx). The
    monthly collector `src/agents/collectors/asia/mpob_collector.py` has **stale 404 URLs** (MPOB
    moved to `bepi.mpob.gov.my/index.php/<cat>/<id>-<slug>-<year>`) and the tables are **JS-rendered**
    (static fetch returns nav only). Needs the AJAX/JSON endpoint or a browser-render collector
    (`claude-in-chrome`) → `bronze.mpob_monthly` → a `write_malaysia_palm_monthly.py`.
  - StatCan (canola), Eurostat/FEDIOL (EU rape), CIARA-CEC (Argentina) — one collector each, unbuilt.
  - Ukraine/Russia sunflower: monthly **trade** exists via customs, monthly **production** is thin —
    seasonalize, don't fake it.

## Decisions locked this session (don't relitigate)

- **Claude-Code builds the templates directly** (the balance-sheet generator), NOT Desktop. Desktop
  can't see the filesystem here, the wiring is mechanical, and generating it in code makes the whole
  chain a scripted, verifiable pipeline. Desktop's lane = the report/forecast-narrative layer where
  judgment matters. (Tore reversed the earlier "enable Desktop's filesystem" pick after seeing the
  generator work.)
- **Monthly blocks are the deliverable; annual PSD is only the control total they rake to.** An
  earlier detour built annual PSD files as if they were the product — they are not. Keep them as the
  tie-out anchor.
- **Naming:** `_flat.xlsx` = generated PSD-annual · `_supply_demand.xlsx` = curated multi-source (US
  reference) · `_balance_sheet.xlsx` = model. Never overload.

## Known-broken / unverified — do NOT assume

- [ ] **`MAXIFS` needs the `_xlfn.` prefix** in openpyxl-written formulas or Excel silently returns 0
  (an empty sheet that PASSES tie-out — the non-triviality cell is what catches it). Already fixed in
  `write_balance_sheet.py`; if you write new formula strings, remember it.
- [ ] **The generated sheets are ANNUAL-closed only.** Brazil soy monthly block exists in the flat
  file but the sheet doesn't consume the monthly rows yet (annual variant). Monthly sheet = add 12
  month rows per series, change the `E`/`F` filters, populate the `ff_` mirror from the monthly rows.
- [ ] **Brazil monthly trade gap:** `bronze.comexstat_trade` is thin (2025-26, exports only, no
  imports). Tore is digging on BZ trade data. Closing it = backfilling the comexstat collector. The
  monthly block's oil production sums to ~95% of the annual control — **the rake step (force monthly
  Σ = annual) is not yet applied.**
- [ ] **models/ is gitignored** — the `.xlsx` files (flat + sheets) live only on the local
  filesystem. Only the scripts are in git. Regenerate flat files + sheets locally before relying on
  them; the two Claudes share via the local tree, not commits.
- [ ] **Coverage matrix still shows Brazil soy as "building" not "done"** — deliberately not marked
  VERIFIED_CLOSED, because "done" in Tore's model means the monthly block closed, not annual-only.
  Decide with Tore whether annual-closed earns green or needs its own state.
- [ ] **Corn oil (Brazil) has no PSD data** (0 rows) — the whole corn-oil complex is deferred; needs a
  derived source. Palm from PSD is CPO+PKO oil S&D only (no kernel-seed/PKC-meal/plantation).
- [ ] The `write_balance_sheet.py` TARGETS for the other 9 cells are **wired but not yet run/verified**
  — generate + recalc each before trusting it.
