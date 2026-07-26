# RLC Spreadsheet Model Completion — Master Plan v1

> **Purpose.** The framing ("the drawings") for the rest of the RLC balance-sheet and
> price-forecast build. Answers three questions Tore posed 2026-07-26:
> 1. What does the Helios/Pepsi report actually need? → **Part A** (report outline, north star).
> 2. How do we build a full country the *same way every time*, no shortcuts? → **Part B** (the SOP).
> 3. What is the full list of country×commodity sheets + prices to finish, and in what order? →
>    **Part C** (master matrix) + **Part D** (phased finish plan).
>
> **The governing ruling this session (Tore, 2026-07-26):** build as if there is no deadline, no
> shortcuts. Sequence so the Helios/Pepsi set lands first. **Fundamentals for every country×commodity
> come first; prices are a single final pass across all combinations.** Prices are done *last*.
>
> Companion docs — do not duplicate, cross-reference:
> `helios_pepsi_deliverables_v1.md` (scope inventory, safe to paraphrase externally),
> `helios_pepsi_spreadsheet_gameplan_internal.md` (method + tiering, INTERNAL),
> `forecast_layer_design_v1.md` / `_build_v1.md` (the forecast storage + callable layer).

**Status:** v1 · drafted 2026-07-26 · supersedes nothing (new synthesis layer above the two Helios specs)

---

## 0. The one idea that orders everything

**The report defines the data; the data does not define the report.** We spent the US build
discovering the sheet set by working backward from numbers we wanted. We are not going to do that
eleven more times. Instead:

1. Fix the **report outline** (Part A) as the requirements spec. Every section names the fundamental
   series it consumes.
2. Fix the **build SOP** (Part B) so that "build country X, complex Y" is a procedure with inputs,
   steps, outputs, and tie-out checks — not a fumble.
3. Walk the **matrix** (Part C) in **priority order** (Part D), running the SOP once per cell.
4. **Prices last.** The guidance-price layer is a cross-cutting pass over *already-closed*
   fundamentals, run once at the end across every combination. Building it per-country as we go would
   force us to re-touch every sheet when the method firms up. One pass, at the end.

Why prices last is correct, not just convenient: the price layer's hard part (Block C — basis to the
quoted reference series) is the least-built thing we have and is **shared machinery** across
complexes. It wants to be designed once against a full set of closed balance sheets, back-tested
once, and applied uniformly — not reinvented 12 times against half-built fundamentals.

---

## Part A — The Helios/Pepsi Weekly Veg-Oils Report (full outline, deadline-free)

This is the finished product the sheets feed. Built to the SOW/sample, not to a Tuesday. Five oil
complexes: **palm, rapeseed/canola, sunflower, soybean oil, corn oil.** Every section is annotated
with **⟵ its data dependency** so it doubles as the requirements list for Part C.

### A0. Cover + Executive Summary
- One-paragraph market call across the five complexes; the week's single most important change.
- ⟵ *No new data — synthesis of the per-complex modules below.*

### A1. The Signal Board (all complexes at a glance)
A single table, one row per complex, columns:
`Complex | Reference series | Decision-Window guidance price | Market level | Signal (below/at/above) | Δ vs last week`
- **Signal is asymmetric** (Pepsi buys, never sells): market **below** guidance → extend coverage;
  **above** → hold to playbook. Decision support, never a transact recommendation (§10.4/§11.5).
- ⟵ Guidance price (A3 per complex); **market level is a Helios/Pepsi-supplied input**, not RLC-
  published (citation constraint — three of five series are private assessments).

### A2. Per-Complex Module — Fundamentals (×5)
For each complex, the fundamental support that *justifies* the guidance number:
- World S&D snapshot: production, exports, ending stocks, **stocks-to-use**, YoY deltas. ⟵ *Tier-C
  world rollup from `bronze.fas_psd`.*
- Key-origin detail: the exporters that set the series price (e.g. MY/ID for palm; UA/RU/AR for sun).
  ⟵ *Tier-A country S&D sheets (Part C).*
- Structural driver of the month (e.g. Indonesia B50 domestic draw; Black Sea harvest; RD feedstock
  pull on SBO). ⟵ *Origin sheets + the feedstock/IFV layer for the biofuel draw.*

### A3. Per-Complex Module — Guidance Price at the Decision Window (×5)
- The **guidance price**: RLC's estimate of where the series trades over the ~8-month (±2) Decision
  Window — **never spot** (the single most important spec, §4.1). Respects each series' native
  quoting convention (rape 12–13 mo forward, sun 6–7 mo).
- **Base forecast + 50% and 90% confidence bands.**
- ⟵ *The guidance-price layer (Part D final pass): fundamental S/U → price mapping → basis to the
  quoted series → forward-window shift. Does not exist yet.*

### A4. Cross-Complex Substitution View
- The one view that makes the knock-on scenario real: when palm gets expensive, India buys sunoil.
  Total veg-oil import demand for the swing buyers, allocated across palm/sun/rape/soy on relative
  price.
- ⟵ *Tier-B importer workbooks (CN, IN, EU, TR) with the shared allocation tab.*

### A5. Scenario Section (the three contracted models, §4.3)
1. **Supply shock** (production/weather/logistics, by origin).
2. **Trade policy** (export restrictions, duties, levies, mandate changes).
3. **Macro / energy** (energy-complex linkage, currency).
- Run individually first; **design the data contract for stacking now**, ship stacking later.
- Start from a **finite curated event list**, show Pepsi, expand on their reaction.
- ⟵ *World + origin fundamentals + shock-sensitivity coefficients (Tier-D stubs carry these).*
- **Honest flag:** this is milestone 3 (30% weight) and the hardest single block. Its *structure*
  can be designed off world-level fundamentals in parallel with the country build; only its
  *calibration* needs finished sheets.

### A6. Budget / NCI Planning Table (D3, monthly)
- Guidance-vs-budget tracking for palm, rapeseed, sunflower, soybean oil (**not corn oil**).
- Absent Pepsi budget inputs, ship **guidance-only** — do not let the table stall on a Pepsi
  dependency.
- ⟵ *A3 guidance prices + (optional) Pepsi budget figures via Helios.*

### A7. "What We're Watching" + Notable Developments by Origin
- Forward calendar of the decisive signals per complex (harvests, WASDE/MPOB/CONAB releases, policy).
- ⟵ *Origin sheets + the data-release calendar.*

### A8. Methodology / Citation Footer
- Public-facing citations limited to **government + exchange sources** (§9). Private assessments
  (Fastmarkets/Argus/Platts) are **never republished**; RLC publishes only its guidance number + the
  delta to a market level Helios/Pepsi owns.
- ⟵ *Provenance from the flat-file `_meta` tabs + vintage-rank ladder.*

### The D2 companion data file
Machine-readable twin of the report: guidance prices, reference-series identifiers, forward-window
IDs, signal indicators, per complex. **Schema must put the market-level column as a Helios/Pepsi
input, not an RLC-published field.** Design it this way now; retrofitting later is a rewrite.

---

## Part B — The Country × Complex Build SOP (the repeatable recipe)

The formalization Tore asked for: turn "we fumbled the US sheets into existence" into a procedure.
**One pass = one country × one complex, fundamentals only, forecast-closed, tied out. It stops
before price.** Follow the pattern already working for US oils
(`scripts/write_oils_supply_flat_files.py` → `models/Oilseeds/United States/`). Do not invent a
second mechanism.

### B0. Inputs (gather before building)
| Input | US example | Non-US source pattern |
|---|---|---|
| Global S&D anchor | `bronze.fas_psd` | same — PSD covers every country/commodity |
| National statistical detail | NASS, Census | CONAB/ABIOVE (BR), StatCan/AAFC (CA), Eurostat/EC (EU), MPOB/GAPKI (palm), INDEC (AR), national bureaus; **USDA-FAS attaché GAIN reports as the universal fallback** |
| Trade | Census (HS/Schedule B) | Comex (BR), Eurostat Comext, national customs; PSD trade as fallback |
| Processing / crush | NASS crush, Fats & Oils | national crush stats where published; else derive from apparent (supply − exports − Δstocks) |

### B1. Scaffold the workbook set for the complex
Use the **complex sheet template** — the sheet set is a property of the *complex*, not the country:

| Complex type | Sheet set | Notes |
|---|---|---|
| **Oilseed crush complex** (soy, canola, sun, cottonseed, peanut, flax) | Seed S&D · Crush · Oil S&D · Meal S&D · Trade · Stocks | the US model; 5–6 tabs |
| **Palm (lauric)** | Plantation (area/yield) · Kernel S&D · Crush · CPO Oil S&D · PKO Oil S&D · PK Meal S&D · Trade · Stocks | **8 sheets — the biggest of the five**, two oils. See `palm_lauric_balance_sheet_template.md` |
| **Corn oil (derived)** | Oil supply (from DCO / wet-mill yield) · Trade | 2 light tabs; no crush complex of its own |
| **Grain** (corn, wheat, sorghum, barley, rice) | S&D · Trade (+ class tabs for wheat) | 1 S&D tab per class + trade |
| **Fat / grease** (tallow, UCO/YG, CWG, lard, poultry) | Supply (slaughter/collection-derived) · Trade · Feedstock allocation · Stocks | US model in `models/Fats and Greases/` |
| **Biofuel / fuel** | Production · Trade · Consumption/blend · Stocks | EIA-driven; US model in `models/Biofuels/` |

Importers get the **reduced set** (Oil S&D + Trade + Stocks only) — nobody outside the tropics grows
oil palm, and a soybean *importer* needs no crush sheet.

### B2. Write the flat files (machine-written, never hand-edited)
- One `write_<complex>_flat_files.py` per complex, following `write_oils_supply_flat_files.py`.
- Conventions (non-negotiable — see `reference_xlsx_flat_file_conventions.md`): rows **ascending**
  (latest at a stable bottom), row 1 group headers / row 2 series / row 3 units / row 4+ dates in
  col A, **raw source units**, a `_meta` tab carrying provenance.
- **Rule 1: the analyst never types in a flat file.** Judgment lives in the workbook layer only. A
  hand-edit is silently reverted on the next regeneration → we ship a wrong number.

### B3. Wire the balance-sheet workbook to the flat files
- SUMIFS/MAXIFS against the flat file, binding by **explicit column letter** (no Excel tables / no
  defined names — appends must not shift the contract).
- **Vintage-rank ladder** (`reference_vintage_rank_ladder.md`): `MAXIFS(vintage_rank)` picks the
  best-available value per cell. Forecast rows (1–9) are the **floor**; any real survey
  (WINTER_SEEDINGS 10 … FINAL 90 … ACTUAL 99) outranks them, so the sheet auto-upgrades with **no
  formula change** as actuals arrive.

### B4. Build the crush linkage (crush complexes only)
- Seed → Oil + Meal through `crush × extraction/yield`. Oil yield and meal yield are per-complex
  constants (fit from history), entered once and carried.
- This is what makes it a *complex* and not three unlinked sheets.

### B5. Close the identities on every tab (bake the check into the sheet)
```
Beginning Stocks + Production + Imports = Total Supply
Domestic Use + Exports                  = Total Distribution
Total Supply − Total Distribution       = Ending Stocks
```
- A visible **tie-out cell** per tab (should read 0). Not a mental check — a cell, or a Python
  assertion in the verify step. Copy the binding per-month tie-out pattern from
  `scripts/rake_feedstock_vintage_aware.py`.

### B6. Close the trade loop at the complex level
- Trade matrix: **Σ country exports ≈ Σ country imports** for the complex. The residual is the world
  statistical discrepancy — **report it, do not hide it.** A clean grand total can still hide
  offsetting errors (per the verify-before-asserting rule).

### B7. Forecast the forward hole (quantity callables → banded rows)
- Each forward series gets a **forecast callable** feeding `silver.<commodity>_series` (banded rows,
  D4 hard-band gate) → flat file → sheet. Clone the working ones:
  `soybean_oil_production_forecast` (production = crush × yield),
  `biofuel_feedstock_use_forecast` (demand-side), and the meal analogues queued at ledger 6g–6i.
- Crush is anchored to Tore's judged annuals; the callable distributes to months on observed yield +
  seasonality. **This is book (b), mechanical** — kept structurally separate from the LLM book
  (`core.forecasts`) so the human-vs-LLM comparison can't be self-referential
  (`project_symbiotic_forecasting`).

### B8. Verify (the gate — nothing ships un-recalced)
- win32com recalc: **0 `#VALUE!` / `#DIV/0!` in history**; forward cells **banded, not blank-or-0**
  (a zero-filled forward hole silently mis-states stocks — the trap caught at ledger 6c).
- Per-month identity tie-out passes; series is **contiguous** (no month gaps — the "175 months with
  a 48-month hole" failure); range guards pass; MAXIFS-collision guard clean (`src/forecast/guards.py`).
- Write the **not-verified list** into the session artifact. That is the deliverable's honesty
  surface.

### B9. STOP — do not build price here
Price is the Part-D final pass. B-output is a **closed fundamental S&D workbook set**: forecast to
the horizon, tied out, machine-refreshable, with the price layer deliberately absent.

### Standing hazards (the ways this rots — from hard-won US experience)
- **Never write to an open workbook** (`~$` lock files corrupted the crush workbook at 6e).
- **Country folders are canonical**; kill bare complex-root duplicates before Desktop links against a
  stale copy (`models/Archive/stale_united_states_dup_20260713` is the scar).
- **Migrations kill builds** — a folder move breaks tasks/.bat/config; do the full migration pass.

---

## Part C — The Master Matrix (every country × commodity fundamental S&D)

Organized by **complex → country tier**. Tiering generalizes the oils gameplan to all commodities:

- **Tier A — price-setting exporters**: full sheet set, no shortcuts. These determine the series.
- **Tier B — swing importers/consumers**: reduced set (Oil/Grain S&D + Trade + Stocks); for veg oils,
  consolidated into one workbook per country with a shared allocation tab.
- **Tier C — world rollup**: automated straight from `bronze.fas_psd`, no manual build.
- **Tier D — scenario-only origins**: single-page stub (production, trade, shock coefficient).

**Status legend:** ✅ built & closed · 🟡 workbook exists, completeness unverified · 🟨 partial ·
⬜ scaffold/empty · ⚙️ automated-from-DB.

### C1. Veg-Oil Complexes — the Helios/Pepsi P1 set
Counts from the 2026-07-21 tracker run (`build_pepsi_coverage_tracker.py`).

| Complex | Tier A (full set) | Tier B (importers) | Tier C | Tier D stubs |
|---|---|---|---|---|
| **Soybean oil** | US ✅ · Brazil 🟨 · Argentina ⬜ | (shared below) | ⚙️ | MX |
| **Rapeseed/canola** | EU ⬜ · Canada ⬜ · Australia ⬜ | (shared) | ⚙️ | RU · TR · BR |
| **Sunflower** | Ukraine ⬜ · Russia ⬜ · Argentina ⬜ | (shared) | ⚙️ | Colombia |
| **Palm (8-sheet)** | Malaysia ⬜ · Indonesia ⬜ | (shared, CPO/PKO only) | ⚙️ | CO · GT · MX |
| **Corn oil (derived)** | US-DCO ✅ · Brazil ⬜ | — | ⚙️ | MX |
| **Shared importers (Tier B)** | — | China · India · EU · Turkey (⬜, 1 wkbk each, allocation tab) | — | — |

**Tier-A country×complex builds: 14, of which 2 done (US soy, US DCO) → 12 to build.**
**Tier-B: 4 importer workbooks. Tier-C: 5 automated rollups. Tier-D: 9 stubs.**
Bare-bones ≈ **123 grid cells** for the five complexes (≈140 with Tier-B consolidation), +112 for
the full Tier-E loop fill if ever built. *(Palm is the largest single build — two oils, 8 sheets.)*

### C2. Oilseed complexes beyond the Pepsi three (P2 — complete the veg-oil franchise)
US built (🟡, workbooks present): cottonseed, peanut, flaxseed, safflower, coconut/lauric, mustard.
Global replication of the *material* ones only:
- **Cottonseed** — US 🟡, + China, India, Pakistan, Brazil (Tier A).
- **Peanut** — US 🟡, + China, India, Argentina.
- **Coconut/copra (lauric)** — Philippines, Indonesia, India (Tier A); ties to palm-kernel lauric.
- Flax/linseed, safflower, mustard, sesame, olive — **minor**; Tier-C world rollup + US only unless a
  client needs them.

### C3. Grains (P3 — the other big price complex; also the SOW No. 2 grains scope)
| Commodity | Tier A exporters | Tier B importers/consumers | Status |
|---|---|---|---|
| **Corn** | US 🟡 · Brazil · Argentina · Ukraine | China · EU · Mexico · Japan | US only |
| **Wheat** (SRW/HRW/HRS/white/durum classes) | US 🟡 · Russia · EU · Canada · Australia · Argentina · Ukraine | Egypt · MENA · Mexico · Indonesia | US classes 🟡 |
| **Sorghum / Barley** | US · Argentina · EU · Australia | China (sorghum) | US only |
| **Rice** | India · Thailand · Vietnam · US · Pakistan | (many, thin margins) | not started |
- Wheat classes that matter to Pepsi (if grains proceed): **SRW, HRW, EU soft milling** — all
  exchange-quoted, therefore *easier* on the citation constraint than the oils. Durum excluded.

### C4. Fats & Greases + Biofuels (P4 — RLC's differentiator, the BBD franchise)
US is the deepest build here (`models/Fats and Greases/`, `models/Biofuels/` — 17 + 22 workbooks) and
is the moat Helios can't replicate. Global extension where a real market exists:
- **Fats/greases** (tallow, UCO/YG, CWG, lard, poultry, DCO): US ✅-ish · EU · Canada · Brazil · China
  (UCO export origin) · Argentina. Feeds the feedstock allocation + IFV engine.
- **Biofuels** (BD, RD, SAF, ethanol): US ✅-ish · EU · Canada · Brazil · Argentina · Indonesia
  (palm biodiesel — the B50 draw that moves palm price).
- **Petroleum fuels** (P5): gasoline, ULSD/distillate, jet, propane, natural gas — mostly US +
  majors; energy CY not ag MY. US-centric, extend only for the macro/energy scenario.

### C5. Other majors (P6 — priority sugar > cotton > dairy)
Sugar, cotton (lint), dairy. US 🟡 for cotton; sugar/dairy not started. Global build only when a
client mandate pulls them.

### Applicability rule (so the matrix isn't a naive cross-product)
A cell exists **only where the country is materially in that commodity**. Malaysia has no corn sheet;
Russia has no palm sheet. The Tier system *is* the applicability filter: Tier A = you set the price,
Tier B = you swing demand, Tier C = you're in the world rollup and nothing more, Tier D = you only
appear in a scenario. Most of the naive N×M grid is Tier-C-automated or absent.

---

## Part D — The 30,000-ft Finish Plan

Phased. Each phase runs the **Part B SOP** once per matrix cell. **Prices are the final phase**,
across everything.

| Phase | What | Cells | Why here |
|---|---|---|---|
| **P0** | US complete + closed (template country) | done + oilseed forward-close finish (meal 6g–6i, remaining oils) | The template every other country copies. Finish it first so the SOP is proven, not theoretical. |
| **P0.5** | **Automate the 5 Tier-C world rollups** from `bronze.fas_psd` | 5, automated | Cheapest work, highest immediate return: a directional read on all five complexes this week, zero manual build. Do this *before* the country grind. |
| **P1** | **Helios/Pepsi fundamentals** — 12 Tier-A builds + 4 Tier-B importers | ~12 + 4 | The commercial priority. Sequence *within* P1 by when the decision window goes live: **sunflower first** (6–7 mo window is live now, Sept Black Sea harvest is the decisive signal), then palm (B50 draw), then rape, then soy BR/AR + corn oil. |
| **P2** | Complete the global veg-oil franchise (minor oils, remaining producers) | ~20–30 est. | Rounds out the oil book; mostly Tier-C + a few Tier-A. |
| **P3** | **Grains** globally (corn, wheat classes, sorghum, barley, rice) | ~30–40 est. | The other large price complex; unlocks SOW No. 2. Wheat classes are citation-clean. |
| **P4** | **Fats/greases + biofuels** globally (BBD franchise) | ~20–30 est. | RLC's differentiator; extends the feedstock/IFV engine internationally. |
| **P5** | Petroleum fuels + macro/energy | ~10 est. | Feeds the macro/energy scenario; mostly US + majors. |
| **P6** | Other majors (sugar, cotton, dairy) | client-pulled | Build on mandate, not speculatively. |
| **P-FINAL** | **THE PRICE PASS — guidance-price layer across every closed fundamental sheet** | all built combos | Prices last (Tore's ruling). One shared engine, designed + back-tested once, applied uniformly. See below. |

### Run in parallel with P1–P3 (do not serialize these):
- **Scenario-model *structure*** (A5) — designable off world-level fundamentals now; only calibration
  needs finished sheets. Milestone 3 is 30% weight and the hardest block; starting it in week 2 (not
  week 6) is the difference between shipping and not.
- **Report shell + D2 schema** — build the report template (Part A) and the data-file schema (with
  the market-level column owned by Helios/Pepsi) early, so P1 fundamentals flow into a finished frame.

### The P-FINAL price pass — the "price forecasts we need" list
For **every closed fundamental combination**, one guidance-price tab, four blocks (from the internal
gameplan §4):
- **A. Fundamental driver** — world + key-origin S&D, S/U, YoY deltas ⟵ complex rollup.
- **B. Price mapping** — S/U → price, own-complex + substitute-complex terms; **fitted, back-tested.**
- **C. Basis to the quoted series** — model flat price → the actual quote (FOB Dutch, six ports, CIF
  Rotterdam, CBOT BO): freight, FX, quality diffs. **This is where the engagement is won or lost, and
  the least-built thing we have.**
- **D. Forward-window shift** — spot-equivalent → Decision Window (mo. 8 ±2), each series' native
  convention.

**Reference series to forecast (the price targets):**

| Complex/commodity | Reference series | Citation status |
|---|---|---|
| Soybean oil | CBOT BO (nearby + deferred) | ✅ exchange, publishable |
| Rapeseed/canola | RSO FOB Dutch / MATIF rapeseed | 🔒 FOB Dutch private; MATIF publishable |
| Sunflower | Sunoil six ports; FOB Argentina | 🔒 private assessment |
| Palm | CPO CIF Rotterdam; FOB Malaysia (Bursa) | 🔒 Rotterdam private; Bursa publishable |
| Corn oil | **RLC-constructed indicative series** (MX, BR) — no liquid benchmark | RLC-labeled |
| Corn (grain) | CBOT C | ✅ exchange |
| Wheat | CBOT ZW (SRW) · KC KE (HRW) · MATIF EBM (EU soft) | ✅ all exchange |
| Fats/biofuels | **IFV** (implied feedstock value — RLC's own engine) + D4 RIN / LCFS / 45Z stack | RLC-constructed |

For the 🔒 private series: RLC forecasts *to* the series but **publishes only its own guidance number
+ the delta** to a market level Helios/Pepsi supplies. This is a schema decision (D2), not a report
footnote — settle it before P-FINAL.

---

## Open decisions / not-verified

- [ ] **Report "the report" interpretation** — Part A assumes "the report" = the Helios/Pepsi weekly
      veg-oils deliverable (D1). If Tore meant a different report, A re-scopes.
- [ ] **Grains in or out** — grains (P3) are drawn here as full RLC scope, but for *Helios* they are
      **SOW No. 2**, not SOW No. 1. Do not build grain sheets *against the Pepsi engagement* until the
      SOW-2 scope question is answered (`helios_pepsi_deliverables_v1.md` §9). For the RLC vision they
      are in scope regardless.
- [ ] **Counts beyond the oils are estimates**, not derived. The oils counts (12/4/5/9) are from the
      tracker run; P2–P6 cell counts are order-of-magnitude ranges labeled "est." — derive real counts
      when each phase is scoped.
- [ ] **US "🟡 built" ≠ verified closed.** Workbook presence in `models/Oilseeds/United States/` (38
      files) is not proof each is current/tied-out. Only US soy oil + US DCO are ledger-verified
      closed. Audit the rest before treating them as P0-done.
- [ ] **Reference-series citation/redistribution** — the 🔒 private-series problem is real and
      structural; it sets the D2 schema. Ruled direction (ledger): deliver guidance not benchmark
      values; move to public benchmarks where arbitrage of the logistical spread allows; **prices are
      attacked last** anyway.
- [ ] **Price mapping (Block B) is unproven** — "S/U → price" is asserted as fittable/back-testable
      but has not been fitted for any complex. That is genuine model risk sitting in P-FINAL.
```
