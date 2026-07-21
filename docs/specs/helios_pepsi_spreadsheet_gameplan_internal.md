# Helios / Pepsi — Spreadsheet Build & Link Game Plan

> **INTERNAL USE ONLY — NOT FOR HELIOS, NOT FOR PEPSI.**
> This document exposes RLC method, build sequence, and where the model is thin. It is the
> counterpart to `helios_pepsi_deliverables_v1.md` (which is the scope inventory and is safe to
> paraphrase externally). Nothing here goes out.

**Drafted:** 2026-07-21 · **Target:** bare-bones spreadsheet complex sufficient to publish a
defensible weekly guidance price for the five contracted reference series.

---

## 0. The design decision that changes everything: build country-major, not complex-major

The existing tracker (`scripts/build_pepsi_coverage_tracker.py`) is organized **complex → country**:
Soybean × 16 countries, Canola × 12, Sunflower × 12 = 40 rows × 5 sheets = **200 sheets to fill.**
That is the full closed loop and it is the right long-run target. It is not a six-week build.

Flip it. The **supply side** is complex-specific — Malaysia does palm, Canada does canola, they
don't overlap. The **demand side is not**: China, India, and the EU appear in every complex, and
their veg-oil import decision is *one substitution-driven allocation*, not four independent ones.
When palm gets expensive, India buys sunoil. Modeling that as four separate country×complex sheets
throws away the only mechanism that makes the cross-commodity knock-on scenario (which we promised
Dominic on 2026-07-13) actually work.

**So:**
- **Exporters** → one workbook per country per complex, full sheet set. Complex-major is fine here.
- **Importers** → **one workbook per country, tab per oil, plus a shared allocation tab** that
  splits total veg-oil import demand across palm/sun/rape/soy on relative price. Build once, serves
  all five complexes and the substitution scenario.

This is the single largest reduction in work available, and it improves the analytics rather than
degrading them.

---

## 1. Scope gaps to close first (blocking)

| # | Gap | Fix |
|---|---|---|
| G1 | Tracker covers **3** complexes (soy, canola, sunflower). SOW covers **5** — **palm and corn oil are missing entirely** | Add both to `MATRIX`/`P1` in `build_pepsi_coverage_tracker.py` |
| G2 | ~~Palm is not a crush complex~~ — **wrong, corrected 2026-07-21.** Palm *is* a full crush complex with **two oils**: FFB→mesocarp gives CPO, and the **kernel is the seed**, crushed into **PKO + palm kernel cake**. The oilseed 5-sheet set is too *small*, not too big | Palm sheet set = **Plantation (immature/mature area, oil yield) · Seed S&D (palm kernel) · Crush · Oil S&D (CPO, industrial vs food/feed/waste split) · Kernel Oil S&D (PKO) · Meal S&D (PKC) · Trade · Stocks** — 8 sheets. Importers get only CPO/PKO/Trade. See `palm_lauric_balance_sheet_template.md` |
| G3 | Corn oil has no reference series and no complex structure | Treat as **derived**: US DCO (already built under the feedstock layer) + BR corn-ethanol DCO + MX wet-mill. 3 light workbooks, not a complex |
| G4 | `build_pepsi_coverage_tracker.py` has **55 uncommitted insertions** from the Jul 20 morning session | Review, finish, commit before touching it again |

---

## 2. Minimum country set per complex ("bare bones")

Tiering is by **whether the country sets the reference-series price**, not by size.

### Tier A — price-setting exporters (full sheet set)
These determine the series we quote. No shortcuts here.

| Complex | Tier A countries | Sheet set |
|---|---|---|
| **Palm** | Malaysia, Indonesia | Plantation · Oil S&D · Trade · Stocks |
| **Sunflower** | Ukraine, Russia, Argentina | Seed S&D · Crush · Oil S&D · Meal S&D · Trade |
| **Rapeseed / canola** | EU, Canada, Australia | Seed S&D · Crush · Oil S&D · Meal S&D · Trade |
| **Soybean oil** | United States ✅, Brazil, Argentina | Seed S&D · Crush · Oil S&D · Meal S&D · Trade |
| **Corn oil** | United States ✅(DCO), Brazil | Derived — Oil supply · Trade |

**12 Tier-A country×complex builds**, of which US soy is done and US DCO is done → **10 to build.**

### Tier B — swing importers (shared country workbook, allocation tab)
One workbook per country. Tabs: `Palm · Sunoil · RSO · SBO · Allocation · Trade`.

| Country | Why | Complexes served |
|---|---|---|
| **China** | Largest veg-oil buyer; crush-driven SBO; rapeseed imports | all 5 |
| **India** | The marginal buyer — sets palm/sun substitution at the margin | palm, sun, SBO |
| **EU** | Importer *and* Tier-A exporter for rapeseed; biodiesel policy draw | all 5 |
| **Turkey** | Sunseed crusher / re-exporter; origin in scope for 2 complexes | sun, rape |

**4 Tier-B workbooks.**

### Tier C — World rollup
Straight from `bronze.fas_psd`, no manual build. One rollup tab per complex, auto-refreshed.
Gives world production, trade, ending stocks, stocks-to-use — the first-order price answer.
**5 rollups, all automated.**

### Tier D — scenario-only origins (stub, no full S&D)
Colombia, Guatemala, Mexico (palm); Colombia (sunflower); Mexico (all five); Russia, Turkey, Brazil
(rapeseed). Single-page: production, trade, and a shock-sensitivity coefficient. Enough to answer a
what-if without carrying a balance sheet.

### The actual counts (from the 2026-07-21 `build_pepsi_coverage_tracker.py` run)

| | Count |
|---|---|
| Tier A country×complex builds | 14, of which **2 already done** (US soy, US corn oil) → **12 to build** |
| Tier B importer workbooks | **4 distinct countries** — China, India, Europe, Turkey (≈22 tabs after consolidation, vs 62 cells built complex-major) |
| Tier C world rollups | **5**, automated from `bronze.fas_psd` |
| Tier D scenario stubs | **9** country×complex |
| **Bare-bones sheet count** | **175 grid cells**, ≈**140** after consolidating Tier B country-major |
| Tier E loop fill (deferred) | +112 → 287 if built |

**Palm is the biggest build of the five, not the smallest** — corrected 2026-07-21 against the
World Lauric Oils template. Palm is a full crush complex with *two* oils (CPO from the mesocarp;
PKO + palm kernel cake from crushing the kernel), so producers carry 8 sheets against the oilseed
complexes' 5. Importers still carry only 3 (CPO, PKO, trade) — nobody outside the tropics grows oil
palm. See `palm_lauric_balance_sheet_template.md`.

**Be honest about what this cut is and isn't.** The old tracker was 200 sheets for **three**
complexes. This is ≈123 for **five** — per complex a real reduction (≈67 → ≈25), but the absolute
number does not collapse, because two contracted complexes were missing entirely before. Reading
"200 → 123" as pure savings is reading it wrong: it is *more scope, less work*, which is the only
honest framing.

---

## 3. Data → spreadsheet → deliverable link chain

Follow the pattern already working for US oils (`scripts/write_oils_supply_flat_files.py` →
`models/Oilseeds/United States/`). Do not invent a second mechanism.

```
bronze.fas_psd                     (global S&D, current to MY2026)
bronze.conab_* / abiove            (Brazil)
bronze.eia_* + feedstock layer     (US biofuel draw, DCO)
MPOB / GAPKI                       (palm — MPOB is §9-citable, GAPKI is not)
        │
        ▼
  flat-file writers  ──────────►  models/<Complex>/<Country>/*.xlsx
  (scripts/write_*_flat_files.py)   rows ASCENDING, latest at stable bottom,
        │                           _meta tab, VLOOKUP-safe
        ▼
  country balance-sheet workbooks (analyst adjustments live HERE, not in the flat file)
        │
        ▼
  complex rollup + TRADE MATRIX  ──►  closes the loop: Σexports ≡ Σimports
        │
        ▼
  guidance-price sheet  (one tab per reference series)
        │  fundamental S&D → price mapping → basis to the quoted series
        │  → forward-window shift to Decision Window (mo. 8 ±2)
        ▼
  D2 weekly data file (XLSX/CSV)  ──►  D1 weekly report
```

**Three rules that keep this from rotting:**
1. **Flat files are machine-written and never hand-edited.** Analyst judgment lives in the workbook
   layer only. The moment someone types over a flat-file cell, the next regeneration silently
   reverts it and we ship a wrong number.
2. **Country folders are canonical** (`Oilseeds\United States`, etc.). Bare copies at the complex
   root are stale — `us_soybean_oil_supply_demand.xlsx` currently exists in *both* places. Kill the
   duplicates before Desktop links against the wrong one.
3. **Never write to an open workbook.** The `~$` lock files sitting in `models/Oilseeds/` and
   `models/Feed Grains/` right now are the failure mode.

---

## 4. The guidance-price layer — the part that doesn't exist yet

Everything above produces **fundamentals**. The deliverable is a **price at a forward window**. That
translation is milestone 4 (15%) and there is currently no spreadsheet for it.

Per reference series, one tab, four blocks:

| Block | Contents | Source |
|---|---|---|
| **A. Fundamental driver** | World + key-origin S&D, stocks-to-use, YoY deltas | complex rollup |
| **B. Price mapping** | S/U → price relationship, own-complex + substitute-complex terms | fitted, back-tested |
| **C. Basis to quoted series** | Model output (flat price / CBOT-equivalent) → the actual quoted basis: FOB Dutch, six ports, CIF Rotterdam. Freight, FX, quality diffs | manual + freight/FX series |
| **D. Forward-window shift** | Spot-equivalent → Decision Window (mo. 8 ±2), respecting each series' native quoting convention (rape 12–13 mo, sun 6–7 mo) | carry/seasonality |

**Block C is where this engagement is won or lost, and it is the least-built thing we have.** The
IFV engine gets us a fundamental fair value. Nobody at Pepsi buys "fundamental fair value" — they
buy RSO FOB Dutch for Feb/Apr. Every unit of error in the basis translation lands directly on the
signal, and the signal is the product.

⚠️ **Citation constraint feeds straight into this.** Three of the five quoted series are private
assessments we cannot republish (see `helios_pepsi_deliverables_v1.md` §5). Design D2's schema now
with the **market-level column owned by Helios/Pepsi as an input**, and RLC publishing only the
guidance number and the delta. Building the sheet the other way and retrofitting it later is a
rewrite.

---

## 5. Six-week sequence

Ordered by **when the decision window goes live**, not by how easy the build is.

| Week | Work | Milestone (§6) |
|---|---|---|
| **1** | Close G1–G4. Regenerate tracker with all 5 complexes + palm's own sheet set. Stand up Tier-C world rollups from `bronze.fas_psd` (all 5, automated). Flat-file writers for non-US countries. | M1 (20%) |
| **2** | **Sunflower Tier A first** — UA, RU, AR. Sunoil quotes only 6–7 months forward, so the Dec/Feb window is *live now* and the September Black Sea harvest is the decisive signal. This is the one series where being late means missing the call entirely. | M1/M2 |
| **3** | **Palm Tier A** — MY, ID, incl. the B50 domestic-draw model (the single largest structural variable in the whole package). Then rapeseed Tier A — EU, CA, AU. | M2 (25%) |
| **4** | Soybean oil Tier A — BR, AR (US done). Corn oil derived series, MX/BR candidates proposed. Tier-B importer workbooks (CN, IN, EU, TR) with the shared allocation tab. | M2/M1 |
| **5** | Trade matrices — close the loop per complex. **Guidance-price layer** (§4 above), all five series, Blocks A–D. Back-test. | M4 (15%) |
| **6** | Scenario models ×3, back-tested and documented. Pilot weekly report + D2 data file. | M3 (30%) + M5 (10%) |

### Honest assessment of this schedule
It does not fit. Milestone 3 — three back-tested scenario models across five complexes, 30% of the
build-out weight — gets one week here, and it is the hardest single block in the engagement. The
balance-sheet work is known-method grind; the scenario models are not.

Two ways out, and one of them should be chosen deliberately rather than discovered in week 6:
- **Start scenario-model design in week 2, in parallel**, off world-level fundamentals, and refine
  as the country detail lands. Scenario *structure* doesn't need finished balance sheets — only
  scenario *calibration* does.
- **Or negotiate the six-week target** (§6 is bracketed `[six (6)] weeks` — still open) to eight,
  before execution rather than after.

Recommend both. The bracket is open; use it.

---

## 6. What we are NOT building (deliberate exclusions)

Write these down so they don't quietly creep back in:

- **Meal balance sheets for palm and corn oil** — no meaningful meal complex.
- **Full S&D for Tier-D scenario origins** — stubs only. Colombia palm does not need a balance sheet
  to answer a what-if.
- **Wheat and corn grain** — **not in SOW No. 1.** Do not start these until the scope question in
  `helios_pepsi_deliverables_v1.md` §9 is answered. If wheat proceeds, note that the classes Pepsi
  actually buys (SRW, HRW, EU soft milling) are all exchange-quoted and therefore *easier* on the
  citation constraint than the oils — but the balance-sheet build is a full additional complex.
- **The buying-playbook layer for sunflower, SBO, and corn oil** — SOW §2 assigns playbook only to
  palm and rapeseed. Don't build machinery we aren't paid for.
- **Rebuilding the scenario layer onto the Helios 0–100 index** during build-out — recalibration is
  a post-pilot workstream. Ship on classical weather inputs first; swap the input layer after the
  index is validated against the historical commentary archive.

---

## 7. Immediate next actions

1. Review and commit the 55 uncommitted lines in `scripts/build_pepsi_coverage_tracker.py`.
2. Extend the tracker to 5 complexes with per-complex sheet sets and the A/B/C/D tiering above.
3. Stand up the five Tier-C world rollups from `bronze.fas_psd` — cheapest work with the highest
   immediate analytical return; gives a directional read on all five complexes this week.
4. Get the wheat scope question in front of Dominic/Francisco. It is a commercial question, and it
   is cheaper to ask than to build.
5. Resolve the reference-series citation constraint in writing during build-out — it sets D2's
   schema.
