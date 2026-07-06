# Tallow (& Animal-Fat) Resolution — Code → Desktop initiating brief

**From:** Claude Code (implementation) | **To:** Claude Desktop (design/methodology) | **Owner:** Tore
**Initiated:** 2026-07-06. **Parent:** UCO/YG Ruling Doc + BBD Feedstock System v1.6 LOCKED.
**Pattern:** build tallow the *same way as UCO* — RLC supply build is canonical, EIA disregarded
(Ruling 1). This brief says exactly what Code needs Desktop to rule on.

## 1. Why now — the budget forces it

Ruling 1 made RLC's UCO number canonical: **UCO biofuel 2024 = 8.73B lb**, which alone exceeds EIA's
entire "Yellow Grease" bucket (7.39B). That's defensible on its own, but it breaks the feedstock
budget: RLC UCO (8.73) + EIA veg (soy 11.76 + canola 3.21 + corn 4.49 = 19.46) + EIA tallow (8.65) =
**~36.8B**, vs ~34B total US BBD feedstock (production-constrained). They can't all be right. Tore's
call: EIA is weak for **both** UCO and tallow — it **under-counts UCO and over-counts tallow**. So
RLC tallow should land *below* EIA's 8.65B, and correcting both is what balances the budget to
production. Tallow is therefore not optional polish — it's what closes Ruling 1.

## 2. The driver — "how many cows get killed" (Tore's point, and it's the strength here)

Unlike UCO (a consumer-strength *proxy*), tallow production is a **hard physical number**:
```
domestic_tallow_production(t) = Σ_species ( slaughter_head[s](t) × fat_yield_lb_per_head[s] )
```
Data is already in bronze — `bronze.nass_livestock_slaughter`: cattle/hog/poultry **head + live-
weight (lb/head) + total lb, monthly, 1907–2026**. So tallow should be "close out of the box," and
it's the RLC number we can defend to a room of 300 because it's grounded in slaughter, not a survey.

**The method extends to the whole animal-fat family** (same slaughter driver, different animal):
- **Beef tallow** (edible EBFT + inedible IBFT) ← cattle slaughter
- **Lard / choice white grease (CWG)** ← hog slaughter
- **Poultry fat (PLT)** ← poultry slaughter
Desktop rules whether v1 does tallow only or the full family (recommend: build the engine once,
parameterize by species, ship tallow first).

## 3. Identity — parallels UCO §2, with one real difference (non-bio is large)

```
supply(t)          = domestic_production(t) + imports(t) − exports(t)
tallow_biofuel(t)  = domestic_production(t) + net_imports(t) − non_bio_use(t)
```
- **Ruling 1: tallow_biofuel is RLC-canonical, NOT capped at EIA.** EIA "Tallow" kept only as a
  defendable comparison.
- **Unlike UCO, non_bio_use is LARGE and real for tallow** — feed, oleochemicals, soap, pet food are
  major, long-standing markets. For UCO non-bio collapsed to a floor; for tallow it's a first-class
  modeled quantity, and it's exactly the lever that pulls RLC tallow *below* EIA's 8.65B. This is the
  crux Desktop must design: the biofuel-vs-non-bio split of tallow production.

## 4. What Code needs Desktop to rule on

1. **Fat-yield coefficients** `fat_yield_lb_per_head[species]` (edible + inedible), and whether keyed
   on head or on live-weight (we have both). Calibration anchor(s): render-industry production
   estimates (NRA / Render Magazine / USDA rendering data) — which, and what year.
2. **Edible/inedible split** (EBFT vs IBFT) — a fixed ratio or a driver? Maps 1:1 to the allocator's
   EBFT/IBFT codes.
3. **Non-bio segmentation** — the feed/oleochemical/soap/pet-food split of tallow production, history
   and forecast. This is the number that lands RLC tallow below EIA; how is it anchored (rendering-
   industry use tables? residual to a total-disappearance identity?).
4. **Import scope** — Tore's curated oilseed-trade file (`Edible Tallow`, `Inedible Tallow` sheets,
   authoritative, historical) is the backbone; which Census HS codes are the live pull, both flows
   (US both imports and exports tallow — Brazil/Australia in, various out).
5. **Budget reconciliation** — cross-validate that RLC tallow + RLC UCO + EIA veg ≈ production; report
   the residual. If RLC tallow doesn't fall enough to close it, which lever moves (yields, non-bio,
   or is production higher).
6. **Vintage ladder + flat-file contract** for `us_tallow_supply.xlsx` (mirror the UCO contract:
   `SLAUGHTER_DERIVED` for production, `CENSUS` for trade, `MODEL_FCST` for forecast).

## 5. Forecast + multi-country (mirror UCO §7)

- **US production forecast:** project cattle/hog/poultry slaughter (cattle-cycle / herd-inventory data
  — NASS cattle inventory) × yields → tallow to 2050, monthly.
- **Import leg (multi-country):** exporters' tallow production = their slaughter × yield (Brazil,
  Australia, NZ are the big tallow exporters) → exportable surplus → US imports, validated against the
  oilseed-trade actuals. Same structure as UCO's exporter model; slaughter data per country from FAO.
- **Policy module** (§8 of the UCO doc) composes — tallow has its own eligibility/tariff levers.

## 6. Labor division (dual-Claude, same as UCO)

- **Code:** `silver.animal_slaughter` (tidy cattle/hog/poultry head+weight); ingest the oilseed-trade
  tallow sheets; implement Desktop's yield/split/non-bio methodology → `silver.tallow_balance`; split
  `silver.feedstock_supply` (RLC tallow replaces EIA tallow for EBFT/IBFT); the tallow flat file.
- **Desktop:** the §4 rulings (yields, splits, non-bio, anchors, reconciliation) + the contract + the
  balance-sheet workbook + the forecast method.

## 7. Sequencing note

Tallow and UCO share the feedstock budget, so the single allocator re-run should wait until **both**
are RLC-built and the rake exempts both (veg oils stay EIA-pinned; UCO + tallow become RLC-canonical).
Build order: Desktop rules §4 → Code builds slaughter + tallow identity → reconcile the budget with
UCO → one allocator re-run + re-rake (UCO/tallow exempt) → re-acceptance → emit both flat files.

**What I need to start:** Desktop's §4 rulings (yields, edible/inedible split, non-bio segmentation,
import HS scope, budget-reconciliation lever, contract). The slaughter data and trade file are staged.
