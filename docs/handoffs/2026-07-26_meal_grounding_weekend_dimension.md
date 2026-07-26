# Handoff — 2026-07-26: meal grounding (weekend calendar dimension + stocks/use study)

Read, then **verify before acting.** Full detail: `docs/specs/meal_stocks_weekend_and_calendar_dimension.md`.

This session started by reconciling the two 2026-07-25 handoffs against reality, found both already
done, and — at Tore's direction — did the grounding work for the soybean-**meal** supply/balance
forecast (the meal analogue of the 6d soyoil callable) BEFORE writing the callable.

## 0. First, the reconciliation (both prior handoffs were already complete — verified, not assumed)

- **`2026-07-25_nass_crush_matrix_fix.md`** — its pending action (apply mig 154) was done by session 6e
  (commit `5e0bf795`). Verified live: `gold.nass_soy_crush_matrix` has the `commodity='soybeans'`
  predicate, conversion factors are 0.001 meal / 0.000001 oil, Dec-2025 ties out (6895.963 / 5107.259
  / 2657.399), and `us_soy_crush.xlsm` is filled through **May-2026** with clean dates + correct
  meal/oil. Nothing left.
- **`2026-07-25_aegus_helios_weather.md`** — weather→yield parked by Tore's own ruling (forecast
  framework first). No open mechanical work.

Ledger updated: 6d/6e marked done (they had landed in git but the table still showed open), 6f added.

## 1. What shipped this session (durable, committed)

| Artifact | What | Verified |
|---|---|---|
| mig `155_reference_month_calendar.sql` | `reference.month_calendar` — pure-calendar MONTH dim, 1990–2050 (732 rows), `ends_on_weekend`/`_saturday`/`_sunday` + quarter-end. Join any month-indexed series on (year, month). | Applied; 209 weekend months (104 Sun/105 Sat); spot-checks correct |
| `docs/specs/meal_stocks_weekend_and_calendar_dimension.md` | The characterization + design for the meal forecast | numbers below |

Commits: `0912f3c1` (mig 155 + spec), plus this handoff + ledger.

## 2. The findings (numbers Tore should carry forward)

**Meal stocks / next-month domestic use** (domestic use = `prod + imports − exports − ΔStocks`; NASS
monthly + `gold.soybean_meal_trade`). **Clean 2020+ window only** — trade data starts 2020-01, so
pre-2020 domestic use is overstated (exports=0) and its ratios are biased low. n=76:
- weekday-ending months **0.087** (median 0.084); weekend-ending **0.100** (median 0.095) → **+15%, +1.3 pp**
- **Sunday** is the signal (0.110, n=11); Saturday (0.090) ≈ weekdays.
- **RD-era downtrend:** 0.108 (2021) → 0.084 (2025) → 0.081 (2026 partial). Exports absorbing RD-driven
  meal surplus keeps domestic stocks tight relative to use.
- Forecast target: ~**8.5–9% weekday-end, ~10–11% Sunday-end**, drifting down ~0.5 pp/yr.

**The weekend artifact is MEAL-SPECIFIC** (the "does it light up elsewhere" test). Trend-free neighbor
test (stock vs mean of its two neighbors), joined to `reference.month_calendar`:
- Meal: weekday −6.6%, weekend +21.5%, **Sunday +34.1%** (n=131) — flares.
- Oil: weekday +0.0%, weekend −0.3%, Sunday +0.9% (n=131) — null.
- The neighbor test **overstates** magnitude (neighbor contamination) — use §2's +15% for the model
  target; the neighbor test is direction/significance + the clean oil null only.
- **Ruling:** flag is fundamental/available to all series; the EFFECT must be measured per series,
  never blanket-applied. Mechanism read: meal = physical plant inventory needing a count (last count
  Friday, weekend drawdown still booked); oil = metered tank storage.

## 3. Dependency structure for closing the meal sheet forward (the design)

```
Production (crush × meal_yield)  ──┐   clone of 6d, NO blockers          -> 6g
Domestic use (slaughter model)   ──┼─→ Ending stocks (target + Sunday)   -> 6h feeds 6i
Imports (small, seasonal)        ──┘        Exports = PLUG                -> 6i
```
Meal FLIPS oil's convention: oil plugs stocks + constant exports; meal sets stocks from a target ratio
and **plugs exports** (exports absorb the RD surplus, Tore's thesis).

## 4. Next session — 6g (recommended), the clean ready unit

**Soybean-meal PRODUCTION callable.** `production_st = crush_bu × meal_yield`, crush anchored to Tore's
judged annuals (SAME shared crush driver as the 6d oil callable), meal_yield = trailing mean of NASS
`meal_production / crush` by calendar month (~ short tons/bu). Emit in **SHORT TONS** (not lb — the meal
sheet is '000 st). Clone `src/kg/callables/soybean_oil_production_forecast.py` + its runner; create
`silver.soybean_meal_series` (clone mig 153, band CHECK). Zero blockers, short, banks the supply anchor.

## 5. Known-broken / unverified — do NOT assume

- [ ] **No slaughter→meal-demand forecast exists** — VERIFIED this session. Ingredients only:
  `gold.livestock_slaughter_monthly` (monthly, rich) + `us_protein_meal_consumption.xlsx` (`dom_use`
  tab, quarterly history). The "simple slaughter model" (6h) must be BUILT; it's the blocker for 6i.
- [ ] **No meal flat-file / wide-render / sheet wiring exists.** Oil has `us_soybean_oil_supply_demand.xlsx`
  + `ff_sbo_*` tabs; meal has NONE. The meal balance sheet lives in `us_soybean_complex_bal_sheets.xlsm`
  (tab `soymeal_balance_sheet`). 6i must build the meal analogue of `write_oils_supply_flat_files.py` +
  rewire the sheet — a real chunk, not a formula tweak.
- [ ] `us_soybean_complex_bal_sheets.xlsm` was **OPEN in Excel** this session (`~$` lock present). Do NOT
  write to it programmatically while open — that is exactly how the crush workbook got corrupted (6e).
- [ ] Pre-2020 meal stocks/use ratios are biased (no trade before 2020-01). Excluded from all numbers.
- [ ] Weekend effect measured on meal + oil only. Canola oil stocks, NASS quarterly grain stocks not yet
  scanned — the flag now makes each a one-line join.
- [ ] 6g production leg alone banks DB-side forecast rows but does NOT visibly close the sheet until the
  6i writer/wiring exists. Don't expect a recalc win from 6g in isolation.
