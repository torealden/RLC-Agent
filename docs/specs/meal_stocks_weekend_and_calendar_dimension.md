# Meal stocks/use characterization + the weekend calendar dimension

**Session 2026-07-26.** Grounding work before the soybean-**meal** supply/balance forecast callable
(the meal analogue of the 6d soyoil production callable). Tore flagged, before any code, that the meal
balance's forward close turns on the **stocks-to-use** discipline and a **month-end weekend** measurement
artifact. This doc records what the data actually says, and the fundamental date dimension built to carry it.

## 1. What shipped (durable)

- **`reference.month_calendar`** (migration `155_reference_month_calendar.sql`, applied + verified) — a
  pure-calendar MONTH dimension, one row per (year, month), 1990–2050 (732 rows). Carries the
  fundamental fact USDA month-end stock reports need: **which day of week the month ends on**, with flags
  `ends_on_weekend` / `ends_on_saturday` / `ends_on_sunday` (Sunday broken out — that's where the signal
  is), plus quarter-end. Commodity-agnostic (no marketing-year mapping; MY start differs by commodity).
  Any month-indexed series joins on (year, month). 209 weekend-ending months (104 Sun / 105 Sat).

## 2. The stocks / next-month-domestic-use ratio (meal)

Ratio = `meal_stocks[m] / meal_domestic_use[m+1]`. Domestic use derived from the identity
`production + imports − exports − ΔStocks` (NASS monthly meal production/stocks + `gold.soybean_meal_trade`).

**Clean-trade window only (2020-01 → 2026-04, n=76).** `gold.soybean_meal_trade` starts 2020-01; before
that, exports default to 0 → domestic use overstated → ratio understated. Early-year figures are NOT
usable; do not read the pre-2020 trend.

- Weekday-ending months: **mean 0.087, median 0.084**
- Weekend-ending months: **mean 0.100, median 0.095**  → **+15%, +1.30 pp**
- By month-end DOW: **Sun 0.110 (n=11)** is the outlier; Sat 0.090 ≈ weekdays; Thu 0.075 is the low.
- **Trend (RD era):** 0.108 (2021) → 0.088 (2023) → 0.084 (2025) → 0.081 (2026 partial). Stocks/use is
  drifting **down** — consistent with RD-driven crush pushing surplus meal into the export market so
  domestic stocks stay tight relative to use. This is the reconciliation signal, not noise.

**Usable center for the forecast target:** ~**8.5–9% weekday-end months, ~10–11% Sunday-end months**,
declining ~0.5 pp/yr in the RD era. No hard rule — a band with a Sunday term and a downward drift.

## 3. The weekend artifact is meal-specific (the "does it light up elsewhere" test)

Trend-free neighbor test — each month's stock vs the mean of its two neighbors (removes trend+seasonality,
tests numerator inflation directly), joined to `reference.month_calendar`:

| Series (NASS month-end stocks) | weekday-end | weekend-end | Sunday-end | n |
|---|---|---|---|---|
| **Soybean MEAL stocks** | **−6.6%** | **+21.5%** | **+34.1%** | 131 |
| Soybean OIL stocks | +0.0% | −0.3% | +0.9% | 131 |

Meal flares; oil is null. The neighbor test **amplifies** magnitude (an elevated weekend month drags its
neighbors' baseline up, so weekday months read artificially low) — the honest meal effect-size is §2's
**+15% / +1.3 pp**; the neighbor test is the direction/significance proof and the clean oil null.

**Ruling implication:** the calendar flag is fundamental and available to *every* dated series, but the
weekend EFFECT is **series-specific and must be measured, never blanket-applied.** Oil would be a false
positive. Mechanism read: meal is physical plant inventory needing a count (last count = Friday, weekend
drawdown still booked); oil is metered tank storage that doesn't.

## 4. Design for the meal supply/balance forecast (next)

Differs from the oil sheet (which was production-only + roll-forward stocks + constant exports):

- **Production** = `crush × meal_yield` — clean clone of `soybean_oil_production_forecast` (6d), crush
  anchored to Tore's judged annuals (same shared crush driver as oil; meal_yield ~ short-ton/bu from NASS
  `meal_production / crush`). Units: short tons ('000 st in the sheet), NOT lb.
- **Ending stocks** = target ratio × next-month domestic use, with a **Sunday-month uplift term** and the
  RD-era downward drift (§2). This makes stocks a *modeled* line, not a free roll-forward residual.
- **Exports = the plug** = `production + imports + beginning stocks − domestic use − ending stocks`.
  Exports absorb the RD-driven meal surplus (Tore's thesis). This flips oil's convention: oil plugs
  stocks, meal plugs exports against a stocks target.
- **Domestic use (forward)** from the existing **slaughter model** (livestock files). ⚠ NOT YET VERIFIED
  that a slaughter→meal-demand forecast is actually wired — infra exists (`silver.animal_slaughter`,
  `gold.livestock_slaughter_monthly`, `gold.hog_slaughter_monthly`, `models/Fats and Greases/
  us_livestock_slaughter.xlsx`) but the meal-demand link is unconfirmed. This is the blocking dependency
  for the balancing model.

## 5. Known-broken / unverified

- [ ] Slaughter→meal-domestic-demand forecast: existence UNVERIFIED (§4). Check before building the plug.
- [ ] `gold.soybean_meal_trade` starts 2020-01 → no clean monthly domestic use before then. Pre-2020
  stocks/use ratios are biased low; excluded from all numbers above.
- [ ] Neighbor test overstates the weekend magnitude (neighbor contamination). Use §2's ratio figure for
  the model target; the neighbor test is for direction/significance only.
- [ ] Meal production callable + series table (`silver.soybean_meal_series`, clone of mig 153) + writer
  routing NOT yet built — this session is the grounding characterization only.
- [ ] Weekend effect measured on meal + oil only. Other month-end series (canola oil stocks, NASS grain
  stocks) not yet scanned — the flag now makes each a one-line join.
