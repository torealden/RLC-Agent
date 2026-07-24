# Session 6d — soyoil SUPPLY forecast: findings + build (2026-07-24)

**Status:** BUILT + verified by Excel recalc. Production forecast published to the book; sheet closes
through the 2027/28 forecast frontier. Everything below verified against live code / the .xlsm / the DB.

## BUILD RESULT (verified, win32com recalc, closed without saving)

- `soybean_oil_production_forecast` callable + `run_soybean_oil_production_forecast.py` runner publish
  **28 banded MODEL_BASE(1) `production` rows** to `silver.soybean_oil_series` (2026-06..2028-09),
  run_id provenance, D7 guard clean. `source`-scoped so 6c biofuel rows are untouched.
- Writer: retained-forecast rows now **routed by supply/demand** (`SUPPLY_SERIES`); a `trade_forward_gap`
  bridges the census reporting lag (Jun–Sep 2026 imports/exports, rank-40 placeholder).
- **The soyoil balance sheet now closes forward.** Annual chain computes for AK/AL/AM
  (2025/26–2027/28): Production 30,546 / 31,927 / 33,700; Ending Stocks 359 / 2,381 / 7,082 mil lb.
- Remaining errors: **5 cosmetic `#DIV/0!` per column** (yield rows 56–60, poisoned by the stale `[3]`
  soy-crush link, Jan–May 2026 crush = 0) — they feed **nothing** now (production sources via SUMIFS),
  so the balance closes despite them. Everything else erroring is **AN (2028/29)+**, beyond the
  2028-09 forecast horizon (demand forecasts stop there too). In-scope horizon = clean balance.
- **⚠ The stock build IS the signal:** ending stocks 2,381 → **7,082** mil lb (≈90 days) by 2027/28.
  Your crush anchor (2,700/2,850) outruns the placeholder demand (flat 6c biofuel + non-bio 15,000 +
  exports 1,250). Resolution is on the demand/export side (Aegus domestic-demand models + trade
  matrix exports), not the crush. Also eyeball: AK draws to **359** (≈4 days) — the sheet's monthly
  non-bio block (hardcoded 15,000 seasonalized) vs the annual residual non-bio (17,864 plug) are two
  different non-bio treatments; pre-existing, out of 6d scope, worth reconciling.

---

**Original pre-build findings below** (verified against live code / the .xlsm / the DB, not inferred).

The handoff framed 6d as "build a soybean-oil SUPPLY forecast (production via crush×yield + stocks
roll-forward) → banded MODEL_BASE(1) rows in `silver.soybean_oil_series`." On inspection the shape of
the task changed. This doc is what I found and the decisions we need to settle.

---

## 1. What the balance sheet actually needs forward (verified in the .xlsm)

`soyoil_balance_sheet`, forward columns AL (MY2026/27), AM (MY2027/28). The annual identity:

- `Beginning Stocks (r10) = prior year Ending Stocks (AK29)` — chains automatically.
- `Production (r11) = AL49` = the monthly production block MY-total.
- `Imports (r12) = 350`, `Exports (r27) = 1250` — **hardcoded constants** (fine forward).
- `Total Supply (r13) = beg + prod + imp`.
- `Ending Stocks (r29) = AL336` = the **month-ending stocks block**, which rolls forward by the
  balance identity: `stock[m] = stock[m-1] + production[m] + imports[m] - exports[m] - domestic_use[m]`.
- `Total Demand (r28) = Total Supply - Ending Stocks` (residual); `Non-bio use (r18)` is a residual plug.

**Consequence — two things the handoff got wrong:**

1. **Stocks need no forecast.** Ending stocks are a pure roll-forward identity off production +
   trade + domestic use. Forecasting a stocks *series* and feeding it in would double-model stocks and
   fight the identity. **Supply forecast = production only.**
2. **Production already has an internal crush×yield fallback** in the sheet:
   `IF(flat-file has no production row, THEN AL53_oilyield × soy_balance_sheet!AL58_crush, ELSE SUMIFS(flat file))`.
   So publishing a `production` row into the flat file makes the **SUMIFS branch fire** and sources
   production from our forecast, **bypassing the internal fallback**. Same seam 6c used for biofuel.
   `ff_sbo_supply` is live-linked (`=[5]soybean_oil_supply!…` down all 8001 rows), so a flat-file
   rewrite reaches those forward cells on link-refresh.

---

## 2. The real blocker (not a missing series)

Forward production is `#DIV/0!` because the sheet's internal fallback is poisoned:

- The soybean **seed** sheet pulls monthly crush from a **stale external link** `'[3]NASS Crush'!$D133…`
  that **stops at Dec 2025**. Jan–May 2026 crush cells return **0**, even though NASS crush actuals
  exist in our DB through **May 2026**.
- Soyoil yield `AK56 = AK40_production / soy!AK61_crush = prod/0 = #DIV/0!` for Jan–May 2026.
- Those poison `SUM(AG56:AK56)` in the forward yield seasonalization → forward production `AL40–48` →
  `AL49` → Total Supply → Ending Stocks → the ~624 supply-side `#VALUE!`.

Plus: forward crush is a **hardcoded analyst annual** (2620 / 2700 / 2850 mil bu), not a model.

**The forecast-layer path sidesteps both**: publish a modeled `production` series → SUMIFS branch →
the stale-link/hardcode fallback is never evaluated for forward months.

---

## 3. Your demand-pull crush model — validated on history

Model: `crush = w·(oil_disappearance/oil_yield) + (1-w)·(meal_disappearance/meal_yield)`,
`w = oil_value_share = (oil_px·oil_yld)/(oil_px·oil_yld + meal_px·meal_yld)`, gated to a stock target.

Backtested on ERS annual history (MY, 1995–2023 with prices):

- **2015+ MAPE = 0.27%**, mean bias −0.18%. It's essentially an **accounting identity** (crush ≈
  disappearance/yield, because production ≈ disappearance ± small stock change), so it needs **no
  fragile regression** on 44 points across a regime break.
- **The value weight barely moves the crush level**: the oil-implied and meal-implied legs agree
  within ~1% every year (same crush, two views, tied by stocks). `w` matters for **stock allocation**
  (build oil vs meal stocks at the margin), not the crush total.
- Unit landmine noted: ERS meal-yield alternates 95/48 lb/bu (a double-count in some years). With
  clean ~47 lb/bu, `w` ≈ 0.35–0.40 historically (matches your ~35%), rising toward ~0.5 now as RD
  bids up oil.

**Reframing:** the "econometric model for the annual numbers" is really a model of forward **demand**
(chiefly oil biofuel use — +55% since 2018 — and meal disappearance). Crush and oil production fall out
of the identity. That demand side is the "what those each look like in practice" we're deferring.

---

## 4. Initial forward numbers (oil leg only, mechanical biofuel baseline) — the signal

| Oil MY | demand-pull implied crush | your hardcode | oil prod (bn lb) |
|---|---|---|---|
| 2025/26 | ~2,370 | 2,620 | 28.2 |
| 2026/27 | ~2,440 | 2,700 | 29.0 |
| 2027/28 | ~2,364 | 2,850 | 28.1 |

The mechanical crush runs **~10–17% below your hardcodes and the gap widens into 2027/28**, because:

- it uses the **6c biofuel forecast** — a *flat trailing-intensity* baseline that **declines** in
  2027/28 (thin forward fuel-production data). Your 2850 embeds continued RD/biodiesel oil-demand
  growth the mechanical baseline doesn't model.
- it's **oil-leg only** — no meal-demand pull yet, and meal is the larger tonnage; strong meal exports
  would raise implied crush and build oil stocks.

This gap **is** the reconciliation signal (`project_symbiotic_forecasting`): mechanical baseline vs your
judged view = the forward biofuel-and-meal demand-growth story.

---

## 5. Proposed build (for when you're back)

1. **`soybean_oil_production_forecast` callable** (pure `forecast(data, assumptions)`, mirrors 6c):
   forward `production_lb = crush × oil_yield`, `crush` = value-weighted demand-pull, DOC-gated.
   Inputs: 6c biofuel forecast (book) + forward non-bio (seasonal) + forward **meal** demand (seasonal,
   your choice) + trailing yields (unit-converted) + trailing value-weight `w`. Emits `production`,
   MODEL_BASE(1), banded via yield/demand dispersion. Horizon 2026-06..2028-09 (28 mo).
2. **Runner** — clone of `run_biofuel_feedstock_use_forecast.py`: publish to `silver.soybean_oil_series`
   (retain gate, run_id, D7 guard). `source='soybean_oil_production_forecast'` so it never touches
   biofuel rows.
3. **Writer routing fix** — `retained_forecast_series()` currently dumps *all* forecast rows into
   `sbo_demand`; must route SUPPLY series (`production`) into `sbo_supply`. Small split by series name.
4. **No stocks forecast** (§1). No new migration (mig 153 table is general).
5. **Verify** via win32com recalc: production `AL49/AM49` compute, stocks roll forward, annual identity
   closes, 624 → ~0 supply errors.

---

## 6. Decisions you owe me (settled 3 already)

Settled on your walk: value-**value**-share weight; **days-of-coverage** stock gate; **forecast meal
demand seasonally** too.

Still open — these are the "in practice" ones:

- **A. Forward oil biofuel demand.** Use the flat 6c mechanical baseline (gives crush ~2,400, the
  signal above), or overlay your RD/biodiesel growth view (your 2850-implied path) as a
  MODEL_ADJUSTED(6) companion? The whole point is to publish *both* and track the gap — but which is
  the MODEL_BASE(1) the sheet reads?
- **B. Days-of-coverage target** — a fixed target (what DOC?) or trailing-average DOC per product?
- **C. Meal demand baseline** — trailing-3-MY-avg annual × seasonal (like oil non-bio), and where do
  meal **exports** come from (your meal-sheet 20500/21750, or a mechanical projection)?
- **D. The stale `[3]` crush link** feeding the whole complex is a separate rot — worth fixing the
  soybean-seed sheet to pull crush from our DB pipeline (through May 2026) rather than a dead
  workbook, independent of this forecast.
