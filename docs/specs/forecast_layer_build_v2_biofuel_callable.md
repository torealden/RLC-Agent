# Forecast Layer — Build v2: first D5 forecast callable (ledger 6c)

**Status:** BUILT + verified (Excel recalc), 2026-07-24. Implements design D5 (the first mechanical
forecast callable) on top of the 6b storage/vocabulary.
**Owner:** Claude Code.
**Depends on:** `forecast_layer_build_v1.md` (6b: `core.forecast_run` + band CHECK + guards),
`write_oils_supply_flat_files.py`, migration 153.

This is the first *real entry in the forecast book*. 6b built the empty vault (storage, gate, guards);
6c puts the first forecast in it and wires it to a balance sheet.

---

## 1. What shipped

| # | Artifact | Where |
|---|---|---|
| 153 | `silver.soybean_oil_series` — 2nd book-(b) home; mirrors `wheat_series` + D4 band CHECK + `run_id` FK | migration |
| — | `biofuel_feedstock_use_forecast` — pure `(data, assumptions)` D5 callable | `src/kg/callables/` |
| — | `run_biofuel_feedstock_use_forecast.py` — logs `forecast_run`, writes banded rows, guard | `scripts/` |
| — | `retained_forecast_series()` merge + `_meta` band note | `write_oils_supply_flat_files.py` |

### The callable (D5)

`forward_sbo_lb[fuel, month] = fuel_production_gal[fuel, month] × intensity[fuel]`, where
`intensity[fuel] = Σ_trailing12 SBO_lb / Σ_trailing12 fuel_gal` (lb SBO per gal, bundling yield × mix).
This is the ruled feedstock method (`project_feedstock_forecast_method`, fuel-prod × mix). A flat
trailing ratio = a pure **MODEL_BASE (rank 1)**: zero judgment, reproducible from inputs. It does NOT
model SBO's slow mix-share erosion — that's Tore's `MODEL_ADJUSTED(6)` to add, and the gap is the
reconciliation signal (`project_symbiotic_forecasting`).

- **Trailing intensities (2025-05..2026-04):** biodiesel 5.81, RD 1.92, SAF 0.55 lb SBO/gal — sane
  (biodiesel soy-heavy, RD tallow/UCO-heavy, SAF nascent). Intensity is **unconditional** (total SBO ÷
  total fuel gal over the fuel's SBO-live window; a producing month with no SBO counts as 0), so an
  intermittent user like SAF isn't inflated by conditioning on SBO>0 months.
- **Fuel-prod gap fill:** `silver.fuel_production_forecast` covers the horizon unevenly (RD complete;
  biodiesel missing Jun–Oct 2026; SAF/coproc thin). Missing forward months are seasonal-projected from
  that fuel's own history (mean of last 3 complete-year totals × mean monthly share) — the house method.
  Each month records its route (`fuel_production_forecast` vs `seasonal_projected`) in the diagnostics.
- **Bands (D4):** `value_low/high` = trailing monthly-intensity dispersion (p10/p90) × forward gal,
  clamped to bracket the point. A **scenario** interval, not a confidence level. Total band = comonotone
  sum of per-fuel bands.
- **Purity:** core `forecast(data, assumptions)` has no `now()`, no DB; horizon + trailing window derive
  from assumptions alone. `load_data()` / `run()` are the impure shell.
- **Horizon:** 2026-05 .. **2028-09** (29 months), set **co-terminal with the non-bio forecast frontier**
  so biofuel_use and non_biofuel_use share one forward edge. (The 6b handoff's "~17 mo / Sep-2027"
  under-scoped it; the real forward demand range is 29 months.)

### The publish path (proved end-to-end)

`run_biofuel_feedstock_use_forecast.py`: `callable.run()` → INSERT `core.forecast_run` (retain gate +
`assumptions jsonb` + `target_keys`) → replace this callable's rank-1 rows in `silver.soybean_oil_series`
carrying the new `run_id` → D7 collision guard → prove the `row → run` join. `--scenario` logs the run
with `retain=false` and writes **no** series rows. **116 rows published** (29 mo × 4 series:
biodiesel/RD/SAF/total), guard clean, `run_id` provenance verified.

---

## 2. Verification (Excel recalc, win32com, closed without saving)

Opened `us_soybean_complex_bal_sheets.xlsm` with links updated to the rewritten flat file, full rebuild,
counted `#VALUE!`/error cells by region via `SpecialCells(xlErrors)`:

| Region | errors after |
|---|---|
| **biofuel monthly blocks (r99–177)** | **0** |
| **non-bio + components (r179–305)** | **0** |
| supply monthly blocks (r30–98) | 225 |
| annual block (r6–29) | 155 |
| domestic-use total (r306–336) | 244 |

Forward biofuel now computes real values: biodiesel MY-totals 7,652 / 8,428 / 7,278 (MY2025/26–2027/28);
**Total BBD Use annual = 14,004 (2026/27), 13,099 (2027/28)** — previously **0**. Positional alignment
confirmed exact: balance-sheet static refs `$B$3/$B$19/$B$35/$B$51` land on the wide-tab biodiesel/RD/
coproc/SAF blocks (Oct-rows 3/19/35/51); annual r14/15/17 pull the monthly MY-total rows (r129/145/113).

---

## 3. ⚠️ Correction to the task premise — the "495 forward #VALUE!" was misattributed

**The forward `#VALUE!` in the soyoil balance sheet were NOT the biofuel gap.** The biofuel forward cells
are `=IF([5]…wide!$AL$3="",0,…)` — IF-guarded, so an empty forward source resolved to **0** (silently
wrong), never `#VALUE!`. The `#VALUE!` come entirely from the **un-forecast SUPPLY side**:
`AL11 Production = AL49 → #DIV/0!`, `AL10 Beginning Stocks = AK29 → #VALUE!`, cascading through Total
Supply (r13) → Ending Stocks (r29) → Total Demand (r28) → the domestic-use residual (r306–336).

So this callable did the right, scoped thing: it replaced the forward biofuel **silent zeros** with real,
banded, provenance-tracked values (BBD Use 14,004/13,099 vs 0) and cleared **all** biofuel + non-bio
demand-block errors (0/0). It did **not** clear the 624 remaining errors, because those need a
**soybean-oil SUPPLY forecast** (production via crush × oil-yield, plus beginning/ending stocks) — a
separate series this callable was never scoped to touch. The handoff/design claim "495 = the biofuel gap"
was wrong; verified by recalc, not assumed.

---

## 4. Not verified / known gaps — next session opens against these

- **[ ] The soyoil balance sheet still can't close forward.** 624 supply-side `#VALUE!` remain (production,
  stocks, and the balance identities). The obvious next D5 callable: **soybean-oil production/stocks
  forecast** (crush × oil yield → production; stocks roll-forward). Only then does the sheet close forward.
- **[ ] Excel recalc was a manual win32com pass**, not wired into any build/CI. The flat file is correct on
  disk; the balance-sheet cached values update only when Tore opens it (links refresh) — expected for the
  external-link contract, but the "0 biofuel errors" result lives in this doc, not in a standing check.
- **[ ] `MODEL_ADJUSTED(6)` never exercised.** Only `MODEL_BASE(1)` is published. Tore's judged mix-drift
  overlay is the intended rank-6 companion; not built.
- **[ ] Bands are a mechanical scenario interval** (intensity dispersion), documented as such in `_meta`
  and the series comment. No per-series confidence-level convention beyond that.
- **[ ] Callable not registered in `core.kg_callable`.** It's invoked via its runner, not the MCP
  invoker. Registering it (like `implied_feedstock_value`, mig 092) is a small follow-up if MCP exposure
  is wanted.
- **[ ] SAF forward is thin.** SAF has almost no forward fuel-production forecast, so 16 of its 29 months
  are seasonal-projected off a short ramping history — small in SBO terms (~1–2% of total) but the least
  certain slice. Flagged per-month in the run's diagnostics.
- **[ ] Migration tracker:** 153 applied + recorded via single-version `apply`. Blanket `apply` still
  unsafe (tracker behind on pre-150 migrations), unchanged from 6b.
