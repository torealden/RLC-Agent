# Session handoff — 2026-07-24 (ledger 6c: first D5 forecast callable)

Read this, then **verify what it claims before acting on it.** Full detail + evidence:
`docs/specs/forecast_layer_build_v2_biofuel_callable.md`. This is the short state + next-opener.

Session 6c built and published the **first real forecast in the book** — the biofuel-feedstock-use
forecast for soybean oil — and, in verifying it, **corrected the premise it was handed.**

---

## 1. What shipped (all verified live / by Excel recalc)

| Artifact | What |
|---|---|
| migration **153** | `silver.soybean_oil_series` — 2nd book-(b) home, mirrors `wheat_series` + D4 band CHECK + `run_id` FK. Applied + recorded. |
| `src/kg/callables/biofuel_feedstock_use_forecast.py` | pure `(data, assumptions)` D5 callable. `fuel_prod_gal × trailing-12mo intensity` → banded `MODEL_BASE`(1) rows. |
| `scripts/run_biofuel_feedstock_use_forecast.py` | logs `core.forecast_run` (retain gate + `assumptions jsonb`), writes 116 banded rows w/ `run_id`, D7 guard. `--scenario` = provenance-only. |
| `scripts/write_oils_supply_flat_files.py` | `retained_forecast_series()` merges published rows into the demand flat file + `_meta` band note. |

**Proved end-to-end:** retain=true → `silver.soybean_oil_series` (116 rows, 29 mo × 4 series) → each row
carries `run_id` → `row → run → assumptions` join works. Guard: 0 collisions.

---

## 2. Verification — Excel recalc (win32com, closed without saving)

Balance sheet `us_soybean_complex_bal_sheets.xlsm`, links updated to the rewritten flat file, full
rebuild, errors counted by region:

- **biofuel monthly blocks (r99–177): 0 errors.** Total BBD Use annual = **14,004** (2026/27), **13,099**
  (2027/28) — was **0**. Biodiesel MY-totals 7,652 / 8,428 / 7,278.
- **non-bio + components (r179–305): 0 errors.**
- Positional refs confirmed exact (`$B$3/$19/$35/$51` → biodiesel/RD/coproc/SAF wide blocks).

---

## 3. ⚠️ The premise correction — READ THIS

**The task said "clear the 495 forward `#VALUE!` (the biofuel gap)." That attribution was wrong.**

The biofuel forward cells are `=IF([5]…wide!$AL$3="",0,…)` — IF-guarded, so an empty forward source read
**0** (silently wrong), never `#VALUE!`. The `#VALUE!` come **entirely from the un-forecast SUPPLY side**:
`AL11 Production = AL49 → #DIV/0!`, `AL10 Beginning Stocks = AK29 → #VALUE!`, cascading into Total Supply
(r13) → Ending Stocks (r29) → Total Demand (r28) → domestic-use residual (r306–336). **624 supply-side
errors remain** after the biofuel fix.

What 6c actually accomplished: replaced the forward biofuel **silent zeros** with real, banded,
provenance-tracked values, and cleared **all** biofuel + non-bio demand-block errors. What it did NOT do:
close the sheet forward — that needs a supply forecast (below). This was verified by recalc, not assumed.

---

## 4. Known broken / unverified — do NOT assume fixed

- [ ] **Soyoil sheet still can't close forward — 624 supply-side `#VALUE!`.** Production, beginning/ending
      stocks, and the balance identities. **This is session 6d** (see §5).
- [ ] **Excel recalc was a one-off manual win32com pass**, not a standing check. The flat file is correct
      on disk; the balance-sheet cached values refresh only when Tore opens it (external-link contract).
- [ ] **`MODEL_ADJUSTED(6)` not exercised** — only `MODEL_BASE(1)` published. Tore's judged mix-drift
      overlay is the intended rank-6 companion.
- [ ] **SAF forward is thin** — 16 of 29 months seasonal-projected off a short ramping history (~1–2% of
      total SBO; flagged per-month in run diagnostics).
- [ ] **Callable not registered in `core.kg_callable`** — invoked via its runner, not the MCP invoker.
- [ ] **Carried from 6b, still open:** `RepointSoyOilCleanup` must NOT run; tallow 3,133 mil lb vs EIA
      uninvestigated; PSD 140/149 not ingested; migration tracker behind on pre-150 migrations.

---

## 5. Open the next session with this (6d)

**Build the soybean-oil SUPPLY forecast** — the *actual* source of the forward `#VALUE!`. Production
(soybean crush × oil yield), plus beginning/ending stocks roll-forward, as `MODEL_BASE`(1) banded rows
into `silver.soybean_oil_series`, published via the same `core.forecast_run` path. **Copy the 6c
callable/runner/writer-merge pattern exactly — the plumbing is proven.** Cheapest first check: what forward
crush signal exists (is there a soybean-crush forecast series, or must crush be projected first)? Only when
production + stocks are forecast do Total Supply / Ending Stocks / Total Demand close forward and the 624
errors clear. Verify with the same win32com recalc.
