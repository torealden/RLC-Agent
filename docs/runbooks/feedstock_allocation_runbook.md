# Feedstock Allocation — Monthly Runbook

**Purpose:** Produce the FFA feedstock-consumption estimates (which feedstock feeds which
biofuel, by facility and nationally) for a new month of actuals, and refresh the flat files
Desktop links to. This process has a **strict dependency order**. Running it partially or
out of order produces output that *looks* fine (SBO bands pass, flat files write) but is
silently 20–30% low. **Always run the whole chain, in order, then validate against EIA.**

Last hard lesson: 2026-07-09. Skipping two derived-table steps dropped tallow + UCO out of
the allocation entirely and produced a 25%-low answer that passed every internal band. Only
the EIA reconciliation caught it. See **Lessons** below.

---

## 0. The dependency chain (why order matters)

```
RAW SOURCES (collectors / manual)                  must be current for the target month
  ├─ NASS Fats & Oils      → silver.monthly_realized (source='NASS_FATS_OILS')
  │                          production + REMOVAL FOR PROCESSING + stocks
  ├─ NASS Livestock Slaughter → bronze.nass_livestock_slaughter  (gates tallow; see L9)
  │                          collect_nass_livestock_slaughter.py — releases ~end of next month
  ├─ EIA feedbiofuel (v2)  → bronze.eia_feedstock_monthly (plant_type='total')
  ├─ Census trade HS1502   → bronze.census_trade   ←— tallow + UCO imports/exports
  │                          (HIDDEN DEPENDENCY — gates tallow & UCO; see L3)
  ├─ EMTS fuel volumes     → silver.fuel_production_forecast
  │                          (MANUAL EPA export, or rfs_data.xlsm fuel_prod_by_type)
  └─ feedstock prices      → bronze.feedstock_prices
        │
        ▼
DERIVED REBUILDS (scripts/, STRICT ORDER)
  0a. collect_nass_livestock_slaughter.py    NASS API → bronze.nass_livestock_slaughter (step 0, L9)
  0b. build_silver_animal_slaughter.py       → silver.animal_slaughter
  0c. build_silver_tallow_production.py       → silver.tallow_production (SLAUGHTER_DERIVED)
  1. build_silver_animal_fat_production.py   NASS F&O → silver.animal_fat_production
  2. build_silver_tallow_balance.py          (TRUNCATES) factual tallow series incl. trade
  3. build_tallow_biofuel_use.py             adds series 'tallow_biofuel_use' (guardrail)
  4. populate_silver_feedstock_supply.py     EIA → silver.feedstock_supply
  5. wire_uco_feedstock_supply.py            DELETE YG rows, WRITE canonical UCO
        │
        ▼
ALLOCATE + RAKE + PUBLISH
  6. allocator  --period YYYY-MM             → gold.feedstock_allocation (one month at a time)
  7. rake_feedstock_vintage_aware.py         → gold.bbd_feedstock_raked  (scale to EIA control)
  8. refresh_feedstock_flat_files.py         → ALL flat files (fats+oils+slaughter, max run_day)
                                               (the rake auto-invokes this on its tail; step is
                                                explicit here for manual/standalone reruns)
        │
        ▼
VALIDATION GATE (MANDATORY)
  Reconcile raked monthly total to EIA BBD control (±5% where EIA available).
```

**Non-obvious edges that will silently break the answer if you skip them:**
- Step **3** (`build_tallow_biofuel_use.py`) is what the allocator reads as its tallow
  guardrail (`silver.tallow_balance` series `tallow_biofuel_use`, class `ALL`). Step 2
  TRUNCATEs the table and does NOT recreate this series. **Skip step 3 → allocator allocates
  zero tallow.**
- Step **5** (`wire_uco_feedstock_supply.py`) DELETEs the EIA-derived YG rows and writes
  canonical UCO (UCO Amendment 1: `YG_BIOFUEL=0`, YG folds into UCO). Step 4 re-adds YG and
  does NOT write UCO. **Skip step 5 → UCO drops out and YG is double-counted.** Step 5 MUST
  run *after* step 4.

---

## 1. Prerequisites — confirm raw sources are current

Before rebuilding anything, verify each raw source reaches the target month. The derived
tables are only as fresh as their raw inputs.

```bash
python - <<'PY'
from dotenv import load_dotenv; load_dotenv('.env')
from src.services.database.db_config import get_connection
with get_connection() as conn:
    cur=conn.cursor()
    q=lambda s,a=(): (cur.execute(s,a), cur.fetchone())[1]
    print('NASS F&O   ', q("SELECT max(calendar_year*100+month) m FROM silver.monthly_realized WHERE source='NASS_FATS_OILS'"))
    print('Slaughter  ', q("SELECT max(year*100+month) m FROM bronze.nass_livestock_slaughter"))
    print('EIA total  ', q("SELECT max(year*100+month) m FROM bronze.eia_feedstock_monthly WHERE plant_type='total'"))
    print('Census 1502', q("SELECT max(year*100+month) m FROM bronze.census_trade WHERE hs_code LIKE '1502%%'"))
    print('Fuel prod  ', q("SELECT to_char(max(period),'YYYY-MM') m FROM silver.fuel_production_forecast"))
PY
```

If **Census 1502** lags the others, tallow + UCO will stop at the census frontier no matter
how fresh NASS/EIA are (this is L3). Run the census-trade collector for the current year
before proceeding, or accept that tallow/UCO won't advance past the last census month.

**EMTS fuel volumes are a manual monthly step** (accepted — ~2 min, see
`reference_emts_manual_export.md`). Either drop the EPA Qlik export into
`data/raw/rfs_data/rin_generation_<MM>_<YYYY>.csv` and run `src/tools/emts_csv_loader.py`,
or load from `models/Biofuels/rfs_data.xlsm` (`fuel_prod_by_type`) via
`scripts/load_fuel_production_from_rfs_workbook.py`.

---

## 2. Run the derived rebuilds (strict order)

```bash
# 0. Slaughter driver (step 0, L9) — refresh + rebuild BEFORE the tallow chain.
#    Without this, SLAUGHTER_DERIVED tallow stalls at its frontier and tallow_biofuel_use
#    (the allocator's tallow guardrail) halts there, zero-allocating tallow past that month.
python scripts/collect_nass_livestock_slaughter.py        # NASS API → bronze (current+prior yr)
python scripts/build_silver_animal_slaughter.py           # → silver.animal_slaughter
python scripts/build_silver_tallow_production.py          # → silver.tallow_production SLAUGHTER

# 1. NASS Fats & Oils → animal fat production (rank 90)
python scripts/build_silver_animal_fat_production.py

# 2. Tallow balance factual series (TRUNCATEs; production + census trade)
python scripts/build_silver_tallow_balance.py

# 3. Tallow biofuel-available guardrail  (REQUIRED — allocator reads this)
python scripts/build_tallow_biofuel_use.py

# 4. EIA → feedstock_supply.  NOTE: default year range ENDS AT 2025 (L4).
#    ALWAYS pass an explicit --range that includes the current year.
python scripts/populate_silver_feedstock_supply.py --range 2025 2026

# 5. Canonical UCO wiring  (REQUIRED — deletes YG, writes UCO; run AFTER step 4)
python scripts/wire_uco_feedstock_supply.py
```

Sanity after step 3: `tallow_biofuel_use` for the latest full year should land ~4.4–4.9 B lb
(2024 target band). After step 5: CY2024 `feedstock_supply` UCO ≈ 8.7 B, YG = 0.

---

## 3. Allocate, rake, publish

```bash
# 6. Allocator — ONE MONTH AT A TIME (per forecast-method ruling)
for m in 01 02 03 04 05; do
  python -m src.engines.feedstock_allocation.allocator --period 2026-$m --scenario base
done

# 7. Vintage-aware rake to EIA control totals
#    Set RUN_DAY in the script to today; it selects latest run per period.
#    On its tail this AUTO-INVOKES refresh_feedstock_flat_files.py (non-fatal), so step 8
#    usually needs no separate run — but run it standalone if the rake warned of a writer failure.
python scripts/rake_feedstock_vintage_aware.py

# 8. All flat files in one shot (fats+greases, SBO+canola oils, slaughter). Reads max(run_day).
#    Idempotent; safe to re-run. Exit 1 if any writer fails.
python scripts/refresh_feedstock_flat_files.py
```

---

## 4. Validation gate (MANDATORY — do not skip, bands are not enough)

Run the gate script (exit 0 = pass, exit 1 = publish blocker):

```bash
python scripts/validate_feedstock_gate.py --run-day <today> --year 2026
```

**Corrected spec (see L10).** The gate does NOT compare the raked *total* to the full EIA BBD
control — that would include Tallow and Yellow Grease, which the rake deliberately leaves
`EXEMPT_RLC` (RLC's supply build is authoritative there). The gate instead:
1. **Rake-controlled** feedstocks (`control_basis` EIA_TOTAL / EIA_BDRD / USDA_SEASONAL) reconcile
   to their EIA control within **±5%** — the meaningful check.
2. **Presence:** SBO, DCO, CO, CWG, EBFT, IBFT, UCO all present. If tallow or UCO is missing, a
   derived step was skipped (3 or 5), census trade is stale (L3), **the slaughter collector is
   stale (L9)**, or the UCO canonical source lags without the bridge (L8). Do not publish.
3. **RLC_CANONICAL vs EIA** (tallow, UCO/YG) reported as *information*, not pass/fail. Tallow
   RLC/EIA at ~50–73% is the **ruled swap-hypothesis residual** (`tallow_ruling_doc §2/§6`): EIA
   over-counts tallow / under-counts UCO (waste-oil booked as tallow), and RLC tallow below EIA is
   the intended correction. Report the residual — do NOT reopen it or "fix" it by raking tallow.

The EIA feedbiofuel dataset includes **Corn and Grain Sorghum** (ethanol feedstocks); the BBD list
in the script already excludes them. Publish (`write_fats_supply_flat_files.py`) only after exit 0.

---

## Lessons learned

- **L1 — Run the whole chain, in order, every time.** Partial runs produce low-but-plausible
  output that passes internal bands and writes flat files. The failure is silent. There is no
  safe subset.
- **L2 — Raw current ≠ allocator current.** The allocator reads *derived* tables
  (`feedstock_supply`, `tallow_balance`, `animal_fat_production`), not the raw NASS/EIA data.
  Every raw refresh must be followed by the derived rebuilds or the allocator silently uses
  stale supply (or falls back to canned `_estimate_supply` national totals).
- **L3 — Census trade is a hidden dependency for tallow + UCO.** Their biofuel-available
  guardrails are built from `bronze.census_trade` HS1502 imports/exports. If census trade lags
  (e.g. stuck at Feb while NASS/EIA reach May), tallow and UCO stop at the census frontier and
  the allocator zero-allocates them for the missing months — regardless of NASS/EIA freshness.
- **L4 — `populate_silver_feedstock_supply.py` default year range ends at 2025.** Always pass
  `--range <start> <currentYear>` explicitly, or the current year never gets supply rows.
- **L5 — Validate against EIA BBD totals, not just the rake's SBO bands.** And exclude Corn +
  Grain Sorghum (ethanol feedstocks) from the EIA control, or the reconciliation is garbage.
- **L6 — `wire_uco` must run after `populate`.** `populate` re-adds EIA YG rows; `wire_uco`
  deletes YG and folds the pool into canonical UCO. Reverse the order and YG double-counts /
  UCO is missing.
- **L7 — This whole class of error is why the chain needs an orchestrator.** A single
  `refresh_feedstock_chain.py --period YYYY-MM` that runs steps 1–8 in declared order (with the
  EIA gate as a hard stop) makes skip/order/staleness errors structurally impossible. Until it
  exists, this runbook is the manual substitute — follow it exactly.
- **L8 — UCO canonical source stops at Dec 2024; `wire_uco` is net-destructive past it without a
  bridge.** `wire_uco` DELETEs all UCO/YG rows then rewrites UCO only for periods in
  `silver.uco_yg_balance` (canonical frontier = Dec 2024). For 2025+ it stripped EIA's Yellow
  Grease and left NO UCO — UCO silently absent from the allocator for all of 2025–26. Fixed
  2026-07-11: `wire_uco` now backfills UCO from EIA Yellow Grease (`source='EIA_YG_BRIDGE'`) for
  months after the canonical frontier. Reversible; auto-retires as the canonical UCO build extends.
- **L9 — the slaughter driver is a collector now (was an orphan frozen at Feb 2026).**
  `bronze.nass_livestock_slaughter` feeds SLAUGHTER_DERIVED tallow production — the oleo-trend
  estimator base the `tallow_biofuel_use` guardrail halts on. It had NO collector and was frozen at
  Feb, silently capping tallow allocation. Built `scripts/collect_nass_livestock_slaughter.py`
  (2026-07-11). Run it as **step 0** before the tallow chain; then rebuild
  `build_silver_animal_slaughter.py` → `build_silver_tallow_production.py` before steps 1–3.
- **L10 — the old ±5% gate was mis-specified.** It compared the raked TOTAL to the full EIA BBD
  control including tallow, but tallow is `EXEMPT_RLC` (never scaled to EIA), so the gate failed by
  exactly the intended RLC/EIA divergence every month even when every rake-controlled feedstock was
  a perfect 100%. Replaced by `scripts/validate_feedstock_gate.py` (checks controlled feedstocks
  vs EIA, presence, and reports canonical divergence separately). See §4.

---

## TODO / not yet automated
- **Flat-file regeneration — DONE (2026-07-13).** `refresh_feedstock_flat_files.py` runs all three
  writers (fats/oils/slaughter) as one step, and `rake_feedstock_vintage_aware.py` auto-invokes it
  on its tail (non-fatal), so a rake can no longer leave stale flat files. Remaining automation gap
  is the *upstream* chain (steps 1–7), below.
- **Full-chain orchestrator** (`refresh_feedstock_chain.py`) implementing steps 1–8 + the EIA gate
  as a dependency DAG, triggerable manually (`--period`) and from a dispatcher ingest event.
  Trigger scope = feedstock-impacting sources only (NASS F&O, EIA feedbiofuel, EMTS fuel,
  prices, census 1502) — NOT weather/CFTC/etc. (steps 1–7 are still run by hand per §2–3.)
- **Poultry fat (PF):** EIA withholds it for recent months; supply must come from NASS
  production, not EIA. Currently PF falls back to estimate for withheld months.
- **Census-trade auto-refresh** as part of the chain so tallow/UCO don't lag (L3).
