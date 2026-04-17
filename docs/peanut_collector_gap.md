# Peanut Collector Gap — Findings 2026-04-16

## What works today

- `peanut_crush` tab cols V, W, X, Y populate on Ctrl+U (migration 027 applied).
- Source: NASS Fats & Oils report, `commodity_desc='OIL'`, `class_desc='PEANUT'`.
- Bronze has 4 peanut oil series from the Fats & Oils report.

## What's missing

The remaining ~17 columns of the peanut_crush tab (B–P, R, S, U, Z) need data
from the **NASS Peanut Stocks and Processing** survey. These use
`commodity_desc='PEANUTS'` and `commodity_desc='CAKE & MEAL'`/`class_desc='PEANUT'`.

## Infrastructure issues discovered

### Issue 1: `bronze.nass_processing` has zero PEANUTS rows

| commodity_desc | rows |
|----------------|------|
| OIL            | 2955 |
| CAKE & MEAL    | 965  |
| (PEANUTS)      | **0** |

But `silver.monthly_realized` has **24 months** of `peanuts_crushed`,
`peanuts_milled`, `peanuts_usage` from source `NASS_PEANUT`. So data HAS
been pulled from NASS at some point but never landed in bronze. Investigation
showed `save_to_bronze` works correctly with synthetic peanut rows — so the
silver data was likely populated via a different/earlier path (manual backfill?).

### Issue 2: `nass_processing` collector runs but doesn't log status

Schedule: monthly day 10 at 15:00 ET (per `master_scheduler.py:512`).

**Bronze IS being updated** — last collected 2026-03-17 for most commodities,
with data through Jan 2026. So the collector is running.

But **`core.collection_status` shows zero `nass_processing` runs** — logging
hook is broken/missing in the dispatch path. Not blocking data collection,
but blocks the ops dashboard from reporting freshness on this collector.

Combined with Issue 1 (PEANUTS never lands in bronze but other commodities do),
the bug is specifically in `save_to_bronze` when processing peanut rows —
likely a row-level exception in the parsed peanut DataFrame that gets caught
silently in the per-row try/except. Synthetic peanut rows insert fine, so the
issue is data-shape-specific (probably some null/missing field NASS returns
for PEANUTS that we don't handle).

### Issue 3: NASS Quickstats API flakiness during this session

Multiple HTTP 500 and timeouts. Whether the collector has also been hitting
these and failing silently is unknown.

## What to do next

1. **Wait for `scripts/probe_peanut_nass.py` to complete** (running in background,
   retries up to ~30min). Output: `scripts/_peanut_nass_probe_output.txt`.
   Will give us actual short_desc values to plug into the reference table.

2. **Investigate scheduler:** why `nass_processing` has never run. Check:
   - `core.collection_status` write path from `collector_runner.py:320`
   - Is dispatcher actually firing this collector?
   - Logs for recent monthly days (Apr 10, Mar 10, Feb 10)

3. **After probe returns actual short_descs**, write migration to:
   - Add `silver.crush_attribute_reference` rows for cols B, D–P, R, S, U, Z
   - Expand `CRUSH_MEAL_COMMODITIES` dict in `nass_processing_collector.py`
     (add peanut entries for CAKE & MEAL commodity_desc)
   - Expand `fetch_peanut_processing` stat_categories to include `PRODUCTION`
     and `DISAPPEARANCE` (needed for Pages 4 and 6 of the report)
   - Extend `_map_attribute` to break out `peanuts_usage` by short_desc into
     `peanut_candy`, `peanut_snacks`, `peanut_butter`, `peanut_other_products`

4. **One-shot backfill** for historical data (2000+ recommended) once the
   collector config is correct.

5. **Peanut Part B (col C):** Simple derivation `= col B × 1.33`. Add as
   `is_formula=TRUE` row in reference table, compute in `gold.fats_oils_crush_matrix`.

6. **Peanut Part C (col P):** Build `silver.peanut_food_use` view using
   Page 7 total + Census HS 1202.41 (raw unshelled imports/exports) + HS 2008.11
   (peanut butter / prepared exports) + Pages 2, 4 roasting stock data.

## Key findings for reference table

| Sheet col | Sheet header | NASS source (expected)                        | Notes |
|-----------|--------------|-----------------------------------------------|-------|
| B | Shelled peanuts crushed | `PEANUTS` / `CRUSHED` (Page 5)           | Unit: thousand pounds |
| C | Shelled peanut crush - Farmer stock basis | **= B × 1.33** | Derived, is_formula=TRUE |
| D | Total edible grade shelled peanuts | `PEANUTS` / `USAGE` Page 7 Total edible | Unit: thousand pounds |
| E | Edible peanuts used in peanut candy | `PEANUTS` / `USAGE` Page 7 candy | Unit: thousand pounds |
| F | Edible peanuts used in peanut snacks | `PEANUTS` / `USAGE` Page 7 snacks | Unit: thousand pounds |
| G | Edible peanuts used in peanut butter | `PEANUTS` / `USAGE` Page 7 butter | Unit: thousand pounds |
| H | Edible peanuts used in other products | `PEANUTS` / `USAGE` Page 7 other | Unit: thousand pounds |
| I | Total shelled peanuts of all grades | `PEANUTS` / `USAGE` Page 7 all grades | Unit: thousand pounds |
| J | In shell peanuts | `PEANUTS` / `USAGE` Page 7 in-shell | Unit: thousand pounds |
| K | Roasting stock (in shell) peanut production | `PEANUTS` / `PRODUCTION` Page 4 roasting | Unit: thousand pounds |
| L | Shelled oil stocks | `PEANUTS` / `STOCKS` Page 3 shelled oil stocks | Unit: thousand pounds |
| M | Total shelled peanuts | `PEANUTS` / `STOCKS` Page 2 FSE Total | Unit: thousand pounds |
| N | Farmer stock | `PEANUTS` / `STOCKS` Page 2 Farmer Stock | Unit: thousand pounds |
| O | Roasting stock (in shell) | `PEANUTS` / `STOCKS` Page 2 Roasting | Unit: thousand pounds |
| P | Peanut food use | **Derived** — see Part C | Needs Census trade |
| R | Cake and meal production | `CAKE & MEAL` / `PEANUT` / `PRODUCTION` | Unit: thousand pounds |
| S | Cake and meal stocks | `CAKE & MEAL` / `PEANUT` / `STOCKS` | Display: 000 ST (÷2000) |
| U | Crude oil production | `OIL` / `PEANUT` / `PRODUCTION` **crude** | Unit: thousand pounds |
| V | Crude oil processed in refining (F&O) | `OIL` / `PEANUT` / removal for processing | **DONE** |
| W | Once refined oil produced (F&O) | `OIL` / `PEANUT` / once-refined production | **DONE** |
| X | Once refined oil removed for processing | `OIL` / `PEANUT` / once-refined removal | **DONE** |
| Y | Crude oil on hand EOM (F&O) | `OIL` / `PEANUT` / crude stocks at refineries | **DONE** |
| Z | Crude oil stocks | `OIL` / `PEANUT` / crude stocks at oil mills | Different from Y — at mills, not refineries |

Confirmation needed on exact short_desc patterns for each from the NASS probe output.
