# UCO Resolution — Code → Desktop initiating brief

**From:** Claude Code (implementation) | **To:** Claude Desktop (design / contract / methodology)
**Initiated:** 2026-07-04 by Tore. **Parent:** BBD Feedstock System design v1.6 LOCKED (allocator canonical,
raked to EIA). **Status:** Code has done the diagnosis + grounded the data; Desktop owns the methodology.

## 1. Why this project

The allocator allocates **zero UCO** because `silver.feedstock_supply` (built from EIA Form 819) has no UCO
line — **EIA never publishes UCO separately; it folds it inside "Yellow Grease."** The rake ties the combined
Yellow-Grease bucket to EIA exactly (national total is fine), but the UCO **split** is missing, which means:
- Per-facility RD mixes 2022+ show understated UCO (Desktop's own flag — the China-import ramp lands on the
  RD fleet and currently gets modeled as yellow grease / other).
- The 2050 forecast can't carry UCO as its own line — and UCO is *the* growth story on the RD side, with
  distinct CI and price. Tore wants it resolved as a distinct, forecastable feedstock.

**The reconciliation guarantee that makes this safe:** `UCO_biofuel + YG_other = EIA Yellow Grease`. UCO is a
**within-bucket split**, so the rake still pins the combined total to EIA — we're only apportioning it. Nothing
we do here can break the national EIA tie.

## 2. Tore's design rules (binding)

1. **Zero out US domestic non-biofuel UCO use — same methodology for history AND forecast — and keep non-bio
   as close to zero as possible for both.** Working assumption: essentially all collected + net-imported UCO
   goes to biofuel. If we develop a strong view on non-bio uses (feed, oleochemicals, pet food) we model it,
   but the default is ≈0, applied identically to history and forecast so it never introduces a seam.
2. **Proxy now, biotracker later.** Domestic collection is proxied now from USDA food-spending; biotracker
   (when it exists) supplies actual collection. **Design the collection series as vintage-laddered so
   biotracker supersedes the proxy with zero rework** — same MAXIFS(vintage_rank) seam as wheat.
3. **Layer something simple now, refine with biotracker** — Tore's stated preference. Don't over-build the
   non-bio model before biotracker makes it obvious.

## 3. The three data streams (all already in bronze — no new collectors)

| stream | source | status |
|---|---|---|
| **Domestic collection proxy** | `bronze.ers_food_sales_monthly` — `food_category` ∈ {Food away from home, Food at home}, monthly, nominal + real, 1997–2024, 19 outlet_types | ready; FAFH = restaurant fryer-oil activity, the collection driver |
| **Imports** | `bronze.census_trade` HS `1518.00.40` (+ we already ship a trade flat file) | ramp confirmed (1.3→5.8→17.5→30B raw 2020-24); **raw magnitude inflated** — needs import-only flow + unit/code-purity calibration |
| **Combined control total** | `bronze.eia_feedstock_monthly` "Yellow Grease" (plant_type=total) | the bucket UCO must sit inside; 5.58 B lb trailing-12mo |

## 4. Methodology skeleton (Desktop designs the specifics)

The identity, applying rule #1 (non-bio ≈ 0, no stock change):
```
UCO_biofuel(t) = domestic_collection(t) + net_imports(t)          [net = imports − exports]
domestic_collection(t) = k · FAFH_proxy(t)        (+ optional FAH term)
YG_other(t) = EIA_Yellow_Grease(t) − UCO_biofuel(t)               (floored at 0)
```
**Decisions Desktop owns:**
- **The collection coefficient `k`** and the FAH/FAFH weighting — and, critically, its **calibration anchor**.
  Options: calibrate to a year with a credible external UCO-collection estimate, or calibrate so
  `UCO_biofuel ≤ EIA_Yellow_Grease` holds across history with `YG_other ≥ 0` (the split can't go negative).
- **The import calibration** — right HS lines, import-only, unit conversion, and whether `1518.00.40` is
  UCO-clean or needs a companion code. Code will do the pull; Desktop rules on what's in-scope.
- **The EIA-YG reconciliation** — what to do in any period where `collection + net_imports > EIA_YG`
  (cap UCO at EIA_YG? re-fit k?). This is the one place the within-bucket split can misbehave.
- **The non-bio term** — default 0; if Tore later wants a feed/oleochemical view, where it plugs in.
- **The biotracker interface** — collection as a series with vintage `PROXY_FOOD` (low rank) now,
  `BIOTRACKER` (high rank) later; forecast vintage `PROXY_FOOD_FCST`. Define the ranks.
- **The 2050 forecast method** — project FAFH (trend + seasonality) and imports forward → `UCO_biofuel` to
  2050, monthly, with non-bio held at 0 by construction.

## 5. Labor division (dual-Claude pattern)

**Code (me) — plumbing:**
- `silver.food_expenditure` from bronze (FAH/FAFH, monthly, real+nominal, tidy).
- Census UCO import series cleaned (import-only, kg→lb, right HS scope per Desktop's ruling).
- Implement Desktop's `k`/split methodology → **`UCO_biofuel` + `YG_other` series**.
- Split `silver.feedstock_supply`: add UCO rows, set `YG = EIA_YG − UCO_biofuel` (preserve the combined total).
- Re-run allocator (UCO now allocated to the RD fleet) → re-rake → re-run acceptance.
- **UCO flat file** per contract (collection / net_imports / UCO_biofuel / YG_other, vintage-laddered).

**Desktop — design/contract/workbooks:**
- The methodology in §4 (k, weighting, calibration anchor, reconciliation, non-bio, biotracker ladder).
- The **UCO flat-file contract** (columns, vintage ranks for PROXY_FOOD / BIOTRACKER / forecast).
- The 2050 forecast method for UCO (and how it composes with the other feedstocks' forecasts).
- Workbook wiring.

## 6. Flat files (new, beyond the existing trade file)

One new writer-owned flat file, `us_uco_supply.xlsx` (or Desktop's naming), LONG per Contract v1.1:
`domestic_collection`, `net_imports`, `uco_biofuel`, `yg_other` — each with the vintage ladder so
proxy→biotracker and actual→forecast upgrade via MAXIFS. It feeds the feedstock-consumption layer as the
UCO/YG split; the existing trade flat file supplies the import leg.

## 7. What I need back from Desktop to start building

1. The `k` calibration anchor + FAH/FAFH weighting (so I can compute `domestic_collection`).
2. The import HS scope ruling (is `1518.00.40` UCO-clean; import-only; any companion code).
3. The EIA-YG reconciliation rule (cap vs re-fit when `collection + net_imports > EIA_YG`).
4. The vintage ranks (PROXY_FOOD / PROXY_FOOD_FCST / BIOTRACKER) and the flat-file contract for `us_uco_supply.xlsx`.

Once those land I build silver → supply-split → re-run → re-rake → UCO flat file, and the RD-fleet mixes 2022+
carry real UCO. Everything stays inside the EIA-pinned Yellow Grease total, and biotracker later is a vintage
swap, not a rebuild.
