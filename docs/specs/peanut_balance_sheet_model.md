# Peanut Complex Balance Sheet Model — Proposal

**Date:** 2026-05-27
**Status:** Proposed (awaiting Tore review)
**Driver:** The standard one-page (production / imports / exports / domestic / stocks)
template doesn't fit peanuts. Peanut flow has multiple decision points (sheller vs
crusher vs roaster) and the food-side disappearance has commercially meaningful
subsegments (butter / candy / snacks / in-shell / other). Helios models this
deeply; we should too — partial coverage is fine for our biofuel core, but
food-industry buyers expect it.

## Why peanuts are different

Three structural reasons the simple template breaks:

1. **Two basis units in active circulation.** ERS reports the master balance
   sheet on a **farmer's stock basis** (in-shell, with hulls). Food use is
   reported on a **shelled basis**. Conversion is roughly 0.75 shelled per
   farmer-stock (USDA convention varies a few percentage points by year).
   Mixing the two without explicit conversion is the #1 source of "weird
   numbers."

2. **Multiple disappearance branches with material industry coverage.**
   Soybeans go ~95% to crush and exports. Peanuts go to four distinct
   destinations with non-trivial shares: **food use (~70%)**, **crush
   (~15-20%)**, **exports (~10%)**, **seed+loss+residual (~5%)**. Each is a
   separate industry conversation.

3. **Food use itself splits into five commercially distinct streams**
   (peanut butter, candy, snacks, other edible, clean in-shell). Each one
   has its own demand drivers (retail snacking, candy seasonality, school
   lunch programs, kosher/halal/specialty channels) that any food-industry
   analyst will want broken out.

## Data sources we have / need

### Already in DB

| Source | Granularity | What it gives us | DB location |
|---|---|---|---|
| **ERS Oil Crops Yearbook Table 11** | Annual, MY (Aug-Jul) | Master farmer's-stock balance sheet 1980/81–2023/24 (Beg Stocks, Production, Imports, Crush, Food Use, Exports, Seed/Loss/Residual, End Stocks, Total Supply, Total Disappearance, Season-Avg Price) | `bronze.ers_oilcrops_raw` table 11 |
| **ERS Oil Crops Yearbook Table 12** | Annual, MY | Food-use breakdown shelled basis (Butter, Candy, Snacks, Other, Clean in-shell, Total) | `bronze.ers_oilcrops_raw` table 12 |
| **ERS Oil Crops Yearbook Tables 13-16** | Annual | Acreage / yield / production by state and region | `bronze.ers_oilcrops_raw` |
| **NASS Peanut Stocks and Processing** | Monthly | Shelled crush, edible usage by category (butter/candy/snacks/other), in-shell usage, roasting stock production, shelled oil stocks, cake & meal production+stocks, crude oil production+stocks (mills basis) | `bronze.nass_processing` source=NASS_PEANUT |
| **NASS Fats and Oils — Peanut block** | Monthly | Crude oil processed in refining, once-refined oil produced, refined oil removed for processing, crude/refined stocks (refiners basis) | `bronze.nass_processing` source=NASS_FATS_OILS |
| **FAS PSD** | Annual MY, by country | International parallel: 1,913 peanut rows including US | `bronze.fas_psd` |
| **Census trade** | Monthly, HS code | Imports/exports of peanuts (HS 1202), peanut oil (HS 1508), peanut butter (HS 2008.11) | `bronze.census_trade` |

### Gap to close

- **NASS Annual Peanut Production (Crop Production report)** — final production numbers each January. Should already be in `bronze.nass_production` but worth verifying.
- **WASDE peanut table** — peanut isn't in the main WASDE summary; ERS Oil Crops Outlook (monthly) is the published projection. Need an ERS Outlook PDF/CSV scraper or manual entry.
- **Quarterly Peanut Stocks** (NASS) — separate from monthly Peanut Stocks & Processing. Verifies the monthly accumulation. Should be in `bronze.nass_stocks` already.
- **Peanut Butter Manufacturers' usage** (industry data — National Peanut Council). Helios likely sources this; we may need to pull or model.

## Proposed model — 4 tiers

```
Tier 0 — Forecast layer (ERS Oil Crops Outlook + RLC analyst)
                       │
                       ▼
Tier 1 — Master Peanut Balance Sheet (farmer's stock basis, in-shell)
   Beg + Production + Imports = Supply
   Supply = Crush + Food Use + Exports + Seed/Loss/Residual + End Stocks
                │            │            │
                ▼            ▼            ▼
Tier 2A — Crush       Tier 2B — Food     Tier 2C — Trade
                     Use Allocation     (Census HS codes)
                     (shelled basis)
                │            │
                ▼            ▼
Tier 3A — Peanut Oil    Five sub-flows:
Tier 3B — Peanut Meal    Butter / Candy / Snacks /
                         Other / Clean In-shell
```

### Tier 1: Master Peanut Balance Sheet *(farmer's stock basis, mil lbs)*

Canon = ERS Oil Crops Yearbook Table 11. Monthly granular fills via NASS.

| Line | Source | Notes |
|---|---|---|
| Beginning Stocks | NASS Peanut Stocks quarterly + carry | Verifies against ERS |
| Production | NASS Crop Production (Jan final) | By state in Tiers 13-16 |
| Imports | Census HS 1202 | Most US peanut imports = ~zero; small from Argentina/India |
| **Total Supply** | derived | |
| Crush | NASS Peanut Stocks & Processing | Convert: shelled crushed × 1/0.75 = farmer-stock basis |
| Food Use | NASS Peanut Stocks & Processing | Convert: shelled food usage × 1/0.75 |
| Exports | Census HS 1202 | In-shell exports already farmer's basis |
| Seed, Loss, Shrinkage, Residual | derived (residual) | This is the "plug" — ERS reports it |
| Ending Stocks | NASS Peanut Stocks | |
| **Total Disappearance** | derived | Reconcile to Total Supply – End Stocks |
| Season-Avg Price | NASS / AMS | Per Table 11 |

### Tier 2A: Peanut Crush Balance Sheet *(shelled basis, mil lbs)*

| Line | Source |
|---|---|
| Shelled Peanuts Crushed | NASS Peanut Stocks & Processing |
| → Crude Oil Production (mills basis) | NASS (~36-40% yield) |
| → Cake & Meal Production | NASS (~45-50% yield) |
| Cake & Meal Stocks | NASS |
| Shelled Oil Stocks | NASS |
| Implied Loss (shells+moisture+misc) | derived (~10-15%) |

Reconciliation: Tier 1 Crush (farmer's basis) × 0.75 ≈ Tier 2A Crushed (shelled basis).

### Tier 2B: Peanut Food Use Allocation *(shelled basis, mil lbs)*

Canon = ERS Oil Crops Yearbook Table 12. Monthly via NASS.

| Sub-flow | Annual canon | Monthly source |
|---|---|---|
| Peanut Butter | ERS T12 | NASS edible_usage_peanut_butter |
| Peanut Candy | ERS T12 | NASS edible_usage_candy |
| Snack Peanuts | ERS T12 | NASS edible_usage_snacks |
| Other Edible | ERS T12 | NASS edible_usage_other |
| Clean In-shell Food Use | ERS T12 | NASS in_shell_usage (in-shell basis — different unit!) |
| **Total Food Use** | ERS T12 | NASS edible_usage_total |

### Tier 3A: Peanut Oil Balance Sheet *(mil lbs)*

| Line | Source |
|---|---|
| Beginning Stocks (Crude) | NASS F&O |
| Crude Oil Production (mills) | NASS Peanut Stocks & Processing |
| Crude Oil Imports | Census HS 1508.10/1508.90 |
| **Total Crude Supply** | derived |
| Crude → Sent to Refining | NASS F&O ("Crude oil processed in refining") |
| Crude Exports | Census HS 1508 |
| Ending Crude Stocks | NASS F&O |
| | |
| Once-Refined Oil Production | NASS F&O |
| Refined → Further Processing | NASS F&O |
| Ending Refined Stocks | NASS F&O |

Reconciliation: Crude Production (NASS Peanut) should ≈ Crude Processed in Refining (NASS F&O) within a month or two of stock-adjusted lag.

### Tier 3B: Peanut Cake & Meal Balance Sheet *(mil lbs)*

| Line | Source |
|---|---|
| Beginning Stocks | NASS |
| Production | NASS (from Tier 2A) |
| Imports | Census (small) |
| Exports | Census |
| Domestic Use (Feed, mostly) | derived as residual |
| Ending Stocks | NASS |

## Reconciliation rules

These are the rules that make this a *system* rather than five separate spreadsheets:

1. **Tier 1 Crush × 0.75 = Tier 2A Shelled Peanuts Crushed** (within rounding)
2. **Tier 1 Food Use × 0.75 = Tier 2B Total Food Use** (shelled basis)
3. **Tier 2B Total Food Use = sum of 5 sub-flows** (Butter + Candy + Snacks + Other + In-shell)
4. **Tier 2A Crude Oil Production ≈ Tier 3A Crude Oil Production** (same source, sanity check)
5. **Tier 2A Cake & Meal Production = Tier 3B Production**
6. **Tier 3A Crude → Refining ≈ Tier 3A Refined Production** (after yield + months-lag adjustment)
7. **Annual sums of monthly NASS data should reconcile to ERS Yearbook Table 11/12 within ±5%**. Larger divergence flags a data issue.

## Implementation sequence (suggested)

1. Build `silver.peanut_balance_sheet_master` (Tier 1, annual + monthly) — pulls from ERS T11 + NASS aggregate
2. Build `silver.peanut_food_use_subbalance` (Tier 2B) — ERS T12 + NASS monthly
3. Build `silver.peanut_crush_subbalance` (Tier 2A) — NASS monthly
4. Build `silver.peanut_oil_balance_sheet` (Tier 3A) — NASS F&O + Peanut + Census trade
5. Build `silver.peanut_meal_balance_sheet` (Tier 3B) — NASS + Census
6. Build `gold.peanut_complex_reconciliation` — runs the 7 reconciliation rules monthly, surfaces variances
7. Update `models/Oilseeds/us_peanut_bal_sheets.xlsm` with corresponding tabs (one per silver layer)

Effort: 3-5 days for the silver/gold layers, another day for spreadsheet templates.

## For lauric oils (coconut + palm kernel)

Same 4-tier template, with two structural differences:
- **No domestic production** of meaningful scale. Tier 1 is import-dominated.
- **No fresh in-shell food channel.** Tier 2B sub-flows are simpler:
  confectionery, baking/food service, food industrial, non-food industrial
  (soap/cosmetics).

Otherwise the model literally copies and changes the feedstock name. Worth
building peanut first as the prototype, then replicating.

## Open questions for Tore

1. **Conversion factor** for farmer-stock → shelled basis: 0.75 is the rough
   USDA convention. Want me to back-solve year-by-year from ERS T11 vs T12
   to get a calibrated annual ratio?
2. **WASDE-equivalent forecast source for peanut** — ERS Oil Crops Outlook
   is monthly. Want us to ingest it (PDF parse), or is RLC analyst input
   sufficient for the forecast layer?
3. **Food-industry sub-flow granularity** — five sub-flows from ERS T12 is
   the minimum. Want more (e.g., split snacks into salted/unsalted, butter
   by smooth/chunky, etc.)? That'd require National Peanut Council or
   industry data, not USDA.
4. **Confectionery channel for lauric oils** — is there a USDA source for
   coconut/PK use by sub-category, or do we model that from industry
   reports?
