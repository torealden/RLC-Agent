# The 23 open `?` workbooks — resolved by content

Companion to `2026-07-22_models_workbook_triage.md`. Every verdict below rests on what is
*inside* the file: byte hash, `_meta` provenance block, tab dimensions, header labels, and
last populated date.

**Tab names were tried first and are worthless here.** Your production workbooks all carry the
same five tabs by convention — `area_planted / area_harvested / yield / production / _meta` —
so `us_cotton_production.xlsx` scored 100% name-containment against `us_peanut_production.xlsx`.
Identical skeleton, unrelated data. Nothing in this file uses that signal.

These are **recommendations, not rulings.** Nothing here has been written to `sys.declaration`
— say the word and I'll persist them under your name.

---

## Archive — 10

| File | Why |
|---|---|
| `Data/US Corn Exports - 01201025.xlsx` | byte-identical to the `Spreadsheet Samples` copy you already marked `A` (sha `55920037ba33`) |
| `Data/US Corn Imports - 01201025.xlsx` | same, sha `7b2900f4cae3` |
| `Data/US Feed Grains Outlook - Dec 25.xlsx` | same, sha `58248937b794` — ERS Feed Grains Yearbook, 29 tables |
| `Data/oiltables.xlsx` | same, sha `6d576cfe863d` — ERS Oil Crops Outlook tables, and already ingested (`scripts/ingest_oil_crops_yearbook.py` → `bronze.ers_oil_crops_yearbook`) |
| `Biofuels/us_renewable_diesel_balance_sheets (version 1).xlsx` | **content-identical** to the live file: 160r × 68c, 6,435 populated cells, 6,286 numerics, values summing to 2,321,310 on both. Different bytes, same sheet. |
| `Feed Grains/us_corn_processing.xlsx` | subsumed — 1 tab, 7 product columns. `us_grain_crush.xlsm[corn_products]` carries 39 columns covering both the corn-input side (*Corn for HFCS, Corn for Glucose & Dextrose, Corn for Starch*) and the product side (*HFCS-42, HFCS-55, Glucose Syrup, Dextrose, Corn Starch, Corn Flour*). This is your `oilseed_prices` / `soybean_prices` case exactly. |
| `Feed Grains/cir_m311k1107.xlsx` | raw Census report — *"Fats and Oils, Production, Consumption, and Stocks — July 2011, M311K(07)-11."* A source document, misfiled under Feed Grains. See the note below before archiving. |
| `Feed Grains/corn_processing_report.xlsx` | not commodity data — 21 rows of NAICS codes, establishment counts, payroll and employee counts. County Business Patterns, not a balance sheet or an input. |
| `Food Grains/keypath_proof_1.xlsx` | test artifact. 152-row `production` tab in flat-file contract format plus a 7-row `keypath_test` tab headed *"header / derived key / planted(mil) / harvested(mil)"*. A proof-of-concept for the SUMIFS key path. |
| `Oilseeds/United States/us_soy_crush.xlsm` | **strict subset** of `us_oilseed_crush.xlsm` — see below |

### `us_soy_crush.xlsm` ⊂ `us_oilseed_crush.xlsm`

| | us_soy_crush | us_oilseed_crush |
|---|---|---|
| tabs | 3 | 10 |
| soy crush tab | `NASS Crush` 132r × 189c, **last 2025-12** | `soy_crush` 146r × 189c, **last 2026-12** |
| `NOPA US Crush` | 747r × 31c | 747r × 31c — identical |
| `Census Crush` | 384r × 267c | 384r × 267c — identical |
| also carries | — | canola / sunflower / cottonseed / peanut crush, NASS Low CI, NASS Other Veg Oils, us_crush_summary |

Same column counts throughout, one extra year of soy data in the oilseed version, and the tab
renamed `NASS Crush` → `soy_crush`. Corroborating: `FatsOilsUpdaterSQL` — your universal Ctrl+U
updater — lives in `us_oilseed_crush.xlsm`, while `us_soy_crush.xlsm` carries the untracked
`Module1`, the older generation.

---

## Keep — 13

| File | Why |
|---|---|
| `Biofuels/us_biodiesel_balance_sheets.xlsx` | full S&D sheet, 161r × 68c. **Not** subsumed by `us_bbd_combined_bal_sheets.xlsx`, which is a 16-row summary plus a capacity table. |
| `Biofuels/us_renewable_diesel_balance_sheets.xlsx` | same — 160r × 68c |
| `Biofuels/us_sustainable_aviation_fuel_balance_sheets.xlsx` | same — 160r × 68c, tab `us_saf` |
| `Cotton/us_cotton_production.xlsx` | different *layout* from `us_oilseed_production.xlsx`, not just different commodity: this is Year × vintage-release (`PP (Mar) / Acreage (Jun) / Aug WASDE / Sep / Oct / Nov / Final (Jan)`), the oilseed file is State × `AP_YYYY`. Vintage tracking vs state detail — two different jobs. |
| `Cotton/us_cotton_trade.xlsx` | US tabs only (`Cotton Exports`, `Cotton Imports`) |
| `Cotton/world_cotton_trade.xlsx` | complementary — Brazil, India and World tabs, no US. Same split as `world_wheat_trade` vs `us_grains_trade`. |
| `Feed Grains/us_grain_crush.xlsm` | the corn grind / ethanol workbook, and the destination for `CornGrindUpdater`. See the note below. |
| `Food Grains/us_food_grain_production.xlsx` | state-level wide matrix (wheat + winter/spring/durum, 3,324 / 4,557 / 1,479 / 588 rows) from `silver.crop_production` mig 122. `us_wheat_production.xlsx` is the *national LONG flat file* — different artifact, not a duplicate. |
| `Food Grains/world_wheat_trade.xlsx` | the wheat member of the family you kept whole — world corn, soybean, rapeseed, sunflower, palm. 7 origins × exports/imports. |
| `Oilseeds/Brazil/brazil_soy_complex_monthly.xlsx` | **this is a flat file** — 394 rows in contract format, `_meta` says *"Generated from gold.abiove_soy_complex_monthly."* A known input under your own rule. |
| `Oilseeds/United States/us_oilseed_production.xlsx` | the aggregate — but incomplete. See below. |
| `Oilseeds/United States/us_veg_oil_prices.xlsx` | current prices. `rlc_prices.xlsx` is the *historical archive* (daily through Jan 2021, monthly through 2024) — different role. |
| `Fats and Greases/us_animal_fat_prices.xlsx` | same — AMS series updated through 2026-05 |

### `us_oilseed_production.xlsx` is the aggregate, but not yet a superset

| Tab | State |
|---|---|
| soybeans, canola, sunflower, sunflower_oil, sunflower_confection, peanut | populated |
| cottonseed | `TODO: derived from cotton or NASS COTTONSEED series` |
| flaxseed, safflower | `TODO: add to nass_crop_production_collector COMMODITIES` |
| peanut_runner / peanut_spanish / peanut_vv | `(no data yet)` |
| mustard | absent entirely |

Archiving `us_cottonseed_`, `us_flaxseed_`, `us_safflower_` and `us_mustard_production.xlsx`
today would lose coverage. Both files come from `silver.crop_production` (mig 122) — finish the
collector and the per-commodity files retire on their own.

---

## Three things worth knowing regardless of the verdicts

**`cir_m311k1107.xlsx` is the last issue of a discontinued series.** It is July 2011, and the
`Census Crush` tab in *both* crush workbooks also stops at 2011-07-01. Census discontinued the
M311K fats-and-oils report after that month. Archiving the PDF-derived workbook is still right
— the data is already in the crush workbooks — but that 2011 wall is a real feature of the
series, not a stale pipeline.

**`us_grain_crush.xlsm` is stale in exactly the place Ctrl+K was meant to fix.** Its
`weekly_ethanol_production` tab stops at **2017-12-22** and `monthly_ethanol_data` at
**2020-10-01**. Its `_meta` was regenerated today (2026-07-22) by your import, but the vintage
rule text is copy-pasted boilerplate from the crop-production writer — *"PP=YEAR-MAR ACREAGE
earliest…"* — which is meaningless for ethanol.

**Two crush tabs carry impossible dates.** `us_oilseed_crush.xlsm[sunflower_crush]` reads
last = **2057-06-01** and `[NASS Other Veg Oils]` reads **2056-10-01**. Almost certainly a date
parse or cell-format artifact rather than real forward data, but I have not opened them to
confirm, and a bad date can propagate into a MAXIFS-driven balance sheet.

---

## Not verified

- Nothing here has been written to `sys.declaration`.
- The `us_cotton_trade` / `world_cotton_trade` split is inferred from tab names (`Cotton Exports`
  vs `Brazil/India/World Cotton Exports`). I did not open both and compare the row labels to
  confirm the US file has no world coverage and vice versa.
- The RD "(version 1)" comparison used cell counts and a value sum, not a cell-by-cell diff.
  Two sheets can share both and still differ. It is strong evidence, not proof.
- 43 `models/` workbooks still report a null `sheet_count` after the property-merge fix. Cause
  unknown; not investigated.
