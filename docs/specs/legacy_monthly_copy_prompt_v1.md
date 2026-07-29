# PROMPT — Copy legacy monthly blocks into new country balance-sheet workbooks

*Paste into a fresh session. Oilseeds pass only (corn & wheat run separately). A verified reference
implementation already exists: `scripts/copy_legacy_monthly_blocks_trial.py` (Australia rapeseed) —
read it first; your job is to GENERALIZE it, not start over.*

---

## Goal
For every country in the legacy world balance files, build a new 3-tab country workbook by copying the
**monthly data blocks** (values only) out of the legacy tab and **re-bucketing each month into the US
marketing year**. Tore builds the MY annuals from the monthly sums and fills implied lines (domestic
use, non-biofuel splits) with formulas later — you do **data only**.

## Source
- Dir: `C:/Users/torem/RLC Dropbox/Tore Alden/Soybean Spreadsheets - Copy/`
- Files this pass (oilseeds): `wldsoybal.xlsx`, `wldrapbal.xlsx`, `wldpeabal.xlsx`, `wldlaubal.xlsx`.
  **Do NOT use `wldoilseedbal.xlsx`** — that's the veg-oil rollup Tore builds later.
- Inside each file, the country tabs are named `"<Country> <Crop>"` (e.g. `Australia Rapeseed`,
  `EU Rapeseed`, `China Rapeseed`). **Process every such country tab. SKIP** the `World <Crop>` tab and
  all analysis/chart tabs (e.g. `Canada Canola Crush`, `Chart1`).
- Load the source with `data_only=True` — copy the **computed values as values** (including bold
  Jacobsen forecast points; they become plain numbers).

## Target
- Template (structure is good, keep it): `models/Oilseeds/China/china_soybean_complex_bal_sheets.xlsx`
  — 3 tabs `soy_balance_sheet` (seed), `soymeal_balance_sheet`, `soyoil_balance_sheet`.
- Output: `models/Oilseeds/<Country>/<country>_<crop>_complex_bal_sheets.xlsx` (mkdir the country dir).
- **Never rename the tabs** — 816 formulas reference the name `soy_balance_sheet`. Renaming breaks the
  model.

## The three balance sheets and their US marketing years
| Product | Target tab | US MY | Monthly blocks to copy (where the source has them) |
|---|---|---|---|
| Seed | `soy_balance_sheet` | **Sep–Aug** | imports, exports, crush |
| Meal | `soymeal_balance_sheet` | **Oct–Sep** | production, imports, exports, month-end stocks |
| Oil | `soyoil_balance_sheet` | **Oct–Sep** | production, imports, exports, month-end stocks |

- **Do NOT copy**: annual MY totals (Tore sums the months), domestic-use blocks (Tore formulas them),
  yield blocks (template formulas), area/price rows.
- Seed has **no** monthly production or monthly stocks in the source (harvest is annual, stocks are
  point-in-time) → leave those target blocks blank.
- Source block headers are crop-named (`RAPESEED IMPORTS`, `SOYBEAN MEAL PRODUCTION`, …); target
  headers are always `CHINA SOYBEAN <X>`. Map by role, not by text. Locate blocks by header search;
  the year-label row is header+1, months are header+2..+13.

## THE CRUX — marketing-year re-bucketing (do not shortcut)
Each country's source blocks are in that **country's own local MY**, which differs by country. **Read
it from the source** — the S&D section's `Carryin (<month> 1)` / `Carryout (<month> 31)` labels and the
monthly block's first month tell you the window (Australia rapeseed = Nov–Oct; others differ — EU,
Canada, etc. each have their own). **Do not hardcode Nov–Oct and do not assume a given calendar month
sits in the same MY on both sheets.**

For every source monthly cell:
1. Compute its true **(calendar month, calendar year)** from the source column's MY label + the local
   convention. (Nov–Oct example: label `93/94` → Nov,Dec = 1993; Jan–Oct = 1994.)
2. Compute the **US MY** that contains that calendar month: seed **Sep–Aug** (month ≥ Sep → year/…;
   else prior year); meal/oil **Oct–Sep** (month ≥ Oct → year/…; else prior year).
3. Write the value into the target block at **[US-MY column][that month row]**.

This is Tore's "start at Sep, copy to the bottom, then shift the tail over a year" done
deterministically — Sep/Oct at the tail of a Nov–Oct source year correctly move to the **next** US
year. The reference script's `us_my()` / `cal_year_from_src()` / `read_block()` implement exactly this;
generalize `cal_year_from_src` to the detected local convention per country.

## Mechanics (from the working reference)
- **Clear first, but only hardcoded NUMBERS** in the template's month rows (removes China's data);
  **preserve formula cells** (yields, cross-tab refs) so they recompute for the new country.
- Target year columns: read the **literal** years from **row 3** of each tab (the per-block year rows
  are `=B$3` formulas, not literal labels).
- Target years run ~1990/91→2046; source starts wherever it starts (Australia rapeseed = 93/94). Map
  only overlapping years; leave the rest as-is.

## Relabel (LABEL cells only — never formulas, never tab names), in THIS order
1. Fix US→China artifacts FIRST: `SCHINATAINABLE`→`SUSTAINABLE`, `CHINAE`→`USE`.
2. `CHINA`→`<COUNTRY>` (upper), `China`→`<Country>`, `Chinese`→`<Country>n`.
3. `SOYBEAN`→`<CROP>` (upper), `Soybean`→`<Crop>`, `soybean`→`<crop>`.
4. Units: everything non-US = **thousand tonnes**, no short tons: `thousand short tons`→`thousand
   tonnes`, `short tons`→`tonnes`, `short ton`→`tonne`.
(Order matters: artifact fixes must precede the CHINA swap or `CHINAE`→`AUSTRALIAE`.)
Leave per-market wording (e.g. "Central Illinois", "CME") and any commodity-specific rows as-is —
Tore does the per-commodity/country cleanup + non-bio splits in the later wiring pass.

## Verify every workbook before moving on (fail loud)
- Round-trip: re-read each source block, recompute the target cell, confirm the written value matches
  within 1e-6 (openpyxl float save/load differs in the last digits — that's fine; a *genuine* mismatch
  is a bug). **0 genuine mismatches.**
- **816 cross-tab formulas** (referencing `soy_balance_sheet`) still present.
- **0 dirty tokens** remain: no `china`, `schinatainable`, `chinae`, `short ton`, `soybean` (case-
  insensitive) in label cells.
- Print a per-country summary: cells written per block, source MY convention detected, years covered.

## Scope / order
Run `wldsoybal` → `wldrapbal` → `wldpeabal` → `wldlaubal`, every country tab in each. Remember the
full-complex rule: keep all three tabs (seed/meal/oil) for every country even if some blocks are blank.
When done, list the workbooks created and any country whose source MY convention you couldn't confidently
detect (stop and ask rather than guess the window).

## Known reference result
Australia rapeseed (`models/Oilseeds/Australia/australia_rapeseed_complex_bal_sheets.xlsx`): 4,342
monthly cells re-bucketed, 0 genuine errors, 816 formulas preserved, 0 dirty tokens. Match that bar.
