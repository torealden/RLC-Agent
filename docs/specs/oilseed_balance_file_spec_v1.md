# Oilseed Complex Balance File — Specification v1

**Status:** draft v1, 2026-07-27. Governs every country oilseed complex workbook (soy, rapeseed/canola,
sunflower, palm, and minor oils). Derived from the US soybean complex reference
(`models/Oilseeds/United States/us_soybean_complex_bal_sheets.xlsm`) — its **model**, not its
**implementation** (see §9, the anti-patterns we deliberately fix).

**The one-line ruling:** the monthly block is the deliverable; the annual balance is a *rollup of the
monthly*, and every cell reads only in-workbook tabs — **zero external-workbook links.** A file that is
annual-only, or that references a closed sibling workbook, is not done (it is the `annual` coverage
state, §10).

---

## 0. Why this spec exists

The US reference has the right conceptual model (monthly-primitive, annual-rollup, per-series blocks,
biofuel breakout) wired the wrong way (external `[2]/[3]/[4]` workbook links, hardcoded literals,
missing rollups, position-based refs). Hand-replicating it — or letting an LLM free-hand it —
reproduces the fragility. The fix is a **deterministic generator** that emits the layout from this
spec plus a per-country *data recipe* (§11), wired only to in-workbook mirror tabs (§6), with tie-outs
baked into every tab (§7). This spec is the contract the generator builds to and the verifier checks.

---

## 1. Workbook anatomy (one file per complex per country)

`models/Oilseeds/<Country>/<country>_<complex>_balance_sheet.xlsx`

| Tab | State | Purpose |
|---|---|---|
| `<member>_balance_sheet` (one per complex member) | visible | the model: annual summary + monthly blocks |
| `ff_<tag>_long` (one hidden mirror per member, or one shared) | hidden | long-form source data, mirrored **in-workbook** (§6) |
| `quarterly_use` | visible, optional | quarterly S&D (seed only; grain-stocks driven) |
| `wasde_comp` | visible, optional | RLC-vs-USDA current-MY comparison |

**Complex membership** (member → short tag):
- **Soybean:** soybeans `sb` · soybean_meal `sm` · soybean_oil `so`
- **Rapeseed/Canola:** rapeseed `rs` · rapeseed_meal `rm` · rapeseed_oil `ro`
- **Sunflower:** sunflowerseed `us` · sunflowerseed_meal `um` · sunflowerseed_oil `uo`
- **Palm:** palm_oil `po` · palm_kernel_oil `ko` (no seed/meal tab — palm is harvested as FFB, not a
  crushed oilseed; palm-kernel *meal* PKC is a minor line, add only where a national source exists)

Tab names must be ≤31 chars (Excel cap); abbreviate the commodity token in the **tab name only**
(`sunflowerseed`→`sun`), never in the A1 title.

---

## 2. Sheet grammar (columns = marketing years, rows = the stack)

```
Row 1  A: "<COUNTRY> OILSEEDS COMPLEX"            (complex banner)
Row 2  A: "<COUNTRY> <MEMBER> SUPPLY AND DEMAND"  B..: calendar year int (1990, 1991, …)
Row 3  A: "<display unit>"                        B..: MY label "1990/91", "1991/92", …
Rows 4..K   ANNUAL SUMMARY block   (display units; §3)
Row  K+1    "Bold, green numbers are RLC estimates and predictions"   (legend)
Rows …      MONTHLY BLOCKS, one per monthly-granular series          (§4)
```

- **Columns B onward = marketing years, ascending**, history through a forward projection horizon. The
  reference runs B(1990/91)…BE(2045/46) = 56 columns. Generator sets the horizon from the recipe
  (`history_start`, `forecast_end`); default history start per `reference_history_start_dates`
  (oilseeds Oct 1993 for monthly; PSD annual reaches 1990).
- **Never reference a cell by absolute position across blocks.** The generator keeps a `row_map` dict
  (`label → row`) and writes every cross-reference through it, so inserting a block cannot silently
  break a downstream formula (the reference's `=C105` disease).

---

## 3. Annual summary block — canonical line items by member type

Annual line items are **derived**: each equals its monthly block's Marketing-year Total (§4), or a
within-block identity. The generator writes `= <MonthlyBlock MY-Total row>`, not a raw source ref.

**Seed** (e.g. soybeans, rapeseed, sunflowerseed):
```
Planted Area · Harvested Area · Harvested/Planted · Yield
Beginning Stocks · Production · Imports · Total Supply
Crush · Seed · Residual · Exports · Total Demand
Ending Stocks · Stocks-to-Use · Farm Price · Futures Price
```
**Meal:**
```
Crush (info) · Yield lbs/bu (info)
Beginning Stocks · Production · Imports · Total Supply
Domestic Use · Exports · Total Demand · Ending Stocks · Stocks-to-Use · Price
```
**Oil (with biofuel breakout — the whole point of the oil tab):**
```
Crush (info) · Oil Yield (info) · Biofuel production lines (BD/RD/SAF) · Biofuel yield (info)
Beginning Stocks · Production · Imports · Total Supply
  Biodiesel Feedstock · Renewable Diesel Feedstock · SAF Feedstock · [Co-processing]
  Total Biomass-Based Diesel Use            (= sum of the four feedstock blocks)
  Non-Biodiesel Domestic Use               (= Total Domestic Use − Total BBD Use)   ← residual
    [7 end-use lines where a national source exists: salad/cooking, baking/frying,
     margarine, misc edible, plastics/resins, paint/varnish, misc non-edible]
  Total Domestic Use · Exports · Total Demand · Ending Stocks · Days of Coverage · Price
```
Non-US countries will not have the US end-use split; carry `Non-Biodiesel Domestic Use` as a single
residual line and omit the 7 sub-lines unless a source exists (`feedback_nonbio_residual_after_biofuel`).

---

## 4. Monthly block grammar (the primitive)

Every S&D line with monthly granularity gets a block:
```
Row r    A: "<COUNTRY> <MEMBER> <SERIES>"        (block title)
Row r+1  A: "(<display unit>)"                    B..: "=B3"  (echoes the MY label)
Row r+2 .. r+13   the 12 months, in the SERIES' marketing-year order (§4.1)
Row r+14 A: "  Marketing-year Total"              B..: "=SUM(<12 month cells>)"
```
- Month **rows are fixed** (12 per block); **year is the column**. Column B holds all 12 months of
  1990/91; column C holds 1991/92; etc.
- The MY-Total row is **mandatory and must be written** — the reference's missing `B145`/`B161` sums
  are a defect, not a style choice.
- Blocks required at minimum: Production, Imports, Exports, (Crush for seed), Month-Ending Stocks,
  Domestic Use. Oil adds: Biodiesel, Renewable Diesel, SAF, Co-processing (→ Total BBD), each its own
  block rolling into the BBD-use block by monthly SUM across components.

### 4.1 Marketing-year basis is per-series, declared explicitly
Seed and its crush products do **not** share a marketing year. Soybeans MY = **Sep–Aug**; the crush
complex (crush, meal, oil) = **Oct–Sep**. The recipe declares `my_basis` per series; the generator
orders the 12 month rows accordingly and labels row r+2 with the correct first month. Do not assume.

### 4.2 The monthly identity (why domestic use is a residual)
Where a monthly ending-stock series exists (e.g. Census crush stocks), Domestic Use is computed
**per month by mass balance**, not sourced:
```
domestic_use[m] = beg_stk[m] + production[m] + imports[m] − exports[m] − end_stk[m]
                  (beg_stk[m] = end_stk[m−1])
```
This is the reference's row-309 pattern (`=B10+B37+B69-B85-B325`), generalized through the `row_map`.
Where no monthly stock series exists, annual domestic use is **apportioned to months by a declared
seasonal share vector** (`fallback: seasonalize`), never faked as flat or invented.

---

## 5. The rake (Σ monthly = annual control)

The annual PSD balance (the `*_flat.xlsx` control, already generated) is the **tie-out anchor**, not the
product. For each series and each marketing year:
```
Σ_12 monthly[series, MY]  ==  annual_control[series, MY]   (within tolerance τ, default 0.5%)
```
- History: if the national monthly source undershoots the control (Brazil oil is ~95% today), the
  generator applies a **declared rake** — scale the 12 months to the control, or book the gap to a
  named residual month — per the recipe's `rake` rule. Silent 95% is a fail, not a pass.
- Forward: monthly = annual forecast × seasonal share (the forecast lives in the annual control;
  monthly only distributes it).

---

## 6. Wiring — in-workbook mirror tabs only (the core fix)

**No formula may reference another workbook.** Source data enters through a hidden long-form mirror tab
inside the same file. Two equivalent implementations; generator uses (a):

**(a) Embedded long table** (`ff_<tag>_long`, cols A..K = the 11-col long schema, §6.1). The generator
**copies** the flat file's rows in at build time (values, not links). Self-contained; portable; the
reason no flat file needs to be open.

**(b) Mirror-of-link** (the session-6d `ff_sbo_supply` style): row 1 note + `=[n]source!A2` fill. Only
if a live link to a maintained flat file is explicitly wanted; still fragile — prefer (a).

### 6.1 Long-form mirror schema (11 cols, matches the flat-file contract)
```
commodity · class · series · marketing_year · period_type · period · vintage · vintage_rank · value · unit · source
```
- `period_type ∈ {annual, monthly, quarterly}`; monthly `period` = `YYYY-MM`.
- Highest `vintage_rank` wins (per `reference_vintage_rank_ladder`): 1–9 reserved for RLC forecasts,
  PSD/WASDE 61–90. The wiring idiom (§6.2) resolves ties by MAXIFS on vintage_rank.

### 6.2 The one verified cell idiom (reuse verbatim from `write_balance_sheet.py`)
```
=IF(COUNTIFS(<crit>)=0,"",
   SUMIFS(ff!$I$2:$I$8001,<crit>, ff!$H$2:$H$8001, _xlfn.MAXIFS(ff!$H$2:$H$8001,<crit>)))
```
where `<crit>` filters commodity/class/series/marketing_year/period_type/period. **`MAXIFS` must be
written `_xlfn.MAXIFS`** or openpyxl-authored files silently return 0 (a passing tie-out on an empty
sheet — the non-triviality guard, §7, is what catches it).

---

## 7. Guards — baked into every tab, checked by win32com recalc

Every `<member>_balance_sheet` tab carries guard cells; the verifier
(`scripts/verify_oilseed_recalc.py`, from this session's `recalc_verify.py`) opens the file, forces a
full recalc, and asserts:

1. **Balance tie-out** = 0: Ending (computed = supply − demand) − Ending (reported) rounds to 0.
2. **Rake tie-out** per series/MY: |Σ monthly − annual control| ≤ τ.
3. **Monthly identity** per series/MY: |Σ (beg+prod+imp−exp−domuse−end)| ≤ τ.
4. **Non-triviality:** SUM(production) > 0 (catches the silent-MAXIFS-zero trap).
5. **Zero `#` errors** anywhere in the used range (`#REF! #VALUE! #NAME? #DIV/0! #N/A #NULL! #NUM!`).

A tab is green only when 1–5 all pass. This is the gate the overnight harness (§ harness spec) loops
against — the LLM never self-certifies.

---

## 8. Forward projection & provenance

- **Bold green** = RLC estimate/forecast (visual convention, `reference_excel_color_conventions`,
  internal green `#3C7D22`). History = default weight.
- Forecast rows enter the mirror as `vintage_rank` 1–9 so they lose to any realized print but win over
  older forecasts. The monthly forecast is a *distribution* of the annual forecast (§5), so it can never
  silently disagree with the control.

---

## 9. Anti-patterns — what we deliberately fix vs. the US reference

| US reference defect | This spec's rule |
|---|---|
| External-workbook links `[2]/[3]/[4]/[5]` | §6 — in-workbook mirror only, values copied at build |
| Hardcoded literals (meal beg-stk `=318`) | every number is sourced through the mirror or computed |
| Missing MY-Total rollups (`B145`, `B161` blank) | §4 — MY-Total row mandatory, generator asserts present |
| Position refs (`=C105`, `=B336`) | §2 — all cross-refs through the `row_map` dict |
| Ad-hoc split-MY handling (Sep vs Oct starts) | §4.1 — `my_basis` declared per series |
| Silent 95%-of-control monthly (Brazil oil) | §5 — rake enforced; undershoot is a fail |
| Annual-only sheets marked "done" | §10 — annual-only is the `annual` state, not done |

---

## 10. Definition of DONE (the coverage bar)

- **`annual`** (teal): annual balance closed + Excel-verified (guards 1,4,5). Monthly blocks absent or
  empty. Where the 11 current Tier-A cells sit today.
- **`done`** (green): every monthly block populated with **real** monthly data (or explicitly
  seasonalized with the fallback flag set), **raked** to the annual control (guard 2), the monthly
  identity closes (guard 3), all five guards green. Only then does a cell earn green in the coverage
  matrix (`build_coverage_matrix_html.py`, `VERIFIED_CLOSED`).

---

## 11. Per-country data recipe (the input the build consumes)

One file per (country, complex): `models/Oilseeds/<Country>/_recipe_<complex>.yaml`. This is the
artifact the overnight LLM fills/repairs and the generator + collectors consume. Schema:

```yaml
country: Brazil
complex: soybean
history_start: 1997-01        # monthly history availability (not the PSD annual start)
forecast_end: 2028-09
members:
  soybean_oil:
    unit_display: "1000 MT"   # display unit; source unit declared per-series
    series:
      production:
        my_basis: Feb-Jan            # Brazil oil crush year
        source:
          name: ABIOVE
          access: db                 # db | api | csv | scrape | derive
          table: silver.abiove_soy_complex     # or endpoint/url/file
          column: oil_production
          unit_source: "1000 MT"
        availability: monthly
        rake: scale_to_control       # scale_to_control | residual_month:<MM> | none
        fallback: null               # seasonalize:<share_vector_id> when availability<monthly
      exports:
        my_basis: Feb-Jan
        source: { name: comexstat, access: db, table: bronze.comexstat_trade, column: qty }
        availability: monthly
        rake: residual_month:Jan
        fallback: null
      imports: { ... }
      month_ending_stocks: { ... }
      # domestic_use omitted — computed by the monthly identity (§4.2)
```

Recipe rules:
- Every `source.access: db` must name a real table/column the collector can read; `scrape`/`api`
  entries name the collector module the LLM writes/owns.
- `my_basis`, `unit_source`, `rake`, `fallback` are **required** per series — the generator refuses to
  build a series with any of them unset (gate-beats-parameter: a missing basis is a stop, not a guess).
- A series with `availability < monthly` and `fallback: null` is a **blocked** cell — the harness emits
  a "needs monthly source or a seasonalization decision" report rather than fabricating.

---

## 12. Build sequence (how this becomes files, fast)

1. **Extend the generator** `write_balance_sheet.py` → `write_oilseed_complex.py`: emit annual summary
   + monthly blocks + guards from the recipe. Deterministic; ~one build, N recipes.
2. **Verifier** `verify_oilseed_recalc.py`: guards 1–5 (generalize this session's `recalc_verify.py`).
3. **Prove on Brazil soy** (data already in DB): write `_recipe_soybean.yaml`, generate, verify, match
   the US structure. This is the golden worked example.
4. **Recipe-fill the other 10** via the overnight harness (separate spec): LLM writes collectors +
   recipes, conductor gates on the verifier, morning report of green/blocked/failed.
5. **Fix the US reference itself** to this spec (rip external links → mirror) so it stops being the
   thing that "still has large problems" and becomes the golden reference it's supposed to be.

Cross-refs: `flat_file_contract_v1.md` (v1.1, mirror-tab wiring), `write_balance_sheet.py` (the idiom),
`reference_vintage_rank_ladder`, `reference_us_oilseed_unit_convention`,
`project_nonbio_residual_after_biofuel`, `reference_xlsx_flat_file_conventions`.
```
