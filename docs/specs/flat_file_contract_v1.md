# Flat-File Contract v1 — the frozen interface between data (Claude-Code) and sheets (Desktop)

> **This is a FROZEN interface.** Claude-Code writes flat files to this schema; Desktop wires balance
> sheets to it. Once Desktop starts wiring, changing a column name or a `series` value breaks every
> sheet. Freeze first, wire second. Grounded in the working US pair
> `models/Oilseeds/United States/us_soybean_oil_supply_demand.xlsx` (the "new SUMIFS" pattern).

**Status:** v1.1 · 2026-07-26 · authority for the parallel build. v1.1 adds the VERIFIED wiring
(§6, read from the US reference — mirror tabs, exact formula, annual variant, non-triviality cell) and
the `_flat.xlsx` naming fix, both from the Desktop bounce.

---

## 1. Physical location (SHARED LOCAL FILESYSTEM is the sync — NOT git)

```
models/<Complex>/<Country>/<country>_<commodity>_flat.xlsx           ← GENERATED PSD-annual flat file
models/<Complex>/<Country>/<country>_<commodity>_supply_demand.xlsx  ← CURATED multi-source flat file (e.g. US)
models/<Complex>/<Country>/<country>_<complex>_balance_sheet.xlsx    ← MODEL workbook (Desktop/Code writes)
```
**Three reserved suffixes — never overload them** (fix, Desktop bounce 2026-07-26): `_flat.xlsx` =
machine-generated PSD annual, safe to overwrite on every run; `_supply_demand.xlsx` = hand-curated,
multi-source, monthly (the US soy oil reference is one of these — a *flat file*, not a model);
`_balance_sheet.xlsx` = a model. The generator writes only `_flat.xlsx`, so it can never clobber a
curated reference.
- **`models/` is gitignored** (large binaries). The two Claudes share these files through the **local
  filesystem** on Tore's machine (`C:\dev\RLC-Agent\models\`), and/or Dropbox — **not** git. Git syncs
  only the CODE (scripts, specs). Do not expect a workbook to travel via a commit.
- `<Complex>` ∈ {Oilseeds, …}; `<Country>` = the folder name already scaffolded under `models/Oilseeds/`
  (e.g. `Brazil`, `Europe`, `Canada`, `Ukraine`, `Malaysia`). One flat file **per commodity** (the
  US pattern: `us_soybean_oil_supply_demand.xlsx` is oil-only), so a crush complex produces separate
  seed / meal / oil flat files.
- **Country folders are canonical.** Never write a bare copy at the complex root.
- **Never write to an open workbook** (`~$` lock = stop). This corrupted the crush workbook at ledger 6e.
- **Reference file (built + verified 2026-07-26):** `models/Oilseeds/Brazil/brazil_soybean_oil_supply_demand.xlsx`
  — the annual-grain analogue of the US oil file; identity ties out to PSD ending stocks exactly.
  Produced by `python scripts/write_psd_flat_file.py soybean_oil BR`.

---

## 2. The long/tidy tab — WHAT SUMIFS QUERIES (the contract's core)

One tab per side: `<commodity>_supply` and `<commodity>_demand`. **Row 1 = headers, exact names,
exact order. Row 2+ = data, one row per observation.**

| # | Column | Type | Notes |
|---|---|---|---|
| A | `commodity` | text | canonical, snake_case: `soybean_oil`, `palm_oil`, `rapeseed_oil`, `sunflower_oil`, `corn_oil`, `soybean_meal`, `rapeseed`, … |
| B | `class` | text | `ALL` unless a sub-class is modeled |
| C | `series` | text | **the SUMIFS criterion** — see §4 vocabulary. Frozen. |
| D | `marketing_year` | int | MY start year (2024 = 2024/25). Per-commodity MY start per CLAUDE.md. |
| E | `period_type` | text | `annual` (PSD, Tuesday grain) or `cal_month` (national monthly, later). Same schema, different grain. |
| F | `period` | text | `ANNUAL` for annual rows; `M01`…`M12` for months. |
| G | `vintage` | text | provenance tag: `PSD`, `CENSUS`, `NASS`, `CONAB`, `MPOB`, `RAKED`, `MODEL_BASE`, `ACTUAL`, … |
| H | `vintage_rank` | int | **the ladder** — MAXIFS picks the highest. See §5. |
| I | `value` | float | raw source units (col J). No pre-division. |
| J | `unit` | text | `1000_MT`, `LB`, `1000_ST`, … source-native. |
| K | `source` | text | free-text lineage, e.g. `USDA_FAS_PSD`. |

There is exactly one authoritative value per (`commodity`,`class`,`series`,`marketing_year`,
`period`) — the one at `MAX(vintage_rank)`. Multiple vintages may coexist; the ladder resolves them.

---

## 3. The wide tab + index + meta (display & anchoring aids — Claude-Code also writes these)

- `<commodity>_supply_wide` / `_demand_wide`: months (or `ANNUAL`) down rows, marketing-years across
  columns, one block per `series`. Row 1 = series TITLE, row 2 = unit + MY headers, rows = periods.
- `_wide_index`: one row per series block — `tab · series · title_row · header_row ·
  first_month_row · last_month_row · total_row · first_my_col · first_my · last_my`. **Desktop uses
  this to anchor formulas to the wide block without hard-coding row numbers.**
- `_meta`: `series · source · unit · last_updated · notes`. Provenance surface for the report footer.

Desktop may wire against **either** the long tab (MAXIFS/SUMIFS, preferred, vintage-aware) **or** the
wide block (direct cell ref via `_wide_index`). Long tab is canonical; wide is the convenience mirror.

---

## 4. `series` vocabulary — FROZEN per complex (the SUMIFS criteria set)

Annual/PSD grain fills all of these for every Tier-A country from `bronze.fas_psd`.

**Oilseed SEED tab** (`<seed>_supply` / `_demand`): `beginning_stocks · production · imports ·
crush · feed_use · fsi_use · exports · ending_stocks`. (Supply = beg+prod+imp; Use = crush+feed+fsi+exp.)

**Oilseed OIL tab** (`<oil>_supply` / `_demand`): `beginning_stocks · production · imports ·
domestic_use · exports · ending_stocks`. PSD gives **total** `domestic_use` only (no food/biofuel/
industrial split — attr 140/149 not ingested). The splits `domestic_use_food · domestic_use_biofuel ·
domestic_use_industrial` are OPTIONAL later-enrichment series that must **sum to `domestic_use`**;
wire the sheet to `domestic_use` now, add splits when a source lands. (US soy oil already carries the
biofuel split from the allocator; that is the monthly-enrichment target for every country.)

**Oilseed MEAL tab** (`<meal>_supply` / `_demand`): `beginning_stocks · production · imports ·
domestic_use · exports · ending_stocks`.

**Palm** (8-sheet): CPO uses the OIL vocabulary; **PKO uses the OIL vocabulary on `palm_kernel_oil`**;
`palm_kernel` (the seed) uses the SEED vocabulary; `palm_kernel_cake` uses the MEAL vocabulary; plus a
`plantation` tab: `mature_area · immature_area · ffb_yield · oil_extraction_rate` (units native).

**Corn oil** (derived, 2-sheet): OIL vocabulary but `production` only (no crush complex); `imports ·
exports · domestic_use_food · domestic_use_biofuel · ending_stocks`.

> **Freeze rule:** these strings are the contract. If a series is missing for a country, write the row
> with `value=0` and a `notes` flag — **never invent a new series name.** New names are a schema
> change and must be agreed, not improvised.

---

## 5. The vintage ladder (why the sheet auto-upgrades)

`MAXIFS(vintage_rank …)` picks the best available number; higher wins. Reserved bands
(`reference_vintage_rank_ladder.md`):

| Vintage | Rank | Meaning |
|---|---|---|
| `MODEL_BASE` / forecast | 1–9 | callable output — the floor |
| survey / interim | 10–60 | national interims |
| **`PSD`** | **70 (proposed)** | WASDE/PSD band 61–90 — the Tuesday backbone |
| `CENSUS` / `NASS` / national actual | 90–95 | monthly actuals |
| `ACTUAL` / final | 99 | final revised |

**Consequence for the sprint:** Desktop wires once to the long tab. When Claude-Code later adds a
CONAB/MPOB monthly row (rank 90) over the PSD annual (rank 70), the sheet upgrades **with no formula
change.** That is the entire reason to build annual-first now.

---

## 6. The VERIFIED wiring (extracted from the US reference 2026-07-26 — not a sketch)

Confirmed by reading `us_soybean_complex_bal_sheets.xlsm` cell `soyoil_balance_sheet!AL37`.

### 6a. Mirror-tab pattern (this is how #VALUE! is avoided)
The balance-sheet workbook does **not** reference the flat file externally (MAXIFS/SUMIFS return
`#VALUE!` against a *closed* external workbook). Instead it carries **in-workbook mirror tabs**
`ff_<tag>_supply` / `ff_<tag>_demand` (e.g. `ff_sbo_supply`), which hold a **copy of the flat file's
long-tab rows** (same 11 columns A–K). All SUMIFS/MAXIFS/COUNTIFS run against those mirror tabs with
**bounded ranges `$2:$8001`** (not whole-column). Populating/refreshing the mirror from the flat file
is Claude-Code's job (generator or macro), not a hand-paste.

### 6b. The exact per-cell formula (the "new SUMIFS connection")
The reference is `IF(COUNTIFS(...)=0, <fallback>, SUMIFS(value, ..., H, MAXIFS(H, ...)) / <unit>)`:
```
=IF(COUNTIFS(ff!$A$2:$A$8001,"soybean_oil", ff!$B$2:$B$8001,"ALL", ff!$C$2:$C$8001,"production",
             ff!$D$2:$D$8001,LEFT(HDR$3,4)*1, ff!$E$2:$E$8001,<period_type>, ff!$F$2:$F$8001,<period>)=0,
    "",                                                       ← blank when no data (see 6d on fallbacks)
    SUMIFS(ff!$I$2:$I$8001, ff!$A..,"soybean_oil", ff!$B..,"ALL", ff!$C..,"production",
           ff!$D..,LEFT(HDR$3,4)*1, ff!$E..,<period_type>, ff!$F..,<period>,
           ff!$H..,MAXIFS(ff!$H.., <same 6 criteria>)) / <unit_divisor>)
```
- Columns are fixed: A commodity · B class · C series · D marketing_year · E period_type · F period ·
  H vintage_rank · I value.
- **MY match:** the sheet's MY header (row 3) is text like `2025/26`; `LEFT(HDR$3,4)*1` coerces it to
  the integer `2025` to match column D (integer). This is why D is stored as an int, not `"2025/26"`.
- **MAXIFS-inside-SUMIFS** picks the value at the best vintage_rank — the vintage ladder, in one cell.

### 6c. ANNUAL variant for the sprint (US reference is MONTHLY — do NOT clone it verbatim)
The US sheet filters `period_type="cal_month", period="M10"…"M09"` (12 month rows/series) and divides
`/1000000` (LB→mil lb). **The PSD flat files are `period_type="annual", period="ANNUAL", unit="1000 MT"`.**
So the sprint template is an *annual variant*: **one row per series**, filter `E="annual", F="ANNUAL"`,
and **no unit divisor** (display in 1000 MT; use `/1000` only if you want MMT). Same mirror tabs, same
MAXIFS/SUMIFS idiom, simpler layout. When monthly national data lands later, add the 12 month rows —
the wiring is identical, only the `E`/`F` filters and the divisor change.

### 6d. Guards
- Bind by explicit column letter, bounded `$2:$8001`. No Excel Tables, no defined names.
- Two tie-out cells per tab, visible, read 0: `TotalSupply−(Beg+Prod+Imp)` and `Ending−(Supply−Distribution)`.
- **Non-triviality cell (required — an all-zero tab passes both tie-outs):** a third visible cell =
  `SUM(production across all MYs)`. **Coverage does not go green until this is > 0.** This is what stops
  an empty wired tab from reading as "done." (Desktop bounce, 2026-07-26.)
- Missing series resolve to blank/0, never `#REF!`; never invent a `series` name.

---

## 7. What is frozen vs extensible

- **Frozen:** the 11 long-tab columns (§2), the `series` vocabulary (§4), the period conventions (§5
  `period_type`/`period`), the file/folder location (§1), the formula binding style (§6).
- **Extensible without breaking wiring:** adding rows (new MY, new months, new vintages), adding a new
  `series` **only by mutual agreement** (then Desktop adds the line), adding wide-tab blocks.
- **A schema change = stop-the-world.** If the contract must change mid-sprint, both Claudes pause,
  agree, and Claude-Code republishes all affected flat files before Desktop resumes.
