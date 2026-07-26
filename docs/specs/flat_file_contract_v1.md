# Flat-File Contract v1 — the frozen interface between data (Claude-Code) and sheets (Desktop)

> **This is a FROZEN interface.** Claude-Code writes flat files to this schema; Desktop wires balance
> sheets to it. Once Desktop starts wiring, changing a column name or a `series` value breaks every
> sheet. Freeze first, wire second. Grounded in the working US pair
> `models/Oilseeds/United States/us_soybean_oil_supply_demand.xlsx` (the "new SUMIFS" pattern).

**Status:** v1 · 2026-07-26 · authority for the 36-hour parallel build.

---

## 1. Physical location (both Claudes write here; git is the sync)

```
models/<Complex>/<Country>/<country>_<complex>_supply_demand.xlsx     ← the flat file (Claude-Code writes)
models/<Complex>/<Country>/<country>_<complex>_balance_sheet.xlsx     ← the workbook  (Desktop writes)
```
- `<Complex>` ∈ {Oilseeds, …}; `<Country>` = the folder name already scaffolded under `models/Oilseeds/`
  (e.g. `Brazil`, `Europe`, `Canada`, `Ukraine`, `Malaysia`).
- **Country folders are canonical.** Never write a bare copy at the complex root.
- **Never write to an open workbook** (`~$` lock = stop). This corrupted the crush workbook at ledger 6e.

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
domestic_use_food · domestic_use_biofuel · domestic_use_industrial · exports · ending_stocks`.
(Biofuel split may be `0` where PSD attr 140/149 not ingested — leave the series, value 0, don't drop it.)

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

## 6. The formula idiom Desktop uses (copy this, don't reinvent)

For a cell resolving `series=production`, `marketing_year=<MY>`, `period=ANNUAL` from the long tab
`brazil_soybean_oil_supply` (columns per §2):

```
best rank :  =MAXIFS(tab!$H:$H, tab!$C:$C,"production", tab!$D:$D,<MY>, tab!$F:$F,"ANNUAL")
value     :  =SUMIFS(tab!$I:$I, tab!$C:$C,"production", tab!$D:$D,<MY>, tab!$F:$F,"ANNUAL", tab!$H:$H,<bestrank cell>)
```
Bind by **column letter** (whole-column ranges), never by Excel Table name or defined name — appends
must shift nothing. Identity checks live in the workbook as visible tie-out cells (should read 0):
`Total Supply − (Beg + Prod + Imp) = 0`, `Ending − (Supply − Use) = 0`.

---

## 7. What is frozen vs extensible

- **Frozen:** the 11 long-tab columns (§2), the `series` vocabulary (§4), the period conventions (§5
  `period_type`/`period`), the file/folder location (§1), the formula binding style (§6).
- **Extensible without breaking wiring:** adding rows (new MY, new months, new vintages), adding a new
  `series` **only by mutual agreement** (then Desktop adds the line), adding wide-tab blocks.
- **A schema change = stop-the-world.** If the contract must change mid-sprint, both Claudes pause,
  agree, and Claude-Code republishes all affected flat files before Desktop resumes.
