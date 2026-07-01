# Flat-File Contract — gold DB → flat files → balance sheets

**Status:** v1 (wheat pilot, generalizes to all commodities/countries).
**Owners:** Claude Code (plumbing: connectors → silver → gold → **writers**). Claude Desktop
(balance-sheet workbooks + the formulas that read these flat files). **The flat file is the seam.**

The rule of the seam: writers may re-sort or re-export rows freely; **key columns and header labels
are stable and never renamed; no non-data rows above the header (row 1).** Everything binds by key.

---

## 1. Two layouts: LONG default, WIDE for trade only

- **All series → LONG** (near-direct serialization of gold; the writer is a passthrough).
- **Trade/export/import → WIDE** (one standard template across all countries so the trade matrix
  overlays cleanly and reads visually). See §5.

---

## 2. LONG flat-file schema (the standard)

Fixed column order, row 1 = headers, data from row 2 down, sorted ascending by
`(series, class, marketing_year, period, vintage_rank)`:

| # | column | notes |
|---|--------|-------|
| 1 | `commodity` | `wheat`, `corn`, `soybeans`, `sugar`, … (lowercase) |
| 2 | `class` | `HRW/HRS/SRW/WHITE/DURUM`, or `ALL` for aggregate — **never blank** (keeps SUMIFS keys clean) |
| 3 | `series` | `area_planted`, `area_harvested`, `yield`, `production`, `seed_use`, `stocks`, `wheat_ground`, `flour_production`, `millfeed_production`, … |
| 4 | `marketing_year` | **numeric MY start year** (wheat MY starts Jun 1 → 2024 = 2024/25). Primary time key. |
| 5 | `period_type` | `annual` \| `quarter` \| `month` \| `week` |
| 6 | `period` | annual→`ANNUAL`; quarter→`Q1..Q4`; month→`1..12`; week→ISO `week_ending` date. Non-blank. |
| 7 | `vintage` | named estimate vintage (§3). Realized/actual data → `ACTUAL`. |
| 8 | `vintage_rank` | integer ordering; **balance sheet always takes MAX(rank)** for a key. `ACTUAL`/final = 99. |
| 9 | `value` | **RAW base units** (balance sheet does display scaling ÷1e6 etc.) |
| 10 | `unit` | `ACRES`, `BU`, `BU/ACRE`, `CWT`, `SHORT_TONS`, `USD/BU`, … |
| 11 | `source` | `NASS_QUICKSTATS`, `CENSUS_M311J`, `FAS_ESR`, `CENSUS_TRADE`, `AMS`, `WASDE` |
| 12 | `release_date` | when the source published this value (breaks vintage_rank ties) |
| 13 | `revision` | optional int/tag for post-final revisions |

**Key columns** (stable, never rename): 1–8 (`commodity, class, series, marketing_year,
period_type, period, vintage, vintage_rank`). `silver.<commodity>_series` carries these same
columns → the writer is a projection, not a transform.

---

## 3. Vintage vocabulary + rank (wheat supply; extend per commodity)

Rank orders releases within a marketing year; MAX wins. Gaps left for inserts.

| vintage | rank | report | timing | applies to |
|---------|-----:|--------|--------|------------|
| `WINTER_SEEDINGS` | 10 | Winter Wheat & Canola Seedings | mid-Jan | winter area_planted |
| `PROSPECTIVE` | 20 | Prospective Plantings | Mar 31 | area_planted |
| `ACREAGE` | 30 | Acreage | Jun 30 | area_planted, area_harvested |
| `CROP_PROD_<MON>` | 40+month | Crop Production (monthly) | in-season | yield, production (Aug=48, Sep=49…) |
| `SMALL_GRAINS` | 60 | Small Grains Summary | late Sep | area, yield, production (final survey) |
| `FINAL` | 90 | WASDE/ERS revised | annual+ | any |
| `ACTUAL` | 99 | realized (milling, trade, stocks) | — | monthly/quarterly realized series |

Derivation from bronze: NASS `reference_period` already encodes most of this
(`"YEAR - AUG FORECAST"`→`CROP_PROD_AUG`, `"YEAR"`→`FINAL`). Acreage-report vintages that share
`reference_period="YEAR"` are separated by `release_date` where available.

---

## 4. How the balance sheet reads LONG (the formula contract for Desktop)

- **Annual, pick-latest-vintage** for `(commodity, class, series, MY)`:
  ```
  r  = MAXIFS(rank, commodity=c, class=k, series=s, marketing_year=y)
  v  = SUMIFS(value, commodity=c, class=k, series=s, marketing_year=y, vintage_rank=r)
  ```
  (or `FILTER`/`INDEX-MATCH` on the same keys). Auto-upgrades Prospective→Acreage→…→Final as the
  writer fills new rows — no formula change ever needed.
- **Monthly/quarterly realized** (milling, stocks): `SUMIFS(value, year=y, period=p)` (`vintage=ACTUAL`).
- Desktop owns the exact formula; it must bind only to the §2 key columns + `vintage_rank`.

---

## 5. WIDE trade template (the one exception)

Standard across countries so the trade matrix overlays. **Reuse `us_grains_trade.xlsm`; match its
layout, don't recreate.** One sheet per `(commodity, flow)` e.g. `Wheat Exports`, `Wheat Imports`:

- Col A = `marketing_year` (or calendar year), Col B = `month` (or `MY_TOTAL`) — **stable keys**.
- Then one column per **partner country**, plus a `WORLD` total column. Partner headers stable.
- Optional `class`/HS dimension as a Col C key if needed.
- Fed by `census_trade` (monthly by partner) + `FAS_ESR` (weekly). Writer pivots gold-long → wide.

---

## 6. File / tab / refresh conventions

- File name: `<country>_<commodity>_<domain>.xlsx` (e.g. `us_wheat_production.xlsx`). Tabs = series groups.
- **`_meta` tab required**: `series | source | api | unit | vintage_set | last_updated | notes`.
- History default: MY **1990+** (or as deep as source allows); grains/oilseeds project default per
  `reference_history_start_dates`.
- **Writer semantics:** replace data rows below the header, preserve header + `_meta`; idempotent;
  rows sorted per §2. **Never touch the balance-sheet workbook** (Desktop's).
- Units: raw base units only; no silent conversion (`reference_units_source_vs_display`).
