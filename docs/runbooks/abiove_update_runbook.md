# Abiove (Brazil Soy Complex) — Update Runbook

**Purpose:** Refresh the Brazilian soy-complex series (crush, meal/oil production,
bean/meal/oil stocks) from Abiove and republish the flat file Desktop links to the
balance sheet. Monthly cadence (Abiove releases mid-month for the prior month).

**Data domain:** Six series, all in **thousand metric tons** (Abiove native "1.000 t";
per the rule "thousand tonnes for all non-US commodities unless otherwise noted").

---

## Why this is semi-manual (the extraction problem)

Abiove's crush/production/stocks series live **only inside a Power BI "publish to web"
report** on https://abiove.org.br/estatisticas-cadeia-da-soja-mensal/. **There is no API**,
and the one downloadable spreadsheet (`exp_YYYYMM.xlsx`) is exports-only (ComexStat) — it
does **not** contain crush/production/stocks. So the monthly input is an operator-extracted
workbook: Claude Desktop (or Tore) copies the Power BI pages into
`data/raw/oilseeds_fats_greases/brazil_crushing_data.xlsx`. Everything downstream is automated.

Tabs consumed from that workbook:
| Tab | Provides |
|---|---|
| `Tabela` | Monthly soybean **crush** history (years across cols, months down), 2012+ |
| `Estoques_Finais` | Monthly **final stocks** — 3 blocks Soja/Farelo/Óleo, 2021+ |
| `Balanco_Brasil` | Monthly processing-sector balance; **meal & oil production** live here (2025+). Two side-by-side year-blocks (e.g. 2025 in C–N, 2026 `(amostra)` in O–R) |
| `Balanco_Complexo_Anual` | Annual whole-complex balance 2014+ (kept in bronze; annual, not in the monthly flat file) |

---

## The chain

```
brazil_crushing_data.xlsx  (operator-extracted Power BI pages)
        │  scripts/load_abiove_crushing_data.py
        ▼
bronze.abiove_soy_complex   (long format, thousand MT, is_projection = "(amostra)")
        │  scripts/build_silver_abiove_monthly.py
        ▼
silver.monthly_realized     (country='BR', source='ABIOVE', unit='1000 MT')
        │  gold.abiove_soy_complex_monthly  (view, migration 143)
        ▼
scripts/write_abiove_flat_file.py
        ▼
models/Oilseeds/Brazil/brazil_soy_complex_monthly.xlsx   ← Desktop links the balance sheet here
```

## Steps

```bash
# 0. Desktop drops the refreshed workbook at
#    data/raw/oilseeds_fats_greases/brazil_crushing_data.xlsx

# 1. Load raw → bronze  (prints a Tabela-vs-Balanco crush reconciliation)
python scripts/load_abiove_crushing_data.py

# 2. Map bronze → silver.monthly_realized (BR/ABIOVE)
python scripts/build_silver_abiove_monthly.py

# 3. Republish the flat file
python scripts/write_abiove_flat_file.py
```

One-time (already applied): migrations `142_bronze_abiove_soy_complex.sql`,
`143_gold_abiove_soy_complex.sql`.

## Validation

- The loader's **crush reconciliation** (Tabela vs Balanco_Brasil) should show `diff=+0`
  for the finalized overlap year. Nonzero for the current `(amostra)` year is expected.
- Sanity: `SELECT sum(value) FROM gold.abiove_soy_complex_monthly WHERE series='crush' AND
  marketing_year=2025` ≈ **58,696** thousand t (Abiove published 2025 annual crush).
- Confirm the latest month advanced: `SELECT max(marketing_year*100+period) FROM
  gold.abiove_soy_complex_monthly`.

## Schema decision (recorded)

- **New `bronze.abiove_soy_complex`** — bronze mirrors the source's native shape; nothing
  existing could hold Abiove's Portuguese balance / thousand-tonne / sample-flagged layout
  (`bronze.nass_processing` is US/NASS-column-hardwired, no country field).
- **Reuse `silver.monthly_realized`** — already country-aware with a per-row `unit`; 5 of 6
  attributes already existed for soybeans. Brazil (`country='BR'`) sits next to US for free
  cross-country comparison. Only `seed_stocks` is a new attribute.
- **New `gold.abiove_soy_complex_monthly`** view — one per flat-file need, cheap.
- General rule: **new bronze almost always; reuse silver when the entity+grain is already
  modeled generically; new gold per consumer.**

## Gotchas (learned building this)

- **openpyxl `read_only=True` corrupts random access** across multiple sheets on one handle
  — it silently returned stale cells and mangled Jan–Apr values. Load with `read_only=False`.
- **`Balanco_Brasil` stacks multiple year-blocks side by side** (row 3 carries the block year,
  row 4 the months). Parse per-block or the later year overwrites the earlier.
- **`(amostra)` = sample/provisional** → `is_projection`/`is_preliminary=TRUE`, vintage `SAMPLE`
  (rank 90) so a later ACTUAL (rank 99) supersedes via MAXIFS. NB: currently only the
  `Balanco_Brasil`-sourced series (meal/oil production) carry the sample flag; crush (Tabela)
  and stocks (Estoques_Finais) 2026 are provisional too but not flagged — treat current-year as
  provisional regardless.
- **Units are thousand MT**, not tonnes and not lbs. Never sum BR (1000 MT) with US
  (short tons / lb) rows in `monthly_realized` without unit-normalizing.

## Known gaps / next increments

1. **Monthly meal & oil production before 2025** — only 2025+ exists in this workbook. Backfill
   from `…/Models/Oilseeds/World Soybean Balance Sheets.xlsx` tabs `Brazil Soymeal` / `Brazil
   Soyoil` (legacy history). Not yet done.
2. **Brazil biodiesel capacity** (`data/raw/biofuels/brazil_bio_capacity.xlsx`) — a separate
   domain (processing/refining unit capacity), not part of the six crush series. Load separately
   if/when needed.
3. **Live extraction** — if Abiove's Power BI `querydata` endpoint proves replayable, the
   operator-extract step could be automated; untested (deferred, low priority vs the manual copy).
