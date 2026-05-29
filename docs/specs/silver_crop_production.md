# silver.crop_production — design

**Status:** v1 schema applied (mig 122). Collector at
`src/agents/collectors/us/nass_crop_production_collector.py`.

## Goal

Long/tidy crop production tracking at state + national level, preserving
full revision history so we can analyze how each season's estimate evolves
from first forecast to final.

## Scope

8 commodities, with class breakouts where NASS splits them:

| Commodity   | Class breakouts (NASS-native -> normalized)               |
|-------------|-----------------------------------------------------------|
| soybeans    | (only ALL CLASSES) -> `all_classes`                       |
| corn        | util_practice GRAIN/SILAGE -> `grain` / `silage`          |
| wheat       | WINTER / SPRING, (EXCL DURUM) / DURUM                     |
| sorghum     | util_practice GRAIN/SILAGE -> `grain` / `silage`          |
| canola      | (only ALL CLASSES)                                        |
| sunflower   | OIL TYPE / NON-OIL TYPE -> `oil_type` / `confection`      |
| cotton      | UPLAND / PIMA                                             |
| peanuts     | RUNNERS / SPANISH / VIRGINIAS & VALENCIAS -> `runner` /   |
|             | `spanish` / `virginia_valencia`                           |
| cottonseed  | derived elsewhere (not direct NASS query) — TODO          |

We store **both** the `all_classes` NASS rollup AND the broken-out classes.
Downstream picks the cut they need.

## Measures (statistics)

- `area_planted`
- `area_harvested`
- `yield`
- `production`
- `production_value` (NASS `PRODUCTION, MEASURED IN $`)
- `harvest_ratio` (derived in views as `area_harvested / area_planted`)

## Revision history

The natural key includes `release_date`, so every NASS release of the
same (commodity, class, statistic, geography, crop_year, reference_period)
appends a new row. This enables vintage analysis:

- AUG-2024 forecast as first published 2024-08-12 -> row A
- AUG-2024 forecast as revised in 2024-09 release -> row B
- FINAL 2024 published 2025-01-12 -> row C
- FINAL 2024 revised in 2026 annual summary -> row D

## Bronze->silver mapping

NASS QuickStats API fields -> silver.crop_production columns:

| NASS field                  | silver column        | Transform                                   |
|-----------------------------|----------------------|---------------------------------------------|
| `commodity_desc`            | `commodity`          | UPPER -> lowercased slug                    |
| `class_desc`                | `class`              | `ALL CLASSES` -> `all_classes`; class-specific normalization; for corn/sorghum see `util_practice_desc` |
| `util_practice_desc`        | `class` (override)   | `GRAIN` -> `grain`, `SILAGE` -> `silage`    |
| `statisticcat_desc`         | `statistic`          | `AREA PLANTED` -> `area_planted`, etc.      |
| `year`                      | `crop_year`          | INT                                         |
| `reference_period_desc`     | `reference_period`   | verbatim                                    |
| derived                     | `is_forecast`        | TRUE if `reference_period` contains `FORECAST` |
| derived                     | `source_report`      | See rule below                              |
| `load_time` -> date         | `release_date`       | YYYY-MM-DD slice                            |
| `agg_level_desc`            | `agg_level`          | verbatim (NATIONAL/STATE/AGRICULTURAL DISTRICT/COUNTY) |
| `state_alpha`               | `state_alpha`        | verbatim                                    |
| `state_fips_code`           | `state_fips`         | verbatim (99 for NATIONAL)                  |
| `asd_code`                  | `asd_code`           | DISTRICT level; '' otherwise                |
| `county_code`               | `county_ansi`        | COUNTY level; '' otherwise                  |
| `Value`                     | `value`              | parsed; NULL if `(D)`/`(NA)`/`(X)`/`(-)`    |
| `unit_desc`                 | `unit`               | verbatim                                    |
| `CV (%)`                    | `cv_pct`             | parsed; NULL if not provided                |
| `short_desc`                | `short_desc`         | verbatim                                    |

### `source_report` derivation

```
if reference_period contains 'FORECAST'         -> 'Crop Production' (monthly forecast)
elif reference_period = 'YEAR':
    if statistic = 'area_planted' and release.month = 3 -> 'Prospective Plantings'
    elif statistic = 'area_planted' and release.month = 6 -> 'Acreage'
    elif release.month = 1                          -> 'Annual Crop Production Summary'
    else                                            -> 'Crop Production' (revision)
```

## Companion: silver.crop_progress_condition

**v1 = schema stub only.** Weekly NASS crop progress + condition ratings.
Natural key on `(commodity, class, statistic, agg_level, state_fips,
crop_year, week_ending)`. Populated by a follow-up migration.

## Replacing the old layer

The following existing tables are now superseded:
- `bronze.nass_production` (59 rows, barely used)
- `bronze.nass_acreage` (30K rows of acreage backfill)
- `bronze.nass_state_yields` (3.7K wide-format rows)
- `silver.nass_production_summary` (59 rows)

After silver.crop_production is fully populated, mig 123 will rename
these to `*_deprecated_20260529` for a 30-day quarantine, then drop.
Do NOT write new code against the old tables.

## Collector usage

```bash
# Full backfill: all 8 commodities, all stats, 2000 forward
python -m src.agents.collectors.us.nass_crop_production_collector

# Targeted: peanut only, recent years
python -m src.agents.collectors.us.nass_crop_production_collector \
    --commodity peanuts --year-ge 2020 --year-le 2026

# State-level only (skip NATIONAL)
python -m src.agents.collectors.us.nass_crop_production_collector --agg STATE
```

## Open questions for follow-up

1. **cottonseed** is a derived stream — typically `cotton production * 1.6`
   (or NASS reports cotton lint + cottonseed separately). Decide whether
   to source it from NASS COTTONSEED commodity_desc (if it exists) or
   derive in a downstream view.
2. **District-level** ingest: schema supports it; v1 collector queries
   NATIONAL+STATE only. Add DISTRICT once we have a use case.
3. **CV(%) ingest reliability**: NASS exposes CV only for specific
   query shapes. Audit which (commodity, statistic) combinations
   actually populate `cv_pct` and document.
