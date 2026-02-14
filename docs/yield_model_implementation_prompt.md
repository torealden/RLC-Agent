# Yield Forecast Model — Implementation Prompt

## Instructions for the LLM

You are building a **state-level US row crop yield forecast model** for the RLC-Agent project. This is a PostgreSQL-backed agricultural data platform with a medallion architecture (Bronze/Silver/Gold). The system already has operational weather collectors, NASS crop data, CPC gridded condition/progress indices, NDVI infrastructure, GFS forecast scaffolding, and World Weather email parsing.

Your job is to create **6 new files** and **ensure 2 database schemas are migrated**, connecting all existing data streams into a yield prediction pipeline.

**Project root:** `C:\Users\torem\RLC Dropbox\RLC Team Folder\RLC-Agent`

---

## Standards and Conventions

### Database Connection Pattern

Every script that connects to PostgreSQL must use this pattern (from `.env` file):

```python
import os
from pathlib import Path
from dotenv import load_dotenv
import psycopg2

PROJECT_ROOT = Path(__file__).resolve().parent.parent  # Adjust .parent count to reach project root
load_dotenv(PROJECT_ROOT / ".env")

def get_db_connection():
    """Get PostgreSQL connection using .env credentials."""
    password = (
        os.environ.get("RLC_PG_PASSWORD")
        or os.environ.get("DATABASE_PASSWORD")
        or os.environ.get("DB_PASSWORD")
    )
    return psycopg2.connect(
        host=os.environ.get("DATABASE_HOST", "localhost"),
        port=os.environ.get("DATABASE_PORT", "5432"),
        database=os.environ.get("DATABASE_NAME", "rlc_commodities"),
        user=os.environ.get("DATABASE_USER", "postgres"),
        password=password,
    )
```

The `.env` file is at the project root. The password environment variable names vary — always try all three in order.

### Logging Pattern

All scripts use Python `logging` with this format:

```python
import logging

logger = logging.getLogger(__name__)

# In main() or CLI entry point:
logging.basicConfig(
    level=logging.INFO,  # or DEBUG if --verbose
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
```

### CLI Pattern

All scripts use `argparse` with subcommands where appropriate:

```python
def main():
    parser = argparse.ArgumentParser(description="...")
    parser.add_argument("--verbose", "-v", action="store_true")
    # ... subcommands or arguments
    args = parser.parse_args()

    log_level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(level=log_level, ...)
```

### Error Handling Pattern

Database operations use try/except with rollback:

```python
conn = get_db_connection()
cur = conn.cursor()
try:
    # ... operations
    conn.commit()
    logger.info(f"Success: {count} rows inserted")
except Exception as e:
    conn.rollback()
    logger.error(f"Database error: {e}")
    raise
finally:
    cur.close()
    conn.close()
```

### Upsert Pattern

Use `INSERT ... ON CONFLICT ... DO UPDATE SET` for idempotent inserts:

```python
cur.execute("""
    INSERT INTO schema.table (col1, col2, col3)
    VALUES (%s, %s, %s)
    ON CONFLICT (col1, col2)
    DO UPDATE SET
        col3 = EXCLUDED.col3,
        updated_at = NOW()
""", (val1, val2, val3))
```

---

## Existing Infrastructure Reference

### Database: `rlc_commodities` on `localhost:5432`

**Schemas:** `bronze`, `silver`, `gold`, `reference`

### Tables You Will Read From

| Table | Contents | Key Columns |
|-------|----------|-------------|
| `silver.weather_observation` | Hourly weather at 29 ag locations | `station_id`, `state`, `observation_time`, `temp_f`, `precip_in` |
| `bronze.cpc_file_manifest` | 2,749 CPC GeoTIFF metadata (2015–2025) | `series_id`, `year`, `nass_week`, `value_mean`, `qa_passed` |
| `bronze.cpc_region_stats` | National zonal statistics per CPC file | `series_id`, `year`, `nass_week`, `region_id`, `stat_name`, `value` |
| `bronze.cpc_series_catalog` | 8 series definitions (4 crops x 2 products) | `series_id`, `product` (condition/progress), `crop`, `valid_min`, `valid_max` |
| `gold.cpc_condition_weekly` | Pivoted condition index by crop/region/week | `crop`, `region_id`, `year`, `nass_week`, `condition_mean`, `condition_p10`, `condition_p90` |
| `gold.cpc_condition_yoy` | Year-over-year condition comparison | `crop`, `region_id`, `nass_week`, `current_mean`, `prior_year_mean`, `avg_5yr_mean`, `yoy_change` |
| `gold.cpc_progress_weekly` | Pivoted progress index by crop/region/week | `crop`, `region_id`, `year`, `nass_week`, `progress_mean` |
| `bronze.usda_nass` | NASS crop progress/condition tabular | `commodity`, `state`, `week_ending`, `value` |
| `bronze.nass_crop_condition` | Weekly condition ratings by state | `commodity`, `state`, `week_ending`, `excellent`, `good`, `fair`, `poor`, `very_poor` |
| `bronze.nass_crop_progress` | Weekly planting/emergence/maturity | `commodity`, `state`, `week_ending`, `progress_pct` |
| `bronze.fas_psd` | Global S&D balance sheets | `commodity`, `country_code`, `marketing_year`, `production` |
| `bronze.weather_emails` | World Weather email text | `email_date`, `subject`, `extracted_text` |
| `bronze.ndvi_observation` | NDVI time series (schema exists, may be empty) | `region_code`, `observation_date`, `ndvi_value`, `ndvi_anomaly` |
| `silver.weather_forecast_daily` | GFS forecast daily (schema exists, may be empty) | `forecast_date`, `valid_date`, `region_code`, `precip_mm`, `tmin_c`, `tmax_c`, `gdd_corn` |
| `reference.crop_region` | 14 key crop regions with metadata | `region_code`, `region_name`, `country_code`, `primary_commodity`, `states_provinces` |

### Key Configuration Files

**`config/weather_yield_thresholds.json`** — Crop-specific agronomic parameters:
```json
{
  "crops": {
    "corn": {
      "gdd_base_c": 10,
      "gdd_cap_c": 30,
      "heat_stress_threshold_c": 30,
      "severe_heat_threshold_c": 35,
      "frost_threshold_c": 0,
      "drought_threshold_mm_week": 13,
      "excess_moisture_mm_week": 75,
      "growth_stages": {
        "planting": {"start_month": 4, "start_day": 15, "end_month": 5, "end_day": 25},
        "pollination": {"start_month": 7, "start_day": 10, "end_month": 8, "end_day": 5, "critical": true},
        "grain_fill": {"start_month": 8, "start_day": 1, "end_month": 9, "end_day": 10}
      },
      "yield_impacts": {
        "drought_2wk_pollination_pct": [-15, -40],
        "heat_3day_100f_pollination_pct": [-10, -25]
      }
    },
    "soybeans": { "gdd_base_c": 10, "flowering": {"start_month": 7, "start_day": 15, "end_month": 8, "end_day": 15, "critical": true} },
    "wheat": { "gdd_base_c": 0, "heading": {"start_month": 5, "start_day": 1, "end_month": 5, "end_day": 25, "critical": true} }
  },
  "regions": {
    "US_CORN_BELT": { "states": ["IA", "IL", "NE", "MN", "IN", "OH", "SD", "WI", "MO", "KS"], "production_share_corn_pct": 65 }
  },
  "anomaly_thresholds": {
    "precipitation": { "severe_deficit_pct": -30, "normal_range_pct": [-15, 20], "excess_pct": 50 },
    "temperature": { "normal_range_c": [-1.5, 1.5] }
  }
}
```

**Read this file at runtime** — do not hardcode values. Load with:
```python
import json
THRESHOLDS_PATH = PROJECT_ROOT / "config" / "weather_yield_thresholds.json"
with open(THRESHOLDS_PATH) as f:
    THRESHOLDS = json.load(f)
```

### Existing Collectors/Services to Reuse

| File | Class/Function | Use For |
|------|---------------|---------|
| `src/services/api/world_weather_service.py` | `WorldWeatherParser.parse_email()` | Parse WW email text → `WorldWeatherReport` with structured changes |
| `src/services/api/world_weather_service.py` | `WeatherRegion` enum | 8 ag weather regions |
| `src/agents/collectors/global/gfs_forecast_collector.py` | `GFSCollector.calculate_gdd()` | GDD calculation with base/cap temp |
| `src/agents/collectors/global/gfs_forecast_collector.py` | `CROP_REGION_BOUNDS` | 14 regions with lat/lon bounding boxes |
| `src/agents/collectors/global/ndvi_collector.py` | `AppEEARSClient` | NASA NDVI time series (if credentials available) |
| `scripts/ingest_crop_production_summary.py` | `parse_file()` | Parse NASS crop production text → state-level yields (currently SQLite) |

### CPC Series IDs

The 8 CPC gridded data series are:
- `cpc_cond_corn_9km_v1`, `cpc_cond_soybeans_9km_v1`, `cpc_cond_cotton_9km_v1`, `cpc_cond_winter_wheat_9km_v1`
- `cpc_prog_corn_9km_v1`, `cpc_prog_soybeans_9km_v1`, `cpc_prog_cotton_9km_v1`, `cpc_prog_winter_wheat_9km_v1`

Condition index: 1.0 (very poor) to 5.0 (excellent). Progress index: 0.0 to 1.0.

### State Abbreviations for Weather Mapping

Weather observations use full state names in `silver.weather_observation.state`. CPC data uses `region_id = 'US'` for national. NASS tabular uses full state names. You will need to map between these.

---

## File 1: `database/migrations/026_yield_model_tables.sql`

Create these objects in the `rlc_commodities` database. Use `CREATE TABLE IF NOT EXISTS` and `CREATE OR REPLACE VIEW` for idempotency.

### Table: `bronze.nass_state_yields`

Historical state-level crop yields — the training target for the yield model.

```sql
CREATE TABLE IF NOT EXISTS bronze.nass_state_yields (
    id              SERIAL PRIMARY KEY,
    commodity       TEXT NOT NULL,          -- 'CORN', 'SOYBEANS', 'WHEAT_WINTER', 'WHEAT_SPRING', 'COTTON'
    state           TEXT NOT NULL,          -- Full name 'Iowa' or abbreviation 'IA'
    state_abbrev    TEXT,                   -- 2-letter abbreviation
    year            INTEGER NOT NULL,
    area_planted    FLOAT,                 -- 1,000 acres
    area_harvested  FLOAT,                 -- 1,000 acres
    yield_per_acre  FLOAT,                 -- bushels/acre (or lbs/acre for cotton)
    production      FLOAT,                 -- 1,000 bushels (or 1,000 480-lb bales for cotton)
    yield_unit      TEXT DEFAULT 'bu/acre',
    production_unit TEXT DEFAULT '1000_bu',
    source          TEXT DEFAULT 'NASS_CROP_PRODUCTION',
    collected_at    TIMESTAMPTZ DEFAULT NOW(),

    CONSTRAINT uq_state_yield UNIQUE (commodity, state, year)
);

CREATE INDEX IF NOT EXISTS idx_state_yields_commodity ON bronze.nass_state_yields(commodity);
CREATE INDEX IF NOT EXISTS idx_state_yields_state ON bronze.nass_state_yields(state);
CREATE INDEX IF NOT EXISTS idx_state_yields_year ON bronze.nass_state_yields(year);
```

### Table: `silver.yield_trend`

Trend yield coefficients per state/crop for detrending.

```sql
CREATE TABLE IF NOT EXISTS silver.yield_trend (
    id              SERIAL PRIMARY KEY,
    commodity       TEXT NOT NULL,
    state           TEXT NOT NULL,
    trend_type      TEXT NOT NULL DEFAULT 'linear',  -- 'linear', 'quadratic'
    intercept       FLOAT NOT NULL,
    slope           FLOAT NOT NULL,
    slope_quadratic FLOAT,                           -- For quadratic trend
    r_squared       FLOAT,
    years_used      TEXT,                            -- e.g. '2000-2024'
    trend_yield_current FLOAT,                       -- Projected trend for current year
    updated_at      TIMESTAMPTZ DEFAULT NOW(),

    CONSTRAINT uq_yield_trend UNIQUE (commodity, state, trend_type)
);
```

### Table: `silver.yield_features`

Weekly feature vectors — the model input table. One row per (state, crop, year, week).

```sql
CREATE TABLE IF NOT EXISTS silver.yield_features (
    id                  SERIAL PRIMARY KEY,
    state               TEXT NOT NULL,
    crop                TEXT NOT NULL,          -- 'corn', 'soybeans', 'winter_wheat', 'cotton'
    year                INTEGER NOT NULL,
    week                INTEGER NOT NULL,       -- ISO week or NASS week
    week_ending_date    DATE,

    -- Weather features (cumulative from planting)
    gdd_cum             FLOAT,                 -- Cumulative GDD from planting date
    gdd_vs_normal_pct   FLOAT,                 -- % deviation from 30yr normal
    precip_cum_mm       FLOAT,                 -- Cumulative precipitation
    precip_vs_normal_pct FLOAT,                -- % deviation from normal
    stress_days_heat    INTEGER DEFAULT 0,     -- Days with Tmax > heat threshold
    stress_days_drought INTEGER DEFAULT 0,     -- Consecutive dry days during critical window
    excess_moisture_days INTEGER DEFAULT 0,    -- Days with precip > excess threshold
    frost_events        INTEGER DEFAULT 0,     -- Frost events since planting

    -- Temperature summary for current week
    tmax_weekly_avg     FLOAT,
    tmin_weekly_avg     FLOAT,
    tavg_weekly         FLOAT,

    -- NDVI / vegetation features
    ndvi_mean           FLOAT,                 -- Current NDVI value
    ndvi_anomaly        FLOAT,                 -- Deviation from historical
    ndvi_trend_4wk      FLOAT,                 -- 4-week linear trend slope

    -- CPC gridded condition/progress
    condition_index     FLOAT,                 -- CPC condition mean (1-5 scale)
    condition_vs_5yr    FLOAT,                 -- Deviation from 5yr avg
    progress_index      FLOAT,                 -- CPC progress mean (0-1 scale)
    progress_vs_normal  FLOAT,                 -- Deviation from normal pace

    -- NASS tabular crop status
    pct_planted         FLOAT,
    pct_emerged         FLOAT,
    pct_silking         FLOAT,                 -- Corn-specific (silking/podding/heading)
    pct_dough           FLOAT,                 -- Corn/wheat-specific
    pct_mature          FLOAT,
    pct_harvested       FLOAT,
    good_excellent_pct  FLOAT,                 -- G/E condition rating from NASS

    -- World Weather signals
    ww_risk_score       FLOAT,                 -- 0-10 composite risk score
    ww_outlook_sentiment FLOAT,                -- -1 (bearish) to +1 (bullish)

    -- Growth stage
    growth_stage        TEXT,                   -- 'planting', 'vegetative', 'reproductive', 'maturity'

    -- Metadata
    feature_version     TEXT DEFAULT 'v1',
    updated_at          TIMESTAMPTZ DEFAULT NOW(),

    CONSTRAINT uq_yield_features UNIQUE (state, crop, year, week)
);

CREATE INDEX IF NOT EXISTS idx_yf_crop_year ON silver.yield_features(crop, year);
CREATE INDEX IF NOT EXISTS idx_yf_state_crop ON silver.yield_features(state, crop);
CREATE INDEX IF NOT EXISTS idx_yf_week_date ON silver.yield_features(week_ending_date);
```

### Table: `silver.yield_model_run`

Model execution log for auditing and debugging.

```sql
CREATE TABLE IF NOT EXISTS silver.yield_model_run (
    run_id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    run_ts          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    model_version   TEXT NOT NULL,
    model_type      TEXT NOT NULL,          -- 'trend_regression', 'random_forest', 'analog', 'ensemble'
    crops_processed TEXT,                   -- 'corn,soybeans' or 'all'
    states_processed TEXT,
    forecast_week   INTEGER,
    training_years  TEXT,                   -- '2000-2023'
    feature_count   INTEGER,
    rmse_cv         FLOAT,
    mae_cv          FLOAT,
    r2_cv           FLOAT,
    notes           TEXT,
    duration_sec    FLOAT
);
```

### Table: `gold.yield_forecast`

Yield projections — the primary output table.

```sql
CREATE TABLE IF NOT EXISTS gold.yield_forecast (
    id                      SERIAL PRIMARY KEY,
    run_id                  UUID REFERENCES silver.yield_model_run(run_id),
    commodity               TEXT NOT NULL,
    state                   TEXT NOT NULL,
    year                    INTEGER NOT NULL,
    forecast_week           INTEGER NOT NULL,
    forecast_date           DATE NOT NULL,

    -- Yield prediction
    yield_forecast          FLOAT NOT NULL,    -- bu/acre
    yield_low               FLOAT,             -- Lower bound (10th percentile)
    yield_high              FLOAT,             -- Upper bound (90th percentile)

    -- Context
    trend_yield             FLOAT,             -- Expected from trend alone
    vs_trend_pct            FLOAT,             -- % deviation from trend
    last_year_yield         FLOAT,
    vs_last_year_pct        FLOAT,
    avg_5yr_yield           FLOAT,

    -- Model details
    model_type              TEXT,               -- Which model produced this
    confidence              FLOAT,              -- 0-1 confidence score
    primary_driver          TEXT,               -- 'weather_stress', 'strong_conditions', etc.
    analog_years            TEXT,               -- 'Similar to 2017, 2021' (from analog model)

    -- National rollup (only for state='US')
    national_production_est FLOAT,             -- Million bushels
    national_yield_est      FLOAT,             -- Bu/acre weighted avg

    -- Week-over-week tracking
    prev_week_forecast      FLOAT,
    wow_change              FLOAT,             -- Week-over-week change in bu/acre

    created_at              TIMESTAMPTZ DEFAULT NOW(),

    CONSTRAINT uq_yield_forecast UNIQUE (commodity, state, year, forecast_week, model_type)
);

CREATE INDEX IF NOT EXISTS idx_yf_commodity_year ON gold.yield_forecast(commodity, year);
CREATE INDEX IF NOT EXISTS idx_yf_forecast_week ON gold.yield_forecast(forecast_week);
CREATE INDEX IF NOT EXISTS idx_yf_run_id ON gold.yield_forecast(run_id);
```

### View: `gold.yield_monitor`

Weekly dashboard view joining forecast + features + CPC condition.

```sql
CREATE OR REPLACE VIEW gold.yield_monitor AS
SELECT
    yf.commodity,
    yf.state,
    yf.year,
    yf.forecast_week,
    yf.forecast_date,
    yf.yield_forecast,
    yf.yield_low,
    yf.yield_high,
    yf.trend_yield,
    yf.vs_trend_pct,
    yf.confidence,
    yf.primary_driver,
    yf.analog_years,
    yf.prev_week_forecast,
    yf.wow_change,
    -- Features
    feat.gdd_cum,
    feat.gdd_vs_normal_pct,
    feat.precip_cum_mm,
    feat.precip_vs_normal_pct,
    feat.stress_days_heat,
    feat.stress_days_drought,
    feat.condition_index,
    feat.condition_vs_5yr,
    feat.good_excellent_pct,
    feat.growth_stage,
    feat.ww_risk_score,
    -- Risk assessment
    CASE
        WHEN feat.stress_days_drought > 7 AND feat.growth_stage = 'reproductive' THEN 'HIGH: Drought during critical window'
        WHEN feat.stress_days_heat > 5 AND feat.growth_stage = 'reproductive' THEN 'HIGH: Heat stress during critical window'
        WHEN feat.condition_vs_5yr < -0.5 THEN 'ELEVATED: Below-average condition'
        WHEN feat.precip_vs_normal_pct < -25 THEN 'MODERATE: Below-normal precipitation'
        WHEN feat.gdd_vs_normal_pct < -10 THEN 'LOW: Behind-pace development'
        ELSE 'NORMAL'
    END AS risk_level
FROM gold.yield_forecast yf
LEFT JOIN silver.yield_features feat
    ON yf.state = feat.state
    AND LOWER(yf.commodity) = feat.crop
    AND yf.year = feat.year
    AND yf.forecast_week = feat.week
WHERE yf.model_type = 'ensemble'
ORDER BY yf.commodity, yf.state, yf.forecast_week DESC;
```

Also include in this migration:

1. **Ensure schemas 014 and 015 are migrated.** Check whether `bronze.ndvi_observation`, `silver.weather_forecast_daily`, and `reference.crop_region` exist. If not, run those schema files. You can include `CREATE TABLE IF NOT EXISTS` statements or reference the existing files.

2. **Seed `reference.weather_climatology`** with placeholder monthly normals for the US crop regions. These can be updated later with real climatology data. Include at minimum US_CORN_BELT, US_SOY_BELT, US_WHEAT_WINTER, US_WHEAT_SPRING with monthly precip and temp normals for the growing season (April–October).

---

## File 2: `scripts/migrate_yields_to_postgres.py`

Migrate historical state-level yield data from the existing SQLite database to PostgreSQL.

### Location
`scripts/migrate_yields_to_postgres.py`

### Requirements

1. Read from SQLite at `data/rlc_commodities.db`, table `nass_crop_production`
2. If SQLite has no data, or for backfill, fetch from NASS QuickStats API:
   - API endpoint: `https://quickstats.nass.usda.gov/api/api_GET/`
   - Requires API key in env var `NASS_API_KEY`
   - Query: commodity_desc=CORN, statisticcat_desc=YIELD, agg_level_desc=STATE, year__GE=2000
   - Do this for CORN, SOYBEANS, WHEAT (all types), COTTON
3. Upsert into `bronze.nass_state_yields`
4. After loading yields, compute trend lines:
   - For each (commodity, state) combination with >= 10 years of data
   - Fit linear regression: `yield = intercept + slope * year`
   - Also fit quadratic: `yield = intercept + slope * year + slope_quadratic * year^2`
   - Store in `silver.yield_trend`
   - Project current year's trend yield
5. Print summary: rows loaded per commodity, trend R-squared distribution

### CLI

```
python scripts/migrate_yields_to_postgres.py                    # Migrate from SQLite
python scripts/migrate_yields_to_postgres.py --fetch-api        # Fetch from NASS QuickStats
python scripts/migrate_yields_to_postgres.py --compute-trends   # Only recompute trends
python scripts/migrate_yields_to_postgres.py --verify           # Print verification summary
```

### Key Details

- Handle commodity name mapping: SQLite uses 'CORN', 'SOYBEANS', 'WHEAT_ALL', 'WHEAT_WINTER', etc. Normalize to consistent names.
- State names: Store both full name and 2-letter abbreviation. Include a US_STATE_ABBREVS dict for mapping.
- Skip 'US' national totals when computing state-level trends (but include them in the bronze table).
- Use `numpy` or `scipy` for linear regression. Include R-squared calculation.
- Log warnings for states with unusual trend slopes (> 3 bu/acre/year for corn suggests data issues).

---

## File 3: `src/models/yield_feature_engine.py`

The core feature engineering pipeline that transforms raw data into model-ready features.

### Location
`src/models/yield_feature_engine.py`

### Class: `YieldFeatureEngine`

```python
class YieldFeatureEngine:
    """
    Transforms raw weather, CPC, NASS, NDVI, and World Weather data
    into weekly feature vectors stored in silver.yield_features.
    """

    def __init__(self, conn=None):
        """Initialize with optional DB connection. Loads thresholds from config."""

    def build_features(self, state: str, crop: str, year: int,
                       week_start: int = 1, week_end: int = None) -> int:
        """
        Build feature vectors for a specific state/crop/year.
        Returns count of feature rows created/updated.
        """

    def build_all_features(self, year: int, crops: list = None,
                           states: list = None) -> dict:
        """
        Build features for all state/crop combinations.
        Returns summary dict.
        """

    def _get_weather_features(self, state: str, crop: str, year: int, week: int) -> dict:
        """Query silver.weather_observation for weather features."""

    def _get_cpc_features(self, crop: str, year: int, week: int) -> dict:
        """Query bronze.cpc_region_stats and gold views for CPC features."""

    def _get_nass_progress(self, crop: str, state: str, year: int, week: int) -> dict:
        """Query bronze.nass_crop_progress and bronze.nass_crop_condition."""

    def _get_ndvi_features(self, state: str, crop: str, year: int, week: int) -> dict:
        """Query bronze.ndvi_observation (or use CPC condition as fallback)."""

    def _get_ww_signals(self, year: int, week: int) -> dict:
        """Parse World Weather email signals for the given week."""

    def _determine_growth_stage(self, crop: str, week: int) -> str:
        """Map calendar week to growth stage using weather_yield_thresholds.json."""

    def _calculate_gdd(self, tmin_f: float, tmax_f: float, crop: str) -> float:
        """Calculate GDD for one day. Convert F to C internally."""
```

### Feature Calculation Details

**Weather features (`_get_weather_features`)**:

Query `silver.weather_observation` for all stations in the given state. For each week:

```sql
SELECT
    DATE_TRUNC('week', observation_time) as week,
    AVG(temp_f) as avg_temp,
    MAX(temp_f) as max_temp,
    MIN(temp_f) as min_temp,
    SUM(precip_in) * 25.4 as precip_mm  -- Convert inches to mm
FROM silver.weather_observation
WHERE state = %s
  AND observation_time >= %s AND observation_time < %s
GROUP BY DATE_TRUNC('week', observation_time)
```

Then compute:
- `gdd_cum`: Sum of daily GDD from planting date through current week. Use crop-specific base temp from `weather_yield_thresholds.json`. Convert F→C: `(temp_f - 32) * 5/9`.
- `gdd_vs_normal_pct`: Compare to `reference.weather_climatology` if available, otherwise use hardcoded normals.
- `precip_cum_mm`: Sum of precipitation from planting date.
- `stress_days_heat`: Count days where max temp exceeds heat threshold (from thresholds config).
- `stress_days_drought`: Consecutive days with precip < drought threshold during critical growth stages.
- `frost_events`: Count days where min temp < frost threshold after planting.

**CPC features (`_get_cpc_features`)**:

CPC data is national-level (region_id='US'), not state-level. Use it as a national proxy.

```sql
-- Current condition
SELECT condition_mean, condition_p10, condition_p90
FROM gold.cpc_condition_weekly
WHERE crop = %s AND year = %s AND nass_week = %s AND region_id = 'US'

-- Comparison to 5yr average
SELECT current_mean, avg_5yr_mean, yoy_change, vs_5yr_avg
FROM gold.cpc_condition_yoy
WHERE crop = %s AND nass_week = %s

-- Progress
SELECT progress_mean
FROM gold.cpc_progress_weekly
WHERE crop = %s AND year = %s AND nass_week = %s AND region_id = 'US'
```

Map CPC crop names: CPC uses 'corn', 'soybeans', 'cotton', 'winter_wheat'. NASS tabular may use 'CORN', 'SOYBEANS', etc.

**NASS tabular progress (`_get_nass_progress`)**:

```sql
SELECT value as progress_pct
FROM bronze.nass_crop_progress
WHERE commodity = %s AND state = %s AND week_ending >= %s AND week_ending < %s

SELECT excellent, good, fair, poor, very_poor
FROM bronze.nass_crop_condition
WHERE commodity = %s AND state = %s AND week_ending >= %s AND week_ending < %s
```

Compute `good_excellent_pct = excellent + good`.

**World Weather signals (`_get_ww_signals`)**:

```sql
SELECT extracted_text
FROM bronze.weather_emails
WHERE email_date >= %s AND email_date < %s
ORDER BY email_date DESC LIMIT 5
```

Parse each email with `WorldWeatherParser` from `src/services/api/world_weather_service.py`. Compute:
- `ww_risk_score`: 0–10 based on count and severity of WeatherChange objects with direction != 'unchanged'. Weight changes by region relevance (US changes score higher for US crops).
- `ww_outlook_sentiment`: Simple keyword scoring. Words like "favorable", "improving", "adequate" → positive. Words like "drought", "stress", "drier-bias", "flooding" → negative. Normalize to [-1, +1].

**Growth stage determination (`_determine_growth_stage`)**:

Use `weather_yield_thresholds.json` growth_stages dictionary. Map current week to a date, compare against start/end dates for each stage. Return the matching stage name.

### CLI

```
python -m src.models.yield_feature_engine --state IA --crop corn --year 2024
python -m src.models.yield_feature_engine --all --year 2024
python -m src.models.yield_feature_engine --all --year 2024 --crop corn
python -m src.models.yield_feature_engine --verify
```

### Important Notes

- Weather data is in Fahrenheit in the database. Convert to Celsius for GDD calculations.
- Not all states have weather stations in `silver.weather_observation`. The 29 stations cover major ag states but not all 50. For states without observations, use the nearest station or regional average.
- CPC gridded data is national — it provides a national condition index. State-level condition comes from NASS tabular data.
- If `bronze.ndvi_observation` is empty (likely), fall back to using CPC condition index as a vegetation proxy. Set `ndvi_mean = None` and `ndvi_anomaly = None` for those rows.
- NASS week numbers may not match ISO week numbers exactly. Use `week_ending_date` for alignment.

---

## File 4: `src/models/yield_prediction_model.py`

The multi-model yield prediction engine.

### Location
`src/models/yield_prediction_model.py`

### Dependencies
```python
import numpy as np
from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor
from sklearn.linear_model import LinearRegression
from sklearn.model_selection import cross_val_score, LeaveOneGroupOut
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score
from sklearn.preprocessing import StandardScaler
import pickle
from pathlib import Path
```

### Class: `YieldPredictionModel`

```python
class YieldPredictionModel:
    """
    Multi-model ensemble for crop yield prediction.

    Models:
    A. Trend-Adjusted Regression — baseline, interpretable
    B. Random Forest — nonlinear interactions, feature importance
    C. Analog Year — "this year looks like..." narrative

    Ensemble: Weighted combination adjusted by growth stage.
    """

    def __init__(self, model_dir: Path = None):
        """Initialize. model_dir stores trained model artifacts."""
        self.model_dir = model_dir or PROJECT_ROOT / "models" / "yield"
        self.model_dir.mkdir(parents=True, exist_ok=True)

    def train(self, crop: str, states: list = None,
              train_years: range = range(2000, 2024)) -> dict:
        """
        Train all three sub-models for a given crop.

        1. Query silver.yield_features + bronze.nass_state_yields
        2. Build feature matrix X and target vector y
        3. Train Models A, B, C
        4. Evaluate via leave-one-year-out cross-validation
        5. Save trained models to disk
        6. Return training metrics dict
        """

    def predict(self, crop: str, year: int, week: int,
                states: list = None) -> list:
        """
        Generate yield predictions for current conditions.

        1. Load trained models
        2. Query current week's features from silver.yield_features
        3. Run all three models
        4. Combine via ensemble weights (adjusted by growth stage)
        5. Generate prediction intervals
        6. Return list of YieldPrediction objects
        """

    def _train_model_a(self, X, y, feature_names) -> dict:
        """
        Model A: Trend-Adjusted Regression

        yield_deviation = f(gdd_departure, july_precip_departure,
                           condition_ge_pct, heat_stress_days)

        actual_yield = trend_yield + yield_deviation

        Use only 4-6 key features for interpretability.
        """

    def _train_model_b(self, X, y, feature_names) -> dict:
        """
        Model B: Random Forest / Gradient Boosting

        Use all available features from silver.yield_features.

        GradientBoostingRegressor(
            n_estimators=200, max_depth=4, learning_rate=0.1,
            min_samples_leaf=5, subsample=0.8
        )

        Return feature importance rankings.
        """

    def _train_model_c(self, X, y, years, feature_names) -> dict:
        """
        Model C: Analog Year

        For each target year:
        1. Compute feature distance to all other years (Euclidean on standardized features)
        2. Find top-5 most similar years
        3. Weight their yields by inverse distance
        4. Return weighted average yield + list of analog years

        Distance uses only weather + condition features (not progress).
        """

    def _get_ensemble_weights(self, crop: str, week: int) -> dict:
        """
        Return model weights based on growth stage.

        Returns dict like {'model_a': 0.5, 'model_b': 0.3, 'model_c': 0.2}

        Corn example:
        - Weeks 1-16 (pre-planting):   A=0.6, B=0.2, C=0.2
        - Weeks 17-26 (vegetative):    A=0.4, B=0.35, C=0.25
        - Weeks 27-35 (reproductive):  A=0.2, B=0.5, C=0.3
        - Weeks 36+ (maturity):        A=0.3, B=0.4, C=0.3
        """

    def _prediction_interval(self, predictions: list, historical_rmse: float,
                             week: int) -> tuple:
        """
        Compute prediction interval.

        Width = historical_rmse * confidence_multiplier_by_week
        Earlier weeks → wider intervals (more uncertainty)

        confidence_multiplier:
        - Week 15 (early): 2.5
        - Week 25 (mid):   1.8
        - Week 30 (late):  1.3
        - Week 38 (final): 1.0
        """
```

### Data Class: `YieldPrediction`

```python
@dataclass
class YieldPrediction:
    commodity: str
    state: str
    year: int
    forecast_week: int
    forecast_date: date
    yield_forecast: float       # bu/acre
    yield_low: float            # 10th percentile
    yield_high: float           # 90th percentile
    trend_yield: float
    vs_trend_pct: float
    last_year_yield: float
    vs_last_year_pct: float
    model_type: str             # 'ensemble', 'trend_regression', etc.
    confidence: float           # 0-1
    primary_driver: str         # Human-readable explanation
    analog_years: str           # From Model C
    feature_importance: dict    # Top 5 features from Model B
```

### Feature Selection for Each Model

**Model A features** (4-6 interpretable features):
1. `gdd_vs_normal_pct` — GDD deviation from normal
2. `precip_vs_normal_pct` — Precipitation deviation (emphasize July for corn)
3. `good_excellent_pct` — NASS G/E condition rating
4. `stress_days_heat` — Heat stress days
5. `condition_index` — CPC condition index (if available)

**Model B features** (all available features from `silver.yield_features`):
- Use all numeric columns except identifiers (state, crop, year, week)
- Handle NULLs: fill with 0 or column median
- StandardScaler for normalization

**Model C distance calculation**:
- Use only: `gdd_vs_normal_pct`, `precip_vs_normal_pct`, `stress_days_heat`, `stress_days_drought`, `condition_index`, `good_excellent_pct`
- Standardize before computing Euclidean distance
- Compare feature profiles at the same week of season

### Model Persistence

Save trained models as pickle files:
```
models/yield/corn_model_a.pkl
models/yield/corn_model_b.pkl
models/yield/corn_model_c_data.pkl   # Feature profiles for analog comparison
models/yield/corn_scaler.pkl
models/yield/corn_metadata.json      # Training metrics, feature names, training years
```

### National Aggregation

For national yield estimate:
1. Get state-level predictions for all states
2. Query `bronze.nass_state_yields` for most recent year's `area_harvested` per state
3. `state_production = state_yield_forecast * state_area_harvested`
4. `national_production = sum(state_production)`
5. `national_yield = national_production / sum(state_area_harvested)`
6. Store national estimate in `gold.yield_forecast` with `state = 'US'`

---

## File 5: `src/models/yield_orchestrator.py`

The integration orchestrator that ties everything together.

### Location
`src/models/yield_orchestrator.py`

### Class: `YieldOrchestrator`

```python
class YieldOrchestrator:
    """
    Weekly execution pipeline for yield forecasting.

    Workflow:
    1. Check data freshness (what's been updated since last run)
    2. Run feature engineering for current week
    3. Load/train prediction models
    4. Generate forecasts
    5. Save to gold.yield_forecast
    6. Log run to silver.yield_model_run
    7. Generate monitoring report
    """

    def __init__(self):
        self.feature_engine = YieldFeatureEngine()
        self.model = YieldPredictionModel()

    def run(self, week: int = None, year: int = None,
            crops: list = None, states: list = None) -> dict:
        """
        Execute full weekly forecast pipeline.

        Default: current week, current year, all crops, all states.
        Returns summary dict with forecast counts, key changes, alerts.
        """

    def train(self, crops: list = None,
              train_years: range = range(2000, 2024)) -> dict:
        """
        Train/retrain all models.

        1. Build historical features (if not already in silver.yield_features)
        2. Train models for each crop
        3. Report cross-validation metrics
        """

    def backtest(self, test_years: range = range(2020, 2025),
                 crops: list = None) -> dict:
        """
        Run backtesting over historical years.

        For each test year:
        1. Train on all other years
        2. Predict test year at multiple weeks (18, 22, 26, 30, 34, 38)
        3. Compare to actual yield
        4. Record RMSE, MAE, directional accuracy
        """

    def report(self, year: int = None, crop: str = None,
               format: str = 'markdown') -> str:
        """
        Generate yield forecast report.

        Sections:
        - National yield estimate vs WASDE/trend
        - State-level top gainers/losers vs trend
        - Key risk factors
        - Week-over-week changes
        - Analog year comparison
        - Weather outlook impact
        """

    def monitor(self) -> str:
        """
        Print dashboard summary of current forecasts.

        Pull from gold.yield_monitor view.
        """

    def _check_data_freshness(self) -> dict:
        """
        Check when each data source was last updated.

        Returns dict like:
        {
            'weather_obs': {'latest': '2026-02-12', 'rows_7d': 1200},
            'cpc_gridded': {'latest_week': 5, 'latest_year': 2026},
            'nass_progress': {'latest_week_ending': '2026-02-09'},
            'world_weather': {'latest_email': '2026-02-12', 'count_7d': 14},
        }
        """

    def _get_current_nass_week(self) -> int:
        """Map current date to NASS week number."""
```

### CLI

```python
def main():
    parser = argparse.ArgumentParser(description="Yield Forecast Orchestrator")
    subparsers = parser.add_subparsers(dest='command')

    # Run forecast
    run_parser = subparsers.add_parser('run', help='Run weekly forecast')
    run_parser.add_argument('--week', type=int, help='NASS week (default: current)')
    run_parser.add_argument('--year', type=int, help='Year (default: current)')
    run_parser.add_argument('--crop', type=str, help='Specific crop')
    run_parser.add_argument('--state', type=str, help='Specific state')

    # Train models
    train_parser = subparsers.add_parser('train', help='Train models')
    train_parser.add_argument('--years', type=str, default='2000-2023',
                             help='Training year range (e.g., 2000-2023)')
    train_parser.add_argument('--crop', type=str, help='Specific crop')

    # Backtest
    bt_parser = subparsers.add_parser('backtest', help='Run backtesting')
    bt_parser.add_argument('--years', type=str, default='2020-2024',
                          help='Test year range')
    bt_parser.add_argument('--crop', type=str, help='Specific crop')

    # Report
    report_parser = subparsers.add_parser('report', help='Generate report')
    report_parser.add_argument('--format', choices=['markdown', 'text', 'json'],
                              default='markdown')
    report_parser.add_argument('--crop', type=str)

    # Monitor
    subparsers.add_parser('monitor', help='Dashboard view')

    # Data check
    subparsers.add_parser('check', help='Check data freshness')

    parser.add_argument('--verbose', '-v', action='store_true')
    args = parser.parse_args()
```

### Report Format (Markdown)

When `--format markdown` is used, output should look like:

```markdown
# Yield Forecast Report — Week 26, 2026

## National Estimates

| Crop | Yield (bu/ac) | vs Trend | vs 2025 | Confidence | Primary Driver |
|------|:---:|:---:|:---:|:---:|---|
| Corn | 178.5 | +1.2% | -0.8% | 0.72 | Normal conditions, adequate moisture |
| Soybeans | 51.8 | +0.5% | +1.3% | 0.68 | Favorable July rainfall |
| Winter Wheat | 52.1 | -1.1% | -2.5% | 0.81 | Dry conditions in KS/OK |

## Week-over-Week Changes

- **Corn** forecast UP 0.8 bu/acre (rain last week across IA/IL)
- **Soybeans** forecast unchanged
- **Winter Wheat** forecast DOWN 1.2 bu/acre (continued drought in HRW belt)

## Key Risk Factors

1. HRW wheat drought: KS precipitation 38% below normal, 12 consecutive dry days
2. Corn belt heat: 95°F+ forecast for July 20-24 across IA/IL/IN during pollination
3. Analog years suggest 2026 corn similar to 2017/2021 (favorable but not record)

## State Highlights

| State | Crop | Yield | vs Trend | Alert |
|---|---|:---:|:---:|---|
| Kansas | Winter Wheat | 38.2 | -15.3% | Severe drought stress |
| Iowa | Corn | 195.0 | +2.1% | On track |
| Illinois | Soybeans | 56.5 | +1.8% | Favorable conditions |
```

---

## File 6: `src/models/yield_model_validator.py`

Backtesting, monitoring, and validation framework.

### Location
`src/models/yield_model_validator.py`

### Class: `YieldModelValidator`

```python
class YieldModelValidator:
    """
    Validates yield forecast model performance through backtesting,
    bias analysis, and comparison to benchmarks.
    """

    def __init__(self):
        pass

    def run_backtest(self, crop: str, test_years: list,
                     forecast_weeks: list = [18, 22, 26, 30, 34, 38]) -> dict:
        """
        Leave-one-year-out backtesting.

        For each test year:
        1. Train model on all other years
        2. Generate predictions at each forecast_week
        3. Compare to actual final yield

        Returns metrics by week:
        {
            'week_18': {'rmse': 12.5, 'mae': 9.8, 'r2': 0.45, 'dir_accuracy': 0.65},
            'week_26': {'rmse': 7.2, 'mae': 5.5, 'r2': 0.72, 'dir_accuracy': 0.80},
            'week_34': {'rmse': 4.1, 'mae': 3.2, 'r2': 0.88, 'dir_accuracy': 0.90},
        }
        """

    def compute_skill_score(self, crop: str, test_years: list) -> dict:
        """
        Compute skill score vs naive benchmarks.

        Benchmarks:
        1. Naive trend: Just use trend_yield
        2. Last year: Use last year's actual yield
        3. 5-year average: Use 5-year rolling mean

        Skill = 1 - (MSE_model / MSE_benchmark)
        Positive = model is better. Negative = model is worse.
        """

    def analyze_bias(self, crop: str, test_years: list) -> dict:
        """
        Analyze systematic bias in forecasts.

        Check for:
        1. Overall bias (mean error): consistently over/under-predicting?
        2. State-level bias: which states are hardest to predict?
        3. Season-phase bias: are early-season forecasts biased differently?
        4. Extreme year bias: are drought/excellent years handled correctly?

        Returns bias_report dict.
        """

    def track_revisions(self, crop: str, year: int) -> list:
        """
        Query gold.yield_forecast for week-over-week forecast changes.

        Returns list of revision events with:
        - week, previous forecast, new forecast, change
        - probable cause (which features changed most)
        """

    def generate_validation_report(self, crop: str, test_years: list) -> str:
        """
        Generate comprehensive validation report.

        Sections:
        1. Overall accuracy metrics by forecast week
        2. Skill scores vs benchmarks
        3. Bias analysis
        4. Worst-case analysis (years with largest errors)
        5. Feature importance stability
        6. Recommendations for model improvement
        """
```

### CLI

```
python -m src.models.yield_model_validator backtest --crop corn --years 2020-2024
python -m src.models.yield_model_validator skill --crop corn
python -m src.models.yield_model_validator bias --crop corn
python -m src.models.yield_model_validator report --crop corn --output validation_report.md
```

### Accuracy Targets

- **Corn RMSE by week:** Week 18: < 15 bu/acre, Week 26: < 10, Week 30: < 7, Week 38: < 4
- **Directional accuracy** (above/below trend): > 70% by week 26, > 85% by week 34
- **Skill score vs trend:** > 0.2 by week 26 (model 20% better than naive trend)

---

## Pre-flight Checklist

Before running any scripts:

1. **Install dependencies:**
   ```
   pip install scikit-learn numpy scipy
   ```
   (psycopg2, python-dotenv, rasterio already installed)

2. **Ensure schemas 014 and 015 are migrated** (NDVI and weather forecast tables). Check:
   ```sql
   SELECT table_name FROM information_schema.tables
   WHERE table_schema = 'reference' AND table_name = 'crop_region';
   ```
   If not present, run `database/schemas/014_ndvi_schema.sql` and `database/schemas/015_weather_forecast_schema.sql`.

3. **Run migration 026** to create yield model tables.

4. **Run yield data migration** to populate `bronze.nass_state_yields`.

5. **Build historical features** for training years (this takes a while — processes weather + CPC + NASS data for 20+ years).

6. **Train models**, then **run backtesting** to validate before production use.

---

## Testing Commands

```bash
# 1. Run migration
python -c "
import psycopg2
from pathlib import Path
from dotenv import load_dotenv
import os
load_dotenv(Path('C:/Users/torem/RLC Dropbox/RLC Team Folder/RLC-Agent/.env'))
conn = psycopg2.connect(host='localhost', port=5432, database='rlc_commodities', user='postgres', password=os.environ.get('RLC_PG_PASSWORD') or os.environ.get('DATABASE_PASSWORD') or os.environ.get('DB_PASSWORD'))
with open('database/migrations/026_yield_model_tables.sql') as f:
    conn.cursor().execute(f.read())
conn.commit()
print('Migration 026 complete')
conn.close()
"

# 2. Migrate yield data
python scripts/migrate_yields_to_postgres.py --verify

# 3. Build features for test year
python -m src.models.yield_feature_engine --state IA --crop corn --year 2024 -v

# 4. Train models
python -m src.models.yield_orchestrator train --crop corn --years 2005-2023 -v

# 5. Backtest
python -m src.models.yield_orchestrator backtest --crop corn --years 2020-2024 -v

# 6. Run current forecast
python -m src.models.yield_orchestrator run -v

# 7. Generate report
python -m src.models.yield_orchestrator report --format markdown

# 8. Validate
python -m src.models.yield_model_validator report --crop corn
```

---

## Data Flow Summary

```
bronze.nass_state_yields ──→ Training targets (actual yields by state/year)
                              │
silver.weather_observation ──→│
bronze.cpc_region_stats ─────→│
gold.cpc_condition_weekly ───→│── yield_feature_engine.py ──→ silver.yield_features
gold.cpc_condition_yoy ──────→│                                    │
bronze.nass_crop_progress ───→│                                    │
bronze.nass_crop_condition ──→│                                    │
bronze.weather_emails ───────→│                                    │
bronze.ndvi_observation ─────→│                                    │
                                                                   │
silver.yield_trend ──────────→ yield_prediction_model.py ←─────────┘
                              │
                              ├──→ gold.yield_forecast (state-level predictions)
                              ├──→ gold.yield_monitor (dashboard view)
                              └──→ silver.yield_model_run (execution log)
```

---

## Notes

- **CPC gridded data covers 2015–2025.** The model can train on 2000+ with weather-only features for earlier years and add CPC features for 2015+. Handle the partial feature availability gracefully (NULL CPC features for pre-2015 training rows).
- **Weather observations cover major ag states but not all 50.** For states without stations, consider using NASS condition data alone (no weather features), or use state-averaged CPC condition as the primary signal.
- **World Weather emails may not be available for historical backtesting.** The `ww_risk_score` and `ww_outlook_sentiment` features should be NULL for historical training data. The model should work with or without these features.
- **Cotton uses different yield units** (lbs/acre instead of bu/acre). Handle this in the model and report formatting.
- **Winter wheat has a different growing season** (planted in fall, harvested in June). Growth stage determination and GDD accumulation start dates are different from corn/soybeans. The `weather_yield_thresholds.json` file has `growth_stages_winter` for wheat.
- **The `reference.crop_region` table** (from schema 014) defines 14 regions with bounding boxes. US regions: US_CORN_BELT, US_SOY_BELT, US_WHEAT_WINTER, US_WHEAT_SPRING. Use `states_provinces` array to map regions to states.
