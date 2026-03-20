# Part 2: Understanding the Data

[← Back to Table of Contents](00_COVER_AND_TOC.md) | [← Previous: Getting Started](01_GETTING_STARTED.md)

---

## 2.1 Data Sources Overview

The platform collects data from over 25 sources across government agencies, exchanges, and international organizations. Understanding these sources helps you interpret the data correctly.

### United States Government Sources

#### USDA (United States Department of Agriculture)

| Source | Agency | Data Type | Frequency | Key Tables |
|--------|--------|-----------|-----------|------------|
| **WASDE** | OCE | Supply & Demand estimates | Monthly (around 12th) | `bronze.wasde_cell`, `gold.us_corn_balance_sheet` |
| **NASS QuickStats** | NASS | Crop progress, acreage, production | Weekly/Annual | `bronze.nass_*`, `silver.crop_progress` |
| **Export Sales** | FAS | Weekly export commitments | Weekly (Thursday) | `bronze.fas_export_sales` |
| **Export Inspections** | FGIS | Actual export shipments | Weekly | `bronze.fgis_inspection_raw` |
| **AMS Market News** | AMS | Cash prices, basis | Daily | `bronze.ams_price` |

💡 **Key Insight:** WASDE is the "gold standard" for supply/demand. Other USDA sources provide leading indicators.

#### Other US Agencies

| Source | Agency | Data Type | Frequency |
|--------|--------|-----------|-----------|
| **Trade Data** | Census Bureau | Import/export by HS code | Monthly |
| **COT Reports** | CFTC | Futures positioning | Weekly (Friday) |
| **Ethanol Production** | EIA | Production, stocks, prices | Weekly (Wednesday) |
| **Petroleum Data** | EIA | Crude, gasoline, diesel | Weekly |
| **Drought Monitor** | NDMC | Drought conditions | Weekly (Thursday) |

### International Sources

#### South America

| Source | Country | Data Type | Frequency |
|--------|---------|-----------|-----------|
| **CONAB** | Brazil | Production, S&D estimates | Monthly |
| **IMEA** | Brazil (Mato Grosso) | State-level crop data | Weekly/Monthly |
| **ABIOVE** | Brazil | Crush, exports | Monthly |
| **MagyP** | Argentina | Production, exports | Monthly |

#### Other Regions

| Source | Region | Data Type |
|--------|--------|-----------|
| **MPOB** | Malaysia | Palm oil production, stocks |
| **FAOSTAT** | Global | Production, trade, food balance |
| **Statistics Canada** | Canada | Grain stocks, trade |
| **CGC** | Canada | Grain deliveries, exports |

**[GRAPHIC: Data Sources World Map]**
*See [Appendix E](APPENDIX_E_GRAPHICS.md#data-sources-map) for graphic specifications*

### Data Release Calendar

Major releases follow predictable schedules:

| Day | Morning (ET) | Afternoon (ET) |
|-----|--------------|----------------|
| **Monday** | — | NASS Crop Progress (4:00 PM) |
| **Tuesday** | — | — |
| **Wednesday** | EIA Petroleum (10:30 AM) | — |
| **Thursday** | FAS Export Sales (8:30 AM) | Drought Monitor (8:30 AM) |
| **Friday** | — | CFTC COT (3:30 PM) |
| **Monthly** | Census Trade (~6th, 8:30 AM) | WASDE (~12th, 12:00 PM) |

---

## 2.2 The Medallion Architecture

The platform organizes data into three layers, each serving a specific purpose.

### Bronze Layer: Raw Data

**Purpose:** Preserve data exactly as received from the source.

**Characteristics:**
- No transformations applied
- Original field names retained
- All records kept (including duplicates)
- Full audit trail with timestamps

**When to use:** Only for debugging or reprocessing. Analysts typically never query Bronze directly.

**Example table: `bronze.wasde_cell`**
```sql
SELECT table_id, row_id, column_id, value_text, collected_at
FROM bronze.wasde_cell
WHERE release_id = 665  -- January 2025 WASDE
LIMIT 5;
```

| table_id | row_id | column_id | value_text | collected_at |
|----------|--------|-----------|------------|--------------|
| 04 | production | 2024/25 | 14,900 | 2025-01-12 12:05:00 |
| 04 | beginning_stocks | 2024/25 | 1,738 | 2025-01-12 12:05:00 |

### Silver Layer: Standardized Data

**Purpose:** Provide a clean, consistent format for analysis.

**Characteristics:**
- Unified schema: `(series_id, observation_time, value)`
- Consistent units (e.g., million bushels, metric tons)
- Data types enforced (numeric values as numbers)
- Quality flags applied
- Revision tracking

**When to use:** For any analysis that needs to combine data from multiple sources.

**Example table: `silver.observation`**
```sql
SELECT
    s.name AS series_name,
    o.observation_time,
    o.value,
    u.code AS unit
FROM silver.observation o
JOIN core.series s ON o.series_id = s.id
JOIN core.unit u ON s.unit_id = u.id
WHERE s.data_source_code = 'wasde'
  AND s.commodity_code = 'corn'
  AND s.name LIKE '%Production%'
ORDER BY o.observation_time DESC
LIMIT 5;
```

| series_name | observation_time | value | unit |
|-------------|------------------|-------|------|
| US Corn Production | 2025-01-01 | 14900 | mil_bu |
| US Corn Production | 2024-12-01 | 14867 | mil_bu |

### Gold Layer: Analysis-Ready Views

**Purpose:** Pre-built views optimized for specific analyses and reports.

**Characteristics:**
- Pivoted, aggregated, or calculated data
- Excel-compatible column names
- Business logic applied (e.g., stocks-to-use ratios)
- Optimized for Power BI

**When to use:** For dashboards, reports, and standard analyses.

**Example view: `gold.us_corn_balance_sheet`**
```sql
SELECT * FROM gold.us_corn_balance_sheet
WHERE marketing_year >= '2023/24'
ORDER BY marketing_year DESC;
```

| marketing_year | area_planted | area_harvested | yield | production | beginning_stocks | imports | total_supply | feed_residual | fsi_use | ethanol | exports | total_use | ending_stocks | stocks_to_use |
|----------------|--------------|----------------|-------|------------|------------------|---------|--------------|---------------|---------|---------|---------|-----------|---------------|---------------|
| 2024/25 | 90.7 | 82.7 | 180.1 | 14,900 | 1,738 | 25 | 16,663 | 5,775 | 1,490 | 5,500 | 2,350 | 15,115 | 1,548 | 10.2% |

**[GRAPHIC: Medallion Architecture Flow Diagram]**
*See [Appendix E](APPENDIX_E_GRAPHICS.md#medallion-flow) for graphic specifications*

---

## 2.3 Database Schema Reference

The database is organized into five schemas:

```
┌─────────────────────────────────────────────────────────────────┐
│                        DATABASE SCHEMAS                          │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  ┌─────────┐    Reference data shared across all layers          │
│  │  core   │    • data_source, series, commodity                 │
│  │         │    • location, unit                                 │
│  └─────────┘                                                     │
│                                                                  │
│  ┌─────────┐    Job tracking and validation                      │
│  │  audit  │    • ingest_run, validation_status                  │
│  │         │    • transformation_session                         │
│  └─────────┘                                                     │
│                                                                  │
│  ┌─────────┐    Raw source data (DO NOT QUERY DIRECTLY)         │
│  │ bronze  │    • wasde_cell, census_trade_raw                   │
│  │         │    • cftc_raw, eia_ethanol_raw                      │
│  └─────────┘                                                     │
│                                                                  │
│  ┌─────────┐    Standardized observations                        │
│  │ silver  │    • observation, trade_flow                        │
│  │         │    • price, crop_progress                           │
│  └─────────┘                                                     │
│                                                                  │
│  ┌─────────┐    Analysis-ready views                             │
│  │  gold   │    • us_corn_balance_sheet                          │
│  │         │    • wasde_changes, trade_summary                   │
│  └─────────┘                                                     │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

### Core Schema: Dimension Tables

These tables provide reference data used throughout the system.

#### `core.data_source`
Registry of all data feeds.

| Column | Description |
|--------|-------------|
| `code` | Unique identifier (e.g., 'wasde', 'nass') |
| `name` | Human-readable name |
| `url` | Source website |
| `frequency` | Expected update frequency |

#### `core.series`
Metadata for each time series.

| Column | Description |
|--------|-------------|
| `id` | Primary key (used in silver.observation) |
| `data_source_code` | Which source this series comes from |
| `series_key` | Unique key within source |
| `name` | Human-readable name |
| `commodity_code` | Which commodity |
| `location_code` | Geographic scope |
| `unit_id` | Measurement unit |

#### `core.commodity`
Commodity reference table.

| Column | Description |
|--------|-------------|
| `code` | Unique identifier (e.g., 'corn', 'soybeans') |
| `name` | Display name |
| `category` | Grouping (grains, oilseeds, energy) |

#### `core.unit`
Measurement units with conversions.

| Column | Description |
|--------|-------------|
| `code` | Unique identifier (e.g., 'mil_bu', 'mt') |
| `name` | Display name |
| `conversion_factor` | Factor to convert to base unit |

---

## 2.4 Key Tables and Views

### Most Useful Gold Views

These views are designed for direct use in Power BI and analysis:

#### Balance Sheets

| View | Description |
|------|-------------|
| `gold.us_corn_balance_sheet` | US corn supply & demand by marketing year |
| `gold.us_soybeans_balance_sheet` | US soybean supply & demand |
| `gold.us_wheat_balance_sheet` | US wheat (all classes) supply & demand |
| `gold.world_corn_balance_sheet` | World corn supply & demand |
| `gold.world_soybeans_balance_sheet` | World soybean supply & demand |

#### Trade Data

| View | Description |
|------|-------------|
| `gold.us_soybean_exports_by_destination` | Monthly exports by country |
| `gold.us_corn_exports_by_destination` | Monthly exports by country |
| `gold.marketing_year_trade_summary` | MY cumulative totals |
| `gold.trade_pace_vs_usda` | Compare actual vs. USDA forecast |

#### Positioning

| View | Description |
|------|-------------|
| `gold.cftc_managed_money_net` | Net managed money positions |
| `gold.cftc_commercial_net` | Net commercial positions |
| `gold.positioning_extremes` | Historical percentiles |

#### WASDE Analysis

| View | Description |
|------|-------------|
| `gold.wasde_changes` | Month-over-month changes |
| `gold.wasde_revision_history` | How estimates changed over time |
| `gold.stocks_to_use_history` | S/U ratio time series |

### Sample Queries

**Get latest corn balance sheet:**
```sql
SELECT *
FROM gold.us_corn_balance_sheet
WHERE marketing_year = '2024/25';
```

**Compare soybean exports year-over-year:**
```sql
SELECT
    destination_country,
    SUM(CASE WHEN marketing_year = '2024/25' THEN quantity_mt END) AS my_2024,
    SUM(CASE WHEN marketing_year = '2023/24' THEN quantity_mt END) AS my_2023,
    ROUND(100.0 * (
        SUM(CASE WHEN marketing_year = '2024/25' THEN quantity_mt END) /
        NULLIF(SUM(CASE WHEN marketing_year = '2023/24' THEN quantity_mt END), 0)
    ) - 100, 1) AS pct_change
FROM gold.us_soybean_exports_by_destination
WHERE destination_country IN ('China', 'Mexico', 'EU')
GROUP BY destination_country;
```

**View WASDE changes from last report:**
```sql
SELECT
    commodity,
    line_item,
    previous_value,
    current_value,
    change,
    pct_change
FROM gold.wasde_changes
WHERE release_date = (SELECT MAX(release_date) FROM gold.wasde_changes)
  AND ABS(pct_change) > 1  -- Only material changes
ORDER BY ABS(pct_change) DESC;
```

---

## Quick Reference: Finding Data

| I want to find... | Query this... |
|-------------------|---------------|
| Current US corn S&D | `gold.us_corn_balance_sheet WHERE marketing_year = '2024/25'` |
| Historical soybean exports | `gold.us_soybean_exports_by_destination` |
| Latest WASDE revisions | `gold.wasde_changes ORDER BY release_date DESC LIMIT 1` |
| CFTC positioning | `gold.cftc_managed_money_net WHERE commodity = 'corn'` |
| Crop progress | `silver.crop_progress WHERE commodity = 'corn' AND metric = 'planted'` |
| Cash prices | `silver.price WHERE price_type = 'cash'` |
| Ethanol production | `silver.observation WHERE series_id IN (SELECT id FROM core.series WHERE data_source_code = 'eia')` |

---

[← Previous: Getting Started](01_GETTING_STARTED.md) | [Next: Daily Operations →](03_DAILY_OPERATIONS.md)
