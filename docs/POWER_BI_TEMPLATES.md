# RLC Commodities - Power BI Templates

## Connection Settings

```
Server: 127.0.0.1
Database: rlc_commodities
Authentication: Database (postgres / your_password)
```

---

## Template 1: Commodity Balance Sheet Dashboard

### Purpose
Interactive balance sheet analysis across commodities, countries, and marketing years.

### Data Source Query
```sql
SELECT
    commodity,
    country,
    section,
    metric,
    marketing_year,
    value,
    unit,
    source_file
FROM bronze.sqlite_commodity_balance_sheets
WHERE value IS NOT NULL
ORDER BY commodity, country, marketing_year DESC
```

### Recommended Visuals

1. **Slicer Panel** (Left side)
   - Commodity (dropdown)
   - Country (dropdown)
   - Marketing Year (slider range)
   - Section (multi-select)

2. **Card KPIs** (Top row)
   - Total Production
   - Total Exports
   - Ending Stocks
   - Stocks-to-Use Ratio

3. **Stacked Bar Chart**: Production by Country
   - X-axis: Country
   - Y-axis: Value (filtered to Production metric)
   - Legend: Marketing Year

4. **Line Chart**: Time Series Trend
   - X-axis: Marketing Year
   - Y-axis: Value
   - Legend: Metric (Production, Exports, Ending Stocks)

5. **Matrix Table**: Full Balance Sheet
   - Rows: Metric
   - Columns: Marketing Year
   - Values: Value

### DAX Measures
```dax
// Total Production
Total Production =
CALCULATE(
    SUM('BalanceSheet'[value]),
    CONTAINSSTRING('BalanceSheet'[metric], "Production")
)

// Ending Stocks
Ending Stocks =
CALCULATE(
    SUM('BalanceSheet'[value]),
    CONTAINSSTRING('BalanceSheet'[metric], "Ending Stocks")
)

// Stocks to Use Ratio
Stocks to Use =
DIVIDE([Ending Stocks], [Total Use], 0) * 100

// Year over Year Change
YoY Change =
VAR CurrentYear = SELECTEDVALUE('BalanceSheet'[marketing_year])
VAR PriorYearValue =
    CALCULATE(
        SUM('BalanceSheet'[value]),
        'BalanceSheet'[marketing_year] = CurrentYear - 1
    )
RETURN
    DIVIDE(SUM('BalanceSheet'[value]) - PriorYearValue, PriorYearValue, 0)
```

---

## Template 2: Supply & Demand Overview

### Purpose
High-level S&D snapshot for quick executive briefing.

### Data Source Query
```sql
SELECT
    commodity,
    country,
    marketing_year,
    MAX(CASE WHEN LOWER(metric) LIKE '%beginning%stocks%' THEN value END) as beginning_stocks,
    MAX(CASE WHEN LOWER(metric) LIKE '%production%' AND LOWER(metric) NOT LIKE '%ethanol%' THEN value END) as production,
    MAX(CASE WHEN LOWER(metric) LIKE '%import%' THEN value END) as imports,
    MAX(CASE WHEN LOWER(metric) LIKE '%total supply%' OR LOWER(metric) = 'supply, total' THEN value END) as total_supply,
    MAX(CASE WHEN LOWER(metric) LIKE '%crush%' THEN value END) as crush,
    MAX(CASE WHEN LOWER(metric) LIKE '%export%' THEN value END) as exports,
    MAX(CASE WHEN LOWER(metric) LIKE '%feed%' THEN value END) as feed_residual,
    MAX(CASE WHEN LOWER(metric) LIKE '%total%use%' OR LOWER(metric) LIKE '%domestic%total%' THEN value END) as total_use,
    MAX(CASE WHEN LOWER(metric) LIKE '%ending%stocks%' THEN value END) as ending_stocks
FROM bronze.sqlite_commodity_balance_sheets
WHERE value IS NOT NULL
GROUP BY commodity, country, marketing_year
ORDER BY commodity, country, marketing_year DESC
```

### Recommended Visuals

1. **Waterfall Chart**: Supply/Demand Flow
   - Shows: Beginning Stocks → +Production → +Imports → -Exports → -Crush → -Feed → Ending Stocks

2. **Clustered Column Chart**: Supply vs Demand Comparison
   - X-axis: Marketing Year
   - Columns: Total Supply, Total Use

3. **Gauge Charts**: Key Ratios
   - Stocks-to-Use %
   - Export % of Production
   - Crush % of Production

4. **Map Visual**: Production by Country
   - Size: Production volume
   - Color: Year-over-year change

---

## Template 3: Multi-Commodity Comparison

### Purpose
Compare metrics across different commodities side by side.

### Data Source Query
```sql
SELECT
    commodity,
    marketing_year,
    MAX(CASE WHEN LOWER(metric) LIKE '%production%' AND LOWER(metric) NOT LIKE '%ethanol%' THEN value END) as production,
    MAX(CASE WHEN LOWER(metric) LIKE '%export%' THEN value END) as exports,
    MAX(CASE WHEN LOWER(metric) LIKE '%ending%stocks%' THEN value END) as ending_stocks,
    MAX(CASE WHEN LOWER(metric) LIKE '%crush%' OR LOWER(metric) LIKE '%domestic crush%' THEN value END) as crush
FROM bronze.sqlite_commodity_balance_sheets
WHERE country = 'United States'  -- or remove for global
  AND value IS NOT NULL
GROUP BY commodity, marketing_year
ORDER BY marketing_year DESC, commodity
```

### Recommended Visuals

1. **Small Multiples**: Production Trends by Commodity
   - Grid of line charts, one per commodity

2. **100% Stacked Bar**: Market Share
   - X-axis: Marketing Year
   - Y-axis: % of total
   - Colors: Each commodity

3. **Scatter Plot**: Production vs Exports
   - X-axis: Production
   - Y-axis: Exports
   - Size: Ending Stocks
   - Color: Commodity

---

## Template 4: Historical Trend Analysis

### Purpose
Deep dive into historical patterns and seasonality.

### Data Source Query
```sql
WITH yearly_data AS (
    SELECT
        commodity,
        country,
        marketing_year,
        metric,
        value,
        LAG(value) OVER (PARTITION BY commodity, country, metric ORDER BY marketing_year) as prev_year_value
    FROM bronze.sqlite_commodity_balance_sheets
    WHERE value IS NOT NULL
)
SELECT
    *,
    value - prev_year_value as absolute_change,
    CASE WHEN prev_year_value > 0 THEN
        ROUND(((value - prev_year_value) / prev_year_value) * 100, 2)
    END as pct_change
FROM yearly_data
ORDER BY commodity, country, metric, marketing_year DESC
```

### Recommended Visuals

1. **Area Chart**: Cumulative Production Over Time
   - Stacked by country

2. **Bullet Chart**: Current vs Historical Average
   - Target: 5-year average
   - Actual: Current year

3. **Heat Map**: Year-over-Year Changes
   - Rows: Metric
   - Columns: Marketing Year
   - Color: % change (red to green)

4. **Box Plot**: Value Distribution by Metric
   - Shows outliers and trends

---

## Template 5: Data Quality Dashboard

### Purpose
Monitor data completeness and freshness.

### Data Source Query
```sql
SELECT
    commodity,
    country,
    source_file,
    COUNT(*) as record_count,
    COUNT(DISTINCT metric) as unique_metrics,
    COUNT(DISTINCT marketing_year) as year_coverage,
    MIN(marketing_year) as earliest_year,
    MAX(marketing_year) as latest_year,
    COUNT(CASE WHEN value IS NULL THEN 1 END) as null_values,
    COUNT(CASE WHEN value IS NOT NULL THEN 1 END) as valid_values
FROM bronze.sqlite_commodity_balance_sheets
GROUP BY commodity, country, source_file
ORDER BY record_count DESC
```

### Recommended Visuals

1. **Table**: Data Coverage Summary
   - Conditional formatting on completeness

2. **Donut Chart**: Valid vs Null Values

3. **Timeline**: Data Freshness
   - When each source was last updated

---

## Template 6: Executive Summary (Single Page)

### Purpose
One-page overview for leadership.

### Data Source Query
```sql
-- Latest year summary for key commodities
SELECT
    commodity,
    country,
    marketing_year,
    MAX(CASE WHEN LOWER(metric) LIKE '%production%' AND LOWER(metric) NOT LIKE '%ethanol%' THEN value END) as production,
    MAX(CASE WHEN LOWER(metric) LIKE '%ending%stocks%' THEN value END) as ending_stocks,
    MAX(CASE WHEN LOWER(metric) LIKE '%export%' THEN value END) as exports
FROM bronze.sqlite_commodity_balance_sheets
WHERE marketing_year IN (
    SELECT DISTINCT marketing_year
    FROM bronze.sqlite_commodity_balance_sheets
    ORDER BY marketing_year DESC
    LIMIT 2
)
AND value IS NOT NULL
GROUP BY commodity, country, marketing_year
ORDER BY commodity, country, marketing_year DESC
```

### Layout
```
┌─────────────────────────────────────────────────────────────┐
│  RLC COMMODITIES EXECUTIVE DASHBOARD                        │
├─────────────┬─────────────┬─────────────┬──────────────────┤
│  SOYBEANS   │    CORN     │    WHEAT    │    COTTON        │
│  [KPI Card] │  [KPI Card] │  [KPI Card] │   [KPI Card]     │
├─────────────┴─────────────┴─────────────┴──────────────────┤
│  [Production Trend Line Chart - All Commodities]            │
├────────────────────────────┬───────────────────────────────┤
│  [Top 5 Exporters Table]   │  [Stocks-to-Use Gauge]        │
├────────────────────────────┴───────────────────────────────┤
│  [Year-over-Year Change Bar Chart]                          │
└─────────────────────────────────────────────────────────────┘
```

---

## Quick Start: Import Data

### Step 1: Connect to PostgreSQL
1. Get Data → PostgreSQL database
2. Server: `127.0.0.1`
3. Database: `rlc_commodities`
4. Click OK → Database auth → Enter credentials

### Step 2: Choose Your Query
1. In Navigator, click "Advanced options"
2. Paste one of the SQL queries above
3. Click OK

### Step 3: Transform Data
1. In Power Query Editor, verify column types
2. marketing_year → Text (for proper sorting)
3. value → Decimal Number
4. Close & Apply

### Step 4: Build Visuals
1. Add slicers first (commodity, country, year)
2. Build KPI cards for key metrics
3. Add trend charts
4. Add detail tables

---

## Available Tables in Database

| Schema | Table | Description | Rows |
|--------|-------|-------------|------|
| bronze | sqlite_commodity_balance_sheets | Main commodity data from SQLite migration | ~105K |
| bronze | census_trade_raw | Census trade data | varies |
| bronze | fgis_inspection_raw | Export inspections | varies |
| bronze | wasde_cell | WASDE report data | varies |
| silver | observation | Standardized time series | varies |
| silver | trade_flow | Aggregated trade flows | varies |
| gold | us_corn_balance_sheet | US Corn S&D view | - |
| gold | us_soybeans_balance_sheet | US Soybeans S&D view | - |
| gold | wasde_changes | WASDE month-over-month changes | - |

---

## Tips for Power BI Performance

1. **Use DirectQuery sparingly** - Import mode is faster for <1M rows
2. **Create relationships** between dimension tables
3. **Use calculated columns** for frequently filtered values
4. **Aggregate at query level** when possible
5. **Index your filters** - commodity, country, marketing_year

---

## Sample DAX Measures Library

```dax
// ========== CORE MEASURES ==========

// Production (handles various naming conventions)
Production =
CALCULATE(
    SUM('Data'[value]),
    OR(
        CONTAINSSTRING('Data'[metric], "Production"),
        'Data'[metric] = "Output"
    ),
    NOT(CONTAINSSTRING('Data'[metric], "Ethanol"))
)

// Exports
Exports =
CALCULATE(
    SUM('Data'[value]),
    CONTAINSSTRING('Data'[metric], "Export")
)

// Ending Stocks
Ending Stocks =
CALCULATE(
    SUM('Data'[value]),
    OR(
        CONTAINSSTRING('Data'[metric], "Ending Stocks"),
        CONTAINSSTRING('Data'[metric], "Closing Stocks")
    )
)

// Stocks-to-Use Ratio (%)
Stocks to Use % =
VAR EndingStocks = [Ending Stocks]
VAR TotalUse =
    CALCULATE(
        SUM('Data'[value]),
        OR(
            CONTAINSSTRING('Data'[metric], "Total Use"),
            CONTAINSSTRING('Data'[metric], "Domestic Total")
        )
    )
RETURN
    IF(TotalUse > 0, DIVIDE(EndingStocks, TotalUse) * 100, BLANK())

// ========== COMPARISON MEASURES ==========

// Prior Year Value
Prior Year =
VAR CurrentYear = MAX('Data'[marketing_year])
VAR PriorYear = CurrentYear - 1
RETURN
    CALCULATE(
        SUM('Data'[value]),
        'Data'[marketing_year] = PriorYear
    )

// Year-over-Year Change
YoY Change = [Current Value] - [Prior Year]

// YoY % Change
YoY % = DIVIDE([YoY Change], [Prior Year], 0) * 100

// 5-Year Average
5Y Average =
VAR MaxYear = MAX('Data'[marketing_year])
RETURN
    CALCULATE(
        AVERAGE('Data'[value]),
        'Data'[marketing_year] >= MaxYear - 4,
        'Data'[marketing_year] <= MaxYear
    )

// Deviation from Average
Deviation from Avg =
    SUM('Data'[value]) - [5Y Average]

// ========== RANKING MEASURES ==========

// Rank by Production
Production Rank =
RANKX(
    ALL('Data'[country]),
    [Production],
    ,
    DESC
)

// Top N Filter
Is Top 5 = IF([Production Rank] <= 5, 1, 0)
```

---

## Next Steps

1. Import the main balance sheet data using Template 1
2. Create slicers for commodity/country/year filtering
3. Build KPI cards for key metrics
4. Add trend visualizations
5. Publish to Power BI Service for sharing
