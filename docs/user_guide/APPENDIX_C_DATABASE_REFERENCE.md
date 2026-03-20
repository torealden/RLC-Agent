# Appendix C: Database Quick Reference

[← Back to Table of Contents](00_COVER_AND_TOC.md)

---

## Connection Information

| Parameter | Value |
|-----------|-------|
| Host | (see your `.env` file) |
| Port | 5432 |
| Database | rlc_commodities |
| SSL | Required |

---

## Schema Overview

```
rlc_commodities
├── core       (Dimension tables - reference data)
├── audit      (Job tracking and validation)
├── bronze     (Raw source data - don't query directly)
├── silver     (Standardized observations)
└── gold       (Analysis-ready views - use these!)
```

---

## Most Useful Tables & Views

### Gold Layer (Start Here)

| View | Description | Key Columns |
|------|-------------|-------------|
| `gold.us_corn_balance_sheet` | US corn S&D | marketing_year, production, ending_stocks, stocks_to_use |
| `gold.us_soybeans_balance_sheet` | US soybean S&D | marketing_year, crush, exports, ending_stocks |
| `gold.us_wheat_balance_sheet` | US wheat S&D | marketing_year, production, exports |
| `gold.wasde_changes` | WASDE revisions | release_date, commodity, line_item, change, pct_change |
| `gold.cftc_managed_money_net` | Spec positioning | report_date, commodity, net_position |
| `gold.us_soybean_exports_by_destination` | Export details | month, destination_country, quantity_mt |
| `gold.marketing_year_trade_summary` | MY trade totals | commodity, marketing_year, total_exports_mt |

### Silver Layer (For Custom Analysis)

| Table | Description | Key Columns |
|-------|-------------|-------------|
| `silver.observation` | Universal time series | series_id, observation_time, value |
| `silver.trade_flow` | Trade transactions | series_id, observation_time, destination, value |
| `silver.price` | Price data | series_id, observation_time, price, unit |

### Core Layer (Reference Data)

| Table | Description | Key Columns |
|-------|-------------|-------------|
| `core.series` | Time series metadata | id, data_source_code, name, commodity_code |
| `core.commodity` | Commodity list | code, name, category |
| `core.data_source` | Data source registry | code, name, frequency |
| `core.unit` | Unit definitions | code, name, conversion_factor |

---

## Common Queries

### Get Latest Balance Sheet

```sql
-- Corn
SELECT * FROM gold.us_corn_balance_sheet
WHERE marketing_year = '2024/25';

-- Soybeans
SELECT * FROM gold.us_soybeans_balance_sheet
WHERE marketing_year = '2024/25';
```

### View WASDE Changes

```sql
-- Latest WASDE changes
SELECT commodity, line_item, previous_value, current_value, change, pct_change
FROM gold.wasde_changes
WHERE release_date = (SELECT MAX(release_date) FROM gold.wasde_changes)
  AND ABS(pct_change) > 1
ORDER BY ABS(pct_change) DESC;
```

### Export Pace

```sql
-- Soybean exports cumulative
SELECT
    marketing_year,
    destination_country,
    SUM(quantity_mt) AS total_mt
FROM gold.us_soybean_exports_by_destination
WHERE marketing_year IN ('2024/25', '2023/24')
GROUP BY marketing_year, destination_country
ORDER BY marketing_year, total_mt DESC;
```

### CFTC Positioning

```sql
-- Current managed money positions
SELECT commodity, report_date, net_position, change_week
FROM gold.cftc_managed_money_net
WHERE report_date = (SELECT MAX(report_date) FROM gold.cftc_managed_money_net)
ORDER BY ABS(net_position) DESC;
```

### Historical Stocks-to-Use

```sql
-- Corn S/U history
SELECT marketing_year, ending_stocks, total_use, stocks_to_use
FROM gold.us_corn_balance_sheet
WHERE marketing_year >= '2015/16'
ORDER BY marketing_year;
```

### Find Available Series

```sql
-- List all series for a commodity
SELECT s.id, s.name, ds.name AS source, s.frequency
FROM core.series s
JOIN core.data_source ds ON s.data_source_code = ds.code
WHERE s.commodity_code = 'soybeans'
ORDER BY ds.name, s.name;
```

### Check Data Freshness

```sql
-- Latest data by source
SELECT
    ds.name AS source,
    MAX(o.observation_time) AS latest_data,
    MAX(o.created_at) AS last_updated
FROM silver.observation o
JOIN core.series s ON o.series_id = s.id
JOIN core.data_source ds ON s.data_source_code = ds.code
GROUP BY ds.name
ORDER BY last_updated DESC;
```

---

## Marketing Year Reference

| Commodity | Marketing Year Start | Example |
|-----------|---------------------|---------|
| Corn | September 1 | 2024/25 = Sep 2024 - Aug 2025 |
| Soybeans | September 1 | 2024/25 = Sep 2024 - Aug 2025 |
| Wheat | June 1 | 2024/25 = Jun 2024 - May 2025 |
| Cotton | August 1 | 2024/25 = Aug 2024 - Jul 2025 |

---

## Unit Conversions

| From | To | Factor |
|------|-----|--------|
| Bushels (corn) | Metric tons | × 0.0254 |
| Bushels (soybeans) | Metric tons | × 0.0272 |
| Bushels (wheat) | Metric tons | × 0.0272 |
| Million bushels | Thousand MT | × 25.4 (corn), × 27.2 (beans) |
| Short tons | Metric tons | × 0.907 |

---

## Query Tools

### Command Line (psql)

```bash
psql "postgresql://user:pass@host:5432/rlc_commodities"
```

### Python

```python
from dashboards.ops.db import query_df

df = query_df("SELECT * FROM gold.us_corn_balance_sheet")
print(df)
```

### Power BI

1. Get Data > PostgreSQL
2. Enter host:port
3. Select database
4. Browse to gold schema

---

## Performance Tips

| Do | Don't |
|----|-------|
| ✅ Query gold views | ❌ Query bronze tables |
| ✅ Add WHERE clauses | ❌ SELECT * on large tables |
| ✅ Use specific columns | ❌ JOIN many tables unnecessarily |
| ✅ Use LIMIT for exploration | ❌ Fetch entire history when not needed |

---

[← Back to Table of Contents](00_COVER_AND_TOC.md) | [Next: Troubleshooting Guide →](APPENDIX_D_TROUBLESHOOTING.md)
