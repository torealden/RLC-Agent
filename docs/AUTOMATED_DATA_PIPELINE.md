# Automated Agricultural Data Pipeline

This document describes the automated data collection, transformation, and visualization pipeline for agricultural commodity data. The LLM can use these scripts to maintain up-to-date market data.

## Overview

The pipeline follows a Bronze → Silver → Gold architecture:

```
DATA SOURCES                BRONZE              SILVER                  GOLD
┌──────────────┐        ┌──────────────┐    ┌──────────────┐    ┌──────────────┐
│ ERS Feed     │───────▶│ Raw Excel    │───▶│ Normalized   │───▶│ Views &      │
│ Grains       │        │ Data         │    │ Prices       │    │ Charts       │
├──────────────┤        ├──────────────┤    ├──────────────┤    ├──────────────┤
│ Census Trade │───────▶│ Raw Trade    │───▶│ Balance      │───▶│ Reports      │
│ Data         │        │ Records      │    │ Sheets       │    │              │
├──────────────┤        ├──────────────┤    ├──────────────┤    ├──────────────┤
│ NASS Crop    │───────▶│ Raw Crop     │───▶│ Industrial   │───▶│ Market       │
│ Production   │        │ Data         │    │ Use          │    │ Analysis     │
└──────────────┘        └──────────────┘    └──────────────┘    └──────────────┘
```

## Data Sources & Release Schedule

| Source | Frequency | Typical Release Day | Data Type |
|--------|-----------|---------------------|-----------|
| ERS Feed Grains | Monthly | 13th-15th | Prices, Balance Sheets, Trade |
| WASDE | Monthly | ~12th | Supply/Demand Projections |
| NASS Crop Production | Monthly | ~10th | Acreage, Yield, Production |
| Census Trade | Monthly | ~5th (2-mo lag) | Import/Export by Country |
| Export Sales | Weekly | Thursday | Sales & Shipments |

## Commands for the LLM

### 1. Check for New Data

```bash
# Check what data releases are coming up
python scripts/data_scheduler.py --schedule

# Check status of all collectors
python scripts/data_scheduler.py --status

# Check all sources for updates (won't download if already current)
python scripts/data_scheduler.py --check-all
```

### 2. Download New Data

```bash
# Download ERS Feed Grains data (checks if new version available)
python scripts/data_scheduler.py --source ers_feed_grains

# Force download even if data seems current
python scripts/data_scheduler.py --source ers_feed_grains --force
```

### 3. Run Transformations

```bash
# Run silver layer transformations (clean & normalize)
python scripts/transformations/silver_transformations.py

# This creates:
# - silver_price: Standardized prices with dates and unit conversions
# - silver_balance_sheet: Balance sheets with calculated ratios
# - silver_trade_flow: Trade data with market shares
```

### 4. Generate Visualizations & Reports

```bash
# Generate all charts and reports
python scripts/visualizations/gold_visualizations.py --all

# Generate specific outputs
python scripts/visualizations/gold_visualizations.py --views    # SQL views only
python scripts/visualizations/gold_visualizations.py --charts   # PNG charts
python scripts/visualizations/gold_visualizations.py --report   # Text report
```

### 5. Full Pipeline (Download → Transform → Visualize)

```bash
# Run the complete pipeline
python scripts/data_scheduler.py --source ers_feed_grains && \
python scripts/transformations/silver_transformations.py && \
python scripts/visualizations/gold_visualizations.py --all
```

## Database Tables

### Bronze Layer (Raw Data)

| Table | Description |
|-------|-------------|
| `farm_price` | Raw farm prices from ERS |
| `cash_price` | Raw cash prices at markets |
| `balance_sheet_item` | Raw S&D components |
| `industrial_use` | Corn industrial usage |
| `census_trade_monthly` | Monthly trade by country |
| `nass_crop_production` | State-level crop data |

### Silver Layer (Normalized)

| Table | Description |
|-------|-------------|
| `silver_price` | Prices with dates, unit conversions, MoM/YoY changes |
| `silver_balance_sheet` | Balance sheets with ratios (stocks/use, export share) |
| `silver_trade_flow` | Trade with market shares |

### Gold Layer (Views)

| View | Description |
|------|-------------|
| `gold_corn_price_summary` | Annual price summaries |
| `gold_corn_balance_sheet` | Formatted balance sheets |
| `gold_corn_price_seasonality` | Monthly price patterns |
| `gold_corn_stocks_use_history` | Stocks-to-use vs price |
| `gold_corn_industrial_use` | Industrial use breakdown |

## Output Files

Generated files are saved to `output/visualizations/`:

- `corn_prices_YYYYMMDD.png` - Price history chart
- `corn_balance_sheet_YYYYMMDD.png` - Supply/demand chart
- `corn_stocks_price_YYYYMMDD.png` - Stocks-to-use vs price scatter
- `corn_report_YYYYMMDD.txt` - Text market report

## Configuration Files

- `config/scheduler_config.json` - Scheduler settings (check intervals, enabled sources)
- `config/scheduler_state.json` - Last run times and history
- `config/data_sources.json` - Data source tracking (last download, file hashes)

## Example Queries for the LLM

### Get Latest Corn Prices
```sql
SELECT marketing_year, price_month, price_usd_bu, mom_pct_change
FROM silver_price
WHERE commodity_code = 'CORN' AND price_type = 'FARM'
ORDER BY price_date DESC
LIMIT 12;
```

### Get Current Balance Sheet
```sql
SELECT * FROM gold_corn_balance_sheet
ORDER BY marketing_year DESC
LIMIT 1;
```

### Get Stocks-to-Use History
```sql
SELECT marketing_year, stocks_use_pct, avg_farm_price
FROM gold_corn_stocks_use_history
WHERE stocks_use_pct IS NOT NULL
ORDER BY marketing_year DESC;
```

### Get Industrial Use Breakdown
```sql
SELECT * FROM gold_corn_industrial_use
ORDER BY marketing_year DESC
LIMIT 5;
```

## Adding New Data Sources

To add a new data source:

1. Create a collector in `scripts/collectors/`:
   ```python
   class NewSourceCollector:
       def __init__(self, force=False): ...
       def collect(self) -> dict: ...
   ```

2. Register in `scripts/data_scheduler.py`:
   ```python
   self.collectors = {
       'ers_feed_grains': ERSFeedGrainsCollector,
       'new_source': NewSourceCollector,  # Add here
   }
   ```

3. Add configuration in `config/scheduler_config.json`:
   ```json
   "new_source": {
       "enabled": true,
       "check_days": [1, 15],
       "priority": 2
   }
   ```

4. Create ingestion logic in `scripts/ingest_feed_grains_data.py` or new file

5. Add silver transformations in `scripts/transformations/`

## Scheduler Daemon Mode

For continuous monitoring (run on server):

```bash
# Run scheduler as background daemon
nohup python scripts/data_scheduler.py --daemon > logs/scheduler.log 2>&1 &

# Or with systemd (create /etc/systemd/system/rlc-scheduler.service)
[Unit]
Description=RLC Agricultural Data Scheduler
After=network.target

[Service]
Type=simple
User=rlc
WorkingDirectory=/path/to/RLC-Agent
ExecStart=/usr/bin/python3 scripts/data_scheduler.py --daemon
Restart=always

[Install]
WantedBy=multi-user.target
```

## Troubleshooting

### Data Not Downloading
- Check if `config/data_sources.json` shows recent download
- Use `--force` flag to override cache
- Check network connectivity

### Transformations Failing
- Ensure bronze tables have data: `SELECT COUNT(*) FROM farm_price`
- Check for NULL values in required fields
- Review `quality_flag` column for data issues

### Charts Not Generating
- Ensure matplotlib is installed: `pip install matplotlib`
- Check `output/visualizations/` directory exists
- Review log output for specific errors

## Version History

- v1.0.0 (2026-01): Initial release with ERS Feed Grains support
  - Bronze: farm_price, cash_price, balance_sheet_item, industrial_use
  - Silver: silver_price, silver_balance_sheet
  - Gold: 5 analytical views, 3 chart types, text report
