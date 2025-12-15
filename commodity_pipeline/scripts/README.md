# Commodity Pipeline Scripts

Scripts for pulling and reporting on commodity market data.

## Prerequisites

```bash
pip install pandas openpyxl beautifulsoup4 lxml requests
```

## Environment Variables

Set these API keys for full functionality:

```bash
export EIA_API_KEY="your-key-here"        # EIA ethanol/petroleum
export NASS_API_KEY="your-key-here"       # USDA NASS
export CENSUS_API_KEY="your-key-here"     # US Census (optional)
```

## Scripts

### pull_commodity_data.py

Comprehensive data pull from all implemented collectors.

```bash
# Test connectivity only
python scripts/pull_commodity_data.py --test-only

# Pull all data (all regions)
python scripts/pull_commodity_data.py

# Pull specific region
python scripts/pull_commodity_data.py --region south_america

# Export as JSON instead of CSV
python scripts/pull_commodity_data.py --export-format json

# Custom output directory
python scripts/pull_commodity_data.py --output-dir ./my_exports
```

**Arguments:**
- `--test-only`: Only test connectivity, don't pull data
- `--region`: `north_america`, `south_america`, `asia_pacific`, `global`, or `all`
- `--export-format`: `csv`, `json`, or `parquet`
- `--output-dir`: Output directory for exports (default: `./data/exports`)
- `--lookback-days`: Days of historical data (default: 365)
- `--no-cache`: Disable caching, fetch fresh data
- `-v, --verbose`: Verbose output

### generate_tuesday_report.py

Generates a weekly report focused on key market data.

```bash
# Generate standard report
python scripts/generate_tuesday_report.py

# High priority sources only (faster)
python scripts/generate_tuesday_report.py --high-priority-only

# Custom output directory
python scripts/generate_tuesday_report.py --output-dir ./reports
```

**Output:**
- `tuesday_report_YYYY-MM-DD.md`: Markdown report
- `{source}_YYYYMMDD.csv`: Raw data files for Power BI

## Data Sources by Priority

### North America - High Priority
- CFTC COT (Positioning - Friday release)
- USDA FAS Export Sales (Thursday release)
- EIA Ethanol (Wednesday release)
- USDA Drought Monitor (Thursday release)
- USDA NASS Crop Progress (Monday release)
- CME Settlements (Daily)

### South America - High Priority
- CONAB (Brazil crop estimates)
- ABIOVE (Brazil soy crush)
- IMEA (Mato Grosso state data)
- MAGyP (Argentina production)

### Asia Pacific
- MPOB (Malaysian palm oil)

## Typical Tuesday Workflow

1. Run full data pull on Monday evening:
   ```bash
   python scripts/pull_commodity_data.py --no-cache
   ```

2. Generate Tuesday report:
   ```bash
   python scripts/generate_tuesday_report.py
   ```

3. Review generated files in `./data/reports/`

4. Import CSV files into Power BI as needed
