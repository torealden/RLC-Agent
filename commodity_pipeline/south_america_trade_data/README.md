# South America Trade Data Pipeline

A comprehensive data pipeline for collecting, harmonizing, and analyzing trade flow data from South American countries.

## Supported Countries

| Country | Source | API Type | Update Frequency |
|---------|--------|----------|------------------|
| Argentina | INDEC | CSV/XLS download | Mid-month (~15th) |
| Brazil | Comex Stat | JSON API | Early month (~5th-10th) |
| Colombia | DANE | Socrata API | Mid-month (~15th) |
| Uruguay | DNA | CKAN API | Mid-month (~15th) |
| Paraguay | DNA / WITS | Multi-source | ~20th (2 month lag) |

## Installation

```bash
pip install -r requirements.txt
```

## Configuration

### Environment Variables

```bash
# Database
export SA_TRADE_DB_TYPE=sqlite  # or mysql, postgresql
export SA_TRADE_DB_HOST=localhost
export SA_TRADE_DB_PORT=3306
export SA_TRADE_DB_NAME=south_america_trade
export SA_TRADE_DB_USER=your_user
export SA_TRADE_DB_PASSWORD=your_password

# API Tokens (optional but recommended)
export SOCRATA_APP_TOKEN=your_token      # Colombia - improves rate limits
export COMTRADE_API_KEY=your_api_key     # Paraguay fallback
```

### Credentials to Obtain

Replace `XXX` placeholders in `config/settings.py` with actual values:

1. **Colombia (DANE)**
   - `socrata_app_token`: Get from [datos.gov.co](https://www.datos.gov.co) (optional)
   - `export_dataset_id`: Find at DANE portal
   - `import_dataset_id`: Find at DANE portal

2. **Uruguay (DNA)**
   - `export_resource_id`: Find at [catalogodatos.gub.uy](https://catalogodatos.gub.uy)
   - `import_resource_id`: Find at catalogodatos.gub.uy

3. **Paraguay (Fallback)**
   - `comtrade_api_key`: Get from [UN Comtrade](https://comtradeapi.un.org) (free tier available)

## Usage

### Command Line

```bash
# Fetch data for a specific country
python -m south_america_trade_data.main fetch --country BRA --year 2024 --month 11

# Run monthly pipeline for all countries
python -m south_america_trade_data.main monthly --year 2024 --month 10

# Historical backfill
python -m south_america_trade_data.main backfill --start-year 2024 --start-month 1

# Check status
python -m south_america_trade_data.main status

# Validate configuration
python -m south_america_trade_data.main validate
```

### Python API

```python
from south_america_trade_data import TradeDataOrchestrator

# Initialize orchestrator
orchestrator = TradeDataOrchestrator()

# Run monthly pipeline
result = orchestrator.run_monthly_pipeline(
    year=2024,
    month=10,
    countries=['BRA', 'ARG'],
    flows=['export', 'import']
)

print(f"Records loaded: {result.total_records_loaded}")
```

### Individual Agents

```python
from south_america_trade_data.agents import BrazilComexStatAgent
from south_america_trade_data.config import BrazilConfig

agent = BrazilComexStatAgent(BrazilConfig())
result = agent.fetch_data(year=2024, month=10, flow='export')

if result.success:
    records = agent.transform_to_records(result.data, 'export')
    print(f"Fetched {len(records)} records")
```

## Data Schema

### Normalized Trade Record

| Field | Type | Description |
|-------|------|-------------|
| reporter_country | str | ISO3 country code of reporter |
| partner_country | str | Trading partner |
| flow | str | 'export' or 'import' |
| period | str | YYYY-MM format |
| hs_code_6 | str | HS code normalized to 6 digits |
| quantity_tons | float | Quantity in metric tons |
| value_usd | float | Value in USD |
| data_source | str | Source identifier |

## Quality Validation

The pipeline includes automatic quality checks:

- **Required field validation**
- **Value range checks**
- **Z-score outlier detection** (>3 standard deviations)
- **Deviation from trailing mean** (>20%)
- **Duplicate detection**
- **Period completeness checking**

## Trade Balance Matrix

For reconciliation, the pipeline builds a reporter-partner-HS matrix comparing:
- Exports reported by exporter
- Imports reported by importer (mirror flow)

Discrepancies are flagged and can be balanced using configurable methods.

## Scheduling

### Recommended Cron Schedules

```cron
# Brazil - 8th of each month
0 8 8 * * python -m south_america_trade_data.main monthly -c BRA

# Argentina - 15th of each month
0 8 15 * * python -m south_america_trade_data.main monthly -c ARG

# Colombia - 15th of each month
0 8 15 * * python -m south_america_trade_data.main monthly -c COL

# Uruguay - 15th of each month
0 8 15 * * python -m south_america_trade_data.main monthly -c URY

# Paraguay - 20th of each month
0 8 20 * * python -m south_america_trade_data.main monthly -c PRY
```

### Built-in Scheduler

```bash
python -m south_america_trade_data.main schedule
```

## API Reference

### Sample API Queries

**Brazil (Comex Stat)**
```
GET https://api-comex.stat.gov.br/comexstat?freq=M&type=export&year=2024&month=08&hs_level=8&partner=all&offset=0&limit=5000
```

**Paraguay (WITS Fallback)**
```
GET https://wits.worldbank.org/API/V1/commodity/export/600/all/TOTAL?year=2023&page=1&max=50000
```

**Colombia (Socrata)**
```
GET https://www.datos.gov.co/resource/{dataset_id}.json?$where=year=2024 AND month=8&$limit=50000
```

**Uruguay (CKAN)**
```
GET https://catalogodatos.gub.uy/api/3/action/datastore_search?resource_id={id}&filters={"anio":2024,"mes":8}
```

## License

Proprietary - Round Lakes Commodities
