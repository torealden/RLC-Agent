# Historical Commodity Database Setup Guide

Complete step-by-step guide to building a historical commodity database from all North American data sources, connecting to visualization tools, and setting up ongoing data collection.

## Table of Contents

1. [Prerequisites](#1-prerequisites)
2. [Database Setup](#2-database-setup)
3. [API Keys Setup](#3-api-keys-setup)
4. [Historical Data Loading](#4-historical-data-loading)
5. [Testing Collectors](#5-testing-collectors)
6. [Power BI Connection](#6-power-bi-connection)
7. [Sample Queries and Views](#7-sample-queries-and-views)
8. [Ongoing Data Collection](#8-ongoing-data-collection)
9. [Troubleshooting](#9-troubleshooting)

---

## 1. Prerequisites

### System Requirements
- Python 3.9+
- PostgreSQL 13+ (recommended) or SQLite for testing
- 10GB+ disk space for historical data
- Power BI Desktop (Windows) or alternative visualization tool

### Install Dependencies

```bash
cd /home/user/RLC-Agent

# Create virtual environment
python -m venv venv
source venv/bin/activate  # Linux/Mac
# or: .\venv\Scripts\activate  # Windows

# Install required packages
pip install -r requirements.txt

# Additional packages for database and visualization
pip install psycopg2-binary sqlalchemy pandas openpyxl xlrd requests beautifulsoup4
```

### Verify Installation

```bash
python -c "from commodity_pipeline.data_collectors import get_available_collectors; print(f'Collectors available: {len(get_available_collectors())}')"
```

---

## 2. Database Setup

### Option A: PostgreSQL (Recommended for Production)

```bash
# Install PostgreSQL (Ubuntu/Debian)
sudo apt-get install postgresql postgresql-contrib

# Create database and user
sudo -u postgres psql
```

```sql
-- In PostgreSQL shell
CREATE USER commodity_user WITH PASSWORD 'your_secure_password';
CREATE DATABASE commodity_db OWNER commodity_user;
GRANT ALL PRIVILEGES ON DATABASE commodity_db TO commodity_user;
\q
```

### Option B: SQLite (For Testing)

```bash
# SQLite requires no setup, just specify file path
export DATABASE_URL="sqlite:///./data/commodity.db"
```

### Run Database Migrations

```bash
# Apply the schema migrations
psql -U commodity_user -d commodity_db -f docs/migrations/003_comprehensive_commodity_schema.sql

# Or for SQLite, use the Python loader (created below)
python scripts/init_database.py
```

---

## 3. API Keys Setup

### Required API Keys (Get These First)

| Service | URL | Time to Get | Notes |
|---------|-----|-------------|-------|
| EIA | https://www.eia.gov/opendata/register.php | Instant | Free, unlimited |
| USDA NASS | https://quickstats.nass.usda.gov/api | Instant | Free, unlimited |
| Census (optional) | https://api.census.gov/data/key_signup.html | Instant | Higher rate limits |

### Setup Environment

```bash
# Copy example and edit
cp .env.example .env

# Edit with your keys
nano .env  # or your preferred editor
```

Minimum required `.env` content:

```bash
# Database
DATABASE_URL=postgresql://commodity_user:your_password@localhost:5432/commodity_db

# Required API Keys
EIA_API_KEY=your_eia_key_here
NASS_API_KEY=your_nass_key_here

# Optional but recommended
CENSUS_API_KEY=your_census_key_here
```

### Verify API Keys

```bash
# Test EIA connection
python -c "
import os
from dotenv import load_dotenv
load_dotenv()
print(f'EIA Key: {os.getenv(\"EIA_API_KEY\", \"NOT SET\")[:10]}...')
print(f'NASS Key: {os.getenv(\"NASS_API_KEY\", \"NOT SET\")[:10]}...')
"
```

---

## 4. Historical Data Loading

### Data Availability by Source

| Collector | Historical Data Available | Recommended Start |
|-----------|---------------------------|-------------------|
| CFTC COT | 1986 - present | 2015 |
| USDA FAS Export Sales | 1990 - present | 2018 |
| USDA FAS PSD | 1960 - present | 2010 |
| USDA NASS | Varies by report | 2018 |
| EIA Ethanol | 2010 - present | 2015 |
| EIA Petroleum | 1980s - present | 2015 |
| Drought Monitor | 2000 - present | 2015 |
| CME Settlements | Current day only* | Current |
| Census Trade | 2013 - present | 2018 |
| Canada CGC | Current year | Current |
| MPOB | 2010 - present | 2015 |

*For historical CME futures data, use Quandl/Nasdaq Data Link or purchase from CME DataMine

### Run Historical Data Loader

```bash
# Load all available historical data (may take 1-2 hours)
python scripts/load_historical_data.py --all --start-year 2018

# Or load specific sources
python scripts/load_historical_data.py --collectors cftc_cot usda_fas eia_ethanol --start-year 2020

# Load with progress display
python scripts/load_historical_data.py --all --start-year 2018 --verbose
```

---

## 5. Testing Collectors

### Quick Test - No Database Required

```python
# test_collectors.py
from commodity_pipeline.data_collectors import (
    CFTCCOTCollector,
    USDATFASCollector,
    DroughtCollector,
)

# Test CFTC COT (no auth needed)
print("Testing CFTC COT...")
cot = CFTCCOTCollector()
result = cot.collect(commodities=['corn', 'soybeans', 'wheat'])
print(f"  Success: {result.success}, Records: {result.records_fetched}")

# Test USDA FAS (no auth needed)
print("Testing USDA FAS...")
fas = USDATFASCollector()
result = fas.collect(data_type='export_sales', commodities=['corn'])
print(f"  Success: {result.success}, Records: {result.records_fetched}")

# Test Drought Monitor (no auth needed)
print("Testing Drought Monitor...")
drought = DroughtCollector()
result = drought.collect()
print(f"  Success: {result.success}, Records: {result.records_fetched}")
```

### Test Collectors Requiring API Keys

```python
# test_api_collectors.py
import os
from dotenv import load_dotenv
load_dotenv()

from commodity_pipeline.data_collectors import (
    EIAEthanolCollector,
    EIAPetroleumCollector,
    NASSCollector,
)

# Test EIA Ethanol
if os.getenv('EIA_API_KEY'):
    print("Testing EIA Ethanol...")
    eia = EIAEthanolCollector()
    result = eia.collect()
    print(f"  Success: {result.success}, Records: {result.records_fetched}")
    if result.warnings:
        print(f"  Warnings: {result.warnings}")

# Test NASS
if os.getenv('NASS_API_KEY'):
    print("Testing USDA NASS...")
    nass = NASSCollector()
    result = nass.collect(data_type='crop_progress', commodities=['corn'])
    print(f"  Success: {result.success}, Records: {result.records_fetched}")
```

### Run Full Collector Test Suite

```bash
python scripts/test_all_collectors.py
```

---

## 6. Power BI Connection

### Option A: Direct PostgreSQL Connection

1. **Open Power BI Desktop**
2. **Get Data → PostgreSQL database**
3. **Enter connection details:**
   - Server: `localhost` (or your server IP)
   - Database: `commodity_db`
   - Data Connectivity mode: `Import` (recommended) or `DirectQuery`

4. **Enter credentials:**
   - User: `commodity_user`
   - Password: your password

5. **Select tables to import:**
   - `trade_flows`
   - `export_sales`
   - `supply_demand`
   - `futures_settlements`
   - `ethanol_data`
   - `cot_positions`
   - `drought_data`

### Option B: Export to CSV/Excel for Power BI

```bash
# Export tables to CSV
python scripts/export_for_powerbi.py --format csv --output ./exports/

# Export to Excel workbook
python scripts/export_for_powerbi.py --format xlsx --output ./exports/commodity_data.xlsx
```

### Option C: ODBC Connection (Any BI Tool)

```bash
# Install ODBC driver
sudo apt-get install odbc-postgresql

# Configure ODBC
sudo nano /etc/odbc.ini
```

```ini
[CommodityDB]
Description = Commodity Database
Driver = PostgreSQL
Servername = localhost
Database = commodity_db
Username = commodity_user
Password = your_password
Port = 5432
```

### Power BI Data Model Relationships

Set up these relationships in Power BI:

```
commodities.code → trade_flows.commodity_code
commodities.code → export_sales.commodity_code
commodities.code → supply_demand.commodity_code
commodities.code → futures_settlements.commodity_code
commodities.code → cot_positions.commodity_code
countries.code → trade_flows.reporter_country
countries.code → trade_flows.partner_country
countries.code → export_sales.destination_country
```

---

## 7. Sample Queries and Views

### Create Analytical Views

```sql
-- Weekly export sales summary by commodity
CREATE OR REPLACE VIEW v_weekly_export_summary AS
SELECT
    commodity_code,
    week_ending,
    marketing_year,
    SUM(net_sales_week) as total_net_sales,
    SUM(shipments_week) as total_shipments,
    SUM(outstanding_sales) as total_outstanding,
    COUNT(DISTINCT destination_country) as num_destinations
FROM export_sales
GROUP BY commodity_code, week_ending, marketing_year
ORDER BY week_ending DESC;

-- Corn supply/demand balance
CREATE OR REPLACE VIEW v_corn_balance AS
SELECT
    marketing_year,
    country_code,
    report_date,
    beginning_stocks,
    production,
    imports,
    total_supply,
    feed_use,
    food_use + seed_use as fsi_use,
    exports,
    ending_stocks,
    ROUND(ending_stocks::numeric / NULLIF(total_demand, 0) * 100, 1) as stocks_to_use_pct
FROM supply_demand
WHERE commodity_code = 'CORN'
ORDER BY marketing_year DESC, report_date DESC;

-- COT net positioning trend
CREATE OR REPLACE VIEW v_cot_net_positions AS
SELECT
    commodity_code,
    report_date,
    noncommercial_net as spec_net,
    commercial_net as comm_net,
    open_interest,
    ROUND(noncommercial_net::numeric / NULLIF(open_interest, 0) * 100, 1) as spec_net_pct
FROM cot_positions
WHERE report_type = 'legacy'
ORDER BY commodity_code, report_date DESC;

-- Ethanol production and stocks
CREATE OR REPLACE VIEW v_ethanol_weekly AS
SELECT
    week_ending,
    production_kbd,
    stocks_kb,
    implied_demand_kbd,
    ROUND(stocks_kb::numeric / NULLIF(production_kbd * 7, 0), 1) as days_supply
FROM ethanol_data
ORDER BY week_ending DESC;

-- Trade flow summary by partner
CREATE OR REPLACE VIEW v_trade_by_partner AS
SELECT
    commodity_code,
    partner_country,
    flow_type,
    DATE_TRUNC('month', trade_date) as month,
    SUM(quantity_mt) as total_mt,
    SUM(value_usd) as total_value_usd,
    AVG(unit_value) as avg_unit_value
FROM trade_flows
GROUP BY commodity_code, partner_country, flow_type, DATE_TRUNC('month', trade_date)
ORDER BY month DESC, total_mt DESC;
```

### Sample Power BI Queries (M/DAX)

```
// Total Export Sales by Marketing Year
TotalExportSales =
CALCULATE(
    SUM(export_sales[accumulated_sales]),
    LASTDATE(export_sales[week_ending])
)

// Spec Net Position Change WoW
SpecNetChange =
VAR CurrentWeek = MAX(cot_positions[report_date])
VAR PriorWeek = DATEADD(cot_positions[report_date], -7, DAY)
RETURN
    CALCULATE(SUM(cot_positions[noncommercial_net]), cot_positions[report_date] = CurrentWeek) -
    CALCULATE(SUM(cot_positions[noncommercial_net]), cot_positions[report_date] = PriorWeek)

// Stocks-to-Use Ratio
StocksToUse =
DIVIDE(
    SUM(supply_demand[ending_stocks]),
    SUM(supply_demand[total_demand]),
    0
) * 100
```

---

## 8. Ongoing Data Collection

### Automated Scheduler

```bash
# Start the scheduler daemon
python scripts/run_scheduler.py --daemon

# Or run specific collections manually
python scripts/run_scheduler.py --collectors eia_ethanol cftc_cot

# Check today's scheduled collections
python -m commodity_pipeline.scheduler.report_scheduler today
```

### Cron Jobs (Linux)

```bash
# Edit crontab
crontab -e
```

Add these entries:

```cron
# Weekday collections based on release schedules

# Monday 4:15 PM ET - USDA Crop Progress (after 4:00 release)
15 16 * * 1 cd /home/user/RLC-Agent && ./venv/bin/python scripts/collect.py --collector usda_nass

# Wednesday 10:45 AM ET - EIA Petroleum & Ethanol (after 10:30 release)
45 10 * * 3 cd /home/user/RLC-Agent && ./venv/bin/python scripts/collect.py --collectors eia_petroleum eia_ethanol

# Thursday 8:45 AM ET - Export Sales, Drought (after 8:30 release)
45 8 * * 4 cd /home/user/RLC-Agent && ./venv/bin/python scripts/collect.py --collectors usda_fas drought

# Thursday 2:00 PM ET - Canada CGC
0 14 * * 4 cd /home/user/RLC-Agent && ./venv/bin/python scripts/collect.py --collector canada_cgc

# Friday 3:45 PM ET - CFTC COT (after 3:30 release)
45 15 * * 5 cd /home/user/RLC-Agent && ./venv/bin/python scripts/collect.py --collector cftc_cot

# Daily 5:30 PM ET - CME Settlements (after market close)
30 17 * * 1-5 cd /home/user/RLC-Agent && ./venv/bin/python scripts/collect.py --collector cme_settlements

# Monthly - 12th at 12:15 PM ET - WASDE (after noon release)
15 12 12 * * cd /home/user/RLC-Agent && ./venv/bin/python scripts/collect.py --collector usda_wasde

# Monthly - 10th at 8:00 AM ET - MPOB
0 8 10 * * cd /home/user/RLC-Agent && ./venv/bin/python scripts/collect.py --collector mpob
```

### Windows Task Scheduler

Create scheduled tasks using PowerShell or Task Scheduler GUI with equivalent timing.

---

## 9. Troubleshooting

### Common Issues

#### API Key Errors

```
Error: 401 Unauthorized / Invalid API key
```

**Solution:** Verify your API key is correct in `.env` and the environment is loaded:

```python
from dotenv import load_dotenv
import os
load_dotenv()
print(os.getenv('EIA_API_KEY'))  # Should show your key
```

#### Database Connection Errors

```
Error: could not connect to server
```

**Solution:** Check PostgreSQL is running and credentials are correct:

```bash
# Check PostgreSQL status
sudo systemctl status postgresql

# Test connection
psql -U commodity_user -d commodity_db -c "SELECT 1;"
```

#### Rate Limiting

```
Error: 429 Too Many Requests
```

**Solution:** Add delays between requests or use caching:

```python
# Collectors have built-in rate limiting, but you can adjust
config = CollectorConfig(rate_limit_delay=2.0)  # 2 seconds between requests
```

#### Missing Historical Data

Some sources don't provide historical data via API. Options:
- CFTC COT: Full history available via bulk download
- CME: Historical data requires DataMine subscription or Quandl
- NASS: Stocks data only quarterly, production annual

### Log Locations

```bash
# View collector logs
tail -f logs/commodity_pipeline.log

# View specific collector errors
grep "ERROR" logs/commodity_pipeline.log | grep "eia"
```

### Health Check Script

```bash
python scripts/health_check.py
```

Output:
```
Database Connection: ✓
EIA API Key: ✓
NASS API Key: ✓
Census API Key: ✓
Last CFTC COT Collection: 2024-01-05 (2 days ago)
Last EIA Ethanol Collection: 2024-01-03 (4 days ago)
...
```

---

## Next Steps

After completing this setup:

1. **Run initial historical load** - Start with 2020 data, expand as needed
2. **Build Power BI dashboards** - Use the sample views as starting points
3. **Set up automated collection** - Configure cron jobs for your needs
4. **Add additional regions** - South America, Europe collectors planned
5. **Custom analysis** - Build commodity-specific analysis notebooks

---

## Support

- Check collector status: `python -m commodity_pipeline.data_collectors.collectors --list`
- View release schedule: `python -m commodity_pipeline.scheduler.report_scheduler week`
- Report issues: GitHub issues page
