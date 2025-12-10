# HigbyBarrett Weekly Report Automation Guide

## Comprehensive Step-by-Step Setup Guide

**Author:** Claude AI Assistant
**Date:** December 10, 2025
**Version:** 1.0

---

## Table of Contents

1. [Report Analysis Summary](#1-report-analysis-summary)
2. [Adding Data to the Database](#2-adding-data-to-the-database)
3. [Setting Up the Business Partner Agent](#3-setting-up-the-business-partner-agent)
4. [Scheduler Configuration](#4-scheduler-configuration)
5. [Data Pipeline Schedule](#5-data-pipeline-schedule)
6. [Quick Start Commands](#6-quick-start-commands)

---

## 1. Report Analysis Summary

### Commodities Covered

Based on the HB Weekly Reports, the following commodities require data:

| Commodity | Contract Symbols | Key Metrics |
|-----------|-----------------|-------------|
| **Corn** | ZC (CBOT) | Futures, Dec-Mar spread, Gulf basis, ethanol production |
| **Wheat (SRW)** | ZW (CBOT) | Futures, Dec-Mar spread, exports, Russian FOB |
| **Wheat (HRW)** | KE (KCBT) | Futures, spreads, Kansas basis |
| **Soybeans** | ZS (CBOT) | Futures, Jan-Mar spread, crush data, China exports |
| **Soybean Meal** | ZM (CBOT) | Futures, spreads, domestic/export demand |
| **Soybean Oil** | ZL (CBOT) | Futures, biofuel demand, exports |

### Data Sources Required

| Source | Report Type | Frequency | Priority | Current Status |
|--------|-------------|-----------|----------|----------------|
| USDA WASDE | Supply/Demand | Monthly | Critical | **TO ADD** |
| USDA Export Inspections | Shipment volumes | Weekly (Thurs) | Critical | Ready (agent exists) |
| USDA Export Sales | Sales commitments | Weekly (Thurs) | Critical | **TO ADD** |
| USDA Crop Progress | Planting/harvest | Weekly (Mon) | High | **TO ADD** |
| EIA Ethanol | Production/stocks | Weekly (Wed) | High | Partially configured |
| NOPA Crush | Soybean crush | Monthly (15th) | High | **TO ADD** |
| CME Futures | Daily prices | Daily | Critical | **TO ADD** |
| CFTC COT | Speculative positions | Weekly (Fri) | Medium | **TO ADD** |
| Brazil ANEC | Export lineups | Weekly | High | Integrated |
| Argentina BCBA | Crop conditions | Weekly | High | Integrated |

---

## 2. Adding Data to the Database

### Step 2.1: Extend Database Schema

Create these additional tables in your database. Add this to a new migration file:

```sql
-- File: migrations/002_hb_report_tables.sql

-- WASDE Supply/Demand Balance Sheets
CREATE TABLE IF NOT EXISTS wasde_balance_sheet (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    report_date DATE NOT NULL,
    marketing_year VARCHAR(20) NOT NULL,  -- e.g., '2024/25'
    commodity VARCHAR(50) NOT NULL,
    region VARCHAR(50) DEFAULT 'US',  -- US, World, etc.

    -- Supply side (million bushels or MMT)
    beginning_stocks DECIMAL(12,2),
    production DECIMAL(12,2),
    imports DECIMAL(12,2),
    total_supply DECIMAL(12,2),

    -- Demand side
    feed_residual DECIMAL(12,2),
    food_seed_industrial DECIMAL(12,2),
    ethanol DECIMAL(12,2),
    crush DECIMAL(12,2),
    exports DECIMAL(12,2),
    total_use DECIMAL(12,2),

    -- Carryout
    ending_stocks DECIMAL(12,2),
    stocks_to_use DECIMAL(6,3),  -- Ratio

    -- Yield (US only)
    planted_area DECIMAL(10,2),
    harvested_area DECIMAL(10,2),
    yield_per_acre DECIMAL(8,2),

    unit VARCHAR(20) DEFAULT 'million_bushels',
    source VARCHAR(50) DEFAULT 'USDA_WASDE',
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,

    UNIQUE(report_date, marketing_year, commodity, region)
);

-- Futures Prices
CREATE TABLE IF NOT EXISTS futures_prices (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    price_date DATE NOT NULL,
    commodity VARCHAR(50) NOT NULL,
    contract_month VARCHAR(20) NOT NULL,  -- e.g., 'Dec25', 'Mar26'
    exchange VARCHAR(20) DEFAULT 'CME',

    open_price DECIMAL(12,4),
    high_price DECIMAL(12,4),
    low_price DECIMAL(12,4),
    close_price DECIMAL(12,4),
    settle_price DECIMAL(12,4),

    volume INTEGER,
    open_interest INTEGER,

    unit VARCHAR(20),  -- cents/bu, $/ton, cents/lb
    source VARCHAR(50) DEFAULT 'CME',
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,

    UNIQUE(price_date, commodity, contract_month, exchange)
);

-- Calendar Spreads
CREATE TABLE IF NOT EXISTS calendar_spreads (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    price_date DATE NOT NULL,
    commodity VARCHAR(50) NOT NULL,
    front_month VARCHAR(20) NOT NULL,
    back_month VARCHAR(20) NOT NULL,
    exchange VARCHAR(20) DEFAULT 'CME',

    spread_value DECIMAL(12,4),  -- Front - Back
    full_carry DECIMAL(12,4),
    pct_of_carry DECIMAL(6,3),

    source VARCHAR(50) DEFAULT 'CME',
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,

    UNIQUE(price_date, commodity, front_month, back_month)
);

-- Export Inspections (Weekly FGIS)
CREATE TABLE IF NOT EXISTS export_inspections (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    report_date DATE NOT NULL,
    week_ending DATE NOT NULL,
    commodity VARCHAR(50) NOT NULL,
    destination VARCHAR(100),

    weekly_volume DECIMAL(12,2),  -- thousand metric tons
    marketing_year_total DECIMAL(12,2),
    year_ago_total DECIMAL(12,2),
    pct_change_yoy DECIMAL(8,2),

    usda_projection DECIMAL(12,2),
    pct_of_projection DECIMAL(8,2),

    unit VARCHAR(20) DEFAULT 'thousand_mt',
    source VARCHAR(50) DEFAULT 'USDA_FGIS',
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,

    UNIQUE(week_ending, commodity, destination)
);

-- Export Sales (Weekly FAS)
CREATE TABLE IF NOT EXISTS export_sales (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    report_date DATE NOT NULL,
    week_ending DATE NOT NULL,
    commodity VARCHAR(50) NOT NULL,
    marketing_year VARCHAR(20) NOT NULL,

    net_sales DECIMAL(12,2),
    exports DECIMAL(12,2),
    outstanding_sales DECIMAL(12,2),
    accumulated_exports DECIMAL(12,2),

    -- By destination
    china_sales DECIMAL(12,2),
    mexico_sales DECIMAL(12,2),
    japan_sales DECIMAL(12,2),
    other_sales DECIMAL(12,2),

    unit VARCHAR(20) DEFAULT 'thousand_mt',
    source VARCHAR(50) DEFAULT 'USDA_FAS',
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,

    UNIQUE(week_ending, commodity, marketing_year)
);

-- NOPA Crush Data
CREATE TABLE IF NOT EXISTS nopa_crush (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    report_date DATE NOT NULL,
    report_month DATE NOT NULL,

    soybeans_crushed DECIMAL(12,2),  -- million bushels
    soybean_oil_stocks DECIMAL(12,2),  -- million pounds

    crush_yoy_change DECIMAL(8,2),  -- percent
    oil_stocks_yoy_change DECIMAL(8,2),

    source VARCHAR(50) DEFAULT 'NOPA',
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,

    UNIQUE(report_month)
);

-- EIA Ethanol Data
CREATE TABLE IF NOT EXISTS eia_ethanol (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    report_date DATE NOT NULL,
    week_ending DATE NOT NULL,

    production_mbpd DECIMAL(10,3),  -- million barrels per day
    stocks_million_barrels DECIMAL(10,2),
    imports_mbpd DECIMAL(10,3),

    production_change_wow DECIMAL(8,2),
    stocks_change_wow DECIMAL(8,2),

    implied_corn_grind DECIMAL(12,2),  -- million bushels (calculated)

    source VARCHAR(50) DEFAULT 'EIA',
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,

    UNIQUE(week_ending)
);

-- CFTC Commitments of Traders
CREATE TABLE IF NOT EXISTS cftc_cot (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    report_date DATE NOT NULL,
    as_of_date DATE NOT NULL,
    commodity VARCHAR(50) NOT NULL,
    exchange VARCHAR(20) DEFAULT 'CME',

    -- Managed Money
    mm_long INTEGER,
    mm_short INTEGER,
    mm_net INTEGER,
    mm_net_change INTEGER,

    -- Commercial
    comm_long INTEGER,
    comm_short INTEGER,
    comm_net INTEGER,

    -- Open Interest
    open_interest INTEGER,

    source VARCHAR(50) DEFAULT 'CFTC',
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,

    UNIQUE(as_of_date, commodity, exchange)
);

-- International FOB Prices
CREATE TABLE IF NOT EXISTS intl_fob_prices (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    price_date DATE NOT NULL,
    commodity VARCHAR(50) NOT NULL,
    origin VARCHAR(50) NOT NULL,  -- Brazil, Argentina, Russia, Ukraine
    port VARCHAR(100),

    fob_price DECIMAL(12,2),
    currency VARCHAR(10) DEFAULT 'USD',
    unit VARCHAR(20),  -- $/mt, $/bu

    premium_to_us DECIMAL(12,2),  -- vs US Gulf
    freight_to_china DECIMAL(12,2),

    source VARCHAR(50),
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,

    UNIQUE(price_date, commodity, origin, port)
);

-- Weather/Crop Conditions
CREATE TABLE IF NOT EXISTS crop_conditions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    report_date DATE NOT NULL,
    week_ending DATE NOT NULL,
    commodity VARCHAR(50) NOT NULL,
    region VARCHAR(50) DEFAULT 'US',

    planted_pct DECIMAL(5,1),
    emerged_pct DECIMAL(5,1),
    harvested_pct DECIMAL(5,1),

    good_excellent_pct DECIMAL(5,1),
    fair_pct DECIMAL(5,1),
    poor_very_poor_pct DECIMAL(5,1),

    five_year_avg_ge DECIMAL(5,1),  -- 5-year average G/E
    year_ago_ge DECIMAL(5,1),

    source VARCHAR(50) DEFAULT 'USDA_NASS',
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,

    UNIQUE(week_ending, commodity, region)
);

-- Create indexes for performance
CREATE INDEX IF NOT EXISTS idx_wasde_date_commodity ON wasde_balance_sheet(report_date, commodity);
CREATE INDEX IF NOT EXISTS idx_futures_date ON futures_prices(price_date);
CREATE INDEX IF NOT EXISTS idx_spreads_date ON calendar_spreads(price_date);
CREATE INDEX IF NOT EXISTS idx_inspections_week ON export_inspections(week_ending);
CREATE INDEX IF NOT EXISTS idx_sales_week ON export_sales(week_ending);
CREATE INDEX IF NOT EXISTS idx_ethanol_week ON eia_ethanol(week_ending);
CREATE INDEX IF NOT EXISTS idx_cot_date ON cftc_cot(as_of_date);
CREATE INDEX IF NOT EXISTS idx_fob_date ON intl_fob_prices(price_date);
CREATE INDEX IF NOT EXISTS idx_conditions_week ON crop_conditions(week_ending);
```

### Step 2.2: Apply Schema Migration

Run the schema migration:

```bash
# Navigate to the commodity pipeline
cd /home/user/RLC-Agent/commodity_pipeline/usda_ams_agent

# Apply schema (adjust path to your migration file)
sqlite3 ./data/rlc_commodities.db < ../migrations/002_hb_report_tables.sql

# Verify tables created
sqlite3 ./data/rlc_commodities.db ".tables"
```

### Step 2.3: Manual Data Entry Workflow

For data not yet automated, use this workflow:

#### Option A: Direct SQL Insert (via Python)

```python
# Example: Insert WASDE data manually
from commodity_pipeline.usda_ams_agent.agents.database_agent import DatabaseAgent

db = DatabaseAgent(db_type='sqlite', connection_params={
    'database': './data/rlc_commodities.db'
})

# Insert WASDE data
wasde_record = {
    'report_date': '2025-11-08',
    'marketing_year': '2024/25',
    'commodity': 'corn',
    'region': 'US',
    'beginning_stocks': 1532,
    'production': 16752,
    'imports': 25,
    'total_supply': 18309,
    'feed_residual': 6100,
    'ethanol': 5600,
    'exports': 3075,
    'total_use': 16155,
    'ending_stocks': 2154,
    'stocks_to_use': 0.133,
    'yield_per_acre': 186.0,
    'unit': 'million_bushels',
    'source': 'USDA_WASDE'
}

# Use raw SQL for custom tables
with db.get_connection() as conn:
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO wasde_balance_sheet
        (report_date, marketing_year, commodity, region, beginning_stocks,
         production, imports, total_supply, feed_residual, ethanol, exports,
         total_use, ending_stocks, stocks_to_use, yield_per_acre, unit, source)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, tuple(wasde_record.values()))
    conn.commit()
```

#### Option B: Spreadsheet Import

Create an Excel/CSV with the schema columns and import:

```python
import pandas as pd

# Read WASDE data from Excel
df = pd.read_excel('wasde_data.xlsx')

# Insert into database
with db.get_connection() as conn:
    df.to_sql('wasde_balance_sheet', conn, if_exists='append', index=False)
```

---

## 3. Setting Up the Business Partner Agent

### Step 3.1: Configure Environment Variables

Update `/home/user/RLC-Agent/commodity_pipeline/usda_ams_agent/.env`:

```bash
# =============================================================================
# HB WEEKLY REPORT CONFIGURATION
# =============================================================================

# Data Source (dropbox or database)
HB_DATA_SOURCE=database

# Database Configuration
HB_DB_TYPE=sqlite
HB_DB_HOST=localhost
HB_DB_PORT=5432
HB_DB_NAME=rlc_commodities
HB_DB_USER=
HB_DB_PASSWORD=

# Dropbox Configuration (for internal spreadsheet)
DROPBOX_ACCESS_TOKEN=sl.u.AGJig...
DROPBOX_APP_KEY=your_app_key
DROPBOX_APP_SECRET=your_app_secret
DROPBOX_REFRESH_TOKEN=your_refresh_token

# LLM Configuration
LLM_PROVIDER=openai
LLM_MODEL=gpt-4
OPENAI_API_KEY=sk-...

# Scheduling
HB_SCHEDULE_DAY=1        # Tuesday (0=Monday)
HB_SCHEDULE_TIME=06:00   # 6:00 AM
HB_TIMEZONE=America/Chicago

# Notifications
REPORT_RECIPIENTS=tore.alden@roundlakescommodities.com
ERROR_RECIPIENTS=tore.alden@roundlakescommodities.com

# API Keys for Data Sources
USDA_AMS_API_KEY=/CY5wVCkVhdLnR1Pf/jb+MUuOw9VB37z
USDA_FAS_API_KEY=your_fas_key
EIA_API_KEY=your_eia_key
```

### Step 3.2: Test the HB Report Writer

```bash
cd /home/user/RLC-Agent/commodity_pipeline/hb_weekly_report_writer

# Test component configuration
python main.py test

# Validate configuration
python main.py validate

# Generate a test report
python main.py generate --date 2025-11-20
```

### Step 3.3: Connect to Master Agent (Optional)

The RLC Master Agent can orchestrate the HB Report Writer. Update the master agent configuration:

```python
# In /home/user/RLC-Agent/rlc_master_agent/config/settings.py

# Add HB Report Writer to data sources
HB_REPORT_CONFIG = {
    'enabled': True,
    'agent_path': '../commodity_pipeline/hb_weekly_report_writer',
    'schedule': {
        'day': 'tuesday',
        'time': '06:00',
        'timezone': 'America/Chicago'
    }
}
```

---

## 4. Scheduler Configuration

### Step 4.1: Built-in Scheduler (Development)

Start the built-in scheduler:

```bash
cd /home/user/RLC-Agent/commodity_pipeline/hb_weekly_report_writer

# Start scheduler (runs in foreground)
python main.py schedule --start

# Or use APScheduler (more robust)
python main.py schedule --start --apscheduler
```

### Step 4.2: System Cron (Production)

Get the cron expression and install:

```bash
# Show cron expression
python main.py schedule --cron

# Output: 0 6 * * 1 cd /home/user/RLC-Agent/commodity_pipeline/hb_weekly_report_writer && python main.py generate

# Install to system crontab
crontab -e

# Add this line (Tuesday 6:00 AM Central):
0 6 * * 2 cd /home/user/RLC-Agent/commodity_pipeline/hb_weekly_report_writer && python main.py generate >> /var/log/hb_report.log 2>&1
```

### Step 4.3: Master Scheduler Architecture

For comprehensive automation, implement a central scheduler that coordinates all data pipelines:

```python
# File: /home/user/RLC-Agent/scheduler/master_scheduler.py

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
import logging

logger = logging.getLogger(__name__)

class MasterScheduler:
    """
    Central scheduler for all RLC data pipelines.
    Coordinates timing of data collection to ensure HB report has fresh data.
    """

    def __init__(self):
        self.scheduler = BackgroundScheduler(timezone='America/Chicago')
        self._setup_jobs()

    def _setup_jobs(self):
        """Configure all scheduled jobs"""

        # =================================================================
        # DATA COLLECTION JOBS (must run BEFORE report generation)
        # =================================================================

        # USDA AMS Daily (Daily Grain Bids, Ethanol)
        # Runs: Daily at 13:00 CT (after markets close)
        self.scheduler.add_job(
            self._run_usda_ams_daily,
            CronTrigger(hour=13, minute=0),
            id='usda_ams_daily',
            name='USDA AMS Daily Reports'
        )

        # USDA Export Inspections (FGIS)
        # Released: Thursday mornings
        # Runs: Thursday at 12:00 CT
        self.scheduler.add_job(
            self._run_export_inspections,
            CronTrigger(day_of_week='thu', hour=12, minute=0),
            id='export_inspections',
            name='USDA Export Inspections'
        )

        # USDA Export Sales (FAS)
        # Released: Thursday mornings
        # Runs: Thursday at 12:30 CT
        self.scheduler.add_job(
            self._run_export_sales,
            CronTrigger(day_of_week='thu', hour=12, minute=30),
            id='export_sales',
            name='USDA Export Sales'
        )

        # EIA Ethanol Weekly
        # Released: Wednesday mornings
        # Runs: Wednesday at 11:00 CT
        self.scheduler.add_job(
            self._run_eia_ethanol,
            CronTrigger(day_of_week='wed', hour=11, minute=0),
            id='eia_ethanol',
            name='EIA Ethanol Report'
        )

        # USDA Crop Progress (seasonal)
        # Released: Monday evenings
        # Runs: Monday at 17:00 CT
        self.scheduler.add_job(
            self._run_crop_progress,
            CronTrigger(day_of_week='mon', hour=17, minute=0),
            id='crop_progress',
            name='USDA Crop Progress'
        )

        # CFTC COT Report
        # Released: Friday afternoons
        # Runs: Friday at 16:00 CT
        self.scheduler.add_job(
            self._run_cftc_cot,
            CronTrigger(day_of_week='fri', hour=16, minute=0),
            id='cftc_cot',
            name='CFTC COT Report'
        )

        # CME Futures Prices
        # Runs: Daily at 18:00 CT (after settlement)
        self.scheduler.add_job(
            self._run_futures_prices,
            CronTrigger(hour=18, minute=0),
            id='futures_prices',
            name='CME Futures Prices'
        )

        # South America Trade Data
        # Runs: Monthly on 15th at 08:00 CT
        self.scheduler.add_job(
            self._run_south_america,
            CronTrigger(day=15, hour=8, minute=0),
            id='south_america',
            name='South America Trade Data'
        )

        # WASDE (Monthly)
        # Released: ~12th of each month
        # Runs: Monthly on 12th at 13:00 CT
        self.scheduler.add_job(
            self._run_wasde,
            CronTrigger(day=12, hour=13, minute=0),
            id='wasde',
            name='USDA WASDE Report'
        )

        # NOPA Crush (Monthly)
        # Released: ~15th of each month
        # Runs: Monthly on 15th at 12:00 CT
        self.scheduler.add_job(
            self._run_nopa_crush,
            CronTrigger(day=15, hour=12, minute=0),
            id='nopa_crush',
            name='NOPA Crush Report'
        )

        # =================================================================
        # HB REPORT GENERATION
        # =================================================================

        # HB Weekly Report
        # Runs: Tuesday at 06:00 CT (after all data collected)
        self.scheduler.add_job(
            self._run_hb_report,
            CronTrigger(day_of_week='tue', hour=6, minute=0),
            id='hb_weekly_report',
            name='HB Weekly Report Generation'
        )

        logger.info("Master scheduler configured with all jobs")

    def _run_usda_ams_daily(self):
        """Run USDA AMS daily data collection"""
        from commodity_pipeline.usda_ams_agent.usda_ams_collector_asynch import USDACollector
        collector = USDACollector()
        collector.run_daily_collection()

    def _run_export_inspections(self):
        """Run FGIS export inspections collection"""
        # TODO: Implement
        logger.info("Export inspections collection - TO IMPLEMENT")

    def _run_export_sales(self):
        """Run FAS export sales collection"""
        # TODO: Implement
        logger.info("Export sales collection - TO IMPLEMENT")

    def _run_eia_ethanol(self):
        """Run EIA ethanol data collection"""
        # TODO: Implement
        logger.info("EIA ethanol collection - TO IMPLEMENT")

    def _run_crop_progress(self):
        """Run USDA crop progress collection"""
        # TODO: Implement
        logger.info("Crop progress collection - TO IMPLEMENT")

    def _run_cftc_cot(self):
        """Run CFTC COT collection"""
        # TODO: Implement
        logger.info("CFTC COT collection - TO IMPLEMENT")

    def _run_futures_prices(self):
        """Run CME futures prices collection"""
        # TODO: Implement
        logger.info("Futures prices collection - TO IMPLEMENT")

    def _run_south_america(self):
        """Run South America trade data collection"""
        from commodity_pipeline.south_america_trade_data.orchestrator import SATradeOrchestrator
        orchestrator = SATradeOrchestrator()
        orchestrator.run_all_countries()

    def _run_wasde(self):
        """Run WASDE data collection"""
        # TODO: Implement
        logger.info("WASDE collection - TO IMPLEMENT")

    def _run_nopa_crush(self):
        """Run NOPA crush data collection"""
        # TODO: Implement
        logger.info("NOPA crush collection - TO IMPLEMENT")

    def _run_hb_report(self):
        """Generate HB Weekly Report"""
        from commodity_pipeline.hb_weekly_report_writer.services.orchestrator import HBReportOrchestrator
        from commodity_pipeline.hb_weekly_report_writer.config.settings import HBWeeklyReportConfig

        config = HBWeeklyReportConfig.from_environment()
        orchestrator = HBReportOrchestrator(config)
        result = orchestrator.run_weekly_report()

        if result.success:
            logger.info(f"HB Report generated successfully: {result.document_path}")
        else:
            logger.error(f"HB Report failed: {result.errors}")

    def start(self):
        """Start the scheduler"""
        self.scheduler.start()
        logger.info("Master scheduler started")

    def stop(self):
        """Stop the scheduler"""
        self.scheduler.shutdown()
        logger.info("Master scheduler stopped")

    def get_status(self):
        """Get status of all scheduled jobs"""
        jobs = []
        for job in self.scheduler.get_jobs():
            jobs.append({
                'id': job.id,
                'name': job.name,
                'next_run': str(job.next_run_time),
                'trigger': str(job.trigger)
            })
        return jobs


if __name__ == '__main__':
    import time

    scheduler = MasterScheduler()
    scheduler.start()

    print("Master scheduler running. Press Ctrl+C to stop.")
    print("\nScheduled Jobs:")
    for job in scheduler.get_status():
        print(f"  {job['name']}: next run at {job['next_run']}")

    try:
        while True:
            time.sleep(60)
    except KeyboardInterrupt:
        scheduler.stop()
```

---

## 5. Data Pipeline Schedule

### Weekly Data Flow Timeline

```
MONDAY
├── 17:00 CT - USDA Crop Progress (seasonal)

TUESDAY
├── 06:00 CT - HB WEEKLY REPORT GENERATED ← FIRST DRAFT
│
│   Report uses data from:
│   - Previous Thursday's export data
│   - Previous Wednesday's ethanol data
│   - Previous Friday's COT data
│   - Monday's settlement prices

WEDNESDAY
├── 11:00 CT - EIA Ethanol Weekly

THURSDAY
├── 12:00 CT - USDA Export Inspections (FGIS)
├── 12:30 CT - USDA Export Sales (FAS)

FRIDAY
├── 16:00 CT - CFTC COT Report

DAILY
├── 13:00 CT - USDA AMS Daily Reports
├── 18:00 CT - CME Futures Prices

MONTHLY
├── 12th, 13:00 CT - USDA WASDE
├── 15th, 08:00 CT - South America Trade
├── 15th, 12:00 CT - NOPA Crush
```

### Data Dependencies for Tuesday Report

| Data Source | Collected | Age When Used | Critical? |
|-------------|-----------|---------------|-----------|
| CME Futures | Mon 18:00 | 12 hours | Yes |
| Export Inspections | Thu 12:00 | 4.75 days | Yes |
| Export Sales | Thu 12:30 | 4.7 days | Yes |
| EIA Ethanol | Wed 11:00 | 5.8 days | Yes |
| Crop Progress | Mon 17:00 | 13 hours | Seasonal |
| CFTC COT | Fri 16:00 | 3.6 days | No |
| WASDE | 12th | Varies | Monthly |

---

## 6. Quick Start Commands

### Immediate Setup (Today)

```bash
# 1. Navigate to project
cd /home/user/RLC-Agent

# 2. Apply database schema
sqlite3 commodity_pipeline/usda_ams_agent/data/rlc_commodities.db < docs/migrations/002_hb_report_tables.sql

# 3. Test HB Report Writer
cd commodity_pipeline/hb_weekly_report_writer
python main.py test
python main.py validate

# 4. Generate first report manually
python main.py generate

# 5. Start scheduler (for Tuesday automation)
python main.py schedule --start
```

### Production Setup

```bash
# 1. Install APScheduler for robust scheduling
pip install apscheduler pytz

# 2. Create systemd service for scheduler
sudo nano /etc/systemd/system/rlc-scheduler.service

# 3. Service content:
[Unit]
Description=RLC Agent Master Scheduler
After=network.target

[Service]
Type=simple
User=your_user
WorkingDirectory=/home/user/RLC-Agent
ExecStart=/usr/bin/python scheduler/master_scheduler.py
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target

# 4. Enable and start
sudo systemctl enable rlc-scheduler
sudo systemctl start rlc-scheduler
sudo systemctl status rlc-scheduler
```

---

## Next Steps

1. **Implement Missing Data Collectors:**
   - WASDE parser (PDF/API)
   - Export Sales (FAS API)
   - Crop Progress (NASS API)
   - CME Futures prices (need data source)
   - CFTC COT parser

2. **Create Data Import Scripts:**
   - Manual WASDE data entry UI
   - Excel import utilities
   - Historical data backfill

3. **Enhance HB Report Writer:**
   - Connect to new database tables
   - Add technical analysis charts
   - Improve LLM prompts for commodity analysis

4. **Test End-to-End:**
   - Run full week simulation
   - Verify data flow timing
   - Test report completeness

---

## Support

For questions about this guide:
- Review existing code in `/home/user/RLC-Agent/commodity_pipeline/`
- Check the architecture plan: `/home/user/RLC-Agent/ARCHITECTURE_PLAN.md`
- Contact: info@roundlakescommodities.com
