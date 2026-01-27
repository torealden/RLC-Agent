# Database Migration - Bronze/Silver/Gold

Migration scripts to set up the medallion architecture and migrate existing data.

## Prerequisites

1. PostgreSQL running locally (or update connection settings in scripts)
2. Python 3.8+ with `psycopg2` installed:
   ```bash
   pip install psycopg2-binary
   ```

## Quick Start

Run the full migration:
```bash
cd database/migrations
python run_full_migration.py
```

Or run steps individually:

## Step-by-Step Migration

### Step 1: Inventory Existing Data
```bash
python 01_inventory_postgres.py
```
- Lists all schemas and tables in PostgreSQL
- Shows row counts
- Saves inventory to `postgres_inventory.json`

### Step 2: Deploy Medallion Schema
```bash
python 02_deploy_medallion_schema.py
```
- Creates `core`, `audit`, `bronze`, `silver`, `gold` schemas
- Sets up dimension tables, audit tables, and views
- Runs SQL files from parent directory

### Step 3: Migrate Existing PostgreSQL Data
```bash
python 03_migrate_existing_to_bronze.py
```
- Copies data from `public.*` tables to `bronze.raw_*` tables
- Preserves original tables (non-destructive)
- Adds audit columns (migrated_at, source_table)

### Step 4: Migrate SQLite Data
```bash
python 04_migrate_sqlite_to_bronze.py
```
- Reads from `data/rlc_commodities.db`
- Creates `bronze.sqlite_*` tables
- Migrates 104,768 rows from commodity_balance_sheets

## Connection Settings

Default PostgreSQL settings (update in each script if different):
```python
PG_HOST = "localhost"
PG_PORT = "5432"
PG_DATABASE = "rlc_commodities"
PG_USER = "postgres"
PG_PASSWORD = "SoupBoss1"
```

## After Migration

### Power BI Connection
Connect to the new schema tables:
- `bronze.*` - Raw source data
- `silver.observation` - Standardized time-series
- `gold.*` - Business-ready views

### Update Collectors
Change collectors to write to bronze schema:
```python
# Old: INSERT INTO public.commodity_balance_sheets
# New: INSERT INTO bronze.wasde_cell
```

## Schema Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                        GOLD LAYER                                │
│  Business-ready views for Power BI and reports                  │
│  (gold.us_corn_balance_sheet, gold.wasde_changes, etc.)        │
├─────────────────────────────────────────────────────────────────┤
│                       SILVER LAYER                               │
│  Standardized time-series: (series_id, time, value)            │
│  (silver.observation)                                            │
├─────────────────────────────────────────────────────────────────┤
│                       BRONZE LAYER                               │
│  Raw source data, exactly as received                           │
│  (bronze.wasde_cell, bronze.sqlite_commodity_balance_sheets)    │
├─────────────────────────────────────────────────────────────────┤
│                     DIMENSION TABLES                             │
│  (core.data_source, core.commodity, core.series, core.unit)    │
└─────────────────────────────────────────────────────────────────┘
```
