# Round Lakes Commodities - PostgreSQL Database

Production-ready PostgreSQL database for multi-agent commodity analytics.

## Quick Start

```bash
# Create database
createdb -U postgres rlc

# Run all DDL scripts in order
psql -U postgres -d rlc -f sql/00_init.sql
psql -U postgres -d rlc -f sql/01_schemas.sql
psql -U postgres -d rlc -f sql/02_core_dimensions.sql
psql -U postgres -d rlc -f sql/03_audit_tables.sql
psql -U postgres -d rlc -f sql/04_bronze_wasde.sql
psql -U postgres -d rlc -f sql/05_silver_observation.sql
psql -U postgres -d rlc -f sql/06_gold_views.sql
psql -U postgres -d rlc -f sql/07_roles_grants.sql
psql -U postgres -d rlc -f sql/08_functions.sql
```

Or use the install script:

```bash
./install.sh
```
# RLC Commodities Database Schema

Production-ready PostgreSQL database for Round Lakes Commodities data platform.

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                     BRONZE (Raw/Source-Faithful)                │
│  wasde_release, wasde_cell, raw_record                          │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                     SILVER (Standardized)                        │
│  observation (series_id, time, value, revision)                  │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                     GOLD (Curated/Business)                      │
│  wasde_corn_headline, wasde_corn_changes, stocks_to_use         │
└─────────────────────────────────────────────────────────────────┘
```

## Schema Reference

| Schema | Purpose | Access Pattern |
|--------|---------|----------------|
| `core` | Dimension tables (data_source, series, unit, location) | Read by all, write by admin |
| `audit` | Ingestion tracking (ingest_run, validation_status) | Write by ingest/checker |
| `bronze` | Raw source data (wasde_release, wasde_cell) | Write by ingest only |
| `silver` | Standardized observations | Write by ingest, read by analysts |
| `gold` | Business views and materialized tables | Read by analysts |
│                        GOLD LAYER                                │
│  Business-ready views, Excel-compatible outputs, dashboards     │
│  (gold.us_corn_balance_sheet, gold.wasde_changes, etc.)         │
├─────────────────────────────────────────────────────────────────┤
│                       SILVER LAYER                               │
│  Standardized time-series: (series_id, time, value)             │
│  Validated, consistent units, quality flags                      │
│  (silver.observation, silver.trade_flow)                         │
├─────────────────────────────────────────────────────────────────┤
│                       BRONZE LAYER                               │
│  Source-faithful raw data, exactly as received                   │
│  Full audit trail, enables reprocessing                          │
│  (bronze.wasde_cell, bronze.census_trade_raw, etc.)             │
├─────────────────────────────────────────────────────────────────┤
│                     DIMENSION TABLES                             │
│  data_source, commodity, location, unit, series                  │
├─────────────────────────────────────────────────────────────────┤
│                      AUDIT SCHEMA                                │
│  ingest_run, validation_status, raw_payload                      │
└─────────────────────────────────────────────────────────────────┘
```

## Quick Start

```bash
# Create database
createdb rlc_commodities

# Run scripts in order
psql -d rlc_commodities -f 001_schema_foundation.sql
psql -d rlc_commodities -f 002_bronze_layer.sql
psql -d rlc_commodities -f 003_silver_layer.sql
psql -d rlc_commodities -f 004_gold_layer.sql
psql -d rlc_commodities -f 005_roles_and_security.sql
psql -d rlc_commodities -f 006_operational_guidance.sql
```

## Script Descriptions

| Script | Purpose |
|--------|---------|
| `001_schema_foundation.sql` | Schemas, dimensions (data_source, unit, location, commodity, series) |
| `002_bronze_layer.sql` | Raw data tables (WASDE, Census, FGIS, AMS, EIA) |
| `003_silver_layer.sql` | Standardized observation table, helper functions |
| `004_gold_layer.sql` | Business views, reconciliation helpers |
| `005_roles_and_security.sql` | Role-based access control |
| `006_operational_guidance.sql` | Config recommendations, monitoring, backups |

## Roles

| Role | Access | Use Case |
|------|--------|----------|
| `ingest_writer` | Write bronze/silver/audit, read core | Collector agents |
| `checker` | Read all, write audit.validation_status | Validation agents |
| `analyst_reader` | Read silver/gold only | Report/trading agents |
| `rlc_admin` | Full access | Administration |

## Key Tables

### Core Dimensions

- **core.data_source** - Registry of data feeds (WASDE, EIA, etc.)
- **core.series** - Time-series metadata (unique per source + key)
- **core.unit** - Measurement units with conversions
- **core.location** - Geographic hierarchy
- **core.commodity** - Commodity reference

### Audit

- **audit.ingest_run** - Job execution tracking
- **audit.validation_status** - Validation state per release
- **audit.ingest_error** - Detailed error logging

### Bronze (WASDE)

- **bronze.wasde_release** - One row per monthly report
- **bronze.wasde_cell** - One row per published value

### Silver

- **silver.observation** - Universal time-series fact table

## Helper Functions

```sql
-- Get or create a series (atomic)
SELECT core.get_or_create_series(
    'wasde',                                    -- data_source_code
    'supply_demand.corn.production.us.monthly', -- series_key
    'US Corn Production',                       -- name
    'corn',                                     -- commodity_code
    'US',                                       -- location_code
    'mil_bu',                                   -- unit_code
    'monthly'                                   -- frequency
);

-- Open an ingest run
SELECT audit.open_ingest_run('wasde', 'wasde_monthly_ingest', 'agent_01');

-- Upsert a WASDE cell (idempotent)
SELECT bronze.upsert_wasde_cell(
    1,                  -- release_id
    '04',               -- table_id (US Corn)
    'production',       -- row_id
    '2024/25',          -- column_id
    '14,900'            -- value_text
);

-- Upsert an observation (idempotent)
SELECT silver.upsert_observation(
    123,                            -- series_id
    '2024-09-01'::TIMESTAMPTZ,      -- observation_time
    14900,                          -- value
    1                               -- ingest_run_id
);

-- Close an ingest run
SELECT audit.close_ingest_run(1, 'success');
```

## Gold Views

| View | Purpose |
|------|---------|
| `gold.wasde_corn_headline` | Key corn metrics pivoted wide |
| `gold.wasde_corn_changes` | Month-over-month changes |
| `gold.wasde_soybean_headline` | Key soybean metrics |
| `gold.us_grains_summary` | Cross-commodity comparison |
| `gold.stocks_to_use` | Calculated S/U ratio |
| `gold.reconciliation_wasde_summary` | Row counts for validation |

## Monitoring

```sql
-- Check connections
SELECT * FROM gold.monitor_connections;

-- Find slow queries
SELECT * FROM gold.monitor_slow_queries;

-- Check ingest runs
SELECT * FROM gold.monitor_ingest_runs;

-- System health check
SELECT * FROM gold.check_system_health();
```

## Backup

Daily backup script at `/opt/rlc/scripts/backup_daily.sh`:
- Base backup with pg_basebackup
- Logical dump with pg_dump
- 7-day retention

## pgBouncer

Recommended settings for ~10 concurrent agents:
- Pool mode: transaction
- Default pool size: 20
- Max client connections: 100

See `99_operational.sql` for full configuration.

## Files

```
database/
├── DATABASE_DESIGN.md          # Detailed architecture document
├── README.md                   # This file
├── install.sh                  # Installation script
└── sql/
    ├── 00_init.sql             # Extensions, settings
    ├── 01_schemas.sql          # Schema creation
    ├── 02_core_dimensions.sql  # Dimension tables
    ├── 03_audit_tables.sql     # Ingestion tracking
    ├── 04_bronze_wasde.sql     # WASDE bronze tables
    ├── 05_silver_observation.sql # Universal time-series
    ├── 06_gold_views.sql       # Business views
    ├── 07_roles_grants.sql     # Access control
    ├── 08_functions.sql        # Helper functions
    ├── 09_sample_dml.sql       # Usage examples
    └── 99_operational.sql      # Server config guidance
```

## License

Internal use only - Round Lakes Commodities
| `ingest_writer` | Write bronze/silver, read all | Collector agents |
| `checker` | Read bronze/silver, write validation | Validation agents |
| `analyst_reader` | Read silver/gold only | Report/trading agents |
| `rlc_admin` | Full access | Maintenance |

## Key Design Principles

1. **Idempotent Writes**: All inserts use `ON CONFLICT DO UPDATE` with natural keys
2. **Full Audit Trail**: Every observation traces back to `ingest_run_id`
3. **Layered Architecture**: Bronze→Silver→Gold separation of concerns
4. **Excel Compatibility**: Gold views match spreadsheet layouts
5. **Cloud-Ready**: Standard PostgreSQL, easy migration to RDS/Azure/GCP

## Example Queries

```sql
-- Get latest US corn balance sheet
SELECT * FROM gold.us_corn_balance_sheet;

-- Check WASDE changes from last report
SELECT * FROM gold.wasde_changes WHERE table_name LIKE '%Corn%';

-- Reconcile Census data against Excel totals
SELECT * FROM gold.reconcile_census_monthly_totals;

-- System health check
SELECT * FROM gold.system_health;
```

## Connection String

```
DATABASE_URL=postgresql://analyst_reader:password@localhost:5432/rlc_commodities
```

## Backup

Daily backups recommended:
```bash
pg_dump -Fc -f rlc_commodities_$(date +%Y%m%d).dump rlc_commodities
```

## Support

See `006_operational_guidance.sql` for monitoring queries, maintenance procedures, and cloud migration guidance.
