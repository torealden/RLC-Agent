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
