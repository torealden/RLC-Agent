# RLC Commodities Database Schema

Production-ready PostgreSQL database for Round Lakes Commodities data platform.

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────┐
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
