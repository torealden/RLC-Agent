# Round Lakes Commodities - PostgreSQL Database Architecture

## Executive Summary

This document defines a **production-ready, local-first PostgreSQL database** for Round Lakes Commodities (RLC). The design supports multi-agent concurrent access, idempotent data pipelines, full auditability, and a clear migration path to cloud infrastructure.

---

## 1. Architecture Overview

### 1.1 The Medallion (Bronze/Silver/Gold) Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              EXTERNAL SOURCES                                │
│   USDA WASDE │ USDA AMS │ Exchanges │ NOAA Weather │ EIA Energy │ Others    │
└───────┬──────┴────┬─────┴─────┬─────┴──────┬───────┴─────┬──────┴────┬──────┘
        │           │           │            │             │           │
        ▼           ▼           ▼            ▼             ▼           ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                           COLLECTOR AGENTS                                   │
│              (Write to Bronze + Silver, tracked via ingest_run)              │
└───────────────────────────────────┬─────────────────────────────────────────┘
                                    │
        ┌───────────────────────────┼───────────────────────────┐
        ▼                           ▼                           ▼
┌───────────────┐          ┌───────────────┐          ┌───────────────┐
│    BRONZE     │          │    SILVER     │          │     GOLD      │
│  (Raw/Source  │  ──────► │ (Standardized │  ──────► │   (Curated    │
│   Faithful)   │  ETL     │  Observations)│  Views   │    Marts)     │
└───────────────┘          └───────────────┘          └───────────────┘
        │                           │                           │
        │                           │                           │
        ▼                           ▼                           ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                        CHECKER AGENTS (Validation)                           │
│           Compare Bronze ↔ Silver ↔ Gold ↔ Excel Spreadsheets                │
└─────────────────────────────────────────────────────────────────────────────┘
        │
        ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                    ANALYST / TRADING / REPORT AGENTS                         │
│                        (Read-only access to Silver/Gold)                     │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 1.2 Layer Definitions

| Layer | Schema | Purpose | Data Characteristics |
|-------|--------|---------|---------------------|
| **Bronze** | `bronze` | Source-faithful storage | Raw text/JSON, original units, original structure. Never transformed. Immutable after initial load. |
| **Silver** | `silver` | Standardized observations | Cleaned, typed, normalized. Universal (series_id, time, value) format. Supports revisions. |
| **Gold** | `gold` | Business-ready views | Aggregated, pivoted, Excel-compatible. Derived from Silver. May be materialized. |
| **Core** | `core` | Dimension tables | Shared reference data: sources, series, units, locations. |
| **Audit** | `audit` | Ingestion tracking | Job runs, validation status, error logs. |

### 1.3 Why This Architecture?

1. **Auditability**: Bronze preserves source truth. If Silver disagrees with a spreadsheet, we can trace back to Bronze and the original API response.

2. **Idempotency**: Natural keys at each layer ensure reruns produce identical results without duplicates.

3. **Separation of Concerns**: Ingestion agents write Bronze+Silver. Analysts read Silver+Gold. No cross-contamination.

4. **Performance**: Silver.observation is the optimized query layer. Bronze is for forensics, not analytics.

5. **Flexibility**: New data sources add Bronze tables + series definitions. Silver.observation remains stable.

---

## 2. Schema Design Principles

### 2.1 Natural Keys vs Surrogate Keys

| Table Type | Key Strategy |
|------------|--------------|
| Dimension tables | Surrogate `id` (SERIAL/BIGSERIAL) + natural unique constraint |
| Bronze fact tables | Composite natural key (source identifiers) |
| Silver.observation | Composite key (series_id, observation_time, revision) |
| Audit tables | Surrogate `id` with timestamps |

### 2.2 Idempotent Upsert Pattern

All write operations use:
```sql
INSERT INTO table (...)
VALUES (...)
ON CONFLICT (natural_key_columns) DO UPDATE SET
    column1 = EXCLUDED.column1,
    updated_at = NOW()
WHERE table.column1 IS DISTINCT FROM EXCLUDED.column1;
```

The `WHERE ... IS DISTINCT FROM` clause prevents unnecessary writes and preserves `updated_at` accuracy.

### 2.3 Revision Handling

Time-series data often has revisions (e.g., WASDE revises previous months). We handle this with:
- `revision` column (integer, default 0)
- Natural key includes revision for true history
- OR `is_latest` boolean flag with trigger maintenance

### 2.4 JSONB Usage Policy

Use JSONB for:
- Raw API payloads (Bronze layer)
- Truly variable metadata (tags, custom attributes)
- Source-specific fields that don't warrant columns

Avoid JSONB for:
- Core queryable attributes (use columns)
- Frequently joined fields
- Fields needed for constraints

---

## 3. Agent Concurrency Model

### 3.1 Agent Types and Access Patterns

| Agent Type | Schemas | Access | Concurrency Pattern |
|------------|---------|--------|---------------------|
| **Collector** | bronze (RW), silver (RW), audit (RW), core (R) | Write-heavy | Sequential per source, parallel across sources |
| **Checker** | all (R), audit (RW) | Read + validate | Parallel reads, serialized validation writes |
| **Analyst/Report** | silver (R), gold (R), core (R) | Read-only | Highly parallel |
| **Trading** | silver (R), gold (R) | Read-only | Highly parallel, latency-sensitive |

### 3.2 Connection Management

For ~10 concurrent agents:
- Use **pgBouncer** in transaction pooling mode
- Pool size: 20 connections (allows headroom)
- Statement timeout: 30s for reads, 5min for ingestion
- Idle timeout: 5min

### 3.3 Locking Strategy

- Bronze writes: Row-level locks via upsert (no explicit locking)
- Silver writes: Row-level locks, concurrent writes to different series OK
- Gold refresh: Use `CONCURRENTLY` for materialized views
- Validation: Advisory locks for release-level validation

---

## 4. Validation and Quality Control

### 4.1 Validation Lifecycle

```
┌──────────┐     ┌──────────┐     ┌───────────┐     ┌──────────┐
│ INGESTED │ ──► │ PENDING  │ ──► │ VALIDATED │ ──► │PUBLISHED │
│          │     │ REVIEW   │     │           │     │ TO GOLD  │
└──────────┘     └──────────┘     └───────────┘     └──────────┘
      │                                   │
      ▼                                   ▼
┌──────────┐                       ┌───────────┐
│  FAILED  │                       │  STALE    │
│  (Error) │                       │(Superseded│
└──────────┘                       └───────────┘
```

### 4.2 Validation Status Table

The `audit.validation_status` table tracks:
- Which release/ingest_run has been validated
- Who validated it (agent or human)
- Validation timestamp
- Any discrepancy notes

### 4.3 Excel Reconciliation

Gold views are designed to match Excel layouts:
- Same column order as spreadsheet tabs
- Same row groupings
- Totals/subtotals at same positions
- Reconciliation queries compare row counts and sums

---

## 5. Series Governance

### 5.1 Preventing Duplicate Series

The `core.series` table enforces uniqueness via:
```sql
UNIQUE (data_source_id, series_key)
```

A helper function `core.get_or_create_series()` provides atomic lookup-or-insert.

### 5.2 Series Naming Convention

```
{source}.{category}.{commodity}.{metric}.{geography}.{frequency}
```

Examples:
- `wasde.supply_demand.corn.ending_stocks.us.monthly`
- `eia.petroleum.crude.price.wti.daily`
- `noaa.weather.temperature.avg.ia.daily`

### 5.3 Series Metadata

Required fields:
- `data_source_id`: FK to data source
- `series_key`: Unique within source
- `name`: Human-readable
- `unit_id`: FK to unit
- `frequency`: daily, weekly, monthly, quarterly, annual
- `commodity`: nullable, for commodity-specific series

Optional (JSONB `metadata`):
- `seasonal_adjustment`
- `revision_schedule`
- `source_url`
- `notes`

---

## 6. Unit and Location Handling

### 6.1 Unit Dimension

The `core.unit` table stores:
- `code`: Standard abbreviation (bu, mt, bbl, usd)
- `name`: Full name
- `unit_type`: mass, volume, currency, count, ratio, other
- `base_unit_id`: For conversion (nullable)
- `conversion_factor`: Multiplier to base unit

Example conversions:
- 1 metric ton = 1000 kg (base for mass)
- 1 bushel corn = 25.4 kg
- 1 barrel = 42 gallons

### 6.2 Location Dimension

Flexible hierarchy supporting:
- Countries (US, BR, AR)
- States/provinces (IA, IL, MT Mato Grosso)
- Regions (Corn Belt, Gulf Coast)
- Points (lat/long for weather stations)

```sql
CREATE TABLE core.location (
    id SERIAL PRIMARY KEY,
    code VARCHAR(50) NOT NULL UNIQUE,
    name VARCHAR(255) NOT NULL,
    location_type VARCHAR(50) NOT NULL,  -- country, state, region, point
    parent_id INTEGER REFERENCES core.location(id),
    iso_country VARCHAR(3),
    iso_subdivision VARCHAR(6),
    latitude DECIMAL(9,6),
    longitude DECIMAL(9,6),
    geometry GEOMETRY(Geometry, 4326),  -- Optional PostGIS
    metadata JSONB DEFAULT '{}'
);
```

---

## 7. Partitioning Strategy

### 7.1 When to Partition

Partition `silver.observation` when:
- Row count exceeds 100 million
- Query patterns consistently filter by time range
- Maintenance windows are tight

### 7.2 Recommended Partitioning

```sql
-- Range partitioning by observation_time (yearly)
CREATE TABLE silver.observation (
    ...
) PARTITION BY RANGE (observation_time);

CREATE TABLE silver.observation_y2020
    PARTITION OF silver.observation
    FOR VALUES FROM ('2020-01-01') TO ('2021-01-01');
```

### 7.3 Partition Maintenance

- Create partitions 1 year ahead
- Archive old partitions to cold storage
- Use pg_partman for automation

---

## 8. Backup and Recovery

### 8.1 Backup Strategy

| Backup Type | Frequency | Retention | Tool |
|-------------|-----------|-----------|------|
| Base backup | Daily 2 AM | 7 days | pg_basebackup |
| WAL archive | Continuous | 7 days | archive_command |
| Logical dump | Weekly | 30 days | pg_dump |

### 8.2 Point-in-Time Recovery

With WAL archiving enabled:
1. Restore base backup
2. Replay WAL to target time
3. Recover to any point in last 7 days

### 8.3 Cloud Migration Path

Local setup mirrors cloud-ready patterns:
- Use `pg_dump` for initial migration
- Enable logical replication for near-zero-downtime cutover
- Same schema works on RDS/Cloud SQL/Aurora

---

## 9. Monitoring Checklist

### 9.1 Key Metrics

| Metric | Warning | Critical |
|--------|---------|----------|
| Connection count | >15 | >18 |
| Disk usage | >70% | >85% |
| WAL lag | >100MB | >500MB |
| Longest query | >60s | >300s |
| Dead tuples ratio | >10% | >20% |

### 9.2 Monitoring Tools

- **pg_stat_statements**: Query performance
- **pg_stat_user_tables**: Table statistics
- **pg_stat_activity**: Active connections
- **pgBadger**: Log analysis
- **Prometheus + postgres_exporter**: Metrics collection

---

## 10. File Organization

```
database/
├── DATABASE_DESIGN.md          # This document
├── sql/
│   ├── 00_init.sql             # Extensions, settings
│   ├── 01_schemas.sql          # Schema creation
│   ├── 02_core_dimensions.sql  # Dimension tables
│   ├── 03_audit_tables.sql     # Ingestion tracking
│   ├── 04_bronze_wasde.sql     # WASDE bronze tables
│   ├── 05_silver_observation.sql # Universal time-series
│   ├── 06_gold_views.sql       # Business views
│   ├── 07_roles_grants.sql     # Access control
│   ├── 08_functions.sql        # Helper functions
│   ├── 09_sample_dml.sql       # Usage examples
│   └── 99_operational.sql      # Server config guidance
└── migrations/                  # Future migrations
    └── README.md
```

---

## Appendix A: Quick Reference

### A.1 Key Tables

| Table | Purpose | Natural Key |
|-------|---------|-------------|
| `core.data_source` | API/feed definitions | `code` |
| `core.series` | Time-series metadata | `(data_source_id, series_key)` |
| `core.unit` | Measurement units | `code` |
| `core.location` | Geographic entities | `code` |
| `audit.ingest_run` | Job execution log | `id` (surrogate) |
| `bronze.wasde_release` | WASDE report metadata | `report_date` |
| `bronze.wasde_cell` | Raw WASDE values | `(release_id, table_id, row_id, column_id)` |
| `silver.observation` | Standardized measurements | `(series_id, observation_time, revision)` |

### A.2 Common Queries

```sql
-- Latest value for a series
SELECT value FROM silver.observation
WHERE series_id = ? AND is_latest = TRUE
ORDER BY observation_time DESC LIMIT 1;

-- All revisions for a data point
SELECT * FROM silver.observation
WHERE series_id = ? AND observation_time = ?
ORDER BY revision DESC;

-- Compare to previous release
SELECT * FROM gold.wasde_corn_changes;
```

---

*Document Version: 1.0*
*Last Updated: 2024*
*Author: Database Architecture Team*
