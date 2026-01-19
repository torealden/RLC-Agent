# RLC-Agent Database Logging Agent Design

## Executive Summary

This document outlines the design for a comprehensive logging system to track agent interactions with data across the Bronze/Silver/Gold medallion architecture. The system implements a "checkout/checkin" pattern for data sessions and captures transformation lineage without storing individual data values.

## Current State Analysis

### Existing Audit Infrastructure

The database already has foundational audit capabilities:

1. **`audit.ingest_run`** - Tracks data collection jobs
   - Records: job type, agent ID, timing, record counts, errors
   - Good for: External data ingestion

2. **`audit.validation_status`** - Tracks checker agent approvals
   - Records: validation type, status, issues
   - Good for: Data quality gates

3. **Basic Lineage** in `silver.observation`:
   - `ingest_run_id` - Links to audit
   - `source_table` - Bronze table name
   - `source_id` - ID in bronze table

### Current Gaps

| Gap | Impact | Priority |
|-----|--------|----------|
| No session tracking for transformation work | Cannot track "who touched what when" | HIGH |
| No transformation formula logging | Cannot reproduce calculations | HIGH |
| No output/visualization tracking | Cannot trace dashboard sources | MEDIUM |
| No inter-table lineage | Cannot trace data flow across Silver tables | MEDIUM |
| No rollback capability | Cannot undo transformations | LOW |

---

## Design Philosophy

### Key Principles

1. **Log Metadata, Not Data** - Never store transformed data values in logs; store only formulas, references, and operations
2. **Bronze is Truth** - Original data is always recoverable from Bronze layer
3. **Right-Sized Granularity** - Log at session/operation level, not row-by-row
4. **Query-Efficient** - Optimize for "what happened to this data?" queries
5. **Retention-Aware** - Include TTL and archival strategy from day one

### Storage Budget Target

Based on commercial best practices:
- Target: < 1% of Bronze layer size
- Example: If Bronze = 10GB, logs should be < 100MB
- Achieved by: Logging operations not values, sampling, and aggregation

---

## Schema Design

### New Audit Tables

```
audit/
├── transformation_session    # "Checkout" - agent starts working with data
├── transformation_operation  # What operations were performed
├── output_artifact          # What outputs were created
└── lineage_edge             # Relationships between data entities
```

### Table Specifications

#### 1. `audit.transformation_session` (The "Checkout" Table)

Records when an agent "checks out" data for transformation work.

```sql
CREATE TABLE audit.transformation_session (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),

    -- Session identification
    session_type VARCHAR(50) NOT NULL,     -- 'BRONZE_TO_SILVER', 'SILVER_AGGREGATE',
                                           -- 'GOLD_VISUALIZATION', 'AD_HOC_ANALYSIS'

    -- Agent information
    agent_id VARCHAR(100) NOT NULL,        -- Which agent
    agent_type VARCHAR(50),                -- 'COLLECTOR', 'TRANSFORMER', 'ANALYST', 'VISUALIZATION'
    agent_version VARCHAR(50),

    -- What data is being accessed
    source_layer VARCHAR(10) NOT NULL,     -- 'BRONZE', 'SILVER', 'GOLD'
    source_tables TEXT[] NOT NULL,         -- Array of table names accessed
    source_filters JSONB,                  -- Any WHERE clauses/filters applied

    -- Time range of source data (not transformation time)
    data_start_date DATE,
    data_end_date DATE,

    -- Session lifecycle
    status VARCHAR(20) NOT NULL DEFAULT 'ACTIVE',  -- 'ACTIVE', 'COMPLETED', 'FAILED', 'ABANDONED'
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMPTZ,

    -- Context
    purpose TEXT,                          -- Human-readable description
    ticket_id VARCHAR(50),                 -- Optional: link to Jira/issue
    parent_session_id UUID,                -- For chained transformations

    -- Summary (populated on completion)
    operations_count INT,
    outputs_count INT,

    FOREIGN KEY (parent_session_id) REFERENCES audit.transformation_session(id)
);
```

#### 2. `audit.transformation_operation` (The "What Happened" Table)

Records individual operations within a session.

```sql
CREATE TABLE audit.transformation_operation (
    id BIGSERIAL PRIMARY KEY,
    session_id UUID NOT NULL REFERENCES audit.transformation_session(id),

    -- Operation details
    operation_type VARCHAR(50) NOT NULL,   -- 'SELECT', 'AGGREGATE', 'JOIN', 'FILTER',
                                           -- 'CALCULATE', 'PIVOT', 'NORMALIZE', 'CLEAN'
    operation_order INT NOT NULL,          -- Sequence within session

    -- What was the input
    input_tables TEXT[],                   -- Tables/views used
    input_columns TEXT[],                  -- Columns accessed (optional, can be NULL for simplicity)
    input_row_count BIGINT,                -- Approximate row count processed

    -- What was the transformation
    transformation_logic TEXT,             -- SQL, formula, or description
    transformation_type VARCHAR(30),       -- 'SQL', 'PYTHON', 'FORMULA', 'MANUAL'

    -- Parameters used (not the data, just the params)
    parameters JSONB,                      -- e.g., {"aggregation": "SUM", "group_by": ["commodity_code"]}

    -- What was the output
    output_table VARCHAR(200),             -- Target table/view
    output_columns TEXT[],
    output_row_count BIGINT,

    -- Quality
    warnings TEXT[],                       -- Any issues encountered
    execution_time_ms INT,

    created_at TIMESTAMPTZ DEFAULT NOW()
);
```

#### 3. `audit.output_artifact` (The "What Was Created" Table)

Records outputs created from transformations (tables, views, charts, exports).

```sql
CREATE TABLE audit.output_artifact (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    session_id UUID NOT NULL REFERENCES audit.transformation_session(id),

    -- Artifact identification
    artifact_type VARCHAR(50) NOT NULL,    -- 'TABLE', 'VIEW', 'MATERIALIZED_VIEW',
                                           -- 'CHART', 'EXPORT', 'REPORT', 'DATAFRAME'
    artifact_name VARCHAR(300) NOT NULL,
    artifact_location VARCHAR(500),        -- Schema.table, file path, or URL

    -- What data does it contain
    source_tables TEXT[],                  -- Which tables feed this artifact
    columns TEXT[],                        -- What columns are included
    row_count BIGINT,

    -- Temporal scope
    data_as_of TIMESTAMPTZ,                -- Point-in-time for the data
    data_start_date DATE,
    data_end_date DATE,

    -- Lifecycle
    is_current BOOLEAN DEFAULT TRUE,
    superseded_by_id UUID,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    expires_at TIMESTAMPTZ,                -- Optional TTL

    -- Metadata
    description TEXT,
    metadata JSONB,

    FOREIGN KEY (superseded_by_id) REFERENCES audit.output_artifact(id)
);
```

#### 4. `audit.lineage_edge` (The "Data Flow" Table)

Records relationships between data entities for lineage tracking.

```sql
CREATE TABLE audit.lineage_edge (
    id BIGSERIAL PRIMARY KEY,

    -- Source node
    source_type VARCHAR(30) NOT NULL,      -- 'TABLE', 'VIEW', 'COLUMN', 'ARTIFACT'
    source_schema VARCHAR(50),
    source_name VARCHAR(200) NOT NULL,
    source_column VARCHAR(100),            -- Optional, for column-level lineage

    -- Target node
    target_type VARCHAR(30) NOT NULL,
    target_schema VARCHAR(50),
    target_name VARCHAR(200) NOT NULL,
    target_column VARCHAR(100),

    -- Relationship
    relationship_type VARCHAR(30) NOT NULL,  -- 'DERIVES_FROM', 'AGGREGATES', 'JOINS',
                                             -- 'FILTERS', 'TRANSFORMS', 'COPIES'

    -- Context
    session_id UUID REFERENCES audit.transformation_session(id),
    transformation_description TEXT,

    -- Validity
    is_current BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    invalidated_at TIMESTAMPTZ,

    -- Unique constraint to prevent duplicate edges
    UNIQUE (source_type, source_schema, source_name, source_column,
            target_type, target_schema, target_name, target_column,
            relationship_type)
);
```

---

## Python Logging Agent

### Core Class Design

```python
class TransformationLogger:
    """
    Logging agent for tracking data transformations.

    Usage:
        with TransformationLogger(agent_id='my_agent', purpose='Monthly ETL') as logger:
            logger.checkout(['bronze.wasde_cell', 'bronze.wasde_release'])

            # Perform transformation
            logger.log_operation(
                operation_type='AGGREGATE',
                input_tables=['bronze.wasde_cell'],
                transformation_logic='SELECT commodity, SUM(value) FROM wasde_cell GROUP BY commodity',
                output_table='silver.commodity_summary'
            )

            # Register output
            logger.register_output(
                artifact_type='TABLE',
                artifact_name='silver.commodity_summary',
                row_count=150
            )
    """
```

### Key Methods

1. **`checkout(tables, filters=None)`** - Start a session, register source tables
2. **`log_operation(operation_type, ...)`** - Record a transformation step
3. **`register_output(artifact_type, ...)`** - Record an output artifact
4. **`checkin(status='COMPLETED')`** - End the session
5. **`add_lineage(source, target, relationship)`** - Record data flow

---

## Storage Optimization Strategy

### 1. Aggregation at Write Time

Don't log every row operation. Instead:
- Log batch operations: "Processed 50,000 rows from bronze.wasde_cell"
- Store formula/logic, not individual results
- Use approximate row counts where exact isn't needed

### 2. Tiered Retention

```sql
-- Hot tier: Recent sessions (30 days)
-- Keep full detail for troubleshooting

-- Warm tier: 30-365 days
-- Aggregate: Combine operations within sessions
CREATE TABLE audit.transformation_session_archive AS
SELECT
    id, session_type, agent_id, agent_type,
    source_layer, source_tables,
    started_at, completed_at, status,
    operations_count, outputs_count,
    purpose
FROM audit.transformation_session
WHERE completed_at < NOW() - INTERVAL '30 days';

-- Cold tier: > 1 year
-- Summary only: Just session counts per agent/type
-- DELETE operations and artifacts, keep session summary
```

### 3. Sampling for High-Volume Operations

For repetitive operations (e.g., daily price updates):
- Log first occurrence with full detail
- Log subsequent occurrences with just timestamps and counts
- Store "pattern" reference to first occurrence

### 4. Partitioning

```sql
-- Partition by month for easier archival
CREATE TABLE audit.transformation_operation (
    ...
) PARTITION BY RANGE (created_at);

CREATE TABLE audit.transformation_operation_2026_01
    PARTITION OF audit.transformation_operation
    FOR VALUES FROM ('2026-01-01') TO ('2026-02-01');
```

---

## Query Patterns for Troubleshooting

### "What happened to this table's data?"

```sql
SELECT
    ts.started_at,
    ts.agent_id,
    ts.purpose,
    to.operation_type,
    to.transformation_logic,
    to.input_row_count,
    to.output_row_count
FROM audit.transformation_session ts
JOIN audit.transformation_operation to ON ts.id = to.session_id
WHERE 'silver.observation' = ANY(ts.source_tables)
   OR to.output_table = 'silver.observation'
ORDER BY ts.started_at DESC;
```

### "What are the sources for this Gold view?"

```sql
WITH RECURSIVE lineage AS (
    SELECT source_schema, source_name, target_schema, target_name, 1 as depth
    FROM audit.lineage_edge
    WHERE target_schema = 'gold' AND target_name = 'us_corn_balance_sheet'

    UNION ALL

    SELECT le.source_schema, le.source_name, le.target_schema, le.target_name, l.depth + 1
    FROM audit.lineage_edge le
    JOIN lineage l ON le.target_schema = l.source_schema AND le.target_name = l.source_name
    WHERE l.depth < 5
)
SELECT DISTINCT source_schema || '.' || source_name as source
FROM lineage
ORDER BY source;
```

### "What did agent X do last week?"

```sql
SELECT
    ts.session_type,
    ts.source_tables,
    ts.status,
    ts.started_at,
    ts.operations_count,
    ts.outputs_count
FROM audit.transformation_session ts
WHERE ts.agent_id = 'wasde_transformer'
  AND ts.started_at >= NOW() - INTERVAL '7 days'
ORDER BY ts.started_at DESC;
```

---

## Implementation Phases

### Phase 1: Schema Foundation (Immediate)
- [ ] Create `audit.transformation_session` table
- [ ] Create `audit.transformation_operation` table
- [ ] Create `audit.output_artifact` table
- [ ] Create `audit.lineage_edge` table
- [ ] Create indexes for common query patterns

### Phase 2: Python Logging Agent (Week 1)
- [ ] Implement `TransformationLogger` class
- [ ] Add context manager support for automatic checkin
- [ ] Add helper functions for common operations
- [ ] Integrate with existing `BaseCollector` class

### Phase 3: Retrofit Existing Agents (Week 2)
- [ ] Add logging to Bronze→Silver transformations
- [ ] Add logging to Silver aggregate functions
- [ ] Add logging to Gold view refreshes
- [ ] Document lineage for all existing tables

### Phase 4: Monitoring & Dashboards (Week 3)
- [ ] Create `gold.transformation_activity` view
- [ ] Add logging volume monitoring
- [ ] Implement archival job
- [ ] Create troubleshooting queries

---

## Estimated Storage Impact

| Component | Records/Day | Bytes/Record | Daily Size | Yearly Size |
|-----------|-------------|--------------|------------|-------------|
| transformation_session | 50 | 500 | 25 KB | 9 MB |
| transformation_operation | 500 | 300 | 150 KB | 55 MB |
| output_artifact | 100 | 400 | 40 KB | 15 MB |
| lineage_edge | 200 | 200 | 40 KB | 15 MB |
| **Total** | | | **255 KB** | **94 MB** |

With archival after 30 days: ~8 MB active storage.

---

## Integration with Existing System

### Relationship to `audit.ingest_run`

```
audit.ingest_run (existing)
    │
    │  External data collection
    ▼
bronze.* tables
    │
    │  transformation_session (new)
    ▼
silver.* tables ─────────────────► output_artifact (new)
    │                                     │
    │  transformation_session (new)       │
    ▼                                     ▼
gold.* views ────────────────────► output_artifact (new)
```

`ingest_run` continues to track data **entering** the system.
`transformation_session` tracks data **moving between layers**.

---

## Appendix: Best Practices Applied

Sources consulted:

1. **Medallion Architecture** - Databricks, Microsoft
   - Bronze preserves raw data for audit/reprocessing
   - Clear separation enables targeted logging

2. **Data Lineage** - OpenLineage standard
   - Track processes, runs, and datasets
   - Use facets for extensible metadata

3. **ETL Auditing** - Enterprise patterns
   - Log at operation level, not row level
   - Include timing for performance analysis
   - Store transformation logic for reproducibility

4. **Storage Optimization** - Google Cloud, AWS best practices
   - Tiered retention (hot/warm/cold)
   - Partition by time
   - Aggregate historical data

5. **Data Observability** - Monte Carlo, Datadog
   - Focus on troubleshooting workflows
   - Optimize for "what went wrong" queries
   - Include quality signals
