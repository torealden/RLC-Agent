-- ============================================================================
-- Round Lakes Commodities - Bronze Layer: WASDE Tables
-- ============================================================================
-- File: 04_bronze_wasde.sql
-- Purpose: Source-faithful storage for USDA WASDE report data
-- Execute: After 03_audit_tables.sql
-- ============================================================================
-- WASDE (World Agricultural Supply and Demand Estimates) is a monthly report
-- from USDA that contains supply/demand balance sheets for major commodities.
-- Each release contains multiple tables with projected values.
-- ============================================================================

-- ============================================================================
-- WASDE_RELEASE: One row per monthly WASDE report
-- ============================================================================
CREATE TABLE IF NOT EXISTS bronze.wasde_release (
    id                  SERIAL PRIMARY KEY,

    -- Natural key: the report date (WASDE is monthly)
    report_date         DATE NOT NULL,          -- First day of report month

    -- Release metadata from USDA
    release_number      INTEGER,                -- USDA's sequential number (e.g., 653)
    release_title       VARCHAR(500),           -- Official title
    release_timestamp   TIMESTAMPTZ,            -- Exact release time (noon ET typically)
    report_month_year   VARCHAR(50),            -- "January 2024" as published

    -- Source tracking
    source_url          VARCHAR(2000),
    source_format       VARCHAR(50),            -- pdf, wasde_api, excel, xml
    source_checksum     VARCHAR(128),

    -- Ingestion tracking
    ingest_run_id       BIGINT REFERENCES audit.ingest_run(id),

    -- Raw data storage
    raw_json            JSONB,                  -- Full API response if available
    raw_xml             TEXT,                   -- XML if that's the source format

    -- Status flags
    is_complete         BOOLEAN DEFAULT FALSE,  -- All tables ingested?
    is_validated        BOOLEAN DEFAULT FALSE,  -- Passed validation checks?

    -- Timestamps
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    -- Natural key constraint
    CONSTRAINT wasde_release_date_unique UNIQUE (report_date)
);

COMMENT ON TABLE bronze.wasde_release IS
    'One row per WASDE monthly release. Stores release metadata and optionally '
    'the full raw API response. Natural key is report_date.';

COMMENT ON COLUMN bronze.wasde_release.report_date IS
    'First day of the report month (e.g., 2024-01-01 for January 2024 WASDE)';
COMMENT ON COLUMN bronze.wasde_release.release_timestamp IS
    'Actual release time if available (typically noon ET on release day)';
COMMENT ON COLUMN bronze.wasde_release.release_number IS
    'USDA sequential WASDE number (e.g., 653)';

-- Indexes
CREATE INDEX IF NOT EXISTS idx_wasde_release_date
    ON bronze.wasde_release(report_date DESC);
CREATE INDEX IF NOT EXISTS idx_wasde_release_ingest
    ON bronze.wasde_release(ingest_run_id);


-- ============================================================================
-- WASDE_TABLE_DEF: Defines the structure of each WASDE table
-- ============================================================================
CREATE TABLE IF NOT EXISTS bronze.wasde_table_def (
    id              SERIAL PRIMARY KEY,
    table_number    VARCHAR(20) NOT NULL,       -- e.g., "01", "01A", "02"
    table_title     VARCHAR(500) NOT NULL,      -- "U.S. Wheat Supply and Use"
    commodity_code  VARCHAR(50),                -- corn, wheat, soybeans, etc.
    region_code     VARCHAR(50),                -- us, world, etc.
    category        VARCHAR(100),               -- supply_demand, trade, stocks
    description     TEXT,
    column_headers  JSONB,                      -- Array of column header info
    metadata        JSONB DEFAULT '{}',
    is_active       BOOLEAN DEFAULT TRUE,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT wasde_table_def_number_unique UNIQUE (table_number)
);

COMMENT ON TABLE bronze.wasde_table_def IS
    'Reference table defining WASDE table structures. Maps table numbers to '
    'commodities and provides column header metadata.';

-- Seed common WASDE tables
INSERT INTO bronze.wasde_table_def (table_number, table_title, commodity_code, region_code, category)
VALUES
    ('01', 'U.S. Wheat Supply and Use', 'wheat', 'us', 'supply_demand'),
    ('02', 'U.S. Wheat Supply and Use (Cont.)', 'wheat', 'us', 'supply_demand'),
    ('03', 'U.S. Coarse Grains Supply and Use', 'coarse_grains', 'us', 'supply_demand'),
    ('04', 'U.S. Corn Supply and Use', 'corn', 'us', 'supply_demand'),
    ('05', 'U.S. Sorghum Supply and Use', 'sorghum', 'us', 'supply_demand'),
    ('06', 'U.S. Barley Supply and Use', 'barley', 'us', 'supply_demand'),
    ('07', 'U.S. Oats Supply and Use', 'oats', 'us', 'supply_demand'),
    ('08', 'U.S. Rice Supply and Use', 'rice', 'us', 'supply_demand'),
    ('09', 'U.S. Oilseed Supply and Use', 'oilseeds', 'us', 'supply_demand'),
    ('10', 'U.S. Soybeans and Products Supply and Use', 'soybeans', 'us', 'supply_demand'),
    ('11', 'World Wheat Supply and Use', 'wheat', 'world', 'supply_demand'),
    ('12', 'World Coarse Grains Supply and Use', 'coarse_grains', 'world', 'supply_demand'),
    ('13', 'World Corn Supply and Use', 'corn', 'world', 'supply_demand'),
    ('14', 'World Rice Supply and Use', 'rice', 'world', 'supply_demand'),
    ('15', 'World Oilseed Supply and Use', 'oilseeds', 'world', 'supply_demand'),
    ('16', 'World Soybean Supply and Use', 'soybeans', 'world', 'supply_demand'),
    ('17', 'U.S. Sugar Supply and Use', 'sugar', 'us', 'supply_demand'),
    ('18', 'U.S. Livestock, Dairy, and Poultry', 'livestock', 'us', 'supply_demand'),
    ('19', 'U.S. Cotton Supply and Use', 'cotton', 'us', 'supply_demand')
ON CONFLICT (table_number) DO NOTHING;


-- ============================================================================
-- WASDE_CELL: One row per published value in WASDE tables
-- ============================================================================
CREATE TABLE IF NOT EXISTS bronze.wasde_cell (
    id                  BIGSERIAL PRIMARY KEY,

    -- Foreign keys
    release_id          INTEGER NOT NULL REFERENCES bronze.wasde_release(id),
    table_def_id        INTEGER REFERENCES bronze.wasde_table_def(id),

    -- Natural key components (source-faithful identifiers)
    table_id            VARCHAR(20) NOT NULL,   -- Source table number
    row_id              VARCHAR(100) NOT NULL,  -- Row identifier from source
    column_id           VARCHAR(100) NOT NULL,  -- Column identifier (marketing year)

    -- Row context
    row_label           VARCHAR(500),           -- Full row text label
    row_indent_level    SMALLINT DEFAULT 0,     -- Nesting level (0=top, 1=sub, etc.)
    row_category        VARCHAR(100),           -- supply, demand, stocks, etc.
    row_order           INTEGER,                -- Order within table

    -- Column context
    column_label        VARCHAR(255),           -- Full column header
    marketing_year      VARCHAR(20),            -- e.g., "2023/24"
    projection_type     VARCHAR(50),            -- est, proj, actual

    -- Value storage: preserve original AND parsed
    value_text          TEXT,                   -- Exact text from source
    value_numeric       DECIMAL(20,4),          -- Parsed numeric (NULL if non-numeric)
    value_unit_text     VARCHAR(50),            -- Unit as it appears in source

    -- Quality/parsing flags
    is_numeric          BOOLEAN,
    is_footnoted        BOOLEAN DEFAULT FALSE,
    footnote_text       VARCHAR(500),
    parse_warning       VARCHAR(500),           -- Any parsing issues

    -- Ingestion tracking
    ingest_run_id       BIGINT REFERENCES audit.ingest_run(id),

    -- Timestamps
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    -- Natural key: unique cell within a release
    CONSTRAINT wasde_cell_natural_key UNIQUE (release_id, table_id, row_id, column_id)
);

COMMENT ON TABLE bronze.wasde_cell IS
    'One row per published value in WASDE tables. Stores exact source text AND '
    'parsed numeric value. Natural key ensures idempotent upserts.';

COMMENT ON COLUMN bronze.wasde_cell.value_text IS
    'Exact text value from source (e.g., "1,850" or "NA" or "-")';
COMMENT ON COLUMN bronze.wasde_cell.value_numeric IS
    'Parsed numeric value (NULL if text is non-numeric like "NA")';
COMMENT ON COLUMN bronze.wasde_cell.marketing_year IS
    'Marketing year from column header (e.g., "2023/24", "2024/25 Est.")';
COMMENT ON COLUMN bronze.wasde_cell.row_category IS
    'Logical category: area_planted, area_harvested, yield, beginning_stocks, '
    'production, imports, supply_total, feed_residual, food_seed_industrial, '
    'ethanol, exports, use_total, ending_stocks, avg_farm_price';

-- Indexes for common query patterns
CREATE INDEX IF NOT EXISTS idx_wasde_cell_release
    ON bronze.wasde_cell(release_id);
CREATE INDEX IF NOT EXISTS idx_wasde_cell_table
    ON bronze.wasde_cell(release_id, table_id);
CREATE INDEX IF NOT EXISTS idx_wasde_cell_row_category
    ON bronze.wasde_cell(row_category);
CREATE INDEX IF NOT EXISTS idx_wasde_cell_my
    ON bronze.wasde_cell(marketing_year);
CREATE INDEX IF NOT EXISTS idx_wasde_cell_ingest
    ON bronze.wasde_cell(ingest_run_id);


-- ============================================================================
-- WASDE_REVISION: Track revisions between releases
-- ============================================================================
CREATE TABLE IF NOT EXISTS bronze.wasde_revision (
    id                  BIGSERIAL PRIMARY KEY,

    -- Reference to the current and previous cells
    current_cell_id     BIGINT NOT NULL REFERENCES bronze.wasde_cell(id),
    previous_cell_id    BIGINT REFERENCES bronze.wasde_cell(id),

    -- Quick access fields (denormalized for query performance)
    release_id          INTEGER NOT NULL REFERENCES bronze.wasde_release(id),
    previous_release_id INTEGER REFERENCES bronze.wasde_release(id),
    table_id            VARCHAR(20) NOT NULL,
    row_id              VARCHAR(100) NOT NULL,
    column_id           VARCHAR(100) NOT NULL,

    -- Revision values
    previous_value      DECIMAL(20,4),
    current_value       DECIMAL(20,4),
    change_value        DECIMAL(20,4),
    change_percent      DECIMAL(10,4),

    -- Timestamps
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT wasde_revision_unique UNIQUE (current_cell_id)
);

COMMENT ON TABLE bronze.wasde_revision IS
    'Pre-calculated revisions between consecutive WASDE releases. Links current '
    'cell to its previous version and stores the change.';

-- Indexes
CREATE INDEX IF NOT EXISTS idx_wasde_revision_release
    ON bronze.wasde_revision(release_id);
CREATE INDEX IF NOT EXISTS idx_wasde_revision_change
    ON bronze.wasde_revision(ABS(change_percent) DESC NULLS LAST);


-- ============================================================================
-- WASDE_ROW_MAPPING: Map row labels to standard metrics
-- ============================================================================
CREATE TABLE IF NOT EXISTS bronze.wasde_row_mapping (
    id              SERIAL PRIMARY KEY,
    table_id        VARCHAR(20) NOT NULL,
    row_label_pattern VARCHAR(500) NOT NULL,   -- Regex or exact match
    row_category    VARCHAR(100) NOT NULL,     -- Standard category
    metric_code     VARCHAR(100) NOT NULL,     -- Standard metric code
    metric_name     VARCHAR(255) NOT NULL,
    sort_order      INTEGER,
    metadata        JSONB DEFAULT '{}',
    is_active       BOOLEAN DEFAULT TRUE,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT wasde_row_mapping_unique UNIQUE (table_id, row_label_pattern)
);

COMMENT ON TABLE bronze.wasde_row_mapping IS
    'Maps WASDE row labels to standardized metric codes. Used for ETL to Silver.';

-- Seed key row mappings for US Corn (Table 04)
INSERT INTO bronze.wasde_row_mapping (table_id, row_label_pattern, row_category, metric_code, metric_name, sort_order)
VALUES
    ('04', 'Area Planted', 'area', 'area_planted', 'Area Planted', 1),
    ('04', 'Area Harvested', 'area', 'area_harvested', 'Area Harvested', 2),
    ('04', 'Yield per Harvested Acre', 'yield', 'yield', 'Yield per Harvested Acre', 3),
    ('04', 'Beginning Stocks', 'supply', 'beginning_stocks', 'Beginning Stocks', 4),
    ('04', 'Production', 'supply', 'production', 'Production', 5),
    ('04', 'Imports', 'supply', 'imports', 'Imports', 6),
    ('04', 'Supply, Total', 'supply', 'supply_total', 'Total Supply', 7),
    ('04', 'Feed and Residual', 'demand', 'feed_residual', 'Feed and Residual', 8),
    ('04', 'Food, Seed, and Industrial', 'demand', 'fsi_total', 'Food, Seed, and Industrial', 9),
    ('04', 'Ethanol and by-products', 'demand', 'ethanol', 'Ethanol and By-products', 10),
    ('04', 'Exports', 'demand', 'exports', 'Exports', 11),
    ('04', 'Use, Total', 'demand', 'use_total', 'Total Use', 12),
    ('04', 'Ending Stocks', 'stocks', 'ending_stocks', 'Ending Stocks', 13),
    ('04', 'Avg. Farm Price', 'price', 'avg_farm_price', 'Average Farm Price', 14)
ON CONFLICT (table_id, row_label_pattern) DO NOTHING;

-- Seed key row mappings for US Soybeans (Table 10)
INSERT INTO bronze.wasde_row_mapping (table_id, row_label_pattern, row_category, metric_code, metric_name, sort_order)
VALUES
    ('10', 'Area Planted', 'area', 'area_planted', 'Area Planted', 1),
    ('10', 'Area Harvested', 'area', 'area_harvested', 'Area Harvested', 2),
    ('10', 'Yield per Harvested Acre', 'yield', 'yield', 'Yield per Harvested Acre', 3),
    ('10', 'Beginning Stocks', 'supply', 'beginning_stocks', 'Beginning Stocks', 4),
    ('10', 'Production', 'supply', 'production', 'Production', 5),
    ('10', 'Imports', 'supply', 'imports', 'Imports', 6),
    ('10', 'Supply, Total', 'supply', 'supply_total', 'Total Supply', 7),
    ('10', 'Crushings', 'demand', 'crushings', 'Crushings', 8),
    ('10', 'Exports', 'demand', 'exports', 'Exports', 9),
    ('10', 'Seed', 'demand', 'seed', 'Seed', 10),
    ('10', 'Residual', 'demand', 'residual', 'Residual', 11),
    ('10', 'Use, Total', 'demand', 'use_total', 'Total Use', 12),
    ('10', 'Ending Stocks', 'stocks', 'ending_stocks', 'Ending Stocks', 13),
    ('10', 'Avg. Farm Price', 'price', 'avg_farm_price', 'Average Farm Price', 14)
ON CONFLICT (table_id, row_label_pattern) DO NOTHING;


-- ============================================================================
-- Generic Bronze table for other data sources
-- ============================================================================
CREATE TABLE IF NOT EXISTS bronze.raw_record (
    id                  BIGSERIAL PRIMARY KEY,
    data_source_id      INTEGER NOT NULL REFERENCES core.data_source(id),
    ingest_run_id       BIGINT REFERENCES audit.ingest_run(id),

    -- Natural key within source
    source_record_id    VARCHAR(500) NOT NULL,
    source_table        VARCHAR(255),           -- Logical grouping

    -- Record data
    record_data         JSONB NOT NULL,
    record_timestamp    TIMESTAMPTZ,

    -- Parsing status
    is_parsed           BOOLEAN DEFAULT FALSE,
    parse_errors        JSONB,

    -- Timestamps
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT raw_record_natural_key UNIQUE (data_source_id, source_table, source_record_id)
);

COMMENT ON TABLE bronze.raw_record IS
    'Generic bronze table for storing raw records from any source as JSONB. '
    'Use for sources without dedicated bronze tables.';

-- Indexes
CREATE INDEX IF NOT EXISTS idx_raw_record_source
    ON bronze.raw_record(data_source_id, source_table);
CREATE INDEX IF NOT EXISTS idx_raw_record_ingest
    ON bronze.raw_record(ingest_run_id);


-- ============================================================================
-- Updated_at trigger
-- ============================================================================
CREATE OR REPLACE FUNCTION bronze.set_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_wasde_release_updated
    BEFORE UPDATE ON bronze.wasde_release
    FOR EACH ROW EXECUTE FUNCTION bronze.set_updated_at();

CREATE TRIGGER trg_wasde_cell_updated
    BEFORE UPDATE ON bronze.wasde_cell
    FOR EACH ROW EXECUTE FUNCTION bronze.set_updated_at();


-- ============================================================================
-- Verification
-- ============================================================================
DO $$
BEGIN
    RAISE NOTICE 'Bronze tables created:';
    RAISE NOTICE '  - bronze.wasde_release';
    RAISE NOTICE '  - bronze.wasde_table_def: % rows', (SELECT COUNT(*) FROM bronze.wasde_table_def);
    RAISE NOTICE '  - bronze.wasde_cell';
    RAISE NOTICE '  - bronze.wasde_revision';
    RAISE NOTICE '  - bronze.wasde_row_mapping: % rows', (SELECT COUNT(*) FROM bronze.wasde_row_mapping);
    RAISE NOTICE '  - bronze.raw_record';
END $$;
