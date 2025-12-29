-- =============================================================================
-- RLC Commodities Database Schema - Bronze Layer
-- Version: 1.0.0
-- =============================================================================
--
-- BRONZE LAYER PHILOSOPHY
-- -----------------------
-- Bronze tables store data exactly as received from sources:
-- - Preserve original field names, formats, and values
-- - Enable full audit trail back to source
-- - Allow re-processing if transformation logic changes
-- - Natural keys ensure idempotent upserts
--
-- =============================================================================

-- =============================================================================
-- WASDE (World Agricultural Supply and Demand Estimates)
-- =============================================================================

-- -----------------------------------------------------------------------------
-- WASDE Release: One row per monthly report
-- -----------------------------------------------------------------------------
CREATE TABLE bronze.wasde_release (
    id SERIAL PRIMARY KEY,

    -- Natural key
    report_date DATE NOT NULL UNIQUE,
    report_month VARCHAR(20) NOT NULL,  -- 'January 2025'

    -- Metadata from USDA
    report_title VARCHAR(500),
    release_datetime TIMESTAMPTZ,

    -- Tracking
    ingest_run_id UUID REFERENCES audit.ingest_run(id),
    raw_file_path VARCHAR(500),
    raw_file_hash VARCHAR(64),

    -- Status
    is_current BOOLEAN DEFAULT FALSE,  -- Most recent release
    superseded_by_date DATE,            -- If corrected

    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_wasde_release_date ON bronze.wasde_release(report_date DESC);

-- -----------------------------------------------------------------------------
-- WASDE Cell: One row per data cell in WASDE tables
-- Stores the exact value as published, plus parsed numeric
-- -----------------------------------------------------------------------------
CREATE TABLE bronze.wasde_cell (
    id BIGSERIAL PRIMARY KEY,

    -- Foreign key to release
    report_date DATE NOT NULL REFERENCES bronze.wasde_release(report_date),

    -- Location within WASDE document
    table_name VARCHAR(200) NOT NULL,    -- e.g., 'U.S. Soybeans and Products Supply and Use'
    table_number VARCHAR(20),             -- e.g., 'Table 06'
    row_label VARCHAR(300) NOT NULL,      -- e.g., 'Ending Stocks'
    column_label VARCHAR(100) NOT NULL,   -- e.g., '2024/25 Proj. Jan' or 'Jan'

    -- Values as published
    raw_value VARCHAR(100),               -- Exact text from PDF/API
    numeric_value NUMERIC(20, 4),         -- Parsed numeric (NULL if not parseable)
    unit_text VARCHAR(50),                -- e.g., 'Mil. Bu.' as shown

    -- Additional context
    marketing_year VARCHAR(20),           -- e.g., '2024/25'
    projection_month VARCHAR(20),         -- e.g., 'Jan' for projections
    is_projection BOOLEAN DEFAULT FALSE,
    is_estimate BOOLEAN DEFAULT FALSE,

    -- Revision tracking
    revision_number INT DEFAULT 0,        -- 0 = original, 1+ = corrections

    -- Tracking
    ingest_run_id UUID REFERENCES audit.ingest_run(id),

    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),

    -- Natural key for idempotent upserts
    UNIQUE (report_date, table_name, row_label, column_label, revision_number)
);

CREATE INDEX idx_wasde_cell_report ON bronze.wasde_cell(report_date);
CREATE INDEX idx_wasde_cell_table ON bronze.wasde_cell(table_name);
CREATE INDEX idx_wasde_cell_row ON bronze.wasde_cell(row_label);
CREATE INDEX idx_wasde_cell_my ON bronze.wasde_cell(marketing_year);

COMMENT ON TABLE bronze.wasde_cell IS 'Raw WASDE data cells. Each row = one cell from a WASDE table.';

-- =============================================================================
-- CENSUS TRADE DATA
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Census Trade Raw: Monthly import/export records by country
-- -----------------------------------------------------------------------------
CREATE TABLE bronze.census_trade_raw (
    id BIGSERIAL PRIMARY KEY,

    -- Natural key components
    year INT NOT NULL,
    month INT NOT NULL,
    flow VARCHAR(10) NOT NULL,         -- 'exports' or 'imports'
    hs_code VARCHAR(10) NOT NULL,
    country_code VARCHAR(10) NOT NULL,

    -- Values from Census API
    country_name VARCHAR(200),
    value_usd NUMERIC(20, 2),          -- Trade value in USD
    quantity NUMERIC(20, 4),           -- Quantity in source units
    quantity_unit VARCHAR(50),         -- Unit description from Census

    -- Commodity mapping
    commodity_code VARCHAR(30),        -- Our internal commodity code

    -- Tracking
    ingest_run_id UUID REFERENCES audit.ingest_run(id),
    api_response_time TIMESTAMPTZ,

    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),

    -- Natural key for idempotent upserts
    UNIQUE (year, month, flow, hs_code, country_code)
);

CREATE INDEX idx_census_trade_date ON bronze.census_trade_raw(year, month);
CREATE INDEX idx_census_trade_flow ON bronze.census_trade_raw(flow);
CREATE INDEX idx_census_trade_hs ON bronze.census_trade_raw(hs_code);
CREATE INDEX idx_census_trade_country ON bronze.census_trade_raw(country_code);
CREATE INDEX idx_census_trade_commodity ON bronze.census_trade_raw(commodity_code);

-- =============================================================================
-- FGIS EXPORT INSPECTIONS
-- =============================================================================

-- -----------------------------------------------------------------------------
-- FGIS Inspection Raw: Weekly export inspection records
-- -----------------------------------------------------------------------------
CREATE TABLE bronze.fgis_inspection_raw (
    id BIGSERIAL PRIMARY KEY,

    -- Natural key components
    cert_date DATE NOT NULL,             -- Certification date
    commodity VARCHAR(50) NOT NULL,      -- Grain type from FGIS
    destination_country VARCHAR(100) NOT NULL,
    port_region VARCHAR(100),

    -- Values from FGIS
    metric_tons NUMERIC(15, 3),
    num_containers INT,
    num_railcars INT,
    num_barges INT,
    num_trucks INT,

    -- Quality metrics (if available)
    test_weight NUMERIC(8, 3),
    moisture NUMERIC(8, 3),
    damaged_kernels NUMERIC(8, 3),
    foreign_material NUMERIC(8, 3),
    protein NUMERIC(8, 3),
    oil NUMERIC(8, 3),

    -- Week calculation
    week_ending_date DATE,              -- Thursday of the week

    -- Tracking
    ingest_run_id UUID REFERENCES audit.ingest_run(id),
    source_file VARCHAR(200),
    source_row_number INT,

    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),

    -- Natural key for idempotent upserts
    UNIQUE (cert_date, commodity, destination_country, COALESCE(port_region, 'UNKNOWN'))
);

CREATE INDEX idx_fgis_cert_date ON bronze.fgis_inspection_raw(cert_date);
CREATE INDEX idx_fgis_week ON bronze.fgis_inspection_raw(week_ending_date);
CREATE INDEX idx_fgis_commodity ON bronze.fgis_inspection_raw(commodity);
CREATE INDEX idx_fgis_destination ON bronze.fgis_inspection_raw(destination_country);

-- =============================================================================
-- USDA AMS PRICES
-- =============================================================================

-- -----------------------------------------------------------------------------
-- AMS Cash Prices: Daily/weekly cash market prices
-- -----------------------------------------------------------------------------
CREATE TABLE bronze.ams_price_raw (
    id BIGSERIAL PRIMARY KEY,

    -- Natural key components
    report_date DATE NOT NULL,
    commodity VARCHAR(50) NOT NULL,
    market_location VARCHAR(200) NOT NULL,
    price_type VARCHAR(100) NOT NULL,    -- 'BID', 'ASK', 'SALE', 'CLOSE'

    -- Values
    price NUMERIC(12, 4),
    price_low NUMERIC(12, 4),
    price_high NUMERIC(12, 4),
    unit_description VARCHAR(100),
    basis_value NUMERIC(8, 4),
    basis_month VARCHAR(20),

    -- Report metadata
    report_title VARCHAR(300),
    slug_id VARCHAR(100),

    -- Tracking
    ingest_run_id UUID REFERENCES audit.ingest_run(id),

    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),

    -- Natural key
    UNIQUE (report_date, commodity, market_location, price_type)
);

CREATE INDEX idx_ams_price_date ON bronze.ams_price_raw(report_date);
CREATE INDEX idx_ams_price_commodity ON bronze.ams_price_raw(commodity);
CREATE INDEX idx_ams_price_location ON bronze.ams_price_raw(market_location);

-- =============================================================================
-- EIA ENERGY DATA
-- =============================================================================

-- -----------------------------------------------------------------------------
-- EIA Series Raw: Energy Information Administration data
-- -----------------------------------------------------------------------------
CREATE TABLE bronze.eia_series_raw (
    id BIGSERIAL PRIMARY KEY,

    -- Natural key components
    series_id VARCHAR(100) NOT NULL,     -- EIA's series identifier
    observation_date DATE NOT NULL,

    -- Value
    value NUMERIC(20, 6),
    unit VARCHAR(50),

    -- Series metadata (stored with each row for simplicity)
    series_name VARCHAR(500),
    frequency VARCHAR(20),
    geography VARCHAR(100),

    -- Tracking
    ingest_run_id UUID REFERENCES audit.ingest_run(id),

    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),

    UNIQUE (series_id, observation_date)
);

CREATE INDEX idx_eia_series ON bronze.eia_series_raw(series_id);
CREATE INDEX idx_eia_date ON bronze.eia_series_raw(observation_date);

-- =============================================================================
-- RAW PAYLOAD STORAGE (for complete audit trail)
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Raw Payload: Store complete API responses for debugging/reprocessing
-- -----------------------------------------------------------------------------
CREATE TABLE bronze.raw_payload (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),

    -- Reference
    ingest_run_id UUID NOT NULL REFERENCES audit.ingest_run(id),

    -- Payload
    payload_type VARCHAR(50) NOT NULL,  -- 'JSON', 'CSV', 'XML', 'BINARY'
    payload JSONB,                       -- For JSON responses
    payload_text TEXT,                   -- For CSV/XML
    payload_binary BYTEA,                -- For binary files

    -- Metadata
    content_hash VARCHAR(64),
    byte_size INT,
    compression VARCHAR(20),

    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_raw_payload_ingest ON bronze.raw_payload(ingest_run_id);

COMMENT ON TABLE bronze.raw_payload IS 'Optional: Store complete API responses for full audit trail. Use for critical sources.';

-- =============================================================================
-- IDEMPOTENT UPSERT EXAMPLES
-- =============================================================================

-- Example: Upsert WASDE cell
--
-- INSERT INTO bronze.wasde_cell (
--     report_date, table_name, row_label, column_label,
--     raw_value, numeric_value, unit_text, marketing_year,
--     ingest_run_id
-- ) VALUES (
--     '2025-01-10', 'U.S. Soybeans and Products Supply and Use',
--     'Ending Stocks', '2024/25 Proj. Jan',
--     '380', 380.0, 'Mil. Bu.', '2024/25',
--     'abc123...'
-- )
-- ON CONFLICT (report_date, table_name, row_label, column_label, revision_number)
-- DO UPDATE SET
--     raw_value = EXCLUDED.raw_value,
--     numeric_value = EXCLUDED.numeric_value,
--     unit_text = EXCLUDED.unit_text,
--     ingest_run_id = EXCLUDED.ingest_run_id,
--     updated_at = NOW();

-- Example: Upsert Census trade record
--
-- INSERT INTO bronze.census_trade_raw (
--     year, month, flow, hs_code, country_code,
--     country_name, value_usd, quantity, quantity_unit,
--     commodity_code, ingest_run_id
-- ) VALUES (
--     2024, 11, 'exports', '120190', '5700',
--     'China', 1500000000, 4500000, 'KILOGRAMS',
--     'SOYBEANS', 'abc123...'
-- )
-- ON CONFLICT (year, month, flow, hs_code, country_code)
-- DO UPDATE SET
--     country_name = EXCLUDED.country_name,
--     value_usd = EXCLUDED.value_usd,
--     quantity = EXCLUDED.quantity,
--     quantity_unit = EXCLUDED.quantity_unit,
--     commodity_code = EXCLUDED.commodity_code,
--     ingest_run_id = EXCLUDED.ingest_run_id,
--     updated_at = NOW();

-- =============================================================================
-- END OF BRONZE LAYER SCRIPT
-- =============================================================================
