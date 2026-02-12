-- =============================================================================
-- ANEC Weekly Exports - Bronze Layer Schema
-- =============================================================================
--
-- ANEC (Associação Nacional dos Exportadores de Cereais) weekly accumulated
-- export data for Brazilian grain shipments.
--
-- Source: https://anec.com.br
-- Frequency: Weekly
-- Commodities: Soybeans, Soybean Meal, Corn, Wheat
--
-- =============================================================================

-- Register ANEC as a data source
INSERT INTO public.data_source (code, name, description, base_url, api_type, update_frequency)
VALUES (
    'ANEC',
    'ANEC - Associação Nacional dos Exportadores de Cereais',
    'Weekly accumulated grain export volumes from Brazil grain exporters association',
    'https://anec.com.br',
    'SCRAPE',
    'WEEKLY'
)
ON CONFLICT (code) DO UPDATE SET
    name = EXCLUDED.name,
    description = EXCLUDED.description,
    base_url = EXCLUDED.base_url,
    updated_at = NOW();

-- -----------------------------------------------------------------------------
-- Bronze: ANEC Weekly Export Reports
-- One row per commodity per weekly report
-- Natural key: (year, week_number, commodity_code) ensures idempotent upserts
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS bronze.anec_weekly_exports (
    id BIGSERIAL PRIMARY KEY,

    -- Natural key (unique identifier for each data point)
    year INT NOT NULL,
    week_number INT NOT NULL,               -- ISO week number (1-52/53)
    commodity_code VARCHAR(30) NOT NULL,     -- 'SOYBEANS', 'SOYBEAN_MEAL', 'CORN', 'WHEAT'

    -- Weekly data
    weekly_volume_tonnes NUMERIC(15, 2),     -- Volume shipped this week (tonnes)
    monthly_volume_tonnes NUMERIC(15, 2),    -- Monthly accumulated volume (tonnes)
    ytd_volume_tonnes NUMERIC(15, 2),        -- Year-to-date accumulated volume (tonnes)
    total_volume_tonnes NUMERIC(15, 2),      -- Total if different from YTD

    -- Year-over-year comparison columns (dynamic per report)
    ytd_prior_year_tonnes NUMERIC(15, 2),    -- Prior year YTD for comparison
    yoy_change_pct NUMERIC(8, 2),            -- Year-over-year change percentage

    -- Source metadata
    report_url VARCHAR(500),                 -- URL of the ANEC article
    pdf_path VARCHAR(500),                   -- Local path to cached PDF
    extraction_method VARCHAR(50),           -- 'pdf_table', 'pdf_text', 'html_table', 'text_regex'
    raw_text TEXT,                           -- Original text if extracted via regex

    -- Additional data columns (for flexible storage of PDF table values)
    extra_data JSONB DEFAULT '{}',

    -- Tracking
    ingest_run_id UUID REFERENCES audit.ingest_run(id),
    collected_at TIMESTAMPTZ DEFAULT NOW(),
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),

    -- Natural key constraint for idempotent upserts
    UNIQUE (year, week_number, commodity_code)
);

-- Indexes for common query patterns
CREATE INDEX IF NOT EXISTS idx_anec_exports_year
    ON bronze.anec_weekly_exports(year DESC);
CREATE INDEX IF NOT EXISTS idx_anec_exports_commodity
    ON bronze.anec_weekly_exports(commodity_code);
CREATE INDEX IF NOT EXISTS idx_anec_exports_year_week
    ON bronze.anec_weekly_exports(year DESC, week_number DESC);
CREATE INDEX IF NOT EXISTS idx_anec_exports_collected
    ON bronze.anec_weekly_exports(collected_at DESC);

COMMENT ON TABLE bronze.anec_weekly_exports IS
    'Weekly accumulated grain export volumes from ANEC (Brazil grain exporters association). '
    'One row per commodity per ISO week. Natural key: (year, week_number, commodity_code).';

-- -----------------------------------------------------------------------------
-- Bronze: ANEC PDF Report Registry
-- Tracks all downloaded PDF reports for audit and reprocessing
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS bronze.anec_report_registry (
    id SERIAL PRIMARY KEY,

    -- Natural key
    year INT NOT NULL,
    week_number INT NOT NULL,

    -- Report metadata
    report_url VARCHAR(500) NOT NULL,
    pdf_url VARCHAR(500),
    pdf_local_path VARCHAR(500),
    pdf_file_hash VARCHAR(64),               -- SHA-256 of downloaded PDF
    pdf_size_bytes INT,

    -- Processing status
    status VARCHAR(30) DEFAULT 'discovered', -- 'discovered', 'downloaded', 'parsed', 'failed'
    parse_method VARCHAR(50),                -- 'pdfplumber', 'html', 'text', 'manual'
    records_extracted INT DEFAULT 0,
    parse_errors TEXT,

    -- HTTP metadata
    http_status INT,
    content_type VARCHAR(100),
    last_checked_at TIMESTAMPTZ,

    -- Tracking
    ingest_run_id UUID REFERENCES audit.ingest_run(id),
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),

    UNIQUE (year, week_number)
);

CREATE INDEX IF NOT EXISTS idx_anec_registry_status
    ON bronze.anec_report_registry(status);

COMMENT ON TABLE bronze.anec_report_registry IS
    'Registry of all ANEC weekly PDF reports. Tracks download and parsing status.';

-- =============================================================================
-- EXAMPLE UPSERT PATTERNS
-- =============================================================================

-- Example: Upsert weekly export data
--
-- INSERT INTO bronze.anec_weekly_exports (
--     year, week_number, commodity_code,
--     weekly_volume_tonnes, monthly_volume_tonnes, ytd_volume_tonnes,
--     report_url, extraction_method, collected_at
-- ) VALUES (
--     2026, 4, 'SOYBEANS',
--     2400000, 4800000, 4800000,
--     'https://anec.com.br/article/anec-exportacoes-acumuladas-042026',
--     'pdf_table', NOW()
-- )
-- ON CONFLICT (year, week_number, commodity_code)
-- DO UPDATE SET
--     weekly_volume_tonnes = EXCLUDED.weekly_volume_tonnes,
--     monthly_volume_tonnes = EXCLUDED.monthly_volume_tonnes,
--     ytd_volume_tonnes = EXCLUDED.ytd_volume_tonnes,
--     report_url = EXCLUDED.report_url,
--     extraction_method = EXCLUDED.extraction_method,
--     collected_at = EXCLUDED.collected_at,
--     updated_at = NOW();

-- =============================================================================
-- GOLD LAYER VIEW: ANEC Export Summary
-- =============================================================================

CREATE OR REPLACE VIEW gold.anec_export_summary AS
SELECT
    e.year,
    e.week_number,
    e.commodity_code,
    c.name AS commodity_name,
    e.weekly_volume_tonnes,
    e.monthly_volume_tonnes,
    e.ytd_volume_tonnes,
    e.ytd_prior_year_tonnes,
    e.yoy_change_pct,
    -- Convert to MMT for readability
    ROUND(e.ytd_volume_tonnes / 1000000.0, 3) AS ytd_mmt,
    ROUND(e.ytd_prior_year_tonnes / 1000000.0, 3) AS ytd_prior_year_mmt,
    e.report_url,
    e.collected_at,
    e.updated_at
FROM bronze.anec_weekly_exports e
LEFT JOIN public.commodity c ON c.code = e.commodity_code
ORDER BY e.year DESC, e.week_number DESC, e.commodity_code;

COMMENT ON VIEW gold.anec_export_summary IS
    'Business-ready view of ANEC weekly export data with commodity names and MMT conversion.';

-- =============================================================================
-- GOLD LAYER VIEW: ANEC YTD Comparison (current year vs prior year)
-- =============================================================================

CREATE OR REPLACE VIEW gold.anec_ytd_comparison AS
WITH current_year AS (
    SELECT
        commodity_code,
        week_number,
        ytd_volume_tonnes,
        weekly_volume_tonnes
    FROM bronze.anec_weekly_exports
    WHERE year = EXTRACT(YEAR FROM CURRENT_DATE)
),
prior_year AS (
    SELECT
        commodity_code,
        week_number,
        ytd_volume_tonnes AS prior_ytd_volume_tonnes,
        weekly_volume_tonnes AS prior_weekly_volume_tonnes
    FROM bronze.anec_weekly_exports
    WHERE year = EXTRACT(YEAR FROM CURRENT_DATE) - 1
)
SELECT
    cy.commodity_code,
    c.name AS commodity_name,
    cy.week_number,
    ROUND(cy.ytd_volume_tonnes / 1000000.0, 3) AS current_ytd_mmt,
    ROUND(py.prior_ytd_volume_tonnes / 1000000.0, 3) AS prior_ytd_mmt,
    ROUND(cy.weekly_volume_tonnes / 1000000.0, 3) AS current_weekly_mmt,
    ROUND(py.prior_weekly_volume_tonnes / 1000000.0, 3) AS prior_weekly_mmt,
    CASE
        WHEN py.prior_ytd_volume_tonnes > 0
        THEN ROUND(
            ((cy.ytd_volume_tonnes - py.prior_ytd_volume_tonnes)
             / py.prior_ytd_volume_tonnes * 100), 1
        )
        ELSE NULL
    END AS ytd_change_pct
FROM current_year cy
LEFT JOIN prior_year py
    ON cy.commodity_code = py.commodity_code
    AND cy.week_number = py.week_number
LEFT JOIN public.commodity c ON c.code = cy.commodity_code
ORDER BY cy.week_number DESC, cy.commodity_code;

COMMENT ON VIEW gold.anec_ytd_comparison IS
    'Year-to-date export comparison: current year vs prior year by commodity and week.';

-- =============================================================================
-- END OF ANEC SCHEMA
-- =============================================================================
