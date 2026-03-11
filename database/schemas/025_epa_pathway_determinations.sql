-- =============================================================================
-- EPA Pathway Determination Letters — Renewable Fuel Plants
-- =============================================================================
-- Facility-specific pathway approvals for RFS2 RIN generation.
-- Source: https://www.epa.gov/renewable-fuel-standard/approved-pathways-renewable-fuel
--
-- Workflow:
--   python src/tools/epa_pathway_collector.py --all
-- =============================================================================

-- =============================================================================
-- REFERENCE: Generally Applicable Pathways (Table 1, 40 CFR 80.1426)
-- =============================================================================
CREATE TABLE IF NOT EXISTS reference.epa_generally_applicable_pathways (
    row_letter CHAR(1) PRIMARY KEY,
    fuel_type VARCHAR(300) NOT NULL,
    feedstock VARCHAR(300) NOT NULL,
    production_process TEXT,
    d_code VARCHAR(100) NOT NULL
);

COMMENT ON TABLE reference.epa_generally_applicable_pathways IS
    'Table 1 of 40 CFR 80.1426: Generally applicable fuel pathways. '
    'Any qualifying producer can use these without a facility-specific petition.';

-- =============================================================================
-- BRONZE: Pathway index (scraped from EPA HTML table)
-- =============================================================================
CREATE TABLE IF NOT EXISTS bronze.epa_pathway_index (
    id SERIAL PRIMARY KEY,
    determination_name VARCHAR(300) NOT NULL,
    category VARCHAR(20) NOT NULL,           -- 'non_ep3' or 'ep3'
    fuel_type VARCHAR(500),
    feedstock VARCHAR(500),
    d_code VARCHAR(20),
    determination_date DATE,
    pdf_url TEXT NOT NULL,
    pdf_filename VARCHAR(300),
    pdf_downloaded BOOLEAN DEFAULT FALSE,
    pdf_parsed BOOLEAN DEFAULT FALSE,
    collected_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (pdf_url)
);

CREATE INDEX IF NOT EXISTS idx_epa_pathway_index_category
    ON bronze.epa_pathway_index(category);

CREATE INDEX IF NOT EXISTS idx_epa_pathway_index_dcode
    ON bronze.epa_pathway_index(d_code);

COMMENT ON TABLE bronze.epa_pathway_index IS
    'Index of EPA pathway determination letters scraped from the approved pathways page. '
    'One row per PDF letter. Source: epa.gov/renewable-fuel-standard/approved-pathways-renewable-fuel';

-- =============================================================================
-- BRONZE: Pathway detail (extracted from PDF letters)
-- =============================================================================
CREATE TABLE IF NOT EXISTS bronze.epa_pathway_detail (
    id SERIAL PRIMARY KEY,
    pathway_index_id INT NOT NULL REFERENCES bronze.epa_pathway_index(id),

    -- Recipient info
    recipient_name VARCHAR(300),
    recipient_title VARCHAR(300),
    company_name VARCHAR(300),
    mailing_address TEXT,

    -- Facility info
    facility_name VARCHAR(500),
    facility_city VARCHAR(200),
    facility_state CHAR(2),
    facility_state_full VARCHAR(50),

    -- Pathway details
    fuel_types TEXT[],
    feedstocks TEXT[],
    d_code INT,
    production_process VARCHAR(500),
    process_energy_sources TEXT[],

    -- GHG results
    ghg_reduction_pct NUMERIC(5,1),
    lifecycle_ghg_gco2e_mj NUMERIC(8,1),
    ghg_baseline_gco2e_mj NUMERIC(8,1),

    -- Regulatory
    table1_row_reference CHAR(1),
    pathway_name VARCHAR(500),

    -- Raw text
    full_text TEXT,
    page_count INT,

    -- Metadata
    parsed_at TIMESTAMPTZ DEFAULT NOW(),
    parse_confidence VARCHAR(20),
    parse_notes TEXT,

    UNIQUE (pathway_index_id)
);

COMMENT ON TABLE bronze.epa_pathway_detail IS
    'Detailed data extracted from EPA pathway determination letter PDFs. '
    'One row per letter. Fields parsed via regex from letter text.';

-- =============================================================================
-- GOLD: Renewable fuel plants view
-- =============================================================================
CREATE OR REPLACE VIEW gold.renewable_fuel_plants AS
SELECT
    COALESCE(d.company_name, i.determination_name) AS company_name,
    d.facility_name,
    d.facility_city,
    d.facility_state,
    i.category,
    i.fuel_type,
    i.feedstock,
    i.d_code,
    i.determination_date,
    d.production_process,
    d.ghg_reduction_pct,
    d.lifecycle_ghg_gco2e_mj,
    d.process_energy_sources,
    d.pathway_name,
    d.recipient_name,
    d.mailing_address,
    i.pdf_url,
    d.parse_confidence
FROM bronze.epa_pathway_index i
LEFT JOIN bronze.epa_pathway_detail d ON d.pathway_index_id = i.id
ORDER BY i.determination_date DESC;

COMMENT ON VIEW gold.renewable_fuel_plants IS
    'EPA-approved renewable fuel facilities with pathway details from determination letters.';

-- =============================================================================
-- GOLD: Pathway summary view
-- =============================================================================
CREATE OR REPLACE VIEW gold.pathway_summary AS
SELECT
    i.category,
    i.d_code,
    i.fuel_type,
    i.feedstock,
    COUNT(*) AS determination_count,
    MIN(i.determination_date) AS earliest_determination,
    MAX(i.determination_date) AS latest_determination,
    COUNT(CASE WHEN d.id IS NOT NULL THEN 1 END) AS parsed_count,
    AVG(d.ghg_reduction_pct) AS avg_ghg_reduction_pct
FROM bronze.epa_pathway_index i
LEFT JOIN bronze.epa_pathway_detail d ON d.pathway_index_id = i.id
GROUP BY i.category, i.d_code, i.fuel_type, i.feedstock
ORDER BY determination_count DESC;

COMMENT ON VIEW gold.pathway_summary IS
    'Aggregated pathway determination counts by category, D-code, fuel type, and feedstock.';
