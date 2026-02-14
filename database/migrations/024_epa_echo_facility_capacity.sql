-- ============================================================================
-- Migration: 024_epa_echo_facility_capacity.sql
-- Date: 2026-02-11
-- Description: Create bronze tables for EPA ECHO facility data and state
--              permit capacity data (crush, refinery, biodiesel).
--
-- Creates:
--   1. bronze.epa_echo_facility     — Raw ECHO facility registry (~200 plants)
--   2. bronze.permit_capacity       — Facility-level capacity from state permits
--   3. bronze.permit_emission_unit  — Equipment-level detail for auditing
--   4. gold.facility_capacity       — Combined ECHO + permit capacity view
--   5. gold.state_crush_capacity    — State-level capacity aggregation
--   6. gold.crush_capacity_ranking  — Ranked facility list by crush rate
--   7. Data source registrations + lineage edges
--
-- Source data:
--   - EPA ECHO collector: ~200 US soybean/oilseed facilities
--   - Iowa capacity collector: 16 Iowa Title V permit capacity parses
-- ============================================================================


-- ============================================================================
-- 0. REGISTER DATA SOURCES
-- ============================================================================

INSERT INTO public.data_source (code, name, description, api_type, update_frequency, base_url)
VALUES
    ('EPA_ECHO', 'EPA ECHO Facility Database',
     'Enforcement and Compliance History Online - facility-level environmental data for oilseed/grain processors',
     'REST', 'QUARTERLY',
     'https://echodata.epa.gov/echo/'),
    ('STATE_PERMIT', 'State Air Permit Capacity Data',
     'Title V and other state-level air permits with equipment capacity detail parsed from permit PDFs',
     'SCRAPE', 'ANNUAL',
     NULL)
ON CONFLICT (code) DO NOTHING;


-- ============================================================================
-- 1. BRONZE TABLE: EPA ECHO Facility Registry
-- ============================================================================

CREATE TABLE IF NOT EXISTS bronze.epa_echo_facility (
    id SERIAL PRIMARY KEY,

    -- Natural key: EPA Facility Registry System ID
    frs_registry_id VARCHAR(20) NOT NULL,

    -- Facility identification
    facility_name VARCHAR(300) NOT NULL,
    street_address VARCHAR(300),
    city VARCHAR(100),
    state VARCHAR(2),
    zip_code VARCHAR(10),
    county_name VARCHAR(100),
    county_fips VARCHAR(10),
    epa_region VARCHAR(5),

    -- Geolocation
    latitude NUMERIC(10, 6),
    longitude NUMERIC(10, 6),

    -- Industry classification
    sic_codes VARCHAR(200),
    naics_codes VARCHAR(200),
    dfr_naics VARCHAR(200),
    dfr_sic VARCHAR(200),

    -- Regulatory status
    operating_status VARCHAR(50),
    air_programs VARCHAR(200),
    air_classification VARCHAR(100),
    air_universe VARCHAR(100),

    -- Cross-reference IDs (from DFR enrichment)
    source_id VARCHAR(50),
    caa_permit_ids TEXT,
    npdes_permit_ids TEXT,
    rcra_handler_ids TEXT,
    tri_facility_id VARCHAR(50),
    ghg_reporter_id VARCHAR(50),

    -- Compliance
    compliance_status TEXT,
    enforcement_actions TEXT,

    -- Search metadata
    search_profile VARCHAR(50),
    source_endpoint VARCHAR(200),
    collector_version VARCHAR(20),

    -- Audit
    ingest_run_id UUID,
    collected_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),

    UNIQUE(frs_registry_id)
);

CREATE INDEX IF NOT EXISTS idx_echo_fac_state
    ON bronze.epa_echo_facility(state);
CREATE INDEX IF NOT EXISTS idx_echo_fac_city_state
    ON bronze.epa_echo_facility(state, city);
CREATE INDEX IF NOT EXISTS idx_echo_fac_sic
    ON bronze.epa_echo_facility(sic_codes);
CREATE INDEX IF NOT EXISTS idx_echo_fac_name
    ON bronze.epa_echo_facility(facility_name);
CREATE INDEX IF NOT EXISTS idx_echo_fac_operating
    ON bronze.epa_echo_facility(operating_status);

COMMENT ON TABLE bronze.epa_echo_facility IS
    'EPA ECHO facility data for oilseed/grain processing plants. Source: ECHO air_rest_services + DFR enrichment. Natural key: frs_registry_id.';


-- ============================================================================
-- 2. BRONZE TABLE: State Permit Capacity (Summary Level)
-- ============================================================================

CREATE TABLE IF NOT EXISTS bronze.permit_capacity (
    id SERIAL PRIMARY KEY,

    -- Natural key: state + permit number uniquely identifies a permit
    state VARCHAR(2) NOT NULL,
    permit_number VARCHAR(50) NOT NULL,

    -- Facility identification (from permit document)
    facility_name VARCHAR(300) NOT NULL,
    facility_number VARCHAR(30),
    city VARCHAR(100),
    facility_description TEXT,

    -- Link to EPA ECHO (NULL until matched)
    frs_registry_id VARCHAR(20),

    -- Crush capacity
    crush_capacity_tons_per_hour NUMERIC(10, 2),
    crush_capacity_bushels_per_day NUMERIC(12, 0),
    crush_description TEXT,

    -- Refinery capacity
    has_refinery BOOLEAN DEFAULT FALSE,
    refinery_capacity_tons_per_hour NUMERIC(10, 2),
    refinery_capacity_tons_per_year NUMERIC(12, 0),
    refinery_description TEXT,
    refining_type VARCHAR(30),

    -- Biodiesel/renewable fuels capacity
    has_biodiesel BOOLEAN DEFAULT FALSE,
    biodiesel_capacity_mgy NUMERIC(10, 2),

    -- Source document metadata
    permit_source_agency VARCHAR(100),
    permit_type VARCHAR(50),
    permit_pdf_url TEXT,
    permit_pdf_hash VARCHAR(64),

    -- Equipment counts
    total_emission_units_found INTEGER,
    crush_units_count INTEGER,
    refinery_units_count INTEGER,
    biodiesel_units_count INTEGER,

    -- Audit
    ingest_run_id UUID,
    collected_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),

    UNIQUE(state, permit_number)
);

CREATE INDEX IF NOT EXISTS idx_permit_cap_state
    ON bronze.permit_capacity(state);
CREATE INDEX IF NOT EXISTS idx_permit_cap_frs
    ON bronze.permit_capacity(frs_registry_id);
CREATE INDEX IF NOT EXISTS idx_permit_cap_crush
    ON bronze.permit_capacity(crush_capacity_tons_per_hour DESC NULLS LAST);
CREATE INDEX IF NOT EXISTS idx_permit_cap_city
    ON bronze.permit_capacity(state, city);
CREATE INDEX IF NOT EXISTS idx_permit_cap_refining_type
    ON bronze.permit_capacity(refining_type)
    WHERE refining_type IS NOT NULL AND refining_type != '';

COMMENT ON TABLE bronze.permit_capacity IS
    'Facility-level capacity summaries from state air permits. One row per facility per permit. Designed for multi-state data. Natural key: (state, permit_number).';


-- ============================================================================
-- 3. BRONZE TABLE: Permit Emission Unit Detail
-- ============================================================================

CREATE TABLE IF NOT EXISTS bronze.permit_emission_unit (
    id SERIAL PRIMARY KEY,

    -- Reference to permit_capacity
    state VARCHAR(2) NOT NULL,
    permit_number VARCHAR(50) NOT NULL,

    -- Equipment identification
    page INTEGER,
    eu_id VARCHAR(30),
    description TEXT,

    -- Capacity
    capacity_value NUMERIC(12, 4),
    capacity_unit VARCHAR(60),

    -- Classification
    category VARCHAR(30),

    -- Source context
    raw_line TEXT,
    context TEXT,

    -- Audit
    ingest_run_id UUID,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Natural key index with COALESCE for nullable eu_id
CREATE UNIQUE INDEX IF NOT EXISTS uq_permit_eu_natural_key
    ON bronze.permit_emission_unit(
        state, permit_number, page,
        COALESCE(eu_id, ''),
        capacity_value, capacity_unit
    );

CREATE INDEX IF NOT EXISTS idx_permit_eu_permit
    ON bronze.permit_emission_unit(state, permit_number);
CREATE INDEX IF NOT EXISTS idx_permit_eu_category
    ON bronze.permit_emission_unit(category);

COMMENT ON TABLE bronze.permit_emission_unit IS
    'Equipment-level detail from state air permits. Many rows per permit. Used for audit/validation of capacity summaries.';


-- ============================================================================
-- 4. GOLD VIEW: Facility Capacity (ECHO + Permit Combined)
-- ============================================================================

CREATE OR REPLACE VIEW gold.facility_capacity AS
SELECT
    COALESCE(f.frs_registry_id, pc.frs_registry_id) AS frs_registry_id,
    COALESCE(f.facility_name, pc.facility_name) AS facility_name,
    COALESCE(f.street_address, '') AS street_address,
    COALESCE(f.city, pc.city) AS city,
    COALESCE(f.state, pc.state) AS state,
    f.zip_code,
    f.county_name,
    f.epa_region,

    -- Geolocation
    f.latitude,
    f.longitude,

    -- Industry codes
    f.sic_codes,
    f.naics_codes,
    f.dfr_naics,
    f.operating_status,

    -- Capacity data
    pc.permit_number,
    pc.crush_capacity_tons_per_hour,
    pc.crush_capacity_bushels_per_day,
    ROUND(pc.crush_capacity_tons_per_hour * 24 * 350, 0) AS crush_capacity_tons_per_year,
    pc.crush_description,

    -- Refinery
    pc.has_refinery,
    pc.refinery_capacity_tons_per_hour,
    pc.refinery_capacity_tons_per_year,
    pc.refining_type,

    -- Biodiesel
    pc.has_biodiesel,
    pc.biodiesel_capacity_mgy,

    -- Regulatory
    f.caa_permit_ids,
    f.tri_facility_id,
    f.ghg_reporter_id,
    f.compliance_status,

    -- Data completeness flags
    CASE WHEN f.frs_registry_id IS NOT NULL THEN TRUE ELSE FALSE END AS has_echo_data,
    CASE WHEN pc.id IS NOT NULL THEN TRUE ELSE FALSE END AS has_permit_capacity,

    -- Metadata
    pc.permit_source_agency,
    f.collected_at AS echo_collected_at,
    pc.collected_at AS permit_collected_at

FROM bronze.epa_echo_facility f
FULL OUTER JOIN bronze.permit_capacity pc
    ON f.frs_registry_id = pc.frs_registry_id
ORDER BY
    pc.crush_capacity_tons_per_hour DESC NULLS LAST,
    COALESCE(f.state, pc.state),
    COALESCE(f.city, pc.city);

COMMENT ON VIEW gold.facility_capacity IS
    'Combined EPA ECHO facility data with state permit capacity. Joins on frs_registry_id. Analyst-facing: use for crush capacity rankings and facility profiles.';


-- ============================================================================
-- 5. GOLD VIEW: State Crush Capacity Aggregation
-- ============================================================================

CREATE OR REPLACE VIEW gold.state_crush_capacity AS
SELECT
    pc.state,
    COUNT(*) AS facility_count,
    COUNT(*) FILTER (WHERE pc.crush_capacity_tons_per_hour > 0) AS facilities_with_crush,
    COUNT(*) FILTER (WHERE pc.has_refinery = TRUE) AS facilities_with_refinery,
    COUNT(*) FILTER (WHERE pc.has_biodiesel = TRUE) AS facilities_with_biodiesel,

    -- Crush totals
    ROUND(SUM(pc.crush_capacity_tons_per_hour), 1) AS total_crush_tons_per_hour,
    ROUND(SUM(pc.crush_capacity_bushels_per_day), 0) AS total_crush_bushels_per_day,
    ROUND(SUM(pc.crush_capacity_tons_per_hour) * 24 * 350, 0) AS total_crush_tons_per_year,

    -- Refinery totals
    ROUND(SUM(pc.refinery_capacity_tons_per_hour), 1) AS total_refinery_tons_per_hour,

    -- Biodiesel totals
    ROUND(SUM(pc.biodiesel_capacity_mgy), 1) AS total_biodiesel_mgy,

    -- Refining types present
    STRING_AGG(DISTINCT pc.refining_type, ', ')
        FILTER (WHERE pc.refining_type IS NOT NULL AND pc.refining_type != '')
        AS refining_types_present,

    -- Average facility size
    ROUND(AVG(pc.crush_capacity_tons_per_hour)
        FILTER (WHERE pc.crush_capacity_tons_per_hour > 0), 1)
        AS avg_crush_tons_per_hour,

    -- Data source
    STRING_AGG(DISTINCT pc.permit_source_agency, ', ') AS data_sources

FROM bronze.permit_capacity pc
GROUP BY pc.state
ORDER BY total_crush_tons_per_hour DESC NULLS LAST;

COMMENT ON VIEW gold.state_crush_capacity IS
    'State-level aggregation of oilseed processing capacity from permits. Use for comparing crush capacity across states.';


-- ============================================================================
-- 6. GOLD VIEW: Crush Capacity Ranking
-- ============================================================================

CREATE OR REPLACE VIEW gold.crush_capacity_ranking AS
SELECT
    ROW_NUMBER() OVER (ORDER BY pc.crush_capacity_tons_per_hour DESC) AS rank,
    pc.facility_name,
    pc.city,
    pc.state,
    pc.permit_number,
    pc.crush_capacity_tons_per_hour,
    pc.crush_capacity_bushels_per_day,
    ROUND(pc.crush_capacity_tons_per_hour * 24 * 350, 0) AS crush_capacity_tons_per_year,
    pc.has_refinery,
    pc.refining_type,
    pc.has_biodiesel,
    pc.biodiesel_capacity_mgy,
    f.frs_registry_id,
    f.latitude,
    f.longitude,
    f.operating_status
FROM bronze.permit_capacity pc
LEFT JOIN bronze.epa_echo_facility f
    ON pc.frs_registry_id = f.frs_registry_id
WHERE pc.crush_capacity_tons_per_hour > 0
ORDER BY pc.crush_capacity_tons_per_hour DESC;

COMMENT ON VIEW gold.crush_capacity_ranking IS
    'Ranked list of oilseed processing facilities by crush capacity. Only includes facilities with known capacity > 0.';


-- ============================================================================
-- 7. LINEAGE EDGES
-- ============================================================================

INSERT INTO audit.lineage_edge (
    source_type, source_schema, source_name,
    target_type, target_schema, target_name,
    relationship_type, transformation_description
) VALUES
    -- API -> bronze.epa_echo_facility
    ('API', NULL, 'EPA_ECHO_API',
     'TABLE', 'bronze', 'epa_echo_facility',
     'COPIES', 'EPA ECHO facility data loaded from collector Excel output'),

    -- File -> bronze.permit_capacity
    ('FILE', NULL, 'state_titlev_permits',
     'TABLE', 'bronze', 'permit_capacity',
     'TRANSFORMS', 'Title V permit PDFs parsed for facility-level capacity summary'),

    -- File -> bronze.permit_emission_unit
    ('FILE', NULL, 'state_titlev_permits',
     'TABLE', 'bronze', 'permit_emission_unit',
     'TRANSFORMS', 'Title V permit PDFs parsed for equipment-level emission unit detail'),

    -- bronze -> gold.facility_capacity
    ('TABLE', 'bronze', 'epa_echo_facility',
     'VIEW', 'gold', 'facility_capacity',
     'JOINS', 'ECHO facility data joined with permit capacity on frs_registry_id'),

    ('TABLE', 'bronze', 'permit_capacity',
     'VIEW', 'gold', 'facility_capacity',
     'JOINS', 'Permit capacity data joined with ECHO facilities'),

    -- bronze -> gold.state_crush_capacity
    ('TABLE', 'bronze', 'permit_capacity',
     'VIEW', 'gold', 'state_crush_capacity',
     'AGGREGATES', 'State-level capacity totals aggregated from individual permits'),

    -- bronze -> gold.crush_capacity_ranking
    ('TABLE', 'bronze', 'permit_capacity',
     'VIEW', 'gold', 'crush_capacity_ranking',
     'DERIVES_FROM', 'Ranked facility list from permit capacity data'),

    ('TABLE', 'bronze', 'epa_echo_facility',
     'VIEW', 'gold', 'crush_capacity_ranking',
     'REFERENCES', 'ECHO facility geolocation and status enrichment')

ON CONFLICT DO NOTHING;


-- ============================================================================
-- VERIFICATION
-- ============================================================================
-- After running this migration:
--
-- 1. Verify tables created:
--    SELECT table_name FROM information_schema.tables
--    WHERE table_schema = 'bronze'
--    AND table_name IN ('epa_echo_facility', 'permit_capacity', 'permit_emission_unit');
--
-- 2. Verify views created:
--    SELECT table_name FROM information_schema.views
--    WHERE table_schema = 'gold'
--    AND table_name IN ('facility_capacity', 'state_crush_capacity', 'crush_capacity_ranking');
--
-- 3. After loading data, verify ECHO facilities:
--    SELECT state, COUNT(*) FROM bronze.epa_echo_facility GROUP BY state ORDER BY COUNT(*) DESC;
--
-- 4. Verify Iowa capacity:
--    SELECT facility_name, crush_capacity_tons_per_hour, has_refinery, refining_type
--    FROM bronze.permit_capacity WHERE state = 'IA'
--    ORDER BY crush_capacity_tons_per_hour DESC;
--
-- 5. Test gold view:
--    SELECT * FROM gold.crush_capacity_ranking LIMIT 10;
--
-- 6. Test state aggregation:
--    SELECT * FROM gold.state_crush_capacity;
--
-- 7. Verify lineage:
--    SELECT source_name, target_name, relationship_type
--    FROM audit.lineage_edge
--    WHERE target_name LIKE '%facility%' OR target_name LIKE '%capacity%' OR target_name LIKE '%crush%';
-- ============================================================================
