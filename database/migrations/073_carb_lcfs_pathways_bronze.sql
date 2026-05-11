-- Migration 073: bronze.carb_lcfs_pathways + silver.carb_pathway_dim + silver.facility_carb_status
-- Date: 2026-05-10
--
-- Loads CARB's LCFS Certified Fuel Pathways into the warehouse so we can:
--   (a) cross-reference against our facility inventory
--   (b) detect closures/idlings via pathway-absence signal (proven by
--       REG Ralston/Madison discovery 2026-05-10)
--   (c) separate the SAF technology pathways (HEFA vs AtJ vs FT) since
--       CARB's "Alternative Jet Fuel (AJF)" lumps them
--
-- Source workbook (refresh quarterly):
--   https://ww2.arb.ca.gov/sites/default/files/classic/fuels/lcfs/fuelpathways/current-pathways_all.xlsx
-- Cached at:
--   domain_knowledge/external_lists/carb_lcfs/current_pathways_all.xlsx
-- Extracted JSON:
--   domain_knowledge/external_lists/carb_lcfs/biofuel_pathways.json
--
-- Refresh cadence: CARB publishes updates as pathways are certified; the
-- "current" file is a rolling snapshot. We treat each load as a snapshot
-- and PRESERVE history so we can detect "was certified Q1, gone by Q2" =
-- closure signal.

BEGIN;

-- ============================================================================
-- bronze.carb_lcfs_pathways — raw snapshots
-- ============================================================================

CREATE TABLE IF NOT EXISTS bronze.carb_lcfs_pathways (
    -- Snapshot identity
    snapshot_date       DATE        NOT NULL,
    pathway_id          TEXT        NOT NULL,

    -- Raw CARB columns
    class               TEXT,            -- Tier 1 / Tier 2 / Lookup Table
    calc_version        TEXT,            -- e.g., "OPGEE 3.0", "CA-GREET 3.0"
    fuel_producer       TEXT,            -- normalized company name
    facility_name       TEXT,            -- normalized facility name
    facility_location   TEXT,            -- state, sometimes city+state
    feedstock           TEXT,            -- e.g., "Soybean Oil (005)"
    fuel_type           TEXT,            -- biodiesel / RD / AJF / etc.
    ci_current          NUMERIC,         -- carbon intensity (g CO2e/MJ)
    fpc                 TEXT,            -- Fuel Pathway Code (FPC)
    certification_date  DATE,
    applicant_description TEXT,          -- full text from CARB workbook

    -- Provenance
    source_file         TEXT,            -- path to the xlsx we loaded
    loaded_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    PRIMARY KEY (snapshot_date, pathway_id)
);

CREATE INDEX IF NOT EXISTS carb_pathways_fuel_producer_idx
    ON bronze.carb_lcfs_pathways (lower(fuel_producer));
CREATE INDEX IF NOT EXISTS carb_pathways_facility_name_idx
    ON bronze.carb_lcfs_pathways (lower(facility_name));
CREATE INDEX IF NOT EXISTS carb_pathways_fuel_type_idx
    ON bronze.carb_lcfs_pathways (fuel_type);
CREATE INDEX IF NOT EXISTS carb_pathways_feedstock_idx
    ON bronze.carb_lcfs_pathways (feedstock);
CREATE INDEX IF NOT EXISTS carb_pathways_snapshot_idx
    ON bronze.carb_lcfs_pathways (snapshot_date DESC);

COMMENT ON TABLE bronze.carb_lcfs_pathways IS
    'CARB LCFS certified fuel pathways. One row per (pathway_id, snapshot_date) — history preserved across refreshes.';

-- ============================================================================
-- silver.carb_pathway_dim — pathway classification dimension
-- ============================================================================
-- CARB's "Alternative Jet Fuel (AJF)" is a single fuel_type but actually
-- covers radically different technologies. This dim splits them:
--
--   HEFA       — Hydrogenated Esters and Fatty Acids (lipid feedstocks)
--   AtJ        — Alcohol-to-Jet (ethanol/isobutanol)
--   FT-SPK     — Fischer-Tropsch Synthetic Paraffinic Kerosene (gasification)
--   CoProcess  — refinery co-processing of lipids in conventional units
--   Other      — anything not yet seen
--
-- Same logic applies to renewable diesel — RD is mostly HEFA but a few
-- pathways use co-processing (CoProcess RD).

CREATE TABLE IF NOT EXISTS silver.carb_pathway_dim (
    pathway_id          TEXT PRIMARY KEY,
    fuel_class          TEXT NOT NULL,        -- 'BD' | 'RD' | 'SAF' | 'RNG' | 'Other'
    technology          TEXT NOT NULL,        -- 'HEFA' | 'AtJ' | 'FT-SPK' | 'CoProcess' | 'Esterification' | 'Other'
    feedstock_family    TEXT,                 -- 'lipid_oilseed' | 'lipid_waste' | 'ethanol' | 'biomass_woody' | 'biomass_dco' | etc.
    is_lipid            BOOLEAN,              -- TRUE if feedstock is fat/oil based
    is_waste_feedstock  BOOLEAN,              -- TRUE if feedstock is UCO/tallow/yellow grease
    classified_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    notes               TEXT
);

COMMENT ON TABLE silver.carb_pathway_dim IS
    'Pathway classification: separates HEFA / AtJ / FT-SPK / CoProcess inside the "Alternative Jet Fuel" + "Renewable Diesel" buckets.';

-- ============================================================================
-- silver.facility_carb_status — per-facility CARB pathway summary
-- ============================================================================
-- One row per (snapshot_date, normalized facility). Used to flag facilities
-- with ZERO active CARB pathways = strong closure-suspect signal.
--
-- "Normalized" means we use the lowercased fuel_producer + facility_name as
-- the key, since CARB doesn't expose a stable facility ID across snapshots.

CREATE OR REPLACE VIEW silver.facility_carb_status AS
WITH latest_snapshot AS (
    SELECT MAX(snapshot_date) AS snapshot_date FROM bronze.carb_lcfs_pathways
)
SELECT
    p.snapshot_date,
    -- Normalized facility key (matches what xref logic uses)
    LOWER(TRIM(p.fuel_producer)) AS norm_fuel_producer,
    LOWER(TRIM(p.facility_name)) AS norm_facility_name,
    MAX(p.fuel_producer) AS fuel_producer,
    MAX(p.facility_name) AS facility_name,
    MAX(p.facility_location) AS facility_location,
    COUNT(*) AS pathway_count,
    COUNT(*) FILTER (WHERE p.fuel_type ILIKE '%biodiesel%') AS bd_pathways,
    COUNT(*) FILTER (WHERE p.fuel_type ILIKE '%renewable diesel%') AS rd_pathways,
    COUNT(*) FILTER (WHERE p.fuel_type ILIKE '%jet%' OR p.fuel_type ILIKE '%saf%' OR p.fuel_type ILIKE '%ajf%') AS saf_pathways,
    -- Feedstock mix (string aggregation)
    STRING_AGG(DISTINCT p.feedstock, '; ' ORDER BY p.feedstock) AS feedstocks_seen,
    -- CI range across pathways
    MIN(p.ci_current) AS ci_min,
    MAX(p.ci_current) AS ci_max,
    AVG(p.ci_current) AS ci_avg,
    -- Most recent certification (proxy for "still active")
    MAX(p.certification_date) AS most_recent_cert_date
FROM bronze.carb_lcfs_pathways p
JOIN latest_snapshot ls ON p.snapshot_date = ls.snapshot_date
GROUP BY p.snapshot_date,
         LOWER(TRIM(p.fuel_producer)),
         LOWER(TRIM(p.facility_name));

COMMENT ON VIEW silver.facility_carb_status IS
    'Latest-snapshot per-facility roll-up of CARB pathways. Use to detect zero-pathway facilities = closure-suspect.';

COMMIT;
