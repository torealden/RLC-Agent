-- ============================================================================
-- FGIS GRAIN EXPORT INSPECTIONS — HISTORICAL CERTIFICATE-LEVEL DATA
-- Round Lakes Commodities
-- ============================================================================
-- File: 032_fgis_inspections_history.sql
-- Purpose: Bronze table for individual FGIS inspection certificates loaded
--          from CY{year}.csv files (1990-present), plus silver aggregation
--          views and gold views matching Census export trade format.
-- Depends: 001_schema_foundation.sql (bronze, silver, gold schemas)
-- ============================================================================

-- ============================================================================
-- BRONZE LAYER — Individual inspection certificates
-- ============================================================================
-- One row per certificate. ~25k rows/year, ~900k total for 36 years.
-- Serial No. is unique within a calendar year file.

CREATE TABLE IF NOT EXISTS bronze.fgis_inspections_history (
    id                  SERIAL PRIMARY KEY,

    -- Certificate identification
    cert_date           DATE NOT NULL,
    serial_no           VARCHAR(20) NOT NULL,
    calendar_year       INTEGER NOT NULL,       -- from filename CY{year}

    -- Inspection type
    type_service        VARCHAR(10),            -- IW, I, PS, W
    type_shipment       VARCHAR(10),            -- BU (bulk), BA (bag), CO (container)
    type_carrier        VARCHAR(5),             -- 1=vessel, 2=rail, 6=truck, etc.

    -- Grain identification
    grain               VARCHAR(50) NOT NULL,   -- CORN, SOYBEANS, WHEAT, etc.
    grain_class         VARCHAR(20),            -- YC, YSB, HRW, SRW, HRS, DUR, etc.
    grain_subclass      VARCHAR(20),
    grade               VARCHAR(20),            -- 1, 2, 2 O/B, US SG, etc.

    -- Destination & port
    destination         VARCHAR(100) NOT NULL,
    port_region         VARCHAR(50),            -- MISSISSIPPI R., COLUMBIA R., INTERIOR, etc.
    port_name           VARCHAR(50),            -- Field Office name
    ams_region          VARCHAR(20),            -- GULF, PACIFIC, INTERIOR, etc.
    fgis_region         VARCHAR(30),
    city                VARCHAR(50),
    state               VARCHAR(10),

    -- Marketing year
    marketing_year      VARCHAR(10),            -- e.g. "2526" = 2025/26

    -- Volumes
    metric_tons         NUMERIC(14,2),
    bushels_1000        NUMERIC(14,3),
    pounds              NUMERIC(16,0),

    -- Quality (key fields only)
    test_weight         NUMERIC(8,2),
    moisture_avg        NUMERIC(6,2),
    damaged_kernels     NUMERIC(6,2),
    foreign_material    NUMERIC(6,2),

    -- Metadata
    carrier_name        VARCHAR(100),
    collected_at        TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Primary uniqueness: serial_no is unique within a calendar year
CREATE UNIQUE INDEX IF NOT EXISTS idx_fgis_hist_uq
    ON bronze.fgis_inspections_history (calendar_year, serial_no);

-- Query indexes
CREATE INDEX IF NOT EXISTS idx_fgis_hist_cert_date
    ON bronze.fgis_inspections_history (cert_date);
CREATE INDEX IF NOT EXISTS idx_fgis_hist_grain
    ON bronze.fgis_inspections_history (grain, cert_date);
CREATE INDEX IF NOT EXISTS idx_fgis_hist_dest
    ON bronze.fgis_inspections_history (destination, cert_date);
CREATE INDEX IF NOT EXISTS idx_fgis_hist_my
    ON bronze.fgis_inspections_history (marketing_year, grain);
CREATE INDEX IF NOT EXISTS idx_fgis_hist_type_serv
    ON bronze.fgis_inspections_history (type_service);


-- ============================================================================
-- SILVER LAYER — Weekly aggregation (FGIS weeks end Thursday)
-- ============================================================================

CREATE OR REPLACE VIEW silver.fgis_inspections_weekly AS
SELECT
    -- FGIS week ends Thursday: find the next Thursday on or after cert_date
    (cert_date + ((4 - EXTRACT(ISODOW FROM cert_date)::int + 7) % 7)::int)::date AS week_ending,
    LOWER(grain) AS grain,
    grain_class,
    destination,
    marketing_year,
    port_region,
    ams_region,
    SUM(metric_tons)    AS metric_tons,
    SUM(bushels_1000)   AS bushels_1000,
    COUNT(*)            AS certificate_count
FROM bronze.fgis_inspections_history
WHERE type_service IN ('IW', 'I')       -- export inspections only
GROUP BY 1, 2, 3, 4, 5, 6, 7;


-- ============================================================================
-- SILVER LAYER — Monthly aggregation
-- ============================================================================

CREATE OR REPLACE VIEW silver.fgis_inspections_monthly AS
SELECT
    EXTRACT(YEAR FROM cert_date)::int   AS year,
    EXTRACT(MONTH FROM cert_date)::int  AS month,
    LOWER(grain) AS grain,
    grain_class,
    destination,
    marketing_year,
    SUM(metric_tons)    AS metric_tons,
    SUM(bushels_1000)   AS bushels_1000,
    COUNT(*)            AS certificate_count
FROM bronze.fgis_inspections_history
WHERE type_service IN ('IW', 'I')
GROUP BY 1, 2, 3, 4, 5, 6;


-- ============================================================================
-- GOLD LAYER — Monthly by destination (Census-comparable format)
-- ============================================================================

CREATE OR REPLACE VIEW gold.fgis_monthly_by_destination AS
SELECT
    EXTRACT(YEAR FROM cert_date)::int   AS year,
    EXTRACT(MONTH FROM cert_date)::int  AS month,
    LOWER(grain) AS grain,
    grain_class,
    destination,
    SUM(metric_tons)            AS metric_tons,
    SUM(bushels_1000)           AS bushels_1000,
    COUNT(*)                    AS shipments
FROM bronze.fgis_inspections_history
WHERE type_service IN ('IW', 'I')
GROUP BY 1, 2, 3, 4, 5
ORDER BY 1 DESC, 2 DESC, grain, metric_tons DESC;


-- ============================================================================
-- GOLD LAYER — Weekly by destination
-- ============================================================================

CREATE OR REPLACE VIEW gold.fgis_weekly_by_destination AS
SELECT
    (cert_date + ((4 - EXTRACT(ISODOW FROM cert_date)::int + 7) % 7)::int)::date AS week_ending,
    LOWER(grain) AS grain,
    grain_class,
    destination,
    port_region,
    SUM(metric_tons)    AS metric_tons,
    SUM(bushels_1000)   AS bushels_1000,
    COUNT(*)            AS shipments
FROM bronze.fgis_inspections_history
WHERE type_service IN ('IW', 'I')
GROUP BY 1, 2, 3, 4, 5
ORDER BY 1 DESC, grain, metric_tons DESC;


-- ============================================================================
-- GOLD LAYER — Marketing year pace by destination
-- ============================================================================
-- Cumulative metric tons by marketing year for pace-vs-USDA tracking.
-- Marketing year encoded as "2526" = 2025/26 MY.

CREATE OR REPLACE VIEW gold.fgis_marketing_year_pace AS
WITH weekly AS (
    SELECT
        (cert_date + ((4 - EXTRACT(ISODOW FROM cert_date)::int + 7) % 7)::int)::date AS week_ending,
        LOWER(grain)     AS grain,
        marketing_year,
        destination,
        SUM(metric_tons) AS weekly_mt
    FROM bronze.fgis_inspections_history
    WHERE type_service IN ('IW', 'I')
      AND marketing_year IS NOT NULL
      AND marketing_year != ''
    GROUP BY 1, 2, 3, 4
)
SELECT
    grain,
    marketing_year,
    destination,
    week_ending,
    weekly_mt,
    SUM(weekly_mt) OVER (
        PARTITION BY grain, marketing_year, destination
        ORDER BY week_ending
    ) AS cumulative_mt
FROM weekly
ORDER BY grain, marketing_year DESC, destination, week_ending;


-- ============================================================================
-- GOLD LAYER — Marketing year pace totals (all destinations)
-- ============================================================================

CREATE OR REPLACE VIEW gold.fgis_marketing_year_pace_total AS
WITH weekly AS (
    SELECT
        (cert_date + ((4 - EXTRACT(ISODOW FROM cert_date)::int + 7) % 7)::int)::date AS week_ending,
        LOWER(grain)     AS grain,
        marketing_year,
        SUM(metric_tons) AS weekly_mt
    FROM bronze.fgis_inspections_history
    WHERE type_service IN ('IW', 'I')
      AND marketing_year IS NOT NULL
      AND marketing_year != ''
    GROUP BY 1, 2, 3
)
SELECT
    grain,
    marketing_year,
    week_ending,
    weekly_mt,
    SUM(weekly_mt) OVER (
        PARTITION BY grain, marketing_year
        ORDER BY week_ending
    ) AS cumulative_mt
FROM weekly
ORDER BY grain, marketing_year DESC, week_ending;


-- ============================================================================
-- GRANTS
-- ============================================================================

GRANT SELECT ON bronze.fgis_inspections_history TO PUBLIC;
GRANT SELECT ON silver.fgis_inspections_weekly TO PUBLIC;
GRANT SELECT ON silver.fgis_inspections_monthly TO PUBLIC;
GRANT SELECT ON gold.fgis_monthly_by_destination TO PUBLIC;
GRANT SELECT ON gold.fgis_weekly_by_destination TO PUBLIC;
GRANT SELECT ON gold.fgis_marketing_year_pace TO PUBLIC;
GRANT SELECT ON gold.fgis_marketing_year_pace_total TO PUBLIC;
