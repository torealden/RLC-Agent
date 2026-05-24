-- Migration 107: FAS ESR country lookup + gold.export_sales_matrix
--
-- Problem this fixes:
--   bronze.fas_export_sales has country_code populated (FAS's numeric codes
--   like 1220, 2010, 4120) but country name is empty — the v2 ESR endpoint
--   doesn't return countryDescription. So joins to silver.trade_country_reference
--   (which uses Census-style country names) yield zero matches.
--
-- Fix:
--   1. New table reference.fas_esr_country — code -> name lookup. Populated
--      from FAS /api/esr/countries via the loader script. 211 countries.
--   2. New view gold.export_sales_matrix — joins bronze.fas_export_sales
--      against the new ref table for country names, then against
--      silver.trade_country_reference for spreadsheet_row mapping. Exposes
--      sales/shipments/commitments columns so the .bas can fill any of the
--      four tabs (Sales, Shipments, Commitments, NMY Sales) from one view.
--   3. Commitments = net_sales + weekly_exports per cell (Tore's spec).

CREATE SCHEMA IF NOT EXISTS reference;

CREATE TABLE IF NOT EXISTS reference.fas_esr_country (
    country_code      INTEGER PRIMARY KEY,
    country_name      TEXT NOT NULL,        -- clean trimmed long form
    short_name        TEXT,                 -- 7-char "countryName" field
    region_id         INTEGER,
    genc_code         TEXT,                 -- 3-letter ISO-ish where present
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_fas_esr_country_name
    ON reference.fas_esr_country(UPPER(country_name));

-- The matrix view.
-- One row per (commodity, country, week_ending, marketing_year) with
-- columns for each cell-value the four tabs need.
CREATE OR REPLACE VIEW gold.export_sales_matrix AS
WITH named AS (
    SELECT
        fes.commodity,
        fes.commodity_code,
        fes.country_code,
        c.country_name              AS fas_country_name,
        fes.marketing_year,
        fes.week_ending,
        fes.unit,
        fes.weekly_exports,
        fes.accumulated_exports,
        fes.outstanding_sales,
        fes.gross_new_sales,
        fes.net_sales,
        fes.prev_my_accumulated
    FROM bronze.fas_export_sales fes
    LEFT JOIN reference.fas_esr_country c ON c.country_code = fes.country_code::INTEGER
)
SELECT
    n.commodity,
    n.commodity_code,
    n.country_code,
    cr.country_name           AS country_name,         -- canonical Census-style name
    n.fas_country_name        AS fas_country_name,     -- FAS-style for debugging
    cr.region,
    cr.region_sort_order,
    cr.country_sort_order,
    cr.spreadsheet_row,
    cr.is_regional_total,
    n.marketing_year,
    n.week_ending,
    EXTRACT(YEAR FROM n.week_ending)::INT  AS year,
    EXTRACT(MONTH FROM n.week_ending)::INT AS month,
    n.unit,
    -- The four cell values
    n.net_sales                                                   AS sales,
    n.weekly_exports                                              AS shipments,
    COALESCE(n.net_sales, 0) + COALESCE(n.weekly_exports, 0)      AS commitments,
    -- NMY sales: same as sales but the .bas filters by marketing_year > current
    n.net_sales                                                   AS nmy_sales,
    -- Cumulative columns also exposed for reference
    n.outstanding_sales,
    n.accumulated_exports,
    n.gross_new_sales,
    n.prev_my_accumulated
FROM named n
LEFT JOIN silver.trade_country_reference cr
    ON UPPER(TRIM(cr.country_name)) = UPPER(TRIM(n.fas_country_name))
       OR UPPER(TRIM(cr.country_name_alt)) = UPPER(TRIM(n.fas_country_name))
WHERE cr.is_active = TRUE OR cr.is_active IS NULL;
