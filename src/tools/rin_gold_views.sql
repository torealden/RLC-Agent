-- =============================================================================
-- Gold Matrix Views for RIN & Biofuel Excel Updaters
-- =============================================================================
-- Run this SQL to create the gold views that the VBA updaters query.
-- These views pivot row-per-D-code data into one-row-per-period matrices
-- suitable for direct column mapping in the spreadsheets.
--
-- Dependencies:
--   bronze.epa_rfs_rin_monthly     (monthly RIN generation by D-code)
--   bronze.epa_rfs_generation      (annual generation totals)
--   bronze.epa_rfs_retirement      (annual retirement by reason)
--   bronze.epa_rfs_available       (annual available RINs)
--   bronze.epa_rfs_fuel_production (annual fuel production by category)
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 1. gold.rin_monthly_matrix
--    Monthly RIN generation pivoted by D-code with both RIN qty and batch volume
--    Used by: RINUpdaterSQL.bas -> "RIN Monthly" sheet
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW gold.rin_monthly_matrix AS
SELECT
    rin_year AS year,
    production_month AS month,
    SUM(CASE WHEN d_code = '3' THEN rin_quantity ELSE 0 END) AS d3_rins,
    SUM(CASE WHEN d_code = '4' THEN rin_quantity ELSE 0 END) AS d4_rins,
    SUM(CASE WHEN d_code = '5' THEN rin_quantity ELSE 0 END) AS d5_rins,
    SUM(CASE WHEN d_code = '6' THEN rin_quantity ELSE 0 END) AS d6_rins,
    SUM(CASE WHEN d_code = '7' THEN rin_quantity ELSE 0 END) AS d7_rins,
    SUM(CASE WHEN d_code = '3' THEN batch_volume ELSE 0 END) AS d3_volume,
    SUM(CASE WHEN d_code = '4' THEN batch_volume ELSE 0 END) AS d4_volume,
    SUM(CASE WHEN d_code = '5' THEN batch_volume ELSE 0 END) AS d5_volume,
    SUM(CASE WHEN d_code = '6' THEN batch_volume ELSE 0 END) AS d6_volume,
    SUM(CASE WHEN d_code = '7' THEN batch_volume ELSE 0 END) AS d7_volume
FROM bronze.epa_rfs_rin_monthly
GROUP BY rin_year, production_month
ORDER BY rin_year, production_month;

-- -----------------------------------------------------------------------------
-- 2. gold.rin_annual_balance
--    Combined annual view: generation + retirement + available by D-code
--    Used by: RINUpdaterSQL.bas -> "RIN Balance" sheet
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW gold.rin_annual_balance AS
WITH gen AS (
    SELECT
        rin_year AS year,
        SUM(CASE WHEN d_code = '3' THEN total_rins ELSE 0 END) AS d3_generated,
        SUM(CASE WHEN d_code = '4' THEN total_rins ELSE 0 END) AS d4_generated,
        SUM(CASE WHEN d_code = '5' THEN total_rins ELSE 0 END) AS d5_generated,
        SUM(CASE WHEN d_code = '6' THEN total_rins ELSE 0 END) AS d6_generated,
        SUM(total_rins) AS total_generated
    FROM bronze.epa_rfs_generation
    GROUP BY rin_year
),
ret AS (
    SELECT
        rin_year AS year,
        SUM(CASE WHEN d_code = '3' THEN rin_quantity ELSE 0 END) AS d3_retired,
        SUM(CASE WHEN d_code = '4' THEN rin_quantity ELSE 0 END) AS d4_retired,
        SUM(CASE WHEN d_code = '5' THEN rin_quantity ELSE 0 END) AS d5_retired,
        SUM(CASE WHEN d_code = '6' THEN rin_quantity ELSE 0 END) AS d6_retired,
        SUM(rin_quantity) AS total_retired
    FROM bronze.epa_rfs_retirement
    GROUP BY rin_year
),
avail AS (
    SELECT
        rin_year AS year,
        SUM(CASE WHEN d_code = '3' THEN total_available ELSE 0 END) AS d3_available,
        SUM(CASE WHEN d_code = '4' THEN total_available ELSE 0 END) AS d4_available,
        SUM(CASE WHEN d_code = '5' THEN total_available ELSE 0 END) AS d5_available,
        SUM(CASE WHEN d_code = '6' THEN total_available ELSE 0 END) AS d6_available,
        SUM(total_available) AS total_available
    FROM bronze.epa_rfs_available
    GROUP BY rin_year
)
SELECT
    g.year,
    g.d3_generated, COALESCE(r.d3_retired, 0) AS d3_retired, COALESCE(a.d3_available, 0) AS d3_available,
    g.d4_generated, COALESCE(r.d4_retired, 0) AS d4_retired, COALESCE(a.d4_available, 0) AS d4_available,
    g.d5_generated, COALESCE(r.d5_retired, 0) AS d5_retired, COALESCE(a.d5_available, 0) AS d5_available,
    g.d6_generated, COALESCE(r.d6_retired, 0) AS d6_retired, COALESCE(a.d6_available, 0) AS d6_available,
    g.total_generated, COALESCE(r.total_retired, 0) AS total_retired, COALESCE(a.total_available, 0) AS total_available
FROM gen g
LEFT JOIN ret r ON g.year = r.year
LEFT JOIN avail a ON g.year = a.year
ORDER BY g.year;

-- -----------------------------------------------------------------------------
-- 3. gold.d4_fuel_matrix
--    D4 (BBD) fuel production pivoted by fuel type
--    Used by: RINUpdaterSQL.bas -> "D4 Fuel Mix" sheet
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW gold.d4_fuel_matrix AS
SELECT
    rin_year AS year,
    -- Physical volumes (gallons)
    SUM(CASE WHEN fuel_category_code = '20'  THEN batch_volume ELSE 0 END) AS biodiesel_vol,
    SUM(CASE WHEN fuel_category_code = '40'  THEN batch_volume ELSE 0 END) AS rd_ev17_vol,
    SUM(CASE WHEN fuel_category_code = '41'  THEN batch_volume ELSE 0 END) AS rd_ev16_vol,
    SUM(CASE WHEN fuel_category_code = '141' THEN batch_volume ELSE 0 END) AS ren_jet_vol,
    SUM(CASE WHEN fuel_category_code NOT IN ('20','40','41','141') THEN batch_volume ELSE 0 END) AS other_vol,
    -- RIN quantities
    SUM(CASE WHEN fuel_category_code = '20'  THEN rin_quantity ELSE 0 END) AS biodiesel_rins,
    SUM(CASE WHEN fuel_category_code = '40'  THEN rin_quantity ELSE 0 END) AS rd_ev17_rins,
    SUM(CASE WHEN fuel_category_code = '41'  THEN rin_quantity ELSE 0 END) AS rd_ev16_rins,
    SUM(CASE WHEN fuel_category_code = '141' THEN rin_quantity ELSE 0 END) AS ren_jet_rins,
    SUM(CASE WHEN fuel_category_code NOT IN ('20','40','41','141') THEN rin_quantity ELSE 0 END) AS other_rins
FROM bronze.epa_rfs_fuel_production
WHERE d_code = '4'
GROUP BY rin_year
ORDER BY rin_year;
