-- =============================================================================
-- Migration 081: gold.us_liquid_fuel_stocks_monthly
-- =============================================================================
-- Parallel to gold.us_liquid_fuel_production_monthly. Feeds the Stocks sheet of
-- models/Biofuels/us_liquid_fuel_and_biofuel_production.xlsx.
--
-- Source: bronze.eia_monthly_biofuels, attribute='stocks', region='NUS'.
-- Native unit is MBBL (thousand barrels). Convert to thousand gallons via × 42.
--
-- Column derivation:
--   biodiesel_kgal     = (combined_bd_rd − renewable_diesel) × 42
--                        EIA reports BD+RD as a single combined series; we
--                        subtract the standalone RD series to isolate BD.
--                        Pre-2011-10 RD series is absent → biodiesel = combined.
--   renewable_diesel_kgal = renewable_diesel × 42
--   ethanol_kgal          = fuel_ethanol × 42
--   co_processing_kgal    = NULL  (EIA does not separate)
--   saf_kgal              = NULL
--   diesel/jet/gasoline   = NULL  (would come from petroleum tables, not in v1)
--   naphtha/propane/etc   = NULL  (lumped in "other_biofuels" — not separable)
-- =============================================================================

CREATE OR REPLACE VIEW gold.us_liquid_fuel_stocks_monthly AS
WITH pivot AS (
    SELECT
        EXTRACT(YEAR  FROM period_month)::int  AS year,
        EXTRACT(MONTH FROM period_month)::int  AS month,
        MAX(value) FILTER (WHERE fuel_type = 'combined_bd_rd')   AS combined_bd_rd_mbbl,
        MAX(value) FILTER (WHERE fuel_type = 'renewable_diesel') AS renewable_diesel_mbbl,
        MAX(value) FILTER (WHERE fuel_type = 'fuel_ethanol')     AS fuel_ethanol_mbbl
    FROM bronze.eia_monthly_biofuels
    WHERE attribute = 'stocks'
      AND region    = 'NUS'
    GROUP BY period_month
)
SELECT
    year,
    month,
    make_date(year, month, 1)                                              AS period_date,
    -- BD = combined − RD where both exist; pre-RD era, BD = combined.
    CASE
        WHEN combined_bd_rd_mbbl IS NOT NULL AND renewable_diesel_mbbl IS NOT NULL
            THEN (combined_bd_rd_mbbl - renewable_diesel_mbbl) * 42.0
        WHEN combined_bd_rd_mbbl IS NOT NULL
            THEN combined_bd_rd_mbbl * 42.0
        ELSE NULL
    END                                                                    AS biodiesel_kgal,
    renewable_diesel_mbbl * 42.0                                           AS renewable_diesel_kgal,
    NULL::numeric                                                          AS co_processing_kgal,
    NULL::numeric                                                          AS saf_kgal,
    fuel_ethanol_mbbl * 42.0                                               AS ethanol_kgal,
    NULL::numeric                                                          AS diesel_kgal,
    NULL::numeric                                                          AS jet_fuel_kgal,
    NULL::numeric                                                          AS gasoline_kgal,
    NULL::numeric                                                          AS renewable_naphtha_kgal,
    NULL::numeric                                                          AS renewable_propane_kgal
FROM pivot
ORDER BY year, month;

COMMENT ON VIEW gold.us_liquid_fuel_stocks_monthly IS
'US monthly liquid-fuel stocks in thousand gallons. Mirrors the Stocks sheet of '
'models/Biofuels/us_liquid_fuel_and_biofuel_production.xlsx. Sourced from '
'bronze.eia_monthly_biofuels (MBBL × 42). BD column = combined_bd_rd − renewable_diesel.';
