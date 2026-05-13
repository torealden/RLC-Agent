-- =============================================================================
-- Migration 085: gold.us_liquid_fuel_domestic_use_monthly
-- =============================================================================
-- Third sibling view to gold.us_liquid_fuel_production_monthly and
-- gold.us_liquid_fuel_stocks_monthly. Feeds the "Domestic Use" sheet of
-- models/Biofuels/us_liquid_fuel_and_biofuel_production.xlsx.
--
-- Approach: apparent consumption per fuel = production + imports − exports.
-- Reason: combined_bd_rd / blender_input is only refinery/blender NET INPUT,
-- which excludes direct-to-end-user volumes (e.g., RD trucked to CA LCFS
-- fleets without going through a refinery blender). Combined blender_input
-- was 1,290 MBBL in Dec 2025 while RD production alone was 6,076 MBBL —
-- conclusive evidence that combined_blender_input is NOT total disappearance.
--
-- Per-fuel apparent consumption ignores monthly stock changes (small relative
-- to flows) and is the standard balance-sheet approach when stocks aren't
-- cleanly attributable.
--
-- Column derivation:
--   biodiesel_kgal    = (BD production + BD imports − BD exports) × 42
--                       (clean series, 2011+)
--   renewable_diesel_kgal = (RD production + RD imports) × 42
--                       (RD exports not published by EIA; assumed negligible
--                       — most US RD stays domestic under CA LCFS demand)
--                       Production starts 2021; pre-2021 the value is NULL
--                       because EIA lumped RD into the BD series.
--   ethanol_kgal      = fuel_ethanol blender_input × 42  (MFERIUS1, 1993+)
--                       Direct measure; the alternative "consumption" series
--                       (M_EPOOXE_VPP_NUS_MBBL) returns 0 for recent months
--                       so it's unreliable.
--   co_processing / saf / diesel / jet / gasoline / naphtha / propane = NULL
-- =============================================================================

CREATE OR REPLACE VIEW gold.us_liquid_fuel_domestic_use_monthly AS
WITH bd AS (
    SELECT
        period_month,
        MAX(value) FILTER (WHERE attribute = 'production') AS prod_mbbl,
        MAX(value) FILTER (WHERE attribute = 'imports')    AS imp_mbbl,
        MAX(value) FILTER (WHERE attribute = 'exports')    AS exp_mbbl
    FROM bronze.eia_monthly_biofuels
    WHERE fuel_type = 'biodiesel' AND region = 'NUS'
    GROUP BY period_month
),
rd AS (
    SELECT
        period_month,
        MAX(value) FILTER (WHERE attribute = 'production') AS prod_mbbl,
        MAX(value) FILTER (WHERE attribute = 'imports')    AS imp_mbbl
    FROM bronze.eia_monthly_biofuels
    WHERE fuel_type = 'renewable_diesel' AND region = 'NUS'
    GROUP BY period_month
),
eth AS (
    SELECT period_month, value AS blend_in_mbbl
    FROM bronze.eia_monthly_biofuels
    WHERE fuel_type = 'fuel_ethanol' AND attribute = 'blender_input' AND region = 'NUS'
),
all_months AS (
    SELECT period_month FROM bd
    UNION SELECT period_month FROM rd
    UNION SELECT period_month FROM eth
)
SELECT
    EXTRACT(YEAR  FROM x.period_month)::int AS year,
    EXTRACT(MONTH FROM x.period_month)::int AS month,
    x.period_month                          AS period_date,
    -- BD apparent consumption: prod + imp − exp; NULL if production missing.
    CASE
        WHEN bd.prod_mbbl IS NOT NULL
        THEN (COALESCE(bd.prod_mbbl, 0)
              + COALESCE(bd.imp_mbbl, 0)
              - COALESCE(bd.exp_mbbl, 0)) * 42.0
        ELSE NULL
    END                                     AS biodiesel_kgal,
    -- RD apparent consumption: prod + imp (no exports series, assume 0).
    -- Requires production (post-2021); pre-2021 returns NULL.
    CASE
        WHEN rd.prod_mbbl IS NOT NULL
        THEN (COALESCE(rd.prod_mbbl, 0)
              + COALESCE(rd.imp_mbbl, 0)) * 42.0
        ELSE NULL
    END                                     AS renewable_diesel_kgal,
    NULL::numeric                           AS co_processing_kgal,
    NULL::numeric                           AS saf_kgal,
    eth.blend_in_mbbl * 42.0                AS ethanol_kgal,
    NULL::numeric                           AS diesel_kgal,
    NULL::numeric                           AS jet_fuel_kgal,
    NULL::numeric                           AS gasoline_kgal,
    NULL::numeric                           AS renewable_naphtha_kgal,
    NULL::numeric                           AS renewable_propane_kgal
FROM all_months x
LEFT JOIN bd  ON bd.period_month  = x.period_month
LEFT JOIN rd  ON rd.period_month  = x.period_month
LEFT JOIN eth ON eth.period_month = x.period_month
ORDER BY x.period_month;

COMMENT ON VIEW gold.us_liquid_fuel_domestic_use_monthly IS
'US monthly liquid-fuel domestic use in thousand gallons. Apparent consumption '
'per fuel: BD = prod + imp − exp; RD = prod + imp (no exports series); '
'ethanol = blender_input (MFERIUS1, direct). Stock changes ignored (small '
'relative to flows). Pre-2021 RD column is NULL because EIA did not publish '
'RD production separately before then.';
