-- =============================================================================
-- Migration 091: Fill SAF + co-processing estimates in stocks + domestic use views
-- =============================================================================
-- Gaps to fill:
--   SAF domestic use        = prod + imports − exports (mass balance, apparent)
--   SAF stocks              = SAF use × 1.0 month (industry-typical for mandate-
--                              driven immature fuel; RD currently runs ~1 month)
--   Co-processing dom use   = production (refinery-integrated, no separable trade)
--   Co-processing stocks    = 0 (refinery-integrated, no separable inventory)
--
-- Production data sources (already in views):
--   EMTS production_category in ('saf', 'co_processing')
--
-- SAF trade flows: from gold.saf_trade_candidates (mig 090), price-threshold
-- view. Volumes are small (~290k gal imports 2024, ~80k gal exports) but
-- captured cleanly.
-- =============================================================================

-- ─── gold.us_liquid_fuel_domestic_use_monthly ───
CREATE OR REPLACE VIEW gold.us_liquid_fuel_domestic_use_monthly AS
WITH bd AS (
    SELECT period_month,
        MAX(value) FILTER (WHERE attribute='production') AS prod_mbbl,
        MAX(value) FILTER (WHERE attribute='imports')    AS imp_mbbl,
        MAX(value) FILTER (WHERE attribute='exports')    AS exp_mbbl
    FROM bronze.eia_monthly_biofuels
    WHERE fuel_type='biodiesel' AND region='NUS' GROUP BY period_month
),
rd AS (
    SELECT period_month,
        MAX(value) FILTER (WHERE attribute='production') AS prod_mbbl,
        MAX(value) FILTER (WHERE attribute='imports')    AS imp_mbbl
    FROM bronze.eia_monthly_biofuels
    WHERE fuel_type='renewable_diesel' AND region='NUS' GROUP BY period_month
),
eth AS (
    SELECT period_month, value AS blend_in_mbbl
    FROM bronze.eia_monthly_biofuels
    WHERE fuel_type='fuel_ethanol' AND attribute='blender_input' AND region='NUS'
),
saf_prod AS (
    -- SAF production from EMTS (gallons → MBBL via /42000)
    SELECT make_date(year, month, 1) AS period_month, volume_gal / 42000.0 AS saf_prod_mbbl
    FROM silver.emts_production_canonical
    WHERE production_category = 'saf'
),
saf_trade AS (
    -- SAF imports - exports in MBBL
    SELECT period_date AS period_month,
           SUM(quantity_gal) FILTER (WHERE flow='imports') / 42000.0 AS saf_imp_mbbl,
           SUM(quantity_gal) FILTER (WHERE flow='exports') / 42000.0 AS saf_exp_mbbl
    FROM gold.saf_trade_candidates
    GROUP BY period_date
),
coproc_prod AS (
    SELECT make_date(year, month, 1) AS period_month, volume_gal / 42000.0 AS coproc_prod_mbbl
    FROM silver.emts_production_canonical
    WHERE production_category = 'co_processing'
),
all_months AS (
    SELECT period_month FROM bd
    UNION SELECT period_month FROM rd
    UNION SELECT period_month FROM eth
    UNION SELECT period_month FROM saf_prod
    UNION SELECT period_month FROM coproc_prod
)
SELECT
    EXTRACT(YEAR  FROM x.period_month)::int AS year,
    EXTRACT(MONTH FROM x.period_month)::int AS month,
    x.period_month                          AS period_date,
    -- BD apparent: prod + imp − exp (mass balance)
    CASE WHEN bd.prod_mbbl IS NOT NULL THEN
        (COALESCE(bd.prod_mbbl,0) + COALESCE(bd.imp_mbbl,0) - COALESCE(bd.exp_mbbl,0)) * 42.0
    ELSE NULL END                                                       AS biodiesel_kgal,
    -- RD apparent: prod + imp (no exports series)
    CASE WHEN rd.prod_mbbl IS NOT NULL THEN
        (COALESCE(rd.prod_mbbl,0) + COALESCE(rd.imp_mbbl,0)) * 42.0
    ELSE NULL END                                                       AS renewable_diesel_kgal,
    -- Co-processing: prod = use (refinery-integrated)
    coproc_prod.coproc_prod_mbbl * 42.0                                 AS co_processing_kgal,
    -- SAF: prod + imp − exp (mass balance, small trade flows)
    CASE WHEN saf_prod.saf_prod_mbbl IS NOT NULL THEN
        (COALESCE(saf_prod.saf_prod_mbbl, 0)
          + COALESCE(saf_trade.saf_imp_mbbl, 0)
          - COALESCE(saf_trade.saf_exp_mbbl, 0)) * 42.0
    ELSE NULL END                                                       AS saf_kgal,
    eth.blend_in_mbbl * 42.0                                            AS ethanol_kgal,
    NULL::numeric AS diesel_kgal,
    NULL::numeric AS jet_fuel_kgal,
    NULL::numeric AS gasoline_kgal,
    NULL::numeric AS renewable_naphtha_kgal,
    NULL::numeric AS renewable_propane_kgal
FROM all_months x
LEFT JOIN bd          ON bd.period_month          = x.period_month
LEFT JOIN rd          ON rd.period_month          = x.period_month
LEFT JOIN eth         ON eth.period_month         = x.period_month
LEFT JOIN saf_prod    ON saf_prod.period_month    = x.period_month
LEFT JOIN saf_trade   ON saf_trade.period_month   = x.period_month
LEFT JOIN coproc_prod ON coproc_prod.period_month = x.period_month
ORDER BY x.period_month;

COMMENT ON VIEW gold.us_liquid_fuel_domestic_use_monthly IS
'US monthly liquid-fuel domestic use in thousand gallons. Apparent consumption '
'per fuel: BD/RD/SAF = prod + imp − exp (where data exists). Co-processing = '
'production (refinery-integrated). Ethanol = blender_input direct (MFERIUS1).';

-- ─── gold.us_liquid_fuel_stocks_monthly ───
CREATE OR REPLACE VIEW gold.us_liquid_fuel_stocks_monthly AS
WITH pivot AS (
    SELECT
        EXTRACT(YEAR  FROM period_month)::int  AS year,
        EXTRACT(MONTH FROM period_month)::int  AS month,
        MAX(value) FILTER (WHERE fuel_type='combined_bd_rd')   AS combined_bd_rd_mbbl,
        MAX(value) FILTER (WHERE fuel_type='renewable_diesel') AS renewable_diesel_mbbl,
        MAX(value) FILTER (WHERE fuel_type='fuel_ethanol')     AS fuel_ethanol_mbbl
    FROM bronze.eia_monthly_biofuels
    WHERE attribute='stocks' AND region='NUS'
    GROUP BY period_month
),
saf_use_mbbl AS (
    -- SAF apparent use in MBBL (rebuilt here rather than joining the domestic_use view to keep stocks view independent)
    SELECT
        EXTRACT(YEAR  FROM ds.period_month)::int  AS year,
        EXTRACT(MONTH FROM ds.period_month)::int  AS month,
        (COALESCE(sp.saf_prod_mbbl, 0)
          + COALESCE(st.saf_imp_mbbl, 0)
          - COALESCE(st.saf_exp_mbbl, 0)) AS use_mbbl
    FROM (SELECT DISTINCT make_date(year, month, 1) AS period_month
          FROM silver.emts_production_canonical
          WHERE production_category='saf') ds
    LEFT JOIN (
        SELECT make_date(year, month, 1) AS period_month,
               volume_gal / 42000.0 AS saf_prod_mbbl
        FROM silver.emts_production_canonical
        WHERE production_category='saf'
    ) sp ON sp.period_month = ds.period_month
    LEFT JOIN (
        SELECT period_date AS period_month,
               SUM(quantity_gal) FILTER (WHERE flow='imports')/42000.0 AS saf_imp_mbbl,
               SUM(quantity_gal) FILTER (WHERE flow='exports')/42000.0 AS saf_exp_mbbl
        FROM gold.saf_trade_candidates GROUP BY period_date
    ) st ON st.period_month = ds.period_month
)
SELECT
    p.year, p.month, make_date(p.year, p.month, 1) AS period_date,
    CASE
        WHEN p.combined_bd_rd_mbbl IS NOT NULL AND p.renewable_diesel_mbbl IS NOT NULL
            THEN (p.combined_bd_rd_mbbl - p.renewable_diesel_mbbl) * 42.0
        WHEN p.combined_bd_rd_mbbl IS NOT NULL THEN p.combined_bd_rd_mbbl * 42.0
        ELSE NULL
    END                                                                 AS biodiesel_kgal,
    p.renewable_diesel_mbbl * 42.0                                      AS renewable_diesel_kgal,
    0::numeric                                                          AS co_processing_kgal,  -- refinery-integrated
    -- SAF stocks estimate: 1.0 months of apparent use (industry-typical for
    -- mandate-driven immature fuel; RD currently runs ~1 month stocks/use).
    -- Heuristic placeholder until EIA publishes SAF stocks directly.
    s.use_mbbl * 1.0 * 42.0                                             AS saf_kgal,
    p.fuel_ethanol_mbbl * 42.0                                          AS ethanol_kgal,
    NULL::numeric AS diesel_kgal,
    NULL::numeric AS jet_fuel_kgal,
    NULL::numeric AS gasoline_kgal,
    NULL::numeric AS renewable_naphtha_kgal,
    NULL::numeric AS renewable_propane_kgal
FROM pivot p
LEFT JOIN saf_use_mbbl s ON s.year = p.year AND s.month = p.month
ORDER BY p.year, p.month;

COMMENT ON VIEW gold.us_liquid_fuel_stocks_monthly IS
'US monthly liquid-fuel stocks in thousand gallons. BD/RD from combined−RD '
'split of EIA. Ethanol direct from EIA. Co-processing = 0 (refinery-integrated). '
'SAF = apparent use × 1.0 month heuristic until EIA publishes SAF stocks.';
