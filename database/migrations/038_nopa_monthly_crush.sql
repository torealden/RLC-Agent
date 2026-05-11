-- ============================================================================
-- Migration 038: bronze.nopa_monthly_crush + silver views
-- ============================================================================
-- Source: data/raw/oilseeds_fats_greases/misc_crush_data.xlsx (NOPA Crush sheet)
-- Coverage: 524 monthly rows from Sep 1979 to present
--
-- This dataset is the GROUND TRUTH for state-level Iowa monthly soybean crush
-- (the validator the Iowa Crush Agent spec needs but NASS doesn't publish).
-- It also has empirical monthly oil/meal yields per bushel — the input for
-- the trend × seasonal yield projection model that replaces the fixed
-- 11.6 lb/bu assumption.
--
-- NOPA member coverage: ~95% of US soybean crush. Regional groupings:
--   IA, IL, MN_MT_ND_SD, IN_KY_OH_MI, AR_MS_TN_LA_MO, NE_KS_OK_TX_CA_WA_OR_MO,
--   SC_NC_DE_MD_VA_GA_AL_FL, Southwest
-- ============================================================================

-- ----------------------------------------------------------------------------
-- 1. Bronze: wide format mirroring the workbook
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS bronze.nopa_monthly_crush (
    period_month                       DATE PRIMARY KEY,

    -- National rollups
    non_nopa_daily_crush_rate_mbu      NUMERIC(10,4),
    daily_nopa_capacity_mbu            NUMERIC(10,4),
    nopa_capacity_mil_bu               NUMERIC(10,4),
    nopa_crush_mil_bu                  NUMERIC(10,4),
    nopa_crush_pct_of_capacity         NUMERIC(6,4),
    nopa_meal_production_kst           NUMERIC(10,4),
    nopa_meal_yield_lbs_per_bu         NUMERIC(8,4),
    nopa_meal_exports_kst              NUMERIC(10,4),
    nopa_oil_production_mlbs           NUMERIC(10,4),
    nopa_oil_yield_lbs_per_bu          NUMERIC(8,4),
    nopa_oil_stocks_mlbs               NUMERIC(10,4),

    -- Crush by region (mil bu)
    crush_il_mbu                       NUMERIC(10,4),
    crush_in_ky_oh_mi_mbu              NUMERIC(10,4),
    crush_se_mbu                       NUMERIC(10,4),  -- SC/NC/DE/MD/VA/GA/AL/FL
    crush_southwest_mbu                NUMERIC(10,4),
    crush_ar_ms_tn_la_mo_mbu           NUMERIC(10,4),
    crush_ne_ks_ok_tx_ca_wa_or_mo_mbu  NUMERIC(10,4),
    crush_ia_mbu                       NUMERIC(10,4),
    crush_mn_mt_nd_sd_mbu              NUMERIC(10,4),
    crush_total_mbu                    NUMERIC(10,4),

    -- Iowa-specific extras
    change_in_iowa_crush_mbu           NUMERIC(10,4),

    -- Capacity by region (mil bu)
    capacity_il_mbu                    NUMERIC(10,4),
    capacity_in_ky_oh_mi_mbu           NUMERIC(10,4),
    capacity_se_mbu                    NUMERIC(10,4),
    capacity_ar_ms_tn_la_mo_mbu        NUMERIC(10,4),
    capacity_ne_ks_ok_tx_ca_wa_or_mo_mbu NUMERIC(10,4),
    capacity_ia_mbu                    NUMERIC(10,4),
    capacity_mn_mt_nd_sd_mbu           NUMERIC(10,4),
    capacity_total_mbu                 NUMERIC(10,4),

    -- Utilization by region (% / fraction)
    util_il                            NUMERIC(6,4),
    util_in_ky_oh                      NUMERIC(6,4),
    util_se                            NUMERIC(6,4),
    util_ar_ms_tn_la_mo                NUMERIC(6,4),
    util_ne_ks_ok_tx_ca_wa_or_mo       NUMERIC(6,4),
    util_ia                            NUMERIC(6,4),
    util_mn_mt_nd_sd                   NUMERIC(6,4),

    -- Other
    avg_daily_crush_mbu                NUMERIC(10,4),
    monthly_record                     NUMERIC(10,4),

    source_file                        TEXT DEFAULT 'misc_crush_data.xlsx',
    ingested_at                        TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_nopa_crush_period ON bronze.nopa_monthly_crush (period_month);

COMMENT ON TABLE bronze.nopa_monthly_crush IS
'NOPA monthly soybean crush data, including regional breakdowns. Source: misc_crush_data.xlsx (NOPA Crush sheet). NOPA covers ~95% of US soybean crush via member reporting. Iowa column (crush_ia_mbu) is the ground-truth validator for the Iowa Crush Agent spec.';

-- ----------------------------------------------------------------------------
-- 2. Silver: Iowa-specific monthly history
-- ----------------------------------------------------------------------------
CREATE OR REPLACE VIEW silver.nopa_iowa_crush AS
SELECT
    period_month,
    EXTRACT(YEAR  FROM period_month)::INTEGER  AS year,
    EXTRACT(MONTH FROM period_month)::INTEGER  AS month,
    crush_ia_mbu                                AS iowa_crush_mil_bu,
    crush_ia_mbu * 1e6                          AS iowa_crush_bushels,
    capacity_ia_mbu                             AS iowa_capacity_mil_bu,
    util_ia                                     AS iowa_utilization,
    -- IA share of national NOPA
    CASE WHEN crush_total_mbu > 0
         THEN crush_ia_mbu / crush_total_mbu
         ELSE NULL END                          AS ia_share_of_nopa,
    crush_total_mbu                             AS nopa_total_crush_mil_bu,
    nopa_oil_yield_lbs_per_bu,
    nopa_meal_yield_lbs_per_bu,
    'NOPA_REPORTED'                              AS source
FROM bronze.nopa_monthly_crush
WHERE crush_ia_mbu IS NOT NULL;

COMMENT ON VIEW silver.nopa_iowa_crush IS
'Iowa-specific monthly soybean crush from NOPA member reports. This is OBSERVED data — replaces the inferred capacity-share computation in silver.ia_implied_monthly_crush as the canonical validator for the Iowa Crush Agent backtest.';

-- ----------------------------------------------------------------------------
-- 3. Silver: monthly yield history (oil + meal per bu)
-- ----------------------------------------------------------------------------
CREATE OR REPLACE VIEW silver.nopa_yield_history AS
SELECT
    period_month,
    EXTRACT(YEAR  FROM period_month)::INTEGER  AS year,
    EXTRACT(MONTH FROM period_month)::INTEGER  AS month,
    -- Marketing year start (Sep) — used for MY-aggregation
    CASE WHEN EXTRACT(MONTH FROM period_month) >= 9
         THEN EXTRACT(YEAR FROM period_month)::INTEGER
         ELSE EXTRACT(YEAR FROM period_month)::INTEGER - 1
    END AS marketing_year,
    nopa_oil_yield_lbs_per_bu       AS oil_yield_lbs_per_bu,
    nopa_meal_yield_lbs_per_bu      AS meal_yield_lbs_per_bu,
    -- Implied hulls (60 lb/bu - oil - meal)
    60.0 - COALESCE(nopa_oil_yield_lbs_per_bu, 0) - COALESCE(nopa_meal_yield_lbs_per_bu, 0)
        AS implied_other_lbs_per_bu,
    nopa_crush_mil_bu               AS national_crush_mil_bu
FROM bronze.nopa_monthly_crush
WHERE nopa_oil_yield_lbs_per_bu IS NOT NULL
   OR nopa_meal_yield_lbs_per_bu IS NOT NULL;

COMMENT ON VIEW silver.nopa_yield_history IS
'Monthly oil and meal yield per bushel from NOPA member reports. Source for the trend × seasonal yield projection model. marketing_year column groups by Sep-Aug MY for annual analysis.';

-- ----------------------------------------------------------------------------
-- 4. Silver: regional crush long format (for multi-region queries)
-- ----------------------------------------------------------------------------
CREATE OR REPLACE VIEW silver.nopa_regional_crush AS
SELECT period_month, 'IA'                         AS region, crush_ia_mbu                       AS crush_mil_bu, capacity_ia_mbu                       AS capacity_mil_bu, util_ia                       AS utilization FROM bronze.nopa_monthly_crush WHERE crush_ia_mbu IS NOT NULL
UNION ALL
SELECT period_month, 'IL'                         AS region, crush_il_mbu                       AS crush_mil_bu, capacity_il_mbu                       AS capacity_mil_bu, util_il                       AS utilization FROM bronze.nopa_monthly_crush WHERE crush_il_mbu IS NOT NULL
UNION ALL
SELECT period_month, 'IN_KY_OH_MI'                AS region, crush_in_ky_oh_mi_mbu              AS crush_mil_bu, capacity_in_ky_oh_mi_mbu              AS capacity_mil_bu, util_in_ky_oh                 AS utilization FROM bronze.nopa_monthly_crush WHERE crush_in_ky_oh_mi_mbu IS NOT NULL
UNION ALL
SELECT period_month, 'SE'                         AS region, crush_se_mbu                       AS crush_mil_bu, capacity_se_mbu                       AS capacity_mil_bu, util_se                       AS utilization FROM bronze.nopa_monthly_crush WHERE crush_se_mbu IS NOT NULL
UNION ALL
SELECT period_month, 'AR_MS_TN_LA_MO'             AS region, crush_ar_ms_tn_la_mo_mbu           AS crush_mil_bu, capacity_ar_ms_tn_la_mo_mbu           AS capacity_mil_bu, util_ar_ms_tn_la_mo           AS utilization FROM bronze.nopa_monthly_crush WHERE crush_ar_ms_tn_la_mo_mbu IS NOT NULL
UNION ALL
SELECT period_month, 'NE_KS_OK_TX_CA_WA_OR_MO'    AS region, crush_ne_ks_ok_tx_ca_wa_or_mo_mbu  AS crush_mil_bu, capacity_ne_ks_ok_tx_ca_wa_or_mo_mbu  AS capacity_mil_bu, util_ne_ks_ok_tx_ca_wa_or_mo  AS utilization FROM bronze.nopa_monthly_crush WHERE crush_ne_ks_ok_tx_ca_wa_or_mo_mbu IS NOT NULL
UNION ALL
SELECT period_month, 'MN_MT_ND_SD'                AS region, crush_mn_mt_nd_sd_mbu              AS crush_mil_bu, capacity_mn_mt_nd_sd_mbu              AS capacity_mil_bu, util_mn_mt_nd_sd              AS utilization FROM bronze.nopa_monthly_crush WHERE crush_mn_mt_nd_sd_mbu IS NOT NULL;

COMMENT ON VIEW silver.nopa_regional_crush IS
'Long-format regional NOPA crush data. One row per (period_month, region). Easier for cross-regional time-series queries.';

-- ----------------------------------------------------------------------------
-- 5. Bronze: COPA weekly Canadian crush (canola + soybean)
-- ----------------------------------------------------------------------------
-- Source: misc_crush_data.xlsx tab "COPA Weekly Crush"
-- Coverage: weekly from Aug 2009 to Jan 2021 (last 600 rows)
-- COPA = Canadian Oilseed Processors Association
CREATE TABLE IF NOT EXISTS bronze.copa_weekly_crush (
    week_ending                       DATE PRIMARY KEY,

    -- Canola
    canola_weekly_crush_tonnes        NUMERIC(12,2),  -- tonnes per week
    canola_weekly_capacity_util       NUMERIC(6,4),
    canola_implied_annual_capacity    NUMERIC(12,2),  -- tonnes per year

    -- Soybean
    soybean_weekly_crush_tonnes       NUMERIC(12,2),
    soybean_weekly_capacity_util      NUMERIC(6,4),
    soybean_implied_annual_capacity   NUMERIC(12,2),

    source_file                       TEXT DEFAULT 'misc_crush_data.xlsx',
    ingested_at                       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_copa_crush_week ON bronze.copa_weekly_crush (week_ending);

COMMENT ON TABLE bronze.copa_weekly_crush IS
'Canadian Oilseed Processors Association (COPA) weekly canola and soybean crush volumes + estimated capacities. Tonnes/week throughput; tonnes/year for the implied annual capacity. Source: misc_crush_data.xlsx COPA Weekly Crush tab.';

-- ----------------------------------------------------------------------------
-- 6. Silver: COPA convenience views (canola, soybean by year-month)
-- ----------------------------------------------------------------------------
CREATE OR REPLACE VIEW silver.copa_canola_monthly AS
SELECT
    date_trunc('month', week_ending)::DATE  AS period_month,
    SUM(canola_weekly_crush_tonnes)         AS canola_crush_tonnes,
    AVG(canola_weekly_capacity_util)        AS canola_avg_utilization,
    AVG(canola_implied_annual_capacity)     AS canola_implied_annual_capacity
FROM bronze.copa_weekly_crush
WHERE canola_weekly_crush_tonnes IS NOT NULL
GROUP BY date_trunc('month', week_ending);

CREATE OR REPLACE VIEW silver.copa_soybean_monthly AS
SELECT
    date_trunc('month', week_ending)::DATE  AS period_month,
    SUM(soybean_weekly_crush_tonnes)        AS soybean_crush_tonnes,
    AVG(soybean_weekly_capacity_util)       AS soybean_avg_utilization,
    AVG(soybean_implied_annual_capacity)    AS soybean_implied_annual_capacity
FROM bronze.copa_weekly_crush
WHERE soybean_weekly_crush_tonnes IS NOT NULL
GROUP BY date_trunc('month', week_ending);

GRANT SELECT ON bronze.nopa_monthly_crush     TO PUBLIC;
GRANT SELECT ON bronze.copa_weekly_crush      TO PUBLIC;
GRANT SELECT ON silver.nopa_iowa_crush         TO PUBLIC;
GRANT SELECT ON silver.nopa_yield_history      TO PUBLIC;
GRANT SELECT ON silver.nopa_regional_crush     TO PUBLIC;
GRANT SELECT ON silver.copa_canola_monthly     TO PUBLIC;
GRANT SELECT ON silver.copa_soybean_monthly    TO PUBLIC;
