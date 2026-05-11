-- ============================================================================
-- Migration 040: silver.state_monthly_crush — prefer NOPA observed
-- ============================================================================
-- Replaces mig 037's inferred capacity-share computation with NOPA-observed
-- Iowa values when available. Falls back to capacity-share inference only
-- for months/states where NOPA didn't publish.
--
-- This makes silver.ia_implied_monthly_crush a real validator (observed
-- where NOPA published, inferred elsewhere) rather than 100% inferred.
-- ============================================================================

DROP VIEW IF EXISTS silver.ia_implied_monthly_crush CASCADE;
DROP VIEW IF EXISTS silver.state_monthly_crush     CASCADE;

CREATE OR REPLACE VIEW silver.state_monthly_crush AS
WITH national AS (
    -- NASS national monthly crush (TONS → bushels via /60 lb)
    SELECT
        year, month,
        make_date(year, month, 1) AS year_month,
        value                      AS national_crush_tons,
        value * 33.333             AS national_crush_bushels  -- 2000 lb/ton / 60 lb/bu
    FROM bronze.nass_processing
    WHERE commodity_desc = 'SOYBEANS'
      AND statisticcat_desc = 'CRUSHED'
      AND agg_level_desc = 'NATIONAL'
      AND month IS NOT NULL
),
nopa_states AS (
    -- NOPA observed state-level (long format)
    SELECT period_month, region AS state, crush_mil_bu * 1e6 AS observed_crush_bushels
    FROM silver.nopa_regional_crush
),
state_share AS (
    SELECT * FROM silver.crush_capacity_state_share
)
SELECT
    s.state,
    n.year, n.month, n.year_month,
    n.national_crush_tons,
    n.national_crush_bushels,
    s.bpd_share,
    s.mmbu_yr_share,
    -- Observed NOPA where available, else inferred via capacity share
    COALESCE(np.observed_crush_bushels,
             n.national_crush_bushels * s.mmbu_yr_share) AS state_crush_bushels,
    COALESCE(np.observed_crush_bushels / 1e6,
             n.national_crush_bushels * s.mmbu_yr_share / 1e6) AS state_crush_mil_bu,
    s.facility_count,
    (np.observed_crush_bushels IS NULL) AS is_inferred,
    CASE WHEN np.observed_crush_bushels IS NOT NULL THEN 'NOPA_OBSERVED'
         ELSE 'national_x_capacity_share'
    END AS inference_method
FROM national n
CROSS JOIN state_share s
LEFT JOIN nopa_states np
       ON np.period_month = n.year_month
      AND np.state = s.state;

CREATE OR REPLACE VIEW silver.ia_implied_monthly_crush AS
SELECT * FROM silver.state_monthly_crush WHERE state = 'IA';

COMMENT ON VIEW silver.state_monthly_crush IS
'Per-state monthly soybean crush. PREFERS NOPA-observed values when
available, falls back to NATIONAL × capacity-share inference otherwise.
is_inferred flag distinguishes the two. NOPA Iowa data covers Sep 1979
through Nov 2020 currently — refresh from misc_crush_data.xlsx for newer.';

COMMENT ON VIEW silver.ia_implied_monthly_crush IS
'Iowa-specific monthly soybean crush. NOPA-observed for 1979-09 through
2020-11; inferred from capacity-share for months without NOPA data. Use
is_inferred = FALSE filter when ground truth is required.';

GRANT SELECT ON silver.state_monthly_crush         TO PUBLIC;
GRANT SELECT ON silver.ia_implied_monthly_crush    TO PUBLIC;
