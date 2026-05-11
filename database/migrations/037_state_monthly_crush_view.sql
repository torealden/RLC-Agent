-- ============================================================================
-- Migration 037: silver.state_monthly_crush + ia_implied_monthly_crush
-- ============================================================================
-- KNOWN LIMITATION (discovered 2026-04-26):
--   NASS does NOT publish state-level monthly soybean crush data. The
--   "Oilseed Crushings, Production, Consumption and Stocks" report is
--   NATIONAL only. NOPA monthly is also national (member-reported).
--   The Iowa-specific monthly soybean crush referenced in the Iowa Crush
--   Agent spec §11 (validation) does NOT exist as an observed series.
--
-- WORKAROUND:
--   silver.state_monthly_crush exposes the NATIONAL crush observed by NASS
--   alongside an INFERRED Iowa share computed from capacity-weighted
--   reference.oilseed_crush_facilities. The implied IA monthly crush is
--   marked is_inferred=true so the agent system never confuses it with
--   ground truth.
--
-- Future improvements:
--   - Build USDA ERS state allocation from Oil Crops Outlook annual
--     state production × processing rate
--   - Sum NASS state Form X data (when published) — currently unavailable
--   - Solicit Iowa Soybean Association industry survey data (paid)
-- ============================================================================

-- ----------------------------------------------------------------------------
-- 1. Capacity-weighted state share (refresh-as-needed)
-- ----------------------------------------------------------------------------
CREATE OR REPLACE VIEW silver.crush_capacity_state_share AS
SELECT
    state,
    SUM(nameplate_bpd)            AS total_bpd,
    SUM(nameplate_mmbu_yr)        AS total_mmbu_yr,
    SUM(nameplate_bpd) /
      NULLIF(SUM(SUM(nameplate_bpd)) OVER (), 0)  AS bpd_share,
    SUM(nameplate_mmbu_yr) /
      NULLIF(SUM(SUM(nameplate_mmbu_yr)) OVER (), 0)  AS mmbu_yr_share,
    COUNT(*)                       AS facility_count
FROM reference.oilseed_crush_facilities
WHERE primary_oilseed = 'soybean'
  AND status = 'Operating'
GROUP BY state;

COMMENT ON VIEW silver.crush_capacity_state_share IS
'Each state''s share of US soybean crush capacity. Used to infer state-level monthly crush from national NASS/NOPA data when state-level monthly is not directly published.';

-- ----------------------------------------------------------------------------
-- 2. National monthly soybean crush + inferred state breakdown
-- ----------------------------------------------------------------------------
CREATE OR REPLACE VIEW silver.state_monthly_crush AS
WITH national AS (
    SELECT
        year,
        month,
        make_date(year, month, 1) AS year_month,
        value                      AS national_crush_tons,
        value * 33.0               AS national_crush_bushels,  -- 1 ton = 33.33 bu approx
        source
    FROM bronze.nass_processing
    WHERE commodity_desc = 'SOYBEANS'
      AND statisticcat_desc = 'CRUSHED'
      AND agg_level_desc = 'NATIONAL'
      AND month IS NOT NULL
)
SELECT
    s.state,
    n.year,
    n.month,
    n.year_month,
    n.national_crush_tons,
    n.national_crush_bushels,
    s.bpd_share,
    s.mmbu_yr_share,
    n.national_crush_bushels * s.mmbu_yr_share  AS implied_state_crush_bushels,
    n.national_crush_tons    * s.mmbu_yr_share  AS implied_state_crush_tons,
    s.facility_count,
    TRUE                                          AS is_inferred,
    'national_x_capacity_share'                   AS inference_method,
    n.source
FROM national n
CROSS JOIN silver.crush_capacity_state_share s;

COMMENT ON VIEW silver.state_monthly_crush IS
'Per-state monthly soybean crush. NATIONAL columns are observed (NASS). State-level columns (implied_state_crush_*) are INFERRED from capacity share — is_inferred=TRUE flags this. Used as a placeholder validator for the Iowa Crush Agent system until a real state-level data source is identified.';

-- ----------------------------------------------------------------------------
-- 3. Iowa-specific convenience view
-- ----------------------------------------------------------------------------
CREATE OR REPLACE VIEW silver.ia_implied_monthly_crush AS
SELECT * FROM silver.state_monthly_crush WHERE state = 'IA';

COMMENT ON VIEW silver.ia_implied_monthly_crush IS
'Iowa-specific implied monthly soybean crush. WARNING: implied_state_crush_* values are INFERRED from Iowa''s capacity share of total US, not observed Iowa-specific NASS data (which does not exist). Do not present as ground truth in published reports.';

-- ----------------------------------------------------------------------------
-- Permissions
-- ----------------------------------------------------------------------------
GRANT SELECT ON silver.crush_capacity_state_share TO PUBLIC;
GRANT SELECT ON silver.state_monthly_crush         TO PUBLIC;
GRANT SELECT ON silver.ia_implied_monthly_crush    TO PUBLIC;
