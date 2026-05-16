-- =============================================================================
-- Migration 090: gold.saf_trade_candidates
-- =============================================================================
-- Identifies likely SAF cargoes in Census trade data by unit price threshold.
-- SAF historically prices $6-15/gal — well above BD ($3.50-5) and bulk petroleum
-- jet kerosene ($2.50-3.50). High-priced shipments under HS 3826 and HS
-- 2710.19.11 are SAF candidates.
--
-- Thresholds (all must hold):
--   HS 3826:        $/gal in ($6, $25)   AND quantity_gal < 300,000
--   HS 2710.19.11:  $/gal in ($5, $20)   AND quantity_gal < 300,000
--   HS 2710.20.x:   $/gal in ($8, $25)   AND quantity_gal < 300,000
--
-- Upper cap of $25/gal excludes specialty/sample cargoes (Korea 2710.20.25
-- at $310/gal is clearly not bulk SAF — Argus Northwest Europe SAF was
-- $14-22/gal in 2024).
--
-- The volume cap is critical. Without it, bulk BD shipments during high-price
-- periods (2022 Ukraine-war inflation, avg BD = $6.69/gal) falsely trigger.
-- Real SAF cargoes in 2024 are <50k gal per country-month. Cap at 300k allows
-- growth headroom as SAF capacity expands.
--
-- Volumes are expected to be small (~150k gal/yr in HS 3826 alone, growing as
-- SAF capacity expands). Country detail preserved for rail-tracker integration:
-- the goal is to tie observed SAF imports to specific producer facilities at
-- origin (Belgium → MPC SAF? Finland → Neste Porvoo?).
--
-- All quantity calcs use 0.301 gal/kg (empirical — Census quantity in kg despite
-- reference table labeling LT).
-- =============================================================================

DROP VIEW IF EXISTS gold.saf_trade_candidates;

CREATE VIEW gold.saf_trade_candidates AS
WITH base AS (
    SELECT
        ct.id, ct.year, ct.month, ct.flow, ct.hs_code,
        ct.country_code,
        COALESCE(tcr.country_name, ct.country_name) AS country_name,
        tcr.region,
        tcr.spreadsheet_row,
        COALESCE(tcr.is_regional_total, false)      AS is_regional_total,
        ct.quantity                                  AS quantity_kg,
        ct.value_usd,
        ct.quantity * 0.301                          AS quantity_gal,
        ct.value_usd / NULLIF(ct.quantity * 0.301, 0) AS usd_per_gal
    FROM bronze.census_trade ct
    LEFT JOIN silver.trade_country_reference tcr
        ON UPPER(ct.country_name::text) = UPPER(tcr.country_name::text)
        OR UPPER(ct.country_name::text) = UPPER(tcr.country_name_alt::text)
    WHERE ct.country_code <> '-'
      AND ct.quantity > 0
)
SELECT
    year, month,
    make_date(year, month, 1)            AS period_date,
    flow,
    hs_code,
    country_code,
    country_name,
    region,
    spreadsheet_row,
    quantity_kg,
    quantity_gal,
    value_usd,
    usd_per_gal,
    CASE
        WHEN hs_code LIKE '3826%'     AND usd_per_gal BETWEEN 6.0 AND 25.0 AND quantity_gal < 300000 THEN 'SAF_3826_premium'
        WHEN hs_code = '2710191100'   AND usd_per_gal BETWEEN 5.0 AND 20.0 AND quantity_gal < 300000 THEN 'SAF_jet_premium'
        WHEN hs_code LIKE '271020%'   AND usd_per_gal BETWEEN 8.0 AND 25.0 AND quantity_gal < 300000 THEN 'SAF_blend_premium'
    END                                  AS saf_signal
FROM base
WHERE COALESCE(is_regional_total, false) = false
  AND quantity_gal < 300000
  AND (
       (hs_code LIKE '3826%'    AND usd_per_gal BETWEEN 6.0 AND 25.0)
    OR (hs_code = '2710191100'  AND usd_per_gal BETWEEN 5.0 AND 20.0)
    OR (hs_code LIKE '271020%'  AND usd_per_gal BETWEEN 8.0 AND 25.0)
  );

COMMENT ON VIEW gold.saf_trade_candidates IS
'SAF cargoes identified by unit-price premium over baseline BD/jet fuel. '
'Thresholds: HS 3826 >$6/gal, HS 2710.19.11 >$5/gal, HS 2710.20 >$8/gal. '
'Country detail preserved for rail-tracker integration. Expected volumes small '
'(~150k gal/yr from 3826 alone in 2024, growing).';
