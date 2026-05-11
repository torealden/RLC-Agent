-- Migration 077: Add EPA RFS pathway data as 2nd signal in facility xref
-- Date: 2026-05-11
--
-- bronze.epa_pathway_detail (222 rows) is the EPA RFS equivalent of CARB
-- LCFS pathways — each row is a determination granting RIN-generating
-- authority for a specific company/facility/fuel/feedstock/process combo.
--
-- Adding it as a 2nd cross-reference signal makes our anomaly detection
-- much more precise:
--
--   - "Operating + zero CARB + zero EPA RFS" = STRONG closure-suspect
--      (not certified anywhere = probably not producing)
--   - "Operating + zero CARB + has EPA RFS" = REAL producer, ships
--      non-CA market only (e.g., DGD ships D4 RINs nationwide but only
--      some pathways are CARB-certified)
--   - "Operating + has CARB + has EPA RFS" = CONFIRMED ACTIVE
--   - "Operating + has CARB + zero EPA RFS" = unusual — investigate
--      (foreign producer? Pathway type EPA hasn't approved yet?)

BEGIN;

-- ============================================================================
-- Extend silver.external_facility_norm to include EPA RFS pathways
-- ============================================================================
-- Drop & recreate (matview can't be modified in place)
DROP MATERIALIZED VIEW IF EXISTS silver.external_facility_norm CASCADE;

CREATE MATERIALIZED VIEW silver.external_facility_norm AS

-- CARB LCFS (unchanged)
SELECT
    'CARB_LCFS'::TEXT AS external_source,
    silver.normalize_facility_name(fuel_producer) || '|' ||
        COALESCE(silver.normalize_facility_name(facility_name), '') AS external_id,
    MAX(fuel_producer) AS external_name,
    MAX(facility_name) AS external_facility_name,
    MAX(facility_location) AS external_location,
    COUNT(*) AS record_count,
    silver.facility_tokens(MAX(fuel_producer) || ' ' || COALESCE(MAX(facility_name), '')) AS tokens,
    silver.normalize_facility_name(MAX(fuel_producer)) AS norm_producer,
    silver.normalize_facility_name(MAX(facility_name)) AS norm_facility,
    MAX(snapshot_date) AS last_seen_snapshot
FROM bronze.carb_lcfs_pathways
WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM bronze.carb_lcfs_pathways)
GROUP BY silver.normalize_facility_name(fuel_producer),
         COALESCE(silver.normalize_facility_name(facility_name), '')

UNION ALL

-- EPA RFS approved pathways (new signal)
SELECT
    'EPA_RFS'::TEXT AS external_source,
    silver.normalize_facility_name(company_name) || '|' ||
        COALESCE(silver.normalize_facility_name(facility_name), '') AS external_id,
    MAX(company_name) AS external_name,
    MAX(facility_name) AS external_facility_name,
    MAX(facility_city) || ', ' || MAX(facility_state) AS external_location,
    COUNT(*) AS record_count,
    silver.facility_tokens(MAX(company_name) || ' ' || COALESCE(MAX(facility_name), '')) AS tokens,
    silver.normalize_facility_name(MAX(company_name)) AS norm_producer,
    silver.normalize_facility_name(MAX(facility_name)) AS norm_facility,
    -- EPA pathways don't have a snapshot concept yet — use the latest parsed_at
    MAX(parsed_at)::DATE AS last_seen_snapshot
FROM bronze.epa_pathway_detail
WHERE company_name IS NOT NULL AND company_name <> ''
GROUP BY silver.normalize_facility_name(company_name),
         COALESCE(silver.normalize_facility_name(facility_name), '');

CREATE INDEX ext_facility_norm_source_idx ON silver.external_facility_norm (external_source);
CREATE INDEX ext_facility_norm_tokens_gin_idx ON silver.external_facility_norm USING GIN (tokens);

COMMENT ON MATERIALIZED VIEW silver.external_facility_norm IS
    'External facility lists for cross-reference. Sources: CARB_LCFS, EPA_RFS. Refresh after bronze data updates.';

-- ============================================================================
-- Recreate the xref view (dependent on external_facility_norm)
-- ============================================================================
CREATE VIEW silver.facility_external_xref AS
WITH joined AS (
    SELECT
        fn.source_table,
        fn.facility_id,
        fn.facility_name,
        fn.state,
        fn.capacity_mmgy,
        fn.status,
        fn.fuel_type,
        en.external_source,
        en.external_id,
        en.external_name,
        en.external_facility_name,
        en.external_location,
        en.record_count,
        (fn.norm_name = en.norm_producer OR fn.norm_name = en.norm_facility) AS exact_name,
        ARRAY(SELECT UNNEST(fn.tokens) INTERSECT SELECT UNNEST(en.tokens)) AS shared_tokens
    FROM silver.facility_norm fn
    CROSS JOIN silver.external_facility_norm en
    WHERE fn.tokens && en.tokens
)
SELECT
    source_table, facility_id, facility_name, state, capacity_mmgy, status, fuel_type,
    external_source, external_id, external_name, external_facility_name, external_location,
    record_count,
    shared_tokens,
    CARDINALITY(shared_tokens) AS overlap_count,
    (SELECT COUNT(*) FROM UNNEST(shared_tokens) t WHERE LENGTH(t) >= 4) AS strong_overlap_count,
    CASE
        WHEN exact_name THEN 1.0
        WHEN (SELECT COUNT(*) FROM UNNEST(shared_tokens) t WHERE LENGTH(t) >= 4) >= 2 THEN 0.7
        WHEN (SELECT COUNT(*) FROM UNNEST(shared_tokens) t WHERE LENGTH(t) >= 4) >= 1 THEN 0.4
        ELSE 0.0
    END AS match_confidence
FROM joined;

-- ============================================================================
-- Upgrade gold.facility_status_anomalies to combine BOTH signals
-- ============================================================================
DROP VIEW IF EXISTS gold.facility_status_anomalies;

CREATE VIEW gold.facility_status_anomalies AS
WITH carb_matches AS (
    SELECT source_table, facility_id,
           MAX(match_confidence) AS best_conf,
           SUM(record_count) FILTER (WHERE match_confidence >= 0.7) AS confident_count
    FROM silver.facility_external_xref WHERE external_source = 'CARB_LCFS'
    GROUP BY source_table, facility_id
),
epa_matches AS (
    SELECT source_table, facility_id,
           MAX(match_confidence) AS best_conf,
           SUM(record_count) FILTER (WHERE match_confidence >= 0.7) AS confident_count
    FROM silver.facility_external_xref WHERE external_source = 'EPA_RFS'
    GROUP BY source_table, facility_id
),
all_facilities AS (
    SELECT
        fn.source_table, fn.facility_id, fn.facility_name, fn.state,
        fn.capacity_mmgy, fn.status, fn.fuel_type,
        COALESCE(cm.best_conf, 0) AS carb_confidence,
        COALESCE(cm.confident_count, 0) AS carb_pathway_count,
        COALESCE(em.best_conf, 0) AS epa_confidence,
        COALESCE(em.confident_count, 0) AS epa_pathway_count
    FROM silver.facility_norm fn
    LEFT JOIN carb_matches cm ON cm.source_table = fn.source_table AND cm.facility_id = fn.facility_id
    LEFT JOIN epa_matches em ON em.source_table = fn.source_table AND em.facility_id = fn.facility_id
)
SELECT
    *,
    (CASE WHEN carb_confidence >= 0.7 THEN 1 ELSE 0 END +
     CASE WHEN epa_confidence >= 0.7 THEN 1 ELSE 0 END) AS confirmed_source_count,
    CASE
        WHEN status IN ('idled', 'closed', 'decommissioned') THEN 'expected_no_signals'
        WHEN capacity_mmgy IS NULL OR capacity_mmgy < 15 THEN 'too_small_for_signals_expected'
        -- Best signals: confirmed by BOTH CARB and EPA = top-tier
        WHEN carb_confidence >= 0.7 AND epa_confidence >= 0.7 THEN 'confirmed_active_both'
        -- Has CARB only: ships to CA but may not be RIN-generating, or EPA data parse miss
        WHEN carb_confidence >= 0.7 AND epa_confidence < 0.7 THEN 'confirmed_carb_only'
        -- Has EPA only: real producer that doesn't ship CA market
        WHEN carb_confidence < 0.7 AND epa_confidence >= 0.7 THEN 'confirmed_epa_only_nonCA'
        -- Weak match on either
        WHEN carb_confidence >= 0.4 OR epa_confidence >= 0.4 THEN 'weak_match_needs_review'
        -- Zero signals from both = strong closure-suspect
        ELSE 'closure_suspect_no_signals'
    END AS anomaly_class
FROM all_facilities;

COMMENT ON VIEW gold.facility_status_anomalies IS
    'Per-facility classification using CARB + EPA RFS as independent signals. closure_suspect_no_signals = highest-confidence "this plant probably isn''t running" flag.';

COMMIT;
