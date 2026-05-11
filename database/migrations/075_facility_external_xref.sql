-- Migration 075: Generic facility ↔ external-list cross-reference
-- Date: 2026-05-10
--
-- Builds the generic xref infrastructure for matching our internal DB
-- facilities against external lists (CARB LCFS, EPA RFS, EIA, etc).
--
-- Design:
--   silver.facility_norm — normalized name index for our DB facilities
--                          (one row per (facility_id, source_table))
--   silver.external_facility_norm — same shape for external-list facilities
--                          (one row per (external_source, external_id))
--   silver.facility_external_xref — joined view that emits matches with
--                          confidence scores
--
-- The signature trick: a SQL function `silver.normalize_facility_name()` that
-- strips corp suffixes, punctuation, common noise tokens. Used consistently
-- on both sides so substring/token matching is reliable.

BEGIN;

-- ============================================================================
-- silver.normalize_facility_name — single source of truth for normalization
-- ============================================================================
CREATE OR REPLACE FUNCTION silver.normalize_facility_name(name TEXT)
RETURNS TEXT
LANGUAGE plpgsql
IMMUTABLE
AS $$
DECLARE
    n TEXT;
BEGIN
    IF name IS NULL THEN
        RETURN NULL;
    END IF;
    n := LOWER(name);
    -- Strip corporate suffixes (must match whole words)
    n := REGEXP_REPLACE(n, '\m(llc|inc|corp|corporation|company|co|holdings|ltd|lp|llp|plc|gmbh|pte|sa|sas|the)\M', ' ', 'g');
    -- Drop punctuation
    n := REGEXP_REPLACE(n, '[^a-z0-9]+', ' ', 'g');
    -- Collapse whitespace
    n := REGEXP_REPLACE(n, '\s+', ' ', 'g');
    n := TRIM(n);
    RETURN n;
END;
$$;

-- Tokenize: returns a sorted set of meaningful tokens (no noise)
CREATE OR REPLACE FUNCTION silver.facility_tokens(name TEXT)
RETURNS TEXT[]
LANGUAGE plpgsql
IMMUTABLE
AS $$
DECLARE
    norm TEXT;
    tokens TEXT[];
    noise TEXT[] := ARRAY['biodiesel','renewable','diesel','energy','fuels','fuel',
                          'biofuels','biofuel','refining','refinery','rng','saf',
                          'plant','inc','llc','industries','solutions','products',
                          'group','partners','holdings','operations','partnership',
                          'enterprises','technologies'];
BEGIN
    norm := silver.normalize_facility_name(name);
    IF norm IS NULL OR norm = '' THEN
        RETURN ARRAY[]::TEXT[];
    END IF;
    tokens := STRING_TO_ARRAY(norm, ' ');
    -- Filter out noise + short tokens
    SELECT ARRAY_AGG(DISTINCT t ORDER BY t) INTO tokens
    FROM UNNEST(tokens) t
    WHERE LENGTH(t) >= 2 AND NOT (t = ANY(noise));
    RETURN COALESCE(tokens, ARRAY[]::TEXT[]);
END;
$$;

-- ============================================================================
-- silver.facility_norm — normalized index of OUR DB facilities
-- ============================================================================
-- Pulls from every facility table we have. One row per (source_table,
-- facility_id). Refreshed via MATERIALIZED VIEW REFRESH.

DROP MATERIALIZED VIEW IF EXISTS silver.facility_norm CASCADE;

CREATE MATERIALIZED VIEW silver.facility_norm AS
SELECT
    'reference.biodiesel_facilities'::TEXT AS source_table,
    facility_id::TEXT,
    name::TEXT AS facility_name,
    operator::TEXT,
    NULL::TEXT AS parent_company,
    city::TEXT, state::TEXT,
    status::TEXT,
    nameplate_mmgy::NUMERIC AS capacity_mmgy,
    'biodiesel'::TEXT AS fuel_type,
    silver.normalize_facility_name(name) AS norm_name,
    silver.normalize_facility_name(operator) AS norm_operator,
    silver.facility_tokens(COALESCE(name, '') || ' ' || COALESCE(operator, '')) AS tokens
FROM reference.biodiesel_facilities

UNION ALL

SELECT
    'reference.renewable_diesel_facilities'::TEXT,
    facility_id::TEXT,
    name::TEXT, operator::TEXT, parent_company::TEXT,
    city::TEXT, state::TEXT, status::TEXT,
    nameplate_mmgy::NUMERIC,
    'renewable_diesel'::TEXT,
    silver.normalize_facility_name(name),
    silver.normalize_facility_name(operator),
    silver.facility_tokens(COALESCE(name, '') || ' ' || COALESCE(operator, '') || ' ' || COALESCE(parent_company, ''))
FROM reference.renewable_diesel_facilities

UNION ALL

SELECT
    'reference.biofuel_facilities'::TEXT,
    facility_id::TEXT,
    facility_name::TEXT,
    company::TEXT,
    NULL::TEXT,
    city::TEXT, state::TEXT, status::TEXT,
    nameplate_mmgy::NUMERIC,
    fuel_type::TEXT,
    silver.normalize_facility_name(facility_name),
    silver.normalize_facility_name(company),
    silver.facility_tokens(COALESCE(facility_name, '') || ' ' || COALESCE(company, ''))
FROM reference.biofuel_facilities;

CREATE INDEX facility_norm_norm_name_idx ON silver.facility_norm (norm_name);
CREATE INDEX facility_norm_state_idx ON silver.facility_norm (state);
CREATE INDEX facility_norm_tokens_gin_idx ON silver.facility_norm USING GIN (tokens);

COMMENT ON MATERIALIZED VIEW silver.facility_norm IS
    'Normalized index of biofuel facilities across all reference tables. Refresh after schema changes: REFRESH MATERIALIZED VIEW silver.facility_norm;';

-- ============================================================================
-- silver.external_facility_norm — same shape for external-list facilities
-- ============================================================================
DROP MATERIALIZED VIEW IF EXISTS silver.external_facility_norm CASCADE;

CREATE MATERIALIZED VIEW silver.external_facility_norm AS
SELECT
    'CARB_LCFS'::TEXT AS external_source,
    -- Use normalized fuel_producer + facility_name as the stable external_id
    silver.normalize_facility_name(fuel_producer) || '|' ||
        COALESCE(silver.normalize_facility_name(facility_name), '') AS external_id,
    MAX(fuel_producer) AS external_name,
    MAX(facility_name) AS external_facility_name,
    MAX(facility_location) AS external_location,
    -- Latest-snapshot pathway count (the strong signal)
    COUNT(*) AS record_count,
    silver.facility_tokens(MAX(fuel_producer) || ' ' || COALESCE(MAX(facility_name), '')) AS tokens,
    silver.normalize_facility_name(MAX(fuel_producer)) AS norm_producer,
    silver.normalize_facility_name(MAX(facility_name)) AS norm_facility,
    MAX(snapshot_date) AS last_seen_snapshot
FROM bronze.carb_lcfs_pathways
WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM bronze.carb_lcfs_pathways)
GROUP BY silver.normalize_facility_name(fuel_producer),
         COALESCE(silver.normalize_facility_name(facility_name), '');

CREATE INDEX ext_facility_norm_source_idx ON silver.external_facility_norm (external_source);
CREATE INDEX ext_facility_norm_tokens_gin_idx ON silver.external_facility_norm USING GIN (tokens);

-- ============================================================================
-- silver.facility_external_xref — the joined match view
-- ============================================================================
-- For each (our facility, external source) pair, emit a match if there is
-- token overlap. Confidence based on:
--   1.0  = exact normalized-name match
--   0.7  = ≥2 distinct token overlaps + at least one strong-token (≥4 chars)
--   0.4  = single distinct strong-token overlap (≥4 chars)
--   0    = no match
--
-- This is a VIEW (not materialized) so it stays current as the inputs
-- refresh. Cheap because both inputs are small.

DROP VIEW IF EXISTS silver.facility_external_xref CASCADE;

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
        -- Match indicators
        (fn.norm_name = en.norm_producer OR fn.norm_name = en.norm_facility) AS exact_name,
        -- Token overlap
        ARRAY(SELECT UNNEST(fn.tokens) INTERSECT SELECT UNNEST(en.tokens)) AS shared_tokens
    FROM silver.facility_norm fn
    CROSS JOIN silver.external_facility_norm en
    WHERE fn.tokens && en.tokens   -- GIN-index-friendly: any overlap at all
)
SELECT
    source_table, facility_id, facility_name, state, capacity_mmgy, status, fuel_type,
    external_source, external_id, external_name, external_facility_name, external_location,
    record_count,
    shared_tokens,
    CARDINALITY(shared_tokens) AS overlap_count,
    -- Strong tokens = ≥4 chars (city names, surnames, distinctive words)
    (SELECT COUNT(*) FROM UNNEST(shared_tokens) t WHERE LENGTH(t) >= 4) AS strong_overlap_count,
    CASE
        WHEN exact_name THEN 1.0
        WHEN (SELECT COUNT(*) FROM UNNEST(shared_tokens) t WHERE LENGTH(t) >= 4) >= 2 THEN 0.7
        WHEN (SELECT COUNT(*) FROM UNNEST(shared_tokens) t WHERE LENGTH(t) >= 4) >= 1 THEN 0.4
        ELSE 0.0
    END AS match_confidence
FROM joined;

COMMENT ON VIEW silver.facility_external_xref IS
    'For each (our facility, external source) pair with token overlap, emit match with confidence. Filter on match_confidence >= 0.4 for usable matches.';

-- ============================================================================
-- gold.facility_status_anomalies — closure-suspect detector
-- ============================================================================
-- Surface DB facilities marked Operating that have ZERO confident matches
-- in CARB LCFS. These are closure candidates.

CREATE OR REPLACE VIEW gold.facility_status_anomalies AS
WITH carb_matches AS (
    SELECT
        source_table,
        facility_id,
        MAX(match_confidence) AS best_carb_confidence,
        SUM(record_count) FILTER (WHERE match_confidence >= 0.7) AS confident_pathway_count
    FROM silver.facility_external_xref
    WHERE external_source = 'CARB_LCFS'
    GROUP BY source_table, facility_id
),
all_facilities AS (
    SELECT
        fn.source_table,
        fn.facility_id,
        fn.facility_name,
        fn.state,
        fn.capacity_mmgy,
        fn.status,
        fn.fuel_type,
        COALESCE(cm.best_carb_confidence, 0) AS best_carb_confidence,
        COALESCE(cm.confident_pathway_count, 0) AS confident_carb_pathways
    FROM silver.facility_norm fn
    LEFT JOIN carb_matches cm
        ON cm.source_table = fn.source_table
       AND cm.facility_id = fn.facility_id
)
SELECT
    *,
    CASE
        WHEN status IN ('idled', 'closed', 'decommissioned') THEN 'expected_no_carb'
        WHEN capacity_mmgy IS NULL OR capacity_mmgy < 15 THEN 'too_small_for_carb_expected'
        WHEN best_carb_confidence >= 0.7 THEN 'confirmed_active'
        WHEN best_carb_confidence >= 0.4 THEN 'weak_match_needs_review'
        WHEN best_carb_confidence = 0 THEN 'closure_suspect_no_carb'
        ELSE 'unknown'
    END AS anomaly_class
FROM all_facilities;

COMMENT ON VIEW gold.facility_status_anomalies IS
    'Per-facility closure-suspect classification. Filter to anomaly_class = closure_suspect_no_carb to find plants like REG Ralston/Madison.';

COMMIT;
