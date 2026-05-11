-- Migration 078: Operator-alias dictionary + extend matcher to use it
-- Date: 2026-05-11
--
-- Problem discovered while wiring EPA RFS as 2nd signal (mig 077):
--   - EPA stores "Renewable Energy Group, Inc." for REG plants
--   - Our DB stores "REG Geismar, LLC"
--   - CARB stores "REG Geismar, LLC"
--   - The tokens [reg, geismar] vs [renewable, energy, group] (after noise
--     filtering produces empty set) share zero overlap → no match
--
-- Fix: a curated operator-alias dictionary that EXPANDS tokens on both
-- sides to include canonical forms. The token set for "Renewable Energy
-- Group, Inc." becomes {reg, renewable, energy, group}.
--
-- This is curated knowledge, not algorithmic — common-sense facts about
-- which operators have multiple naming forms. Update as we encounter
-- new misses.

BEGIN;

-- ============================================================================
-- silver.operator_alias — canonical ↔ alias mapping
-- ============================================================================
CREATE TABLE IF NOT EXISTS silver.operator_alias (
    canonical_token TEXT NOT NULL,    -- the short/distinctive token (e.g., 'reg')
    alias_phrase TEXT NOT NULL,        -- the long form (e.g., 'renewable energy group')
    notes TEXT,
    PRIMARY KEY (canonical_token, alias_phrase)
);

INSERT INTO silver.operator_alias (canonical_token, alias_phrase, notes) VALUES
    -- REG = Renewable Energy Group
    ('reg', 'renewable energy group', 'REG biodiesel/RD operator (acquired by Chevron 2022)'),
    ('reg', 'chevron renewable energy group', 'Post-Chevron acquisition name form'),
    -- AGP = Ag Processing Inc
    ('agp', 'ag processing', 'AGP cooperative (oilseed crush + biodiesel)'),
    ('agp', 'ag processing inc', 'Full corp name form'),
    -- CVR (Coffeyville)
    ('cvr', 'cvr energy', 'CVR Energy — Coffeyville + Wynnewood refineries'),
    ('cvr', 'cvr renewables wyn', 'CVR subsidiary at Wynnewood OK'),
    -- DGD = Diamond Green Diesel
    ('dgd', 'diamond green diesel', 'Valero/Darling JV'),
    -- ADM
    ('adm', 'archer daniels midland', 'ADM full corp name'),
    -- P66 = Phillips 66
    ('p66', 'phillips 66', 'P66 — refining + biofuels'),
    -- Marathon = MPC = Marathon Petroleum
    ('marathon', 'marathon petroleum', 'Marathon Petroleum Corporation'),
    ('mpc', 'marathon petroleum', 'MPC ticker alias'),
    -- HF = Holly Frontier = HollyFrontier = HF Sinclair
    ('hollyfrontier', 'hf sinclair', 'Post-2022 name change'),
    ('holly', 'hf sinclair', 'Short form'),
    -- Bunge
    ('bunge', 'bunge north america', 'Bunge subsidiary form'),
    ('bunge', 'bunge global', 'Bunge global form'),
    -- BP
    ('bp', 'british petroleum', 'BP former name'),
    -- WIE
    ('wie', 'western iowa energy', 'WIE biodiesel'),
    -- IRE
    ('ire', 'iowa renewable energy', 'IRE Washington IA'),
    -- Seaboard
    ('seaboard', 'seaboard energy', 'Seaboard Foods biodiesel arm'),
    ('hpb', 'high plains bioenergy', 'Pre-rename to Seaboard Energy'),
    -- World Energy
    ('world', 'world energy', 'World Energy LLC'),
    -- Neste
    ('neste', 'neste oyj', 'Neste full corp name'),
    ('neste', 'neste corporation', 'Neste alt form'),
    -- Cargill
    ('cargill', 'cargill incorporated', 'Cargill full corp name'),
    ('cargill', 'cargill biodiesel', 'Cargill biodiesel subsidiary'),
    -- LanzaJet (AtJ leader)
    ('lanzajet', 'lanza jet', 'LanzaJet space variations'),
    -- AltAir / World Energy Paramount (same facility under different operator)
    ('altair', 'alt air', 'AltAir Paramount space variation'),
    ('altair', 'world energy paramount', 'Same physical facility');

-- ============================================================================
-- Update silver.facility_tokens to expand via aliases
-- ============================================================================
CREATE OR REPLACE FUNCTION silver.facility_tokens(name TEXT)
RETURNS TEXT[]
LANGUAGE plpgsql
IMMUTABLE
AS $$
DECLARE
    norm TEXT;
    base_tokens TEXT[];
    expanded_tokens TEXT[];
    noise TEXT[] := ARRAY['biodiesel','renewable','diesel','fuels','fuel',
                          'biofuels','biofuel','refining','refinery','rng','saf',
                          'plant','inc','llc','industries','solutions','products',
                          'partners','holdings','operations','partnership',
                          'enterprises','technologies'];
    -- NOTE: REMOVED 'energy' and 'group' from noise — they are
    -- distinguishing tokens for operators like Renewable Energy Group,
    -- CVR Energy, World Energy, ENI etc. Alias expansion below catches
    -- the long↔short mappings.
    -- 'company' kept as noise; 'corp' kept as suffix
BEGIN
    norm := silver.normalize_facility_name(name);
    IF norm IS NULL OR norm = '' THEN
        RETURN ARRAY[]::TEXT[];
    END IF;
    base_tokens := STRING_TO_ARRAY(norm, ' ');
    -- Filter out noise + short tokens
    SELECT ARRAY_AGG(DISTINCT t ORDER BY t) INTO base_tokens
    FROM UNNEST(base_tokens) t
    WHERE LENGTH(t) >= 2 AND NOT (t = ANY(noise));
    base_tokens := COALESCE(base_tokens, ARRAY[]::TEXT[]);

    -- Expand via aliases: for each alias_phrase contained in `norm`,
    -- add the canonical_token. And for each canonical_token in base_tokens,
    -- add the constituent tokens of its alias phrases.
    WITH alias_hits AS (
        -- Long-form → canonical (e.g., "renewable energy group" → "reg")
        SELECT canonical_token AS expanded
        FROM silver.operator_alias
        WHERE norm LIKE '%' || alias_phrase || '%'
        UNION
        -- Canonical → constituent tokens (e.g., "reg" → "renewable", "energy", "group")
        SELECT UNNEST(STRING_TO_ARRAY(alias_phrase, ' ')) AS expanded
        FROM silver.operator_alias
        WHERE canonical_token = ANY(base_tokens)
    )
    SELECT ARRAY_AGG(DISTINCT t ORDER BY t) INTO expanded_tokens
    FROM (
        SELECT UNNEST(base_tokens) AS t
        UNION
        SELECT expanded AS t FROM alias_hits
    ) all_t
    WHERE LENGTH(t) >= 2 AND NOT (t = ANY(noise));

    RETURN COALESCE(expanded_tokens, base_tokens);
END;
$$;

-- ============================================================================
-- Refresh the matviews so the new tokenizer takes effect
-- ============================================================================
-- silver.facility_norm and silver.external_facility_norm both call
-- silver.facility_tokens(), so they need a hard rebuild.
DROP MATERIALIZED VIEW IF EXISTS silver.external_facility_norm CASCADE;
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
    facility_id::TEXT, name::TEXT, operator::TEXT, parent_company::TEXT,
    city::TEXT, state::TEXT, status::TEXT,
    nameplate_mmgy::NUMERIC, 'renewable_diesel'::TEXT,
    silver.normalize_facility_name(name),
    silver.normalize_facility_name(operator),
    silver.facility_tokens(COALESCE(name, '') || ' ' || COALESCE(operator, '') || ' ' || COALESCE(parent_company, ''))
FROM reference.renewable_diesel_facilities
UNION ALL
SELECT
    'reference.biofuel_facilities'::TEXT,
    facility_id::TEXT, facility_name::TEXT, company::TEXT, NULL::TEXT,
    city::TEXT, state::TEXT, status::TEXT,
    nameplate_mmgy::NUMERIC, fuel_type::TEXT,
    silver.normalize_facility_name(facility_name),
    silver.normalize_facility_name(company),
    silver.facility_tokens(COALESCE(facility_name, '') || ' ' || COALESCE(company, ''))
FROM reference.biofuel_facilities;

CREATE INDEX facility_norm_norm_name_idx ON silver.facility_norm (norm_name);
CREATE INDEX facility_norm_state_idx ON silver.facility_norm (state);
CREATE INDEX facility_norm_tokens_gin_idx ON silver.facility_norm USING GIN (tokens);

CREATE MATERIALIZED VIEW silver.external_facility_norm AS
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
    MAX(parsed_at)::DATE AS last_seen_snapshot
FROM bronze.epa_pathway_detail
WHERE company_name IS NOT NULL AND company_name <> ''
GROUP BY silver.normalize_facility_name(company_name),
         COALESCE(silver.normalize_facility_name(facility_name), '');

CREATE INDEX ext_facility_norm_source_idx ON silver.external_facility_norm (external_source);
CREATE INDEX ext_facility_norm_tokens_gin_idx ON silver.external_facility_norm USING GIN (tokens);

-- Recreate xref + anomaly views (CASCADE'd)
CREATE VIEW silver.facility_external_xref AS
WITH joined AS (
    SELECT
        fn.source_table, fn.facility_id, fn.facility_name, fn.state,
        fn.capacity_mmgy, fn.status, fn.fuel_type,
        en.external_source, en.external_id, en.external_name,
        en.external_facility_name, en.external_location, en.record_count,
        (fn.norm_name = en.norm_producer OR fn.norm_name = en.norm_facility) AS exact_name,
        ARRAY(SELECT UNNEST(fn.tokens) INTERSECT SELECT UNNEST(en.tokens)) AS shared_tokens
    FROM silver.facility_norm fn
    CROSS JOIN silver.external_facility_norm en
    WHERE fn.tokens && en.tokens
)
SELECT
    source_table, facility_id, facility_name, state, capacity_mmgy, status, fuel_type,
    external_source, external_id, external_name, external_facility_name, external_location,
    record_count, shared_tokens,
    CARDINALITY(shared_tokens) AS overlap_count,
    (SELECT COUNT(*) FROM UNNEST(shared_tokens) t WHERE LENGTH(t) >= 4) AS strong_overlap_count,
    CASE
        WHEN exact_name THEN 1.0
        WHEN (SELECT COUNT(*) FROM UNNEST(shared_tokens) t WHERE LENGTH(t) >= 4) >= 2 THEN 0.7
        WHEN (SELECT COUNT(*) FROM UNNEST(shared_tokens) t WHERE LENGTH(t) >= 4) >= 1 THEN 0.4
        ELSE 0.0
    END AS match_confidence
FROM joined;

CREATE OR REPLACE VIEW gold.facility_status_anomalies AS
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
    SELECT fn.source_table, fn.facility_id, fn.facility_name, fn.state,
        fn.capacity_mmgy, fn.status, fn.fuel_type,
        COALESCE(cm.best_conf, 0) AS carb_confidence,
        COALESCE(cm.confident_count, 0) AS carb_pathway_count,
        COALESCE(em.best_conf, 0) AS epa_confidence,
        COALESCE(em.confident_count, 0) AS epa_pathway_count
    FROM silver.facility_norm fn
    LEFT JOIN carb_matches cm ON cm.source_table = fn.source_table AND cm.facility_id = fn.facility_id
    LEFT JOIN epa_matches em ON em.source_table = fn.source_table AND em.facility_id = fn.facility_id
)
SELECT *,
    (CASE WHEN carb_confidence >= 0.7 THEN 1 ELSE 0 END +
     CASE WHEN epa_confidence >= 0.7 THEN 1 ELSE 0 END) AS confirmed_source_count,
    CASE
        WHEN status IN ('idled', 'closed', 'decommissioned') THEN 'expected_no_signals'
        WHEN capacity_mmgy IS NULL OR capacity_mmgy < 15 THEN 'too_small_for_signals_expected'
        WHEN carb_confidence >= 0.7 AND epa_confidence >= 0.7 THEN 'confirmed_active_both'
        WHEN carb_confidence >= 0.7 AND epa_confidence < 0.7 THEN 'confirmed_carb_only'
        WHEN carb_confidence < 0.7 AND epa_confidence >= 0.7 THEN 'confirmed_epa_only_nonCA'
        WHEN carb_confidence >= 0.4 OR epa_confidence >= 0.4 THEN 'weak_match_needs_review'
        ELSE 'closure_suspect_no_signals'
    END AS anomaly_class
FROM all_facilities;

-- Recreate gold.feedstock_allocation_by_padd (depended on facility_norm? actually depended on biofuel_facilities directly, leave alone)

COMMIT;
