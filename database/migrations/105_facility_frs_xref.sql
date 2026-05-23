-- Migration 105: silver.facility_frs_xref
--
-- Phase 1 of the EPA ECHO architecture flip (Task #66).
-- Maps every silver.facility_map row to its FRS Registry ID so a new
-- enrichment collector can hit ECHO directly by FRS ID instead of doing
-- daily SIC sweeps that return ~1,600 false positives per profile.
--
-- Population paths:
--   1. discovered_epa rows in facility_map already use frs_registry_id as
--      facility_id — trivial self-match, confidence 1.00.
--   2. curated rows match against bronze.epa_echo_facility on state + city
--      (or city-in-name) + name-token overlap. City is a hard requirement
--      because parent companies (AGP, Cargill, Big River) run multiple
--      facilities in the same state and name-only matching crosses them.
--
-- Confidence tiers (curated rows):
--   1.00 — exact city match + >=2 shared name tokens
--   0.80 — exact city match + 1 shared name token (e.g., AGP <-> AG PROCESSING)
--   0.60 — facility_map city appears as substring in ECHO city OR vice versa
--          (handles 'Cedar Rapids' vs 'CEDAR RAPIDS NE')
--          + name overlap
--   <0.60 not stored (forces manual review for these)
--
-- This sits ALONGSIDE silver.facility_map (existing view stays unchanged).
-- New gold view facility_map_with_frs LEFT JOINs the xref.

CREATE TABLE IF NOT EXISTS silver.facility_frs_xref (
    facility_id       TEXT PRIMARY KEY,
    frs_registry_id   TEXT NOT NULL,
    match_method      TEXT NOT NULL,   -- 'self', 'city_exact', 'city_substring', 'manual'
    match_confidence  NUMERIC(3,2) NOT NULL,
    overlap_count     INTEGER,
    shared_tokens     TEXT[],
    facility_map_city TEXT,
    echo_city         TEXT,
    notes             TEXT,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_facility_frs_xref_frs
    ON silver.facility_frs_xref(frs_registry_id);
CREATE INDEX IF NOT EXISTS idx_facility_frs_xref_method
    ON silver.facility_frs_xref(match_method);

-- ----- Path 1: discovered EPA rows self-match -----
INSERT INTO silver.facility_frs_xref
    (facility_id, frs_registry_id, match_method, match_confidence, notes)
SELECT
    fm.facility_id,
    fm.facility_id AS frs_registry_id,
    'self' AS match_method,
    1.00 AS match_confidence,
    'discovered_epa row; facility_id already the FRS' AS notes
FROM silver.facility_map fm
WHERE fm.source_table = 'gold.facility_capacity'
  AND fm.facility_id ~ '^[0-9]+$'
ON CONFLICT (facility_id) DO NOTHING;

-- ----- Path 2: curated rows, city-anchored token match -----
WITH curated AS (
    SELECT
        fm.facility_id,
        fm.name,
        fm.state,
        LOWER(REGEXP_REPLACE(COALESCE(fm.city, ''), '[^a-zA-Z0-9 ]', ' ', 'g')) AS clean_city,
        LOWER(REGEXP_REPLACE(COALESCE(fm.name, ''), '[^a-zA-Z0-9 ]', ' ', 'g')) AS clean_name,
        REGEXP_SPLIT_TO_ARRAY(
            LOWER(REGEXP_REPLACE(COALESCE(fm.name, ''), '[^a-zA-Z0-9 ]', ' ', 'g')),
            '\s+'
        ) AS name_tokens
    FROM silver.facility_map fm
    WHERE fm.source_table IN ('reference.facility_master', 'reference.oilseed_crush_facilities')
      AND fm.name IS NOT NULL AND fm.state IS NOT NULL
      AND NOT EXISTS (
          SELECT 1 FROM silver.facility_frs_xref x WHERE x.facility_id = fm.facility_id
      )
),
echo AS (
    SELECT
        frs_registry_id,
        facility_name,
        state,
        LOWER(REGEXP_REPLACE(COALESCE(city, ''), '[^a-zA-Z0-9 ]', ' ', 'g')) AS clean_city,
        LOWER(REGEXP_REPLACE(COALESCE(facility_name, ''), '[^a-zA-Z0-9 ]', ' ', 'g')) AS clean_name,
        REGEXP_SPLIT_TO_ARRAY(
            LOWER(REGEXP_REPLACE(COALESCE(facility_name, ''), '[^a-zA-Z0-9 ]', ' ', 'g')),
            '\s+'
        ) AS name_tokens
    FROM bronze.epa_echo_facility
    WHERE frs_registry_id IS NOT NULL
      AND facility_name IS NOT NULL
      AND state IS NOT NULL
),
joined AS (
    SELECT
        c.facility_id,
        c.name AS facility_map_name,
        c.state,
        c.clean_city AS facility_map_city,
        e.frs_registry_id,
        e.facility_name AS external_name,
        e.clean_city AS echo_city,
        -- city match flags
        (c.clean_city <> '' AND c.clean_city = e.clean_city) AS city_exact,
        (c.clean_city <> '' AND e.clean_city <> '' AND (
            position(c.clean_city in e.clean_city) > 0
            OR position(e.clean_city in c.clean_city) > 0
        )) AS city_substring,
        -- Name-token overlap (excluding stopwords AND tokens that appear in
        -- either facility's city — sharing 'cedar' and 'rapids' across two
        -- different Cedar Rapids plants is not a brand-level signal).
        ARRAY(
            SELECT t FROM unnest(c.name_tokens) t
            WHERE t = ANY(e.name_tokens)
              AND length(t) >= 3
              AND t NOT IN ('inc','llc','ltd','corp','company','plant','plants',
                            'mill','mills','the','and','for','from','north',
                            'south','east','west','america')
              -- Drop city tokens from either side
              AND NOT (c.clean_city LIKE '%' || t || '%')
              AND NOT (e.clean_city LIKE '%' || t || '%')
        ) AS shared_tokens
    FROM curated c
    JOIN echo e ON c.state = e.state
),
scored AS (
    SELECT
        facility_id,
        facility_map_name,
        state,
        facility_map_city,
        frs_registry_id,
        external_name,
        echo_city,
        shared_tokens,
        COALESCE(array_length(shared_tokens, 1), 0) AS overlap_count,
        city_exact,
        city_substring,
        -- Count how many EPA candidates share this exact city — if there's
        -- only one, then a city-alone match is unambiguous; if there are
        -- multiple, we need brand-token overlap to disambiguate.
        COUNT(*) FILTER (WHERE city_exact)
            OVER (PARTITION BY facility_id) AS city_exact_candidates,
        CASE
            -- City exact + 2+ name tokens (strong brand+location signal)
            WHEN city_exact AND COALESCE(array_length(shared_tokens, 1), 0) >= 2 THEN 1.00
            -- City exact + 1 name token (still a real brand signal)
            WHEN city_exact AND COALESCE(array_length(shared_tokens, 1), 0) >= 1 THEN 0.80
            -- (Dropped: city-only matching at 0.60 produced cross-industry
            --  false positives — e.g., Iowa Premium Beef vs Tama Ethanol,
            --  both unique in their cities but different industries entirely.
            --  Auto-match requires brand-token overlap; pure city matches
            --  go to manual review.)
            -- City substring + 2+ name tokens (handles 'Cedar Rapids' vs 'CEDAR RAPIDS NE')
            WHEN city_substring AND COALESCE(array_length(shared_tokens, 1), 0) >= 2 THEN 0.70
            ELSE 0.00
        END AS match_confidence,
        ROW_NUMBER() OVER (
            PARTITION BY facility_id
            ORDER BY
                city_exact DESC,
                COALESCE(array_length(shared_tokens, 1), 0) DESC,
                city_substring DESC
        ) AS rn
    FROM joined
    WHERE city_exact OR city_substring
)
INSERT INTO silver.facility_frs_xref
    (facility_id, frs_registry_id, match_method, match_confidence,
     overlap_count, shared_tokens, facility_map_city, echo_city, notes)
SELECT
    facility_id,
    frs_registry_id,
    CASE
        WHEN city_exact THEN 'city_exact'
        WHEN city_substring THEN 'city_substring'
        ELSE 'unknown'
    END AS match_method,
    match_confidence,
    overlap_count,
    shared_tokens,
    facility_map_city,
    echo_city,
    CONCAT(facility_map_name, ' -> ', external_name) AS notes
FROM scored
WHERE rn = 1
  AND match_confidence >= 0.60
ON CONFLICT (facility_id) DO NOTHING;

-- ----- Helper views -----
CREATE OR REPLACE VIEW gold.facility_map_with_frs AS
SELECT
    fm.*,
    x.frs_registry_id,
    x.match_method AS frs_match_method,
    x.match_confidence AS frs_match_confidence
FROM silver.facility_map fm
LEFT JOIN silver.facility_frs_xref x ON x.facility_id = fm.facility_id;

CREATE OR REPLACE VIEW gold.facility_frs_coverage AS
SELECT
    fm.source_table,
    COUNT(*) AS total_facilities,
    COUNT(x.frs_registry_id) AS with_frs,
    ROUND(100.0 * COUNT(x.frs_registry_id) / NULLIF(COUNT(*), 0), 1) AS coverage_pct,
    COUNT(*) FILTER (WHERE x.match_method = 'self') AS self_matched,
    COUNT(*) FILTER (WHERE x.match_method = 'city_exact') AS city_exact_matched,
    COUNT(*) FILTER (WHERE x.match_method = 'city_substring') AS city_substring_matched,
    COUNT(*) FILTER (WHERE x.match_confidence >= 1.00) AS confidence_full,
    COUNT(*) FILTER (WHERE x.match_confidence >= 0.80 AND x.match_confidence < 1.00) AS confidence_high,
    COUNT(*) FILTER (WHERE x.match_confidence >= 0.60 AND x.match_confidence < 0.80) AS confidence_medium
FROM silver.facility_map fm
LEFT JOIN silver.facility_frs_xref x ON x.facility_id = fm.facility_id
GROUP BY fm.source_table;
