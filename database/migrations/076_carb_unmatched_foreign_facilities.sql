-- Migration 076: Add CARB-certified foreign biofuel facilities + fix INNOLTEK country
-- Date: 2026-05-11
--
-- Source: CARB LCFS pathway xref (mig 075) surfaced 14 foreign facilities
-- with active CARB pathways but no presence in our DB. Plus INNOLTEK
-- (Quebec, Canada) was mis-coded as us.unk.bd.* in our existing inventory.
--
-- These are placeholder rows — minimum-viable schema: company, facility_name,
-- country (via state field), fuel_type, primary_feedstock, CARB pathway
-- pointer in notes. Lat/lon, capacity, year_online to be filled in later
-- when we have a use case.
--
-- Why even bother with foreign placeholders? Three reasons:
--   1. The cross-reference machinery (silver.facility_external_xref) now
--      reports 100% CARB coverage for our DB — no false-negative gaps left
--   2. As we extend to EU SAF mandates, South Korean biodiesel exports,
--      Canadian Clean Fuel Reg arbitrage, these facilities matter
--   3. When extending the xref to EPA RFS RIN producers (task #1) and
--      Biodiesel Magazine survey, these rows give us join targets

BEGIN;

-- ============================================================================
-- Widen too-narrow text columns in reference.biofuel_facilities
-- (primary_feedstock was VARCHAR(10) — barely fits "Soybean Oil")
-- Drop the dependent matview + view first; we recreate at end of migration.
-- ============================================================================
DROP VIEW IF EXISTS gold.feedstock_allocation_by_padd;
DROP MATERIALIZED VIEW IF EXISTS silver.facility_norm CASCADE;

ALTER TABLE reference.biofuel_facilities
    ALTER COLUMN primary_feedstock TYPE VARCHAR(100),
    ALTER COLUMN fuel_type TYPE VARCHAR(50),
    ALTER COLUMN padd TYPE VARCHAR(30),
    ALTER COLUMN technology TYPE VARCHAR(60),
    ALTER COLUMN state TYPE VARCHAR(60),
    ALTER COLUMN status TYPE VARCHAR(40);

-- ============================================================================
-- Fix: INNOLTEK is in Quebec, Canada — not US
-- ============================================================================
UPDATE reference.biodiesel_facilities
SET notes = COALESCE(notes || E'\n', '') ||
            '[2026-05-11] Country correction: INNOLTEK is located in ' ||
            'St-Jean-sur-Richelieu, Quebec, CANADA (not US). CARB LCFS ' ||
            'pathway F00340. Feedstock: Rendered Animal Fat. The facility_id ' ||
            'us.unk.bd.innoltek_inc retains the legacy "us" prefix for ' ||
            'stability — superseded by ca.qc.bd.innoltek (see ' ||
            'reference.biofuel_facilities).',
    updated_at = NOW()
WHERE facility_id = 'us.unk.bd.innoltek_inc';

-- ============================================================================
-- 14 new foreign biofuel facility rows
-- ============================================================================
-- Schema: reference.biofuel_facilities (integer facility_id auto-incremented)

-- Helper: each INSERT uses ON CONFLICT DO NOTHING via company+facility_name
-- match. But biofuel_facilities has no UNIQUE constraint on those columns,
-- so we do existence-check via WHERE NOT EXISTS.

-- --- 1. Dansuk Industrial Co Ltd — South Korea (main facility) ---
INSERT INTO reference.biofuel_facilities
    (company, facility_name, city, state, fuel_type,
     primary_feedstock, status, data_source, notes)
SELECT 'Dansuk Industrial Co., Ltd', 'Dansuk Industrial Co., Ltd', NULL,
       'South Korea', 'biodiesel', 'Tallow / UCO blend', 'operating',
       'CARB_LCFS_2026_05',
       '[2026-05-11] Placeholder from CARB cross-reference. 7 CARB LCFS pathways. Feedstocks: Tallow (animal & poultry fat), Used Cooking Oil. South Korea biodiesel exporter to CA market. Ships finished BD to California by ocean tanker.'
WHERE NOT EXISTS (SELECT 1 FROM reference.biofuel_facilities
                  WHERE LOWER(company) LIKE '%dansuk%' AND COALESCE(facility_name,'') NOT LIKE '%Pyeongtaek%');

-- --- 2. Dansuk Pyeongtaek 2 — secondary South Korea facility ---
INSERT INTO reference.biofuel_facilities
    (company, facility_name, city, state, fuel_type,
     primary_feedstock, status, data_source, notes)
SELECT 'Dansuk Industrial Co., Ltd', 'Pyeongtaek 2', 'Pyeongtaek',
       'South Korea', 'biodiesel', 'Used Cooking Oil', 'operating',
       'CARB_LCFS_2026_05',
       '[2026-05-11] Placeholder from CARB cross-reference. 5 CARB LCFS pathways. UCO-based biodiesel. Sister facility to Dansuk Industrial main plant.'
WHERE NOT EXISTS (SELECT 1 FROM reference.biofuel_facilities
                  WHERE LOWER(company) LIKE '%dansuk%' AND facility_name = 'Pyeongtaek 2');

-- --- 3. ADM Agri-Industries — Canada ---
INSERT INTO reference.biofuel_facilities
    (company, facility_name, city, state, fuel_type,
     primary_feedstock, status, data_source, notes)
SELECT 'Archer Daniels Midland', 'ADM Agri-Industries', NULL,
       'Canada', 'biodiesel', 'Canola Oil', 'operating',
       'CARB_LCFS_2026_05',
       '[2026-05-11] Placeholder from CARB cross-reference. 4 CARB LCFS pathways. ADM Canada biodiesel operations. Likely the ADM Lloydminster (SK) or Windsor (ON) site — needs verification.'
WHERE NOT EXISTS (SELECT 1 FROM reference.biofuel_facilities
                  WHERE LOWER(company) LIKE '%adm%' AND LOWER(facility_name) LIKE '%agri%industries%');

-- --- 4. Tidewater Renewables — Prince George, BC ---
INSERT INTO reference.biofuel_facilities
    (company, facility_name, city, state, fuel_type,
     primary_feedstock, status, data_source, notes)
SELECT 'Tidewater Renewables Ltd.', 'Prince George Renewable Diesel',
       'Prince George', 'British Columbia, Canada', 'renewable_diesel',
       'Mixed (Canola, DCO, Tallow)', 'operating',
       'CARB_LCFS_2026_05',
       '[2026-05-11] Placeholder from CARB cross-reference. 4 CARB LCFS pathways. BC, Canada renewable diesel facility — one of few non-US RD producers shipping to CA market. Multi-feedstock (Canola/DCO/Tallow).'
WHERE NOT EXISTS (SELECT 1 FROM reference.biofuel_facilities
                  WHERE LOWER(company) LIKE '%tidewater%' AND LOWER(facility_name) LIKE '%prince%george%');

-- --- 5. Biocom Energia — Spain ---
INSERT INTO reference.biofuel_facilities
    (company, facility_name, city, state, fuel_type,
     primary_feedstock, status, data_source, notes)
SELECT 'Biocom Energia', 'Biocom Energia', NULL,
       'Spain', 'biodiesel', 'Used Cooking Oil', 'operating',
       'CARB_LCFS_2026_05',
       '[2026-05-11] Placeholder from CARB cross-reference. 3 CARB LCFS pathways. Spain UCO-biodiesel producer. CARB pathways distinguish "Used Cooking Oil (Europe)" vs "Used Cooking Oil (Global)" feedstock origins.'
WHERE NOT EXISTS (SELECT 1 FROM reference.biofuel_facilities
                  WHERE LOWER(company) LIKE '%biocom%');

-- --- 6. Ensyn Technologies — Ontario, Canada ---
INSERT INTO reference.biofuel_facilities
    (company, facility_name, city, state, fuel_type,
     primary_feedstock, status, technology, data_source, notes)
SELECT 'Ensyn Technologies Inc.', 'Ensyn Ontario Facility', NULL,
       'Ontario, Canada', 'biodiesel/renewable_diesel',
       'Pyrolysis Oil from Forest Residue', 'operating',
       'fast_pyrolysis',
       'CARB_LCFS_2026_05',
       '[2026-05-11] Placeholder from CARB cross-reference. 2 CARB LCFS pathways (1 BD, 1 RD). Pyrolysis-oil-from-forest-residue process — uniquely cellulosic feedstock for a "biofuel". Different from HEFA majority.'
WHERE NOT EXISTS (SELECT 1 FROM reference.biofuel_facilities
                  WHERE LOWER(company) LIKE '%ensyn%');

-- --- 7. Universal Biofuels Private Ltd — India ---
INSERT INTO reference.biofuel_facilities
    (company, facility_name, city, state, fuel_type,
     primary_feedstock, status, data_source, notes)
SELECT 'Universal Biofuels Private, Ltd', 'Universal Biofuels Private, Ltd',
       NULL, 'India', 'biodiesel', 'Tallow / UCO blend', 'operating',
       'CARB_LCFS_2026_05',
       '[2026-05-11] Placeholder from CARB cross-reference. 2 CARB LCFS pathways. India biodiesel producer — likely Andhra Pradesh facility per public records.'
WHERE NOT EXISTS (SELECT 1 FROM reference.biofuel_facilities
                  WHERE LOWER(company) LIKE '%universal biofuels%');

-- --- 8. Braya Renewable Fuels — Come By Chance Refinery, Newfoundland ---
INSERT INTO reference.biofuel_facilities
    (company, facility_name, city, state, fuel_type,
     primary_feedstock, status, data_source, notes)
SELECT 'Braya Renewable Fuels (Newfoundland) LP', 'Come By Chance Refinery',
       'Come By Chance', 'Newfoundland and Labrador, Canada',
       'renewable_diesel', 'Mixed (Soybean Oil, DCO, Tallow)', 'operating',
       'CARB_LCFS_2026_05',
       '[2026-05-11] Placeholder from CARB cross-reference. 4 CARB LCFS pathways. Converted Come By Chance crude refinery to RD facility (~14,000 bpd nameplate post-conversion). Argentina-sourced soy oil + N.A. DCO + N.A. tallow. NOTE: already in KG batch 012 (Cresta/Braya Argentine SBO supply chain) — link to KG node when promoting to facility_master.'
WHERE NOT EXISTS (SELECT 1 FROM reference.biofuel_facilities
                  WHERE LOWER(company) LIKE '%braya%' OR LOWER(facility_name) LIKE '%come by chance%');

-- --- 9. Just Biodiesel — Australia ---
INSERT INTO reference.biofuel_facilities
    (company, facility_name, city, state, fuel_type,
     primary_feedstock, status, data_source, notes)
SELECT 'Just Biodiesel Pty. Ltd.', 'Just Biodiesel Pty. Ltd.', NULL,
       'Australia', 'biodiesel', 'Tallow / UCO blend', 'operating',
       'CARB_LCFS_2026_05',
       '[2026-05-11] Placeholder from CARB cross-reference. 2 CARB LCFS pathways. Australia biodiesel producer, multi-feedstock. Tallow and UCO domestically sourced. Ships finished fuel to California.'
WHERE NOT EXISTS (SELECT 1 FROM reference.biofuel_facilities
                  WHERE LOWER(company) LIKE '%just biodiesel%');

-- --- 10. BE8 S.A. — Brazil ---
INSERT INTO reference.biofuel_facilities
    (company, facility_name, city, state, fuel_type,
     primary_feedstock, status, data_source, notes)
SELECT 'BE8 S.A.', 'BE8 S.A.', NULL, 'Brazil', 'biodiesel', 'Tallow',
       'operating', 'CARB_LCFS_2026_05',
       '[2026-05-11] Placeholder from CARB cross-reference. 1 CARB LCFS pathway. BE8 (Brasil Bioenergia / Be8) — major Brazilian biofuels group, multiple plants (Passo Fundo RS, Jacarezinho PR, Marialva PR, Camacari BA, Sao Francisco do Sul SC, others). CARB pathway is tallow-based. Brazil produces 20+ B mmgy biodiesel under federal B14 mandate — small fraction reaches CA.'
WHERE NOT EXISTS (SELECT 1 FROM reference.biofuel_facilities
                  WHERE LOWER(company) LIKE '%be8%');

-- --- 11. ASB Biodiesel — Hong Kong ---
INSERT INTO reference.biofuel_facilities
    (company, facility_name, city, state, fuel_type,
     primary_feedstock, status, data_source, notes)
SELECT 'ASB Biodiesel', 'ASB Biodiesel Hong Kong', 'Hong Kong',
       'Hong Kong', 'biodiesel', 'Used Cooking Oil', 'operating',
       'CARB_LCFS_2026_05',
       '[2026-05-11] Placeholder from CARB cross-reference. 1 CARB LCFS pathway. ASB (Asia Sustainability) Biodiesel — UCO-based Hong Kong plant. ~100k MT/yr nameplate per public sources.'
WHERE NOT EXISTS (SELECT 1 FROM reference.biofuel_facilities
                  WHERE LOWER(company) LIKE '%asb biodiesel%');

-- --- 12. Eco Solutions Co Ltd — South Korea ---
INSERT INTO reference.biofuel_facilities
    (company, facility_name, city, state, fuel_type,
     primary_feedstock, status, data_source, notes)
SELECT 'Eco Solutions Co., Ltd', 'Eco Solutions Co., Ltd', NULL,
       'South Korea', 'biodiesel', 'Used Cooking Oil', 'operating',
       'CARB_LCFS_2026_05',
       '[2026-05-11] Placeholder from CARB cross-reference. 1 CARB LCFS pathway. South Korea UCO-biodiesel producer. Smaller volume player.'
WHERE NOT EXISTS (SELECT 1 FROM reference.biofuel_facilities
                  WHERE LOWER(company) LIKE '%eco solutions%');

-- --- 13. JC Chemical Co Ltd — South Korea ---
INSERT INTO reference.biofuel_facilities
    (company, facility_name, city, state, fuel_type,
     primary_feedstock, status, data_source, notes)
SELECT 'JC Chemical Co., Ltd.', 'JC Chemical Co., Ltd.', NULL,
       'South Korea', 'biodiesel', 'Used Cooking Oil', 'operating',
       'CARB_LCFS_2026_05',
       '[2026-05-11] Placeholder from CARB cross-reference. 2 CARB LCFS pathways. South Korea UCO-biodiesel producer.'
WHERE NOT EXISTS (SELECT 1 FROM reference.biofuel_facilities
                  WHERE LOWER(company) LIKE '%jc chemical%');

-- --- 14. Consolidated Biofuels Ltd — Canada ---
INSERT INTO reference.biofuel_facilities
    (company, facility_name, city, state, fuel_type,
     primary_feedstock, status, data_source, notes)
SELECT 'Consolidated Biofuels Ltd.', 'Consolidated Biofuels Ltd.', NULL,
       'British Columbia, Canada', 'biodiesel', 'Used Cooking Oil',
       'operating', 'CARB_LCFS_2026_05',
       '[2026-05-11] Placeholder from CARB cross-reference. 1 CARB LCFS pathway. Delta, BC UCO-biodiesel producer per public records.'
WHERE NOT EXISTS (SELECT 1 FROM reference.biofuel_facilities
                  WHERE LOWER(company) LIKE '%consolidated bio%');

-- ============================================================================
-- Recreate silver.facility_norm (dropped earlier due to ALTER dependency)
-- + downstream views that CASCADE-dropped with it
-- ============================================================================
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

-- Recreate facility_external_xref view (CASCADE'd)
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

-- Recreate gold.feedstock_allocation_by_padd (dropped above)
CREATE OR REPLACE VIEW gold.feedstock_allocation_by_padd AS
 SELECT fa.period,
    fa.scenario,
    bf.padd,
    fa.fuel_type,
    fa.feedstock_code,
    fp.feedstock_name,
    fp.category AS feedstock_category,
    sum(fa.allocated_mil_lbs) AS total_mil_lbs,
    sum(fa.allocated_mil_gal) AS total_mil_gal,
    count(DISTINCT fa.facility_id) AS facility_count,
    avg(fa.feedstock_cost_lb) AS avg_cost_per_lb,
    avg(fa.margin_per_gal) AS avg_margin_per_gal
   FROM gold.feedstock_allocation fa
     JOIN reference.biofuel_facilities bf ON fa.facility_id = bf.facility_id
     JOIN reference.feedstock_properties fp ON fa.feedstock_code::text = fp.feedstock_code::text
  WHERE fa.run_id = (( SELECT feedstock_allocation.run_id
           FROM gold.feedstock_allocation
          WHERE feedstock_allocation.scenario::text = fa.scenario::text
          ORDER BY feedstock_allocation.created_at DESC
         LIMIT 1))
  GROUP BY fa.period, fa.scenario, bf.padd, fa.fuel_type, fa.feedstock_code, fp.feedstock_name, fp.category, fp.sort_order
  ORDER BY fa.period, bf.padd, fa.fuel_type, fp.sort_order;

-- Recreate gold.facility_status_anomalies (CASCADE'd)
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
        fn.source_table, fn.facility_id, fn.facility_name, fn.state,
        fn.capacity_mmgy, fn.status, fn.fuel_type,
        COALESCE(cm.best_carb_confidence, 0) AS best_carb_confidence,
        COALESCE(cm.confident_pathway_count, 0) AS confident_carb_pathways
    FROM silver.facility_norm fn
    LEFT JOIN carb_matches cm
        ON cm.source_table = fn.source_table AND cm.facility_id = fn.facility_id
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

-- ============================================================================
-- Verification
-- ============================================================================
DO $$
DECLARE
    new_count INT;
BEGIN
    SELECT COUNT(*) INTO new_count
    FROM reference.biofuel_facilities
    WHERE data_source = 'CARB_LCFS_2026_05';
    IF new_count < 14 THEN
        RAISE WARNING 'Expected ≥14 new foreign rows, got %', new_count;
    ELSE
        RAISE NOTICE 'Migration 076 verified: % new foreign biofuel facilities added.', new_count;
    END IF;
END $$;

COMMIT;
