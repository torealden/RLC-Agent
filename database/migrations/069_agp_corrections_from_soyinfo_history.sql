-- Migration 069: AGP corrections from SoyInfo Center 1923-2021 history
-- Date: 2026-05-10
--
-- Source: Shurtleff & Aoyagi (2021), History of Cooperative Soybean
-- Processing in the United States 1923-2021. SoyInfo Center, 410 p.
-- Free PDF cached at:
--   domain_knowledge/company_reports/agp/soyinfo_cooperative_soybean_processing_1923_2021.pdf
-- Curated extract:
--   domain_knowledge/company_reports/agp/agp_history_extracted.md
--
-- Three concrete corrections to the canonical AGP rows we set in
-- mig 064 (where most numbers were tagged "PENDING VERIFICATION"):
--
--   1. ia.agp_algona is BIODIESEL-ONLY, not a crusher.
--      AGP acquired the former East Fork Biodiesel LLC plant
--      July 2011. ~60 mgy biodiesel. NOT a soybean crush plant.
--
--   2. ia.agp_mason_city has no AGP biodiesel. The 30 mgy we
--      attributed in mig 064 was Freedom Fuels LLC (separate
--      entity, possibly co-located).
--
--   3. ne.agp_david_city: AGP has no presence in David City NE per
--      the 2021 source. AGP's only Nebraska plant is Hastings.
--      Mark non-canonical pending re-research (the row may be
--      another operator's plant misattributed to AGP).
--
-- Plus historical lineage notes added to all canonical AGP plants
-- because predecessor cooperative provenance is calibration-relevant
-- for the Courtney Lawson conversation.

BEGIN;

-- ============================================================================
-- 1. ia.agp_algona — biodiesel-only, not a crusher
-- ============================================================================

UPDATE reference.oilseed_crush_facilities
SET nameplate_mmbu_yr = NULL,
    biodiesel_capacity_mgy = COALESCE(biodiesel_capacity_mgy, 60),
    refining_capability = NULL,
    notes = COALESCE(notes || E'\n', '') ||
            '[2026-05-10] CORRECTION per SoyInfo Center 2021: Algona is ' ||
            'BIODIESEL-ONLY, ~60 mgy. AGP acquired the former East Fork ' ||
            'Biodiesel LLC plant July 2011 (idle prior to acquisition). ' ||
            'No soybean crush capacity. The 46 mmbu/yr crush capacity ' ||
            'set in mig 064 was wrong.',
    data_source = 'soyinfo_2021_p_history',
    updated_at = NOW()
WHERE facility_id = 'ia.agp_algona';

-- ============================================================================
-- 2. ia.agp_mason_city — remove erroneous biodiesel attribution
-- ============================================================================

UPDATE reference.oilseed_crush_facilities
SET biodiesel_capacity_mgy = NULL,
    notes = COALESCE(notes || E'\n', '') ||
            '[2026-05-10] CORRECTION per SoyInfo Center 2021: the 30 mgy ' ||
            'biodiesel attributed in mig 064 was Freedom Fuels LLC ' ||
            '(separate operator, possibly co-located in Mason City). ' ||
            'AGP Mason City is crush-only; came from AGRI Industries ' ||
            'acquisition 1985-12-31.',
    updated_at = NOW()
WHERE facility_id = 'ia.agp_mason_city';

-- ============================================================================
-- 3. mo.agp_st_joseph — add biodiesel (we missed in mig 064)
-- ============================================================================

UPDATE reference.oilseed_crush_facilities
SET biodiesel_capacity_mgy = COALESCE(biodiesel_capacity_mgy, 55),
    notes = COALESCE(notes || E'\n', '') ||
            '[2026-05-10] CORRECTION per SoyInfo Center 2021: AGP St. Joseph ' ||
            'is one of three AGP biodiesel plants (with Algona + Sergeant ' ||
            'Bluff). Combined 175 mgy across three plants. St. Joseph ~55 ' ||
            'mgy estimated by triangulation; PENDING VERIFICATION.',
    updated_at = NOW()
WHERE facility_id = 'mo.agp_st_joseph';

-- ============================================================================
-- 4. ne.agp_david_city — mark non-canonical (likely not AGP at all)
-- ============================================================================

UPDATE reference.oilseed_crush_facilities
SET is_canonical = FALSE,
    notes = COALESCE(notes || E'\n', '') ||
            '[2026-05-10] FLAGGED per SoyInfo Center 2021: AGP has no ' ||
            'plant in David City NE. AGP''s only NE plant is Hastings. ' ||
            'The David City row was likely misattributed to AGP — there ' ||
            'is/was a real crush plant in David City NE, but under a ' ||
            'different operator (possibly historical AGRI Industries or ' ||
            'a Cooperative Producers Inc plant). Marking non-canonical ' ||
            'pending re-research.',
    verification_method = 'public_knowledge_2026_05_10',
    updated_at = NOW()
WHERE facility_id = 'ne.agp_david_city';

-- ============================================================================
-- 5. Historical lineage notes on all canonical AGP plants
-- ============================================================================

-- ia.agp_eagle_grove: Boone Valley Cooperative Processing Association,
-- founded 1944 March, fire 1947-08-23, rebuilt. The plant that became AGP
-- itself in March 1984. AGP's "home" plant.
UPDATE reference.oilseed_crush_facilities
SET commissioned_year = COALESCE(commissioned_year, 1944),
    notes = COALESCE(notes || E'\n', '') ||
            '[2026-05-10] LINEAGE: Founded 1944-03 by Boone Valley ' ||
            'Cooperative Processing Association. Plant destroyed by fire ' ||
            '1947-08-23, rebuilt. Boone Valley led the 1983 cooperative ' ||
            'consolidation, renamed AGP 1984-03-07. Eagle Grove is ' ||
            'effectively AGP''s founding plant.',
    updated_at = NOW()
WHERE facility_id = 'ia.agp_eagle_grove';

-- ia.agp_sheldon: one of the original 7 IA cooperative crushers (1945 era),
-- ran by Farmers Regional Cooperative (Big 4 Div., Fort Dodge), later
-- Farmland → AGP via 1983 merger.
UPDATE reference.oilseed_crush_facilities
SET notes = COALESCE(notes || E'\n', '') ||
            '[2026-05-10] LINEAGE: One of original 7 IA cooperative ' ||
            'crushers (per 1945 Soybean Digest). Run as Farmers Regional ' ||
            'Cooperative (Big 4 Division, Fort Dodge IA); later Farmland ' ||
            'Industries; → AGP via 1983-08-31 merger.',
    updated_at = NOW()
WHERE facility_id = 'ia.agp_sheldon';

-- mn.agp_dawson: Tri-County Soy Bean Co-op 1951, renamed Dawson Mills 1969,
-- to Land O'Lakes 1980, to AGP 1983.
UPDATE reference.oilseed_crush_facilities
SET commissioned_year = COALESCE(commissioned_year, 1951),
    notes = COALESCE(notes || E'\n', '') ||
            '[2026-05-10] LINEAGE: Founded 1951-11 as Tri-County Soy Bean ' ||
            'Co-operative Association. Renamed Dawson Mills 1969 (Joe ' ||
            'Givens, GM since 1952). Merged into Land O''Lakes 1980-03-01 ' ||
            '(named Land O''Lakes Soybean Division). Acquired by AGP via ' ||
            '1983-08-31 cooperative merger.',
    updated_at = NOW()
WHERE facility_id = 'mn.agp_dawson';

-- mo.agp_st_joseph: Dannen Mills (pre-1963) → CMA 1963 → Far-Mar-Co 1968
-- → Farmland 1977 → AGP 1983
UPDATE reference.oilseed_crush_facilities
SET notes = COALESCE(notes || E'\n', '') ||
            '[2026-05-10] LINEAGE: Originally Dannen Mills (Dannen family). ' ||
            'CMA (Consumers Marketing Association, Kansas City MO) ' ||
            'purchased 1963-09. Became part of Far-Mar-Co 1968 merger. ' ||
            'Farmland Industries 1977-05-02 (Far-Mar-Co absorbed by ' ||
            'Farmland). To AGP via 1983-08-31 merger.',
    updated_at = NOW()
WHERE facility_id = 'mo.agp_st_joseph';

-- ia.agp_sergeant_bluff: Farmland 1975, AGP 1983, FIRST biodiesel 1996.
UPDATE reference.oilseed_crush_facilities
SET commissioned_year = COALESCE(commissioned_year, 1975),
    notes = COALESCE(notes || E'\n', '') ||
            '[2026-05-10] LINEAGE: Built 1975-08 by Farmland Industries. ' ||
            'To AGP via 1983-08-31 merger. **First plant to make ' ||
            'biodiesel** — SoyGold brand started 1996-11. Soy methyl ' ||
            'ester capacity expanded 2017.',
    updated_at = NOW()
WHERE facility_id = 'ia.agp_sergeant_bluff';

-- ia.agp_manning: AGRI Industries → AGP 1985-12-31
UPDATE reference.oilseed_crush_facilities
SET notes = COALESCE(notes || E'\n', '') ||
            '[2026-05-10] LINEAGE: Owned by AGRI Industries Inc (Iowa). ' ||
            'Acquired by AGP 1985-12-31 (purchased with Mason City; ' ||
            'brought AGP plant count to 8, ~11.5%% US crush capacity).',
    updated_at = NOW()
WHERE facility_id = 'ia.agp_manning';

-- ia.agp_mason_city: AGRI Industries → AGP 1985-12-31
UPDATE reference.oilseed_crush_facilities
SET notes = COALESCE(notes || E'\n', '') ||
            '[2026-05-10] LINEAGE: Owned by AGRI Industries Inc (Iowa). ' ||
            'Acquired by AGP 1985-12-31 (with Manning).',
    updated_at = NOW()
WHERE facility_id = 'ia.agp_mason_city';

-- ia.agp_emmetsburg: AGP-built 1996-97
UPDATE reference.oilseed_crush_facilities
SET commissioned_year = COALESCE(commissioned_year, 1997),
    notes = COALESCE(notes || E'\n', '') ||
            '[2026-05-10] LINEAGE: **First plant AGP built from scratch** ' ||
            '(prior plants were all acquired). Construction 1996, opened ' ||
            'October 1997, ribbon cutting 1997-09-17.',
    updated_at = NOW()
WHERE facility_id = 'ia.agp_emmetsburg';

-- ne.agp_hastings: AGP-built corn 1995, soy 1999
UPDATE reference.oilseed_crush_facilities
SET commissioned_year = COALESCE(commissioned_year, 1999),
    notes = COALESCE(notes || E'\n', '') ||
            '[2026-05-10] LINEAGE: AGP-built. Corn processing/ethanol ' ||
            'plant came online 1995-11. **Soybean crush plant began ' ||
            'June 1999** — first farmer-owned soybean processing plant ' ||
            'in Nebraska, westernmost soybean processing plant in US, ' ||
            'AGP''s 9th soybean plant. Per 2021 source, the site has ' ||
            'two solvent extraction plants side by side.',
    updated_at = NOW()
WHERE facility_id = 'ne.agp_hastings';

-- sd.agp_aberdeen: AGP-built, ground breaking 2017-05-03
UPDATE reference.oilseed_crush_facilities
SET commissioned_year = COALESCE(commissioned_year, 2019),
    notes = COALESCE(notes || E'\n', '') ||
            '[2026-05-10] LINEAGE: AGP-built. Ground breaking ' ||
            '2017-05-03. AGP''s 10th soybean processing plant. ~25 ' ||
            'miles south of ND border.',
    updated_at = NOW()
WHERE facility_id = 'sd.agp_aberdeen';

COMMIT;
