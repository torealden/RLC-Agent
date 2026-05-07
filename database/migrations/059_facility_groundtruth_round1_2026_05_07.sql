-- Migration 059: facility ground-truth round 1 corrections
-- Date: 2026-05-07
--
-- User ground-truthed all 14 questioned/spot-check facilities from
-- domain_knowledge/facility_groundtruth/round_001_ia_multi_industry_2026_05_07.csv
-- by visual map verification (screenshots saved). Findings:
--   - 11 facilities confirmed real with verified street addresses
--   - 1 wrong state (Stockton Biodiesel = Port of Stockton, CA — not Tama, IA)
--   - 1 phantom predecessor row (Iowa Ethanol LLC, Hanlontown — same plant
--     as ia.poet_hanlontown, POET acquired 2010)
--   - 1 operator change (West Central Cooperative merged into Landus
--     Cooperative ~2018-19)
--   - 1 missing facility surfaced: Cargill operates a corn-milling/ethanol
--     plant in Fort Dodge separate from the soy-crush plant we already have.
--
-- This migration applies the categorical fixes. Coordinate refinements
-- (city centroid → actual plant addresses) are deferred to a separate
-- geocoding pass that runs Nominatim against the now-known street
-- addresses.

BEGIN;

-- 1. DELETE Stockton Biodiesel — wrong state (it's at Port of Stockton, CA)
DELETE FROM reference.facility_master
WHERE facility_id = 'ia.stockton_biodiesel_tama';

-- 2. DELETE Iowa Ethanol Hanlontown — POET acquired the plant in 2010;
--    same physical location as ia.poet_hanlontown (43.330, -93.371). Keep
--    POET row, drop the predecessor.
DELETE FROM reference.facility_master
WHERE facility_id = 'ia.iowa_ethanol_hanlontown';

-- 3. UPDATE West Central Cooperative → Landus Cooperative
--    The two cooperatives merged ~2018-19. Google Maps confirms the
--    Ralston facility is now operated as "Landus Cooperative" (formerly
--    "West Central Cooperative"). The facility_id is left as-is to
--    preserve the historical key but operator metadata updated.
UPDATE reference.oilseed_crush_facilities
SET operator = 'Landus Cooperative',
    parent_company = 'Landus Cooperative',
    notes = COALESCE(notes || E'\n', '') ||
            '[2026-05-07] Operator updated West Central Cooperative -> Landus ' ||
            'Cooperative (merger ~2018-19). Verified Google Maps card shows ' ||
            'Landus Cooperative at 406 1st St, Ralston, IA 51459.',
    verified_at = NOW(),
    verified_by = 'tore_alden',
    verification_method = 'satellite'
WHERE facility_id = 'ia.west_central_cooperative_ralston';

-- 4. ADD Cargill Fort Dodge ethanol plant (separate from existing soy crush)
--    User screenshot for ia.cargill_fort_dodge (which we have in
--    oilseed_crush_facilities) actually shows the "Cargill Corn Milling
--    Ethanol Plant" at 1950 Harvest Ave, Fort Dodge, IA 50501. This is a
--    different physical facility from the soy crush plant. Add it to
--    facility_master under industry_code='ethanol'.
INSERT INTO reference.facility_master (
    facility_id, name, industry_code, operator, parent_company,
    city, county, state, country,
    lat, lon, status, data_source,
    notes, sources,
    verified_at, verified_by, verification_method,
    created_at, updated_at
) VALUES (
    'ia.cargill_fort_dodge_ethanol',
    'Cargill Corn Milling — Fort Dodge',
    'ethanol',
    'Cargill',
    'Cargill',
    'Fort Dodge',
    'Webster',
    'IA',
    'US',
    42.504,  -- approx; refine in coord pass
    -94.191, -- approx
    'active',
    'satellite',
    'Address: 1950 Harvest Ave, Fort Dodge, IA 50501. Distinct from ' ||
    'ia.cargill_fort_dodge (oilseed crush, separate physical plant).',
    'Google Maps verification 2026-05-07',
    NOW(),
    'tore_alden',
    'satellite',
    NOW(),
    NOW()
)
ON CONFLICT (facility_id) DO NOTHING;

-- 5. Mark the 11 ground-truthed facilities verified (no data change, just
--    record the verification)
UPDATE reference.facility_master
SET verified_at = NOW(),
    verified_by = 'tore_alden',
    verification_method = 'satellite',
    notes = COALESCE(notes || E'\n', '') ||
            '[2026-05-07] Ground-truthed via satellite imagery; address ' ||
            'confirmed.'
WHERE facility_id IN (
    'ia.chevron_reg_newton',
    'ia.rose_acre_stuart',
    'ia.poet_ashton',
    'ia.valero_fort_dodge',
    'ia.quad_county_galva',          -- already mig 058 verified; this just adds verification
    'ia.poet_hanlontown',
    'ia.pine_lake_steamboat_rock',
    'ia.seaboard_triumph_sioux_city',
    'ia.tyson_waterloo',
    'ia.agp_manning',
    'ia.agp_sergeant_bluff'
);

-- Same for the oilseed_crush row that got verified
UPDATE reference.oilseed_crush_facilities
SET verified_at = NOW(),
    verified_by = 'tore_alden',
    verification_method = 'satellite',
    notes = COALESCE(notes || E'\n', '') ||
            '[2026-05-07] Ground-truthed via satellite imagery.'
WHERE facility_id IN (
    'ia.cargill_fort_dodge',
    'ia.agp_manning',
    'ia.agp_sergeant_bluff'
);

COMMIT;
