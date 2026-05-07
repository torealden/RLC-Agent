-- Migration 058: facility data quality pass
-- Date: 2026-05-07
--
-- Background: ground-truth verification (satellite imagery, EPA FRS lookups,
-- public news) caught several rows in `reference.oilseed_crush_facilities`
-- and `reference.facility_master` that misrepresent reality. The third-party
-- list `Soybean Crushing Plants and Oil Processors.xlsx` lumped some
-- biofuel-named facilities into the crush list, and `057_seed_iowa_multi_industry_facilities`
-- contained one fabricated row.
--
-- This migration:
--   1. Marks confirmed-bad rows in oilseed_crush_facilities as is_canonical=FALSE
--      (preserves the rows for provenance, but excludes from canonical queries).
--   2. Updates Quad County Corn Processors status to 'idle' (halted operations
--      late 2025; board pursuing capital for upgrades).
--   3. Removes fabricated Hormel Force City row from facility_master.
--
-- Verification methods used:
--   - satellite: Maps imagery showing facility absence or scale mismatch
--   - epa_frs: EPA Facility Registry NAICS/SIC lookup
--   - news: public news of plant closure/bankruptcy
--   - sanity: capacity exceeds plausible bounds for known operator class

BEGIN;

-- 1. Tri-City Energy LLC, Keokuk IA
--    EPA FRS 110028006805, NAICS 325998 ("Other Misc Chemical"), SIC 2869
--    ("Industrial Organic Chemicals"). Industrial chemicals / biofuel
--    operation, not soybean crushing.
UPDATE reference.oilseed_crush_facilities
SET is_canonical = FALSE,
    notes = COALESCE(notes || E'\n', '') ||
            '[2026-05-07] Marked non-canonical: EPA FRS 110028006805, NAICS 325998 ' ||
            'industrial chemicals / biofuel; not oilseed crush. Verified satellite + EPA FRS.',
    verified_at = NOW(),
    verified_by = 'tore_alden',
    verification_method = 'satellite+epa_frs'
WHERE facility_id = 'ia.tri_city_energy_llc_keokuk';

-- 2. Maple River Energy LLC, Galva IA
--    Coordinates collide with Quad County Corn Processors (real ethanol plant).
--    Satellite imagery shows small-town grain elevator at the labeled point,
--    no oilseed crush facility. Likely an artifact of the source xlsx
--    incorrectly relabeling Quad County.
UPDATE reference.oilseed_crush_facilities
SET is_canonical = FALSE,
    notes = COALESCE(notes || E'\n', '') ||
            '[2026-05-07] Marked non-canonical: satellite imagery shows no crush ' ||
            'facility at coords; conflicts with Quad County Corn Processors ' ||
            'ethanol plant at same Galva location.',
    verified_at = NOW(),
    verified_by = 'tore_alden',
    verification_method = 'satellite'
WHERE facility_id = 'ia.maple_river_energy_llc_galva';

-- 3. Soy Energy LLC, Marcus IA
--    Soy Energy LLC entered bankruptcy in 2014; assets sold to REG (now
--    Chevron). No active soybean crush operation. Was originally a
--    biodiesel facility, never operated as a commodity crusher at scale.
UPDATE reference.oilseed_crush_facilities
SET is_canonical = FALSE,
    notes = COALESCE(notes || E'\n', '') ||
            '[2026-05-07] Marked non-canonical: Soy Energy LLC bankruptcy 2014; ' ||
            'sold to REG (now Chevron). No active crush operation.',
    verified_at = NOW(),
    verified_by = 'tore_alden',
    verification_method = 'news'
WHERE facility_id = 'ia.soy_energy_llc_marcus';

-- 4. Ultra Soy of America, South Milford IN
--    Listed at 225,533 BPD. That would be the largest soy crusher in
--    North America by ~60%; almost certainly a unit error or fabrication
--    in the source list. Pending real verification.
UPDATE reference.oilseed_crush_facilities
SET is_canonical = FALSE,
    notes = COALESCE(notes || E'\n', '') ||
            '[2026-05-07] Marked non-canonical pending verification: nameplate ' ||
            '225,533 BPD exceeds plausible bounds for any single soy crusher.',
    verified_at = NOW(),
    verified_by = 'tore_alden',
    verification_method = 'sanity'
WHERE facility_id = 'in.ultra_soy_of_america_south_milford';

-- 5. Quad County Corn Processors, Galva IA — operational status update
--    Halted operations late 2025; board pursuing capital for facility
--    improvements. Indefinite shutdown.
UPDATE reference.facility_master
SET status = 'idle',
    notes = COALESCE(notes || E'\n', '') ||
            '[2026-05-07] Status changed from active to idle: halted ' ||
            'operations late 2025; board pursuing capital for upgrades.',
    verified_at = NOW(),
    verified_by = 'tore_alden',
    verification_method = 'news',
    updated_at = NOW()
WHERE facility_id = 'ia.quad_county_galva';

-- 6. Remove fabricated Hormel Force City row
--    "Force City" is not a real Iowa city. Hormel does not operate a
--    pork plant in Wright County IA. Likely a hallucinated row from
--    yesterday's seed migration. Hormel's actual IA presence is at
--    Osceola (pork), Knoxville, and Fort Dodge (Jennie-O turkey), none
--    of which are currently in facility_master. Future seed should add
--    those.
DELETE FROM reference.facility_master
WHERE facility_id = 'ia.hormel_force_city';

COMMIT;
