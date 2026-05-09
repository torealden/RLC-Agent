-- Migration 064: complete AGP facility inventory
-- Date: 2026-05-09
--
-- Per user 2026-05-09 — AGP is the priority operator to "complete"
-- because the user has a calibration channel (an executive contact at
-- AGP). Goal: clean, fully-documented inventory of every AGP plant
-- with explicit provenance on what's verified vs estimated, so the
-- contact can flag where our inference is wrong.
--
-- Three actions:
--   1. Dedupe remaining ag_processing_* / agp_* pairs in MN, MO, NE
--      (the IA pairs were already deduped via mig 052).
--   2. Fill missing capacities from NOPA member directory + AGP
--      public communications. ALL inferred values are tagged in notes
--      with 'public_knowledge_2026_05_09; PENDING VERIFICATION' so
--      they're easy to find when ground-truthed against AGP.
--   3. Flag uncertain rows (Van Buren AR — AGP presence not confirmed)
--      as is_canonical=FALSE pending verification.

BEGIN;

-- ============================================================================
-- 1. Dedupe MN / MO / NE pairs
-- ============================================================================

-- mn.agp_dawson: copy capacity from ag_processing_dawson, mark dupe superseded
UPDATE reference.oilseed_crush_facilities
SET nameplate_mmbu_yr = COALESCE(nameplate_mmbu_yr, 24),
    notes = COALESCE(notes || E'\n', '') ||
            '[2026-05-09] Capacity 24 mmbu/yr merged from ag_processing_dawson dupe.',
    updated_at = NOW()
WHERE facility_id = 'mn.agp_dawson';

UPDATE reference.oilseed_crush_facilities
SET is_canonical = FALSE,
    superseded_by = 'mn.agp_dawson',
    notes = COALESCE(notes || E'\n', '') ||
            '[2026-05-09] Superseded by mn.agp_dawson (canonical AGP id).',
    updated_at = NOW()
WHERE facility_id = 'mn.ag_processing_dawson';

-- mo.agp_st_joseph
UPDATE reference.oilseed_crush_facilities
SET nameplate_mmbu_yr = COALESCE(nameplate_mmbu_yr, 24),
    notes = COALESCE(notes || E'\n', '') ||
            '[2026-05-09] Capacity 24 mmbu/yr merged from ag_processing_st_joseph dupe.',
    updated_at = NOW()
WHERE facility_id = 'mo.agp_st_joseph';

UPDATE reference.oilseed_crush_facilities
SET is_canonical = FALSE,
    superseded_by = 'mo.agp_st_joseph',
    notes = COALESCE(notes || E'\n', '') ||
            '[2026-05-09] Superseded by mo.agp_st_joseph.',
    updated_at = NOW()
WHERE facility_id = 'mo.ag_processing_st_joseph';

-- ne.agp_hastings
UPDATE reference.oilseed_crush_facilities
SET nameplate_mmbu_yr = COALESCE(nameplate_mmbu_yr, 27),
    notes = COALESCE(notes || E'\n', '') ||
            '[2026-05-09] Capacity 27 mmbu/yr merged from ag_processing_hastings dupe.',
    updated_at = NOW()
WHERE facility_id = 'ne.agp_hastings';

UPDATE reference.oilseed_crush_facilities
SET is_canonical = FALSE,
    superseded_by = 'ne.agp_hastings',
    notes = COALESCE(notes || E'\n', '') ||
            '[2026-05-09] Superseded by ne.agp_hastings.',
    updated_at = NOW()
WHERE facility_id = 'ne.ag_processing_hastings';

-- ============================================================================
-- 2. Fill missing capacities (from NOPA + public AGP comms; FLAGGED as estimates)
-- ============================================================================

-- ia.agp_algona: large IA crusher; NOPA indicates ~46 mmbu/yr
UPDATE reference.oilseed_crush_facilities
SET nameplate_mmbu_yr = COALESCE(nameplate_mmbu_yr, 46),
    refining_capability = COALESCE(refining_capability, 'Unknown'),
    notes = COALESCE(notes || E'\n', '') ||
            '[2026-05-09] Capacity 46 mmbu/yr from public knowledge (NOPA member ' ||
            'directory + AGP press); PENDING VERIFICATION with AGP.',
    data_source = COALESCE(data_source, 'public_knowledge_2026_05_09'),
    updated_at = NOW()
WHERE facility_id = 'ia.agp_algona';

-- sd.agp_aberdeen: newer AGP plant ~26 mmbu/yr
UPDATE reference.oilseed_crush_facilities
SET nameplate_mmbu_yr = COALESCE(nameplate_mmbu_yr, 26),
    notes = COALESCE(notes || E'\n', '') ||
            '[2026-05-09] Capacity 26 mmbu/yr from public knowledge; ' ||
            'PENDING VERIFICATION with AGP.',
    data_source = COALESCE(data_source, 'public_knowledge_2026_05_09'),
    updated_at = NOW()
WHERE facility_id = 'sd.agp_aberdeen';

-- ============================================================================
-- 3. Flag refining/biodiesel co-location for AGP plants per public knowledge
-- ============================================================================

-- AGP Sergeant Bluff: refining + biodiesel co-located (60 mgy)
UPDATE reference.oilseed_crush_facilities
SET refining_capability = COALESCE(refining_capability, 'Yes (refining + co-located biodiesel)'),
biodiesel_capacity_mgy = COALESCE(biodiesel_capacity_mgy, 60),
    notes = COALESCE(notes || E'\n', '') ||
            '[2026-05-09] Refining capability + ~60 mgy biodiesel ' ||
            'co-located per AGP public; PENDING VERIFICATION.',
    updated_at = NOW()
WHERE facility_id = 'ia.agp_sergeant_bluff';

-- AGP Mason City: ~30 mgy biodiesel co-located per AGP public; refining unconfirmed
UPDATE reference.oilseed_crush_facilities
SET biodiesel_capacity_mgy = COALESCE(biodiesel_capacity_mgy, 30),
notes = COALESCE(notes || E'\n', '') ||
            '[2026-05-09] ~30 mgy biodiesel co-located per AGP public; ' ||
            'PENDING VERIFICATION.',
    updated_at = NOW()
WHERE facility_id = 'ia.agp_mason_city';

-- ============================================================================
-- 4. Uncertain row: Van Buren AR — AGP presence not confirmed
-- ============================================================================

UPDATE reference.oilseed_crush_facilities
SET is_canonical = FALSE,
    notes = COALESCE(notes || E'\n', '') ||
            '[2026-05-09] AGP presence in Van Buren AR not confirmed by ' ||
            'NOPA directory; could be Riceland Foods plant or a shutdown. ' ||
            'Marked non-canonical pending verification.',
    verification_method = 'sanity'
WHERE facility_id = 'ar.ag_processing_van_buren';

COMMIT;
