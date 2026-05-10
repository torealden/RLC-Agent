-- Migration 071: MO facility corrections from MO DNR scraper run
-- Date: 2026-05-10
--
-- After running scripts/permit_scrapers/mo.py against our 5 MO
-- canonical facilities, three concrete corrections surfaced:
--
--   1. mo.agp_st_joseph: split into crush + biodiesel.
--      MO DNR has them under SEPARATE Site IDs:
--        - 021-0060 (crush plant)
--        - 021-0118 (biodiesel plant)
--      Same physical address, different regulatory boundaries.
--      Latest Title V: OP2020-020 (crush, 2020) + OP102024-005
--      (biodiesel, 2024).
--
--   2. mo.adm_kansas_city: CLOSED in 2003.
--      ADM "discontinued operations indefinitely effective July 21,
--      2003" at the North Kansas City Missouri soybean crushing and
--      refining facility (per IATP news + ADM corporate filings).
--      Our DB has 24.448 mmbu/yr canonical — wrong.
--
--   3. mo.cargill_kansas_city: confirmed exists as
--      "Cargill - Rochester-Kansas City" at MO Site ID 095-2001.
--      Latest Title V: OP2017-017 (2017). NOTE: there is also a
--      separate "Cargill Birmingham Rd-Kansas City" facility
--      (OP2017-053) — different operation. Worth a follow-up
--      to confirm which our row maps to.
--
-- Plus add MO DNR Site IDs to the active facilities for cross-ref.

BEGIN;

-- ============================================================================
-- 1. mo.adm_kansas_city — closed 2003-07-21
-- ============================================================================

UPDATE reference.oilseed_crush_facilities
SET is_canonical = FALSE,
    status = 'closed',
    nameplate_mmbu_yr = NULL,
    notes = COALESCE(notes || E'\n', '') ||
            '[2026-05-10] CLOSED — ADM "discontinued operations ' ||
            'indefinitely effective July 21, 2003" at the North ' ||
            'Kansas City Missouri soybean crushing and refining ' ||
            'facility. Per IATP news + ADM corporate communications. ' ||
            'No active permits found in MO DNR''s database (checked ' ||
            'via permit_scrapers/mo.py 2026-05-10).',
    verification_method = 'mo_dnr_search_2026_05_10',
    updated_at = NOW()
WHERE facility_id = 'mo.adm_kansas_city';

-- ============================================================================
-- 2. mo.agp_st_joseph — add MO Site ID + note biodiesel split
-- ============================================================================

UPDATE reference.oilseed_crush_facilities
SET state_dnr_facility_num = '021-0060',
    notes = COALESCE(notes || E'\n', '') ||
            '[2026-05-10] MO DNR Site ID 021-0060 (crush plant). ' ||
            'Latest Title V: OP2020-020 (issued 2020-10-30). NOTE: ' ||
            'biodiesel plant at same address has SEPARATE MO Site ' ||
            'ID 021-0118 with its own Title V OP102024-005 (issued ' ||
            '2024-10-28, brand new — biodiesel was on Construction ' ||
            'Permits before). Should be modeled as separate facility ' ||
            'mo.agp_st_joseph_biodiesel in future. AGP COO contact ' ||
            'on the permit cover letter: Lou Rickers.',
    updated_at = NOW()
WHERE facility_id = 'mo.agp_st_joseph';

-- ============================================================================
-- 3. mo.adm_deerfield — add MO Site ID
-- ============================================================================

UPDATE reference.oilseed_crush_facilities
SET state_dnr_facility_num = '217-0043',
    notes = COALESCE(notes || E'\n', '') ||
            '[2026-05-10] MO DNR Site ID 217-0043 (Vernon County). ' ||
            'Latest Title V: OP2017-062 (issued 2017-08-11). 10 ' ||
            'permits in MO DNR archive 2012-2024.',
    updated_at = NOW()
WHERE facility_id = 'mo.adm_deerfield';

-- ============================================================================
-- 4. mo.adm_mexico — add MO Site ID
-- ============================================================================

UPDATE reference.oilseed_crush_facilities
SET state_dnr_facility_num = '007-0002',
    notes = COALESCE(notes || E'\n', '') ||
            '[2026-05-10] MO DNR Site ID 007-0002 (Audrain County). ' ||
            'Latest Title V: OP2018-108 (issued 2018-12-10). 7 ' ||
            'permits in MO DNR archive 2009-2023.',
    updated_at = NOW()
WHERE facility_id = 'mo.adm_mexico';

-- ============================================================================
-- 5. mo.cargill_kansas_city — add Site ID + flag uncertainty
-- ============================================================================

UPDATE reference.oilseed_crush_facilities
SET state_dnr_facility_num = '095-2001',
    notes = COALESCE(notes || E'\n', '') ||
            '[2026-05-10] MO DNR Site ID 095-2001 (Jackson County, ' ||
            '"Cargill - Rochester-Kansas City"). Latest Title V: ' ||
            'OP2017-017 (issued 2017-03-22). NOTE: a separate ' ||
            'Cargill facility "Cargill Birmingham Rd-Kansas City" ' ||
            '(OP2017-053) also exists — possibly a different ' ||
            'operation. PENDING VERIFICATION: which facility is ' ||
            'the soybean crush plant in our DB.',
    updated_at = NOW()
WHERE facility_id = 'mo.cargill_kansas_city';

COMMIT;
