-- Migration 070: REVERT mig 069's David City demotion
-- Date: 2026-05-10
--
-- I was wrong in mig 069 to mark ne.agp_david_city non-canonical.
-- The 2021 SoyInfo Center book has a 2020 cutoff date and Bill
-- Lester's interview was Feb 2021. The David City announcement came
-- 11 months later, in January 2022. The book's silence on David
-- City was simply an artifact of timing.
--
-- Real story (per Nebraska Examiner, AGP press releases, and
-- Google Maps satellite imagery showing the active site):
--   - Announced Jan 2022
--   - $700M investment — AGP's largest ever
--   - 50 million bushels/year capacity (matches our DB exactly)
--   - 150,000 bu/day crush rate
--   - 1.8 million lbs/day crude oil degumming capacity
--   - 275-acre site, 13.6 miles rail track, 2.5 miles paved roads
--   - 80 jobs
--   - Ground breaking 2023
--   - AGP's 11th soybean processing location (Aberdeen was 10th)
--   - Commercial operations starting end of August 2025
--
-- Lesson: book cutoffs hide newer plants. Always cross-reference
-- with current operator press releases and satellite imagery before
-- demoting a row based on book silence alone.

BEGIN;

UPDATE reference.oilseed_crush_facilities
SET is_canonical = TRUE,
    commissioned_year = COALESCE(commissioned_year, 2025),
    notes = COALESCE(notes || E'\n', '') ||
            '[2026-05-10] CORRECTION TO MIG 069: David City IS a real ' ||
            'AGP plant (verified via Google Maps satellite + AGP press ' ||
            'releases + Nebraska Examiner). Mig 069 wrongly demoted it ' ||
            'based on the 2021 SoyInfo book''s silence — but the book ' ||
            'pre-dates the Jan 2022 announcement. ' ||
            'Plant facts: $700M investment, 50 mmbu/yr (AGP''s largest), ' ||
            '150k bu/day crush, 1.8 mlb/day degumming, 275-acre site, ' ||
            '13.6 mi rail, 80 jobs. Announced 2022-01, ground breaking ' ||
            '2023, commercial ops end of August 2025. AGP''s 11th ' ||
            'soybean processing location.',
    data_source = 'agp_press_release_2022; nebraska_examiner_2025',
    updated_at = NOW()
WHERE facility_id = 'ne.agp_david_city';

-- Also: fix our prior coordinates (the city centroid) to plant lat/lon
-- since we have a satellite image showing the actual location. The
-- plant is NW of David City town center on the W side of Highway 15.
-- Approximate centroid of the visible facility per Google Maps view:
UPDATE reference.oilseed_crush_facilities
SET lat = 41.2761, lon = -97.1356,
    notes = COALESCE(notes || E'\n', '') ||
            '[2026-05-10] Refined coordinates from Google Maps satellite ' ||
            'view (plant centroid, ~1.2 mi NW of David City center).',
    updated_at = NOW()
WHERE facility_id = 'ne.agp_david_city';

COMMIT;
