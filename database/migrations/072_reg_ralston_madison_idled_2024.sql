-- Migration 072: REG Ralston IA + REG Madison WI idled March 2024
-- Date: 2026-05-10
--
-- Source: Chevron 10-K FY2024 (filed 2025-02-21) discloses "two refineries
-- idled in 2024" but does NOT name them. Confirmed by cross-reference:
--
--   1. CARB LCFS pathways: REG Ralston and REG Madison both ABSENT from
--      the 2024-2026 certified pathways list (892 pathways for 94
--      facilities; every other REG production plant has ≥3 pathways).
--      Pathway absence is the strongest "not shipping CA / not running"
--      market signal.
--
--   2. Industry press (March 2024):
--        - Biodiesel Magazine "Chevron REG idles Ralston, Madison facilities"
--        - Oil & Gas Journal "Chevron to shutter two US biodiesel plants"
--        - Carroll Broadcasting (Iowa local): "24 jobs lost at Ralston";
--          "first biodiesel plant for REG, opened 2002"
--        - Chevron REG attribution: weak biodiesel volumes under federal
--          Renewable Fuel Standard (RFS); RVO too low for production economics
--
-- Combined capacity offline: 50 mmgy (30 + 20).
--
-- We don't fully close them — Chevron used "idled indefinitely" wording,
-- which preserves restart optionality (and the goodwill on the balance
-- sheet — FY2024 10-K confirms no goodwill impairment on the REG
-- acquisition despite the idlings).

BEGIN;

-- ============================================================================
-- 1. REG Ralston, LLC (Iowa) — 30 mmgy
-- ============================================================================

UPDATE reference.biodiesel_facilities
SET status = 'idled',
    notes = COALESCE(notes || E'\n', '') ||
            '[2026-05-10] IDLED INDEFINITELY effective ~March 2024 per ' ||
            'Chevron REG public announcement. 24 jobs eliminated at Ralston, ' ||
            'IA. Plant was the first biodiesel plant REG ever built ' ||
            '(opened 2002). Citation: Chevron 10-K FY2024 confirms "two ' ||
            'refineries idled in 2024" (unnamed in 10-K); Biodiesel ' ||
            'Magazine + Oil & Gas Journal + Carroll Broadcasting Mar 2024 ' ||
            'name Ralston + Madison WI as the two. Driver: weak biodiesel ' ||
            'RVO under federal RFS made production uneconomic. CARB LCFS ' ||
            'pathways: NONE current (vs. 23 for REG Newton, 19 for REG ' ||
            'Mason City — both still running). Note: Chevron has NOT ' ||
            'taken goodwill impairment on REG acquisition (FY2024 10-K), ' ||
            'so restart optionality is preserved.',
    updated_at = NOW()
WHERE facility_id = 'us.unk.bd.reg_ralston_llc';

-- ============================================================================
-- 2. REG Madison, LLC (Wisconsin) — 20 mmgy
-- ============================================================================

UPDATE reference.biodiesel_facilities
SET status = 'idled',
    notes = COALESCE(notes || E'\n', '') ||
            '[2026-05-10] IDLED INDEFINITELY effective ~March 2024 per ' ||
            'Chevron REG public announcement, simultaneously with REG ' ||
            'Ralston IA. Combined Ralston+Madison capacity offline = 50 ' ||
            'mmgy (30 + 20). Citations: Biodiesel Magazine "Chevron REG ' ||
            'idles Ralston, Madison facilities"; Land Line Media ' ||
            '"Chevron shutting down biodiesel plants in Iowa, Wisconsin". ' ||
            'Driver: weak biodiesel RVO under federal RFS. CARB LCFS ' ||
            'pathways: NONE current. Restart optionality preserved (no ' ||
            'goodwill impairment in Chevron 10-K FY2024).',
    updated_at = NOW()
WHERE facility_id = 'us.unk.bd.reg_madison_llc';

-- ============================================================================
-- Verification — both rows must exist
-- ============================================================================
DO $$
DECLARE
    n INT;
BEGIN
    SELECT COUNT(*) INTO n FROM reference.biodiesel_facilities
        WHERE facility_id IN ('us.unk.bd.reg_ralston_llc', 'us.unk.bd.reg_madison_llc')
          AND status = 'idled';
    IF n <> 2 THEN
        RAISE EXCEPTION 'Expected 2 idled rows, got %', n;
    END IF;
    RAISE NOTICE 'Migration 072 verified: 2 REG plants marked idled.';
END $$;

COMMIT;
