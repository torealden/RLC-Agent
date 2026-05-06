-- Migration 052: Iowa facility dedup + parent_company backfill
--
-- Date: 2026-05-06
--
-- Why:
--   reference.oilseed_crush_facilities had 34 IA rows representing roughly
--   24 unique plants. Multiple data-load passes layered duplicate records
--   (e.g., 'ia.agp_eagle_grove' from one source plus
--   'ia.ag_processing_eagle_grove' from another for the same plant; five
--   Cargill Cedar Rapids records from versioning on the same physical
--   plants). parent_company was NULL across all 34 rows.
--
--   Building the Market Field facility graph on this data would create
--   spurious weight-1.0 edges between duplicate records, artificially
--   inflating Cargill-internal coupling and AGP-internal coupling. This
--   migration cleans that up before the graph builder runs.
--
-- Method:
--   - ADD COLUMN superseded_by + is_canonical (reversible — superseded
--     records are not deleted, just flagged)
--   - For each duplicate cluster: pick a canonical record, point others
--     at it via superseded_by, set is_canonical=FALSE on superseded
--   - Backfill missing data (capacity, etc.) from superseded twins INTO
--     the canonical record where the canonical has NULL fields
--   - Backfill parent_company across all IA rows
--   - Mark CF Processing Creston as 'closed' per user's authoritative
--     plant list
--
-- Authoritative source for capacity values: user's plant list at
--   D:\Plant Lists\Oilseed Crushing Plants\Soybean Crushing Plants and
--   Oil Processors.xlsx
-- (file is dated; supplements with Shell Rock, Platinum Alta, White
-- River — newer plants — that are NOT in the file.)
--
-- East/West Cedar Rapids split confirmed with user: separate plants
-- ~1mi apart along the river. v006r3 + v044r4 (lon -91.647) = East;
-- v010r3 (lon -91.670) = West.
--
-- Creston: user confirmed via satellite imagery that CF Processing
-- (closed) and White River Nutrition are TWO DISTINCT facilities at
-- different sites in Creston. Kept as separate records.
--
-- Iowa rows after this migration:
--   34 starting rows
--   -10 marked superseded (6 AGP twins, 3 Cargill CR versions,
--                          1 Cargill Eddyville version)
--   = 24 canonical IA plants

-- =============================================================================
-- 1. Add columns (idempotent)
-- =============================================================================

ALTER TABLE reference.oilseed_crush_facilities
    ADD COLUMN IF NOT EXISTS superseded_by TEXT,
    ADD COLUMN IF NOT EXISTS is_canonical  BOOLEAN NOT NULL DEFAULT TRUE;

CREATE INDEX IF NOT EXISTS idx_oilseed_crush_facilities_canonical
    ON reference.oilseed_crush_facilities (is_canonical)
    WHERE is_canonical = TRUE;

COMMENT ON COLUMN reference.oilseed_crush_facilities.superseded_by IS
'If non-NULL, this row is a duplicate of the facility_id named here. The canonical (kept) row has is_canonical=TRUE.';
COMMENT ON COLUMN reference.oilseed_crush_facilities.is_canonical IS
'TRUE for the canonical record per physical plant. Queries that count plants must filter is_canonical=TRUE.';

-- =============================================================================
-- 2. AGP family: 6 mergers (Ag Processing -> AGP records)
-- =============================================================================

-- Merge capacity from "Ag Processing" twins into "AGP" canonicals (Manning
-- gets the user-confirmed 19.2 even though canonical previously had 26.6).

UPDATE reference.oilseed_crush_facilities AS c
SET nameplate_mmbu_yr = src.nameplate_mmbu_yr
FROM (VALUES
    ('ia.agp_eagle_grove',     'ia.ag_processing_eagle_grove',     38.4),
    ('ia.agp_emmetsburg',      'ia.ag_processing_emmetsburg',      21.0),
    ('ia.agp_manning',         'ia.ag_processing_manning',         19.2),
    ('ia.agp_mason_city',      'ia.ag_processing_mason_city',      19.2),
    ('ia.agp_sergeant_bluff',  'ia.ag_processing_sergeant_bluff',  34.9),
    ('ia.agp_sheldon',         'ia.ag_processing_sheldon',         22.7)
) AS src(canonical_id, superseded_id, nameplate_mmbu_yr)
WHERE c.facility_id = src.canonical_id;

-- Mark "Ag Processing" twins superseded
UPDATE reference.oilseed_crush_facilities
SET superseded_by =
    CASE facility_id
        WHEN 'ia.ag_processing_eagle_grove'    THEN 'ia.agp_eagle_grove'
        WHEN 'ia.ag_processing_emmetsburg'     THEN 'ia.agp_emmetsburg'
        WHEN 'ia.ag_processing_manning'        THEN 'ia.agp_manning'
        WHEN 'ia.ag_processing_mason_city'     THEN 'ia.agp_mason_city'
        WHEN 'ia.ag_processing_sergeant_bluff' THEN 'ia.agp_sergeant_bluff'
        WHEN 'ia.ag_processing_sheldon'        THEN 'ia.agp_sheldon'
    END,
    is_canonical = FALSE,
    notes = COALESCE(notes || ' | ', '') ||
            'Superseded by AGP-prefixed canonical record on 2026-05-06; capacity merged forward.'
WHERE facility_id IN (
    'ia.ag_processing_eagle_grove',
    'ia.ag_processing_emmetsburg',
    'ia.ag_processing_manning',
    'ia.ag_processing_mason_city',
    'ia.ag_processing_sergeant_bluff',
    'ia.ag_processing_sheldon'
);

-- =============================================================================
-- 3. Cargill Cedar Rapids: 3 mergers (East gets v006r3+v044r4; West gets v010r3)
-- =============================================================================

-- East canonical gets coords from v006r3/v044r4 (lat 41.970, lon -91.647)
UPDATE reference.oilseed_crush_facilities
SET lat = COALESCE(lat, 41.970),
    lon = COALESCE(lon, -91.647)
WHERE facility_id = 'ia.cargill_cedar_rapids_east'
  AND (lat IS NULL OR lat = 0);

-- West canonical gets coords from v010r3 (lat 41.976, lon -91.670)
UPDATE reference.oilseed_crush_facilities
SET lat = COALESCE(lat, 41.976),
    lon = COALESCE(lon, -91.670)
WHERE facility_id = 'ia.cargill_cedar_rapids_west'
  AND (lat IS NULL OR lat = 0);

-- Mark version records superseded
UPDATE reference.oilseed_crush_facilities
SET superseded_by = 'ia.cargill_cedar_rapids_east',
    is_canonical = FALSE,
    notes = COALESCE(notes || ' | ', '') ||
            'Superseded by ia.cargill_cedar_rapids_east on 2026-05-06 (data-load version of East plant; coords -91.647 = east of river).'
WHERE facility_id IN ('ia.cargill_cedar_rapids_v006r3', 'ia.cargill_cedar_rapids_v044r4');

UPDATE reference.oilseed_crush_facilities
SET superseded_by = 'ia.cargill_cedar_rapids_west',
    is_canonical = FALSE,
    notes = COALESCE(notes || ' | ', '') ||
            'Superseded by ia.cargill_cedar_rapids_west on 2026-05-06 (data-load version of West plant; coords -91.670 = west of river).'
WHERE facility_id = 'ia.cargill_cedar_rapids_v010r3';

-- =============================================================================
-- 4. Cargill Eddyville: 1 merger (v004r3 stays canonical, v006r1 superseded)
-- =============================================================================

UPDATE reference.oilseed_crush_facilities
SET superseded_by = 'ia.cargill_eddyville_v004r3',
    is_canonical = FALSE,
    notes = COALESCE(notes || ' | ', '') ||
            'Superseded by ia.cargill_eddyville_v004r3 on 2026-05-06 (duplicate data-load version).'
WHERE facility_id = 'ia.cargill_eddyville_v006r1';

-- =============================================================================
-- 5. CF Processing Creston: mark closed per user's authoritative plant list
-- =============================================================================

UPDATE reference.oilseed_crush_facilities
SET status = 'closed',
    notes = COALESCE(notes || ' | ', '') ||
            'Marked closed 2026-05-06 per Soybean Crushing Plants list (D:\Plant Lists). White River Nutrition operates a separate facility in Creston (ia.white_river_creston).'
WHERE facility_id = 'ia.cf_processing_creston';

-- =============================================================================
-- 6. Backfill parent_company across all IA rows (canonical + superseded)
-- =============================================================================

UPDATE reference.oilseed_crush_facilities
SET parent_company =
    CASE
        WHEN operator IN ('AGP', 'Ag Processing', 'Ag Processing Inc.')
            THEN 'AGP'
        WHEN operator IN ('Cargill', 'Cargill Cedar Rapids',
                          'Cargill Cedar Rapids West', 'Cargill Cedar Rapids East')
            THEN 'Cargill'
        WHEN operator = 'ADM'
            THEN 'Archer-Daniels-Midland'
        WHEN operator = 'Bunge'
            THEN 'Bunge Global'
        WHEN operator = 'West Central Cooperative'
            THEN 'Landus Cooperative'
        ELSE operator    -- private / unknown parent: keep operator name as parent
    END
WHERE state = 'IA' AND parent_company IS NULL;
