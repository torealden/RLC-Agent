-- Migration 074: Replace bronze.carb_lcfs_pathways PK with surrogate
-- Date: 2026-05-10
--
-- pathway_id is NOT unique in the CARB workbook — same pathway_id can have
-- multiple rows for historical CIs as a pathway gets re-certified with new
-- feedstock combinations. Example: T2N-1154 (Used Cooking Oil) has CI 14.97
-- in one row and 25.91 in another (UCO vs UCO (UCO) feedstock breakdown).
--
-- We want all rows, not deduplicated. Switch to BIGSERIAL surrogate key.

BEGIN;

-- Drop the existing PK and table data (we just loaded 527 of 892 rows —
-- want to reload with the full set)
TRUNCATE TABLE bronze.carb_lcfs_pathways;
ALTER TABLE bronze.carb_lcfs_pathways DROP CONSTRAINT carb_lcfs_pathways_pkey;

ALTER TABLE bronze.carb_lcfs_pathways
    ADD COLUMN row_id BIGSERIAL PRIMARY KEY;

-- New uniqueness constraint allows the same pathway_id to appear multiple
-- times within a snapshot, as long as (snapshot, pathway, feedstock,
-- fuel_type, CI) form a distinct combo. Some duplicates may still occur
-- in source data — we accept them as raw bronze.
-- (No unique constraint added — bronze is intentionally permissive.)

CREATE INDEX IF NOT EXISTS carb_pathways_pathway_id_idx
    ON bronze.carb_lcfs_pathways (pathway_id);

COMMIT;
