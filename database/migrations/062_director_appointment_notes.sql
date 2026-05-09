-- Migration 062: add `notes` column to silver.director_appointment
-- Mig 061 missed this column. The DEF 14A extractor's loader uses it
-- to mark "cross-referenced from another company's DEF 14A" rows.

BEGIN;

ALTER TABLE silver.director_appointment
    ADD COLUMN IF NOT EXISTS notes TEXT;

COMMIT;
