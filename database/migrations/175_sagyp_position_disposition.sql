-- 175: sagyp_position_map becomes a disposition registry (unmapped-position monitor support)
--
-- Desktop follow-up on mig 173: exact-code mapping converts code drift into silent series
-- death — a new posicion under a curated family would land bronze-only and vanish from
-- silver with no signal. Fix: every KNOWN posicion sharing a curated HS6 family gets a row
-- here. series_key NULL + is_active false = "reviewed, deliberately bronze-only" (bagged /
-- packaged / specialty variants). The collector then flags any pulled posicion that shares
-- a curated HS6 but has NO row at all — that is a new/drifted code needing review, surfaced
-- as a warning (-> 'partial' run) instead of silent loss.

BEGIN;

ALTER TABLE reference.sagyp_position_map ALTER COLUMN series_key DROP NOT NULL;

COMMENT ON TABLE reference.sagyp_position_map IS
    'Disposition registry for SAGyP posiciones in curated HS6 families. series_key set + '
    'is_active -> promoted to silver.price_mark; series_key NULL + is_active=false -> '
    'reviewed, deliberately bronze-only. A pulled posicion sharing a curated HS6 with NO '
    'row here is flagged by the collector as needing review.';

-- Seed disposition rows for every posicion already observed in bronze that shares a
-- curated HS6 family but is not mapped. Idempotent; re-run after backfills to sweep in
-- historical codes (scripts/audit_sagyp_series_coverage.py reports what this catches).
INSERT INTO reference.sagyp_position_map (posicion, series_key, description, is_active)
SELECT DISTINCT b.posicion, NULL,
       'auto-seeded disposition row (reviewed family, variant not promoted)', false
FROM bronze.sagyp_fob_raw b
WHERE left(b.posicion, 6) IN (SELECT left(posicion, 6) FROM reference.sagyp_position_map
                              WHERE is_active)
  AND NOT EXISTS (SELECT 1 FROM reference.sagyp_position_map m WHERE m.posicion = b.posicion)
ON CONFLICT (posicion) DO NOTHING;

COMMIT;
