-- 141_facility_effective_dated_histories.sql
-- Effective-dated capacity/status history (design v1.6 §3.2). Uses the PRE-EXISTING (empty)
-- reference.facility_capacity_history — an effective_date change-log carrying both nameplate and
-- status — rather than creating a parallel from/to table (avoids more sprawl). This is the as-of-
-- period truth the allocator queries so back-runs use each period's real facility state.
-- Seed: scripts/seed_facility_histories.py. Query pattern: latest effective_date <= period.
BEGIN;
CREATE INDEX IF NOT EXISTS ix_fac_cap_hist_asof
    ON reference.facility_capacity_history (facility_id, effective_date DESC);
COMMIT;
