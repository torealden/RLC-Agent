-- Migration 176: The Feedstock Report — Issue 0 spec reconciliation
--
-- Per docs/specs/HANDOFF-feedstock-report-issue0.md (2026-08-06 rulings).
-- Reconciles the mig-118 tables with the Issue 0 spec:
--   * feedstock_issue: free_mode flag, 'locked' status, coverage window ends
--     MONDAY settlement close (ruled 2026-08-06; was Friday), render output paths
--   * price_dashboard_snapshot: canonical feedstock_code, last_observed,
--     carry-forward flag (staleness rules are renderer-enforced, but the
--     snapshot must carry the facts the renderer needs)
--   * credit_stack_snapshot: per-(issue, instrument) row grain for the credit
--     monitor (the mig-118 per-feedstock stack matrix columns are kept for the
--     later paid-tier stack table; Issue 0 uses instrument rows)
--
-- All tables have zero rows as of 2026-08-06, so these ALTERs are shape-only.

BEGIN;

-- =============================================================
-- feedstock_issue
-- =============================================================
ALTER TABLE reports.feedstock_issue
    ADD COLUMN IF NOT EXISTS free_mode      BOOLEAN NOT NULL DEFAULT TRUE,
    ADD COLUMN IF NOT EXISTS coverage_start DATE,
    ADD COLUMN IF NOT EXISTS html_path      TEXT,
    ADD COLUMN IF NOT EXISTS linkedin_kit_path TEXT;

-- 'locked' added to the lifecycle (draft -> locked -> published).
ALTER TABLE reports.feedstock_issue DROP CONSTRAINT IF EXISTS feedstock_issue_status_chk;
ALTER TABLE reports.feedstock_issue ADD CONSTRAINT feedstock_issue_status_chk
    CHECK (status IN ('draft','in_review','locked','published','archived'));

COMMENT ON COLUMN reports.feedstock_issue.week_ending IS
'Coverage window end = MONDAY settlement close (ruled 2026-08-06; snapshots run Monday evening, publish Tuesday). Column name predates the ruling.';
COMMENT ON COLUMN reports.feedstock_issue.coverage_start IS
'Coverage window start (the Tuesday after the prior issue''s Monday close).';
COMMENT ON COLUMN reports.feedstock_issue.free_mode IS
'TRUE = public/free issue: IFVS-008 gates apply (IFV rank-only, citation whitelist, no Argus/OPIS levels).';

-- =============================================================
-- feedstock_price_dashboard_snapshot
-- =============================================================
ALTER TABLE reports.feedstock_price_dashboard_snapshot
    ADD COLUMN IF NOT EXISTS feedstock_code    VARCHAR(10),
    ADD COLUMN IF NOT EXISTS last_observed     DATE,
    ADD COLUMN IF NOT EXISTS is_carried_forward BOOLEAN NOT NULL DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS is_manual_entry   BOOLEAN NOT NULL DEFAULT FALSE;

COMMENT ON COLUMN reports.feedstock_price_dashboard_snapshot.feedstock_code IS
'11-code canonical vocabulary: SBO, CAN, DCO, BFT, CWG, YG, PLT, UCO, CAM, CAR, OTH.';
COMMENT ON COLUMN reports.feedstock_price_dashboard_snapshot.last_observed IS
'Date of the last ACTUAL print behind this row. Renderer shows it whenever it is not in the coverage week (carry-forward rule, ruled 2026-08-06).';
COMMENT ON COLUMN reports.feedstock_price_dashboard_snapshot.is_carried_forward IS
'TRUE when no print fell inside the coverage week and weekly_avg is the last actual print. W/w change must be NULL on carried rows.';
COMMENT ON COLUMN reports.feedstock_price_dashboard_snapshot.is_manual_entry IS
'TRUE when the row was loaded via `report snapshot manual` (supervised CSV) rather than a collector-fed series.';

-- Rows older than 21 days at snapshot time are NOT inserted (they go to the
-- coverage-expanding note) — enforced in ETL and re-checked by the renderer.

-- =============================================================
-- feedstock_credit_stack_snapshot — per-instrument row grain
-- =============================================================
ALTER TABLE reports.feedstock_credit_stack_snapshot
    ALTER COLUMN feedstock_code DROP NOT NULL,
    ALTER COLUMN region DROP NOT NULL,
    ADD COLUMN IF NOT EXISTS instrument        VARCHAR(40),
    ADD COLUMN IF NOT EXISTS price             NUMERIC,
    ADD COLUMN IF NOT EXISTS wow_change        NUMERIC,
    ADD COLUMN IF NOT EXISTS unit              VARCHAR(20),
    ADD COLUMN IF NOT EXISTS source            VARCHAR(40),
    ADD COLUMN IF NOT EXISTS last_observed     DATE,
    ADD COLUMN IF NOT EXISTS is_carried_forward BOOLEAN NOT NULL DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS is_manual_entry   BOOLEAN NOT NULL DEFAULT FALSE;

CREATE UNIQUE INDEX IF NOT EXISTS uq_feedstock_credit_stack_instrument
    ON reports.feedstock_credit_stack_snapshot (issue_id, instrument)
    WHERE instrument IS NOT NULL;

COMMENT ON COLUMN reports.feedstock_credit_stack_snapshot.instrument IS
'Credit instrument code for the Credit Stack Monitor: D4_RIN, D6_RIN, D3_RIN, LCFS_CA, CFP_OR, CFS_WA, CFR_CA(45Z notes only). One row per (issue, instrument). The mig-118 per-feedstock matrix columns stay for the later paid-tier stack table.';

COMMIT;

-- Verification:
-- SELECT column_name FROM information_schema.columns
--  WHERE table_schema='reports' AND table_name='feedstock_issue' ORDER BY ordinal_position;
