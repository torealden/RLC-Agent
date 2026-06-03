-- Migration 131: bronze.census_trade_verified_empty
--
-- Memoizes (hs_code, flow, year) tuples that the census_trade_gap_audit
-- script has confirmed are genuinely empty in the Census Bureau API
-- (return 0 records on backfill attempt). Used to:
--   1. Skip futile re-fetches on the next --fix run
--   2. Distinguish "real-world empty" gaps from "ingest bug" gaps in
--      the audit report
--
-- Per Tore (2026-06-03): after the first sweep filled 777 real records
-- across 13 code/flow pairs, the audit's re-report still flagged those
-- same pairs even though most of the remaining "gaps" are Census-empty.
-- This table makes the audit idempotent — subsequent runs only chase
-- new gaps, not previously-verified-empty ones.

BEGIN;

CREATE TABLE IF NOT EXISTS bronze.census_trade_verified_empty (
    id              BIGSERIAL PRIMARY KEY,
    hs_code         VARCHAR(20)  NOT NULL,
    flow            VARCHAR(10)  NOT NULL,
    year            INTEGER      NOT NULL,
    verified_at     TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    verified_count  INTEGER      NOT NULL DEFAULT 1,
    notes           TEXT,
    UNIQUE (hs_code, flow, year)
);

CREATE INDEX IF NOT EXISTS idx_cte_hs_flow
    ON bronze.census_trade_verified_empty (hs_code, flow);

COMMENT ON TABLE bronze.census_trade_verified_empty IS
'Memoization of (hs_code, flow, year) tuples that Census API has confirmed empty. Populated by scripts/census_trade_gap_audit.py --fix when an API call returns 0 records. Subsequent audit runs skip these tuples to avoid futile re-fetches.';

COMMIT;

-- Verification:
-- SELECT COUNT(*) FROM bronze.census_trade_verified_empty;
-- SELECT hs_code, flow, COUNT(*) AS years_verified_empty
-- FROM bronze.census_trade_verified_empty
-- GROUP BY hs_code, flow ORDER BY hs_code, flow;
