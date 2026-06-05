-- Migration 132: bronze.census_trade_audit_acknowledged
--
-- Pair-level acknowledgment for the gap auditor. When an (hs_code, flow)
-- pair has been reviewed and the residual gaps are confirmed as real-world
-- (i.e., Census itself doesn't report some months but we've pulled every
-- year that could be pulled), an entry here suppresses the pair from the
-- audit's "needs attention" list on future runs.
--
-- Semantics:
--   bronze.census_trade_verified_empty       (hs, flow, YEAR)   = Census API
--                                                                returned 0 for that year
--   bronze.census_trade_audit_acknowledged   (hs, flow)         = analyst has
--                                                                reviewed remaining gaps
--                                                                and accepts them
--
-- Per Tore (2026-06-05): for some commodities (copra, palm/PK oilcake, etc.)
-- US really doesn't trade much; gaps in those subheadings are expected,
-- not bugs.

BEGIN;

CREATE TABLE IF NOT EXISTS bronze.census_trade_audit_acknowledged (
    id              BIGSERIAL PRIMARY KEY,
    hs_code         VARCHAR(20)  NOT NULL,
    flow            VARCHAR(10)  NOT NULL,
    acknowledged_at TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    acknowledged_by VARCHAR(60),
    notes           TEXT,
    UNIQUE (hs_code, flow)
);

CREATE INDEX IF NOT EXISTS idx_ack_hs_flow
    ON bronze.census_trade_audit_acknowledged (hs_code, flow);

COMMENT ON TABLE bronze.census_trade_audit_acknowledged IS
'Pair-level acknowledgment of residual gaps in bronze.census_trade. Audit script suppresses acknowledged pairs from the "needs attention" output. Used when a (hs_code, flow) pair has been re-pulled but Census itself reports only partial months — the remaining gap is real-world, not an ingest bug.';

COMMIT;
