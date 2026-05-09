-- Migration 060: silver.due_diligence_report
-- Storage for FIC Layer 4 due-diligence reports.
--
-- Each row is one full report generated for one facility at one point in
-- time. Generation pulls facility profile + edges + permits + sentiment +
-- news + operator's SEC filings (when public) + KG context, then calls
-- Claude (cloud, since these are client-facing) with a structured prompt.
-- Both the markdown rendering and the structured JSON are persisted, plus
-- the input data summary for audit.

BEGIN;

CREATE SCHEMA IF NOT EXISTS silver;

CREATE TABLE IF NOT EXISTS silver.due_diligence_report (
    id                      BIGSERIAL PRIMARY KEY,
    facility_id             TEXT NOT NULL,
    generated_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    generated_by            TEXT NOT NULL DEFAULT 'fic_user',
    model                   TEXT NOT NULL,
    prompt_version          TEXT NOT NULL,

    report_json             JSONB NOT NULL,
    report_markdown         TEXT NOT NULL,

    -- What was fed into the report (so we can audit / reproduce)
    input_summary           JSONB,

    -- Cost / usage metadata
    input_tokens            INTEGER,
    output_tokens           INTEGER,
    cost_usd                NUMERIC(10, 4),
    elapsed_sec             NUMERIC(8, 2),

    notes                   TEXT
);

CREATE INDEX IF NOT EXISTS idx_dd_report_facility
    ON silver.due_diligence_report (facility_id, generated_at DESC);
CREATE INDEX IF NOT EXISTS idx_dd_report_generated_at
    ON silver.due_diligence_report (generated_at DESC);

COMMENT ON TABLE silver.due_diligence_report IS
'FIC Layer 4 — structured due-diligence reports per facility, generated '
'on demand from the FIC console. One row per (facility_id, generated_at).';

COMMIT;
