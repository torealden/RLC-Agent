-- ============================================================================
-- RLC-Agent Control Plane: LLM Call Log (Audit Trail)
-- ============================================================================
-- File: 026_llm_call_log.sql
-- Purpose: Tamper-evident audit trail for every LLM call made by the
--          autonomous pipeline. Each row records model, tokens, cost,
--          latency, and a SHA-256 hash chain for integrity verification.
--          Sensitivity-aware: high-sensitivity calls store metadata only.
-- Depends: 01_schemas.sql (core schema)
-- Source:  AUTONOMOUS_PIPELINE_GUIDE.md Phase 1
-- ============================================================================

-- ---------------------------------------------------------------------------
-- 1. LLM_CALL_LOG: Every LLM invocation
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS core.llm_call_log (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    pipeline_run_id UUID,                        -- Groups calls within a single pipeline run
    task_type       TEXT NOT NULL,                -- 'narrative', 'analysis', 'summary',
                                                 -- 'chart_config', 'synthesis'
    model_id        TEXT NOT NULL,                -- 'claude-sonnet-4-20250514', 'llama3.1:70b', etc.
    provider        TEXT NOT NULL,                -- 'anthropic' or 'ollama'
    sensitivity     INTEGER NOT NULL DEFAULT 0,   -- 0=public, 1=licensed, 2=internal,
                                                 -- 3=proprietary, 4=restricted
    prompt_hash     VARCHAR(64),                  -- SHA-256 of system+user prompt
    output_hash     VARCHAR(64),                  -- SHA-256 of model output
    tokens_in       INTEGER,
    tokens_out      INTEGER,
    cost_usd        NUMERIC(10,6),                -- Estimated cost of this call
    latency_ms      INTEGER,                      -- Wall-clock milliseconds
    status          TEXT NOT NULL DEFAULT 'success',  -- 'success', 'error', 'timeout'
    error_message   TEXT,                         -- Populated on failure
    details         JSONB,                        -- Full prompt/response (sensitivity <= 1)
                                                 -- or metadata only (sensitivity >= 2)
    context_keys    TEXT[],                       -- KG node keys used as context
    record_hash     VARCHAR(64),                  -- SHA-256 of record body fields
    chain_hash      VARCHAR(64),                  -- SHA-256(record_hash + prior chain_hash)
    called_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ---------------------------------------------------------------------------
-- 2. INDEXES
-- ---------------------------------------------------------------------------
CREATE INDEX IF NOT EXISTS idx_llm_call_log_task_type
    ON core.llm_call_log (task_type, called_at DESC);

CREATE INDEX IF NOT EXISTS idx_llm_call_log_model_id
    ON core.llm_call_log (model_id, called_at DESC);

CREATE INDEX IF NOT EXISTS idx_llm_call_log_pipeline_run
    ON core.llm_call_log (pipeline_run_id, called_at)
    WHERE pipeline_run_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_llm_call_log_called_at
    ON core.llm_call_log (called_at DESC);

-- ---------------------------------------------------------------------------
-- 3. TABLE COMMENT
-- ---------------------------------------------------------------------------
COMMENT ON TABLE core.llm_call_log IS
    'Control Plane audit trail: every LLM call with tamper-evident hash chaining. '
    'record_hash = SHA-256 of body fields; chain_hash = SHA-256(record_hash + prior chain_hash). '
    'Sensitivity >= 2 stores metadata only (no prompt/response text in details). '
    'Cost tracked per-call for budget monitoring.';

-- ---------------------------------------------------------------------------
-- Verification
-- ---------------------------------------------------------------------------
DO $$
BEGIN
    RAISE NOTICE '026_llm_call_log.sql executed successfully:';
    RAISE NOTICE '  - core.llm_call_log: table created';
    RAISE NOTICE '  - 4 indexes created (task_type, model_id, pipeline_run, called_at)';
END $$;
