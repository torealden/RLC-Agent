-- =============================================================================
-- WASDE Training Iterations
-- Tracks iterative LLM report training: each run, its scores, and feedback
-- =============================================================================

-- ---------------------------------------------------------------------------
-- Training Runs — one row per training session (a batch of iterations)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS core.training_runs (
    run_id          SERIAL PRIMARY KEY,
    report_type     VARCHAR(50) NOT NULL DEFAULT 'wasde',
    phase           INTEGER NOT NULL DEFAULT 1,    -- 1=data_accuracy, 2=delta, 3=context, 4=full
    started_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    finished_at     TIMESTAMPTZ,
    status          VARCHAR(20) NOT NULL DEFAULT 'running',  -- running, completed, failed
    config          JSONB DEFAULT '{}',             -- prompt version, model, phase config
    notes           TEXT
);

-- ---------------------------------------------------------------------------
-- Training Iterations — one row per LLM generation within a run
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS core.training_iterations (
    iteration_id    SERIAL PRIMARY KEY,
    run_id          INTEGER NOT NULL REFERENCES core.training_runs(run_id),
    iteration_num   INTEGER NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    -- LLM output
    prompt_hash     VARCHAR(64),
    model_id        VARCHAR(100),
    narrative       TEXT,
    tokens_in       INTEGER,
    tokens_out      INTEGER,
    cost_usd        NUMERIC(10,6),
    latency_ms      INTEGER,

    -- Auto-evaluation scores (0.0 to 1.0)
    score_data_accuracy   NUMERIC(4,3),   -- numbers match DB
    score_completeness    NUMERIC(4,3),   -- all sections present
    score_delta_accuracy  NUMERIC(4,3),   -- MoM changes correct
    score_no_hallucination NUMERIC(4,3),  -- no fabricated numbers
    score_overall         NUMERIC(4,3),   -- weighted composite

    -- Human feedback (Felipe fills these)
    human_rating    INTEGER,              -- 1-5 scale
    human_feedback  TEXT,                 -- free-form corrections
    human_approved  BOOLEAN DEFAULT FALSE,
    reviewed_at     TIMESTAMPTZ,
    reviewed_by     VARCHAR(50),

    -- Data snapshot used (for reproducibility)
    data_snapshot   JSONB,                -- the exact data fed to the LLM

    UNIQUE (run_id, iteration_num)
);

-- ---------------------------------------------------------------------------
-- Training Feedback Log — structured corrections for prompt refinement
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS core.training_feedback (
    feedback_id     SERIAL PRIMARY KEY,
    iteration_id    INTEGER NOT NULL REFERENCES core.training_iterations(iteration_id),
    category        VARCHAR(50) NOT NULL,  -- 'wrong_number', 'missing_section', 'hallucination', 'tone', 'inference'
    severity        INTEGER NOT NULL DEFAULT 2,  -- 1=minor, 2=moderate, 3=critical
    description     TEXT NOT NULL,
    correction      TEXT,                  -- what the correct output should be
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Index for quick lookups
CREATE INDEX IF NOT EXISTS idx_training_iterations_run
    ON core.training_iterations(run_id, iteration_num);
CREATE INDEX IF NOT EXISTS idx_training_feedback_iter
    ON core.training_feedback(iteration_id);


-- ---------------------------------------------------------------------------
-- View: Latest training progress per report type
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW core.training_progress AS
SELECT
    tr.report_type,
    tr.phase,
    tr.run_id,
    COUNT(ti.iteration_id) AS total_iterations,
    COUNT(CASE WHEN ti.human_approved THEN 1 END) AS approved_count,
    MAX(ti.score_overall) AS best_score,
    AVG(ti.score_overall) AS avg_score,
    MAX(ti.human_rating) AS best_human_rating,
    tr.started_at,
    tr.status
FROM core.training_runs tr
LEFT JOIN core.training_iterations ti ON ti.run_id = tr.run_id
GROUP BY tr.run_id, tr.report_type, tr.phase, tr.started_at, tr.status
ORDER BY tr.started_at DESC;
