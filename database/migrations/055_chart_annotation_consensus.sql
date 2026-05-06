-- Migration 055: Consensus annotation table for best-of-N chart extraction
--
-- Date: 2026-05-06
--
-- Why:
--   Per memory feedback_llm_extraction_variance.md, single-run LLM
--   extraction shows 50-70% bidirectional variance on structured docs.
--   For chart annotations the user is now running 3 extractions per
--   page; we need a place to store the cross-run CONSENSUS — events
--   that appeared in multiple runs are higher-confidence; events that
--   appeared in only 1 of N are flagged as uncertain.
--
--   bronze.handwritten_chart_annotation already supports multiple
--   extractor_versions (v1-r1, v1-r2, v1-r3) coexisting per page.
--   This table sits on top: one row per consensus event, linking to
--   the bronze rows that fed it.

CREATE TABLE IF NOT EXISTS silver.market_event_consensus (
    id                       BIGSERIAL PRIMARY KEY,
    source_file_hash         TEXT NOT NULL,
    page_number              INTEGER NOT NULL,
    chart_contract           TEXT,
    chart_commodity          TEXT,
    market_id                TEXT NOT NULL,
    -- Consensus values
    consensus_date           DATE,
    consensus_text           TEXT NOT NULL,    -- most common verbatim across runs
    topic_key                TEXT NOT NULL,
    consensus_polarity       NUMERIC,          -- median across runs
    consensus_intensity      NUMERIC,
    -- Cross-run agreement metrics
    n_runs_total             INTEGER NOT NULL,
    n_runs_with_event        INTEGER NOT NULL,
    agreement_score          NUMERIC GENERATED ALWAYS AS
        (CAST(n_runs_with_event AS NUMERIC) / NULLIF(n_runs_total, 0)) STORED,
    median_confidence        NUMERIC,
    polarity_stdev           NUMERIC,          -- variance check; high = runs disagreed
    -- Traceability
    bronze_ids               BIGINT[] NOT NULL,
    consolidator_version     TEXT NOT NULL,    -- 'v1' for our first consolidation pass
    consolidated_at          TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    UNIQUE (source_file_hash, page_number, consensus_date, consensus_text, consolidator_version)
);

CREATE INDEX IF NOT EXISTS idx_event_consensus_date
    ON silver.market_event_consensus (consensus_date);
CREATE INDEX IF NOT EXISTS idx_event_consensus_agreement
    ON silver.market_event_consensus (agreement_score DESC);
CREATE INDEX IF NOT EXISTS idx_event_consensus_topic
    ON silver.market_event_consensus (topic_key);

COMMENT ON TABLE silver.market_event_consensus IS
'Cross-run consensus events from chart annotation extraction. agreement_score = events appearing in N of N_total runs / N_total — events at 1.0 are unanimously read; at 0.33 only one of three runs caught it. Use agreement_score>=0.67 (i.e., 2-of-3) for calibration; lower = candidate for human review.';
