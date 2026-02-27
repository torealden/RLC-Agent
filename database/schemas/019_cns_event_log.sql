-- ============================================================================
-- RLC-Agent CNS: Event Log & LLM Briefing (Status Layer)
-- ============================================================================
-- File: 019_cns_event_log.sql
-- Purpose: System-wide event log that the Desktop LLM reads at session start.
--          Every significant event (collection complete, failure, anomaly,
--          report generated) gets a row. The llm_briefing view shows
--          unacknowledged events so the LLM knows what happened since last time.
-- Depends: 01_schemas.sql (core schema)
-- Source:  WIRING_PLAN.md Part 2
-- ============================================================================

-- ---------------------------------------------------------------------------
-- 1. EVENT_LOG: Every significant system event
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS core.event_log (
    id              SERIAL PRIMARY KEY,
    event_type      TEXT NOT NULL,    -- 'collection_complete', 'collection_failed',
                                     -- 'report_generated', 'data_anomaly',
                                     -- 'schedule_overdue', 'kg_batch_loaded',
                                     -- 'system_alert'
    event_time      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    source          TEXT,             -- collector name or system component
    summary         TEXT NOT NULL,    -- Human-readable: "CFTC COT collected: 847 rows,
                                     --   corn net long +15,234 contracts"
    details         JSONB,            -- Structured data for LLM consumption
    acknowledged    BOOLEAN DEFAULT FALSE,  -- LLM marks TRUE after reading
    priority        INTEGER DEFAULT 3       -- 1=critical, 2=important, 3=info
);

CREATE INDEX IF NOT EXISTS idx_event_log_unread
    ON core.event_log (acknowledged, event_time DESC)
    WHERE acknowledged = FALSE;

CREATE INDEX IF NOT EXISTS idx_event_log_type
    ON core.event_log (event_type, event_time DESC);

CREATE INDEX IF NOT EXISTS idx_event_log_source
    ON core.event_log (source, event_time DESC);

COMMENT ON TABLE core.event_log IS
    'CNS Status Layer: Every significant system event gets a row here. '
    'The LLM reads unacknowledged events via llm_briefing view at session start. '
    'Priority: 1=critical, 2=important, 3=info.';


-- ---------------------------------------------------------------------------
-- 2. LLM_BRIEFING: What the LLM needs to know right now
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW core.llm_briefing AS
SELECT
    id,
    event_type,
    event_time,
    source,
    summary,
    details,
    priority
FROM core.event_log
WHERE acknowledged = FALSE
ORDER BY priority ASC, event_time DESC;

COMMENT ON VIEW core.llm_briefing IS
    'Unacknowledged events for Desktop LLM. Query this at session start. '
    'After reading, UPDATE core.event_log SET acknowledged = TRUE WHERE id IN (...).';


-- ---------------------------------------------------------------------------
-- 3. HELPER: Function to log an event (convenient for collectors/agents)
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION core.log_event(
    p_event_type TEXT,
    p_source TEXT,
    p_summary TEXT,
    p_details JSONB DEFAULT NULL,
    p_priority INTEGER DEFAULT 3
) RETURNS INTEGER AS $$
DECLARE
    v_id INTEGER;
BEGIN
    INSERT INTO core.event_log (event_type, source, summary, details, priority)
    VALUES (p_event_type, p_source, p_summary, p_details, p_priority)
    RETURNING id INTO v_id;

    RETURN v_id;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION core.log_event IS
    'Log a system event. Returns the event ID. '
    'Example: SELECT core.log_event(''collection_complete'', ''cftc_cot'', ''CFTC COT collected: 847 rows'', ''{"rows": 847}'', 3);';


-- ---------------------------------------------------------------------------
-- 4. HELPER: Acknowledge events (LLM calls this after reading briefing)
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION core.acknowledge_events(
    p_event_ids INTEGER[]
) RETURNS INTEGER AS $$
DECLARE
    v_count INTEGER;
BEGIN
    UPDATE core.event_log
    SET acknowledged = TRUE
    WHERE id = ANY(p_event_ids)
      AND acknowledged = FALSE;

    GET DIAGNOSTICS v_count = ROW_COUNT;
    RETURN v_count;
END;
$$ LANGUAGE plpgsql;


-- ---------------------------------------------------------------------------
-- Verification
-- ---------------------------------------------------------------------------
DO $$
BEGIN
    RAISE NOTICE '019_cns_event_log.sql executed successfully:';
    RAISE NOTICE '  - core.event_log: table created';
    RAISE NOTICE '  - core.llm_briefing: view created';
    RAISE NOTICE '  - core.log_event(): helper function created';
    RAISE NOTICE '  - core.acknowledge_events(): helper function created';
END $$;
