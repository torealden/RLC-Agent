-- ============================================================================
-- Migration 034: reports schema — calls_register + release_calendar
-- ============================================================================
-- Two operational tables for the BBD weekly report:
--
--   reports.calls_register   — running ledger of forward calls made in prior
--                              issues, with status (open/closed/hit/miss/partial).
--                              Builds analytical credibility issue-over-issue and
--                              becomes a marketing asset after ~12 issues.
--
--   reports.release_calendar — known data-release dates (WASDE, EIA weekly
--                              petroleum, NASS reports, EMTS, Census FT-900,
--                              EPA RFS, CARB LCFS, etc.) so the Calendar
--                              section in the weekly can be auto-populated and
--                              subscribers are never surprised by a release.
-- ============================================================================

BEGIN;

CREATE SCHEMA IF NOT EXISTS reports;

-- ----------------------------------------------------------------------------
-- 1. calls_register
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS reports.calls_register (
    id              SERIAL PRIMARY KEY,
    issue_date      DATE        NOT NULL,
    issue_number    INTEGER,
    section         VARCHAR(40),  -- e.g., 'executive_read', 'market_signals', 'implied_feedstock_value'
    call_text       TEXT        NOT NULL,
    -- Quantitative call (optional)
    metric          VARCHAR(60),  -- e.g., 'gulf_rd_margin', 'd4_rin_price', 'sbo_stocks'
    direction       VARCHAR(10),  -- 'up', 'down', 'flat', 'range'
    target_value    NUMERIC,
    target_unit     VARCHAR(20),
    target_date     DATE,         -- when the call should be evaluated
    confidence      VARCHAR(10),  -- 'high', 'med', 'low'
    -- Outcome tracking
    status          VARCHAR(15)  NOT NULL DEFAULT 'open'
                    CHECK (status IN ('open','closed','hit','miss','partial','withdrawn')),
    actual_value    NUMERIC,
    actual_date     DATE,
    outcome_notes   TEXT,
    -- Provenance
    author          VARCHAR(60),
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_calls_register_status      ON reports.calls_register (status);
CREATE INDEX IF NOT EXISTS idx_calls_register_target_date ON reports.calls_register (target_date);
CREATE INDEX IF NOT EXISTS idx_calls_register_issue_date  ON reports.calls_register (issue_date);

COMMENT ON TABLE reports.calls_register IS
'Forward calls made in the BBD weekly report, tracked through to outcome.
 Drives section 11 (Calls Register). Read-only queries by issue/status/metric.';

-- ----------------------------------------------------------------------------
-- 2. release_calendar
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS reports.release_calendar (
    id              SERIAL PRIMARY KEY,
    release_code    VARCHAR(40)  NOT NULL,  -- e.g., 'WASDE', 'EIA_PSW', 'NASS_FATS_OILS', 'EMTS', 'CENSUS_FT900', 'CARB_LCFS'
    release_name    VARCHAR(120) NOT NULL,  -- human-readable
    publisher       VARCHAR(60)  NOT NULL,  -- USDA, EIA, EPA, Census, CARB, etc.
    release_date    DATE         NOT NULL,
    release_time    TIME,                   -- ET, when known (e.g., 12:00 for WASDE)
    -- What does this release affect?
    covers_period   VARCHAR(60),            -- e.g., 'WoW Mar 14', 'Feb 2026', 'MY 2025/26'
    affects_sections VARCHAR(200),          -- comma list of report section codes
    -- Operational
    importance      VARCHAR(10) DEFAULT 'normal'
                    CHECK (importance IN ('high','normal','low')),
    is_holiday_shifted BOOLEAN  DEFAULT FALSE,
    notes           TEXT,
    source_url      TEXT,
    -- Provenance
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (release_code, release_date)
);

CREATE INDEX IF NOT EXISTS idx_release_calendar_date ON reports.release_calendar (release_date);
CREATE INDEX IF NOT EXISTS idx_release_calendar_code ON reports.release_calendar (release_code);

COMMENT ON TABLE reports.release_calendar IS
'Known publication dates for data series feeding the weekly report.
 Drives section 12 (Calendar). Populated from each collector''s known
 release schedule; manual entry for one-off events (hearings, deadlines).';

-- ----------------------------------------------------------------------------
-- 3. Convenience views
-- ----------------------------------------------------------------------------

-- Open + due-soon calls
CREATE OR REPLACE VIEW reports.v_calls_open AS
SELECT id, issue_date, issue_number, section, call_text, metric, direction,
       target_value, target_unit, target_date, confidence,
       (target_date - CURRENT_DATE) AS days_until_target
FROM reports.calls_register
WHERE status = 'open'
ORDER BY target_date NULLS LAST, issue_date;

-- Hit-rate by section
CREATE OR REPLACE VIEW reports.v_calls_hit_rate AS
SELECT section,
       COUNT(*)                                 AS total_evaluated,
       COUNT(*) FILTER (WHERE status = 'hit')   AS hits,
       COUNT(*) FILTER (WHERE status = 'miss')  AS misses,
       COUNT(*) FILTER (WHERE status = 'partial') AS partials,
       ROUND(100.0 * COUNT(*) FILTER (WHERE status = 'hit')
             / NULLIF(COUNT(*) FILTER (WHERE status IN ('hit','miss','partial')), 0), 1) AS hit_pct
FROM reports.calls_register
WHERE status IN ('hit','miss','partial')
GROUP BY section
ORDER BY total_evaluated DESC;

-- This-week calendar
CREATE OR REPLACE VIEW reports.v_release_calendar_this_week AS
SELECT release_date, release_time, release_code, release_name, publisher,
       covers_period, affects_sections, importance, notes
FROM reports.release_calendar
WHERE release_date BETWEEN date_trunc('week', CURRENT_DATE)::DATE
                       AND (date_trunc('week', CURRENT_DATE) + INTERVAL '6 days')::DATE
ORDER BY release_date, release_time NULLS LAST;

-- ----------------------------------------------------------------------------
-- 4. Permissions
-- ----------------------------------------------------------------------------
GRANT USAGE ON SCHEMA reports TO PUBLIC;
GRANT SELECT, INSERT, UPDATE ON reports.calls_register   TO PUBLIC;
GRANT SELECT, INSERT, UPDATE ON reports.release_calendar TO PUBLIC;
GRANT SELECT ON reports.v_calls_open                     TO PUBLIC;
GRANT SELECT ON reports.v_calls_hit_rate                 TO PUBLIC;
GRANT SELECT ON reports.v_release_calendar_this_week     TO PUBLIC;
GRANT USAGE, SELECT ON SEQUENCE reports.calls_register_id_seq    TO PUBLIC;
GRANT USAGE, SELECT ON SEQUENCE reports.release_calendar_id_seq  TO PUBLIC;

COMMIT;
