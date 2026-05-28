-- Migration 118: The Feedstock Report — schemas
--
-- Per docs/specs/feedstock_report_plumbing_plan_v2.md.
-- Implements the publication-specific tables for The Feedstock Report
-- and the shared policy_events table; extends the existing
-- reports.calls_register with a publication column.
--
-- Naming convention (per client-separation rule, memory:
-- feedback_client_process_separation):
--   reports.feedstock_*     = publication-specific
--   reports.policy_events   = shared across all RLC reports
--   reports.calls_register  = shared, distinguished by publication col
--   reports.release_calendar= shared

BEGIN;

-- =============================================================
-- One row per issue of The Feedstock Report
-- =============================================================
CREATE TABLE IF NOT EXISTS reports.feedstock_issue (
    id              SERIAL PRIMARY KEY,
    issue_number    INTEGER UNIQUE NOT NULL,
    issue_date      DATE NOT NULL,            -- Sunday of publication
    week_ending     DATE NOT NULL,            -- Friday of the week covered
    status          VARCHAR(20) NOT NULL DEFAULT 'draft',
                    CONSTRAINT feedstock_issue_status_chk
                    CHECK (status IN ('draft','in_review','published','archived')),
    title           TEXT,
    cover_lead      TEXT,                     -- the "INSIDE THIS ISSUE" cover blurb
    published_at    TIMESTAMPTZ,
    docx_path       TEXT,
    pdf_path        TEXT,
    dropbox_path    TEXT,
    website_url     TEXT,
    notion_page_id  VARCHAR(64),
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    updated_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_feedstock_issue_date     ON reports.feedstock_issue (issue_date DESC);
CREATE INDEX IF NOT EXISTS idx_feedstock_issue_status   ON reports.feedstock_issue (status);


-- =============================================================
-- One row per section per issue
-- =============================================================
CREATE TABLE IF NOT EXISTS reports.feedstock_section_content (
    id              SERIAL PRIMARY KEY,
    issue_id        INTEGER NOT NULL REFERENCES reports.feedstock_issue(id) ON DELETE CASCADE,
    section_code    VARCHAR(40) NOT NULL,
                    -- 'executive_read', 'price_dashboard', 'credit_stack',
                    -- 'production_tracker', 'implied_feedstock_value',
                    -- 'supply_demand', 'policy_monitor', 'trade_flow',
                    -- 'market_signals', 'news', 'calls_register', 'week_ahead'
    section_number  INTEGER NOT NULL,
    title           TEXT,
    prose           TEXT,
    bullets         JSONB,
    data_snapshot   JSONB,
    chart_paths     TEXT[],
    word_count      INTEGER,
    author          VARCHAR(40),
    last_edited_at  TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (issue_id, section_code)
);

CREATE INDEX IF NOT EXISTS idx_feedstock_section_issue ON reports.feedstock_section_content (issue_id);


-- =============================================================
-- Curated news items per issue (Section 10)
-- =============================================================
CREATE TABLE IF NOT EXISTS reports.feedstock_news_items (
    id              SERIAL PRIMARY KEY,
    issue_id        INTEGER REFERENCES reports.feedstock_issue(id) ON DELETE CASCADE,
    headline        TEXT NOT NULL,
    url             TEXT,
    source          VARCHAR(80),
    short_take      TEXT,
    importance      VARCHAR(10),
                    CONSTRAINT feedstock_news_importance_chk
                    CHECK (importance IN ('high','med','low') OR importance IS NULL),
    published_at    DATE,
    sort_order      INTEGER,                  -- for display ordering within issue
    added_at        TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_feedstock_news_issue ON reports.feedstock_news_items (issue_id, sort_order);


-- =============================================================
-- Policy events (SHARED — both reports may reference)
-- =============================================================
CREATE TABLE IF NOT EXISTS reports.policy_events (
    id              SERIAL PRIMARY KEY,
    event_date      DATE NOT NULL,
    jurisdiction    VARCHAR(40),              -- 'US Federal', 'CA', 'EU', 'Brazil', etc.
    category        VARCHAR(40),              -- 'RFS', '45Z', 'LCFS', 'tariff', 'mandate', etc.
    headline        TEXT NOT NULL,
    what            TEXT,
    so_what         TEXT,
    source_url      TEXT,
    impact_horizon  VARCHAR(20),              -- 'immediate', '0-3mo', '3-12mo', '12+mo'
    created_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_policy_events_date     ON reports.policy_events (event_date DESC);
CREATE INDEX IF NOT EXISTS idx_policy_events_category ON reports.policy_events (category);


-- =============================================================
-- Frozen credit stack snapshot (Section 03 reproducibility)
-- =============================================================
CREATE TABLE IF NOT EXISTS reports.feedstock_credit_stack_snapshot (
    id              SERIAL PRIMARY KEY,
    issue_id        INTEGER REFERENCES reports.feedstock_issue(id) ON DELETE CASCADE,
    feedstock_code  VARCHAR(10) NOT NULL,
    region          VARCHAR(20) NOT NULL,
    d4_rin_cents    NUMERIC,
    lcfs_per_gal    NUMERIC,
    forty_five_z    NUMERIC,
    btc_per_gal     NUMERIC,
    state_credit    NUMERIC,
    total_stack     NUMERIC,
    notes           TEXT,
    UNIQUE (issue_id, feedstock_code, region)
);


-- =============================================================
-- Frozen price dashboard snapshot (Section 02, "the price page")
-- =============================================================
CREATE TABLE IF NOT EXISTS reports.feedstock_price_dashboard_snapshot (
    id              SERIAL PRIMARY KEY,
    issue_id        INTEGER REFERENCES reports.feedstock_issue(id) ON DELETE CASCADE,
    product         VARCHAR(40) NOT NULL,     -- e.g., 'soybean_oil', 'tallow', 'd4_rin', 'lcfs'
    location        VARCHAR(60),              -- e.g., 'Iowa', 'Gulf', 'PNW', 'California'
    week_ending     DATE NOT NULL,
    weekly_avg      NUMERIC,
    wow_change_pct  NUMERIC,
    mom_change_pct  NUMERIC,
    yoy_change_pct  NUMERIC,
    range_52w_low   NUMERIC,                  -- for range-bar rendering
    range_52w_high  NUMERIC,
    unit            VARCHAR(20),
    source          VARCHAR(40),
    is_placeholder  BOOLEAN DEFAULT FALSE,    -- true while waiting on intl data
    UNIQUE (issue_id, product, location)
);


-- =============================================================
-- Extend reports.calls_register to track publication
-- =============================================================
ALTER TABLE reports.calls_register
    ADD COLUMN IF NOT EXISTS publication VARCHAR(40) DEFAULT 'feedstock_report';

CREATE INDEX IF NOT EXISTS idx_calls_register_publication ON reports.calls_register (publication);

COMMENT ON COLUMN reports.calls_register.publication IS
'Publication this call belongs to: feedstock_report, hb_report, etc. Per client-separation rule, callers filter by this column.';


COMMIT;

-- Verification queries:
-- SELECT table_name FROM information_schema.tables
--   WHERE table_schema = 'reports' ORDER BY table_name;
-- SELECT column_name FROM information_schema.columns
--   WHERE table_schema = 'reports' AND table_name = 'calls_register'
--   ORDER BY ordinal_position;
