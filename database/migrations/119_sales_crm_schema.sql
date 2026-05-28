-- Migration 119: Sales CRM schema
--
-- Per docs/specs/feedstock_report_plumbing_plan_v2.md.
-- Schema for tracking contacts, prospects, subscribers, and sales
-- activity. Designed to make Rich's workflow easy (Tore's note:
-- "the easier we can make that kind of stuff for him, the better").
--
-- Pattern: contacts are people/companies in the world. Prospects =
-- contacts in our sales pipeline for a specific publication.
-- Subscribers = contacts who have purchased. Activity = log of every
-- touchpoint. issue_delivery = per-issue per-subscriber delivery log
-- (used when we wire actual email send).

BEGIN;

CREATE SCHEMA IF NOT EXISTS sales;

-- =============================================================
-- People + companies
-- =============================================================
CREATE TABLE IF NOT EXISTS sales.contact (
    id              SERIAL PRIMARY KEY,
    first_name      VARCHAR(60),
    last_name       VARCHAR(60),
    email           VARCHAR(120) UNIQUE,
    company         VARCHAR(120),
    title           VARCHAR(120),
    linkedin_url    TEXT,
    phone           VARCHAR(40),
    notes           TEXT,
    source          VARCHAR(60),              -- how we found them: 'referral', 'conference', 'inbound', 'cold', etc.
    introduced_by   VARCHAR(120),             -- referral chain
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    updated_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_sales_contact_company ON sales.contact (company);
CREATE INDEX IF NOT EXISTS idx_sales_contact_email   ON sales.contact (email);


-- =============================================================
-- Pipeline state per contact per publication
-- =============================================================
CREATE TABLE IF NOT EXISTS sales.prospect (
    id              SERIAL PRIMARY KEY,
    contact_id      INTEGER NOT NULL REFERENCES sales.contact(id) ON DELETE CASCADE,
    publication     VARCHAR(40) NOT NULL,     -- 'feedstock_report', 'hb_report', etc.
    stage           VARCHAR(30) NOT NULL,
                    CONSTRAINT prospect_stage_chk
                    CHECK (stage IN ('prospect','contacted','engaged','trial',
                                     'negotiating','won','lost','paused','unsubscribed')),
    last_contact    DATE,
    next_action     TEXT,
    next_action_due DATE,
    owner           VARCHAR(60),              -- 'rich', 'tore', etc.
    notes           TEXT,
    won_at          TIMESTAMPTZ,
    lost_at         TIMESTAMPTZ,
    lost_reason     VARCHAR(120),
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    updated_at      TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (contact_id, publication)
);

CREATE INDEX IF NOT EXISTS idx_sales_prospect_stage     ON sales.prospect (publication, stage);
CREATE INDEX IF NOT EXISTS idx_sales_prospect_owner     ON sales.prospect (owner);
CREATE INDEX IF NOT EXISTS idx_sales_prospect_next_due  ON sales.prospect (next_action_due);


-- =============================================================
-- Subscribers (paid + comp)
-- =============================================================
CREATE TABLE IF NOT EXISTS sales.subscriber (
    id              SERIAL PRIMARY KEY,
    contact_id      INTEGER NOT NULL REFERENCES sales.contact(id) ON DELETE CASCADE,
    publication     VARCHAR(40) NOT NULL,
    plan            VARCHAR(30),              -- 'paid', 'comp', 'trial'
                    CONSTRAINT subscriber_plan_chk
                    CHECK (plan IN ('paid','comp','trial') OR plan IS NULL),
    started_at      DATE NOT NULL,
    ended_at        DATE,
    renews_at       DATE,
    annual_price    NUMERIC,
    payment_status  VARCHAR(20) DEFAULT 'active',
                    CONSTRAINT subscriber_payment_chk
                    CHECK (payment_status IN ('active','past_due','cancelled')),
    delivery_email  VARCHAR(120),             -- if different from contact.email
    notes           TEXT,
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    updated_at      TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (contact_id, publication, started_at)
);

CREATE INDEX IF NOT EXISTS idx_sales_subscriber_pub_active
    ON sales.subscriber (publication, payment_status)
    WHERE payment_status = 'active';


-- =============================================================
-- Per-issue delivery log
-- =============================================================
CREATE TABLE IF NOT EXISTS sales.issue_delivery (
    id              SERIAL PRIMARY KEY,
    publication     VARCHAR(40) NOT NULL,
    issue_id        INTEGER,                  -- FK to publication's issue table (loose — depends on publication)
    subscriber_id   INTEGER REFERENCES sales.subscriber(id) ON DELETE CASCADE,
    sent_at         TIMESTAMPTZ,
    delivery_method VARCHAR(20),              -- 'email', 'website_download', 'manual'
    opened_at       TIMESTAMPTZ,
    clicked_at      TIMESTAMPTZ,
    notes           TEXT
);

CREATE INDEX IF NOT EXISTS idx_sales_delivery_pub_issue ON sales.issue_delivery (publication, issue_id);
CREATE INDEX IF NOT EXISTS idx_sales_delivery_subscriber ON sales.issue_delivery (subscriber_id);


-- =============================================================
-- Sales activity log (Rich's daily entries)
-- =============================================================
CREATE TABLE IF NOT EXISTS sales.activity (
    id              SERIAL PRIMARY KEY,
    contact_id      INTEGER REFERENCES sales.contact(id) ON DELETE CASCADE,
    activity_type   VARCHAR(30),
                    CONSTRAINT activity_type_chk
                    CHECK (activity_type IN
                           ('call','email','meeting','demo','sample_sent',
                            'quote_sent','follow_up','other')),
    occurred_at     TIMESTAMPTZ NOT NULL,
    owner           VARCHAR(60),
    summary         TEXT,
    outcome         VARCHAR(30),              -- 'positive', 'neutral', 'negative'
                    CONSTRAINT activity_outcome_chk
                    CHECK (outcome IN ('positive','neutral','negative') OR outcome IS NULL),
    follow_up       TEXT,
    follow_up_due   DATE,
    created_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_sales_activity_contact      ON sales.activity (contact_id, occurred_at DESC);
CREATE INDEX IF NOT EXISTS idx_sales_activity_owner_recent ON sales.activity (owner, occurred_at DESC);
CREATE INDEX IF NOT EXISTS idx_sales_activity_followup_due ON sales.activity (follow_up_due) WHERE follow_up_due IS NOT NULL;


COMMIT;

-- Verification:
-- SELECT table_name FROM information_schema.tables WHERE table_schema = 'sales' ORDER BY table_name;
