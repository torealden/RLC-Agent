# The Feedstock Report — Plumbing Plan v2

**Supersedes:** `feedstock_report_plumbing_plan.md` (v1).
**Date:** 2026-05-28.
**Triggered by:** Tore's answers to v1's six open questions + the client-process-separation rule.

This v2 incorporates the rule that **each client report is its own
process tree** (per memory `feedback_client_process_separation`).
The Feedstock Report gets its own orchestrator, prose tables, brand,
and delivery — separate from the Higby Barrett report. Shared layers
(data, KG, callables, chart primitives) remain shared.

---

## Decisions locked in (from Tore's answers)

### 1. Publishing cadence — **Sunday**
Publish Sunday so it's in subscribers' inboxes Monday morning. That's
the standard "Monday-morning market intelligence" rhythm; aligns with
how subscribers consume institutional weeklies. Friday-close data,
weekend writing, Sunday publication.

### 2. Brand — green built around #3C7D22
Anchor color is RLC balance-sheet forest green. Concrete palette
shipped at `src/reports/feedstock_report/brand.py`. Swatch rendered
at `output/visualizations/feedstock_report_brand_swatch.png`.

Palette:
- `FOREST` `#3C7D22` — primary anchor (headers, key data lines)
- `DEEP_FOREST` `#1F4012` — titles, axis labels
- `SAGE` `#A8C99A` — secondary fills, range bars
- `PALE_GREEN` `#E8F0E2` — subtle backgrounds, alt-row stripes
- `BURNT_ORANGE` `#C97B2C` — anomaly/alert markers (sparingly)
- `STEEL_BLUE` `#4A6B8A` — complementary cool for "other" series
- `SLATE` `#6B6F73` — neutral series, grid emphasis
- `INK` `#2A2A2A` — body text
- `PAPER` `#F8F8F5` — warm off-white background (institutional)
- `NEGATIVE` `#A83232` — restrained red for "down" / "miss"

5-series rotation for multi-line charts: `FOREST → STEEL_BLUE →
BURNT_ORANGE → SAGE → DEEP_FOREST`.

**Font note:** Tore wants Google Sans. That family is restricted to
Google properties and not licensed for our redistribution. The closest
visually-identical open substitute is **Inter**
(https://rsms.me/inter/), free for commercial use. Calibri is the
Windows-DOCX fallback when Inter isn't installed. If Tore confirms he
has Google Sans installed locally and only needs his copies to render
with it (not redistribution), we can flip the font name string —
production rendering machines just need the font available at render
time.

### 3. Calls Register — start with a table, prediction-market style
Not "buy futures at $X" (regulatory). Calls structured like
prediction markets — some binary, some developmental with targets.
Track over time in a table.

Implication for schema: `reports.calls_register` (mig 034) is already
shaped this way (target_value, target_date, status open/hit/miss/
partial/withdrawn, confidence). Good — minimal schema work needed.
Add `publication` column so we can track HB calls vs Feedstock Report
calls separately.

### 4. Bespoke chart workflow — undecided, see proposal below
Tore is open. Will scan more institutional reports to nail the
aesthetic before finalizing. Proposed interim workflow below.

### 5. Distribution — email + website download
Initially: email subscribers + a website link to download. Need PDF
output. Subscriber email list + prospect list both required.

### 6. Subscriber + prospect tracking — yes, build schema for sales
Tore has Rich on sales. Schema needs to make Rich's workflow easy.
Proposed `sales.*` schema below.

### Plus — SAF prices via synthetic series
Tore: "FM publishes exactly that kind of price and we can generate a
better version of what they did." We build a `silver.saf_implied_cost`
series from feedstock + processing + credits, parallel to the IFV
callable. Removes the "SAF spot pricing" gap from v1 — we generate
our own.

### Plus — international RD/BD/UCO/tallow prices
Tore: "use placeholders for now and I will work on them." Plumbing
should accept null/placeholder rows gracefully. Section 02 renders
the row with an "—" or "TBD" marker until the data lands.

---

## Architectural updates

### Orchestrator separation
Build `src/reports/feedstock_report/orchestrator.py` as a NEW orchestrator,
not a reuse of `hb_report_orchestrator.py`. Patterns and components
copied (record creation → context gather → writer → builder → delivery),
but the file is its own.

### Schema naming
Per the client-separation rule:

| Old (v1) | New (v2) |
|---|---|
| `reports.issue` | `reports.feedstock_issue` |
| `reports.section_content` | `reports.feedstock_section_content` |
| `reports.news_items` | `reports.feedstock_news_items` |
| `reports.policy_events` | `reports.policy_events` (KEEP shared — events are real-world facts; both reports may reference) |
| `reports.credit_stack_snapshot` | `reports.feedstock_credit_stack_snapshot` |
| `reports.price_dashboard_snapshot` | `reports.feedstock_price_dashboard_snapshot` |
| `reports.calls_register` (existing) | KEEP shared, add `publication` column |
| `reports.release_calendar` (existing) | KEEP shared, neutral |

The `policy_events` and `release_calendar` and `calls_register` tables
stay shared because their content is real-world (a Farm Bill amendment
exists whether we cover it or not). Each report selects which rows it
includes via its own `section_content` row.

---

## Bespoke chart workflow (proposed)

Three options ranked by simplicity:

### Option A — File-drop folder (recommended first cut)
```
output/reports/feedstock_report/issues/2026-W22/bespoke_charts/
  01_palm_arbitrage.png
  02_ev_displacement_curve.png
```
- Tore (or analyst) drops PNGs in `bespoke_charts/` for the current
  issue's folder.
- Numeric prefix controls insertion order.
- Filename (after the number) becomes the figure caption.
- Generator scans the folder at build time, inserts each PNG into
  Section 06 (Supply-Demand Watch) in numeric order.
- Caption from filename; optional `.txt` sidecar for longer caption.

**Pros:** zero infra, works with Excel exports, works with Powerpoint
exports, works with anything that produces PNG/JPG.
**Cons:** charts aren't reproducible from data (just static images).

### Option B — Python script in `bespoke/<issue>/`
- Tore writes a small Python script per issue (or copies a template).
- Generator runs the script, captures its output PNG.
- Pros: charts ARE reproducible.
- Cons: friction for Tore — needs to write Python each week.

### Option C — Excel template with named ranges
- Tore maintains an `.xlsx` template with charts that pull from gold
  views. Each issue, refresh and export charts as PNG.
- Pros: data-driven, Excel-comfortable workflow.
- Cons: requires Excel automation (com.client / xlwings).

**Recommendation:** start with **Option A**, layer in Option C for
recurring chart types as they stabilize, reserve Option B for one-offs.

---

## New schemas (Migration 118)

```sql
-- One row per issue of The Feedstock Report
CREATE TABLE reports.feedstock_issue (
    id              SERIAL PRIMARY KEY,
    issue_number    INTEGER UNIQUE NOT NULL,
    issue_date      DATE NOT NULL,            -- Sunday of publication
    week_ending     DATE NOT NULL,            -- Friday of the week covered
    status          VARCHAR(20) NOT NULL DEFAULT 'draft',
                    -- draft / in_review / published / archived
    title           TEXT,
    cover_lead      TEXT,                     -- the "INSIDE THIS ISSUE" cover blurb
    published_at    TIMESTAMPTZ,
    docx_path       TEXT,                     -- local path of generated DOCX
    pdf_path        TEXT,                     -- local path of generated PDF
    dropbox_path    TEXT,                     -- Dropbox path of final PDF
    website_url     TEXT,                     -- URL when posted to site
    notion_page_id  VARCHAR(64),
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    updated_at      TIMESTAMPTZ DEFAULT NOW()
);

-- One row per section per issue
CREATE TABLE reports.feedstock_section_content (
    id              SERIAL PRIMARY KEY,
    issue_id        INTEGER NOT NULL REFERENCES reports.feedstock_issue(id) ON DELETE CASCADE,
    section_code    VARCHAR(40) NOT NULL,     -- 'executive_read', 'price_dashboard', etc.
    section_number  INTEGER NOT NULL,         -- 1..12
    title           TEXT,
    prose           TEXT,
    bullets         JSONB,                    -- 3-things-to-know etc.
    data_snapshot   JSONB,                    -- frozen data values used in section
    chart_paths     TEXT[],                   -- bespoke chart files included
    word_count      INTEGER,
    author          VARCHAR(40),              -- 'agent', 'analyst', or both
    last_edited_at  TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (issue_id, section_code)
);

-- Curated news items per issue (Section 10)
CREATE TABLE reports.feedstock_news_items (
    id              SERIAL PRIMARY KEY,
    issue_id        INTEGER REFERENCES reports.feedstock_issue(id) ON DELETE CASCADE,
    headline        TEXT NOT NULL,
    url             TEXT,
    source          VARCHAR(80),
    short_take      TEXT,
    importance      VARCHAR(10),              -- high/med/low
    published_at    DATE,
    added_at        TIMESTAMPTZ DEFAULT NOW()
);

-- Policy events — SHARED across all RLC reports
CREATE TABLE reports.policy_events (
    id              SERIAL PRIMARY KEY,
    event_date      DATE NOT NULL,
    jurisdiction    VARCHAR(40),
    category        VARCHAR(40),
    headline        TEXT NOT NULL,
    what            TEXT,
    so_what         TEXT,
    source_url      TEXT,
    impact_horizon  VARCHAR(20),
    created_at      TIMESTAMPTZ DEFAULT NOW()
);

-- Frozen credit stack snapshot
CREATE TABLE reports.feedstock_credit_stack_snapshot (
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

-- Frozen price dashboard snapshot (the "price page" for Section 02)
CREATE TABLE reports.feedstock_price_dashboard_snapshot (
    id              SERIAL PRIMARY KEY,
    issue_id        INTEGER REFERENCES reports.feedstock_issue(id) ON DELETE CASCADE,
    product         VARCHAR(40) NOT NULL,
    location        VARCHAR(60),
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

-- ALTER existing reports.calls_register to track which publication
ALTER TABLE reports.calls_register
    ADD COLUMN IF NOT EXISTS publication VARCHAR(40) DEFAULT 'feedstock_report';

-- Index for filtering by publication
CREATE INDEX IF NOT EXISTS idx_calls_register_publication
    ON reports.calls_register (publication);
```

### Sales CRM schema (Migration 119 — for Rich)

```sql
CREATE SCHEMA IF NOT EXISTS sales;

-- People and companies we sell to
CREATE TABLE sales.contact (
    id              SERIAL PRIMARY KEY,
    first_name      VARCHAR(60),
    last_name       VARCHAR(60),
    email           VARCHAR(120) UNIQUE,
    company         VARCHAR(120),
    title           VARCHAR(120),
    linkedin_url    TEXT,
    phone           VARCHAR(40),
    notes           TEXT,
    source          VARCHAR(60),              -- how we found them
    introduced_by   VARCHAR(120),             -- referral chain
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    updated_at      TIMESTAMPTZ DEFAULT NOW()
);

-- Sales pipeline state per contact per publication
CREATE TABLE sales.prospect (
    id              SERIAL PRIMARY KEY,
    contact_id      INTEGER NOT NULL REFERENCES sales.contact(id),
    publication     VARCHAR(40) NOT NULL,     -- 'feedstock_report', 'hb_report', etc.
    stage           VARCHAR(30) NOT NULL,
                    -- prospect / contacted / engaged / trial / negotiating /
                    -- won / lost / paused / unsubscribed
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

-- Subscribers (paid + comp)
CREATE TABLE sales.subscriber (
    id              SERIAL PRIMARY KEY,
    contact_id      INTEGER NOT NULL REFERENCES sales.contact(id),
    publication     VARCHAR(40) NOT NULL,
    plan            VARCHAR(30),              -- 'paid', 'comp', 'trial'
    started_at      DATE NOT NULL,
    ended_at        DATE,
    renews_at       DATE,
    annual_price    NUMERIC,
    payment_status  VARCHAR(20) DEFAULT 'active',
                    -- active / past_due / cancelled
    delivery_email  VARCHAR(120),             -- if different from contact.email
    notes           TEXT,
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    updated_at      TIMESTAMPTZ DEFAULT NOW()
);

-- Per-issue delivery log
CREATE TABLE sales.issue_delivery (
    id              SERIAL PRIMARY KEY,
    publication     VARCHAR(40) NOT NULL,
    issue_id        INTEGER,                  -- FK to publication's issue table
    subscriber_id   INTEGER REFERENCES sales.subscriber(id),
    sent_at         TIMESTAMPTZ,
    delivery_method VARCHAR(20),              -- 'email', 'website_download', 'manual'
    opened_at       TIMESTAMPTZ,              -- if we track opens
    clicked_at      TIMESTAMPTZ,
    notes           TEXT
);

-- Sales activity log (Rich's daily log)
CREATE TABLE sales.activity (
    id              SERIAL PRIMARY KEY,
    contact_id      INTEGER REFERENCES sales.contact(id),
    activity_type   VARCHAR(30),
                    -- call / email / meeting / demo / sample_sent / quote_sent / other
    occurred_at     TIMESTAMPTZ NOT NULL,
    owner           VARCHAR(60),
    summary         TEXT,
    outcome         VARCHAR(30),              -- 'positive', 'neutral', 'negative'
    follow_up       TEXT,
    follow_up_due   DATE,
    created_at      TIMESTAMPTZ DEFAULT NOW()
);
```

Bare minimum to start: `contact` + `prospect` + `subscriber` + `activity`.
The `issue_delivery` table can come later when we wire actual mail
delivery.

For Rich's workflow, the heavy use will be `prospect` (which stage
each contact is in) and `activity` (log a call, log an email).
Suggest a thin Streamlit or Notion-table front-end so he doesn't
touch SQL.

---

## Implementation phases (revised)

### Phase 1 — Brand + Foundation (1-2 days)
- ✅ `src/reports/feedstock_report/brand.py` — palette + fonts + matplotlib style (DONE)
- Mig 118: feedstock_* schemas + shared policy_events + calls_register publication column
- Mig 119: sales schema
- `src/reports/feedstock_report/data_pack.py` — extracts all data-dense
  inputs for one week (prices, credits, S&D, production, etc.)

### Phase 2 — Auto-rendered sections (3-5 days)
- Section 02 Price Dashboard — renderer using brand palette, range-bar
  style adapted from `scripts/presentation/price_dashboard_v3.py` but
  light-mode green-branded
- Section 03 Credit Stack — table renderer
- Section 04 Production Tracker — bar+line chart
- Section 05 IFV — already a callable, wire to template
- Section 06 S&D Watch — base chart + bespoke-chart-folder pickup
- Section 11 Calls Register — table renderer from `reports.calls_register`
  filtered to `publication = 'feedstock_report'`

### Phase 3 — Analyst-input sections (2-3 days)
- Section 01 Executive Read — anomaly-flag feed + 3-things + on-watch
- Section 07 Policy Monitor — `policy_events` table renderer
- Section 09 Market Signals — spread monitor data pack
- Section 10 Important News — `feedstock_news_items` table workflow
- Section 12 Week Ahead — `release_calendar` renderer

### Phase 4 — Document assembly (2-3 days)
- New `src/reports/feedstock_report/document_builder.py`
  (NOT a reuse of HB builder — copy patterns, fork the file)
- python-docx with brand-injected fonts
- PDF export (LibreOffice headless or docx2pdf)
- Bespoke chart folder pickup at Section 06

### Phase 5 — Delivery + sales workflow (2-3 days)
- Email delivery via Gmail API (Tore's existing pattern) to
  `sales.subscriber` list
- Website download link generation
- Subscriber CRUD (could be Streamlit, Notion, or a thin admin page)
- Rich's pipeline view

**Total estimate (revised):** 10-15 days of focused work.

---

## Open questions remaining for Tore

1. **Font** — confirm Inter as the open substitute for Google Sans,
   OR confirm you have Google Sans installed locally and we should
   keep the name "Google Sans" in the brand module (production
   render machines just need the font installed).
2. **Bespoke chart workflow** — Option A (file drop) acceptable as
   first cut, or do you want to discuss B/C alternatives now?
3. **Website hosting** — does RLC have a site ready to host download
   PDFs, or do we need to spin one up?
4. **Email sender** — use existing Gmail API pattern (tore@RLC) or
   a separate distribution email like `weekly@rlcompanies.com`?
5. **Rich's CRM front-end** — Streamlit page, Notion table sync, or
   just direct SQL/spreadsheet for v1?

I'd start Phase 1 (schemas + data pack) now while you mull questions
1-5. Schema decisions can land first; UI/distribution decisions can
shape Phase 5.
