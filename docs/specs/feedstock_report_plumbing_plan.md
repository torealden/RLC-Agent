# The Feedstock Report — Plumbing Plan

**Source of truth:** `output/reports/misc_tore/templates/The_Feedstock_Report_Template - Content.xlsx`
plus `clients/feedstock_report_taxonomy.md` (527-line section spec).
Current rendered draft: `output/reports/misc_tore/templates/the_feedstock_report_1_1.docx` (Issue 17, "BIOMASS-BASED DIESEL WEEKLY" — Sundays).

**Status:** 2026-05-28 — plan only. No code/schema changes yet. Awaiting Tore's review.

---

## What we have

| Asset | Use |
|---|---|
| `clients/feedstock_report_taxonomy.md` | 12-section spec (purpose, position, inputs, layout, voice, QA) |
| `output/reports/misc_tore/templates/*.docx` | Two brand variants + Tore's edited Issue 17 |
| `docs/checklists/weekly_report.md` | Mon-Thu-Fri production checklist |
| `src/orchestrators/hb_report_orchestrator.py` | Existing weekly-report orchestrator (Higby Barrett) — pattern to reuse |
| `reports.calls_register` (mig 034) | Calls tracking schema — open/hit/miss/withdrawn, target_value, target_date, actual_value, etc. |
| `reports.release_calendar` (mig 034) | Calendar/events feed (Section 12) |
| `src/kg/forecast_book.py` | Forecast tracking infra |
| `src/services/forecast/tracker.py` | Forecast-outcome tracker |
| `output/reports/higby_barrett/HB_Weekly_Report_V2_*.md` | Sample HB outputs — DOCX builder is parallel work |

The HB orchestrator handles: report record creation → KG context gather → writer agent → document builder → Dropbox upload → notifications. Same skeleton works for the Feedstock Report; the section roster is what's different.

---

## Per-section data inventory

Sections 02–06 are data-dense and should auto-populate; 01, 07–12 mix data with analyst prose.

### 01 Executive Read *(analyst)*
**Inputs available now:** anomaly flags from DOC daily ops cycle, prior issue's open calls, current price-move outliers.
**Inputs needed:** none new; analyst writes from the data signals.

### 02 Price Dashboard *(auto)*
**Data needed — fixed list, this is the "price page":**

| Feedstock / Product | Locations | Source |
|---|---|---|
| Soybean Oil | Iowa, Illinois, Indiana, US Gulf, US PNW, Brazil, Argentina | `bronze.feedstock_prices`, NOPA, AMS, OPIS |
| Canola Oil | West Coast, US PNW, US Gulf, Canada | `bronze.feedstock_prices`, AGP |
| DCO | Iowa, Illinois, Indiana, US Gulf, Canada | `bronze.feedstock_prices` |
| Palm Oil | US West Coast, Feedlots, Malaysia, Indonesia, Europe | `bronze.feedstock_prices`, MPOB |
| UCO | Iowa, Gulf domestic, Gulf export, US PNW, China, Europe | `bronze.feedstock_prices`, **GAP — need int'l UCO source (Reuters/Argus?)** |
| Tallow | Iowa, Illinois, Texas, Louisiana, US Gulf, US PNW, Brazil, Australia | `bronze.feedstock_prices`, **GAP — Brazil/Australia tallow** |
| Choice White Grease | Chicago, Missouri River | `bronze.feedstock_prices` |
| Poultry Fat | Southeast, West Coast | `bronze.feedstock_prices` |
| Brown Grease | TBD | **GAP — no source identified** |
| RIN Prices | D4, D6 weekly | `bronze.credit_prices` |
| LCFS Credits | CA, OR, WA | `bronze.credit_prices` |
| Renewable Diesel | Iowa, California, Northeast, Europe, Brazil, Argentina | `bronze.fuel_prices` + **GAP — international RD** |
| Biodiesel | Iowa, US Gulf, Northeast, Europe, Brazil, Argentina | `bronze.fuel_prices` + **GAP — international BD** |
| SAF | California, Northeast, Europe, Brazil | **GAP — SAF spot pricing not yet collected** |

Format: weekly avg, WoW, MoM, YoY, plus a range bar chart per row.
Output cadence: every Sunday, freeze Friday close.

### 03 Credit Stack Snapshot *(auto)*
**Data needed:**
- D4/D6 RIN prices (have)
- LCFS by state (have for CA; OR/WA partial)
- 45Z estimated credit by feedstock CI (need: `silver.ci_value_framework` from KG)
- BTC where still applicable (have)
- Combined stack values per (feedstock × region) — derived

**Gap:** no `silver.credit_stack_snapshot` table yet — needs to be computed per issue and frozen for tracking.

### 04 Production Tracker *(auto)*
**Data needed:**
- EIA Monthly Biofuels production (have: `bronze.eia_monthly_biofuels`)
- EIA Monthly Biofuels capacity (have: `bronze.eia_capacity_monthly` via Form 819)
- Utilization = production / capacity (derive)
- Operator filings (announced restarts, idlings) — **GAP — track manually, store in news table**

### 05 Implied Feedstock Value *(auto)*
**Data needed (all exists):**
- Fuel price (RD Gulf reference) — `bronze.fuel_prices`
- RIN, LCFS, 45Z, BTC — `bronze.credit_prices` + reference tables
- Processing cost — `reference.feedstock_properties`
- lb/gal conversion — `reference.feedstock_properties.lbs_per_gal_hefa`
- Output: implied feedstock $/lb back-solved from fuel + credits − processing
- Already exists as a callable: `src/kg/callables/implied_feedstock_value.py`

### 06 Supply-Demand Watch *(auto + analyst commentary)*
**The "fundamental page" Tore mentioned — US S&D data needed:**

| Series | Source | Status |
|---|---|---|
| BBD balance sheet (BD+RD+SAF+coproc) production / imports / exports / stocks / consumption | `bronze.eia_monthly_biofuels` + Census trade | ✅ |
| Feedstock supply by type (annual + monthly) | `bronze.eia_feedstock_monthly`, `silver.feedstock_supply` | ✅ |
| Feedstock prices | `bronze.feedstock_prices` | ✅ |
| Implied feedstock demand vs supply (gap analysis) | `gold.feedstock_allocation_national` | ✅ |
| Forecast layer (forward S&D projections) | **GAP** — needs ERS Oil Crops Outlook ingestion + scenario layer |
| Crush margins | `gold.crush_margins` (need to verify view exists) | ⚠️ |

**Bespoke chart per issue:** Tore noted he'll add a custom chart each week. Plumbing should support optional chart insertion at this section position.

### 07 Policy Monitor *(analyst, table from events feed)*
**Data needed:**
- Domestic + foreign policy events with date, what, so-what
- **GAP — need `reports.policy_events` table** to log events as they happen; analyst curates the week's relevant ones for the table.

### 08 Trade & Flow Watch *(auto, chart-heavy)*
**Data needed:**
- Census trade by HS code, partner — `bronze.census_trade` (have, well covered)
- EPA EMTS pathway records — **GAP — verify ingestion status**
- Map renderer with arrows — **NEW infrastructure needed** (matplotlib + cartopy, or a static svg lib)

### 09 Market Signals *(auto)*
**Data needed:**
- Futures spreads (e.g., SBO vs Brent, RD vs ULSD basis) — partial; futures data is in `bronze.futures_daily_settlement`
- Cascade map of spread relationships — **NEW: needs visualization layer**
- Cash-basis behavior — `silver.cash_price`

### 10 Important News *(analyst)*
**Data needed:**
- **GAP — need `reports.news_items` table** (headline, url, source, short_take, importance_tag, included_issue)
- Could feed from Notion or a curated weekly list

### 11 Calls Register + Forecast Charts *(auto from existing schema)*
**Data needed (all in `reports.calls_register` already):**
- Open calls with target_date / target_value
- Recently resolved (hit/miss/partial)
- Forecast tracking visualization (line of called vs actual)
- May need to extend `calls_register` with intermediate-actuals tracking (e.g., weekly updates between call and target_date)

### 12 Week Ahead *(analyst, drawn from release_calendar)*
**Data needed:**
- `reports.release_calendar` (have schema; verify it's populated)
- Adds analyst commentary per item

---

## New schemas needed (Migration 118)

```sql
-- One row per issue
CREATE TABLE reports.issue (
    id              SERIAL PRIMARY KEY,
    issue_number    INTEGER UNIQUE NOT NULL,
    publication     VARCHAR(40) NOT NULL,   -- 'the_feedstock_report'
    issue_date      DATE NOT NULL,
    week_ending     DATE NOT NULL,           -- typically Friday of the week covered
    status          VARCHAR(20) NOT NULL DEFAULT 'draft',
                    -- draft / in_review / published / archived
    title           TEXT,
    cover_lead      TEXT,                    -- the "INSIDE THIS ISSUE" cover blurb
    published_at    TIMESTAMPTZ,
    pdf_path        TEXT,                    -- Dropbox path of final PDF
    notion_page_id  VARCHAR(64),
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    updated_at      TIMESTAMPTZ DEFAULT NOW()
);

-- One row per section per issue — prose content + chart references
CREATE TABLE reports.section_content (
    id              SERIAL PRIMARY KEY,
    issue_id        INTEGER NOT NULL REFERENCES reports.issue(id),
    section_code    VARCHAR(40) NOT NULL,    -- 'executive_read', 'price_dashboard', etc.
    section_number  INTEGER NOT NULL,        -- 1..12
    title           TEXT,
    prose           TEXT,                    -- analyst/agent prose, markdown
    bullets         JSONB,                   -- 3-things-to-know etc.
    data_snapshot   JSONB,                   -- frozen data values used in the section
    chart_paths     TEXT[],                  -- file paths to bespoke charts
    word_count      INTEGER,
    author          VARCHAR(40),             -- 'agent', 'analyst', or both
    last_edited_at  TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (issue_id, section_code)
);

-- Curated news items per issue (Section 10)
CREATE TABLE reports.news_items (
    id              SERIAL PRIMARY KEY,
    issue_id        INTEGER REFERENCES reports.issue(id),
    headline        TEXT NOT NULL,
    url             TEXT,
    source          VARCHAR(80),
    short_take      TEXT,                    -- why we included it
    importance      VARCHAR(10),             -- high/med/low
    published_at    DATE,
    added_at        TIMESTAMPTZ DEFAULT NOW()
);

-- Policy events (Section 07)
CREATE TABLE reports.policy_events (
    id              SERIAL PRIMARY KEY,
    event_date      DATE NOT NULL,
    jurisdiction    VARCHAR(40),             -- 'US Federal', 'CA', 'EU', 'Brazil', etc.
    category        VARCHAR(40),             -- 'RFS', '45Z', 'LCFS', 'tariff', etc.
    headline        TEXT NOT NULL,
    what            TEXT,
    so_what         TEXT,
    source_url      TEXT,
    impact_horizon  VARCHAR(20),             -- 'immediate', '0-3mo', '3-12mo', '12+mo'
    is_published    BOOLEAN DEFAULT FALSE,
    issue_id        INTEGER REFERENCES reports.issue(id),
    created_at      TIMESTAMPTZ DEFAULT NOW()
);

-- Frozen credit stack snapshot (Section 03 reproducibility)
CREATE TABLE reports.credit_stack_snapshot (
    id              SERIAL PRIMARY KEY,
    issue_id        INTEGER REFERENCES reports.issue(id),
    feedstock_code  VARCHAR(10) NOT NULL,
    region          VARCHAR(20) NOT NULL,    -- 'CA', 'gulf', 'iowa', etc.
    d4_rin_cents    NUMERIC,
    lcfs_per_gal    NUMERIC,
    forty_five_z    NUMERIC,                 -- estimated $/gal
    btc_per_gal     NUMERIC,
    state_credit    NUMERIC,
    total_stack     NUMERIC,
    notes           TEXT,
    UNIQUE (issue_id, feedstock_code, region)
);

-- Frozen price dashboard snapshot (Section 02 reproducibility + history)
CREATE TABLE reports.price_dashboard_snapshot (
    id              SERIAL PRIMARY KEY,
    issue_id        INTEGER REFERENCES reports.issue(id),
    product         VARCHAR(40) NOT NULL,    -- 'soybean_oil', 'tallow_iowa', etc.
    location        VARCHAR(60),
    week_ending     DATE NOT NULL,
    weekly_avg      NUMERIC,
    wow_change_pct  NUMERIC,
    mom_change_pct  NUMERIC,
    yoy_change_pct  NUMERIC,
    unit            VARCHAR(20),
    source          VARCHAR(40),
    UNIQUE (issue_id, product, location)
);
```

Existing `reports.calls_register` + `reports.release_calendar` keep their current shape; just add a foreign key `issue_id` to `calls_register` so we can mark which issue made the call.

---

## Implementation plan (proposed phases)

### Phase 1 — Foundation (1-2 days)
- Mig 118: schemas above
- Build "price page" extractor — pulls latest weekly closes for the entire fixed list into `reports.price_dashboard_snapshot`
- Build "fundamental page" data pack — assembles BBD S&D + feedstock supply/demand chart inputs

### Phase 2 — Section-by-section generators (3-5 days)
- Section 02 (Price Dashboard) — auto-render full table from snapshot
- Section 03 (Credit Stack) — auto-render
- Section 04 (Production Tracker) — auto-render
- Section 05 (Implied Feedstock Value) — already a callable; just wire to template
- Section 06 (S&D Watch) — auto-render base + slot for bespoke chart
- Section 11 (Calls Register) — auto-render from `reports.calls_register`

### Phase 3 — Analyst-input sections (2-3 days)
- Section 01 Executive Read — anomaly-flag feed for analyst
- Section 07 Policy Monitor — `reports.policy_events` table + entry workflow
- Section 09 Market Signals — spread-monitor data pack
- Section 10 Important News — `reports.news_items` entry workflow
- Section 12 Week Ahead — `reports.release_calendar` + analyst notes

### Phase 4 — Document assembly (2-3 days)
- DOCX builder borrowed from HB orchestrator, retargeted to Feedstock Report template
- Bespoke chart injection slot at Section 06
- PDF export
- Dropbox upload + Notion log

### Phase 5 — Cron + delivery (1 day)
- Sunday morning auto-build of all data-dense sections
- Friday close freeze for price/credit/margin data
- Notification on Notion page when ready for analyst final pass

**Total estimate:** 9-14 days of focused work.

---

## Gaps that need source decisions before Phase 1

These five gaps need Tore's input on data sourcing:

1. **International UCO prices** (China, Europe) — Argus? Reuters? Apparent buyer pricing scrapes?
2. **Brazil + Australia tallow prices**
3. **Brown grease** — is there an actual market price source, or is this a category we'd model from related?
4. **International RD + BD prices** (EU, Brazil, Argentina) — Argus? Platts? Other industry source?
5. **SAF spot pricing** — is there a published price, or do we use producer-reported floors?

Each of these is in your dashboard list but I don't see a collector for them in the codebase.

---

## Open questions for Tore

1. **Publishing cadence** — taxonomy says Sundays, checklist says Friday. Which?
2. **Brand variant** — the templates folder has `.docx` + ` - MSU Green Letters.docx` + the issue 17 doc (BBD WEEKLY title). Which lockup is final?
3. **Calls Register tracking visualization** — do you want a chart showing called value vs actual over time, or just a table?
4. **Bespoke charts at Section 06** — file-drop workflow (you drop a PNG in a folder and the generator picks it up)? Or pull from a Notion attachment?
5. **Distribution mechanism** — PDF to email subscribers? Web? Both?
6. **Subscriber list** — exists yet, or is this self-distribution only at first?

I'd suggest answering questions 1, 4, and 5 before Phase 1 since they shape the schema and orchestrator. The rest can wait until Phase 4-5.
