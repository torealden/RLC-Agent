# RLC-Agent Wiring Plan: From Working Parts to Integrated System

**Created:** 2026-02-14
**Purpose:** Connect all working components into a coordinated, automated system with contextual intelligence

---

## Overview

The RLC-Agent has ~28 working collectors, 100+ spreadsheet models, a medallion-architecture database, report generation, and scheduling definitions -- all functioning independently. This plan wires them together into three layers:

1. **The Dispatcher** -- makes everything run on time, automatically
2. **The Status Layer** -- tells the Desktop LLM what just happened and what's fresh
3. **The Knowledge Graph** -- gives the LLM the analyst's brain, not just the analyst's data

---

## Part 1: The Dispatcher Daemon

### What It Does

A persistent service that reads the existing `ReportScheduler`/`MasterScheduler` schedule definitions and actually executes collectors at the right times. This is the missing "construction crew" for the blueprint that already exists.

### Architecture

```
┌──────────────────────────────────────────────────────┐
│                  DISPATCHER DAEMON                     │
│                                                        │
│  ┌─────────────┐    ┌──────────────┐    ┌──────────┐ │
│  │  APScheduler │───→│ CollectorRunner│───→│ StatusLog│ │
│  │  (cron-like) │    │ (exec + retry)│    │ (DB)     │ │
│  └─────────────┘    └──────────────┘    └──────────┘ │
│         │                    │                  │      │
│         │reads               │uses              │writes│
│         ▼                    ▼                  ▼      │
│  MasterScheduler      COLLECTOR_MAP      collection   │
│  (existing, has       (existing, has     _status      │
│   all timing)          all classes)       table        │
└──────────────────────────────────────────────────────┘
```

### Implementation Steps

#### Step 1.1: Create `collection_status` Table

Add to the database schema. This is the bridge between the dispatcher and the LLM.

```sql
CREATE TABLE core.collection_status (
    id              SERIAL PRIMARY KEY,
    collector_name  TEXT NOT NULL,           -- e.g. 'cftc_cot'
    run_started_at  TIMESTAMPTZ NOT NULL,
    run_finished_at TIMESTAMPTZ,
    status          TEXT NOT NULL DEFAULT 'running',  -- running | success | failed | partial
    rows_collected  INTEGER DEFAULT 0,
    rows_inserted   INTEGER DEFAULT 0,
    error_message   TEXT,
    data_period     TEXT,                    -- e.g. 'week_ending_2026-02-13'
    commodities     TEXT[],                  -- e.g. {'corn','wheat','soybeans'}
    is_new_data     BOOLEAN DEFAULT TRUE,    -- false if re-run with no new records
    triggered_by    TEXT DEFAULT 'scheduler',-- scheduler | manual | backfill
    notes           TEXT
);

CREATE INDEX idx_collection_status_recent
    ON core.collection_status (collector_name, run_started_at DESC);

-- View the LLM will query to understand current state
CREATE VIEW core.latest_collections AS
SELECT DISTINCT ON (collector_name)
    collector_name,
    run_finished_at,
    status,
    rows_collected,
    data_period,
    commodities,
    is_new_data
FROM core.collection_status
ORDER BY collector_name, run_started_at DESC;
```

#### Step 1.2: Create `data_freshness` View

The LLM's primary interface. "What do I know, and how fresh is it?"

```sql
CREATE VIEW core.data_freshness AS
SELECT
    cs.collector_name,
    ds.display_name,
    ds.category,
    cs.run_finished_at AS last_collected,
    cs.status AS last_status,
    cs.rows_collected AS last_row_count,
    cs.data_period,
    cs.is_new_data,
    -- How stale is this data?
    EXTRACT(EPOCH FROM (NOW() - cs.run_finished_at)) / 3600 AS hours_since_collection,
    -- When should it run next? (from schedule metadata)
    ds.expected_frequency,
    ds.expected_release_day,
    ds.expected_release_time_et,
    -- Is it overdue?
    CASE
        WHEN ds.expected_frequency = 'daily'
            AND cs.run_finished_at < CURRENT_DATE THEN TRUE
        WHEN ds.expected_frequency = 'weekly'
            AND cs.run_finished_at < CURRENT_DATE - INTERVAL '8 days' THEN TRUE
        WHEN ds.expected_frequency = 'monthly'
            AND cs.run_finished_at < CURRENT_DATE - INTERVAL '35 days' THEN TRUE
        ELSE FALSE
    END AS is_overdue
FROM core.latest_collections cs
LEFT JOIN core.data_source ds ON ds.short_code = cs.collector_name;
```

#### Step 1.3: Build the Dispatcher (`src/dispatcher/dispatcher.py`)

Wire `APScheduler` (already a dependency) to the existing `MasterScheduler` schedule definitions.

```
src/dispatcher/
├── __init__.py
├── dispatcher.py          # Main daemon
├── collector_runner.py    # Execute + log + retry
└── notifications.py       # Signal LLM / log events
```

**Core logic for `dispatcher.py`:**
- On startup: read all `RELEASE_SCHEDULES` from `MasterScheduler`
- For each enabled collector: register an APScheduler job at the defined time
- Each job calls `collector_runner.run()` which:
  1. Writes a `collection_status` row (status='running')
  2. Instantiates the collector from `COLLECTOR_MAP`
  3. Calls `collector.collect()`
  4. Saves results to database
  5. Updates `collection_status` row (status='success'/'failed', row counts, etc.)
  6. Writes a notification event (see Part 2)

**Retry logic:** Already defined in `MasterScheduler` (`max_retries`, `retry_delay_minutes` per collector). Use those values.

**Holiday/seasonal awareness:** Already encoded in `MasterScheduler` (e.g., crop progress is April-November only, collectors shift days on holidays). The dispatcher reads and respects these.

#### Step 1.4: Wire Into Existing `src/main.py`

Fill in the TODO stubs in `RLCAgent`:

```python
# Instead of placeholders, these methods now:
def start_scheduler(self):
    self.dispatcher = Dispatcher()
    self.dispatcher.start()        # APScheduler background thread

def run_daily_collection(self):
    return self.dispatcher.run_todays_collectors()

def collect_data(self, source=None):
    return self.dispatcher.run_collector(source)
```

#### Step 1.5: Systemd / Windows Service Wrapper

The dispatcher needs to run persistently. Create:
- `deployment/rlc-dispatcher.service` (Linux systemd)
- `deployment/rlc-dispatcher-win.py` (Windows service via `pywin32` or Task Scheduler wrapper)

Both just call `Dispatcher().start()` and keep the process alive.

---

## Part 2: The Status & Notification Layer

### What It Does

Gives the Desktop LLM awareness of what happened. When new CFTC data arrives, the LLM should know without being asked.

### Architecture

```
Dispatcher fires collector
        │
        ▼
collection_status row written
        │
        ▼
Notification event created ──→ event_log table
        │
        ├──→ Desktop LLM reads on startup / periodically
        ├──→ Optional: webhook/file signal for immediate awareness
        └──→ Optional: email alert for critical failures
```

#### Step 2.1: Create `event_log` Table

```sql
CREATE TABLE core.event_log (
    id              SERIAL PRIMARY KEY,
    event_type      TEXT NOT NULL,    -- 'collection_complete', 'collection_failed',
                                     -- 'report_generated', 'data_anomaly',
                                     -- 'schedule_overdue'
    event_time      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    source          TEXT,             -- collector name or system component
    summary         TEXT NOT NULL,    -- Human-readable: "CFTC COT collected: 847 rows,
                                     --   corn net long +15,234 contracts"
    details         JSONB,            -- Structured data for LLM consumption
    acknowledged    BOOLEAN DEFAULT FALSE,  -- LLM marks TRUE after reading
    priority        INTEGER DEFAULT 3       -- 1=critical, 2=important, 3=info
);

CREATE INDEX idx_event_log_unread
    ON core.event_log (acknowledged, event_time DESC)
    WHERE acknowledged = FALSE;
```

#### Step 2.2: Create the LLM Briefing View

What the Desktop LLM reads when it "wakes up" or when a user starts a session:

```sql
CREATE VIEW core.llm_briefing AS
SELECT
    event_type,
    event_time,
    source,
    summary,
    details,
    priority
FROM core.event_log
WHERE acknowledged = FALSE
ORDER BY priority ASC, event_time DESC;
```

The LLM's first action in any session should be:
```sql
SELECT * FROM core.llm_briefing;
```

This gives it: "Since you last checked, here's what happened: CFTC data arrived Friday, EIA ethanol came in Wednesday, Drought Monitor updated Thursday. USDA FAS export sales failed -- timeout error, needs retry."

#### Step 2.3: Smart Summaries in Event Details

The `collector_runner` doesn't just log "847 rows collected." It computes a **delta summary**:

```json
{
    "collector": "cftc_cot",
    "data_date": "2026-02-10",
    "commodities_updated": ["corn", "wheat", "soybeans", "soy_oil", "soy_meal"],
    "notable_changes": [
        {"commodity": "corn", "metric": "managed_money_net", "value": 152340, "change": 15234, "pct_of_oi": 12.3},
        {"commodity": "soybeans", "metric": "managed_money_net", "value": -45600, "change": -8200, "pct_of_oi": -5.1}
    ],
    "context_flags": ["corn_net_long_above_90th_percentile", "soybeans_flipped_net_short"]
}
```

This is where Part 3 (Knowledge Graph) becomes critical -- those `context_flags` need the analyst's brain to generate.

---

## Part 3: The Knowledge Graph (Contextual Intelligence)

### The Problem

When the system collects a data point -- say, CFTC corn net long at 152,340 contracts -- the LLM sees a number. When **you** see that number, you instantly know:

- "That's in the 92nd percentile of the 5-year range"
- "Funds tend to liquidate pre-planting when positions are this extended"
- "Brazil second-crop corn is being planted right now -- weather risk could support or collapse this"
- "The export pace is behind the USDA projection, which undermines the bull case"
- "Last time managed money was this long in February, March corn dropped 25 cents"

That hidden context -- the web of relationships, seasonal patterns, historical precedents, and causal chains -- is what the knowledge graph encodes.

### What It Is

A structured graph of relationships between concepts in the commodity world. Not a database of facts -- a database of **connections and context**.

```
[CFTC Corn Net Position]
    ├── SEASONAL_PATTERN ──→ [Typically peaks pre-planting (Mar-Apr)]
    ├── RISK_THRESHOLD ──→ [>90th percentile of 5yr range = extended]
    ├── CAUSAL_LINK ──→ [Export pace vs USDA projection supports/undermines]
    ├── CAUSAL_LINK ──→ [Brazil safrinha planting progress affects risk]
    ├── HISTORICAL_ANALOG ──→ [Feb 2023: similar positioning, -$0.25 March]
    ├── CROSS_MARKET ──→ [Soy/corn spread relationship at this ratio]
    └── TRIGGERS ──→ [WASDE revisions, crop condition drops, demand surprises]
```

### Architecture

```
┌─────────────────────────────────────────────────────────┐
│                   KNOWLEDGE GRAPH                        │
│                                                          │
│  ┌────────────┐   ┌──────────────┐   ┌───────────────┐ │
│  │   Nodes    │   │    Edges     │   │   Contexts    │ │
│  │ (entities) │──→│(relationships)│──→│  (enrichment) │ │
│  └────────────┘   └──────────────┘   └───────────────┘ │
│                                                          │
│  Node types:          Edge types:        Context types:  │
│  - DataSeries         - CAUSES           - Seasonal norm │
│  - Commodity          - COMPETES_WITH    - Percentile    │
│  - Region             - SUBSTITUTES      - Historical    │
│  - Report             - LEADS            -   analog      │
│  - MarketParticipant  - SEASONAL_PATTERN - Risk threshold│
│  - PolicyMechanism    - RISK_THRESHOLD   - Expert rule   │
│  - SeasonalEvent      - CROSS_MARKET     - Causal chain  │
│  - PriceLevel         - TRIGGERS                         │
│  - BalanceSheet       - SUPPLIES                         │
│                       - CONSUMES                         │
│                       - PART_OF                          │
└─────────────────────────────────────────────────────────┘
```

### Implementation: PostgreSQL + JSONB (Not a Separate Graph DB)

You don't need Neo4j. Your data is already in Postgres. The knowledge graph lives alongside it.

#### Step 3.1: Knowledge Graph Schema

```sql
-- The nodes: entities in the commodity world
CREATE TABLE core.kg_node (
    id          SERIAL PRIMARY KEY,
    node_type   TEXT NOT NULL,          -- 'data_series', 'commodity', 'region',
                                       -- 'report', 'policy', 'seasonal_event',
                                       -- 'market_participant', 'balance_sheet_line'
    node_key    TEXT NOT NULL UNIQUE,   -- 'cftc.corn.managed_money_net'
    label       TEXT NOT NULL,          -- 'CFTC Corn Managed Money Net Position'
    properties  JSONB DEFAULT '{}'      -- Flexible metadata
);

-- The edges: relationships between entities
CREATE TABLE core.kg_edge (
    id              SERIAL PRIMARY KEY,
    source_node_id  INTEGER REFERENCES core.kg_node(id),
    target_node_id  INTEGER REFERENCES core.kg_node(id),
    edge_type       TEXT NOT NULL,      -- See edge types above
    weight          NUMERIC DEFAULT 1.0,-- Strength of relationship (0-1)
    direction       TEXT DEFAULT 'forward', -- forward, reverse, bidirectional
    properties      JSONB DEFAULT '{}', -- Flexible: lag, conditions, etc.
    created_by      TEXT DEFAULT 'manual', -- manual | derived | learned
    confidence      NUMERIC DEFAULT 1.0   -- 1.0 = certain, <1.0 = inferred
);

CREATE INDEX idx_kg_edge_source ON core.kg_edge(source_node_id);
CREATE INDEX idx_kg_edge_target ON core.kg_edge(target_node_id);
CREATE INDEX idx_kg_edge_type ON core.kg_edge(edge_type);

-- The context enrichments: the analyst's brain
CREATE TABLE core.kg_context (
    id              SERIAL PRIMARY KEY,
    node_id         INTEGER REFERENCES core.kg_node(id),
    context_type    TEXT NOT NULL,       -- 'seasonal_norm', 'percentile_range',
                                        -- 'risk_threshold', 'historical_analog',
                                        -- 'expert_rule', 'causal_chain'
    context_key     TEXT NOT NULL,       -- 'feb_normal_range', '5yr_percentile'
    context_value   JSONB NOT NULL,      -- The actual context data (see examples below)
    applicable_when TEXT,                -- SQL-like condition: 'month BETWEEN 2 AND 4'
    source          TEXT DEFAULT 'analyst', -- analyst | computed | learned
    last_updated    TIMESTAMPTZ DEFAULT NOW()
);
```

#### Step 3.2: Populating the Knowledge Graph -- Examples

**Seasonal Norms:**
```json
-- kg_context for node 'cftc.corn.managed_money_net'
{
    "context_type": "seasonal_norm",
    "context_key": "monthly_ranges_5yr",
    "context_value": {
        "jan": {"p10": -50000, "p25": 10000, "median": 75000, "p75": 140000, "p90": 180000},
        "feb": {"p10": -30000, "p25": 30000, "median": 95000, "p75": 155000, "p90": 195000},
        "mar": {"p10": -20000, "p25": 50000, "median": 120000, "p75": 175000, "p90": 220000}
    },
    "applicable_when": "always",
    "source": "computed"
}
```

**Expert Rules:**
```json
-- kg_context for node 'cftc.corn.managed_money_net'
{
    "context_type": "expert_rule",
    "context_key": "extended_position_risk",
    "context_value": {
        "rule": "When managed money net long exceeds 90th percentile of 5-year seasonal range, risk of liquidation-driven selloff increases significantly",
        "action": "Flag as risk factor in any bullish analysis",
        "historical_hit_rate": 0.72,
        "typical_magnitude": "15-30 cent correction in front-month corn",
        "typical_timeframe": "2-4 weeks"
    },
    "source": "analyst"
}
```

**Causal Chains:**
```json
-- kg_edge: 'brazil.safrinha.planting_progress' --CAUSES--> 'cbot.corn.price'
{
    "edge_type": "CAUSES",
    "properties": {
        "mechanism": "Late safrinha planting reduces yield potential, tightening global corn S&D",
        "lag": "2-4 months (planting to yield impact recognition)",
        "conditions": "Only material when planting is >10% behind 5-year pace",
        "magnitude": "10-20% safrinha yield reduction historically adds $0.30-0.60/bu to CBOT",
        "confidence_notes": "Strong when combined with dry forecasts for Mato Grosso"
    }
}
```

**Cross-Market Relationships:**
```json
-- kg_edge: 'soybean_oil.price' --COMPETES_WITH--> 'palm_oil.price'
{
    "edge_type": "COMPETES_WITH",
    "properties": {
        "mechanism": "Substitutable in biodiesel feedstock; price premium/discount drives switching",
        "normal_spread_range": {"min_cents_lb": 2, "max_cents_lb": 12},
        "switching_threshold": "When SBO premium to palm exceeds 10c/lb, biodiesel economics favor palm",
        "regions_affected": ["US", "EU"],
        "seasonal_factor": "Palm production peaks Oct-Jan, widening availability"
    }
}
```

**Balance Sheet Context:**
```json
-- kg_context for node 'usda.corn.ending_stocks'
{
    "context_type": "risk_threshold",
    "context_key": "stocks_to_use_levels",
    "context_value": {
        "comfortable": {"min_pct": 12, "price_behavior": "Range-bound, carry market"},
        "tight": {"min_pct": 8, "max_pct": 12, "price_behavior": "Volatility increases, weather premium builds"},
        "critical": {"max_pct": 8, "price_behavior": "Rationing mode, prices spike to destroy demand"},
        "current_my": "2025/26",
        "current_value_pct": 10.2,
        "assessment": "tight"
    },
    "source": "analyst"
}
```

#### Step 3.3: How the LLM Uses the Knowledge Graph

When new data arrives (via the dispatcher), the LLM enrichment pipeline runs:

```
New CFTC data arrives
        │
        ▼
Query kg_node for 'cftc.corn.managed_money_net'
        │
        ▼
Pull all kg_context for this node
   ├── seasonal_norm: "Current value 152,340 is in 92nd percentile for February"
   ├── expert_rule: "Above 90th pctl = extended, liquidation risk elevated"
   └── risk_threshold: "Flag when >90th percentile"
        │
        ▼
Pull all kg_edge FROM this node
   ├── CAUSES: link to corn price behavior
   ├── CROSS_MARKET: soy/corn spread relationship
   └── SEASONAL_PATTERN: pre-planting liquidation tendency
        │
        ▼
Build enriched event summary for event_log:
   "CFTC corn net long at 152,340 (92nd percentile for Feb).
    Position is extended -- historical liquidation risk is elevated.
    Brazil safrinha planting is 85% complete (normal pace).
    Export commitments are 12% behind USDA projection pace.
    Comparable positioning in Feb 2023 preceded a $0.25 correction."
```

The LLM doesn't figure this out on its own. The knowledge graph **tells** it what an experienced analyst would think.

#### Step 3.4: Populating the Graph -- Phased Approach

**Phase 1 -- Manual (Analyst-Driven, Weeks 1-4):**
Build a simple CLI or Notion form where you enter relationships:
- "CFTC corn positioning CAUSES corn price movement, with these rules..."
- "Soybean oil COMPETES_WITH palm oil, with these thresholds..."
- Start with the 20-30 most important relationships you carry in your head
- Focus on the commodities you analyze most: corn, soybeans, wheat, biofuels

**Phase 2 -- Computed (Automated, Weeks 4-8):**
Scripts that auto-calculate from historical data:
- Seasonal percentile ranges (5-year rolling) for every data series
- Correlation matrices between series (soy oil vs palm oil price, etc.)
- Historical analogs (find past periods with similar positioning/balance sheet)
- Pace calculations (export commitments vs USDA projection pace)

**Phase 3 -- Learned (LLM-Assisted, Ongoing):**
As the Desktop LLM works with you and writes reports:
- It proposes new relationships it discovers
- You confirm or reject (approval workflow)
- Confidence scores update over time
- The graph grows organically from your analytical work

---

## Part 4: Connecting It All -- The Full Loop

### The Weekly Cycle (Automated)

```
MONDAY
  10:00 AM  Dispatcher runs: Export Inspections collector
   4:00 PM  Dispatcher runs: Crop Progress collector (Apr-Nov)
            → collection_status updated
            → event_log: "Crop progress collected. Corn good/excellent
              at 68%, down 2 pts week-over-week. KG context: below
              5-year average of 72% for this week."

TUESDAY
   6:00 AM  Dispatcher checks: all weekly data collected?
            → Queries core.data_freshness for this week's sources
            → If complete: triggers HBReportOrchestrator
            → If gaps: logs which collectors failed, retries or flags

   9:00 AM  HBReportOrchestrator generates report
            → Queries database for fresh data
            → Queries knowledge graph for context on notable changes
            → Writes report with enriched analysis
            → Uploads to Dropbox, sends email
            → event_log: "HB Weekly Report generated and distributed"

WEDNESDAY
  10:30 AM  Dispatcher runs: EIA Ethanol + Petroleum collectors
            → event_log: enriched with KG context on stock levels

THURSDAY
   8:30 AM  Dispatcher runs: USDA FAS Export Sales
   1:30 PM  Dispatcher runs: Drought Monitor, Canada CGC
            → event_log entries with enriched summaries

FRIDAY
   3:30 PM  Dispatcher runs: CFTC COT, USDA AMS Feedstocks
            → event_log: "CFTC collected. Corn managed money net long
              152,340 (92nd percentile Feb). Extended positioning --
              historical liquidation risk elevated."

MONTHLY (varies)
  ~10th     Dispatcher runs: CONAB, MPOB
  ~12th     Dispatcher runs: WASDE (when collector built)
  ~15th     Dispatcher runs: NOPA Crush, Trade Data Orchestrator

AT ANY TIME -- Desktop LLM Session
  1. LLM reads core.llm_briefing → sees unacknowledged events
  2. LLM reads core.data_freshness → knows what's current vs stale
  3. User asks question → LLM queries data + knowledge graph
  4. LLM produces analysis enriched with analyst-level context
  5. LLM marks events as acknowledged
```

### The Desktop LLM System Prompt (Fragment)

The Desktop LLM should include instructions like:

```
You are an agricultural commodity analyst for Round Lakes Commodities.
When you start a session:
  1. Query core.llm_briefing for unread events since your last session
  2. Query core.data_freshness for any overdue or stale data sources
  3. Summarize what's new and what needs attention

When analyzing any data point:
  1. Query core.kg_context for the relevant node to get seasonal norms,
     risk thresholds, and expert rules
  2. Query core.kg_edge to find related factors (causal links, cross-market
     relationships, competing commodities)
  3. Always frame analysis within the context the knowledge graph provides,
     not just the raw number

When writing reports:
  1. Check core.data_freshness to confirm all inputs are current
  2. Use knowledge graph context to identify the 3-5 most important themes
  3. Flag any data anomalies (values outside historical norms per KG context)
```

---

## Part 5: Implementation Sequence

### Phase 1 -- The Dispatcher (Weeks 1-2)
**Goal: Everything runs on time, automatically.**

| Task | Est. Effort | Builds On |
|------|-------------|-----------|
| Create `collection_status` table | 1 hour | Existing DB schema |
| Create `data_freshness` view | 1 hour | collection_status |
| Build `collector_runner.py` (execute + log + retry) | 4 hours | Existing `scripts/collect.py` logic |
| Build `dispatcher.py` (APScheduler + MasterScheduler) | 4 hours | Existing `master_scheduler.py` definitions |
| Wire dispatcher into `src/main.py` stubs | 2 hours | Existing RLCAgent class |
| Create systemd/Windows service wrapper | 2 hours | New |
| **Test: all 16 mapped collectors run on schedule for 1 week** | 1 week | Everything above |

### Phase 2 -- The Status Layer (Week 2-3)
**Goal: The LLM knows what happened.**

| Task | Est. Effort | Builds On |
|------|-------------|-----------|
| Create `event_log` table | 1 hour | Existing DB |
| Create `llm_briefing` view | 1 hour | event_log |
| Add event emission to `collector_runner.py` | 2 hours | Phase 1 dispatcher |
| Build delta summary logic (compare new vs previous collection) | 4 hours | Existing data in silver layer |
| Add report generation events to HBReportOrchestrator | 2 hours | Existing orchestrator |
| Add failure/overdue alerting | 2 hours | data_freshness + event_log |
| **Test: run session, LLM reads briefing, understands current state** | 2 days | Everything above |

### Phase 3 -- Knowledge Graph Foundation (Weeks 3-6)
**Goal: The LLM has the analyst's context.**

| Task | Est. Effort | Builds On |
|------|-------------|-----------|
| Create `kg_node`, `kg_edge`, `kg_context` tables | 1 hour | Existing DB |
| Build CLI tool for manual relationship entry | 4 hours | New |
| Seed nodes for all existing data series (auto-generate from core.series) | 2 hours | Existing series registry |
| **Manual entry: 20-30 critical relationships from your head** | 8 hours (your time) | CLI tool |
| Build seasonal percentile calculator (auto-compute from history) | 4 hours | Existing silver.observation data |
| Build pace calculator (cumulative vs projection tracking) | 4 hours | Export sales data + USDA projections |
| Build historical analog finder | 6 hours | silver.observation + kg_context |
| Wire KG context into event_log summaries | 4 hours | Phase 2 + KG tables |
| **Test: new data arrives, event log includes analyst-level context** | 3 days | Everything above |

### Phase 4 -- LLM Integration (Weeks 6-8)
**Goal: Desktop LLM is a functioning analyst.**

| Task | Est. Effort | Builds On |
|------|-------------|-----------|
| Write LLM system prompt with DB/KG query instructions | 4 hours | All phases |
| Build KG query tools for LLM (function calling / tool use) | 4 hours | KG tables |
| Create "morning briefing" routine | 2 hours | llm_briefing view |
| Create "analyze this data point" routine with KG enrichment | 4 hours | KG context + edges |
| Wire HBReportOrchestrator to use KG context | 4 hours | Existing report writer |
| **Test: full week cycle, LLM generates context-rich analysis** | 1 week | Everything |

### Phase 5 -- Knowledge Graph Growth (Ongoing)
**Goal: The graph gets smarter over time.**

| Task | Est. Effort | Builds On |
|------|-------------|-----------|
| LLM proposes new relationships from analysis work | Ongoing | Phase 4 |
| Approval workflow for proposed relationships | 4 hours | Existing approval manager |
| Automated correlation discovery across series | 8 hours | silver.observation |
| Confidence scoring and decay (stale relationships lose weight) | 4 hours | kg_edge |
| Notion sync for KG documentation | 4 hours | Existing Notion integration |

---

## Part 6: What This Doesn't Cover (Future Work)

- **WASDE collector** (P0 gap -- needed, but separate from wiring)
- **NOPA collector scheduling** (the updater works, just needs dispatcher registration)
- **BioTrack integration** (still building -- will plug into same dispatcher pattern)
- **RLC Orchestrator system** (the `rlc-orchestrator/` directory -- may overlap with this plan; should be reconciled)
- **PowerBI refresh triggers** (could add as post-collection event in dispatcher)
- **Codebase consolidation** (the duplicate directories issue -- orthogonal to this plan)

---

## File Structure (New Components)

```
src/
├── dispatcher/
│   ├── __init__.py
│   ├── dispatcher.py              # APScheduler daemon
│   ├── collector_runner.py        # Execute, log, retry
│   └── notifications.py           # Event emission
├── knowledge_graph/
│   ├── __init__.py
│   ├── kg_manager.py              # CRUD for nodes, edges, contexts
│   ├── kg_enricher.py             # Enrich data points with context
│   ├── seasonal_calculator.py     # Auto-compute seasonal norms
│   ├── pace_calculator.py         # Track cumulative vs projection
│   ├── analog_finder.py           # Historical pattern matching
│   └── kg_cli.py                  # Manual entry tool
database/
├── schemas/
│   ├── 011_collection_status.sql  # Dispatcher status tracking
│   ├── 012_event_log.sql          # LLM notification layer
│   └── 013_knowledge_graph.sql    # KG tables
```

---

## Success Criteria

When this is done, you should be able to:

1. **Walk away for a week** and come back to a database full of fresh data, collected on schedule
2. **Open a Desktop LLM session** and immediately see: "Here's what happened since Monday: 6 collections succeeded, 1 failed (FAS timeout, retried and succeeded), CFTC corn is extended at 92nd percentile"
3. **Ask "What should I be paying attention to in corn?"** and get: "Managed money is extended pre-planting, export pace is lagging, and Brazil safrinha is on track -- the bull case depends on a weather event or demand surprise"
4. **Generate a weekly report** that reads like you wrote it, because the LLM has your analytical framework encoded in the knowledge graph
