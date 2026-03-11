# Autonomous Pipeline Architecture Guide for Round Lakes Companies

**From Data Collection to Walk-Away Report Generation**

*Version 1.1 — March 2026*
*Incorporates concepts from the Sovereign Intelligence Architecture (V2)*

---

## Table of Contents

1. [Executive Summary](#1-executive-summary)
2. [What Exists Today](#2-what-exists-today)
3. [Architecture Overview](#3-architecture-overview)
4. [Control Plane](#4-control-plane)
5. [Sensitivity Classification System](#5-sensitivity-classification-system)
6. [Model Router](#6-model-router)
7. [Prompt Registry](#7-prompt-registry)
8. [Post-Collection Hooks](#8-post-collection-hooks)
9. [Analysis Templates](#9-analysis-templates)
10. [Report Generation Pipeline](#10-report-generation-pipeline)
11. [Delivery System](#11-delivery-system)
12. [WASDE Day Walkthrough](#12-wasde-day-walkthrough)
13. [Phased Build Plan](#13-phased-build-plan)
14. [Future Considerations](#14-future-considerations)

---

## 1. Executive Summary

### What We're Building

This document describes the architecture for an **autonomous report generation pipeline** — a system that collects commodity market data, analyzes it through structured templates and LLM-driven narrative generation, and delivers finished reports without human intervention.

The design goal is the **"ranching test"**: the system runs unattended while the analyst is in the field. Data arrives on schedule, analysis fires automatically, reports assemble themselves, and delivery happens without anyone touching a keyboard.

### How This Connects to the LLM Architecture Guide

The companion document (*LLM Architecture Guide for RLC*) establishes a three-tier privacy model for LLM usage:

| Tier | Description | Applies Here |
|------|-------------|--------------|
| **Tier 1 — Local** | Ollama models (7B/32B/70B) for sensitive data | Internal position data, proprietary analysis |
| **Tier 2 — Cloud API** | Claude API (sonnet/opus) for general analysis | Public market data narrative generation |
| **Tier 3 — Managed** | ChatGPT/web UIs for ad-hoc queries | Not used in autonomous pipeline |

This pipeline architecture **implements** that privacy model through a concrete Control Plane, Sensitivity Classification, and Model Router — the five upgrades recommended in the architecture review:

1. **Control Plane** — Governance layer wrapping every LLM call with logging and audit
2. **Sensitivity Classification** — Data tagged Level 0-4 with routing rules
3. **Model Router** — Automatic model selection based on task + sensitivity + complexity
4. **LLM Decision Logging** — Every call logged to `core.llm_call_log` for cost/quality tracking
5. **Build-Time vs Run-Time Separation** — Prompts versioned in registry, not hardcoded

---

## 2. What Exists Today

The autonomous pipeline doesn't start from scratch. Substantial infrastructure is already in production.

### 2.1 Dispatcher (APScheduler + 22 Collectors)

The dispatcher is an APScheduler-based daemon that fires data collectors on schedule.

| Component | File | Purpose |
|-----------|------|---------|
| Dispatcher daemon | `src/dispatcher/dispatcher.py` | BackgroundScheduler with Eastern Time, job coalescing, 1hr misfire grace |
| Collector runner | `src/dispatcher/collector_runner.py` | Executes collector + logs to collection_status + event_log |
| Collector registry | `src/dispatcher/collector_registry.py` | 22 collectors mapped via lazy imports |
| Schedule config | `src/schedulers/master_scheduler.py` | 29 schedule entries (RELEASE_SCHEDULES dict) |
| CLI | `src/dispatcher/cli.py` | `python -m src.dispatcher {start\|run\|today\|status\|list\|schedule}` |

**Key collectors by frequency:**

- **Weekly**: CFTC COT (Fri 15:30), Crop Progress (Mon 16:00), EIA Petroleum (Wed 10:30), EIA Ethanol (Wed 10:30), Drought Monitor, Canada CGC, AMS Feedstocks, AMS DDGS
- **Monthly**: WASDE (12th at 12:00), MPOB, Census Trade, EPA RFS
- **Quarterly**: NASS Stocks, StatsCanada Stocks
- **On-Demand**: ERS Feed Grains, ERS Oil Crops, ERS Wheat
- **Daily**: CME Settlements, FAS Export Sales

### 2.2 CNS Layer (Central Nervous System)

Four database tables + views that track everything happening in the system.

| Table / View | Purpose |
|--------------|---------|
| `core.collection_status` | Row per collector run: status, rows collected/inserted, timing |
| `core.event_log` | Structured events with JSONB details, priority levels, acknowledgment |
| `core.data_freshness` (view) | JOINs collection_status with data_source on `collector_key` — shows staleness, overdue |
| `core.llm_briefing` (view) | Unacknowledged events formatted for LLM consumption |

**Helper functions:**
- `core.log_event(event_type, source, summary, details, priority)` — canonical event insertion
- `core.acknowledge_events(event_ids)` — mark events as read
- `core.register_kg_source(source_name, source_type, ...)` — register KG data sources

### 2.3 Knowledge Graph

PostgreSQL-backed knowledge graph storing analyst expertise as structured data.

| Table | Count | Purpose |
|-------|-------|---------|
| `core.kg_node` | ~122 nodes | Commodities, data series, reports, policies |
| `core.kg_edge` | ~58 edges | Causal links, substitution, competition relationships |
| `core.kg_context` | ~38 contexts | Analyst knowledge: seasonal norms, percentiles, pace tracking |
| `core.kg_source` | ~47 sources | Provenance tracking for all KG data |
| `core.kg_provenance` | — | Links nodes/edges back to source documents |
| `core.kg_processing_batch` | 5 batches | Extraction batch tracking |

**KG Manager** (`src/knowledge_graph/kg_manager.py`):
- `search_nodes(node_type, label_pattern, key_pattern, limit)` — find nodes
- `get_node(node_key)` — single node lookup
- `get_node_context(node_key)` — all contexts for a node
- `get_node_edges(node_key, edge_type, direction)` — relationship traversal
- `get_enriched_context(node_key)` — node + all contexts + all edges combined
- `get_stats()` — KG health metrics
- `upsert_context(node_key, context_type, ...)` — write/update context
- `bulk_upsert_contexts(contexts, source)` — batch context writes

### 2.4 Delta Summarizer

Post-collection enrichment that computes week-over-week and month-over-month changes.

| Collector | What It Computes |
|-----------|-----------------|
| `cftc_cot` | Net managed money positions, 1w/4w changes, percentile rankings, extreme flags |
| `usda_nass_crop_progress` | Good/Excellent %, 1w change, YoY comparison, deterioration/improvement flags |
| `eia_ethanol` | Production kbd, stocks kb, 4-week MA, large-change flags |
| `nass_processing` | Monthly crush values, MoM change %, large-change flags |

File: `src/dispatcher/delta_summarizer.py`
Entry point: `compute_delta(collector_name, conn) -> Optional[Dict]`

### 2.5 MCP Server (16 Tools)

Claude Desktop integration providing natural-language database access.

**Database Tools (9):** query_database, list_tables, describe_table, get_balance_sheet, get_production_ranking, get_stocks_to_use, get_commodity_summary, analyze_supply_demand, get_brazil_production

**CNS Tools (4):** get_data_freshness, get_briefing, acknowledge_events (only WRITE tool), get_collection_history

**KG Tools (3):** search_knowledge_graph, get_kg_context, get_kg_relationships

File: `src/mcp/commodities_db_server.py`

### 2.6 Existing Report Writer

A template-based report generation system for the HB Weekly Report.

| Component | File | Purpose |
|-----------|------|---------|
| ReportWriterAgent | `src/agents/reporting/report_writer_agent.py` | Orchestrates report generation with LLM |
| DocumentBuilder | `src/services/document/document_builder.py` | Generates Word documents from ReportContent |
| PlaceholderDocumentBuilder | `src/services/document/document_builder.py` | HTML fallback when python-docx unavailable |

**ReportWriterAgent methods:** `generate_report()`, `_generate_executive_summary()`, `_generate_commodity_section()`, `_generate_macro_section()`, `_generate_weather_section()`, `_generate_synthesis()`

**DocumentBuilder methods:** `build_document()`, `_add_title()`, `_add_section()`, `_add_price_table()`, `_add_spread_table()`, `_add_key_triggers()`

---

## 3. Architecture Overview

### 3.1 End-to-End Pipeline

```
  COLLECTION          ENRICHMENT           ANALYSIS            GENERATION          DELIVERY
  ──────────          ──────────           ────────            ──────────          ────────

  ┌──────────┐    ┌───────────────┐    ┌──────────────┐    ┌──────────────┐    ┌──────────┐
  │Dispatcher │    │Delta Summary  │    │Analysis      │    │LLM Narrative │    │Email     │
  │  22       │───>│KG Enrichment  │───>│Templates     │───>│Chart Engine  │───>│File Save │
  │collectors │    │Seasonal Norms │    │(per report)  │    │PDF Assembly  │───>│SMS/Phone │
  └──────────┘    │Pace Tracking  │    └──────────────┘    └──────────────┘    └──────────┘
       │          └───────────────┘           │                    │
       │                │                     │                    │
       v                v                     v                    v
  ┌──────────────────────────────────────────────────────────────────────────┐
  │                         CONTROL PLANE                                    │
  │  Classifier │ Policy Engine │ Model Router │ Tool Gateway │ Auditor     │
  │  core.collection_status │ core.event_log │ core.llm_call_log (chained) │
  │  Tier S (sovereign) │ Tier H (hybrid) │ Tier C (cloud)               │
  └──────────────────────────────────────────────────────────────────────────┘
       │                                                          │
       v                                                          v
  ┌──────────┐                                            ┌──────────────┐
  │PostgreSQL│                                            │Knowledge     │
  │bronze/   │                                            │Graph         │
  │silver/   │                                            │(nodes/edges/ │
  │gold      │                                            │contexts)     │
  └──────────┘                                            └──────────────┘
```

### 3.2 Three-Tier Execution Model (from Sovereign Intelligence Architecture)

The companion *Sovereign Intelligence Architecture* document defines three execution tiers. This pipeline maps onto them:

| Tier | Name | Egress | What Runs Here |
|------|------|--------|---------------|
| **S** | Sovereign Local | None — no internet | Report narrative generation with proprietary KG contexts, HB Weekly drafts, any Level 2+ data processing |
| **H** | Hybrid Local | Constrained — government APIs only | Dispatcher collectors (USDA, EIA, CFTC API calls), delta summarizer, KG enrichment |
| **C** | Cloud Extended | External APIs permitted | Claude API calls for Level 0-1 data analysis (WASDE, CFTC, EIA narratives) |

**Key insight:** The dispatcher runs at Tier H (it needs internet to fetch from government APIs). Report generation with proprietary analysis should be Tier S when using internal KG contexts. The Model Router enforces this: sensitivity 0-1 data can route to Tier C (Claude API), while sensitivity 2+ stays at Tier S (Ollama).

### 3.3 Control Plane Concept

Every component in the pipeline communicates through the Control Plane — a governance layer with six functions (aligned with the Sovereign Intelligence Architecture):

1. **Classifier** — Assigns sensitivity level (0-4) and task type at request time
2. **Policy Engine** — Converts (sensitivity, task, environment) into permissions: allowed tier, required redaction, approval gates
3. **Model Router** — Sends inference calls to local or cloud endpoints behind a single interface
4. **Tool Gateway** — MCP tool access enforcement with scopes (existing: `execute_query()` safety filter)
5. **Auditor / Call Logger** — Tamper-evident logs for every routed step (hash chaining)
6. **Failover Controller** — Detects model unavailability, shifts tiers with mandatory redaction

The existing CNS layer (`core.event_log`, `core.collection_status`) becomes the **foundation** of the Control Plane. We extend it with `core.llm_call_log` and the routing/classification services.

**Practical scope for Phase 1:** We implement the Model Router, Call Logger (with hash chaining), and Sensitivity Config. The Policy Engine starts as a config-driven lookup table. The Failover Controller is built into the Model Router's fallback logic. Full policy engine and approval gates are Phase 4 hardening.

### 3.4 Data Flow Principles

1. **Data moves through tiers**: bronze (raw) → silver (cleaned) → gold (analytics-ready)
2. **LLM calls are always mediated**: never direct — always through the Model Router
3. **Every decision is logged**: collection events, LLM calls, report generation, delivery
4. **Enrichment is best-effort**: failures in delta/KG/seasonal don't block the pipeline
5. **Reports are reproducible**: same data + same prompts + same model = same output
6. **Sensitivity determines routing**: Level 0-1 may use cloud; Level 2+ stays local; Level 4 never touches an LLM
7. **Audit integrity is protected**: hash-chained log records prevent silent tampering

---

## 4. Control Plane

### 4.1 LLM Call Log Table

**Migration 026: `core.llm_call_log`**

```sql
CREATE TABLE core.llm_call_log (
    id              SERIAL PRIMARY KEY,
    call_id         UUID DEFAULT gen_random_uuid(),
    called_at       TIMESTAMPTZ DEFAULT NOW(),

    -- What called it
    task_type       VARCHAR(50) NOT NULL,       -- 'narrative', 'analysis', 'chart_config', 'summary'
    pipeline_run_id UUID,                       -- Groups calls within one report generation
    collector_name  VARCHAR(100),               -- If triggered by a collector

    -- Model selection
    model_id        VARCHAR(100) NOT NULL,      -- 'ollama/llama3.1:70b', 'claude-sonnet-4-20250514'
    model_provider  VARCHAR(20) NOT NULL,       -- 'ollama', 'anthropic'
    sensitivity     SMALLINT NOT NULL DEFAULT 0,-- 0-4

    -- Prompt tracking
    prompt_template VARCHAR(100),               -- 'wasde_analysis_v1', 'cftc_weekly_v1'
    prompt_version  VARCHAR(20),                -- 'v1', 'v2'
    prompt_hash     VARCHAR(64),                -- SHA-256 of rendered prompt
    context_keys    TEXT[],                      -- KG nodes / data tables referenced

    -- Response tracking
    output_hash     VARCHAR(64),                -- SHA-256 of response
    tokens_in       INTEGER,
    tokens_out      INTEGER,
    latency_ms      INTEGER,
    estimated_cost  NUMERIC(10,6),              -- USD

    -- Quality
    status          VARCHAR(20) DEFAULT 'success', -- 'success', 'error', 'timeout', 'fallback'
    error_message   TEXT,
    fallback_model  VARCHAR(100),               -- If primary model failed

    -- Audit integrity (from Sovereign Intelligence Architecture)
    record_hash     VARCHAR(64),                -- SHA-256 of this record's body
    chain_hash      VARCHAR(64),                -- SHA-256(record_hash || prior chain_hash)

    -- Metadata
    details         JSONB DEFAULT '{}'
);

CREATE INDEX idx_llm_call_log_task ON core.llm_call_log(task_type);
CREATE INDEX idx_llm_call_log_model ON core.llm_call_log(model_id);
CREATE INDEX idx_llm_call_log_pipeline ON core.llm_call_log(pipeline_run_id);
CREATE INDEX idx_llm_call_log_called ON core.llm_call_log(called_at);

COMMENT ON TABLE core.llm_call_log IS
    'Control plane audit trail for every LLM invocation in the pipeline';
```

### 4.2 Call Logger Service

**File: `src/services/llm/call_logger.py`**

```
CallLogger
  ├── log_call(task_type, model_id, prompt_template, ...) -> call_id
  ├── log_error(call_id, error_message, fallback_model)
  ├── get_pipeline_summary(pipeline_run_id) -> Dict
  ├── get_cost_report(start_date, end_date) -> Dict
  └── get_model_usage(days=30) -> Dict
```

**How it works:**
- Every LLM call in the pipeline creates a `CallLogger` context
- The logger inserts a row at call start, updates with response metrics on completion
- Pipeline runs share a `pipeline_run_id` (UUID) so all calls for one report are grouped
- Cost estimates use a lookup table of per-token rates by model

### 4.3 Tamper-Evident Hash Chaining

Adopted from the Sovereign Intelligence Architecture to protect audit integrity:

```
record_hash = SHA-256(call_id + model_id + prompt_hash + output_hash + tokens + timestamp)
chain_hash  = SHA-256(record_hash + prior_record's_chain_hash)
```

**Why this matters:** If someone deletes or modifies a log row, the chain breaks — the next record's `chain_hash` won't validate against the altered prior. This provides tamper evidence without requiring external infrastructure.

The Call Logger computes both hashes on every insert. A periodic verification job (daily cron) walks the chain and alerts on breaks.

### 4.4 Sensitivity-Aware Prompt Logging

For Level 0-1 calls (public data), the full rendered prompt and response can be stored in `details` JSONB for debugging and quality review.

For Level 2+ calls (internal/confidential data), the Call Logger stores **metadata only**:
- `prompt_hash` and `output_hash` (for reproducibility verification)
- `tokens_in`, `tokens_out`, `latency_ms` (for cost/performance tracking)
- `context_keys` (which KG nodes were referenced)
- Raw prompt/response text is **never written to the database**

This follows the SIA principle: "Level 3-4 logs store metadata-only plus hashes; never raw prompts/outputs."

### 4.5 Why This Matters

| Use Case | What the Control Plane Enables |
|----------|-------------------------------|
| **Cost tracking** | "WASDE reports cost $0.42 each; CFTC weeklies cost $0.18" |
| **Quality monitoring** | Compare output hashes across runs — detect drift |
| **Audit trail** | Any report section traceable to exact prompt + model + data |
| **Optimization** | Identify which tasks can move to cheaper/faster models |
| **Debugging** | When a report reads wrong, find the exact LLM call that produced it |

---

## 5. Sensitivity Classification System

### 5.1 Level Definitions

| Level | Label | Description | Examples | Routing |
|-------|-------|-------------|----------|---------|
| **0** | Public | Government data, published reports | WASDE, CFTC COT, EIA | Cloud OK (Claude API) |
| **1** | Market | Publicly available but commercially valuable | CME settlements, price spreads | Cloud OK |
| **2** | Internal | Company analysis, proprietary views | RLC position analysis, trade recommendations | Local only (Ollama) |
| **3** | Confidential | Client data, internal positions | Client portfolio data, active trade signals | Local only, encrypted at rest |
| **4** | Credential | API keys, passwords, connection strings | .env contents, database passwords | Never sent to any LLM |

### 5.2 RLC Data Source Mapping

| Data Source | Sensitivity | Rationale |
|-------------|-------------|-----------|
| USDA WASDE | 0 (Public) | Published government report |
| CFTC COT | 0 (Public) | Published government data |
| EIA Petroleum/Ethanol | 0 (Public) | Published government data |
| NASS Crop Progress | 0 (Public) | Published government data |
| CME Settlements | 1 (Market) | Publicly available, commercially standard |
| FAS Export Sales | 0 (Public) | Published government data |
| CONAB Brazil | 0 (Public) | Published government data |
| MPOB Malaysia | 0 (Public) | Published government data |
| KG Contexts (seasonal norms) | 1 (Market) | Computed from public data |
| KG Contexts (proprietary analysis) | 2 (Internal) | RLC analytical IP |
| HB Weekly Report drafts | 2 (Internal) | Proprietary analysis output |
| Client position data | 3 (Confidential) | Client-specific information |

### 5.3 Configuration

**File: `src/config/sensitivity.yaml`**

```yaml
sensitivity_levels:
  0: { label: "Public",       allow_cloud: true,  allow_local: true }
  1: { label: "Market",       allow_cloud: true,  allow_local: true }
  2: { label: "Internal",     allow_cloud: false, allow_local: true }
  3: { label: "Confidential", allow_cloud: false, allow_local: true, encrypt_at_rest: true }
  4: { label: "Credential",   allow_cloud: false, allow_local: false }

data_source_defaults:
  usda_wasde: 0
  cftc_cot: 0
  eia_petroleum: 0
  eia_ethanol: 0
  usda_nass_crop_progress: 0
  cme_settlements: 1
  usda_fas_export_sales: 0
  kg_seasonal_norms: 1
  kg_proprietary: 2
  hb_weekly_draft: 2
  client_positions: 3
```

### 5.4 Routing Rules

```
IF sensitivity >= 2:
    MUST use local model (Ollama) — Tier S
    MUST NOT send data to any cloud API
    Call Logger stores metadata only (no raw prompts)

IF sensitivity >= 3:
    MUST use local model — Tier S
    MUST encrypt prompt/response at rest
    Call Logger stores metadata only

IF sensitivity == 4:
    MUST NOT send to any LLM (local or cloud)
    Log attempt as security violation
```

### 5.5 Pre-Flight Redaction Check

Before any Tier C (cloud) call, the Model Router runs a pre-flight scan on the rendered prompt. This catches cases where Level 2+ content accidentally leaks into a cloud-bound request (e.g., proprietary KG contexts mixed into a WASDE analysis prompt).

**Redaction categories** (from SIA):

| Category | What Gets Caught | Action |
|----------|-----------------|--------|
| **Secrets** | API keys, tokens, connection strings | Block call, log violation |
| **Deal identifiers** | Contract IDs, counterparty names | Replace with placeholders (COUNTERPARTY_A) |
| **Proprietary analysis** | KG contexts tagged sensitivity >= 2 | Strip from prompt, log warning |
| **Numeric masking** | Internal position sizes, P&L figures | Preserve relative changes, mask absolutes |

**Practical scope for Phase 1:** The pre-flight check scans for KG context keys tagged Level 2+ and blocks them from cloud prompts. Full regex-based redaction (secrets, deal IDs) is Phase 4.

---

## 6. Model Router

### 6.1 Overview

**File: `src/services/llm/model_router.py`**

The Model Router selects the optimal model for each LLM call based on three inputs:

1. **Task type** — What kind of work (narrative, analysis, summarization, chart config)
2. **Sensitivity level** — What data is involved (determines cloud vs local)
3. **Complexity** — How hard the task is (determines model size)

### 6.2 Routing Logic

```
ModelRouter
  ├── route(task_type, sensitivity, complexity='medium') -> ModelConfig
  ├── get_available_models() -> List[ModelConfig]
  ├── health_check() -> Dict[str, bool]
  └── get_fallback(model_id) -> Optional[ModelConfig]
```

**Decision matrix:**

| Task Type | Sensitivity 0-1 | Sensitivity 2+ |
|-----------|----------------|----------------|
| **narrative** (report text) | Claude Sonnet | Ollama 70B |
| **analysis** (data interpretation) | Claude Sonnet | Ollama 32B |
| **summary** (brief condensation) | Claude Haiku | Ollama 7B |
| **chart_config** (chart parameters) | Claude Haiku | Ollama 7B |
| **synthesis** (multi-source) | Claude Opus | Ollama 70B |

**Complexity overrides:**

| Complexity | Effect |
|------------|--------|
| `low` | Downgrade one tier (Sonnet → Haiku, 70B → 32B) |
| `medium` | Use default from matrix |
| `high` | Upgrade one tier (Sonnet → Opus, 32B → 70B) |

### 6.3 Supported Models

**Cloud (Anthropic API):**

| Model | ID | Use Case | Cost (approx/1K tokens) |
|-------|----|----------|------------------------|
| Claude Opus | `claude-opus-4-20250514` | Complex synthesis, high-stakes narrative | $15 in / $75 out |
| Claude Sonnet | `claude-sonnet-4-20250514` | Standard narrative generation | $3 in / $15 out |
| Claude Haiku | `claude-haiku-4-5-20251001` | Summarization, config generation | $0.80 in / $4 out |

**Local (Ollama):**

| Model | Size | VRAM | Use Case |
|-------|------|------|----------|
| Llama 3.1 70B | 70B | ~40GB | Full narrative, sensitive analysis |
| Llama 3.1 32B | 32B | ~20GB | Standard analysis, moderate tasks |
| Llama 3.1 8B | 8B | ~5GB | Summaries, config, simple extraction |

### 6.4 Fallback Behavior

```
Primary model unavailable?
  ├── Cloud model timeout → retry once → fallback to next cloud tier
  ├── Cloud model error → fallback to local equivalent
  ├── Local model unavailable → fallback to smaller local model
  └── All models unavailable → log error, skip LLM step, use template fallback
```

Every fallback is logged to `core.llm_call_log` with `status='fallback'` and `fallback_model` populated.

---

## 7. Prompt Registry

### 7.1 Directory Structure

**Base path: `src/prompts/`**

```
src/prompts/
├── __init__.py
├── registry.py                    # PromptRegistry class
├── base_template.py               # BasePromptTemplate with variable injection
│
├── analysis/                      # Data analysis prompts
│   ├── wasde_analysis_v1.py
│   ├── cftc_weekly_v1.py
│   ├── eia_weekly_v1.py
│   ├── export_sales_v1.py
│   └── crop_conditions_v1.py
│
├── narrative/                     # Report narrative prompts
│   ├── executive_summary_v1.py
│   ├── commodity_section_v1.py
│   ├── market_outlook_v1.py
│   └── weather_impact_v1.py
│
├── chart/                         # Chart configuration prompts
│   ├── price_chart_v1.py
│   └── position_chart_v1.py
│
└── synthesis/                     # Multi-source synthesis prompts
    ├── weekly_brief_v1.py
    └── cross_market_v1.py
```

### 7.2 Prompt Template Structure

Each template is a Python file containing:

```python
TEMPLATE_ID = "wasde_analysis_v1"
TEMPLATE_VERSION = "v1"
TASK_TYPE = "analysis"
DEFAULT_MODEL_TIER = "sonnet"

SYSTEM_PROMPT = """You are a senior commodity analyst at Round Lakes Companies..."""

USER_TEMPLATE = """
Analyze the following WASDE report data for {commodity}.

## Current Month Data
{current_data}

## Prior Month Data (for comparison)
{prior_data}

## Knowledge Graph Context
{kg_context}

## Instructions
- Identify the most significant changes month-over-month
- Explain what drove the changes (supply vs demand factors)
- Note any revisions to prior estimates
- Flag anything that breaks from seasonal norms
...
"""

REQUIRED_VARIABLES = ["commodity", "current_data", "prior_data", "kg_context"]
OPTIONAL_VARIABLES = ["seasonal_norms", "historical_range"]
```

### 7.3 How Data Gets Injected

```
Analysis Template (queries DB)
  │
  ├── current_data  ← SQL query against silver/gold tables
  ├── prior_data    ← Same query with date offset
  ├── kg_context    ← KGManager.get_enriched_context(node_key)
  ├── seasonal_norms← KG contexts where context_type='seasonal_norm'
  └── delta_summary ← delta_summarizer.compute_delta() output
  │
  v
PromptRegistry.render(template_id, variables) -> rendered_prompt
  │
  v
ModelRouter.route(task_type, sensitivity) -> model_config
  │
  v
LLM Call (with CallLogger wrapping)
```

### 7.4 Adding New Report Types

To add a new report type:

1. Create a prompt template in the appropriate subdirectory
2. Define an analysis template (Section 9) that queries the right data
3. Register the template in `registry.py`
4. Add a schedule entry in `master_scheduler.py` if it should fire automatically
5. The pipeline handles the rest: routing, logging, assembly, delivery

---

## 8. Post-Collection Hooks

### 8.1 Current Hook Architecture

When a collector finishes, `CollectorRunner.run_collector()` executes an enrichment pipeline within the event logging step (lines 264-335 of `collector_runner.py`):

```
Collector completes
  │
  ├── 1. Insert collection_status row (running → success/failed)
  ├── 2. Log event to core.event_log via core.log_event()
  │
  └── 3. Enrichment Pipeline (all best-effort, wrapped in try-except):
        ├── Delta Summary    (delta_summarizer.compute_delta)
        ├── KG Enrichment    (KGEnricher.enrich_collection_event)
        ├── Seasonal Norms   (SeasonalCalculator — for CFTC, Crop Progress)
        └── Pace Tracking    (PaceCalculator — for NASS Processing, NOPA Crush)
```

### 8.2 Extending Hooks for Report Generation

The autonomous pipeline adds a new hook type: **analysis triggers**. After enrichment completes, the system checks whether this collection event should trigger a report pipeline.

```
Enrichment completes
  │
  └── 4. Analysis Trigger Check (NEW):
        ├── Is this collector associated with a report template?
        ├── Has all required data for that report arrived?
        ├── Is the report schedule window active?
        │
        └── If YES to all: queue report generation pipeline
```

### 8.3 Event-Driven vs Time-Driven Triggers

| Trigger Type | When It Fires | Example |
|-------------|---------------|---------|
| **Event-driven** | Immediately after specific collector completes | WASDE collector → WASDE analysis pipeline |
| **Time-driven** | On a fixed schedule regardless of collection | Tuesday 6AM → HB Weekly Report |
| **Dependency-driven** | After N collectors all complete | Mon CFTC + Crop Progress + EIA all done → Weekly synthesis |

Configuration lives in the report template definition (Section 9), not in the dispatcher. The dispatcher only fires collectors. The analysis trigger system watches events and decides when to fire pipelines.

### 8.4 Existing Enrichment Detail

**Delta Summarizer** (`src/dispatcher/delta_summarizer.py`):
- `compute_delta('cftc_cot', conn)` — Net managed money, 1w/4w changes, percentile rankings, extreme flags
- `compute_delta('usda_nass_crop_progress', conn)` — Good/Excellent %, deterioration/improvement flags
- `compute_delta('eia_ethanol', conn)` — Production kbd, stocks kb, 4-week MA
- `compute_delta('nass_processing', conn)` — Monthly crush MoM change

**KG Enricher** (`src/knowledge_graph/kg_enricher.py`):
- `enrich_collection_event(collector_name, rows_collected, data_period)` — finds relevant KG nodes and adds contextual intelligence to the event

**Seasonal Calculator** (`src/knowledge_graph/seasonal_calculator.py`):
- `compute_cftc_seasonal_norms()` — historical percentile bands for CFTC positions
- `compute_crop_condition_norms()` — historical good/excellent percentage bands

**Pace Calculator** (`src/knowledge_graph/pace_calculator.py`):
- `run_all()` — computes pace vs USDA projection for multiple commodities
- `compute_soy_crush_pace()` — soybean crush pace vs annual targets

---

## 9. Analysis Templates

### 9.1 Template Architecture

Each report type has an **analysis template** — a Python class that:
1. Queries the database for relevant data
2. Computes deltas and comparisons
3. Retrieves KG context for analyst perspective
4. Packages everything into a structured dict for LLM narrative generation

**Base path: `src/analysis/templates/`**

```
src/analysis/templates/
├── __init__.py
├── base_template.py          # BaseAnalysisTemplate abstract class
├── wasde_template.py
├── cftc_weekly_template.py
├── eia_weekly_template.py
├── export_sales_template.py
└── crop_conditions_template.py
```

### 9.2 BaseAnalysisTemplate Interface

```python
class BaseAnalysisTemplate:
    template_id: str
    report_type: str
    required_collectors: List[str]    # Which collectors must have fresh data
    trigger_mode: str                 # 'event', 'time', 'dependency'
    trigger_collector: Optional[str]  # For event-driven: which collector triggers this

    def check_data_ready(self, conn) -> bool
    def gather_data(self, conn) -> Dict
    def compute_analysis(self, data: Dict) -> Dict
    def get_kg_context(self, kg_manager: KGManager) -> Dict
    def build_prompt_context(self) -> Dict  # Returns variables for prompt template
```

### 9.3 Per-Report-Type Templates

#### WASDE Template (`wasde_template.py`)

| Step | What It Does | Data Source |
|------|-------------|-------------|
| **Gather** | Current + prior month balance sheets for corn, soybeans, wheat, soybean oil, soybean meal | `gold.wasde_balance_sheet` |
| **Delta** | MoM changes for production, consumption, ending stocks, S/U ratio | Computed from gathered data |
| **KG Context** | Seasonal norms for S/U ratios, historical production ranges, policy contexts | `kg_manager.get_enriched_context('wasde_corn')`, etc. |
| **Prompt Context** | Structured dict with current, prior, changes, KG context | Passed to `wasde_analysis_v1` prompt |

**Trigger:** Event-driven — fires when `usda_wasde` collector completes successfully.

#### CFTC Weekly Template (`cftc_weekly_template.py`)

| Step | What It Does | Data Source |
|------|-------------|-------------|
| **Gather** | Latest 2 weeks + 4-week-ago positions for 6 commodities | `bronze.cftc_cot` |
| **Delta** | Weekly change, 4-week change, percentile ranking, extreme flags | `delta_summarizer` output |
| **KG Context** | Seasonal position norms, historical extreme thresholds | `kg_manager.get_enriched_context('cftc_*')` |
| **Prompt Context** | Position changes, extremes, KG norms, notable flags | Passed to `cftc_weekly_v1` prompt |

**Trigger:** Event-driven — fires when `cftc_cot` collector completes (Friday ~15:30 ET).

#### EIA Weekly Template (`eia_weekly_template.py`)

| Step | What It Does | Data Source |
|------|-------------|-------------|
| **Gather** | Latest 2 weeks ethanol production, stocks; petroleum data | `silver.ethanol_weekly`, `silver.petroleum_weekly` |
| **Delta** | Production change kbd, stocks change kb, 4-week MA | `delta_summarizer` output |
| **KG Context** | RINs pricing context, mandate levels, ethanol margin benchmarks | KG nodes for RINs, ethanol |
| **Prompt Context** | Production, stocks, changes, RIN context | Passed to `eia_weekly_v1` prompt |

**Trigger:** Event-driven — fires when `eia_ethanol` collector completes (Wednesday ~10:30 ET).

#### Export Sales Template (`export_sales_template.py`)

| Step | What It Does | Data Source |
|------|-------------|-------------|
| **Gather** | Latest weekly export sales by commodity and destination | `silver.fas_export_sales` |
| **Delta** | Week-over-week change, pace vs USDA projection, top buyers | Computed from gathered data |
| **KG Context** | Annual export projections, historical pace benchmarks | KG pace_tracking contexts |
| **Prompt Context** | Sales by commodity, pace %, top destinations, changes | Passed to `export_sales_v1` prompt |

**Trigger:** Event-driven — fires when `usda_fas_export_sales` collector completes.

#### Crop Conditions Template (`crop_conditions_template.py`)

| Step | What It Does | Data Source |
|------|-------------|-------------|
| **Gather** | Latest 2 weeks G/E ratings by state and national | `silver.nass_crop_condition_ge` |
| **Delta** | Weekly change, YoY comparison, rapid deterioration/improvement | `delta_summarizer` output |
| **KG Context** | Historical condition-to-yield relationships, seasonal norms | KG seasonal_norm contexts |
| **Prompt Context** | National + state conditions, changes, flags, KG norms | Passed to `crop_conditions_v1` prompt |

**Trigger:** Event-driven — fires when `usda_nass_crop_progress` collector completes (Monday ~16:00 ET).

---

## 10. Report Generation Pipeline

### 10.1 Pipeline Stages

```
Analysis Template output (structured context dict)
  │
  ├── Stage 1: PROMPT RENDERING
  │     PromptRegistry.render(template_id, context) -> rendered_prompt
  │
  ├── Stage 2: LLM NARRATIVE GENERATION
  │     ModelRouter.route(task_type, sensitivity) -> model
  │     CallLogger wraps the call
  │     LLM generates structured narrative sections
  │
  ├── Stage 3: CHART GENERATION
  │     Chart templates (matplotlib) consume analysis data
  │     One chart per key metric (positions, prices, conditions, etc.)
  │     Charts saved as PNG files
  │
  ├── Stage 4: REPORT ASSEMBLY
  │     DocumentBuilder combines narrative + charts -> Word doc
  │     Table generation (price tables, position tables)
  │     Formatting and styling
  │
  ├── Stage 5: VALIDATION
  │     Sanity checks on output (non-empty sections, reasonable numbers)
  │     Completeness scoring (existing _calculate_completeness method)
  │
  └── Stage 6: OUTPUT
        Save to reports directory
        Log to core.event_log
        Queue for delivery (Section 11)
```

### 10.2 LLM Narrative Generation

The LLM call follows a structured pattern:

1. **System prompt** sets the persona (senior commodity analyst at RLC)
2. **User prompt** provides data + instructions from the rendered template
3. **Response** is structured text with section headers matching the report format
4. **Post-processing** extracts sections, validates content, handles formatting

All calls go through the Model Router and are wrapped by the Call Logger:

```python
# Pseudocode for a narrative generation call
with CallLogger(task_type='narrative', pipeline_run_id=run_id) as logger:
    model = ModelRouter.route('narrative', sensitivity=0, complexity='medium')
    prompt = PromptRegistry.render('wasde_analysis_v1', context)
    response = model.generate(system=SYSTEM_PROMPT, user=prompt)
    logger.log_response(tokens_in, tokens_out, latency_ms)
    return parse_narrative(response)
```

### 10.3 Chart Engine

**Matplotlib-based chart templates per report type:**

| Report | Charts |
|--------|--------|
| WASDE | S&D waterfall, ending stocks trend, S/U ratio comparison |
| CFTC | Net position bars, percentile ranking heatmap, 52-week range |
| EIA | Production + stocks dual-axis, 4-week MA trend |
| Export Sales | Cumulative pace line vs projection, top-destination stacked bars |
| Crop Conditions | G/E% trend line, state-level choropleth, YoY comparison |

Charts are generated from the same analysis data that feeds the LLM — ensuring charts and narrative are always consistent.

### 10.4 Report Assembly

The existing `DocumentBuilder` class (`src/services/document/document_builder.py`) handles Word doc assembly:

1. `build_document(content: ReportContent)` — main entry point
2. Adds title, sections, tables, key triggers, metadata
3. Chart PNGs inserted as inline images
4. Returns `DocumentResult` with path and metadata

### 10.5 Output Validation

Before marking a report as ready for delivery:

- [ ] All required sections present and non-empty
- [ ] Numerical values within reasonable ranges (no $0 corn, no 500% S/U)
- [ ] Charts generated successfully for all required metrics
- [ ] Completeness score above threshold (currently `_calculate_completeness` in ReportWriterAgent)
- [ ] No placeholder text remaining (currently `_identify_placeholders`)
- [ ] File size reasonable (not 0 bytes, not >50MB)

---

## 11. Delivery System

### 11.1 Email Delivery

**SMTP-based email for report distribution:**

| Setting | Value |
|---------|-------|
| Provider | Configurable (Gmail, Outlook, custom SMTP) |
| Auth | App passwords or OAuth |
| Recipients | Per-report distribution lists in config |
| Format | Email body with summary + Word doc attachment |

```
src/services/delivery/
├── __init__.py
├── email_sender.py          # SMTP email with attachments
├── file_manager.py          # Report file storage/archival
└── notification_service.py  # SMS/phone alerts
```

### 11.2 File Storage

Reports are organized by date and type:

```
reports/
├── 2026/
│   ├── 03/
│   │   ├── wasde/
│   │   │   └── WASDE_Analysis_2026-03-12.docx
│   │   ├── cftc/
│   │   │   ├── CFTC_Weekly_2026-03-07.docx
│   │   │   └── CFTC_Weekly_2026-03-14.docx
│   │   └── weekly/
│   │       └── HB_Weekly_2026-03-04.docx
```

### 11.3 Phone/Text Notification

Light-touch alerts that a report is ready:

- SMS via Twilio (or similar) for urgent reports (WASDE day)
- Summary text only — the full report is in email/file storage
- Configurable per-report-type: some reports just save silently

### 11.4 Event Logging

Every delivery action creates an event:

```sql
SELECT core.log_event(
    'report_delivered',              -- event_type
    'wasde_pipeline',                -- source
    'WASDE March analysis delivered to 3 recipients',
    '{"report_type": "wasde", "recipients": 3, "file_path": "...", "email_sent": true}'::jsonb,
    2                                -- priority (important)
);
```

---

## 12. WASDE Day Walkthrough

**Scenario: WASDE report releases on March 12, 2026 at 12:00 PM ET.**

### Timeline

```
11:45 AM ET — SYSTEM READY
├── Dispatcher running (APScheduler daemon, started at system boot)
├── usda_wasde job scheduled: CronTrigger(day=12, hour=12, minute=0)
├── All prerequisites met: PostgreSQL up, Ollama running (if needed)
```

```
12:00 PM ET — COLLECTION FIRES
├── APScheduler triggers _run_scheduled_collector('usda_wasde')
├── CollectorRunner.run_with_retry(max_retries=3, retry_delay_minutes=15)
│
├── _insert_status_running() → core.collection_status row created
│   Status: 'running', triggered_by: 'scheduler'
│
├── USDATFASCollector.collect() executes
│   ├── Fetches WASDE data from USDA API
│   ├── Parses response → standardized records
│   └── Inserts into bronze/silver tables
│
├── _update_status_result() → core.collection_status updated
│   Status: 'success', rows_collected: 450, rows_inserted: 450
```

```
12:01 PM ET — ENRICHMENT PIPELINE
├── _log_event('collection_complete', 'usda_wasde', summary, details)
│
├── Delta Summary:
│   compute_delta('usda_wasde', conn)
│   → MoM changes for production, stocks, S/U ratios
│   → Notable: "Corn ending stocks revised down 50M bu"
│
├── KG Enrichment:
│   KGEnricher.enrich_collection_event('usda_wasde', 450, 'march_2026')
│   → Adds context: "Lowest corn S/U since 2020/21"
│
├── Event logged with enriched details (JSONB)
│
├── [CONTROL PLANE LOGGED]
│   core.event_log row created with full enrichment data
```

```
12:02 PM ET — ANALYSIS TRIGGER FIRES (NEW)
├── Analysis trigger system detects: usda_wasde completed successfully
├── WASDETemplate.check_data_ready() → True
│
├── WASDETemplate.gather_data()
│   ├── Current month balance sheets (corn, soybeans, wheat, SBO, SBM)
│   ├── Prior month for comparison
│   └── 3-year history for trends
│
├── WASDETemplate.compute_analysis()
│   ├── MoM changes calculated
│   ├── Largest movers identified
│   └── Surprise vs market expectations flagged
│
├── WASDETemplate.get_kg_context()
│   ├── Seasonal norms for S/U ratios
│   ├── Historical production ranges
│   └── Policy contexts (RFS mandates, trade agreements)
│
├── build_prompt_context() → structured dict ready for LLM
```

```
12:03 PM ET — LLM NARRATIVE GENERATION
├── pipeline_run_id = UUID generated for this run
│
├── [CONTROL PLANE] ModelRouter.route('analysis', sensitivity=0, complexity='high')
│   → Selected: claude-sonnet-4-20250514 (public data, high complexity)
│
├── PromptRegistry.render('wasde_analysis_v1', context)
│   → Full prompt with current data, prior data, KG context
│
├── [CONTROL PLANE] CallLogger wraps the call:
│   │ call_id: abc-123
│   │ task_type: 'narrative'
│   │ model_id: 'claude-sonnet-4-20250514'
│   │ sensitivity: 0
│   │ prompt_template: 'wasde_analysis_v1'
│   │ prompt_hash: sha256(rendered_prompt)
│   │
│   ├── Claude API call executes (~15-30 seconds)
│   │
│   │ tokens_in: 4,200
│   │ tokens_out: 2,800
│   │ latency_ms: 22,000
│   │ estimated_cost: $0.054
│   │ status: 'success'
│   │
│   └── core.llm_call_log row inserted
│
├── Narrative parsed into sections:
│   ├── Executive summary
│   ├── Corn analysis
│   ├── Soybean complex analysis
│   ├── Wheat analysis
│   └── Market outlook
```

```
12:04 PM ET — CHART GENERATION
├── WASDE chart templates fire:
│   ├── Ending stocks comparison (corn, soybeans, wheat) → PNG
│   ├── S/U ratio trend (3-year) → PNG
│   ├── MoM change waterfall → PNG
│   └── Global production rankings → PNG
│
├── Charts generated from same analysis data as narrative
│   (ensures charts and text are consistent)
```

```
12:04 PM ET — REPORT ASSEMBLY
├── DocumentBuilder.build_document(content)
│   ├── Title page: "WASDE Analysis — March 12, 2026"
│   ├── Executive summary section
│   ├── Commodity sections with inline charts
│   ├── Data tables (balance sheets, S/U ratios)
│   ├── Market outlook section
│   └── Metadata footer (data sources, model used, generation time)
│
├── Output: reports/2026/03/wasde/WASDE_Analysis_2026-03-12.docx
│
├── Validation:
│   ├── All sections present? YES
│   ├── Completeness score: 0.94
│   ├── Charts embedded: 4/4
│   └── File size: 2.1 MB (reasonable)
```

```
12:05 PM ET — DELIVERY
├── Email sent to distribution list:
│   ├── Subject: "WASDE Analysis — March 12, 2026"
│   ├── Body: Executive summary (first 3 paragraphs)
│   ├── Attachment: WASDE_Analysis_2026-03-12.docx
│   └── Recipients: configured WASDE distribution list
│
├── SMS notification: "WASDE analysis ready — check email"
│
├── [CONTROL PLANE] Event logged:
│   core.log_event('report_delivered', 'wasde_pipeline',
│     'WASDE March analysis delivered', {...}, priority=2)
```

```
12:05 PM ET — COMPLETE
├── Total elapsed: ~5 minutes (collection to delivery)
├── Human intervention required: NONE
│
├── Audit trail available:
│   ├── core.collection_status: collection timing, row counts
│   ├── core.event_log: enrichment details, delivery confirmation
│   ├── core.llm_call_log: model, tokens, cost, prompt/output hashes
│   └── File system: report document + chart PNGs
```

### Control Plane Summary for This Run

| Metric | Value |
|--------|-------|
| LLM calls | 1 (narrative generation) |
| Model used | Claude Sonnet |
| Total tokens | ~7,000 (4,200 in + 2,800 out) |
| Estimated cost | $0.054 |
| Latency | 22 seconds |
| Charts generated | 4 |
| Data sensitivity | Level 0 (Public) |
| Pipeline run ID | Traceable UUID linking all artifacts |

---

## 13. Phased Build Plan

### Phase 1: Control Plane Foundation

**Goal:** Build the infrastructure that every subsequent phase depends on.

- [ ] **Migration 026**: Create `core.llm_call_log` table with indexes, `record_hash`, `chain_hash` columns
- [ ] **Model Router** (`src/services/llm/model_router.py`): routing logic, Tier S/H/C enforcement, health checks, fallbacks
- [ ] **Call Logger** (`src/services/llm/call_logger.py`): log every LLM call with hash chaining and sensitivity-aware prompt storage
- [ ] **Sensitivity Config** (`src/config/sensitivity.yaml`): data source sensitivity mappings with Tier S/H/C routing rules
- [ ] **Pre-flight check**: Block Level 2+ KG contexts from cloud-bound prompts
- [ ] **Prompt Registry** (`src/prompts/registry.py`): template loading, variable injection, versioning
- [ ] **Base Analysis Template** (`src/analysis/templates/base_template.py`): abstract class for all templates
- [ ] **Integration test**: Model Router + Call Logger + test prompt end-to-end

**Deliverables:** LLM calls are routed correctly, logged with tamper-evident audit trail, and sensitivity is enforced.

### Phase 2: First Report Pipeline — WASDE

**Goal:** One report type works end-to-end, from collection trigger to delivered document.

- [ ] **WASDE Analysis Template** (`src/analysis/templates/wasde_template.py`)
- [ ] **WASDE Prompt** (`src/prompts/analysis/wasde_analysis_v1.py`)
- [ ] **Analysis Trigger** in collector_runner.py: detect WASDE completion, fire pipeline
- [ ] **Chart Templates** for WASDE (ending stocks, S/U trend, MoM waterfall)
- [ ] **Pipeline Orchestrator** (`src/pipeline/report_pipeline.py`): coordinates template → prompt → LLM → charts → assembly
- [ ] **Integration test**: Run WASDE collector → analysis fires → report generated
- [ ] **Manual validation**: Review generated report quality, iterate on prompts

**Deliverables:** WASDE day produces a complete report automatically.

### Phase 3: Additional Report Pipelines

**Goal:** Extend to all major report types.

- [ ] **CFTC Weekly** template + prompt + charts (position bars, percentile heatmap)
- [ ] **EIA Weekly** template + prompt + charts (production/stocks dual-axis)
- [ ] **Export Sales** template + prompt + charts (pace line, destination bars)
- [ ] **Crop Conditions** template + prompt + charts (G/E trend, state map)
- [ ] **Weekly Synthesis** template + prompt (combines all weekly data into brief)
- [ ] **Cross-report consistency**: ensure shared data is handled consistently
- [ ] **Prompt iteration**: refine based on output quality for each report type

**Deliverables:** Five report types running autonomously on their respective schedules.

### Phase 4: Delivery + Hardening

**Goal:** Reports reach recipients reliably, system handles failures gracefully.

- [ ] **Email Sender** (`src/services/delivery/email_sender.py`): SMTP with attachments
- [ ] **File Manager** (`src/services/delivery/file_manager.py`): organized storage, archival
- [ ] **Notification Service** (`src/services/delivery/notification_service.py`): SMS alerts
- [ ] **Error Recovery**: retry logic for failed LLM calls, partial report handling
- [ ] **Validation Hardening**: catch edge cases (empty data, API outages, model timeouts)
- [ ] **Full redaction engine**: regex-based scanning for secrets, deal IDs, PII before Tier C calls
- [ ] **Policy engine config**: allow/deny/require-approval table wrapping the model router
- [ ] **Monitoring Dashboard** via MCP: new tools for pipeline health, cost tracking
- [ ] **Chain verification job**: daily cron that walks `llm_call_log` hash chain, alerts on breaks
- [ ] **Documentation**: operator runbook, troubleshooting guide

**Deliverables:** Production-grade autonomous pipeline that runs without supervision.

### Latency Targets (SLOs)

| Report Type | Target | Measurement |
|-------------|--------|-------------|
| WASDE Analysis | End-to-end < 5 minutes | Collection trigger to delivered document |
| CFTC Weekly | End-to-end < 3 minutes | Collection trigger to delivered document |
| EIA Weekly | End-to-end < 3 minutes | Collection trigger to delivered document |
| Export Sales | End-to-end < 3 minutes | Collection trigger to delivered document |
| Crop Conditions | End-to-end < 3 minutes | Collection trigger to delivered document |
| Weekly Synthesis | End-to-end < 20 minutes | All inputs ready to delivered document |
| LLM narrative call | p95 < 30 seconds | Single API call latency |

### Governance Cadence

Adopted from the Sovereign Intelligence Architecture to prevent system drift:

| Frequency | Review |
|-----------|--------|
| **Monthly** | Cost report (per-model, per-report-type), error rates, latency vs SLOs |
| **Quarterly** | Prompt quality review (compare outputs to analyst edits), model portfolio assessment, sensitivity mapping review |
| **Semi-annual** | Full policy review, audit log integrity verification, DR drill (restore from backup) |
| **Annual** | Architecture review, threat model update, sovereignty audit (prove no Level 2+ data left local tiers) |

---

## 14. Future Considerations

### 14.1 Air-Gapped Trading Subsystem

For actual trade signal generation, the system would need a fully air-gapped environment:

- Dedicated hardware with no internet access
- Local models only (no cloud API calls ever)
- Separate database with no connection to the analysis pipeline
- Physical security controls
- This is a Level 3+ sensitivity operation and is architecturally separate from the report pipeline

### 14.2 Fine-Tuning Local Models

Once the pipeline produces enough reports with analyst feedback:

- Collect (prompt, response, analyst_edit) triples
- Fine-tune local Llama models on RLC's specific writing style and analytical approach
- Gradually shift more tasks from cloud to local fine-tuned models
- Reduces cost and latency while improving domain specificity

### 14.3 Client-Facing Dashboard

A web interface showing:

- Report generation status and history
- Data freshness across all collectors
- KG visualization (nodes, edges, contexts)
- Cost tracking and model usage
- Could be built on the existing MCP tool layer

### 14.4 Multi-Report Synthesis

A weekly market brief that combines insights from all report types:

- WASDE supply/demand context + CFTC positioning + EIA energy data + export pace
- Cross-market correlations (soybean oil ↔ RINs ↔ palm oil)
- This is the `weekly_brief_v1` synthesis template — the most complex LLM task
- Requires all individual reports to complete first (dependency-driven trigger)

### 14.5 Containerization (from SIA)

When moving to production, the SIA recommends per-component isolation:

- Collectors in separate containers with network egress limited to government API endpoints
- Report generation in a Tier S container with no outbound network
- Model Router as the sole gateway to cloud APIs
- Read-only root filesystems, user namespace remapping, tmpfs for scratch
- This is infrastructure hardening — not needed for the initial build, but important for production

### 14.6 Disaster Recovery

The SIA treats DR as part of sovereignty. Minimum controls to implement:

- PostgreSQL WAL archiving for point-in-time recovery (existing: `docs/DATABASE_EXPORT_GUIDE.md`)
- Prompt registry versioned in git (built into Phase 1 design)
- Model manifests: which Ollama models at which versions are required
- Quarterly restore drill: prove the system can rebuild from backups on clean hardware

---

## Appendix A: Key File Paths

| Component | Path |
|-----------|------|
| Dispatcher daemon | `src/dispatcher/dispatcher.py` |
| Collector runner | `src/dispatcher/collector_runner.py` |
| Collector registry | `src/dispatcher/collector_registry.py` |
| Delta summarizer | `src/dispatcher/delta_summarizer.py` |
| Master scheduler | `src/schedulers/master_scheduler.py` |
| CLI | `src/dispatcher/cli.py` |
| DB config | `src/services/database/db_config.py` |
| MCP server | `src/mcp/commodities_db_server.py` |
| KG manager | `src/knowledge_graph/kg_manager.py` |
| KG enricher | `src/knowledge_graph/kg_enricher.py` |
| Seasonal calculator | `src/knowledge_graph/seasonal_calculator.py` |
| Pace calculator | `src/knowledge_graph/pace_calculator.py` |
| Report writer agent | `src/agents/reporting/report_writer_agent.py` |
| Document builder | `src/services/document/document_builder.py` |
| Base collector | `src/agents/base/base_collector.py` |
| **NEW: Model router** | `src/services/llm/model_router.py` |
| **NEW: Call logger** | `src/services/llm/call_logger.py` |
| **NEW: Sensitivity config** | `src/config/sensitivity.yaml` |
| **NEW: Prompt registry** | `src/prompts/registry.py` |
| **NEW: Analysis templates** | `src/analysis/templates/` |
| **NEW: Report pipeline** | `src/pipeline/report_pipeline.py` |
| **NEW: Delivery services** | `src/services/delivery/` |

## Appendix B: Database Schemas

| Schema | Purpose | Key Tables |
|--------|---------|------------|
| `public` | Dimension tables | data_source, commodity, country |
| `core` | CNS + Control Plane | collection_status, event_log, llm_call_log, kg_* |
| `bronze` | Raw collected data | cftc_cot, wasde_*, eia_*, fas_* |
| `silver` | Cleaned/standardized | ethanol_weekly, nass_crop_condition_ge, monthly_realized |
| `gold` | Analytics-ready views | wasde_balance_sheet, renewable_fuel_plants |
| `reference` | Static reference data | epa_generally_applicable_pathways |
| `audit` | System audit trails | (future use) |
| `meta` | Metadata | (future use) |

## Appendix C: Migration History

| # | File | What It Creates |
|---|------|----------------|
| 018 | `018_collection_status.sql` | `core.collection_status` table |
| 019 | `019_event_log.sql` | `core.event_log` + `core.llm_briefing` view |
| 020 | `020_kg_tables.sql` | `core.kg_node`, `kg_edge`, `kg_context` |
| 021 | `021_kg_provenance.sql` | `core.kg_source`, `kg_provenance`, `kg_processing_batch` |
| 025 | `025_epa_pathway_determinations.sql` | `reference.epa_generally_applicable_pathways`, `bronze.epa_pathway_*` |
| **026** | **`026_llm_call_log.sql`** | **`core.llm_call_log` (Phase 1)** |
