# LLM Report Generation Guide

**Last Updated:** March 2, 2026

This guide explains how the RLC-Agent autonomous report pipeline works, using the WASDE monthly report as a working example. The same architecture will power the weekly HigbyBarrett report once we finish building it out.

---

## How It Works (Big Picture)

The system has four layers that run automatically:

```
1. DATA COLLECTION     Collectors pull data from APIs on a schedule
         |
2. DELTA ANALYSIS      System computes what changed since last time
         |
3. LLM NARRATIVE       Claude writes a professional analyst report
         |
4. DELIVERY            Report logged, eventually emailed/uploaded
```

Each layer is independent and observable. If something breaks at layer 2, layer 1's data is still safely in the database. If the LLM produces a bad narrative, validation catches it before delivery.

---

## The WASDE Pipeline (Working Example)

The WASDE pipeline runs monthly. Here is exactly what happens when USDA releases a new WASDE report.

### Step 1: Dispatcher Fires the Collector

The dispatcher daemon watches the clock. On WASDE day (e.g., April 9, 2026 at 12:00 PM ET), it fires the `usda_wasde` collector automatically.

You can also fire it manually from the command line:

```bash
python -m src.dispatcher run usda_wasde
```

The collector hits the USDA FAS PSD API, downloads fresh balance sheet data for all commodities and countries, and inserts rows into the `bronze.fas_psd` database table. It logs its progress to `core.collection_status`.

### Step 2: Delta Summarizer Runs

After the collector succeeds, the system automatically computes month-over-month changes. For example, the March 2026 WASDE showed:

- **Corn**: ending stocks cut 2,540 (1000 MT), exports raised 2,540
- **Soybeans**: no changes
- **Wheat**: ending stocks raised 131 (FSI cut 131)

These deltas are computed by comparing the **same marketing year** across two different report dates in the database.

### Step 3: Analysis Template Gathers Context

The `WASDeAnalysisTemplate` class pulls together everything the LLM needs:

1. **US balance sheets** -- current vs prior for corn, soybeans, wheat
2. **Global S&D** -- Brazil, Argentina, China production & exports
3. **Knowledge Graph context** -- analyst frameworks, historical patterns, seasonal norms
4. **Metadata** -- report date, marketing year, whether it's the August WASDE (methodology shift)

All of this gets formatted into a structured prompt.

### Step 4: LLM Generates the Narrative

The system routes to the appropriate Claude model based on task type and sensitivity level, sends the prompt, and receives a ~500-word professional analysis. The March 2026 test produced this output:

> **Headline Changes**
> - US corn exports raised 2.54 MMT to 83.82 MMT, driving ending stocks down to 54.02 MMT (12.9% stocks-to-use)
> - Brazil soybean production increased 2 MMT to 180 MMT
> - Argentina wheat exports raised 2 MMT on higher production
>
> **US Corn** -- The USDA made a significant adjustment to corn demand, raising exports by 2.54 MMT while leaving all other categories unchanged. This pushes 2025/26 ending stocks down to 54.02 MMT, a stocks-to-use ratio of 12.9% (down from 13.6%). The export increase likely reflects stronger-than-expected shipments...

The full narrative covers: Headline Changes, US Corn, US Soybeans, US Wheat, Global Context, and Market Implications.

### Step 5: Validation & Logging

The pipeline validates the narrative (must be 50+ words, must mention at least one commodity) and logs everything:

- `core.event_log` -- "WASDE report generated, 464 words"
- `core.llm_call_log` -- model used, tokens, cost ($0.10), latency (34 sec), hash chain for audit

### Cost

Each WASDE report costs roughly **$0.10** and takes about **30 seconds** to generate. The system uses Claude Opus for analysis tasks, which produces the highest quality output. For less critical tasks (summaries, formatting), it automatically routes to cheaper/faster models.

---

## What the HB Weekly Report Pipeline Will Look Like

The HB report uses the exact same architecture but with more data sources and a different prompt template.

### Data Sources (Collected Automatically)

| Source | Day | What It Provides |
|--------|-----|------------------|
| CME Settlements | Daily 5 PM | Futures prices (ZC, ZW, KE, ZS, ZM, ZL, CL, NG) |
| CFTC COT | Friday 3:30 PM | Managed money positioning, commercial hedging |
| USDA Export Sales | Thursday 8:30 AM | Weekly export commitments by destination |
| EIA Ethanol | Wednesday 10:30 AM | Production, stocks, implied corn grind |
| EIA Petroleum | Wednesday 10:30 AM | Crude/gas/diesel stocks and prices |
| NASS Crop Progress | Monday 4 PM | Planting %, condition ratings (seasonal) |
| Drought Monitor | Thursday 8:30 AM | US drought status |
| USDA WASDE | Monthly ~10th | Balance sheet revisions (when available) |

### Weekly Timeline

```
MONDAY
  4:00 PM  NASS Crop Progress arrives (seasonal, Apr-Nov)
  8:00 PM  Pre-report check: verify all weekly data collected

TUESDAY
  6:00 AM  HB Report Pipeline fires:
           1. Gather all data from the past week
           2. Compute week-over-week deltas (prices, positioning, stocks)
           3. Pull Knowledge Graph context (analyst frameworks)
           4. Send structured prompt to Claude
           5. Receive ~1,200 word narrative
           6. Validate and log
           7. (Future) Assemble .docx with charts, upload to Dropbox

WEDNESDAY - FRIDAY
  Mid-week collectors run for next week's report
```

### HB Report Sections (Target)

Based on the sample reports in `domain_knowledge/sample_reports/`:

1. **Executive Summary** (200-250 words) -- Key price moves, supply/demand developments, sentiment
2. **Macro & Energy Update** (150 words) -- USD, crude oil, natural gas, transportation costs
3. **Weather & Crop Conditions** (150 words) -- Argentina, Brazil, US growing regions
4. **Corn Analysis** (200 words) -- Prices, ethanol grind, feed, exports, technicals
5. **Wheat Analysis** (200 words) -- SRW/HRW/HRS, Black Sea competition, domestic use
6. **Soybean Complex** (250 words) -- Beans, meal, oil, crush margins, China, biofuel
7. **Key Triggers to Watch** (5-7 bullets) -- Upcoming reports, price levels, weather risks
8. **Market Data Appendix** -- Prices, spreads, COT tables

### What's Built vs What's Left

| Component | Status |
|-----------|--------|
| Data collectors (all 7 weekly sources) | Built, registered in dispatcher |
| Dispatcher scheduling (weekly triggers) | Built, running |
| Delta summarizers (CFTC, crop conditions, ethanol) | Built |
| Knowledge Graph (analyst frameworks) | Built (224 nodes, 143 edges) |
| HB Analysis Template | **Needs to be built** (like WASDeAnalysisTemplate) |
| HB Prompt Template | **Needs to be built** (like WASDeAnalysisV1) |
| Chart generation | **Phase 2.5** (stubs in place) |
| .docx assembly | **Phase 2.5** (stubs in place) |
| Dropbox upload | **Phase 2.5** |

---

## How to Monitor the System

### Check What Data Has Arrived

The MCP server (accessible through Claude Desktop or the CLI) has tools for this:

```
get_data_freshness()    -- Shows when each collector last ran
get_briefing()          -- Shows unread events (new data, failures, alerts)
get_collection_history('usda_wasde')  -- History for a specific collector
```

Or from the command line:

```bash
# What's scheduled for today?
python -m src.dispatcher today

# What ran recently?
python -m src.dispatcher status

# List all registered collectors
python -m src.dispatcher list
```

### Check the Event Log

Every significant system event is logged to `core.event_log`. You can query it:

```sql
-- Recent events
SELECT event_time, event_type, source, summary
FROM core.event_log
ORDER BY id DESC LIMIT 20;

-- Just report generation events
SELECT event_time, summary, details->>'word_count' as words
FROM core.event_log
WHERE event_type = 'report_generated'
ORDER BY id DESC;
```

### Check LLM Costs

Every LLM call is logged with cost tracking:

```sql
SELECT created_at, task_type, model_id, tokens_in, tokens_out, cost_usd
FROM core.llm_call_log
ORDER BY created_at DESC LIMIT 10;
```

---

## How to Run a Report Manually

### WASDE (Monthly)

```python
from dotenv import load_dotenv
load_dotenv()

from src.analysis.templates.wasde_template import WASDeAnalysisTemplate
from src.pipeline.report_pipeline import ReportPipeline

template = WASDeAnalysisTemplate()
print("Data ready:", template.check_data_ready())

pipeline = ReportPipeline(template)
result = pipeline.run(triggered_by='manual')

print(f"Success: {result.success}")
print(f"Words: {len(result.llm_narrative.split()) if result.llm_narrative else 0}")
print(result.llm_narrative)
```

### HB Weekly (Coming Soon)

Once the HB template is built, it will work the same way:

```python
from src.analysis.templates.hb_weekly_template import HBWeeklyTemplate
from src.pipeline.report_pipeline import ReportPipeline

template = HBWeeklyTemplate()
pipeline = ReportPipeline(template)
result = pipeline.run(triggered_by='manual')
```

The pipeline handles everything: data gathering, delta computation, KG enrichment, LLM call, validation, and logging.

---

## What Felipe Needs to Do Each Week (Future State)

Once the HB pipeline is fully built, the weekly workflow will be:

1. **Monday evening**: System automatically verifies all weekly data has arrived. If anything is missing, it logs an alert.

2. **Tuesday 6:00 AM**: Pipeline fires automatically. Felipe receives a notification (email or Slack) with the generated report.

3. **Felipe reviews**: Read the LLM narrative, check the data tables, verify the analysis makes sense. The LLM is good but not perfect -- it needs human review.

4. **Felipe edits**: Make any corrections, add client-specific commentary, adjust tone. The LLM draft saves ~2 hours of writing time but is a first draft, not a final product.

5. **Felipe approves**: Once satisfied, trigger the .docx assembly and Dropbox upload (or do this manually for now).

### What to Watch For in LLM Output

- **Hallucinated numbers**: The prompt tells Claude to only use provided data, but always verify key figures against the database
- **Missing context**: The LLM doesn't know about last week's client conversations or market rumors not captured in data
- **Stale data**: If a collector failed, the LLM may use old data without realizing it. Check `get_data_freshness()` if something looks off
- **Tone**: The system prompt targets "professional institutional" tone. Adjust if it doesn't match HB's voice

---

## Architecture Diagram

```
                    USDA APIs     CFTC     EIA      CME
                       |           |        |        |
                       v           v        v        v
                  +-----------------------------------------+
                  |         COLLECTOR LAYER                  |
                  |  22 registered collectors, each with     |
                  |  its own API client and parser            |
                  +-----------------------------------------+
                              |
                              v
                  +-----------------------------------------+
                  |        BRONZE LAYER (PostgreSQL)         |
                  |  Raw data: fas_psd, cftc_cot,           |
                  |  eia_ethanol, cme_settlements, etc.      |
                  +-----------------------------------------+
                              |
                              v
                  +-----------------------------------------+
                  |     ANALYSIS TEMPLATE                    |
                  |  gather_data() -> compute_analysis()     |
                  |  + Knowledge Graph enrichment            |
                  +-----------------------------------------+
                              |
                              v
                  +-----------------------------------------+
                  |     REPORT PIPELINE                      |
                  |  Prompt rendering -> Model routing ->    |
                  |  LLM call -> Validation -> Event log     |
                  +-----------------------------------------+
                              |
                              v
                  +-----------------------------------------+
                  |     DELIVERY (Phase 2.5)                 |
                  |  Charts -> .docx assembly -> Dropbox     |
                  +-----------------------------------------+
```

---

## Key File Locations

| What | Where |
|------|-------|
| Dispatcher CLI | `python -m src.dispatcher {start\|run\|today\|status\|list}` |
| Master schedule | `src/schedulers/master_scheduler.py` |
| Collector registry | `src/dispatcher/collector_registry.py` |
| Collector runner | `src/dispatcher/collector_runner.py` |
| WASDE template | `src/analysis/templates/wasde_template.py` |
| WASDE prompt | `src/prompts/analysis/wasde_analysis_v1.py` |
| Report pipeline | `src/pipeline/report_pipeline.py` |
| Model router | `src/services/llm/model_router.py` |
| LLM client | `src/services/llm/llm_client.py` |
| Call logger | `src/services/llm/call_logger.py` |
| Knowledge Graph | `src/knowledge_graph/kg_manager.py` |
| Sample HB reports | `domain_knowledge/sample_reports/` |
| Generated HB reports | `output/reports/higby_barrett/` |
| HB report prompt (v2) | `output/reports/higby_barrett/HB_Weekly_PROMPT_V2_*.txt` |

---

## Questions?

If something isn't clear or you want to test something, the easiest way is to open a Claude Code session in the `C:\dev\RLC-Agent` directory and ask. Claude has full access to the database, the Knowledge Graph, and all the code.
