# RLC-Agent System Inventory & Architecture

*Generated: 2026-01-23*

## Executive Summary

The RLC-Agent codebase contains **120+ Python files** organized into a modular architecture for agricultural commodity data collection, analysis, and reporting. The system uses a **medallion data architecture** (Bronze/Silver/Gold) with PostgreSQL as the primary database.

---

## Component Categories

### 1. ENTRY POINTS & ORCHESTRATORS

| Component | Location | Purpose | Status |
|-----------|----------|---------|--------|
| **main.py** | src/main.py | CLI entry point for all operations | ACTIVE |
| **agent_scheduler.py** | rlc_scheduler/agent_scheduler.py | Central scheduler daemon (USDA calendars, holidays) | ACTIVE |
| **pipeline_orchestrator.py** | src/orchestrators/ | ETL pipeline coordinator | ACTIVE |
| **hb_report_orchestrator.py** | src/orchestrators/ | Weekly report generation | ACTIVE |
| **master_scheduler.py** | src/schedulers/ | Collection timing management | ACTIVE |

### 2. CORE AGENTS (src/agents/core/)

| Agent | Purpose | Dependencies |
|-------|---------|--------------|
| **master_agent.py** | Central "brain" - plans, delegates, observes | All other agents |
| **data_agent.py** | Data retrieval and aggregation | Collectors |
| **database_agent.py** | Database operations | PostgreSQL |
| **verification_agent.py** | Data quality checks | Bronze/Silver tables |
| **memory_manager.py** | Long-term memory (Notion) | Notion API |
| **approval_manager.py** | Autonomy level management | - |

### 3. DATA COLLECTORS (src/agents/collectors/)

#### US Government Sources
| Collector | Data Source | Frequency |
|-----------|-------------|-----------|
| usda_fas_collector.py | Export Sales, PSD | Weekly/Monthly |
| usda_nass_collector.py | Crop Progress, Production | Weekly/Monthly |
| usda_ers_collector.py | Feed Grains, Oil Crops | Monthly |
| usda_ams_collector.py | Commodity Prices | Daily |
| cftc_cot_collector.py | Commitments of Traders | Weekly (Friday) |
| eia_ethanol_collector.py | Ethanol Production | Weekly |
| eia_petroleum_collector.py | Petroleum Status | Weekly |
| epa_rfs_collector.py | Renewable Fuel Standard | Monthly |
| census_trade_collector.py | International Trade | Monthly |
| drought_collector.py | Drought Monitor | Weekly |

#### International Sources
| Collector | Country/Region | Data |
|-----------|----------------|------|
| conab_collector.py | Brazil | Crop estimates |
| imea_collector.py | Brazil (Mato Grosso) | Soybean forecasts |
| abiove_collector.py | Brazil | Vegetable oils |
| ibge_sidra_collector.py | Brazil | Economic stats |
| magyp_collector.py | Argentina | Crop data |
| canada_cgc_collector.py | Canada | Grain Commission |
| canada_statscan_collector.py | Canada | Statistics |
| mpob_collector.py | Malaysia | Palm oil |
| faostat_collector.py | Global | FAO statistics |

### 4. ANALYSIS AGENTS (src/agents/analysis/)

| Agent | Function |
|-------|----------|
| **fundamental_analyzer.py** | S&D balance, stocks-to-use, yield trends |
| **price_forecaster.py** | Price predictions with confidence intervals |
| **spread_and_basis_analyzer.py** | Spread relationships, basis patterns |

### 5. REPORTING AGENTS (src/agents/reporting/)

| Agent | Output |
|-------|--------|
| **report_writer_agent.py** | HB Weekly Report narratives |
| **price_data_agent.py** | Price data formatting |
| **market_research_agent.py** | Market analysis content |
| **internal_data_agent.py** | Internal RLC data |

### 6. SCHEDULER AGENTS (rlc_scheduler/agents/)

| Agent | Schedule | Function |
|-------|----------|----------|
| **weather_email_agent.py** | 3x weekday, 1x Sat, 2x Sun | Weather email forwarding |
| **weather_collector_agent.py** | Daily 6:00 AM | Weather data collection |
| **data_checker_agent.py** | On-demand | Data validation |

### 7. INTEGRATION AGENTS (src/agents/integration/)

| Agent | Service | Function |
|-------|---------|----------|
| **email_agent.py** | Gmail | Triage, summarize, respond |
| **calendar_agent.py** | Google Calendar | Schedule management |
| **notion_manager.py** | Notion | Knowledge base, memory |

### 8. SERVICES (src/services/)

| Service | Location | Purpose |
|---------|----------|---------|
| usda_api.py | api/ | USDA API wrapper |
| census_api.py | api/ | Census API wrapper |
| weather_api.py | api/ | OpenWeather/Open-Meteo |
| db_config.py | database/ | PostgreSQL/SQLite config |
| schema.py | database/ | Medallion schema |
| document_builder.py | document/ | Word doc generation |
| location_service.py | services/ | Geographic management |
| city_enrollment_service.py | services/ | City enrollment |

### 9. SCRIPTS (scripts/)

#### Data Collection
- collect.py - Manual collection runner
- pull_historical_weather.py
- pull_extended_historical.py
- pull_weekly_inspections.py
- pull_census_trade.py (+ variants)
- pull_cgc_trade.py
- pull_statcan_trade.py

#### Data Ingestion
- ingest_wasde.py
- ingest_wheat_data.py
- ingest_corn_trade.py
- ingest_feed_grains_data.py
- ingest_oil_crops_data.py
- ingest_ers_data.py

#### Transformations
- scripts/transformations/silver_transformations.py
- scripts/transformations/wheat_silver_transformations.py
- scripts/transformations/oilseed_silver_transformations.py

#### Visualizations
- scripts/visualizations/gold_visualizations.py
- scripts/visualizations/wheat_visualizations.py
- scripts/visualizations/oilseed_visualizations.py

---

## Data Flow Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                         EXTERNAL DATA SOURCES                        │
│  USDA (FAS/NASS/ERS/AMS) | CFTC | EIA | EPA | Census | Weather APIs │
│  CONAB | IMEA | ABIOVE | MAGYP | StatsCan | CGC | MPOB | FAO        │
└─────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────┐
│                           COLLECTORS                                 │
│  src/agents/collectors/us/     src/agents/collectors/south_america/ │
│  src/agents/collectors/canada/ src/agents/collectors/asia/          │
└─────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────┐
│                      PIPELINE ORCHESTRATOR                           │
│              src/orchestrators/pipeline_orchestrator.py              │
└─────────────────────────────────────────────────────────────────────┘
                                    │
                    ┌───────────────┼───────────────┐
                    ▼               ▼               ▼
            ┌───────────┐   ┌───────────┐   ┌───────────┐
            │  BRONZE   │   │  SILVER   │   │   GOLD    │
            │  (Raw)    │──▶│(Standard) │──▶│(Analysis) │
            └───────────┘   └───────────┘   └───────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────┐
│                        ANALYSIS AGENTS                               │
│  fundamental_analyzer.py | price_forecaster.py | spread_analyzer.py │
└─────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────┐
│                       REPORTING AGENTS                               │
│         report_writer_agent.py | hb_report_orchestrator.py          │
└─────────────────────────────────────────────────────────────────────┘
                                    │
                    ┌───────────────┼───────────────┐
                    ▼               ▼               ▼
            ┌───────────┐   ┌───────────┐   ┌───────────┐
            │   EMAIL   │   │  DOCUMENTS│   │  POWERBI  │
            │  (Gmail)  │   │  (.docx)  │   │ (Exports) │
            └───────────┘   └───────────┘   └───────────┘
```

---

## Scheduler Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                    AGENT_SCHEDULER.PY (Daemon)                       │
│                 rlc_scheduler/agent_scheduler.py                     │
│                                                                      │
│  Manages:                                                            │
│  - USDA release calendar awareness                                   │
│  - Federal holiday detection and report shifting                     │
│  - Weather email schedules (7:30, 13:00, 20:00 weekdays)            │
│  - WASDE, Crop Progress, Export Sales timing                        │
└─────────────────────────────────────────────────────────────────────┘
                                    │
         ┌──────────────┬───────────┼───────────┬──────────────┐
         ▼              ▼           ▼           ▼              ▼
┌─────────────┐ ┌─────────────┐ ┌─────────┐ ┌─────────┐ ┌─────────────┐
│Weather Email│ │Weather Coll.│ │ WASDE   │ │Export   │ │Crop Progress│
│   Agent     │ │   Agent     │ │ Agent   │ │ Sales   │ │   Agent     │
└─────────────┘ └─────────────┘ └─────────┘ └─────────┘ └─────────────┘
```

---

## IDENTIFIED GAPS

### 1. Missing/Incomplete Components
| Gap | Description | Priority |
|-----|-------------|----------|
| **Price Database** | Historical futures/cash prices not yet built | HIGH |
| **CME Data Collector** | Settlements mentioned but agent not found | HIGH |
| **usda_export_sales_agent** | Referenced in scheduler but file missing | MEDIUM |
| **Tender Alert System** | alert_system.py exists but integration unclear | LOW |

### 2. Incomplete Data Coverage
| Area | Current State | Needed |
|------|---------------|--------|
| Argentina Weather | Only 2025-2026 for some locations | 2010-2026 |
| St. Louis Weather | Only 53 days (newly enrolled) | Full history |
| Price Data | No historical price database | CME/ICE data |

### 3. Documentation Gaps
| Item | Status |
|------|--------|
| API documentation | Partial |
| Agent interaction diagrams | Missing |
| Deployment guide | Partial |

---

## IDENTIFIED DUPLICATES/OVERLAPS

### 1. Report Folders (DUPLICATE)
| Location 1 | Location 2 | Resolution |
|------------|------------|------------|
| data/reports/marketing_years/ | (Previously created elsewhere?) | Consolidate to data/reports/ |
| data/reports/special_situations/ | (User mentioned another location) | Verify and merge |

### 2. Script Variants (POTENTIAL DUPLICATES)
| Files | Purpose | Action |
|-------|---------|--------|
| pull_census_trade.py | Census trade pull | Review variants |
| pull_census_trade_modified.py | Modified version | Consolidate? |
| pull_census_trade_git.py | Git version | Consolidate? |

### 3. Base Collector Classes
| Location | Purpose |
|----------|---------|
| src/agents/base/base_collector.py | Main base class |
| src/collectors/base_collector.py | Another base? |
| scripts/collectors/ | Script-based collectors |

*Recommendation: Consolidate to single inheritance hierarchy*

### 4. Database Config
| Location | Purpose |
|----------|---------|
| src/services/database/db_config.py | Main config |
| deployment/db_config.py | Deployment copy |

*Recommendation: Single source of truth*

---

## UNUSED/ORPHANED ASSETS

| File | Last Purpose | Status |
|------|--------------|--------|
| Silver Operators/IBKR API Connector.py | Interactive Brokers | SEMI-ACTIVE |
| biotrack/biotrack_ai.py | Sustainability tracking | SEMI-ACTIVE |
| inventory_balance_sheet_data.py | Balance sheet inventory | UNCLEAR |

---

## RECOMMENDATIONS

### Immediate Actions
1. **Create usda_export_sales_agent.py** - Referenced but missing
2. **Build CME price collector** - Critical for analysis
3. **Consolidate report folders** - Avoid confusion
4. **Pull historical data for Argentina** - Complete coverage

### Windows Service Candidates
These processes should run as Windows Services/Tasks instead of in Claude:

| Process | Current | Recommended |
|---------|---------|-------------|
| agent_scheduler.py | Background in Claude | Windows Task Scheduler |
| weather_collector_agent.py | Via scheduler | Windows Task (daily 6 AM) |
| weather_email_agent.py | Via scheduler | Windows Task (3x daily) |
| data_checker_agent.py | Manual | Windows Task (daily) |

### Desktop LLM Handoff Candidates
Tasks suitable for Desktop LLM (Ollama) to run independently:

1. **Weather report generation** - Study context, generate summaries
2. **Marketing year analysis** - Read special situations, compare patterns
3. **Email triage** - Classify and summarize incoming emails
4. **Report editing** - Review and polish generated reports

---

## FILE COUNT SUMMARY

| Category | Count |
|----------|-------|
| Core Agents | 7 |
| Data Collectors | 25+ |
| Analysis Agents | 3 |
| Reporting Agents | 4 |
| Integration Agents | 3 |
| Scheduler Agents | 4 |
| Services | 10+ |
| Scripts | 40+ |
| Config Files | 10+ |
| **TOTAL** | **120+** |

---

## NEXT STEPS

1. Review this inventory with user
2. Prioritize gap resolution
3. Set up Windows Task Scheduler for background processes
4. Document Desktop LLM handoff procedures
5. Create visual flow diagram (Mermaid/draw.io)
