# RLC-Agent Project - Notion Organization Guide

**Generated**: January 2026
**Purpose**: Comprehensive project structure for importing into Notion

---

## Project Overview

| Property | Value |
|----------|-------|
| **Project Name** | RLC-Agent |
| **Description** | Automated commodity market data gathering, analysis, and reporting system |
| **Architecture** | Bronze/Silver/Gold data lake with multi-agent orchestration |
| **Primary Database** | PostgreSQL |
| **Status** | Active Development |

---

## Notion Database Structure Recommendations

### 1. Components Database

| Component | Type | Status | Priority | Description |
|-----------|------|--------|----------|-------------|
| PostgreSQL Database | Infrastructure | Completed | - | Bronze/Silver/Gold data architecture |
| USDA AMS Collector | Data Collector | Completed | - | Daily grain/ethanol/livestock prices |
| USDA Export Sales Collector | Data Collector | Completed | - | Weekly export sales data |
| USDA Crop Progress Collector | Data Collector | Completed | - | Weekly planting/harvest progress |
| Census Trade Collector | Data Collector | Completed | - | US trade statistics |
| EIA Petroleum Collector | Data Collector | Completed | - | Energy data |
| Brazil COMEX Collector | Data Collector | Completed | - | Brazil trade data |
| Argentina INDEC Collector | Data Collector | Completed | - | Argentina trade data |
| Canada AAFC Collector | Data Collector | Completed | - | Canada crop data |
| WASDE PDF Parser | Data Collector | Completed | - | Monthly S&D reports |
| Wheat Tender Monitoring | Analysis | Completed | - | Real-time tender tracking |
| PowerBI Dashboards | Visualization | Completed | - | 5 dashboard files |
| HB Weekly Report | Automation | In Progress | High | Weekly report automation |
| BioTrack Railcar | Monitoring | In Progress | Medium | Prototype stage |
| RLC Orchestrator | Infrastructure | In Progress | High | Phase 1 implementation |
| CME Futures Data | Data Collector | To Do | High | Real-time futures prices |
| NOPA Soybean Crush | Data Collector | To Do | High | Monthly crush data |
| Ukraine/Russia Exports | Data Collector | To Do | Medium | Black Sea export data |
| Master Scheduler | Infrastructure | To Do | High | Automated collection scheduling |

### 2. Tasks Database (54 Total Tasks)

#### CRITICAL / URGENT

| Task | Category | Priority | Status | Notes |
|------|----------|----------|--------|-------|
| Rotate API keys in .env | Security | URGENT | Not Started | Keys exposed in api Manager folder |
| Build USDA WASDE Collector | Data | Critical | Not Started | Key monthly S&D data |
| Build NOPA US Soy Crush Collector | Data | Critical | Not Started | Critical for soy analysis |

#### BioTrack Rail Monitoring (6 tasks)

| Task | Priority | Status |
|------|----------|--------|
| Test rail car tracker with real data | High | Not Started |
| Satellite Integration (Sentinel-2) | Medium | Planned Phase 2 |
| IoT Sensor Integration | Medium | Planned Phase 2 |
| Multi-camera Fusion | Medium | Planned Phase 2 |
| Predictive Volume Models | Low | Planned Phase 3 |
| Anomaly Detection | Low | Planned Phase 3 |

#### Data Collectors - Missing (13 tasks)

| Data Source | Region | Priority |
|-------------|--------|----------|
| USDA WASDE | US | Critical |
| NOPA US Soy Crush | US | Critical |
| CME Futures Prices | US | Critical |
| UN Comtrade | Global | High |
| USDA ERS Wheat Yearbook | US | Medium |
| Eurostat EU Ag Data | Europe | Medium |
| EU MARS Bulletin | Europe | Medium |
| Russia SovEcon | Russia | Medium |
| China GACC Imports | China | Medium |
| Australia ABARES | Australia | Medium |
| Ukraine Ministry Ag | Ukraine | Medium |
| Argentina Rosario Exchange | South America | Medium |
| Indonesia GAPKI | Southeast Asia | Low |

#### Agent Architecture (7 tasks)

| Agent | Purpose | Priority |
|-------|---------|----------|
| Master Scheduler | Automated collection scheduling | High |
| Analyst Agent | Central orchestrator | High |
| Data Checker Agent | Source vs DB validation | High |
| Tool Registry | Modular tool access | High |
| Trade Flow Analysis Agent | Trade pattern analysis | Medium |
| Price Analysis Agent | Price/spread analysis | Medium |
| Reporting Team Agents | Automated report generation | Medium |

#### HB Report Automation (6 tasks)

| Integration | Data Type | Priority |
|-------------|-----------|----------|
| USDA WASDE → HB Report | Monthly S&D | High |
| USDA Export Sales → HB Report | Weekly exports | High |
| NOPA Crush → HB Report | Monthly crush | High |
| CME Futures → HB Report | Daily prices | High |
| USDA Crop Progress → HB Report | Planting/harvest | Medium |
| CFTC COT Reports → HB Report | Positioning | Medium |

### 3. Documentation Database

| Document | Category | Description | Last Updated |
|----------|----------|-------------|--------------|
| ARCHITECTURE_PLAN.md | Design | Multi-agent system architecture | Active |
| AUTOMATED_DATA_PIPELINE.md | Infrastructure | ETL pipeline design | Active |
| BALANCE_SHEET_KNOWLEDGE.md | Knowledge | Commodity balance sheet logic | Reference |
| CONSOLIDATION_PLAN.md | Planning | System integration roadmap | Active |
| DATA_SOURCE_REGISTRY.md | Reference | All data source specifications | Active |
| HB_REPORT_AUTOMATION_GUIDE.md | Guide | HB report automation process | Active |
| INCOMPLETE_TASKS_SUMMARY.md | Tracking | 54 pending tasks | Active |
| POWERBI_DASHBOARD_GUIDE.md | Guide | Dashboard setup instructions | Reference |
| WHEAT_TENDER_MONITORING.md | Guide | Tender system documentation | Reference |

---

## Project Directory Structure

```
/home/user/RLC-Agent/
├── src/                          # Core source code
│   ├── agents/                   # Agent implementations
│   │   ├── collectors/           # 41+ data collectors
│   │   ├── analysis/             # Analysis agents
│   │   ├── reporting/            # Report generation
│   │   ├── integration/          # External integrations (Notion)
│   │   └── base/                 # Base agent classes
│   ├── orchestrators/            # Workflow coordinators
│   ├── schedulers/               # Task scheduling
│   ├── services/                 # Shared services
│   ├── tools/                    # Agent tools
│   └── utils/                    # Utilities
├── database/                     # Database schemas and migrations
├── PowerBI/                      # 5 dashboard files
├── biotrack/                     # Rail car monitoring prototype
├── rlc-orchestrator/             # Orchestration system (Phase 1)
├── scripts/                      # Automation scripts
├── docs/                         # 20+ documentation files
├── config/                       # Configuration files
└── tests/                        # Test suites
```

---

## Key Metrics

| Metric | Value |
|--------|-------|
| Data Collectors | 41+ |
| Lines of Code (estimated) | 50,000+ |
| PostgreSQL Tables | Bronze/Silver/Gold tiers |
| PowerBI Dashboards | 5 |
| Documentation Files | 20+ |
| Pending Tasks | 54 |
| Critical Tasks | 3 |

---

## Recommended Notion Pages

### Home Dashboard
- Project status overview
- Key metrics widgets
- Quick links to databases

### Components
- Database: All system components with status
- Views: By Type, By Status, By Priority

### Task Tracker
- Database: All 54 pending tasks
- Views: By Priority, By Category, Kanban
- Linked to: Components

### Data Sources
- Database: All data source specifications
- Views: By Region, By Status, By Frequency
- Linked to: Components, Tasks

### Documentation Hub
- Embedded docs or links to GitHub
- Organized by category

### Roadmap
- Timeline view of implementation phases
- Phase 1: Foundation (Current)
- Phase 2: Analyst Agent
- Phase 3: Analysis & Reporting

---

## Implementation Phases

### Phase 1: Foundation (Current)
- [x] PostgreSQL database setup
- [x] 41+ data collectors
- [x] PowerBI dashboards
- [ ] Master Scheduler implementation
- [ ] Codebase consolidation

### Phase 2: Analyst Agent
- [ ] Central orchestrator
- [ ] Tool registry
- [ ] Data validation agents
- [ ] Team orchestrators

### Phase 3: Analysis & Reporting
- [ ] Trade flow analysis
- [ ] Price analysis
- [ ] Automated HB reports
- [ ] Webinar content generation

### Phase 4: Advanced Features
- [ ] Real-time futures data
- [ ] ML-based forecasting
- [ ] Executive agent layer
- [ ] Multi-user support

---

## Notion Integration Setup

To enable direct Notion integration from Claude Code:

1. **Create Notion Integration**
   - Go to https://www.notion.so/my-integrations
   - Create new integration with read/write access
   - Copy the Internal Integration Token

2. **Configure MCP Server**
   Add to your Claude Code MCP configuration:
   ```json
   {
     "mcpServers": {
       "notion": {
         "command": "npx",
         "args": ["-y", "@modelcontextprotocol/server-notion"],
         "env": {
           "NOTION_API_KEY": "your-integration-token"
         }
       }
     }
   }
   ```

3. **Share Databases**
   - Share your Notion databases with the integration
   - Get database IDs from page URLs

4. **Update notion_manager.py**
   - Add token and database IDs to configuration
   - Test connection with sample page creation

---

## Quick Reference

### File Locations
- Main entry point: `src/main.py`
- Data collectors: `src/agents/collectors/`
- Orchestrators: `src/orchestrators/`
- Documentation: `docs/`
- PowerBI: `PowerBI/`

### Key Documentation
- Architecture: `docs/ARCHITECTURE_PLAN.md`
- Tasks: `docs/INCOMPLETE_TASKS_SUMMARY.md`
- Data sources: `docs/DATA_SOURCE_REGISTRY.md`
- HB Reports: `docs/HB_REPORT_AUTOMATION_GUIDE.md`
