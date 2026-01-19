# RLC-Agent System Architecture Plan

## Executive Summary

This document outlines a comprehensive multi-agent architecture for automating commodity market data gathering, cleaning, parsing, analysis, storage, and report generation. The system is designed as a hierarchical agent structure with specialized agents managed by higher-level orchestrating agents.

---

## Current State Analysis

### What Exists

| Component | Status | Lines of Code | Notes |
|-----------|--------|---------------|-------|
| USDA AMS Collector | **Working** (bug fixed) | 1,073 | Async data collection with price parsing |
| Database Agent | **Ready** | 648 | SQLite/MySQL/PostgreSQL support |
| Verification Agent | **Ready** | 526 | Data quality validation |
| Pipeline Orchestrator | **Ready** | 563 | ETL workflow coordination |
| HB Weekly Report Writer | **Needs Integration** | 3,508 | Multi-agent report generation |
| South America Trade Data | **Needs Integration** | 5,196 | 5 country agents |
| Export Inspections Agent | **Needs Integration** | 3,169 | FGIS data collection |
| RLC Master Agent | **Prototype** | 5,932 | Needs redesign for new architecture |

### Key Issues to Address

1. **Code Duplication**: `usda_ams_collector_asynch.py` exists in 2 locations
2. **Disconnected Pipelines**: Each data source operates independently
3. **No Central Scheduler**: Manual invocation of each pipeline
4. **Missing Database Agent Entry Point**: Currently a library, not a standalone service
5. **No Analysis Layer**: Data collection exists but no automated analysis

---

## Proposed Architecture

### Hierarchical Agent Structure

```
                    ┌─────────────────────────────────────────┐
                    │         FUTURE: Executive Agent          │
                    │  (Business Strategy, High-level Goals)   │
                    └────────────────────┬────────────────────┘
                                         │
                    ┌────────────────────▼────────────────────┐
                    │            ANALYST AGENT                 │
                    │  - Coordinates all data operations       │
                    │  - Schedules data pulls                  │
                    │  - Triggers analysis after new data      │
                    │  - Synthesizes reports/presentations     │
                    │  - Manages quality control               │
                    └────────────────────┬────────────────────┘
                                         │
         ┌──────────────┬────────────────┼─────────────────┬─────────────────┐
         │              │                │                 │                 │
         ▼              ▼                ▼                 ▼                 ▼
    ┌─────────┐   ┌─────────┐     ┌───────────┐    ┌───────────┐    ┌───────────┐
    │  DATA   │   │DATABASE │     │ ANALYSIS  │    │ REPORTING │    │ QUALITY   │
    │COLLECTION│   │ TEAM   │     │   TEAM    │    │   TEAM    │    │ ASSURANCE │
    │  TEAM   │   │         │     │           │    │           │    │   TEAM    │
    └────┬────┘   └────┬────┘     └─────┬─────┘    └─────┬─────┘    └─────┬─────┘
         │             │                │                │                │
    ┌────┴────┐   ┌────┴────┐     ┌─────┴─────┐   ┌──────┴──────┐   ┌─────┴─────┐
    │ USDA    │   │ Storage │     │ Trade     │   │ Report      │   │ Data      │
    │ Agent   │   │ Agent   │     │ Flow      │   │ Writer      │   │ Validator │
    ├─────────┤   ├─────────┤     │ Analyzer  │   │ Agent       │   │ Agent     │
    │ Trade   │   │ Checker │     ├───────────┤   ├─────────────┤   ├───────────┤
    │ Data    │   │ Agent   │     │ Price     │   │ Presentation│   │ Anomaly   │
    │ Agents  │   └─────────┘     │ Forecaster│   │ Builder     │   │ Detector  │
    ├─────────┤                   ├───────────┤   ├─────────────┤   └───────────┘
    │ Export  │                   │ Seasonal  │   │ Webinar     │
    │ Inspect │                   │ Analyst   │   │ Generator   │
    └─────────┘                   └───────────┘   └─────────────┘
```

---

## Phase 1: Foundation (Immediate)

### 1.1 Consolidate Codebase

**Action**: Remove duplication and establish single source of truth

```
/home/user/RLC-Agent/
├── src/                          # All source code
│   ├── agents/                   # All agent implementations
│   │   ├── base_agent.py         # Abstract base class
│   │   ├── collector_agents/     # Data collection agents
│   │   │   ├── usda_collector.py
│   │   │   ├── trade_data_agents/
│   │   │   └── export_inspections.py
│   │   ├── database_agent.py     # Storage operations
│   │   ├── verification_agent.py # Quality checks
│   │   └── analysis_agents/      # Future analysis agents
│   ├── core/                     # Core infrastructure
│   │   ├── scheduler.py          # Master scheduler
│   │   ├── message_bus.py        # Inter-agent communication
│   │   └── config.py             # Centralized configuration
│   ├── tools/                    # Callable tools for agents
│   │   ├── api_tools.py
│   │   ├── database_tools.py
│   │   ├── analysis_tools.py
│   │   └── reporting_tools.py
│   └── orchestrators/            # Agent coordinators
│       ├── analyst_agent.py      # Main orchestrator
│       ├── data_team_orchestrator.py
│       └── analysis_team_orchestrator.py
├── data/                         # Data storage
├── config/                       # Configuration files
├── logs/                         # Log files
└── tests/                        # Test suites
```

### 1.2 How to Start the Database Agent

**Answer to your question**: The Database Agent is currently designed as a **library component**, not a standalone service. It's used by the pipeline orchestrator.

**To run data collection with database storage:**

```bash
# Navigate to the USDA AMS agent directory
cd /home/user/RLC-Agent/commodity_pipeline/usda_ams_agent

# Run daily data collection (includes database storage)
python main.py daily

# Run for a specific date
python main.py daily --date 12/04/2025

# Check database status
python main.py status

# Verify data integrity
python main.py verify

# Test connections
python main.py test
```

**The database is automatically:**
- Created at `./data/rlc_commodities.db` (SQLite)
- Schema initialized on first run
- Records inserted with deduplication
- Quality metrics logged

---

## Phase 2: Analyst Agent Implementation

### 2.1 Analyst Agent Design

The Analyst Agent is the **central orchestrator** that coordinates all lower-level agents.

```python
# src/orchestrators/analyst_agent.py

class AnalystAgent:
    """
    Master orchestrator for commodity market data operations.
    Coordinates scheduling, data collection, analysis, and reporting.
    """

    def __init__(self):
        self.scheduler = MasterScheduler()
        self.data_team = DataTeamOrchestrator()
        self.database_team = DatabaseTeamOrchestrator()
        self.analysis_team = AnalysisTeamOrchestrator()
        self.reporting_team = ReportingTeamOrchestrator()
        self.qa_team = QualityAssuranceOrchestrator()

        # Tool registry - all tools available to the analyst
        self.tools = ToolRegistry()

    async def run_daily_workflow(self):
        """Execute the complete daily data workflow"""
        # 1. Check what data sources have new publications
        schedule = await self.scheduler.get_todays_schedule()

        # 2. Collect data from scheduled sources
        collection_results = await self.data_team.collect_scheduled(schedule)

        # 3. Validate and store in database
        storage_results = await self.database_team.store_with_validation(
            collection_results
        )

        # 4. Run QA checks
        qa_results = await self.qa_team.verify_new_data(storage_results)

        # 5. Trigger analysis on new data
        if qa_results.passed:
            analysis_results = await self.analysis_team.analyze_new_data(
                storage_results.new_records
            )

            # 6. Generate reports if significant findings
            if analysis_results.has_actionable_insights:
                await self.reporting_team.create_alert_report(analysis_results)

        return DailyWorkflowResult(
            collection=collection_results,
            storage=storage_results,
            qa=qa_results,
            analysis=analysis_results
        )

    async def handle_request(self, request: AnalystRequest):
        """Handle ad-hoc requests from user or higher-level agents"""
        match request.type:
            case RequestType.DATA_QUERY:
                return await self._handle_data_query(request)
            case RequestType.RUN_ANALYSIS:
                return await self.analysis_team.run_analysis(request.params)
            case RequestType.GENERATE_REPORT:
                return await self.reporting_team.generate(request.params)
            case RequestType.CHECK_DATA_QUALITY:
                return await self.qa_team.run_full_check()
            case _:
                return await self._route_to_appropriate_team(request)
```

### 2.2 Master Scheduler

```python
# src/core/scheduler.py

class MasterScheduler:
    """
    Coordinates timing of all data collection to align with publication schedules.
    """

    # Data source publication schedules
    SCHEDULES = {
        'usda_ams_daily': {
            'frequency': 'daily',
            'time': '06:00',  # 6 AM CT
            'timezone': 'America/Chicago',
            'sources': ['daily_grain_bids', 'daily_ethanol', 'daily_livestock']
        },
        'usda_ams_weekly': {
            'frequency': 'weekly',
            'day': 'thursday',
            'time': '15:00',
            'sources': ['weekly_ethanol', 'weekly_grain_coproducts']
        },
        'fgis_export_inspections': {
            'frequency': 'weekly',
            'day': 'friday',
            'time': '11:00',
            'timezone': 'America/New_York'
        },
        'south_america_trade': {
            'frequency': 'monthly',
            'day': 15,  # Around 15th of each month
            'sources': ['argentina_indec', 'brazil_comex', 'uruguay_dna']
        },
        'hb_weekly_report': {
            'frequency': 'weekly',
            'day': 'tuesday',
            'time': '09:00',
            'type': 'output',  # This is a report we generate
        }
    }

    async def get_todays_schedule(self) -> List[ScheduledTask]:
        """Return list of data collection tasks for today"""
        ...

    async def run_scheduler_loop(self):
        """Main scheduler loop - runs continuously"""
        while True:
            now = datetime.now(pytz.UTC)
            tasks = await self.get_pending_tasks(now)

            for task in tasks:
                try:
                    await self.execute_task(task)
                    await self.mark_completed(task)
                except Exception as e:
                    await self.handle_task_failure(task, e)

            await asyncio.sleep(60)  # Check every minute
```

### 2.3 Tool Registry

Agents don't perform all actions directly - they call **tools** that encapsulate specific capabilities:

```python
# src/tools/tool_registry.py

class ToolRegistry:
    """
    Registry of all tools available to agents.
    Tools are modular, reusable capabilities.
    """

    def __init__(self):
        self.tools = {}
        self._register_default_tools()

    def _register_default_tools(self):
        # Data collection tools
        self.register('fetch_usda_data', USDAFetchTool())
        self.register('fetch_trade_data', TradeDataFetchTool())
        self.register('fetch_export_inspections', ExportInspectionsTool())

        # Database tools
        self.register('query_database', DatabaseQueryTool())
        self.register('insert_records', DatabaseInsertTool())
        self.register('validate_data', DataValidationTool())

        # Analysis tools
        self.register('calculate_trade_flows', TradeFlowAnalysisTool())
        self.register('price_analysis', PriceAnalysisTool())
        self.register('seasonal_analysis', SeasonalAnalysisTool())
        self.register('anomaly_detection', AnomalyDetectionTool())

        # Reporting tools
        self.register('generate_chart', ChartGeneratorTool())
        self.register('create_presentation', PresentationBuilderTool())
        self.register('send_notification', NotificationTool())

    async def execute(self, tool_name: str, params: Dict) -> ToolResult:
        """Execute a tool with given parameters"""
        if tool_name not in self.tools:
            raise ToolNotFoundError(f"Tool '{tool_name}' not registered")

        tool = self.tools[tool_name]
        return await tool.execute(params)
```

---

## Phase 3: Team Orchestrators

### 3.1 Data Collection Team

```python
# src/orchestrators/data_team_orchestrator.py

class DataTeamOrchestrator:
    """
    Coordinates all data collection agents.
    """

    def __init__(self):
        self.usda_agent = USDACollectorAgent()
        self.trade_agents = {
            'argentina': ArgentinaTradeAgent(),
            'brazil': BrazilTradeAgent(),
            'colombia': ColombiaTradeAgent(),
            'paraguay': ParaguayTradeAgent(),
            'uruguay': UruguayTradeAgent()
        }
        self.export_agent = ExportInspectionsAgent()

    async def collect_scheduled(self, schedule: List[ScheduledTask]) -> CollectionResults:
        """Collect data from all scheduled sources in parallel"""
        tasks = []

        for item in schedule:
            if item.source_type == 'usda':
                tasks.append(self.usda_agent.collect(item.reports))
            elif item.source_type == 'trade':
                tasks.append(self._collect_trade_data(item))
            elif item.source_type == 'fgis':
                tasks.append(self.export_agent.collect())

        results = await asyncio.gather(*tasks, return_exceptions=True)
        return self._aggregate_results(results)

    async def collect_specific(self, source: str, params: Dict) -> CollectionResult:
        """Collect from a specific source on demand"""
        ...
```

### 3.2 Database Team

```python
# src/orchestrators/database_team_orchestrator.py

class DatabaseTeamOrchestrator:
    """
    Coordinates database operations and data integrity.
    """

    def __init__(self):
        self.storage_agent = StorageAgent()
        self.checker_agent = DataCheckerAgent()

    async def store_with_validation(self, data: CollectionResults) -> StorageResults:
        """Store data with pre and post validation"""

        # Pre-validation
        pre_check = await self.checker_agent.validate_incoming(data)
        if not pre_check.valid:
            return StorageResults(failed=True, reason=pre_check.errors)

        # Store data
        insert_results = await self.storage_agent.insert_batch(data.records)

        # Post-validation: verify stored data matches source
        post_check = await self.checker_agent.verify_storage(
            source_records=data.records,
            stored_count=insert_results.inserted
        )

        return StorageResults(
            inserted=insert_results.inserted,
            skipped=insert_results.skipped,
            validation=post_check
        )

    async def compare_with_source(self, source: str, date_range: tuple) -> ComparisonResult:
        """Compare database records against source data"""
        db_records = await self.storage_agent.query(source, date_range)
        source_records = await self._fetch_source_for_comparison(source, date_range)

        return self.checker_agent.compare(db_records, source_records)
```

---

## Phase 4: Integration Points

### 4.1 Starting the Complete System

Create a unified entry point:

```python
# main.py

import asyncio
from src.orchestrators.analyst_agent import AnalystAgent
from src.core.scheduler import MasterScheduler

async def main():
    """Main entry point for the RLC Agent System"""

    # Initialize the Analyst Agent
    analyst = AnalystAgent()

    # Start the scheduler in background
    scheduler_task = asyncio.create_task(analyst.scheduler.run_scheduler_loop())

    # Run the interactive CLI or API server
    await run_interface(analyst)

if __name__ == "__main__":
    asyncio.run(main())
```

### 4.2 CLI Interface

```bash
# Start the full system with scheduler
python main.py start

# Run specific commands
python main.py collect usda --date today
python main.py collect trade-data --country brazil --month 2025-12
python main.py analyze trade-flows --commodity corn
python main.py report weekly
python main.py status
python main.py db status
python main.py db verify
```

---

## Phase 5: Analysis & Reporting Teams

### 5.1 Analysis Team

```python
# src/orchestrators/analysis_team_orchestrator.py

class AnalysisTeamOrchestrator:
    """
    Coordinates analysis agents for market insights.
    """

    def __init__(self):
        self.trade_flow_analyzer = TradeFlowAnalyzer()
        self.price_analyzer = PriceAnalyzer()
        self.seasonal_analyzer = SeasonalAnalyzer()

    async def analyze_new_data(self, new_records: List[Dict]) -> AnalysisResults:
        """Run relevant analyses on newly arrived data"""

        # Determine what analyses to run based on data type
        analyses = self._determine_analyses(new_records)

        results = []
        for analysis_type in analyses:
            result = await self._run_analysis(analysis_type, new_records)
            results.append(result)

        return AnalysisResults(results)

    async def run_trade_flow_analysis(self, params: Dict) -> TradeFlowReport:
        """Comprehensive trade flow analysis"""
        return await self.trade_flow_analyzer.analyze(params)
```

### 5.2 Reporting Team

```python
# src/orchestrators/reporting_team_orchestrator.py

class ReportingTeamOrchestrator:
    """
    Coordinates report generation agents.
    """

    def __init__(self):
        self.report_writer = ReportWriterAgent()
        self.presentation_builder = PresentationBuilderAgent()
        self.webinar_generator = WebinarGeneratorAgent()

    async def create_weekly_report(self) -> Report:
        """Generate the weekly HB report"""

        # Gather all required data
        market_data = await self._gather_market_data()
        price_data = await self._gather_price_data()
        trade_data = await self._gather_trade_data()

        # Generate the report
        report = await self.report_writer.generate(
            template='hb_weekly',
            data={
                'market': market_data,
                'prices': price_data,
                'trade': trade_data
            }
        )

        return report

    async def create_presentation(self, topic: str, data: Dict) -> Presentation:
        """Create a presentation on a specific topic"""
        return await self.presentation_builder.build(topic, data)
```

---

## Data Flow Diagram

```
                              ┌──────────────────────────────────────────┐
                              │              SCHEDULER                    │
                              │  (Triggers based on publication times)    │
                              └───────────────────┬──────────────────────┘
                                                  │
                         ╔════════════════════════▼═════════════════════════╗
                         ║                 ANALYST AGENT                     ║
                         ║            (Central Orchestrator)                 ║
                         ╚═══════════════════════╦══════════════════════════╝
                                                 ║
           ┌─────────────────────────────────────╬─────────────────────────────────────┐
           │                                     ║                                     │
           ▼                                     ▼                                     ▼
    ┌─────────────┐                      ┌─────────────┐                      ┌─────────────┐
    │  DATA TEAM  │                      │DATABASE TEAM│                      │ANALYSIS TEAM│
    └──────┬──────┘                      └──────┬──────┘                      └──────┬──────┘
           │                                    │                                    │
    ┌──────▼──────┐                      ┌──────▼──────┐                      ┌──────▼──────┐
    │ USDA Agent  │──┐                   │Storage Agent│                      │Trade Flow   │
    ├─────────────┤  │                   ├─────────────┤                      │ Analyzer    │
    │ Trade Agents│  │                   │Checker Agent│                      ├─────────────┤
    ├─────────────┤  │                   └──────┬──────┘                      │Price Analyst│
    │ Export Agent│  │                          │                             ├─────────────┤
    └─────────────┘  │                          ▼                             │Seasonal     │
                     │                   ┌─────────────┐                      │ Analyzer    │
                     └──────────────────▶│  DATABASE   │◀─────────────────────┘
                        (Write)          │ (SQLite/    │      (Read)
                                         │ MySQL/PG)   │
                                         └─────────────┘
                                                │
                                                │ (Query for Reports)
                                                ▼
                                        ┌─────────────┐
                                        │REPORTING    │
                                        │   TEAM      │
                                        └──────┬──────┘
                                               │
                              ┌────────────────┼────────────────┐
                              ▼                ▼                ▼
                       ┌───────────┐    ┌───────────┐    ┌───────────┐
                       │Weekly     │    │Custom     │    │Webinar    │
                       │Reports    │    │Analyses   │    │Content    │
                       └───────────┘    └───────────┘    └───────────┘
```

---

## Implementation Priorities

### Immediate (This Week)

1. **Test the price parsing fix** - Run the collector and verify CSV output now includes prices
2. **Consolidate duplicate code** - Remove `/api Manager/` duplication
3. **Create unified entry point** - Single `main.py` for all operations

### Short Term (Next 2 Sprints)

1. **Implement Master Scheduler** - Enable automated data pulls
2. **Create Analyst Agent skeleton** - Basic orchestration structure
3. **Integrate all data sources** - Connect existing pipelines
4. **Implement Data Checker Agent** - Source vs. database comparison

### Medium Term (Next Quarter)

1. **Build Analysis Tools** - Trade flow, price analysis, seasonal patterns
2. **Implement Reporting Team** - Automated report generation
3. **Add monitoring/alerting** - System health and data quality alerts
4. **API layer** - REST API for external access

### Long Term (Next 6 Months)

1. **Machine learning models** - Price forecasting, anomaly detection
2. **Executive Agent layer** - Strategic decision support
3. **Multi-user support** - Role-based access control
4. **Cloud deployment** - Scalable infrastructure

---

## Technology Recommendations

| Component | Recommended Technology | Rationale |
|-----------|----------------------|-----------|
| **Agent Framework** | Python + asyncio | Already in use, good async support |
| **Scheduler** | APScheduler or custom | Simple, embeddable, persistent |
| **Database** | PostgreSQL (production) | Robust, scalable, good for time series |
| **Message Queue** | Redis (optional) | If agents need to communicate async |
| **API** | FastAPI | Modern, async, auto-documentation |
| **Monitoring** | Prometheus + Grafana | Industry standard, open source |
| **CI/CD** | GitHub Actions | Already using GitHub |
| **Container** | Docker | Portable, consistent environments |

---

## Testing the Price Fix

Run this to verify the fix works:

```bash
cd "/home/user/RLC-Agent/api Manager"

# Run the collector for today's date
python usda_ams_collector_asynch.py --date 12/04/2025

# Check the output CSV file for price columns
cat usda_data/20251204/Daily_Ethanol_Report_20251204.csv

# You should now see columns like:
# commodity, location, price, price_low, price_high, price_avg, avg_price, basis_min, basis_max, etc.
```

---

## Summary

The architecture is designed around these principles:

1. **Hierarchical Authority**: Analyst Agent orchestrates, specialized agents execute
2. **Modular Tools**: Agents call tools, tools are reusable across agents
3. **Separation of Concerns**: Data collection, storage, analysis, reporting are separate teams
4. **Automated Scheduling**: Data pulls happen automatically at optimal times
5. **Quality Assurance**: Every data flow includes validation and verification
6. **Extensibility**: Easy to add new data sources, analyses, or report types

The system grows from what you have (working data collectors and database) to a fully automated commodity market analysis platform.
