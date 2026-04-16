# RLC-Agent System Capabilities Inventory

*Generated 2026-04-15. Two-tier inventory — high-level categories + exhaustive enumeration under each. Baseline for upcoming reorganization; nothing should be forgotten or orphaned in the move.*

---

## TIER 1 — HIGH-LEVEL CAPABILITY CATEGORIES

### 1. Data Collection & Ingestion
Multi-source agricultural commodity data collection via 74+ specialized collectors organized by geography and commodity type, with medallion architecture (Bronze/Silver/Gold) storage.

### 2. Databases & Data Layer
PostgreSQL-based medallion architecture with 150+ tables across Bronze (raw ingestion), Silver (cleaned/normalized), Gold (analytics views), Core (operational), and Public (reference) schemas.

### 3. Orchestration & Scheduling
Three-tier scheduler infrastructure: master schedules (APScheduler), dispatcher daemon (real-time execution), and report schedulers with event-log-based CNS briefing system.

### 4. Analysis & Modeling
Fundamental analysis engines (yield forecasting, margin models, pricing), balance sheet builders, positioning analysis, and commodity-specific crush/feedstock allocation models.

### 5. MCP / LLM Integration
Single MCP server providing 30+ Claude tools for database queries, briefing/event management, knowledge graph access, forecast tracking, and report generation.

### 6. Knowledge Graph
Analyst-authored node/edge/context system (224 nodes, 121 contexts documented) tracking commodity insights, market dynamics, relationships, and enrichment data.

### 7. Dashboards & UI
Streamlit applications for ops monitoring (health, collectors, event log) and data exploration (series registry, balance sheets, crop conditions, pricing).

### 8. Report & Content Generation
Graphics engine (matplotlib/plotly), report writers (WASDE template, crop progress, biofuel summaries), and publishing pipeline (email/Notion channels, DOCX/HTML formatters).

### 9. External Integrations
APIs to USDA (FAS, NASS, AMS, ERS), EIA, EPA (RFS, ECHO), CFTC, Census Bureau, CME, Yahoo Finance, weather services (GFS, NDVI), and Brazil state agencies (CONAB, IMEA, ABIOVE).

### 10. VBA/Excel Tools
71 model workbooks in `Models/` organized by commodity group (Biofuels, Oilseeds, Feed Grains, Food Grains, Fats/Greases), with feedstock allocation, crush margin, and RFS mandate tracking.

### 11. Operations Infrastructure
Event logging (`core.event_log`), collection status tracking, heartbeat monitoring, dispatcher watchdog, health checks, and data freshness metrics.

### 12. Domain Knowledge Repository
Centralized reference: data dictionaries, crop maps (38 countries, 201 maps), crop calendars, balance sheet templates, sample reports, special situations (drought, trade war, wars).

### 13. Briefing & Pace Tracking Tools
MCP functions for unacknowledged events (`get_briefing`), data freshness queries (`get_data_freshness`), PACE tracking (seasonal norms, historical pace), and knowledge graph enrichment.

### 14. User Interfaces & Tools
CLI dispatcher, dashboard apps, MCP chat integration, database query tools, balance sheet extractors, and log review agents.

### 15. Pipeline & Provenance
Event logs, collection_status tracking, forecast feedback loops, training iterations, and confidence/accuracy metrics tied to source and methodology.

### 16. File-Based Artifacts
Output directory housing report PDFs, balance sheet templates, visualizations (PNG/SVG), flat files (.csv), and processed data exports for PowerBI/external tools.

---

## TIER 2 — EXHAUSTIVE CAPABILITY LIST

### 1. Data Collectors (74+ across src/agents/collectors/)

#### US Commodity Sources
- **CFTC Commitments of Traders** (`cftc_cot_collector.py`) → `bronze.cftc_cot`, `silver.cftc_position_history`
- **USDA NASS Crop Progress** → `bronze.nass_crop_progress` (weekly by state)
- **USDA NASS Conditions** → `bronze.nass_crop_condition` (weekly G/E/F/P/VP ratings)
- **USDA NASS Production** → `bronze.usda_nass`
- **USDA AMS Cash Prices** (`usda_ams_cash_prices`) → `bronze.usda_ams_cash_prices` (daily)
- **USDA ERS Oil Crops / Wheat / Feed Grains** → `bronze.usda_ers_data`
- **USDA FAS PSD Balance Sheets** → `bronze.fas_psd` (global S&D, 22 commodities, 175+ countries, 1990-2025 after backfill)
- **USDA FAS Export Sales** → `bronze.fas_export_sales` (1.19M rows, 1999+)
- **US Census Trade** → `bronze.census_trade` (470K rows, 14 HS codes)
- **EPA RFS RIN** → `bronze.epa_rfs_rin_generation`, `bronze.epa_rfs_rin_transaction` (disabled, manual download)
- **EPA ECHO Facilities** — 4 profiles: ethanol, biodiesel, milling, oilseed
- **EIA Ethanol** → `bronze.eia_ethanol`
- **EIA Petroleum** → `bronze.eia_petroleum`
- **CME Settlements** → `bronze.cme_settlements`
- **Yahoo Finance Futures** (`yfinance_futures`) — daily 5:15 PM ET
- **FGIS Grain Inspections** → `bronze.fgis_inspections` (534K rows, 1990-2026)
- **Drought Monitor** → `bronze.drought_conditions`
- **⚠ ORPHAN: NDVI Satellite** (`ndvi_collector.py`) — code built, never registered
- **⚠ ORPHAN: GFS Weather Forecast** (`gfs_forecast_collector.py`) — code built, never registered
- **⚠ ORPHAN: GEFS Ensemble** (`gefs_ensemble_collector.py`) — code built, never registered

#### Brazil Sources
- **CONAB** → `bronze.conab_production` (7,255 records), `bronze.conab_supply_demand`, `bronze.conab_prices`
- **IMEA Mato Grosso** → `bronze.imea_mato_grosso`
- **ABIOVE** — Soybean association
- **ANEC** — Ethanol
- **IBGE SIDRA** → `bronze.ibge_sidra` (table 1612 + c81 classification)
- **ComexStat** → `bronze.comexstat_trade`

#### Argentina/Other LatAm
- **MAGYP Argentina** → `bronze.magyp_argentina`
- **INDEC Argentina** — Monthly ZIP, Latin-1 CSV `;`-delimited
- **Paraguay Agent** — Soy focus
- **Colombia Agent** — Coffee focus

#### Canada
- **Canada CGC** → `bronze.canada_cgc_weekly`, `bronze.canada_cgc_exports` (138K rows, CSV-based)
- **Canada StatsCan** → `bronze.canada_statscan` (57K rows, CSV bulk)

#### Global
- **FAOSTAT** (currently returning 521 Cloudflare errors — FAO infrastructure issue)
- **MPOB Malaysia/Indonesia** → palm oil data
- **Wheat Tenders** → `bronze.wheat_tenders`

#### Weather System (`rlc_scheduler/agents/`)
- **Weather Collector Agent** → OpenWeather + Open-Meteo → `bronze.weather_raw` + `silver.weather_observation` (just restored 2026-04-15 after 35-day outage)
- **Weather Intelligence Agent** → Gmail meteorologist emails → `bronze.weather_daily_brief` + `bronze.weather_email_extract`
- **Weather Email Classifier / Extractor / Synthesizer / Research** — support modules
- **Meteorologist senders:** 4 addresses from World Weather Inc (bizkc.rr.com domain)

---

### 2. Databases & Schemas (150+ tables)

#### Bronze Layer — raw ingestion (31+ tables)
FAS PSD, FAS export sales, NASS family, CONAB family, Census trade, CFTC COT, EIA ethanol/petroleum, EPA RFS/EMTS, weather family, AMS cash prices, ERS data, CME settlements, NDVI, wheat tenders, drought conditions, IBGE/ComexStat, MAGYP/INDEC, Canada CGC/StatsCan, FGIS, MPOB, FAOSTAT.

#### Silver Layer — cleaned/standardized (26+ tables)
- `silver.monthly_realized` — NASS monthly actuals (fats/oils, grain crush, flour, peanut)
- `silver.user_sd_estimate` — user projections
- `silver.crop_progress` / `silver.crop_condition` / `silver.nass_latest_progress` / `silver.nass_crop_condition_ge`
- `silver.conab_production` / `silver.conab_balance_sheet`
- `silver.census_trade_monthly`
- `silver.cftc_position_history`
- `silver.ethanol_weekly`
- `silver.weather_observation` (152K rows), `silver.weather_alert`
- **⚠ EMPTY: silver.weather_forecast_daily** — full ensemble schema but zero rows
- **⚠ EMPTY: silver.weather_forecast_period** — same
- `silver.cash_price` / `silver.futures_price`
- `silver.fuel_production_forecast`

#### Gold Layer — analytics views (53 views)
US balance sheets (corn/soy/wheat/meal/oil), Brazil (soy/corn/national/by-state), crop conditions (current vs 5yr avg), CFTC (sentiment/MM extremes/per-commodity positioning), energy (ethanol weekly/petroleum/prices), biofuels (RIN monthly/D4/D6/generation summary), weather (latest/summary/regional/7day outlook/forecast summary/alerts), forecast tracking (actual pairs/accuracy latest/vs actual), futures validated, **yield_forecast**.

#### Core Layer — operational
`kg_node`, `kg_edge`, `kg_context`, `kg_source`, `kg_provenance`, `kg_processing_batch`, `kg_confidence_ranking`, `kg_source_coverage`, `event_log`, `collection_status`, `latest_collections`, `training_runs`, `forecasts`, `forecast_actual_pairs`, `forecast_feedback`.

#### Reference Layer (public)
`weather_location` (28 active), `weather_location_alias`, `reference.weather_climatology`.

---

### 3. Schedulers & Orchestration

#### Three Parallel Schedulers (THIS IS A REORG TARGET)
1. **`src/schedulers/master_scheduler.py`** (current primary) — 37 declarative RELEASE_SCHEDULES with `ReleaseSchedule` dataclasses, holiday-aware USDA release calendar
2. **`rlc_scheduler/agent_scheduler.py`** (legacy but alive) — uses `schedule` library, runs weather agents via `RLC_AgentScheduler` Windows task (restored 2026-04-15)
3. **`src/scheduler/agents/`** (legacy mirror, orphan) — duplicate of rlc_scheduler, never imported

#### Dispatcher (`src/dispatcher/`)
- APScheduler cron daemon under Windows task `\RLC\RLC Dispatcher` (+ watchdog)
- Reads RELEASE_SCHEDULES, registers jobs with misfire grace
- Heartbeat every 5 min to `scripts/deployment/dispatcher_heartbeat.json`
- `collector_registry.py` maps schedule keys to class paths

#### Windows Scheduled Tasks
- `\RLC\RLC Dispatcher` (Running) — modern APScheduler daemon
- `\RLC\RLC Dispatcher Watchdog` (every 15 min) — restart if stale
- `RLC_AgentScheduler` (Running as of 2026-04-15) — legacy rlc_scheduler daemon
- `RLC_DailyNotionExport` (daily 5:30 PM) — JSON export for Claude Desktop → Notion
- `RLC_USDAExportSalesAlert` (Thu 8:25 AM) — audible release alert

---

### 4. Analysis & Modeling Engines

#### Feedstock Allocation (`src/engines/feedstock_allocation/`)
`allocator.py`, `margin_model.py`, `price_resolver.py`, `volume_estimator.py`, `ingest_capacity_master.py`, `ingest_profitability_workbook.py`, `ingest_training_prices.py` / `_v2.py`, `populate_policy_timeline.py`

#### Oilseed Crush (`src/engines/oilseed_crush/`)
`engine.py`, `margin_calculator.py`, `price_resolver.py`, `volume_estimator.py`, `config.py`

#### Yield Forecasting (`src/models/`)
`yield_prediction_model.py`, `yield_feature_engine.py`, `yield_model_validator.py`, `yield_orchestrator.py`, `uco_collection_model.py`

#### Daily Ops Cycle (DOC) (`src/engines/doc/daily_ops.py`)
Post-close verification, margin re-computation, anomaly detection, event_log summary

#### Fundamental / Price / Spread (`src/agents/analysis/`)
`fundamental_analyzer.py`, `price_forecaster.py`, `spread_and_basis_analyzer.py`

---

### 5. MCP Tools (`src/mcp/commodities_db_server.py`)

**Query:** `query_database`, `list_tables`, `describe_table`, `get_commodity_summary`
**Balance Sheet / S&D:** `get_balance_sheet`, `get_production_ranking`, `get_stocks_to_use`, `analyze_supply_demand`, `get_brazil_production`
**Ops:** `get_data_freshness`, `get_collection_history`
**Briefing (CNS):** `get_briefing`, `acknowledge_events`
**Knowledge Graph:** `search_knowledge_graph`, `get_kg_context`, `get_kg_relationships`
**Forecast:** `record_forecast`, `get_forecast_accuracy`
**Reporting:** `generate_report` (WASDE, crop progress, ethanol, positioning sub-types)

---

### 6. Knowledge Graph (`src/knowledge_graph/`)

- **Store:** `core.kg_node` (225 rows), `core.kg_edge` (146), `core.kg_context` (121), `core.kg_source` (82), `core.kg_provenance` (**0 rows — broken join table**)
- **Manager:** `kg_manager.py` — search/get/upsert
- **Enrichers:** `kg_enricher.py`, `pace_calculator.py`, `seasonal_calculator.py`
- **CLI:** `kg_cli.py`
- **Known issues:** 72 of 225 nodes are fully orphan (no contexts, no edges); `kg_provenance` has zero rows; vocabulary drift on context_type and edge_type (26 / 31 distinct values, many near-duplicates)

---

### 7. Dashboards (`dashboards/`)

- **Data Dashboard** (`dashboards/data/app.py`) — series registry, balance sheets, crop conditions, pricing, biofuel margins
- **Ops Dashboard** (`dashboards/ops/app.py`) — collector health, event log, freshness, heartbeat, LLM call log

---

### 8. Report & Content Generation

- **Graphics Agent** (`src/agents/graphics_generator_agent.py`) — matplotlib/plotly
- **Report Writers** (`src/agents/reporting/`) — `report_writer_agent.py`, `market_research_agent.py`, `price_data_agent.py`, `internal_data_agent.py`
- **Templates** (`src/analysis/templates/`) — `wasde_template.py`, `base_template.py`
- **Publishers** (`src/agents/publishing/`) — `docx_formatter.py`, `html_formatter.py`, `email_channel.py`, `notion_channel.py`

---

### 9. External Integrations

Government: USDA FAS/NASS/AMS/ERS, Census FT-900, CFTC, EPA (RFS/ECHO/EMTS), EIA
International: CONAB, IMEA, ABIOVE, ANEC, IBGE SIDRA, MAGYP, INDEC, StatsCan, CGC, MPOB, FAOSTAT
Market: CME, Yahoo Finance, Tradestation (⚠ partial), IBKR (⚠ partial)
Weather: NOAA GFS/GEFS (⚠ orphan), NASA NDVI (⚠ orphan), OpenWeather, Open-Meteo, meteorologist Gmail
Productivity: Gmail OAuth, Notion API, Google Calendar/Drive (⚠ config present, usage unclear)

---

### 10. VBA/Excel Tools (71 workbooks in `Models/`)

Categories: Biofuels (19), Oilseeds (15), Feed Grains (10), Food Grains (8), Fats & Greases (12), Data/Reference (7).

**VBA updaters** (keyboard shortcuts via `Application.OnKey`):
- Ctrl+I Census trade (`TradeUpdaterSQL.bas`)
- Ctrl+B Biofuel S&D (`BiofuelDataUpdater.bas`)
- Ctrl+D EIA Feedstock (`EIAFeedstockUpdater.bas`)
- Ctrl+E EMTS/Feedstock
- Ctrl+R RIN data
- Ctrl+U **FatsOilsUpdaterSQL** (universal, replaces CrushUpdaterSQL)
- Ctrl+G FGIS Inspections (TODO)

ODBC via psqlODBC x64, `sslmode=require` for RDS.

---

### 11. Ops Infrastructure

- `core.event_log` — system-wide event stream
- `core.collection_status` — latest run per collector
- `scripts/deployment/dispatcher_watchdog.py` — heartbeat monitor, auto-restart
- `core.accuracy_metrics` — forecast/model accuracy
- Anomaly detection in DOC
- Python logging → `logs/` directory

---

### 12. Domain Knowledge (`domain_knowledge/`)

- **Data dictionaries** (9 files) — FAS PSD, EIA series, EPA RFS, HS codes, Census trade, ag econ models, weather forecast reference, marketing year bible
- **Crop maps** (201 maps, 38 countries) — `crop_map_inventory.json` master registry
- **Crop calendars** — 12 monthly GIFs
- **Balance sheet templates** — biofuels, fats/greases, feed grains, food grains, oilseeds
- **Sample reports** — HB weekly, case studies
- **Special situations** — 2012 drought, 2018 trade war, 2020 China demand surge, 2020 derecho, 2022 Ukraine war

---

### 13. Briefing & Pace Tracking

- **Briefing (CNS):** `get_briefing`, `acknowledge_events` — LLM inbox over event_log
- **Pace:** `pace_calculator.py` — soy crush / corn grind vs USDA annual projection
- **Seasonal norms:** `seasonal_calculator.py` — CFTC monthly percentiles, crop condition weekly p10-p90
- **Computed contexts in KG:** 9 seasonal norms + 6 pace tracking records

---

### 14. User Interfaces

- **CLI dispatcher** (`src/dispatcher/cli.py`)
- **db_query.py** (`src/tools/db_query.py`) — SQL CLI (ACTIVE — agent that scanned this reported it missing but it's in use)
- **Balance sheet extractor** (`scripts/deployment/balance_sheet_extractor.py`)
- **Log review agent** (`src/agents/log_review_agent.py`)
- **Forecast tracker** (`scripts/deployment/forecast_tracker.py`)
- **PowerBI export** (`scripts/deployment/powerbi_export.py`)

---

### 15. Pipeline & Provenance

- `core.event_log` — full audit trail
- `core.collection_status` — SLA monitoring
- `core.forecasts` / `core.forecast_actual_pairs` / `core.forecast_feedback` — forecast tracking
- `core.training_runs` / `training_iterations` / `training_feedback` — model iteration logs
- `core.kg_source` / `core.kg_provenance` — KG sourcing (provenance empty)

---

### 16. File-Based Artifacts

- `output/reports/`, `output/balance_sheet_templates/`, `output/visualizations/`
- `Models/` — 71 workbooks
- `report_samples/` — templates, exports
- `exports/` — PowerBI feeds

---

## ⚠ KNOWN ORPHANS, GAPS, AND DRIFT

### Code built but not connected
1. `src/agents/collectors/global/gfs_forecast_collector.py` — GFS forecast, full DB wiring, never registered
2. `src/agents/collectors/global/gefs_ensemble_collector.py` — Ensemble p10/p50/p90
3. `src/agents/collectors/global/ndvi_collector.py` — NDVI charts + time series
4. `src/scheduler/agents/` — duplicate weather agents, never imported (archive candidate)
5. `rlc_scheduler/agents/_archived/weather_email_agent.py` — superseded by intelligence_agent

### Built but broken until 2026-04-15
- Weather observations (`silver.weather_observation`) — 35-day outage, restored
- Both `RLC_AgentScheduler` and `RLC_DailyNotionExport` Windows tasks — path drift from three successive repo relocations

### Schema exists, no data
- `silver.weather_forecast_daily` (ensemble schema with GDD, frost, p10/p50/p90 percentiles)
- `bronze.weather_forecast_run`
- `bronze.weather_alerts_raw`
- `bronze.ndvi_data` (schema not even created)

### Multi-scheduler confusion
Three parallel schedulers. Decision needed during reorg: consolidate into one, or define clear ownership (modern for new collectors, legacy for weather).

### CLAUDE.md drift
Some tables/tools referenced in CLAUDE.md don't exist or have different names. Node/edge/context counts in CLAUDE.md (224/143/121) don't match live (225/146/121) — minor drift.

---

## FINAL COUNT

- Collectors: 74+ classes
- Bronze tables: 31+
- Silver tables: 26+
- Gold views: 53
- MCP tools: 30+
- KG: 225 nodes / 146 edges / 121 contexts / 82 sources
- Dashboards: 2 (data, ops)
- Excel workbooks: 71
- Domain knowledge files: 200+ crop maps, 9 dictionaries, 12 calendars
- Windows scheduled tasks: 5
- Python: ~50,000 LOC estimate

---

*End of inventory. This document is the baseline for the upcoming file-structure reorganization.*
