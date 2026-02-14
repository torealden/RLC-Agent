# RLC-Agent Inventory
**Generated:** 2026-02-04
**Total Components:** 208+ (94% complete)
**Database:** PostgreSQL (rlc_commodities) - Medallion Architecture

---

## Quick Stats

| Category | Count | Status |
|----------|-------|--------|
| Core Agents | 6 | Complete |
| Orchestrators | 4 | 3 Complete, 1 In-Progress |
| US Collectors | 21 | 18 Complete, 3 Partial |
| South America Collectors | 13 | Complete |
| Global/Market Collectors | 10 | 8 Complete, 2 Partial |
| Analysis Agents | 3 | Complete |
| Reporting Agents | 5 | Complete |
| Integration Agents | 5 | 4 Complete, 1 Partial |
| Specialized (BioTrack) | 1 | Complete (simulation mode) |
| **PLANNED: Rail Car Project** | 4 | Placeholder |
| **PLANNED: VaR Analysis** | 3 | Placeholder |

---

## PART 1: CORE INFRASTRUCTURE

### 1.1 Master Agent System

| Agent | Location | Status | Description |
|-------|----------|--------|-------------|
| **RLC Master Agent** | `src/agents/core/master_agent.py` | COMPLETE | Central orchestrator |
| **Approval Manager** | `src/agents/core/approval_manager.py` | COMPLETE | Autonomy control |
| **Memory Manager** | `src/agents/core/memory_manager.py` | COMPLETE | Context persistence |
| **Verification Agent** | `src/agents/core/verification_agent.py` | COMPLETE | Data quality |
| **Data Agent** | `src/agents/core/data_agent.py` | COMPLETE | Data access layer |
| **Transformation Logger** | `src/agents/core/transformation_logger.py` | COMPLETE | ETL tracking |

**Inputs:** User queries, API triggers, scheduled events
**Outputs:** Task execution results, approval decisions, audit logs
**Dependencies:** PostgreSQL, LLM provider (Ollama/OpenAI), Google OAuth

---

### 1.2 Orchestrators

| Orchestrator | Location | Frequency | Status |
|--------------|----------|-----------|--------|
| **HB Report Orchestrator** | `src/orchestrators/hb_report_orchestrator.py` | Weekly (Tue 09:00 ET) | COMPLETE |
| **Pipeline Orchestrator** | `src/orchestrators/pipeline_orchestrator.py` | On-demand | COMPLETE |
| **Trade Data Orchestrator** | `src/orchestrators/trade_data_orchestrator.py` | Monthly | COMPLETE |
| **CONAB Soybean Orchestrator** | `src/orchestrators/conab_soybean_orchestrator.py` | Monthly | IN-PROGRESS |

**Inputs:** Configuration, source data, scheduling signals
**Outputs:** Processed data, validation reports, generated documents
**Dependencies:** Collector agents, database, file systems

---

### 1.3 Schedulers

| Scheduler | Location | Status |
|-----------|----------|--------|
| **Master Scheduler** | `src/scheduler/master_scheduler.py` | COMPLETE |
| **Report Scheduler** | `src/scheduler/report_scheduler.py` | COMPLETE |
| **Trade Scheduler** | `src/scheduler/trade_scheduler.py` | COMPLETE |
| **Agent Scheduler** | `src/scheduler/agent_scheduler.py` | COMPLETE |

**Configuration:** `config/collection_schedule.json`

---

## PART 2: DATA COLLECTORS

### 2.1 United States Collectors (21)

| Agent | Location | Frequency | Status | Inputs | Outputs |
|-------|----------|-----------|--------|--------|---------|
| USDA NASS Collector | `src/agents/collectors/us/usda_nass_collector.py` | Weekly | COMPLETE | QUICK STATS API | Bronze tables |
| NASS Processing Collector | `src/agents/collectors/us/nass_processing_collector.py` | Monthly | COMPLETE | USDA reports | Monthly realized |
| USDA AMS Collector | `src/agents/collectors/us/usda_ams_collector.py` | Daily | COMPLETE | Market News API | CSV + DB |
| USDA AMS Asynch | `src/agents/collectors/us/usda_ams_asynch.py` | Daily | COMPLETE | Web scraping | Multiple formats |
| USDA FAS Collector | `src/agents/collectors/us/usda_fas_collector.py` | Monthly | IN-PROGRESS | PSD API | Bronze PSD |
| USDA ERS Collector | `src/agents/collectors/us/usda_ers_collector.py` | Weekly | COMPLETE | USDA ERS | Bronze ERS |
| Census Trade Collector | `src/agents/collectors/us/census_trade_collector.py` | Monthly | COMPLETE | Census API | Trade flows |
| Census Trade v2 | `src/agents/collectors/us/census_trade_v2.py` | Monthly | COMPLETE | Census API | Enhanced trade |
| EIA Ethanol Collector | `src/agents/collectors/us/eia_ethanol_collector.py` | Weekly | PARTIAL | EIA API | Bronze ethanol |
| EIA Petroleum Collector | `src/agents/collectors/us/eia_petroleum_collector.py` | Weekly | PARTIAL | EIA API | Bronze petroleum |
| EPA RFS Collector | `src/agents/collectors/us/epa_rfs_collector.py` | Monthly | COMPLETE | EPA RIMS | RIN data |
| EPA RFS v2 | `src/agents/collectors/us/epa_rfs_v2.py` | Monthly | COMPLETE | EPA RIMS v2 | Validated RIN |
| CFTC COT Collector | `src/agents/collectors/us/cftc_cot_collector.py` | Weekly (Fri) | COMPLETE | CFTC | Positioning |
| Drought Collector | `src/agents/collectors/us/drought_collector.py` | Weekly | COMPLETE | NOAA/USDA | Drought index |
| ERS Food Expenditure | `src/agents/collectors/us/ers_food_expenditure.py` | Quarterly | COMPLETE | USDA ERS | Consumption |

**Dependencies:** API keys (NASS, FAS, AMS, Census, EIA), rate limiting

---

### 2.2 South America Collectors (13)

| Agent | Country | Location | Status |
|-------|---------|----------|--------|
| Brazil Agent | Brazil | `src/agents/collectors/south_america/brazil_agent.py` | COMPLETE |
| Brazil Lineup Agent | Brazil | `src/agents/collectors/south_america/brazil_lineup_agent.py` | COMPLETE |
| CONAB Collector | Brazil | `src/agents/collectors/south_america/conab_collector.py` | COMPLETE |
| CONAB Soybean Agent | Brazil | `src/agents/collectors/south_america/conab_soybean_agent.py` | COMPLETE |
| IMEA Collector | Brazil | `src/agents/collectors/south_america/imea_collector.py` | COMPLETE |
| IBGE SIDRA Collector | Brazil | `src/agents/collectors/south_america/ibge_sidra_collector.py` | COMPLETE |
| ABIOVE Collector | Brazil | `src/agents/collectors/south_america/abiove_collector.py` | COMPLETE |
| Argentina Agent | Argentina | `src/agents/collectors/south_america/argentina_agent.py` | COMPLETE |
| Magyp Collector | Argentina | `src/agents/collectors/south_america/magyp_collector.py` | COMPLETE |
| Paraguay Agent | Paraguay | `src/agents/collectors/south_america/paraguay_agent.py` | COMPLETE |
| Colombia Agent | Colombia | `src/agents/collectors/south_america/colombia_agent.py` | COMPLETE |
| Uruguay Agent | Uruguay | `src/agents/collectors/south_america/uruguay_agent.py` | COMPLETE |
| Base Collector | Regional | `src/agents/collectors/south_america/base_collector.py` | COMPLETE |

**Inputs:** Government APIs, web scraping
**Outputs:** Production data, trade flows, state-level granularity
**Dependencies:** Web scraping, regional calendar handling

---

### 2.3 Global/Market Collectors (10)

| Agent | Source | Location | Status |
|-------|--------|----------|--------|
| CME Settlements Collector | CME Globex | `src/agents/collectors/market/cme_settlements.py` | COMPLETE |
| Futures Data Collector | Yahoo Finance | `src/agents/collectors/market/futures_collector.py` | COMPLETE |
| IBKR Collector | Interactive Brokers | `src/agents/collectors/market/ibkr_collector.py` | COMPLETE |
| TradeStation Collector | TradeStation | `src/agents/collectors/market/tradestation_collector.py` | PARTIAL |
| Yahoo Futures Collector | Yahoo Finance | `src/agents/collectors/market/yahoo_futures.py` | COMPLETE |
| FAOSTAT Collector | UN FAO | `src/agents/collectors/global/faostat_collector.py` | COMPLETE |
| GFS Forecast Collector | NOAA GFS | `src/agents/collectors/global/gfs_forecast.py` | COMPLETE |
| GEFS Ensemble Collector | NOAA GEFS | `src/agents/collectors/global/gefs_ensemble.py` | COMPLETE |
| NDVI Collector | NASA MODIS | `src/agents/collectors/global/ndvi_collector.py` | COMPLETE |
| MPOB Collector | Malaysian Palm Board | `src/agents/collectors/global/mpob_collector.py` | COMPLETE |

**Inputs:** APIs, public databases, satellite services
**Outputs:** Time series, forecast grids, satellite imagery
**Dependencies:** NASA token, Yahoo Finance, CME data feeds

---

### 2.4 Specialized Collectors (5)

| Agent | Purpose | Location | Status |
|-------|---------|----------|--------|
| Wheat Tender Collector | Tender tracking | `src/agents/collectors/tenders/wheat_tender.py` | COMPLETE |
| Alert System | Notifications | `src/agents/collectors/tenders/alert_system.py` | COMPLETE |
| IBKR API Connector | Broker integration | `src/agents/integration/ibkr_api_connector.py` | COMPLETE |
| Calendar Agent | Google Calendar | `src/agents/integration/calendar_agent.py` | COMPLETE |
| Email Agent | Gmail/SMTP | `src/agents/integration/email_agent.py` | COMPLETE |

---

## PART 3: ANALYSIS & REPORTING

### 3.1 Analysis Agents (3)

| Agent | Location | Purpose | Status |
|-------|----------|---------|--------|
| **Fundamental Analyzer** | `src/agents/analysis/fundamental_analyzer.py` | S&D, balance sheets | COMPLETE |
| **Price Forecaster** | `src/agents/analysis/price_forecaster.py` | Price prediction | COMPLETE |
| **Spread & Basis Analyzer** | `src/agents/analysis/spread_and_basis_analyzer.py` | Spread analysis | COMPLETE |

**Inputs:** Silver tables, market data
**Outputs:** Analysis reports, KPIs, forecast models
**Dependencies:** Scikit-learn, Pandas, Statsmodels

---

### 3.2 Reporting Agents (5)

| Agent | Location | Produces | Status |
|-------|----------|----------|--------|
| **Report Writer** | `src/agents/reporting/report_writer_agent.py` | HB-style commentary | COMPLETE |
| **Market Research Agent** | `src/agents/reporting/market_research_agent.py` | Market analysis | COMPLETE |
| **Price Data Agent** | `src/agents/reporting/price_data_agent.py` | Price reports | COMPLETE |
| **Internal Data Agent** | `src/agents/reporting/internal_data_agent.py` | Proprietary analysis | COMPLETE |
| **Graphics Generator** | `src/agents/graphics_generator_agent.py` | Charts, maps | COMPLETE |

**Inputs:** Gold views, analysis results
**Outputs:** Documents, charts, formatted reports
**Dependencies:** Document templates, LLM for commentary

---

## PART 4: SPECIALIZED SYSTEMS

### 4.1 BioTrack AI (Rail Car Monitoring) - EXISTING

| Component | Location | Status |
|-----------|----------|--------|
| **Main Controller** | `biotrack/biotrack_ai.py` | COMPLETE |
| **Frame Capture** | `biotrack/frame_capture.py` | COMPLETE |
| **Detection Engine** | `biotrack/detection.py` | COMPLETE |
| **OCR Module** | `biotrack/ocr_module.py` | COMPLETE |
| **Volume Estimator** | `biotrack/volume_estimator.py` | COMPLETE |
| **Dashboard** | `biotrack/dashboard.py` | COMPLETE |

**Current Status:** Simulation mode - ready for real camera integration

**Inputs:** Public rail camera feeds
**Outputs:** Tank car counts, volume estimates, commodity inference
**Dependencies:** YOLOv8, OpenCV, Tesseract, Streamlit

---

### 4.2 Standalone Persistent Agent

| Component | Location | Status |
|-----------|----------|--------|
| **Agent Core** | `src/agents/standalone/agent.py` | COMPLETE |
| **Config** | `src/agents/standalone/config.py` | COMPLETE |
| **Task Submission** | `src/agents/standalone/submit_task.py` | COMPLETE |
| **Tools Registry** | `src/agents/standalone/tools.py` | COMPLETE |

**Inputs:** Task descriptions, context
**Outputs:** Task results, audit logs
**Dependencies:** LLM provider, tool implementations

---

## PART 5: PLANNED AGENTS (PLACEHOLDERS)

### 5.1 Rail Car Project - Enhanced (4 agents)

Building on BioTrack AI foundation, expanding to production-grade system.

| Agent | Location (Planned) | Purpose | Status | Priority |
|-------|-------------------|---------|--------|----------|
| **Rail Camera Ingestion Agent** | `src/agents/rail/camera_ingestion_agent.py` | Multi-source camera feed management, frame extraction, quality filtering | PLACEHOLDER | HIGH |
| **Rail Car Detection Agent** | `src/agents/rail/detection_agent.py` | YOLOv8 tank car detection, car type classification, count validation | PLACEHOLDER | HIGH |
| **Rail Car OCR Agent** | `src/agents/rail/ocr_agent.py` | UMLER code extraction, reporting mark reading, commodity inference | PLACEHOLDER | MEDIUM |
| **Rail Volume Analytics Agent** | `src/agents/rail/analytics_agent.py` | Volume aggregation, trend analysis, facility-level reporting | PLACEHOLDER | MEDIUM |

**Planned Inputs:**
- Camera Sources: 48+ RailStream, 100+ Railfan public feeds
- Historical rail movement data
- UMLER database for car specifications
- Facility location data (refineries, ethanol plants, crushers)

**Planned Outputs:**
- Real-time tank car counts by location
- Volume estimates (gallons) by commodity type
- Facility inflow/outflow tracking
- Anomaly alerts (unusual volumes, new patterns)
- Bronze table: `rail_observations`
- Silver table: `rail_volume_daily`
- Gold view: `rail_volume_by_facility`, `rail_commodity_flows`

**Dependencies:**
- YOLOv8 (object detection)
- Tesseract OCR
- OpenCV (image processing)
- geopandas (location mapping)
- Streamlit (dashboard)
- GPU recommended for real-time processing

**Integration Points:**
- Feed into biofuel S&D models
- Cross-reference with EIA/EPA data
- Alert system for unusual movements

---

### 5.2 VaR Analysis System (3 agents)

Value at Risk analysis for commodity position management.

| Agent | Location (Planned) | Purpose | Status | Priority |
|-------|-------------------|---------|--------|----------|
| **Position Ingestion Agent** | `src/agents/var/position_ingestion_agent.py` | Import positions from IBKR, TradeStation, manual entry; normalize to standard format | PLACEHOLDER | HIGH |
| **VaR Calculation Agent** | `src/agents/var/var_calculation_agent.py` | Historical VaR, Monte Carlo VaR, parametric VaR; multiple confidence levels | PLACEHOLDER | HIGH |
| **Risk Reporting Agent** | `src/agents/var/risk_reporting_agent.py` | Daily P&L attribution, VaR breach alerts, stress testing, limit monitoring | PLACEHOLDER | MEDIUM |

**Planned Inputs:**
- Position data from brokers (IBKR, TradeStation)
- Historical price data (CME settlements, cash prices)
- Volatility surfaces
- Correlation matrices
- User-defined risk limits

**Planned Outputs:**
- Daily VaR by position, portfolio, commodity
- P&L attribution (delta, gamma, theta, vega)
- Stress test scenarios
- Limit utilization reports
- Bronze table: `positions_raw`
- Silver table: `positions_normalized`, `price_returns`
- Gold view: `var_daily`, `var_by_commodity`, `pnl_attribution`

**VaR Methodologies:**
1. **Historical VaR** - Based on actual historical returns
2. **Parametric VaR** - Assumes normal distribution, uses volatility
3. **Monte Carlo VaR** - Simulated scenarios with correlations

**Confidence Levels:** 95%, 99%, 99.5%
**Holding Periods:** 1-day, 10-day, 30-day

**Dependencies:**
- NumPy, SciPy (statistical calculations)
- Pandas (data manipulation)
- IBKR API (position feeds)
- Historical price database
- Volatility estimation models

**Integration Points:**
- IBKR Collector (existing) for position data
- CME Settlements Collector for prices
- Futures Data Collector for historical returns
- Alert System for breach notifications

---

## PART 6: DATABASE SCHEMA

### 6.1 Layer Summary

| Layer | Tables/Views | Purpose |
|-------|--------------|---------|
| Bronze | 31 tables | Raw data as collected |
| Silver | 26 tables | Cleaned, standardized |
| Gold | 53 views | Analytics-ready |
| Audit | 3 tables | Tracking, validation |
| Core | 6 tables | Reference data |

### 6.2 Key Tables

**Bronze (Raw):**
- `fas_psd`, `fas_export_sales` - USDA FAS data
- `usda_nass`, `nass_crop_progress`, `nass_crop_condition` - Crop data
- `conab_production`, `conab_supply_demand` - Brazil data
- `census_trade` - US trade flows
- `cftc_cot` - Positioning data
- `eia_ethanol`, `eia_petroleum` - Energy data
- `weather_observations` - Weather data
- `cme_settlements` - Futures prices

**Silver (Standardized):**
- `monthly_realized` - Monthly S&D actuals
- `crop_progress`, `crop_condition` - Standardized crop data
- `conab_balance_sheet` - Brazil S&D
- `cftc_position_history` - Net positioning
- `cash_price`, `futures_price` - Standardized prices

**Gold (Analytics):**
- `fas_us_*_balance_sheet` - US balance sheets
- `brazil_*_production` - Brazil production views
- `cftc_*_positioning` - Positioning views
- `eia_*_weekly` - Energy views

### 6.3 Planned Tables (Rail Car & VaR)

**Rail Car Project:**
```sql
-- Bronze
bronze.rail_observations (
    id, camera_id, capture_time, frame_path,
    detection_count, raw_detections_json
)

-- Silver
silver.rail_volume_daily (
    date, location_id, commodity_type,
    car_count, estimated_gallons, confidence
)

-- Gold
gold.rail_volume_by_facility
gold.rail_commodity_flows
gold.rail_anomaly_alerts
```

**VaR Analysis:**
```sql
-- Bronze
bronze.positions_raw (
    id, source, import_time, account_id,
    symbol, quantity, price, raw_data_json
)

-- Silver
silver.positions_normalized (
    date, account_id, commodity, contract,
    quantity, market_value, delta_equivalent
)
silver.price_returns (
    date, symbol, return_1d, return_10d, volatility_30d
)

-- Gold
gold.var_daily
gold.var_by_commodity
gold.pnl_attribution
gold.limit_utilization
```

---

## PART 7: CONFIGURATION FILES

| File | Location | Purpose |
|------|----------|---------|
| `.env` | Root | All credentials (26 API keys) |
| `collection_schedule.json` | `config/` | Collection timing |
| `eia_series_config.json` | `config/` | EIA series mapping |
| `fas_api_endpoints.json` | `config/` | FAS API config |
| `weather_cities.json` | `config/` | Weather locations |
| `mcp_config.json` | `config/` | MCP server config |

---

## PART 8: KNOWN ISSUES

| Issue | Component | Priority | Notes |
|-------|-----------|----------|-------|
| USDA FAS API Errors | FAS Collector | HIGH | Returning 500 errors |
| Code Duplication | USDA AMS | MEDIUM | Two asynch versions |
| EIA Integration | EIA Collectors | MEDIUM | Needs save_to_bronze |
| IBKR Gateway | IBKR Collector | MEDIUM | Requires local gateway |

---

## PART 9: DEPENDENCY SUMMARY

### External APIs (26 total)
- USDA: NASS, FAS, AMS, ERS
- Census Bureau
- EIA, EPA
- CFTC
- OpenWeather, Open-Meteo
- NASA Earthdata
- Yahoo Finance
- Interactive Brokers
- Google (Calendar, Gmail)
- Notion, Dropbox

### Python Libraries (Key)
- Data: pandas, numpy, scipy
- Database: psycopg2, sqlalchemy
- ML: scikit-learn, statsmodels
- Vision: opencv-python, ultralytics (YOLOv8)
- OCR: pytesseract
- Web: requests, aiohttp, beautifulsoup4
- LLM: openai, anthropic, ollama
- Viz: matplotlib, plotly, streamlit

---

## PART 10: NEXT STEPS

### Immediate (This Week)
1. Review this inventory for accuracy
2. Prioritize Rail Car vs VaR implementation
3. Define detailed requirements for placeholder agents

### Short Term (Next Month)
1. Implement first placeholder agent
2. Create database schemas for new systems
3. Build integration tests

### Medium Term (Next Quarter)
1. Complete Rail Car Project MVP
2. Complete VaR Analysis MVP
3. Integrate with existing orchestrators
4. Deploy checker pattern for quality assurance

---

## Appendix: File Locations Quick Reference

```
RLC-Agent/
├── src/
│   ├── agents/
│   │   ├── core/           # Master agent, approval, memory
│   │   ├── collectors/     # All data collectors
│   │   │   ├── us/         # US sources
│   │   │   ├── south_america/  # SA sources
│   │   │   ├── global/     # Global sources
│   │   │   ├── market/     # Market data
│   │   │   └── tenders/    # Tender tracking
│   │   ├── analysis/       # Analysis agents
│   │   ├── reporting/      # Report generators
│   │   ├── integration/    # External integrations
│   │   ├── loaders/        # Data loaders
│   │   └── standalone/     # Persistent agent
│   ├── orchestrators/      # Workflow managers
│   ├── scheduler/          # Scheduling system
│   ├── services/           # Shared services
│   └── main.py             # Entry point
├── biotrack/               # Rail monitoring (existing)
├── config/                 # Configuration files
├── database/               # Migrations, schemas
├── domain_knowledge/       # Reference data
├── docs/                   # Documentation
├── scripts/                # Utility scripts
└── tests/                  # Test suites
```
