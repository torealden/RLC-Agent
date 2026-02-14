# RLC Data Platform Inventory
**Generated: 2026-01-28**

---

## DATABASE DATA INVENTORY

### Bronze Layer (Raw Data)

| Table | Records | Period | Source | Frequency |
|-------|---------|--------|--------|-----------|
| `bronze.census_trade` | 1,461 | 2024-10 to 2024-12 | CENSUS_TRADE | Monthly |
| `bronze.cftc_cot` | 306 | 2025-02-04 to 2026-01-20 | CFTC | Weekly |
| `bronze.eia_raw_ingestion` | 612 | 2025-10-31 to 2026-01-16 | EIA | Weekly |
| `bronze.ers_oilcrops_raw` | 25,441 | Historical | USDA_ERS | Monthly |
| `bronze.ers_wheat_raw` | 53,373 | Historical | USDA_ERS | Monthly |
| `bronze.futures_daily_settlement` | 10 | 2026-01-27 | yahoo_finance | Daily |
| `bronze.nass_acreage` | 31 | 2025 | USDA_NASS | Annual |
| `bronze.nass_crop_condition` | 252 | 2025 | USDA_NASS | Weekly |
| `bronze.nass_crop_progress` | 197 | 2025 | USDA_NASS | Weekly |
| `bronze.nass_production` | 52 | 2025 | USDA_NASS | Monthly |
| `bronze.nass_stocks` | 48 | 2025 | USDA_NASS | Quarterly |
| `bronze.price_report_raw` | 22,849 | 2019-12-09 to 2026-01-23 | USDA_AMS | Daily |
| `bronze.weather_email_extract` | 45 | 2026-01-22 to 2026-01-28 | Email | Daily |
| `bronze.weather_raw` | 152,763 | 2010-01-01 to 2026-01-28 | open_meteo, openweather | Hourly |

### Silver Layer (Cleaned/Transformed)

| Table | Records | Period | Description |
|-------|---------|--------|-------------|
| `silver.cash_price` | 3,491 | 2020-08-04 to 2026-01-23 | Daily cash bid prices |
| `silver.futures_price` | 10 | 2026-01-27 | Daily futures settlements |
| `silver.weather_observation` | 152,763 | 2010-01-01 to 2026-01-28 | Cleaned weather data |

### Gold Layer (Analytics-Ready Views)

| View | Records | Period | Description |
|------|---------|--------|-------------|
| `gold.eia_ethanol_weekly` | 12 | 2025-10-31 to 2026-01-16 | Ethanol production/stocks |
| `gold.eia_petroleum_weekly` | 12 | 2025-10-31 to 2026-01-16 | Crude/gasoline/diesel stocks |
| `gold.eia_prices_daily` | 12 | 2025-10-31 to 2026-01-16 | WTI, Brent, RBOB prices |
| `gold.cftc_sentiment` | 6 | Current | MM positioning percentiles |
| `gold.cftc_latest_positions` | 6 | Current | Latest COT positions |
| `gold.nass_condition_yoy` | - | Current | Crop condition YoY comparison |
| `gold.us_soybean_balance_sheet` | - | Historical | Soybean S&D balance sheet |
| `gold.us_wheat_balance_sheet` | - | Historical | Wheat S&D balance sheet |

### Reference Tables

| Table | Records | Description |
|-------|---------|-------------|
| `public.commodity` | 12 | Commodity definitions (corn, soybeans, etc.) |
| `public.data_source` | 13 | Data source definitions |
| `public.location` | 21 | Countries and regions |
| `public.unit` | 19 | Unit conversions |
| `public.weather_location` | 28 | Weather monitoring locations |

---

## COLLECTORS INVENTORY

### US Government Data

| Collector | File | API Key Required | Status | Data |
|-----------|------|------------------|--------|------|
| USDA NASS | `usda_nass_collector.py` | NASS_API_KEY | Working | Crop progress, condition, acreage, production, stocks |
| USDA FAS | `usda_fas_collector.py` | FAS_API_KEY | API Down | Export sales, PSD data |
| USDA AMS | `usda_ams_collector.py` | USDA_AMS_API_KEY | Working | Daily cash prices |
| USDA ERS | `usda_ers_collector.py` | None | Working | Oilcrops, wheat data |
| EIA Ethanol | `eia_ethanol_collector.py` | EIA_API_KEY | Working | Weekly ethanol data |
| EIA Petroleum | `eia_petroleum_collector.py` | EIA_API_KEY | Working | Weekly petroleum data |
| Census Trade | `census_trade_collector.py` | CENSUS_API_KEY (optional) | Working | Monthly import/export |
| CFTC COT | `cftc_cot_collector.py` | None | Working | Weekly positioning |
| EPA RFS | `epa_rfs_collector.py` | None | Available | Renewable fuel standards |
| US Drought | `drought_collector.py` | None | Available | Drought monitor |

### Market Data

| Collector | File | API Key Required | Status | Data |
|-----------|------|------------------|--------|------|
| CME Settlements | `cme_settlements_collector.py` | None | Available | Daily settlements |
| Yahoo Futures | `yahoo_futures_collector.py` | None | Working | Daily futures prices |
| IBKR | `ibkr_collector.py` | IBKR credentials | Available | Real-time prices |
| TradeStation | `tradestation_collector.py` | TS credentials | Available | Real-time prices |

### International Data

| Collector | File | Region | Status | Data |
|-----------|------|--------|--------|------|
| CONAB | `conab_collector.py` | Brazil | Working | Brazilian crop estimates |
| ABIOVE | `abiove_collector.py` | Brazil | Available | Brazilian crush data |
| IMEA | `imea_collector.py` | Brazil | Available | Mato Grosso data |
| IBGE SIDRA | `ibge_sidra_collector.py` | Brazil | Available | Brazilian stats |
| MAGyP | `magyp_collector.py` | Argentina | Available | Argentine ag data |
| Stats Canada | `canada_statscan_collector.py` | Canada | Available | Canadian ag stats |
| Canada CGC | `canada_cgc_collector.py` | Canada | Available | Grain Commission data |
| MPOB | `mpob_collector.py` | Malaysia | Available | Palm oil data |
| FAOSTAT | `faostat_collector.py` | Global | Available | FAO global data |

---

## AGENTS INVENTORY

### Core Agents

| Agent | File | Description |
|-------|------|-------------|
| Master Agent | `master_agent.py` | Orchestrates other agents |
| Database Agent | `database_agent.py` | Database operations |
| Data Agent | `data_agent.py` | Data processing |
| Verification Agent | `verification_agent.py` | Data validation |

### Reporting Agents

| Agent | File | Description |
|-------|------|-------------|
| Report Writer | `report_writer_agent.py` | Generates reports |
| Market Research | `market_research_agent.py` | Market analysis |
| Price Data | `price_data_agent.py` | Price analysis |
| Internal Data | `internal_data_agent.py` | Internal data access |

### Integration Agents

| Agent | File | Description |
|-------|------|-------------|
| Email Agent | `email_agent.py` | Email processing |
| Calendar Agent | `calendar_agent.py` | Calendar integration |

### Regional Agents

| Agent | File | Region | Description |
|-------|------|--------|-------------|
| Brazil Agent | `brazil_agent.py` | Brazil | Brazilian market data |
| Argentina Agent | `argentina_agent.py` | Argentina | Argentine market data |
| Paraguay Agent | `paraguay_agent.py` | Paraguay | Paraguayan market data |
| Uruguay Agent | `uruguay_agent.py` | Uruguay | Uruguayan market data |
| Colombia Agent | `colombia_agent.py` | Colombia | Colombian market data |

### Scheduler Agents

| Agent | File | Description |
|-------|------|-------------|
| Weather Collector | `weather_collector_agent.py` | Scheduled weather collection |
| Cash Price Collector | `cash_price_collector_agent.py` | Scheduled price collection |
| Data Checker | `data_checker_agent.py` | Data validation |
| Weather Email | `weather_email_agent.py` | Weather email processing |

---

## ORCHESTRATORS INVENTORY

| Orchestrator | File | Description |
|--------------|------|-------------|
| Pipeline | `pipeline_orchestrator.py` | Main data pipeline |
| HB Report | `hb_report_orchestrator.py` | HB report generation |
| Trade Data | `trade_data_orchestrator.py` | Trade data pipeline |
| CONAB Soybean | `conab_soybean_orchestrator.py` | Brazilian soybean data |

---

## SCHEDULERS INVENTORY

| Scheduler | File | Description |
|-----------|------|-------------|
| Agent Scheduler | `agent_scheduler.py` | Main agent scheduling |
| Master Scheduler | `master_scheduler.py` | Top-level coordination |
| Report Scheduler | `report_scheduler.py` | Report scheduling |
| Trade Scheduler | `trade_scheduler.py` | Trade data scheduling |

---

## COLLECTION SCHEDULE (Recommended)

| Data Source | Frequency | Day/Time (ET) | Collector |
|-------------|-----------|---------------|-----------|
| NASS Crop Progress | Weekly | Mon 4:30 PM | usda_nass_collector |
| EIA Ethanol | Weekly | Wed 11:00 AM | eia_ethanol_collector |
| CFTC COT | Weekly | Fri 4:00 PM | cftc_cot_collector |
| Census Trade | Monthly | 6th 9:00 AM | census_trade_collector |
| AMS Cash Prices | Daily | 5:00 PM CT | usda_ams_collector |
| Weather | Hourly | Continuous | weather_collector_agent |
| FAS Export Sales | Weekly | Thu 8:45 AM | usda_fas_collector (when API restored) |

---

## API KEYS STATUS

| Key | Environment Variable | Status |
|-----|---------------------|--------|
| NASS | NASS_API_KEY | Configured |
| FAS | FAS_API_KEY | Configured (API down) |
| EIA | EIA_API_KEY | Configured |
| Census | CENSUS_API_KEY | Configured |
| AMS | USDA_AMS_API_KEY | Configured |
| Weather | OPENWEATHER_API_KEY | Configured |
