# RLC-Agent

**An AI-powered Agricultural Economist Assistant for Commodity Market Analysis**

RLC-Agent is a local LLM-based system designed to replicate the analytical capabilities of an agricultural economist. It automates data collection from government and industry sources, generates market reports, and provides intelligent analysis of commodity markets including grains, oilseeds, biofuels, and fats & greases.

---

## Table of Contents

- [Overview](#overview)
- [Key Features](#key-features)
- [Architecture](#architecture)
- [Database](#database)
- [Folder Structure](#folder-structure)
- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Configuration](#configuration)
- [Usage](#usage)
- [Data Sources](#data-sources)
- [Domain Knowledge](#domain-knowledge)
- [Reports & Outputs](#reports--outputs)
- [LLM Integration](#llm-integration)
- [Scheduling](#scheduling)
- [Development](#development)
- [Troubleshooting](#troubleshooting)

---

## Overview

RLC-Agent serves as an AI business partner for Round Lakes Commodities (RLC), providing:

- **Automated Data Collection**: Pulls data from USDA, EIA, EPA, Census Bureau, CFTC, and international sources on scheduled intervals
- **Market Analysis**: Processes commodity data through a medallion architecture (Bronze → Silver → Gold)
- **Report Generation**: Creates weekly market reports with executive summaries and price analysis
- **Intelligent Queries**: Answers natural language questions about commodity markets via MCP server integration
- **Weather Monitoring**: Tracks agricultural weather conditions and sends automated alerts

The system is built to run locally using Ollama for LLM inference, with optional cloud LLM support (Claude, GPT-4) for advanced analysis.

**For LLM context and database reference, see [`CLAUDE.md`](CLAUDE.md)** - the comprehensive context document for AI-assisted analysis.

---

## Key Features

### Data Collection & Processing
- **Multi-source ingestion**: USDA (NASS, FAS, ERS, AMS), EIA, EPA, Census Bureau, CFTC, CME
- **International coverage**: Brazil (CONAB, ABIOVE, IMEA), Argentina (MAGyP), Canada (StatCan, CGC), Malaysia (MPOB)
- **Medallion architecture**: Raw data (Bronze) → Cleaned/normalized (Silver) → Analytics-ready (Gold)
- **Automated scheduling**: Data pulled according to official release schedules (WASDE, Weekly Petroleum, etc.)

### AI-Powered Analysis
- **Master Agent**: Central orchestrator coordinating specialized sub-agents
- **Report Writer Agent**: Generates narrative market analysis
- **Market Research Agent**: Identifies bullish/bearish factors
- **Fundamental Analyzer**: S&D balance, stocks-to-use, yield analysis
- **Price Forecaster**: Price predictions with confidence intervals
- **Graphics Generator**: Chart and visualization creation

### Database & Analytics
- **PostgreSQL database** with 110+ tables/views
- **Bronze layer**: 31 tables of raw ingested data
- **Silver layer**: 26 tables of cleaned, standardized data
- **Gold layer**: 53 analytics-ready views
- **MCP Server**: Direct LLM database access via Model Context Protocol

### Integrations
- **Email**: Gmail integration for notifications and report distribution
- **Calendar**: Google Calendar for scheduling and reminders
- **Notion**: Long-term memory and knowledge management
- **Dropbox**: Report distribution and file sharing
- **PowerBI**: Interactive dashboards

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                         User Interface                          │
│         (CLI / Interactive Mode / Claude Code / MCP)            │
└─────────────────────────────────────────────────────────────────┘
                                 │
                                 ▼
┌─────────────────────────────────────────────────────────────────┐
│                    Master Agent / Orchestrators                  │
│         (Request routing, task planning, coordination)          │
└─────────────────────────────────────────────────────────────────┘
                                 │
         ┌───────────────────────┼───────────────────────┐
         ▼                       ▼                       ▼
┌─────────────────┐   ┌─────────────────┐   ┌─────────────────┐
│  Data Agents    │   │ Reporting Agents │   │Integration Agents│
│  - Collectors   │   │  - Report Writer │   │  - Email        │
│  - Analyzers    │   │  - Research      │   │  - Calendar     │
│  - Transformers │   │  - Graphics      │   │  - Notion       │
└─────────────────┘   └─────────────────┘   └─────────────────┘
         │                       │                       │
         └───────────────────────┼───────────────────────┘
                                 ▼
┌─────────────────────────────────────────────────────────────────┐
│                      Services Layer                              │
│          (Database, APIs, Document Generation, MCP)              │
└─────────────────────────────────────────────────────────────────┘
                                 │
                                 ▼
┌─────────────────────────────────────────────────────────────────┐
│                    PostgreSQL Database                           │
│    Bronze (31 tables) → Silver (26 tables) → Gold (53 views)    │
└─────────────────────────────────────────────────────────────────┘
```

### Agent Types

| Agent | Purpose | Location |
|-------|---------|----------|
| **Master Agent** | Central orchestrator, routes requests | `src/agents/core/master_agent.py` |
| **Data Agent** | Fetches market data, prices, weather | `src/agents/core/data_agent.py` |
| **Database Agent** | Manages database operations | `src/agents/core/database_agent.py` |
| **Verification Agent** | Data quality checks | `src/agents/core/verification_agent.py` |
| **Report Writer** | Generates narrative reports | `src/agents/reporting/report_writer_agent.py` |
| **Fundamental Analyzer** | S&D balance, stocks-to-use analysis | `src/agents/analysis/fundamental_analyzer.py` |
| **Price Forecaster** | Price predictions | `src/agents/analysis/price_forecaster.py` |
| **Graphics Generator** | Chart and visualization creation | `src/agents/graphics_generator_agent.py` |
| **Weather Agents** | Weather collection and alerts | `src/scheduler/agents/weather_*_agent.py` |

---

## Database

**Connection**: `localhost:5432`, database: `rlc_commodities`, user: `postgres`

### Layer Summary

| Layer | Tables/Views | Purpose |
|-------|--------------|---------|
| Bronze | 31 tables | Raw ingested data from all sources |
| Silver | 26 tables | Cleaned, standardized, enriched data |
| Gold | 53 views | Analytics-ready views for dashboards |

### Key Tables

| Table/View | Description | Records |
|------------|-------------|---------|
| `bronze.fas_psd` | Global S&D balance sheets (USDA FAS) | 220+ countries/commodities |
| `gold.fas_us_corn_balance_sheet` | US Corn balance sheet | Latest |
| `gold.fas_us_soybeans_balance_sheet` | US Soybeans balance sheet | Latest |
| `gold.brazil_soybean_production` | Brazil soy by state | 1,750 |
| `bronze.conab_production` | Brazil all crops by state | 7,255 |
| `gold.cftc_sentiment` | Managed money positioning | Latest |
| `silver.weather_observation` | Hourly weather data | 152,792 |
| `silver.monthly_realized` | Monthly S&D actuals (NASS) | 400+ |
| `bronze.census_trade` | US import/export trade | 1,536 |

**Full database documentation**: See [`CLAUDE.md`](CLAUDE.md) and [`domain_knowledge/LLM_DATABASE_CONTEXT.md`](domain_knowledge/LLM_DATABASE_CONTEXT.md)

---

## Folder Structure

```
RLC-Agent/
│
├── .env                      # Environment variables and API keys
├── .env.example              # Template for environment setup
├── .mcp.json                 # MCP server configuration
├── CLAUDE.md                 # LLM context document (comprehensive)
├── README.md                 # This file
├── requirements.txt          # Python dependencies
│
├── src/                      # Main application source code
│   ├── main.py              # CLI entry point
│   ├── agents/              # AI agents
│   │   ├── core/            # Master agent, data agent, memory manager
│   │   ├── base/            # Base classes for agents
│   │   ├── analysis/        # Fundamental analyzers, forecasters
│   │   ├── reporting/       # Report generation agents
│   │   ├── collectors/      # Data collectors by region
│   │   │   ├── us/          # USDA, EIA, Census, CFTC, EPA
│   │   │   ├── south_america/ # Brazil (CONAB), Argentina
│   │   │   ├── canada/      # StatCan, CGC
│   │   │   └── asia/        # Malaysia (MPOB)
│   │   └── integration/     # Email, calendar, Notion
│   ├── mcp/                 # MCP server for LLM database access
│   │   └── commodities_db_server.py
│   ├── orchestrators/       # Workflow coordinators
│   ├── scheduler/           # Task scheduling system
│   ├── services/            # Shared services (APIs, database, docs)
│   ├── tools/               # LLM tool definitions, db_query.py
│   └── utils/               # Configuration, helpers
│
├── database/                 # Database schema and migrations
│   └── schemas/             # SQL schema files (001-016)
│
├── config/                   # Application configuration
│   ├── eia_series_config.json    # EIA series by commodity
│   ├── weather_locations.json    # Weather monitoring locations
│   └── usda_commodities.json     # USDA commodity mappings
│
├── data/                     # Data storage
│   ├── weather_graphics/    # Weather images from emails
│   └── generated_graphics/  # Custom visualizations
│
├── output/                   # Generated outputs
│   ├── reports/             # Generated reports
│   └── logs/                # Application logs
│
├── dashboards/               # PowerBI dashboard files
│
├── domain_knowledge/         # Agricultural economist knowledge base
│   ├── LLM_CONTEXT.md       # Full domain context
│   ├── LLM_DATABASE_CONTEXT.md # Database schema reference
│   ├── balance_sheets/      # S&D templates by commodity
│   ├── crop_calendars/      # Global planting/harvest timing
│   ├── crop_maps/           # 201 maps for 38 countries
│   ├── data_dictionaries/   # API references (EIA, USDA, EPA)
│   ├── sample_reports/      # Professional market commentary
│   ├── special_situations/  # Historical market events
│   └── templates/           # Report templates
│
├── docs/                     # Project documentation
├── tests/                    # Test suite
└── archive/                  # Deprecated code
```

---

## Prerequisites

### Required Software

| Software | Version | Purpose |
|----------|---------|---------|
| Python | 3.11+ | Runtime environment |
| PostgreSQL | 14+ | Primary database |
| Ollama | Latest | Local LLM inference |
| Git | Latest | Version control |

### Required API Keys

| Key | Source | Purpose |
|-----|--------|---------|
| `EIA_API_KEY` | [eia.gov](https://www.eia.gov/opendata/register.php) | Energy/ethanol data |
| `NASS_API_KEY` | [quickstats.nass.usda.gov](https://quickstats.nass.usda.gov/api) | USDA crop data |
| `FAS_API_KEY` | [api.data.gov](https://api.data.gov/signup/) | USDA FAS PSD data |
| `TAVILY_API_KEY` | [tavily.com](https://tavily.com) | Web search |

### Optional API Keys

| Key | Source | Purpose |
|-----|--------|---------|
| `ANTHROPIC_API_KEY` | Anthropic | Cloud LLM (Claude) |
| `OPENAI_API_KEY` | OpenAI | Cloud LLM (GPT-4) |
| `NOTION_API_KEY` | Notion | Long-term memory |
| `CENSUS_API_KEY` | Census Bureau | Trade data |
| `DROPBOX_ACCESS_TOKEN` | Dropbox | Report distribution |

---

## Installation

### 1. Clone and Setup Environment

```bash
cd RLC-Agent

# Create virtual environment
python -m venv venv

# Activate (Windows)
venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

### 2. Configure Environment

```bash
# Copy example environment file
cp .env.example .env

# Edit .env with your API keys and database settings
```

### 3. Setup Database

```bash
# Create PostgreSQL database
createdb rlc_commodities

# Run schema migrations (in order)
psql -d rlc_commodities -f database/schemas/001_schema_foundation.sql
psql -d rlc_commodities -f database/schemas/002_bronze_layer.sql
psql -d rlc_commodities -f database/schemas/003_silver_layer.sql
psql -d rlc_commodities -f database/schemas/004_gold_layer.sql
# Continue with remaining schema files (005-016)...
```

### 4. Setup Ollama (Optional for local LLM)

```bash
# Install Ollama (see ollama.ai)
ollama pull llama3.1
ollama serve
```

### 5. Verify Installation

```bash
# Test database connection
python src/tools/db_query.py --analysis commodity_coverage

# Run interactive mode
python -m src.main interactive
```

---

## Configuration

### Environment Variables (.env)

```bash
# Database
DATABASE_URL=postgresql://postgres:password@localhost:5432/rlc_commodities

# LLM Provider
OLLAMA_HOST=http://localhost:11434
DEFAULT_MODEL=llama3.1
# ANTHROPIC_API_KEY=sk-ant-...

# Data Source APIs
EIA_API_KEY=your_eia_key
NASS_API_KEY=your_nass_key
FAS_API_KEY=your_fas_key
TAVILY_API_KEY=your_tavily_key

# Optional Integrations
NOTION_API_KEY=your_notion_key
DROPBOX_ACCESS_TOKEN=your_dropbox_token

# Agent Settings
AUTONOMY_LEVEL=supervised
LOG_LEVEL=INFO
```

---

## Usage

### Interactive Mode

```bash
python -m src.main interactive

# Example queries:
# "What is the current US corn stocks-to-use ratio?"
# "Compare Brazil vs US soybean production"
# "Show CFTC managed money positioning"
```

### Database Queries (CLI)

```bash
# Get US corn balance sheet
python src/tools/db_query.py --analysis us_corn_balance

# Get commodity coverage summary
python src/tools/db_query.py --analysis commodity_coverage

# Custom SQL query
python src/tools/db_query.py "SELECT * FROM gold.cftc_sentiment"
```

### Data Collection

```bash
# Run all daily collectors
python -m src.main collect --daily

# Collect specific source
python -m src.main collect --source usda_fas
python -m src.main collect --source eia
python -m src.main collect --source nass
```

### Report Generation

```bash
# Generate weekly report
python -m src.main report --weekly
```

---

## Data Sources

### United States

| Source | Agency | Data Types | Frequency |
|--------|--------|------------|-----------|
| NASS | USDA | Crop progress, production, processing | Weekly/Monthly |
| FAS | USDA | Global S&D (PSD), export sales | Monthly/Weekly |
| ERS | USDA | Supply/demand projections | Monthly |
| AMS | USDA | Cash prices, market news | Daily |
| EIA | DOE | Ethanol, petroleum, natural gas | Weekly/Monthly |
| EPA | EPA | RFS/RIN data | Monthly |
| CFTC | CFTC | Commitments of Traders | Weekly |
| Census | Commerce | Trade statistics (HS codes) | Monthly |

### International

| Source | Country | Data Types |
|--------|---------|------------|
| CONAB | Brazil | Crop estimates, S&D by state |
| ABIOVE | Brazil | Soybean crush, exports |
| IMEA | Brazil | Mato Grosso regional data |
| MAGyP | Argentina | Crop data, exports |
| StatCan | Canada | Trade statistics |
| CGC | Canada | Grain exports, inspections |
| MPOB | Malaysia | Palm oil production |

---

## Domain Knowledge

The `domain_knowledge/` folder contains comprehensive reference materials:

### Data Dictionaries
- **USDA FAS PSD API Reference** - Commodity codes, country codes, marketing years
- **EIA Series ID Reference** - 150+ series for ethanol, biodiesel, petroleum, natural gas
- **EPA RFS RIN Reference** - RIN codes and generation data
- **HS Codes Reference** - Trade classification codes

### Crop Maps
- **201 maps** covering **38 countries**
- US county-level production maps (corn, soybeans, wheat, cotton, etc.)
- Brazil state-level production maps

### Balance Sheets
Templates organized by commodity: biofuels, feed grains, food grains, oilseeds, fats & greases

### Special Situations
Historical market event documentation:
- 2012 US Drought
- 2018 China Trade War
- 2020 China Demand Surge
- 2020 Derecho
- 2022 Ukraine War

---

## LLM Integration

### MCP Server

The MCP server (`src/mcp/commodities_db_server.py`) provides Claude Code with direct database access:

**Available Tools:**
| Tool | Function |
|------|----------|
| `get_balance_sheet` | S&D balance sheet for commodity/country |
| `get_production_ranking` | Global production rankings |
| `get_stocks_to_use` | Stocks-to-use ratio analysis |
| `analyze_supply_demand` | Comprehensive S&D with YoY changes |
| `get_brazil_production` | Brazil state-level production |
| `query_database` | Custom SQL queries |

**Setup**: Configure in `.mcp.json`, see `src/mcp/README.md`

### CLAUDE.md

The [`CLAUDE.md`](CLAUDE.md) file provides comprehensive context for LLM analysis:
- Complete database schema (all Bronze/Silver/Gold tables)
- Data source references and field descriptions
- Marketing year conventions
- Unit conversions
- Country codes
- Query examples
- Regional references

---

## Reports & Outputs

### Generated Reports

| Report | Description | Frequency | Location |
|--------|-------------|-----------|----------|
| HB Weekly | Comprehensive market summary | Weekly | `output/reports/` |
| Weather Summary | Agricultural weather conditions | Daily | `output/reports/weather_summaries/` |
| Price Analysis | Commodity price movements | On-demand | `output/reports/` |

### Visualizations

Generated graphics stored in `data/generated_graphics/`:
- Time series charts (prices, production, stocks)
- Year-over-year comparison charts
- CFTC positioning charts
- Balance sheet tables

---

## Scheduling

### Data Update Schedule

| Data Source | Update Frequency | Timing |
|-------------|------------------|--------|
| USDA FAS PSD | Monthly | WASDE day 12:00 PM ET |
| USDA NASS Progress | Weekly | Monday 4:00 PM ET |
| USDA NASS Processing | Monthly | ~10th of month |
| CFTC COT | Weekly | Friday 3:30 PM ET |
| EIA Petroleum | Weekly | Wednesday 10:30 AM ET |
| Export Sales | Weekly | Thursday 8:30 AM ET |
| Weather | Hourly | Continuous |

### Windows Task Scheduler

```powershell
cd src/scheduler/tasks
.\setup_windows_tasks.ps1
```

---

## Development

### Running Tests

```bash
pytest tests/
```

### Adding a New Data Collector

1. Create collector in `src/agents/collectors/{region}/`
2. Inherit from `BaseCollector`
3. Implement `collect()` and `transform()` methods
4. Create Bronze table in `database/schemas/`
5. Add Silver transformation
6. Update `CLAUDE.md` with new table documentation

### Code Conventions

- **Agents**: Inherit from base classes in `src/agents/base/`
- **Collectors**: Follow patterns in `src/agents/collectors/us/`
- **SQL**: Schema changes in numbered files in `database/schemas/`

---

## Troubleshooting

### Database Connection Errors

```bash
# Check PostgreSQL is running
pg_isready

# Verify connection
psql -d rlc_commodities -c "SELECT 1"

# Check tables exist
psql -d rlc_commodities -c "\dt bronze.*"
```

### LLM Not Responding

```bash
# Check Ollama
ollama list
curl http://localhost:11434/api/tags

# Restart Ollama
ollama serve
```

### Missing Data

1. Check API keys in `.env`
2. Verify data source release schedule
3. Run manual collection: `python -m src.main collect --source <name>`
4. Check logs in `output/logs/`

---

## License

Proprietary - Round Lakes Commodities

---

## Support

- Review [`CLAUDE.md`](CLAUDE.md) for LLM context and database reference
- Check `domain_knowledge/LLM_DATABASE_CONTEXT.md` for detailed schema
- Review `docs/` for additional documentation
- Check logs in `output/logs/`

---

*Last updated: February 2026*
