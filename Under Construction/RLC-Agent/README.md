# RLC-Agent

**An AI-powered Agricultural Economist Assistant for Commodity Market Analysis**

RLC-Agent is a local LLM-based system designed to replicate the analytical capabilities of an agricultural economist. It automates data collection from government and industry sources, generates market reports, and provides intelligent analysis of commodity markets including grains, oilseeds, biofuels, and fats & greases.

---

## Table of Contents

- [Overview](#overview)
- [Key Features](#key-features)
- [Architecture](#architecture)
- [Folder Structure](#folder-structure)
- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Configuration](#configuration)
- [Usage](#usage)
- [Data Sources](#data-sources)
- [Reports & Outputs](#reports--outputs)
- [Scheduling](#scheduling)
- [Development](#development)
- [Troubleshooting](#troubleshooting)

---

## Overview

RLC-Agent serves as an AI business partner for Round Lakes Commodities (RLC), providing:

- **Automated Data Collection**: Pulls data from USDA, EIA, Census Bureau, and international sources on scheduled intervals
- **Market Analysis**: Processes commodity data through a medallion architecture (Bronze → Silver → Gold)
- **Report Generation**: Creates weekly market reports with executive summaries and price analysis
- **Intelligent Queries**: Answers natural language questions about commodity markets
- **Weather Monitoring**: Tracks agricultural weather conditions and sends automated alerts

The system is built to run locally using Ollama for LLM inference, ensuring data privacy and eliminating API costs for day-to-day operations.

---

## Key Features

### Data Collection & Processing
- **Multi-source ingestion**: USDA (NASS, FAS, ERS, AMS), EIA, EPA, Census Bureau, CME
- **International coverage**: Brazil (CONAB, ABIOVE), Argentina (MAGyP), Canada (StatCan, CGC)
- **Medallion architecture**: Raw data (Bronze) → Cleaned/normalized (Silver) → Analytics-ready (Gold)
- **Automated scheduling**: Data pulled according to official release schedules

### AI-Powered Analysis
- **Master Agent**: Central orchestrator coordinating specialized sub-agents
- **Report Writer Agent**: Generates narrative market analysis
- **Market Research Agent**: Identifies bullish/bearish factors
- **Price Data Agent**: Fetches and analyzes price movements
- **Standalone Agent**: Background task processor with tool-calling capabilities

### Reporting & Visualization
- **HB Weekly Report**: Comprehensive weekly market summary
- **PowerBI Dashboards**: Interactive visualizations for trade flows, prices, and balance sheets
- **Weather Summaries**: Daily agricultural weather reports via email

### Integrations
- **Email**: Gmail integration for notifications and report distribution
- **Calendar**: Google Calendar for scheduling and reminders
- **Notion**: Long-term memory and knowledge management
- **Dropbox**: Report distribution and file sharing

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                         User Interface                          │
│              (CLI / Interactive Mode / Scheduled)               │
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
│  - Transformers │   │  - Price Data    │   │  - Notion       │
└─────────────────┘   └─────────────────┘   └─────────────────┘
         │                       │                       │
         └───────────────────────┼───────────────────────┘
                                 ▼
┌─────────────────────────────────────────────────────────────────┐
│                      Services Layer                              │
│          (Database, APIs, Document Generation)                   │
└─────────────────────────────────────────────────────────────────┘
                                 │
                                 ▼
┌─────────────────────────────────────────────────────────────────┐
│                    PostgreSQL Database                           │
│         Bronze (Raw) → Silver (Clean) → Gold (Analytics)        │
└─────────────────────────────────────────────────────────────────┘
```

### Agent Types

| Agent | Purpose | Location |
|-------|---------|----------|
| **Master Agent** | Central orchestrator, routes requests to sub-agents | `src/agents/core/master_agent.py` |
| **Data Agent** | Fetches market data, prices, weather | `src/agents/core/data_agent.py` |
| **Database Agent** | Manages database operations | `src/agents/core/database_agent.py` |
| **Report Writer** | Generates narrative reports | `src/agents/reporting/report_writer_agent.py` |
| **Market Research** | Identifies market factors | `src/agents/reporting/market_research_agent.py` |
| **Standalone Agent** | Background task processor with tools | `src/agents/standalone/agent.py` |
| **Weather Agent** | Collects weather data, sends alerts | `src/scheduler/agents/weather_*_agent.py` |

---

## Folder Structure

```
RLC-Agent/
│
├── .env                      # Environment variables and API keys
├── .env.example              # Template for environment setup
├── .gitignore                # Git ignore rules
├── .mcp.json                 # MCP configuration
├── requirements.txt          # Python dependencies
├── LLM_SETUP_PLAN.md        # Detailed setup guide
├── README.md                 # This file
│
├── src/                      # Main application source code
│   ├── main.py              # CLI entry point
│   ├── agents/              # AI agents
│   │   ├── core/            # Master agent, data agent, memory manager
│   │   ├── base/            # Base classes for agents
│   │   ├── analysis/        # Fundamental analyzers, forecasters
│   │   ├── reporting/       # Report generation agents
│   │   ├── collectors/      # Data collectors by region
│   │   │   ├── us/          # USDA, EIA, Census, etc.
│   │   │   ├── south_america/ # Brazil, Argentina, etc.
│   │   │   ├── canada/      # StatCan, CGC
│   │   │   └── ...          # Other regions
│   │   ├── integration/     # Email, calendar, Notion, IBKR
│   │   └── standalone/      # Background task processor
│   ├── orchestrators/       # Workflow coordinators
│   ├── scheduler/           # Task scheduling system
│   │   ├── agents/          # Scheduled task agents
│   │   ├── tasks/           # Task definitions and batch files
│   │   └── config/          # Scheduler configuration
│   ├── services/            # Shared services
│   │   ├── api/             # External API clients
│   │   ├── database/        # Database operations
│   │   └── document/        # Document generation
│   ├── tools/               # LLM tool definitions
│   └── utils/               # Configuration, helpers
│
├── database/                 # Database schema and migrations
│   ├── schemas/             # SQL schema files (001-009)
│   ├── migrations/          # Database migrations
│   ├── views/               # SQL view definitions
│   ├── queries/             # Reusable SQL queries
│   └── sql/                 # Additional SQL scripts
│
├── config/                   # Application configuration
│   ├── data_sources_master.csv  # Master list of data sources
│   ├── weather_locations.json   # Weather monitoring locations
│   └── weather_email_config.json # Email alert settings
│
├── data/                     # Data storage (mostly gitignored)
│   ├── raw/                 # Raw downloaded data
│   ├── processed/           # Transformed data
│   ├── cached/              # API response cache
│   └── exports/             # Exported datasets
│
├── output/                   # Generated outputs
│   ├── reports/             # Generated reports
│   │   └── weather_summaries/ # Daily weather reports
│   ├── logs/                # Application logs
│   └── visualizations/      # Generated charts
│
├── scripts/                  # Utility scripts
│   ├── collectors/          # Data collection scripts
│   ├── transformations/     # Data transformation scripts
│   ├── visualizations/      # Chart generation scripts
│   ├── deployment/          # Deployment utilities
│   └── data/                # Data processing scripts
│
├── dashboards/               # Visualization assets
│   ├── powerbi/             # PowerBI dashboard files
│   └── templates/           # Dashboard templates
│
├── domain_knowledge/         # Agricultural economist knowledge base
│   ├── balance_sheets/      # Excel balance sheet models
│   │   ├── biofuels/
│   │   ├── feed_grains/
│   │   ├── food_grains/
│   │   ├── oilseeds/
│   │   └── fats_greases/
│   ├── sample_reports/      # Reference reports by commodity
│   ├── sample_presentations/ # Historical presentations
│   ├── operator_guides/     # Agent operator documentation
│   ├── llm_context/         # LLM training context
│   ├── crop_calendars/      # Planting/harvest schedules
│   ├── crop_maps/           # Regional crop maps
│   ├── glossaries/          # Commodity terminology
│   ├── market_specs/        # Futures contract specifications
│   ├── methodology/         # Analytical methodologies
│   └── var_analysis/        # Value at Risk documentation
│
├── biotrack/                 # Biofuel facility tracking system
│   ├── biotrack_ai.py       # Main tracking script
│   ├── config/              # Facility configurations
│   └── README.md            # Biotrack documentation
│
├── docs/                     # Project documentation
│   ├── architecture/        # System design documents
│   ├── setup/               # Installation guides
│   ├── api/                 # API documentation
│   └── runbooks/            # Operational procedures
│
├── tests/                    # Test suite
│   └── collectors/          # Collector tests
│
└── archive/                  # Deprecated/historical code
    └── deprecated_code/     # Old implementations
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
| `TAVILY_API_KEY` | [tavily.com](https://tavily.com) | Web search for research |

### Optional API Keys

| Key | Source | Purpose |
|-----|--------|---------|
| `ANTHROPIC_API_KEY` | Anthropic | Cloud LLM (Claude) |
| `OPENAI_API_KEY` | OpenAI | Cloud LLM (GPT-4) |
| `NOTION_API_KEY` | Notion | Long-term memory |
| `CENSUS_API_KEY` | Census Bureau | Trade data (avoids rate limits) |
| `DROPBOX_ACCESS_TOKEN` | Dropbox | Report distribution |

---

## Installation

### 1. Clone and Setup Environment

```bash
# Navigate to project directory
cd RLC-Agent

# Create virtual environment
python -m venv venv

# Activate virtual environment
# Windows:
venv\Scripts\activate
# Linux/Mac:
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt
```

### 2. Configure Environment

```bash
# Copy example environment file
cp .env.example .env

# Edit .env with your API keys and settings
# Required: Database connection, at least one LLM provider
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
# Continue with remaining schema files...
```

### 4. Setup Ollama

```bash
# Install Ollama (see ollama.ai for platform-specific instructions)
# Pull recommended model
ollama pull llama3.1

# Verify Ollama is running
ollama list
```

### 5. Verify Installation

```bash
# Test LLM connection
python -c "from src.utils.config import Settings; s = Settings(); print('Config loaded')"

# Test database connection
python -c "from src.services.database.db_config import get_engine; print('DB connected:', get_engine() is not None)"

# Run interactive mode
python -m src.main interactive
```

---

## Configuration

### Environment Variables (.env)

```bash
# Database
DATABASE_URL=postgresql://username:password@localhost:5432/rlc_commodities

# LLM Provider (choose one or more)
OLLAMA_HOST=http://localhost:11434
DEFAULT_MODEL=llama3.1
# ANTHROPIC_API_KEY=sk-ant-...
# OPENAI_API_KEY=sk-...

# Data Source APIs
EIA_API_KEY=your_eia_key
NASS_API_KEY=your_nass_key
TAVILY_API_KEY=your_tavily_key

# Optional Integrations
NOTION_API_KEY=your_notion_key
DROPBOX_ACCESS_TOKEN=your_dropbox_token

# Agent Settings
AUTONOMY_LEVEL=supervised  # supervised, semi_autonomous, autonomous
LOG_LEVEL=INFO
```

### Data Sources Configuration

Edit `config/data_sources_master.csv` to enable/disable specific data sources and set collection frequencies.

---

## Usage

### Interactive Mode

```bash
# Start interactive CLI
python -m src.main interactive

# Example queries:
# "What is the current corn price?"
# "Summarize today's emails"
# "Show my schedule for today"
# "Generate daily briefing"
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

# Generate report for specific date
python -m src.main report --weekly --date 2026-01-26
```

### Standalone Agent (Background Tasks)

```bash
# Run in interactive mode
python src/agents/standalone/agent.py --interactive

# Run as daemon (continuous background processing)
python src/agents/standalone/agent.py --daemon

# Submit a task to the queue
python src/agents/standalone/agent.py --task "Analyze corn export trends"
```

### Database Queries

```bash
# Query via CLI
python -m src.main query "Show recent data ingest status"

# Direct SQL (via standalone agent tools)
# Use query_database tool for SELECT queries
```

---

## Data Sources

### United States

| Source | Agency | Data Types | Frequency |
|--------|--------|------------|-----------|
| NASS | USDA | Crop progress, production, stocks | Weekly/Monthly |
| FAS | USDA | Export sales, trade data | Weekly |
| ERS | USDA | Supply/demand projections, prices | Monthly |
| AMS | USDA | Market news, prices | Daily |
| EIA | DOE | Ethanol production, petroleum | Weekly |
| EPA | EPA | RFS/RIN data | Monthly |
| Census | Commerce | Trade statistics | Monthly |

### International

| Source | Country | Data Types |
|--------|---------|------------|
| CONAB | Brazil | Crop estimates, supply/demand |
| ABIOVE | Brazil | Soybean crush, exports |
| MAGyP | Argentina | Crop data, exports |
| StatCan | Canada | Trade statistics |
| CGC | Canada | Grain exports, inspections |

---

## Reports & Outputs

### Generated Reports

| Report | Description | Frequency | Location |
|--------|-------------|-----------|----------|
| HB Weekly | Comprehensive market summary | Weekly | `output/reports/` |
| Weather Summary | Agricultural weather conditions | Daily | `output/reports/weather_summaries/` |
| Price Analysis | Commodity price movements | On-demand | `output/reports/` |

### Visualizations

| Dashboard | Description | Location |
|-----------|-------------|----------|
| US Balance Sheets | Supply/demand visualization | `dashboards/powerbi/` |
| Trade Flows | Soybean export flows | `dashboards/powerbi/` |
| USDA Prices | Price trend analysis | `dashboards/powerbi/` |

### Logs

| Log | Purpose | Location |
|-----|---------|----------|
| weather_collector.log | Weather data collection | `output/logs/` |
| weather_email.log | Email alert system | `output/logs/` |
| data_checker.log | Data validation | `output/logs/` |

---

## Scheduling

### Windows Task Scheduler

```powershell
# Setup scheduled tasks
cd src/scheduler/tasks
.\setup_windows_tasks.ps1
```

### Manual Scheduler

```bash
# Start the scheduler
python src/scheduler/agent_scheduler.py run
```

### Default Schedule

| Task | Schedule | Time (ET) |
|------|----------|-----------|
| NASS Crop Progress | Monday (Apr-Nov) | 4:00 PM |
| EIA Petroleum | Wednesday | 10:30 AM |
| USDA Export Sales | Thursday | 8:30 AM |
| Weather Collection | Daily | 6:00 AM, 12:00 PM |
| Weather Email | Daily | 7:00 AM |

---

## Development

### Running Tests

```bash
# Run all tests
pytest tests/

# Run specific test
pytest tests/test_wheat_tender_collector.py
```

### Code Structure Conventions

- **Agents**: Inherit from base classes in `src/agents/base/`
- **Collectors**: Follow the pattern in `src/agents/collectors/us/`
- **Services**: Stateless utilities in `src/services/`
- **SQL**: Schema changes go in numbered files in `database/schemas/`

### Adding a New Data Collector

1. Create collector in appropriate region folder (`src/agents/collectors/{region}/`)
2. Inherit from `BaseCollector`
3. Implement `collect()` and `transform()` methods
4. Add to `config/data_sources_master.csv`
5. Create Bronze table in `database/schemas/`
6. Add Silver transformation in `scripts/transformations/`

---

## Troubleshooting

### LLM Not Responding

```bash
# Check Ollama is running
ollama list

# Restart Ollama
ollama serve

# Test connection
curl http://localhost:11434/api/tags
```

### Database Connection Errors

```bash
# Check PostgreSQL is running
pg_isready

# Verify connection string in .env
psql $DATABASE_URL -c "SELECT 1"

# Check migrations were run
psql -d rlc_commodities -c "\dt bronze.*"
```

### Missing Data in Reports

1. Verify API keys are valid in `.env`
2. Check data source release schedule (some data only available on certain days)
3. Run manual collection: `python -m src.main collect --source <source_name>`
4. Check logs in `output/logs/`

### Scheduler Not Running

```bash
# Check Windows Task Scheduler
schtasks /query /tn "RLC*"

# View scheduler logs
cat src/scheduler/logs/scheduler.log
```

---

## License

Proprietary - Round Lakes Commodities

---

## Support

For questions or issues:
- Review documentation in `docs/`
- Check `LLM_SETUP_PLAN.md` for detailed setup instructions
- Review logs in `output/logs/`

---

*Last updated: January 2026*
