# LLM Setup Plan for RLC-Agent Report Generation

This document provides a step-by-step guide to get your LLM up and running for generating commodity market reports.

---

## Overview

The RLC-Agent system uses LLMs for:
- **HB Weekly Report** generation (market analysis, executive summaries)
- **Market Research** (bullish/bearish factor identification)
- **Natural language queries** about commodity data

**Supported LLM Providers:**
| Provider | Model | Use Case |
|----------|-------|----------|
| Anthropic | Claude | Primary cloud LLM |
| OpenAI | GPT-4 | Fallback cloud LLM |
| Ollama | Llama 3.1 | Local/self-hosted |

---

## Step 1: Environment Setup

### 1.1 Create Virtual Environment

```bash
cd /home/user/RLC-Agent
python -m venv venv
source venv/bin/activate
```

### 1.2 Install Dependencies

```bash
pip install -r requirements.txt
```

### 1.3 Create Environment File

```bash
cp .env.example .env
```

Edit `.env` with your actual credentials.

---

## Step 2: Configure LLM Provider

Choose **one** of the following options:

### Option A: Anthropic Claude (Recommended for Production)

Add to `.env`:
```
ANTHROPIC_API_KEY=sk-ant-...your-key-here...
```

**Pros:** Best reasoning, follows instructions well, handles commodity analysis effectively.

### Option B: OpenAI GPT-4

Add to `.env`:
```
OPENAI_API_KEY=sk-...your-key-here...
```

**Pros:** Widely available, good general performance.

### Option C: Ollama (Local/Self-Hosted)

1. Install Ollama: https://ollama.ai/download

2. Pull a model:
```bash
ollama pull llama3.1
```

3. Add to `.env`:
```
OLLAMA_HOST=http://localhost:11434
DEFAULT_MODEL=llama3.1
```

**Pros:** No API costs, data stays local, works offline.
**Cons:** Requires local compute resources (8GB+ RAM recommended).

---

## Step 3: Database Setup

The system requires a database for storing commodity data that the LLM analyzes.

### Option A: PostgreSQL (Production)

1. Install PostgreSQL:
```bash
# Ubuntu/Debian
sudo apt-get install postgresql postgresql-contrib

# macOS
brew install postgresql
```

2. Create database:
```bash
createdb rlc_commodities
```

3. Run migrations (in order):
```bash
psql -d rlc_commodities -f database/sql/001_schema_foundation.sql
psql -d rlc_commodities -f database/sql/002_bronze_layer.sql
psql -d rlc_commodities -f database/sql/003_silver_layer.sql
psql -d rlc_commodities -f database/sql/004_gold_layer.sql
```

4. Add to `.env`:
```
DATABASE_URL=postgresql://username:password@localhost:5432/rlc_commodities
```

### Option B: SQLite (Development/Testing)

No setup required - the system falls back to SQLite automatically:
```
# SQLite file created at: ./data/rlc_commodities.db
```

---

## Step 4: Configure Required API Keys

For full functionality, add these API keys to `.env`:

### Essential Keys

| Key | Source | Purpose |
|-----|--------|---------|
| `EIA_API_KEY` | https://www.eia.gov/opendata/register.php | Energy/ethanol data |
| `NASS_API_KEY` | https://quickstats.nass.usda.gov/api | USDA crop data |
| `TAVILY_API_KEY` | https://tavily.com | Web search for market research |

### Optional Keys

| Key | Source | Purpose |
|-----|--------|---------|
| `NOTION_API_KEY` | https://developers.notion.com | Long-term memory system |
| `CENSUS_API_KEY` | https://api.census.gov/data/key_signup.html | Trade data (avoids rate limits) |
| `DROPBOX_ACCESS_TOKEN` | Dropbox App Console | Report distribution |

---

## Step 5: Collect Initial Data

Before generating reports, you need data in your database.

### Run Daily Collection

```bash
python -m src.main collect --daily
```

This collects data from all active sources.

### Or Collect Specific Sources

```bash
# USDA Foreign Agricultural Service (export sales)
python -m src.main collect --source usda_fas

# Energy Information Administration
python -m src.main collect --source eia

# USDA NASS (crop progress)
python -m src.main collect --source nass
```

### Verify Data Collection

```bash
# Check database for recent data
python -m src.main query "Show recent data ingest status"
```

---

## Step 6: Generate Your First Report

### 6.1 Test LLM Connection

```bash
# Interactive mode to test LLM
python -m src.main interactive
```

Type a simple query like: "What are current corn prices?"

### 6.2 Generate Weekly Report

```bash
# Generate HB Weekly Report for current date
python -m src.main report --weekly

# Or specify a date
python -m src.main report --weekly --date 2026-01-26
```

### 6.3 Report Output

Reports are generated as Word documents and saved to:
- Local: `./reports/` directory
- Dropbox: If `DROPBOX_ACCESS_TOKEN` is configured

---

## Step 7: Automate with Scheduler (Optional)

### Start the Scheduler

```bash
python -m src.main schedule --start
```

This runs data collection according to release schedules:

| Source | Schedule | Time (ET) |
|--------|----------|-----------|
| NASS Crop Progress | Monday (Apr-Nov) | 4:00 PM |
| EIA Petroleum | Wednesday | 10:30 AM |
| USDA Export Sales | Thursday | 8:30 AM |
| CME Settlements | Daily | 5:00 PM |

### Or Use the Dedicated Scheduler

```bash
cd rlc_scheduler
python agent_scheduler.py run
```

---

## Step 8: Verify Everything Works

### Checklist

- [ ] Virtual environment activated
- [ ] Dependencies installed (`pip install -r requirements.txt`)
- [ ] `.env` file created with LLM API key
- [ ] Database setup (PostgreSQL or SQLite fallback)
- [ ] At least one data source API key configured
- [ ] Initial data collection completed
- [ ] Interactive query works
- [ ] Report generation succeeds

### Quick Validation Commands

```bash
# Test LLM configuration
python -c "from src.utils.config import Settings; s = Settings(); print(f'LLM configured: {bool(s.api.anthropic_api_key or s.api.openai_api_key)}')"

# Test database connection
python -c "from src.services.database.db_config import get_engine; print('DB connected:', get_engine() is not None)"

# Generate test report
python -m src.main report --weekly
```

---

## Troubleshooting

### LLM Not Responding

1. Check API key is set in `.env`
2. Verify internet connection (for cloud LLMs)
3. For Ollama, ensure service is running: `ollama serve`

### Database Connection Errors

1. Verify `DATABASE_URL` in `.env`
2. Check PostgreSQL is running: `pg_isready`
3. Ensure migrations have been run

### Missing Data in Reports

1. Run data collection: `python -m src.main collect --daily`
2. Check data source API keys are valid
3. Verify release schedules (some data only available on certain days)

### Report Quality Issues

The report system tracks quality metrics:
- **Completeness score**: Target 80%+
- **Placeholder count**: Should be 0
- **Questions raised**: Check data gaps

---

## Architecture Reference

```
┌─────────────────────────────────────────────────────────┐
│                    User Request                          │
└─────────────────────────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────┐
│                 HB Report Orchestrator                   │
│   src/orchestrators/hb_report_orchestrator.py           │
└─────────────────────────────────────────────────────────┘
                           │
        ┌──────────────────┼──────────────────┐
        ▼                  ▼                  ▼
┌───────────────┐  ┌───────────────┐  ┌───────────────┐
│ Report Writer │  │ Price Data    │  │ Market        │
│ Agent         │  │ Agent         │  │ Research Agent│
│ (LLM-powered) │  │ (Database)    │  │ (LLM-powered) │
└───────────────┘  └───────────────┘  └───────────────┘
        │                  │                  │
        └──────────────────┼──────────────────┘
                           ▼
┌─────────────────────────────────────────────────────────┐
│                    PostgreSQL Database                   │
│   Bronze → Silver → Gold (Medallion Architecture)       │
└─────────────────────────────────────────────────────────┘
```

---

## Key Files

| File | Purpose |
|------|---------|
| `src/main.py` | CLI entry point |
| `src/utils/config.py` | Configuration management |
| `src/agents/reporting/report_writer_agent.py` | LLM report generation |
| `src/orchestrators/hb_report_orchestrator.py` | Report workflow |
| `.env` | API keys and settings |

---

## Next Steps After Setup

1. **Customize report templates** in `Models/` directory
2. **Add more data sources** by creating collectors in `src/agents/collectors/`
3. **Configure Notion integration** for persistent memory
4. **Set up email notifications** for report distribution
5. **Deploy scheduler** as a systemd service for 24/7 operation

---

*Document generated: 2026-01-26*
*For questions or issues, refer to the main README or create a GitHub issue.*
