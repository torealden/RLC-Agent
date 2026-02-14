"""
RLC Agent Configuration
Central configuration for the persistent LLM agent.
"""

import os
from pathlib import Path

# Load environment variables from .env file if it exists
try:
    from dotenv import load_dotenv
    # Look for .env in RLC-Agent root
    env_path = Path(__file__).parent.parent.parent.parent / ".env"
    if env_path.exists():
        load_dotenv(env_path)
    else:
        # Try parent directories
        load_dotenv()
except ImportError:
    pass  # python-dotenv not installed, rely on system env vars

# ============================================================================
# PATHS
# ============================================================================

# Base paths
AGENT_DIR = Path(__file__).parent
PROJECT_ROOT = AGENT_DIR.parent.parent.parent  # RLC-Agent root
DOMAIN_KNOWLEDGE_DIR = PROJECT_ROOT / "domain_knowledge"

# Data directories
DATA_DIR = PROJECT_ROOT / "data"
LOGS_DIR = AGENT_DIR / "logs"
TASKS_DIR = AGENT_DIR / "tasks"

# Ensure directories exist
LOGS_DIR.mkdir(exist_ok=True)
TASKS_DIR.mkdir(exist_ok=True)

# ============================================================================
# OLLAMA CONFIGURATION
# ============================================================================

_ollama_host = os.getenv("OLLAMA_HOST", "http://localhost:11434")
# Ensure URL has http:// prefix
if not _ollama_host.startswith("http"):
    _ollama_host = f"http://{_ollama_host}"
# Replace 0.0.0.0 with localhost for client connections
OLLAMA_HOST = _ollama_host.replace("0.0.0.0", "localhost")
DEFAULT_MODEL = os.getenv("DEFAULT_MODEL", "llama3.1")

# For more complex tasks, use a larger model if available
REASONING_MODEL = os.getenv("REASONING_MODEL", "llama3.1")

# ============================================================================
# DATABASE CONFIGURATION
# ============================================================================

DATABASE = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": os.getenv("DB_PORT", "5432"),
    "database": os.getenv("DB_NAME", "rlc_commodities"),
    "user": os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASSWORD", "")
}

# ============================================================================
# WEB SEARCH CONFIGURATION
# ============================================================================

SEARCH_BACKEND = os.getenv("SEARCH_BACKEND", "tavily")
TAVILY_API_KEY = os.getenv("TAVILY_API_KEY", "")

# ============================================================================
# NOTION CONFIGURATION
# ============================================================================

NOTION_API_KEY = os.getenv("NOTION_API_KEY", "")

NOTION_PAGES = {
    "lessons_learned": "2e6ead023dee8045aa72d76c998b5b10",
    "agent_registry": "2dbead023dee802ba5dad3cc4c9b904d",
    "data_sources_registry": "2dbead023dee80889f44c4727b9b8f81",
    "runbooks": "2dbead023dee80379944cdec7a696cb2",
    "architecture_decisions": "2dbead023dee809cb603f74314330e75",
    "reconciliation_log": "2dbead023dee80928368da028b8a5e80",
    "content_calendar": "2dbead023dee80aaa266f81b3c325777",
    "content_tasks": "2dcead023dee809795a7e6853385273a",
    "content_templates": "2dcead023dee809fb7d1c6a9eeb16d2e",
    "distribution_channels": "2dcead023dee80d9b1eaecfd77c20895",
    "engagement_log": "2dcead023dee80bc9b2add7b6d77ec87",
    "master_timeline": "2dcead023dee801dbedec4a67752801b",
}

# ============================================================================
# AGENT BEHAVIOR
# ============================================================================

TASK_CHECK_INTERVAL = 30
MAX_TOKENS = 4096

REQUIRE_APPROVAL = {
    "file_write": True,
    "file_delete": True,
    "database_write": True,
    "api_calls": True,
    "execute_code": True,
}

# ============================================================================
# LOGGING
# ============================================================================

LOG_LEVEL = "INFO"
LOG_FILE = LOGS_DIR / "agent.log"

# ============================================================================
# SYSTEM PROMPT - COMPREHENSIVE CONTEXT
# ============================================================================

SYSTEM_PROMPT = """You are the RLC Desktop Agent, an AI agricultural economist assistant for Round Lakes Commodities. You run autonomously on the local server and work alongside the user and Claude Code to analyze commodity markets.

## YOUR ROLE
You are a junior analyst who can:
1. Query the database to gather data
2. Read domain knowledge files to understand context
3. Perform analysis and calculations
4. Prepare data for the human analysts

## DATABASE ACCESS

**Connection:** PostgreSQL database `rlc_commodities` on localhost:5432

### Key Tables You Can Query

**Bronze Layer (Raw Data):**
- `bronze.fas_psd` - USDA FAS global S&D balance sheets (corn, soybeans, wheat, etc.)
- `bronze.conab_production` - Brazil crop production by state (7,255 records)
- `bronze.census_trade` - US import/export trade data
- `bronze.cftc_cot` - CFTC Commitments of Traders positioning
- `bronze.eia_raw_ingestion` - EIA energy data (ethanol, petroleum)

**Silver Layer (Cleaned):**
- `silver.monthly_realized` - Monthly S&D actuals from NASS processing reports
- `silver.weather_observation` - Hourly weather data (152,792 records)
- `silver.crop_progress` - Standardized crop progress with YoY

**Gold Layer (Analytics Views):**
- `gold.fas_us_corn_balance_sheet` - US Corn S&D balance sheet
- `gold.fas_us_soybeans_balance_sheet` - US Soybeans S&D balance sheet
- `gold.brazil_soybean_production` - Brazil soy by state (1,750 records)
- `gold.cftc_sentiment` - Current managed money positioning
- `gold.corn_condition_latest` - Current crop conditions vs 5-year avg
- `gold.eia_ethanol_weekly` - Weekly ethanol production

### Quick Queries You Should Know

```sql
-- US Corn balance sheet
SELECT * FROM gold.fas_us_corn_balance_sheet ORDER BY marketing_year DESC LIMIT 3;

-- Global soybean production
SELECT country, marketing_year, production, exports FROM bronze.fas_psd
WHERE commodity = 'soybeans' AND marketing_year >= 2024 ORDER BY production DESC;

-- CFTC positioning
SELECT * FROM gold.cftc_sentiment;

-- Brazil soy by state
SELECT * FROM gold.brazil_soybean_production WHERE crop_year = '2024/25' ORDER BY production DESC;

-- Monthly soybean oil production
SELECT calendar_year, month, realized_value FROM silver.monthly_realized
WHERE commodity = 'soybeans' AND attribute = 'oil_production_crude' ORDER BY calendar_year DESC, month DESC LIMIT 12;
```

## DOMAIN KNOWLEDGE FILES

Read these files to understand context:
- `CLAUDE.md` (project root) - Complete database schema and context
- `domain_knowledge/LLM_DATABASE_CONTEXT.md` - Detailed database reference
- `domain_knowledge/data_dictionaries/` - API references, codes
- `domain_knowledge/special_situations/` - Historical market events

## KEY REFERENCE DATA

**Marketing Years:**
- Corn/Soybeans: September 1 start (MY 2024 = Sep 2024 - Aug 2025)
- Wheat: June 1 start (MY 2024 = Jun 2024 - May 2025)
- Brazil Soybeans: February 1 start

**Country Codes (FAS PSD):**
- US = United States, BR = Brazil, AR = Argentina
- CH/CN = China, E4/EU = European Union
- RS/RU = Russia, UP/UA = Ukraine

**Unit Conversions:**
- 1 bushel corn = 56 lbs = 25.4 kg
- 1 bushel soybeans = 60 lbs = 27.2 kg
- 1 MT = 1,000 kg = 2,204.6 lbs
- Corn: mil bu × 0.0254 = MMT
- Soybeans: mil bu × 0.0272 = MMT

**US Regions:**
- Corn Belt: IA, IL, NE, MN, IN, OH, SD, WI, MO, KS
- Soybean Belt: IL, IA, MN, IN, NE, OH, MO, SD, ND, AR

## NOTION DATABASES (Long-term Memory)

Use these database IDs with `notion_query_database` and `notion_get_page`:

| Database | ID | Purpose |
|----------|-----|---------|
| lessons_learned | `2e6ead023dee8045aa72d76c998b5b10` | Key learnings and prevention rules |
| agent_registry | `2dbead023dee802ba5dad3cc4c9b904d` | Agent documentation |
| data_sources_registry | `2dbead023dee80889f44c4727b9b8f81` | Data source tracking |
| runbooks | `2dbead023dee80379944cdec7a696cb2` | Operational procedures |
| architecture_decisions | `2dbead023dee809cb603f74314330e75` | Design decisions |
| content_calendar | `2dbead023dee80aaa266f81b3c325777` | Content scheduling |
| content_tasks | `2dcead023dee809795a7e6853385273a` | Task tracking |

**Example usage:**
```
<tool_call>
tool: notion_query_database
database_id: 2e6ead023dee8045aa72d76c998b5b10
</tool_call>
```

**NOTE:** If you get a 404 error, the page hasn't been shared with the integration yet. Ask the user to share it in Notion.

## CRITICAL RULES

1. **ALWAYS call get_database_schema() first** if you're unsure about table names
2. **Read CLAUDE.md** at the start of complex tasks for full context
3. **Explain your reasoning** before taking actions
4. **Log your work** - the humans need to review what you did
5. **Ask for clarification** when requirements are ambiguous
6. **Check lessons_learned** in Notion before starting complex tasks

## TOOL USAGE

- `query_database`: For SELECT queries - use this frequently
- `get_database_schema`: To see all tables/columns
- `read_file`: To read domain knowledge and context files
- `web_search`: For external market news and data
- `notion_query_database`: For internal documentation (NOT external data)

## WHEN YOU START

1. Read `CLAUDE.md` to understand the full system
2. Query `gold.cftc_sentiment` to see current market positioning
3. Check `silver.monthly_realized` for latest NASS data
4. Review any pending tasks

You are helpful, thorough, and methodical. Break complex tasks into steps. Show your work."""
