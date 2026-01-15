"""
RLC Agent Configuration
Central configuration for the persistent LLM agent.
"""

import os
from pathlib import Path

# Load environment variables from .env file if it exists
try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).parent.parent / ".env")
except ImportError:
    pass  # python-dotenv not installed, rely on system env vars

# ============================================================================
# PATHS
# ============================================================================

# Base paths - adjust for your system
AGENT_DIR = Path(__file__).parent
PROJECT_ROOT = AGENT_DIR.parent

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

OLLAMA_HOST = os.getenv("OLLAMA_HOST", "http://localhost:11434")
DEFAULT_MODEL = os.getenv("DEFAULT_MODEL", "llama3.1")

# For more complex tasks, use a larger model if available
REASONING_MODEL = os.getenv("REASONING_MODEL", "llama3.1")  # Could be "llama3.1:70b" if you have RAM

# ============================================================================
# DATABASE CONFIGURATION
# ============================================================================

DATABASE = {
    "host": os.getenv("DATABASE_HOST", "localhost"),
    "port": os.getenv("DATABASE_PORT", "5432"),
    "database": os.getenv("DATABASE_NAME", "rlc_commodities"),
    "user": os.getenv("DATABASE_USER", "postgres"),
    "password": os.getenv("DATABASE_PASSWORD", "")  # REQUIRED - set in .env file
}

# ============================================================================
# WEB SEARCH CONFIGURATION
# ============================================================================

# Search backend: "tavily" (recommended) or "duckduckgo" (free fallback)
SEARCH_BACKEND = os.getenv("SEARCH_BACKEND", "tavily")

# Tavily API key - get free key at https://tavily.com
TAVILY_API_KEY = os.getenv("TAVILY_API_KEY", "")  # REQUIRED for tavily backend - set in .env file

# ============================================================================
# NOTION CONFIGURATION
# ============================================================================

# Get your integration token from: https://www.notion.so/my-integrations
NOTION_API_KEY = os.getenv("NOTION_API_KEY", "")  # REQUIRED - set in .env file

# Main pages/databases the agent should know about (optional - can also search)
NOTION_PAGES = {
    "lessons_learned" : "2e6ead023dee8045aa72d76c998b5b10",
    "agent_registry" : "2dbead023dee802ba5dad3cc4c9b904d",
    "data_sources_registry" : "2dbead023dee80889f44c4727b9b8f81",
    "runbooks": "2dbead023dee80379944cdec7a696cb2",
    "architecture_decisions" : "2dbead023dee809cb603f74314330e75",
    "reconciliation_log" : "2dbead023dee80928368da028b8a5e80",
    "content_calendar" : "2dbead023dee80aaa266f81b3c325777",
    "content_tasks" : "2dcead023dee809795a7e6853385273a",
    "content_templates" : "2dcead023dee809fb7d1c6a9eeb16d2e",
    "distribution_channels" : "2dcead023dee80d9b1eaecfd77c20895",
    "engagement_log" : "2dcead023dee80bc9b2add7b6d77ec87",
    "master_timeline" : "2dcead023dee801dbedec4a67752801b",
}

# ============================================================================
# AGENT BEHAVIOR
# ============================================================================

# How often to check for new tasks (seconds)
TASK_CHECK_INTERVAL = 30

# Maximum tokens for LLM responses
MAX_TOKENS = 4096

# Whether to require approval for certain actions
REQUIRE_APPROVAL = {
    "file_write": True,      # Writing files
    "file_delete": True,     # Deleting files
    "database_write": True,  # Modifying database
    "api_calls": True,       # Making external API calls
    "execute_code": True,    # Running Python code
}

# ============================================================================
# LOGGING
# ============================================================================

LOG_LEVEL = "INFO"
LOG_FILE = LOGS_DIR / "agent.log"

# ============================================================================
# SYSTEM PROMPT
# ============================================================================

SYSTEM_PROMPT = """You are the RLC Agent, an AI business partner specializing in commodity market analysis. You run persistently on RLC-Server and help with:

**Core Responsibilities:**
1. Data Management - Find, collect, and organize commodity market data
2. Analysis - Analyze market trends, trade flows, and supply/demand balances
3. Forecasting - Help develop and refine commodity price and volume forecasts
4. Reporting - Generate reports and summaries

**Your Capabilities:**
- Search the internet for information and data sources (web_search, web_search_news)
- Read and write files on the local system
- Query and update the PostgreSQL database
- Execute Python scripts
- Analyze data using pandas
- Access Notion pages and databases (read and update) - NOTE: Notion is for INTERNAL documentation only, not external data

**Your Constraints:**
- Always explain your reasoning before taking action
- For sensitive operations (database writes, file changes), request approval
- Log all significant actions
- When uncertain, ask for clarification

**CRITICAL DATABASE RULES:**
- ALWAYS call get_database_schema() BEFORE attempting any database queries
- NEVER assume table names exist - verify them from the schema first
- The database uses bronze/silver/gold medallion architecture with specific table names

**CRITICAL TOOL USAGE RULES:**
- web_search and web_search_news: Use for external internet information
- notion_search/notion_get_page: Use ONLY for internal Notion documentation - NOT for external news or data
- If a web search fails, try simpler queries or use web_search instead of web_search_news (or vice versa)
- Do NOT suggest Notion as an alternative to failed web searches

**LESSONS LEARNED:**
- Before starting complex tasks, query the "lessons_learned" Notion database for relevant prevention rules
- When you make a mistake or encounter an error pattern, suggest adding it to lessons_learned

**Current Project Context:**
- Database: PostgreSQL with bronze/silver/gold medallion architecture
- Training data: Historical commodity data through 2019/20 marketing year
- Test data: 2021-2025 reserved for validating forecasts
- Key commodities: Soybeans, soybean meal/oil, corn, wheat, canola

When given a task, break it down into steps, explain your approach, then execute."""
