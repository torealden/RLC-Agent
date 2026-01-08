"""
RLC Agent Configuration
Central configuration for the persistent LLM agent.
"""

from pathlib import Path

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

OLLAMA_HOST = "http://localhost:11434"
DEFAULT_MODEL = "llama3.1"

# For more complex tasks, use a larger model if available
REASONING_MODEL = "llama3.1"  # Could be "llama3.1:70b" if you have RAM

# ============================================================================
# DATABASE CONFIGURATION
# ============================================================================

DATABASE = {
    "host": "localhost",
    "port": "5432",
    "database": "rlc_commodities",
    "user": "postgres",
    "password": "SoupBoss1"
}

# ============================================================================
# WEB SEARCH CONFIGURATION
# ============================================================================

# DuckDuckGo search (free, no API key needed)
SEARCH_BACKEND = duckduckgo

# Alternative: Tavily (better for agents, requires free API key)
# SEARCH_BACKEND = "tavily"
# TAVILY_API_KEY = "your-api-key-here"  # Get free key at tavily.com

# ============================================================================
# NOTION CONFIGURATION
# ============================================================================

# Get your integration token from: https://www.notion.so/my-integrations
NOTION_API_KEY = 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  # Paste your secret_... token here

# Main pages/databases the agent should know about (optional - can also search)
NOTION_PAGES = {
    agent_registry: https://www.notion.so/2dbead023dee802ba5dad3cc4c9b904d?v=2dbead023dee809b8be2000ce0d374f3&source=copy_link,
    data_sources_registry: https://www.notion.so/2dbead023dee80889f44c4727b9b8f81?v=2dbead023dee808bac53000c2dd27cd4&source=copy_link,
    runbooks: https://www.notion.so/2dbead023dee80379944cdec7a696cb2?v=2dbead023dee80459126000cba3e8611&source=copy_link,
    architecture_decisions: https://www.notion.so/2dbead023dee809cb603f74314330e75?v=2dbead023dee80648a18000c6d03a273&source=copy_link,
    reconciliation_log: https://www.notion.so/2dbead023dee80928368da028b8a5e80?v=2dbead023dee805a959d000c9dcb68fa&source=copy_link,
    content_calendar: https://www.notion.so/2dbead023dee80aaa266f81b3c325777?v=2dbead023dee80019fd0000cb30741eb&source=copy_link,
    content_tasks: https://www.notion.so/2dcead023dee809795a7e6853385273a?v=2dcead023dee8076a9de000c82912f88&source=copy_link,
    content_templates: https://www.notion.so/2dcead023dee809fb7d1c6a9eeb16d2e?v=2dcead023dee804bbf34000cec89709a&source=copy_link,
    distribution_channels: https://www.notion.so/2dcead023dee80d9b1eaecfd77c20895?v=2dcead023dee80f2a27e000c3d1c7937&source=copy_link,
    engagement_log: https://www.notion.so/2dcead023dee80bc9b2add7b6d77ec87?v=2dcead023dee80a8b49d000c327d452d&source=copy_link,
    master_timeline: https://www.notion.so/2dcead023dee801dbedec4a67752801b?v=2dcead023dee8086903c000ce495be3e&source=copy_link,
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
- Search the internet for information and data sources
- Read and write files on the local system
- Query and update the PostgreSQL database
- Execute Python scripts
- Analyze data using pandas
- Access Notion pages and databases (read and update)

**Your Constraints:**
- Always explain your reasoning before taking action
- For sensitive operations (database writes, file changes), request approval
- Log all significant actions
- When uncertain, ask for clarification

**Current Project Context:**
- Database: PostgreSQL with bronze/silver/gold medallion architecture
- Training data: Historical commodity data through 2019/20 marketing year
- Test data: 2021-2025 reserved for validating forecasts
- Key commodities: Soybeans, soybean meal/oil, corn, wheat, canola

When given a task, break it down into steps, explain your approach, then execute."""
