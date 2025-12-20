# RLC-Agent Consolidation Plan

## Executive Summary

After comprehensive analysis, I've found that the RLC-Agent repository contains **three separate agent systems** that don't communicate with each other, plus **100+ duplicate files** in the Other Files directory. This plan consolidates everything so your Desktop LLM can access all capabilities.

---

## Current State: Three Isolated Systems

### 1. `rlc_master_agent/` - Most Complete (5,500 LOC)
**Status:** Production-ready but NOT connected to Desktop LLM

**Has:**
- Email Agent (Gmail - read, send, organize)
- Calendar Agent (Google Calendar - full CRUD)
- Memory Manager (Notion integration + local fallback)
- Data Agent (USDA, Census Bureau, Weather)
- Verification Agent (quality checks)
- Approval Manager (3 autonomy levels)
- Proper settings/configuration system

**Missing:**
- Tool-calling architecture (uses sub-agent delegation instead)
- RAG document search
- Connection to commodity_pipeline

### 2. `deployment/` - Currently Active on Desktop LLM (2,747 LOC)
**Status:** What you're running on RLC-SERVER

**Has:**
- Tool-calling agent (start_agent.py)
- 17 tools in agent_tools.py:
  - File operations (read, write, list)
  - Database queries
  - Data collectors
  - Web search
  - Email/Calendar (NEW - just added)
  - RAG document search (NEW - just added)
- setup_google_oauth.py
- document_rag.py

**Missing:**
- Notion memory integration
- Advanced autonomy controls
- Sub-agent architecture

### 3. `commodity_pipeline/` - Data Collection Engine (42,407 LOC)
**Status:** Comprehensive but isolated

**Has:**
- 25 data collectors (USDA, South America, Eurostat, etc.)
- HB Weekly Report Writer (multi-agent report generation)
- South America Trade Data agents
- Export Inspections Agent
- Database layer (SQLite/PostgreSQL)
- Scheduling system

**Missing:**
- Any connection to the other two systems
- Cannot be called from Desktop LLM

---

## Problem: Desktop LLM Cannot Access Most Capabilities

```
What Desktop LLM CAN access:          What EXISTS but LLM CANNOT access:
────────────────────────────────      ─────────────────────────────────────
✅ 17 basic tools                     ❌ 25 commodity data collectors
✅ File read/write                    ❌ Notion long-term memory
✅ Web search                         ❌ HB Weekly Report Writer
✅ Database queries                   ❌ South America trade agents
✅ Email/Calendar (newly added)       ❌ Export Inspections Agent
✅ RAG document search (newly added)  ❌ Verification/Approval systems
                                      ❌ Business process wiki
                                      ❌ Interaction logging
```

---

## Consolidation Plan

### Phase 1: Integrate Notion Memory (This Week)

Add Notion tools to `deployment/agent_tools.py`:

```python
# New tools to add:
- store_memory       # Save facts, preferences, observations
- recall_memories    # Search past memories
- log_interaction    # Track conversations
- get_process        # Retrieve business processes
- list_processes     # See all documented processes
```

**Files to merge:**
- Copy relevant code from `rlc_master_agent/memory_manager.py`
- Add `notion-client` to requirements.txt

### Phase 2: Connect All Data Collectors (This Week)

Current `list_collectors` tool only shows 7 collectors. Expand to all 25:

```python
# Enhanced collector registry - all 25 collectors:
COLLECTORS = {
    # USDA
    "usda_ams": "USDA AMS - Daily grain/livestock reports",
    "usda_fas": "USDA FAS - Foreign agricultural data",
    "usda_nass": "USDA NASS - Agricultural statistics",
    "usda_ers": "USDA ERS - Economic research data",

    # South America
    "conab": "CONAB Brazil - Crop estimates",
    "abiove": "ABIOVE Brazil - Soy processing",
    "imea": "IMEA - Mato Grosso exports",
    "argentina_indec": "Argentina trade statistics",
    "paraguay_censo": "Paraguay agricultural data",
    "uruguay_dna": "Uruguay livestock/grain data",
    "colombia_trade": "Colombia export data",

    # Global
    "fao_stat": "FAO - Global food statistics",
    "eurostat": "European Union statistics",
    "mpob": "Malaysia Palm Oil Board",

    # Energy
    "eia_ethanol": "EIA - Ethanol production",
    "eia_petroleum": "EIA - Petroleum data",
    "epa_rfs": "EPA - Renewable fuel standards",

    # Futures/Trading
    "cme_settlements": "CME - Daily settlements",
    "cftc_cot": "CFTC - Commitment of traders",
    "canada_cgc": "Canada Grain Commission",
    "canada_statscan": "Statistics Canada",
    "drought_monitor": "US Drought Monitor",

    # Trade
    "census_trade": "US Census - Trade data",
    "fgis_inspections": "FGIS - Export inspections"
}
```

### Phase 3: Add Report Generation Tools (Next Week)

Make HB Weekly Report Writer accessible:

```python
# New tools:
- generate_weekly_report   # Create HB-style weekly report
- get_report_status        # Check report generation progress
- list_report_templates    # Available report formats
```

### Phase 4: Add Trading/Analysis Tools (Next 2 Weeks)

Based on research, add commodity-specific tools:

```python
# New analysis tools:
- calculate_basis          # Cash - Futures spread
- analyze_spread          # Inter-commodity spreads
- detect_price_anomaly    # Unusual price movements
- get_seasonal_pattern    # Historical seasonality
- calculate_crush_margin  # Soybean/corn processing margins
```

---

## Files to Archive/Delete

### Move to `Other Files/Archive/`:
These are duplicates or deprecated - the functionality exists in better form elsewhere:

```
Other Files/Desktop Assistant/      → Archive (25 files - duplicates of rlc_master_agent)
Other Files/Email and Calendar Set Up/ → Archive (15 files - duplicates)
Other Files/api Manager/            → Archive (duplicate + security issue with API keys)
Other Files/commodity_pipeline/     → Archive (subset duplicate)
```

### Keep in `Other Files/`:
```
Other Files/Planning/               → Keep (documentation)
Other Files/HB Report Samples/      → Keep (examples)
Other Files/Utilities/              → Keep (may have useful OAuth debug tools)
```

---

## Security Issue: URGENT

**File: `api Manager/.env`** contains real API keys:
- USDA AMS API key
- US Census API key
- Quick Stats API key
- Dropbox token
- SMTP credentials

**Action:**
1. Rotate all these API keys immediately
2. Delete this file from git history
3. Add to .gitignore

---

## Recommended Architecture

### After Consolidation:

```
/deployment/
├── start_agent.py          # Main entry point (enhanced)
├── agent_tools.py          # 30+ tools (expanded)
├── document_rag.py         # RAG system (existing)
├── memory_tools.py         # NEW - Notion integration
├── trading_tools.py        # NEW - Analysis tools
├── report_tools.py         # NEW - Report generation
├── setup_google_oauth.py   # OAuth setup (existing)
└── setup_notion.py         # NEW - Notion setup

/commodity_pipeline/
└── (keep as-is, tools call into it)

/rlc_master_agent/
└── (keep as reference, code merged into deployment/)

/data/
├── tokens/                 # OAuth tokens
├── document_index.json     # RAG index
├── memory_cache.json       # Local memory fallback
└── rlc_commodities.db      # SQLite database
```

### Tool Categories for Desktop LLM:

```
File Operations (4):        Data Collection (25):
  - read_file                - usda_ams, usda_fas, ...
  - write_file               - conab, abiove, ...
  - list_directory           - cme_settlements, ...
  - search_documents

Email (3):                  Calendar (2):
  - check_email              - check_calendar
  - get_email_content        - get_todays_schedule
  - send_email (future)

Memory (5):                 Analysis (5):
  - store_memory             - calculate_basis
  - recall_memories          - analyze_spread
  - log_interaction          - detect_anomaly
  - get_process              - get_seasonal
  - list_processes           - calculate_margin

Reports (3):                System (3):
  - generate_report          - get_system_status
  - get_report_status        - run_python_code
  - list_templates           - query_database
```

**Total: ~50 tools** (up from current 17)

---

## Notion Database Structure

For long-term memory, set up these Notion databases:

### 1. Memory Database
Properties:
- Name (title) - Brief summary
- Type (select) - preference, fact, decision, observation, process, contact, feedback
- Category (select) - trading, ranch, analytics, communications, scheduling, general
- Content (rich text) - Full content
- Source (rich text) - Where this came from
- Confidence (number) - 0-1 confidence score
- Created (date) - When stored

### 2. Interactions Database
Properties:
- Name (title) - User input summary
- Date (date) - When interaction occurred
- User Input (rich text) - What user asked
- Agent Response (rich text) - What agent did
- Actions (multi-select) - Actions taken
- Tools Used (multi-select) - Tools called
- Success (checkbox) - Did it work
- Feedback (rich text) - User feedback

### 3. Business Processes Wiki
Properties:
- Name (title) - Process name
- Category (select) - trading, data, reporting, scheduling
- Frequency (select) - daily, weekly, monthly, as-needed
- Automation Status (select) - manual, partial, full
- Last Run (date) - When last executed
- Steps (page content) - Bulleted list of steps

---

## Implementation Priority

### This Week:
1. ✅ Email/Calendar tools (DONE)
2. ✅ RAG document search (DONE)
3. ⏳ Notion memory integration
4. ⏳ Expand collector registry to 25

### Next Week:
5. Add trading analysis tools
6. Connect HB Report Writer
7. Clean up duplicates in Other Files

### Following Weeks:
8. Upgrade to Qwen2.5:14b or 32b
9. Implement memory tiers (short/medium/long-term)
10. Add backtesting tools

---

## Research Findings

### Model Recommendations
Your current Qwen2.5:7b is good, but for trading analysis consider:
- **Qwen2.5:14b** - Better reasoning, still fits on RTX 5080
- **Qwen2.5:32b** - With 4-bit quantization, excellent for analysis
- **FinGPT** - Specialized for financial analysis

### Memory Strategy (from research)
Implement tiered memory like trading firms:
- **Short-term (14 days):** Recent prices, positions, news
- **Medium-term (90 days):** Seasonal patterns, trends
- **Long-term (365+ days):** Historical patterns, year-over-year

### Similar Projects
- **TradingAgents** - Multi-agent trading framework (highly relevant)
- **FinGPT** - Open-source financial LLM
- **n8n + Ollama** - Workflow automation with Notion

---

## Action Items for Today

When you return from Freddie's walk:

1. **Pull latest changes:**
   ```powershell
   cd C:\RLC\projects\rlc-agent
   git pull origin main
   ```

2. **Install new dependencies:**
   ```powershell
   .\.venv\Scripts\Activate.ps1
   uv pip install notion-client pypdf google-api-python-client google-auth-oauthlib
   ```

3. **Build document index:**
   ```powershell
   python deployment/document_rag.py --index
   ```

4. **Set up Google OAuth:**
   ```powershell
   python deployment/setup_google_oauth.py
   ```

5. **Test the agent:**
   ```powershell
   python deployment/start_agent.py
   ```
   Try: "Check my email" or "Search documents for soybean balance sheet"

---

## Summary

Your repository has excellent components but they're not connected. The key insight is:

> **You have 42,407 lines of commodity data collection code that your Desktop LLM cannot access.**

This consolidation plan fixes that by:
1. Expanding tools from 17 to ~50
2. Adding Notion for long-term memory
3. Connecting all 25 data collectors
4. Cleaning up 100+ duplicate files
5. Creating a unified architecture

The goal is to make your LLM a true business partner that can:
- Remember your analysis methods (Notion)
- Access all commodity data (collectors)
- Search your Excel models (RAG)
- Check email/calendar (Google)
- Generate reports (HB Writer)
- Analyze markets (trading tools)
