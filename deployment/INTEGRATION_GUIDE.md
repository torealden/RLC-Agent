# RLC-Agent Integration with RLC-SERVER

## Overview

This guide explains how to connect the RLC-Agent repository (your agent codebase) with the RLC-SERVER (your Windows machine running the persistent Ollama LLM).

### What You Have

**RLC-SERVER (Windows 11 Desktop):**
```
C:\RLC\
├── models\           # Ollama model storage (qwen2.5:7b, llama3.1:8b, etc.)
├── ollama\           # Ollama config/logs
├── whisper\          # Whisper transcription
│   └── transcripts\  # Voice transcripts (JSON)
├── services\         # NSSM scripts
├── logs\             # Centralized logging
└── projects\         # <-- YOUR AGENT CODE GOES HERE
    └── rlc-agent\    # This repository
```

**RLC-Agent Repository (this codebase):**
- 25+ data collectors (USDA, FGIS, South America, etc.)
- Database schema for commodity data
- Report generation agents
- Master orchestrator framework

---

## Quick Start (3 Steps)

### Step 1: Clone to RLC-SERVER

On the RLC-SERVER, open PowerShell as Administrator:

```powershell
# Navigate to projects folder
cd C:\RLC\projects

# Clone the repository
git clone https://github.com/torealden/RLC-Agent.git rlc-agent

# Run the setup script
cd rlc-agent
.\deployment\setup_rlc_server.ps1
```

### Step 2: Configure API Keys

Edit the `.env` file with your API keys:

```powershell
notepad C:\RLC\projects\rlc-agent\.env
```

Required API keys:
- `NASS_API_KEY` - USDA National Agricultural Statistics
- `EIA_API_KEY` - Energy Information Administration
- `USDA_AMS_API_KEY` - USDA Agricultural Marketing Service

### Step 3: Start the Master Agent

```powershell
cd C:\RLC\projects\rlc-agent
.\.venv\Scripts\Activate.ps1
python deployment/start_agent.py
```

You should see:
```
🔄 Initializing RLC Master Agent...
✅ Connected to Ollama (qwen2.5:7b)
✅ Directories initialized

============================================================
🤖 RLC Master Agent - Interactive Mode
============================================================

Commands:
  Type any request in natural language
  'status'  - Check system status
  'collect' - Run data collection
  'report'  - Generate weekly report
  'voice'   - Process today's voice transcripts
  'quit'    - Exit

You:
```

---

## How It Works

### Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                         RLC-SERVER                                   │
├─────────────────────────────────────────────────────────────────────┤
│                                                                      │
│   ┌─────────────────┐    ┌─────────────────────────────────────┐   │
│   │  Ollama LLM     │    │     RLC Master Agent                │   │
│   │  (qwen2.5:7b)   │◄───│                                     │   │
│   │  Port: 11434    │    │  ┌─────────────────────────────┐    │   │
│   └─────────────────┘    │  │ Data Collection Team        │    │   │
│                          │  │ - USDA Agent                │    │   │
│   ┌─────────────────┐    │  │ - Trade Data Agents         │    │   │
│   │  Whisper        │    │  │ - Export Inspections Agent  │    │   │
│   │  Transcription  │───►│  └─────────────────────────────┘    │   │
│   │  (voice→text)   │    │                                     │   │
│   └─────────────────┘    │  ┌─────────────────────────────┐    │   │
│                          │  │ Database Team               │    │   │
│                          │  │ - SQLite/PostgreSQL         │    │   │
│                          │  │ - Data validation           │    │   │
│                          │  └─────────────────────────────┘    │   │
│                          │                                     │   │
│                          │  ┌─────────────────────────────┐    │   │
│                          │  │ Reporting Team              │    │   │
│                          │  │ - Weekly HB Reports         │    │   │
│                          │  │ - Market analysis           │    │   │
│                          │  └─────────────────────────────┘    │   │
│                          └─────────────────────────────────────┘   │
│                                                                      │
└─────────────────────────────────────────────────────────────────────┘
```

### Data Flow

1. **Voice Input** (optional): Speak into mic → Whisper transcribes → JSON files
2. **Master Agent**: Reads transcripts, interprets commands using local LLM
3. **Data Collection**: Agents pull from USDA, trade sources, etc.
4. **Database Storage**: Validated data stored in SQLite/PostgreSQL
5. **Analysis**: Market trends, trade flows, price analysis
6. **Reports**: Automated weekly reports, presentations

---

## Directory Structure After Deployment

```
C:\RLC\projects\rlc-agent\
├── deployment\              # Deployment scripts
│   ├── start_agent.py       # Main entry point
│   ├── setup_rlc_server.ps1 # Windows setup script
│   └── deploy_to_rlc_server.py
│
├── src\                     # Source code (after consolidation)
│   ├── agents\              # All agent implementations
│   │   ├── collector_agents\
│   │   └── analysis_agents\
│   ├── orchestrators\       # Team orchestrators
│   └── core\                # Scheduler, message bus, etc.
│
├── commodity_pipeline\      # Current working agents
│   ├── usda_ams_agent\      # USDA price collection
│   ├── south_america_trade_data\
│   ├── export_inspections_agent\
│   └── hb_weekly_report_writer\
│
├── data\                    # Local database and exports
│   └── rlc_commodities.db
│
├── config\                  # Configuration files
├── docs\                    # Documentation
└── .env                     # Your API keys (never commit!)
```

---

## Running Specific Tasks

### Collect USDA Data
```powershell
cd C:\RLC\projects\rlc-agent
.\.venv\Scripts\Activate.ps1

# Collect today's USDA AMS data
python -m commodity_pipeline.usda_ams_agent.main daily

# Collect for a specific date
python -m commodity_pipeline.usda_ams_agent.main daily --date 12/18/2025
```

### Generate Weekly Report
```powershell
python -m commodity_pipeline.hb_weekly_report_writer.main
```

### Query the Database
```python
# Python example
from sqlalchemy import create_engine
import pandas as pd

engine = create_engine("sqlite:///data/rlc_commodities.db")
df = pd.read_sql("SELECT * FROM price_data ORDER BY date DESC LIMIT 100", engine)
print(df)
```

---

## Setting Up Automated Scheduling

### Option 1: Windows Task Scheduler (Simple)

Create a scheduled task for daily data collection:

```powershell
# Create task for daily USDA collection at 7 AM
$action = New-ScheduledTaskAction -Execute "powershell.exe" -Argument @"
-ExecutionPolicy Bypass -File C:\RLC\projects\rlc-agent\scripts\daily_collect.ps1
"@
$trigger = New-ScheduledTaskTrigger -Daily -At "07:00"
$settings = New-ScheduledTaskSettingsSet -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries

Register-ScheduledTask -TaskName "RLC-DailyCollect" -Action $action -Trigger $trigger -Settings $settings
```

### Option 2: Master Agent Scheduler (Advanced)

The Master Agent includes a scheduler based on data publication times:

```python
# In start_agent.py
SCHEDULES = {
    'usda_ams_daily': {'time': '06:00', 'frequency': 'daily'},
    'fgis_export': {'day': 'friday', 'time': '11:00'},
    'hb_weekly_report': {'day': 'tuesday', 'time': '09:00'},
}
```

Start with scheduler mode:
```powershell
python deployment/start_agent.py --scheduler-only
```

---

## Voice Command Integration

The Master Agent can process Whisper transcripts for action items:

1. **Whisper Service** saves transcripts to `C:\RLC\whisper\transcripts\`
2. **Master Agent** reads these files and interprets them
3. Commands like "collect ethanol data" or "generate the weekly report" are understood

Example voice workflow:
```
You (speaking): "Hey, I need the latest USDA ethanol data for the report"

[Whisper transcribes to JSON]

Master Agent processes:
- Identifies request: "collect ethanol data"
- Routes to Data Collection Team
- Triggers USDA AMS ethanol report collection
```

---

## Connecting Power BI

Your database can feed Power BI dashboards:

### Direct Database Connection
1. Open Power BI Desktop
2. Get Data → PostgreSQL (or SQLite with ODBC)
3. Connect to `C:\RLC\projects\rlc-agent\data\rlc_commodities.db`

### Scheduled Export
```powershell
# Export data for Power BI
python scripts/create_powerbi_export.py

# Output: C:\RLC\projects\rlc-agent\exports\powerbi_data.csv
```

---

## Troubleshooting

### Ollama Not Responding
```powershell
# Check service status
nssm status OllamaLLM

# Restart if needed
nssm restart OllamaLLM

# Check logs
Get-Content C:\RLC\logs\ollama-stderr.log -Tail 50
```

### Agent Not Starting
```powershell
# Check Python environment
cd C:\RLC\projects\rlc-agent
.\.venv\Scripts\Activate.ps1
python --version  # Should be 3.11+

# Check dependencies
uv pip list

# Reinstall if needed
uv pip install -r requirements.txt
```

### Database Issues
```powershell
# Check database exists
Test-Path C:\RLC\projects\rlc-agent\data\rlc_commodities.db

# Reinitialize if needed
python scripts/init_database.py
```

---

## Next Steps

1. **Configure API Keys**: Edit `.env` with your USDA, EIA keys
2. **Test Data Collection**: Run a single collection to verify setup
3. **Set Up Scheduling**: Create Windows tasks for automated pulls
4. **Connect Power BI**: Build dashboards from the commodity database
5. **Enable Voice**: Start Whisper service for voice commands

---

## Getting Help

- Check logs: `C:\RLC\logs\`
- Repository issues: `https://github.com/torealden/RLC-Agent/issues`
- Documentation: `docs/` folder in this repository
