# RLC Scheduler System

Automated scheduling system for RLC data collection agents and daily Notion sync.

## Quick Start

### 1. Install Dependencies
```bash
pip install schedule
```

### 2. Setup Windows Scheduled Tasks
Run PowerShell as Administrator:
```powershell
cd C:\Users\torem\rlc_scheduler
.\setup_windows_tasks.ps1
```

This creates:
- **RLC_DailyNotionExport** - 5:30 PM daily export for Claude Desktop
- **RLC_AgentScheduler** - Continuous daemon that triggers agents on schedule
- **RLC_USDAExportSalesAlert** - Thursday 8:25 AM reminder

## Daily Workflow

### Log Activities Throughout the Day
```bash
# Log a new agent
python daily_activity_log.py log "Built Census Trade Agent v2" --category agent --status Live

# Log a lesson learned
python daily_activity_log.py log "Fixed OAuth scope mismatch" --category lesson --status Fixed

# Log an architecture decision
python daily_activity_log.py log "Chose Redis Streams over Kafka" --category decision

# Log a data source
python daily_activity_log.py log "Connected USDA FAS API" --category source --status Active

# View today's log
python daily_activity_log.py show
```

### End of Day Export
```bash
python daily_activity_log.py export
```

This generates:
- `exports/notion_sync_YYYY-MM-DD.json`
- A prompt to copy to Claude Desktop

### After Claude Desktop Syncs
```bash
python daily_activity_log.py clear
```

## Agent Scheduler

### Running the Scheduler
```bash
# Start daemon (runs continuously)
python agent_scheduler.py run

# List all configured schedules
python agent_scheduler.py list

# Show next 10 upcoming jobs
python agent_scheduler.py next

# Manually trigger an agent
python agent_scheduler.py trigger census_trade_agent
```

### Configured Data Sources

| Source | Schedule | Time (ET) | Agent | Notes |
|--------|----------|-----------|-------|-------|
| USDA Export Sales | Thursday* | 8:30 AM | usda_export_sales_agent | Holiday-aware |
| USDA Export Inspections | Monday* | 10:00 AM | usda_export_inspections_agent | Holiday-aware |
| WASDE Report | Specific dates | 12:00 PM | usda_wasde_agent | See 2026 dates below |
| Crop Progress | Monday* (Apr-Nov) | 4:00 PM | usda_crop_progress_agent | Holiday-aware, seasonal |
| Grain Stocks | Quarterly | 12:00 PM | usda_grain_stocks_agent | See schedule below |
| Prospective Plantings | Last biz day Mar | 12:00 PM | usda_plantings_agent | With Mar Grain Stocks |
| Acreage | Last biz day Jun | 12:00 PM | usda_acreage_agent | With Jun Grain Stocks |
| Cattle on Feed | Monthly (~22nd) | 3:00 PM | usda_cattle_agent | |
| Census Trade | Specific dates | 8:30 AM | census_trade_agent | |
| EIA Petroleum | Wednesday* | 10:30 AM | eia_petroleum_agent | Holiday-aware |
| CONAB Brazil | Monthly (~10th) | 9:00 AM | conab_agent | |
| CME Settlements | Weekdays* | 5:00 PM | cme_settlements_agent | Holiday-aware |

*Holiday-aware: Shifts to next business day if federal holiday falls earlier in the week

### Grain Stocks Schedule
- **March**: Last business day (with Prospective Plantings)
- **June**: Last business day (with Acreage)
- **September**: Last day of month
- **December**: Published with January WASDE (same day/time)

### 2026 WASDE Dates
Jan 12, Feb 10, Mar 10, Apr 9, May 12, Jun 11, Jul 10, Aug 12, Sep 11, Oct 9, Nov 10, Dec 10

### Adding New Schedules

Edit `agent_scheduler.py` and add to `USDA_RELEASES` or `OTHER_SCHEDULES`:

```python
"new_source": {
    "name": "New Data Source",
    "schedule": "weekly",  # daily, weekly, monday-friday, monthly, quarterly, annual
    "time": "09:00",
    "agent": "new_source_agent",
    "description": "Description of the data source"
}
```

## File Structure

```
rlc_scheduler/
├── daily_activity_log.py    # Activity logging and Notion export
├── agent_scheduler.py       # Agent scheduling daemon
├── setup_windows_tasks.ps1  # Windows Task Scheduler setup
├── end_of_day_export.bat    # Manual export trigger
├── config/                  # Configuration files
├── logs/                    # Execution logs
│   ├── scheduler.log
│   ├── agent_results.json
│   └── activity_YYYY-MM-DD.json
└── exports/                 # Daily Notion sync files
    └── notion_sync_YYYY-MM-DD.json
```

## Notion Database IDs

| Database | ID |
|----------|-----|
| agent_registry | 2dbead02-3dee-804a-b611-000b7fe5b299 |
| data_sources_registry | 2dbead02-3dee-8062-ae13-000ba10e3beb |
| architecture_decisions | 2dbead02-3dee-802f-a0a7-000b20d183ca |
| runbooks | 2dbead02-3dee-804d-b167-000b11e5f92f |
| lessons_learned | 2e6ead02-3dee-80d1-a7d7-000bf28e86d6 |
| master_timeline | 2dcead02-3dee-80ae-8990-000b75ea7d59 |
| reconciliation_log | 2dbead02-3dee-8050-ae40-000bd8ff835c |

## Troubleshooting

### Task not running?
```powershell
# Check task status
Get-ScheduledTask -TaskName "RLC_*" | Format-Table TaskName, State

# View task history
Get-WinEvent -LogName "Microsoft-Windows-TaskScheduler/Operational" -MaxEvents 20
```

### Agent not found?
The scheduler looks for agents in:
1. `~/commodity_pipeline/<agent_name>/main.py`
2. `~/commodity_pipeline/<agent_name>.py`
3. `~/RLC-Agent/<agent_name>/main.py`

### View scheduler logs
```bash
type logs\scheduler.log
type logs\agent_results.json
```
