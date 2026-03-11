# Dispatcher & Backfill User Guide

## Overview

The RLC-Agent has two data collection modes:

- **Dispatcher** — Runs continuously in the background, firing collectors on their configured schedules (weekly CFTC, daily futures, monthly WASDE, etc.)
- **Backfill** — One-time manual command to populate historical data for all collectors

---

## Dispatcher

### Starting the Dispatcher

The dispatcher is registered as a Windows Task Scheduler task that auto-starts at login.

**First-time setup** (run as Administrator):
```powershell
powershell -ExecutionPolicy Bypass -File C:\dev\RLC-Agent\scripts\deployment\setup_dispatcher_task.ps1
Start-ScheduledTask -TaskName "RLC Dispatcher" -TaskPath "\RLC\"
```

**Check if it's running:**
```powershell
Get-ScheduledTask -TaskName "RLC Dispatcher" -TaskPath "\RLC\" | Select-Object State
```

**Stop it:**
```powershell
Stop-ScheduledTask -TaskName "RLC Dispatcher" -TaskPath "\RLC\"
```

### Dispatcher CLI Commands

```bash
python -m src.dispatcher start       # Start in foreground (for debugging)
python -m src.dispatcher status      # Show data freshness for all collectors
python -m src.dispatcher list        # List all registered collectors
python -m src.dispatcher schedule    # Show the full weekly schedule
python -m src.dispatcher today       # Show what's scheduled today
python -m src.dispatcher today -x    # Show AND execute today's collectors
python -m src.dispatcher run <name>  # Run a single collector manually
```

### Checking Data Freshness

```bash
python -m src.dispatcher status
```

Shows a table with each collector's last run time, row count, age in hours, and whether it's overdue. The dispatcher also checks for overdue data daily at 8:00 AM ET and logs alerts to the event system.

---

## Backfill

### How It Works

Backfill populates historical data by running collectors with date-range parameters. It uses the same `CollectorRunner` pipeline as the dispatcher, so all runs get:
- Logged to `core.collection_status` and `core.event_log`
- Knowledge Graph enrichment
- Seasonal norm recalculation (for CFTC, crop conditions)

### Tiers

Collectors are organized into 3 priority tiers:

| Tier | Focus | Collectors | Est. Time |
|------|-------|------------|-----------|
| **1** | Prices | `usda_ams_cash_prices`, `eia_petroleum` | 30-60 min |
| **2** | Primary S&D | `cftc_cot`, `usda_fas_export_sales`, `epa_rfs` | ~5 min |
| **3** | Secondary S&D | `usda_nass_crop_progress`, `eia_ethanol`, `census_trade`, `mpob`, `drought_monitor`, `canada_cgc`, `canada_statscan` | ~70 min |

### Basic Commands

```bash
# Preview the plan (always start here)
python -m src.dispatcher backfill --dry-run

# Run all tiers in order (~2-3 hours total)
python -m src.dispatcher backfill

# Run a specific tier
python -m src.dispatcher backfill --tier 2

# Run multiple tiers
python -m src.dispatcher backfill --tier 1,2

# Run specific collectors only
python -m src.dispatcher backfill --collectors cftc_cot eia_ethanol
```

### Recommended Execution Order

Start with the fastest, most critical data first:

```bash
# Step 1: Preview
python -m src.dispatcher backfill --dry-run

# Step 2: Primary S&D (CFTC, export sales, EPA) — ~5 min
python -m src.dispatcher backfill --tier 2

# Step 3: Prices — ~30-60 min
python -m src.dispatcher backfill --tier 1

# Step 4: Secondary S&D — ~70 min
python -m src.dispatcher backfill --tier 3
```

### Resuming an Interrupted Backfill

If you Ctrl+C during a run (or it crashes), progress is saved automatically. Resume where you left off:

```bash
python -m src.dispatcher backfill --resume

# Or resume a specific tier
python -m src.dispatcher backfill --tier 1 --resume

# Check what's already done vs remaining
python -m src.dispatcher backfill --dry-run --resume
```

Progress is tracked in `data/backfill_progress.json`.

### Disaster Recovery

If data goes stale (database restore, outage, etc.), quickly catch up on the last N days:

```bash
# Backfill all collectors for the last 7 days
python -m src.dispatcher backfill --since 7

# Preview first
python -m src.dispatcher backfill --since 7 --dry-run
```

This collapses all 12 collectors into 12 tasks (one per collector) instead of the full 141-task historical plan.

### Controlling Speed

Override the delay between API call chunks:

```bash
# Slower (be gentle with APIs)
python -m src.dispatcher backfill --delay 30

# Faster (if you're confident about rate limits)
python -m src.dispatcher backfill --delay 2
```

### Excluded Collectors

These are intentionally excluded from backfill:

| Collector | Reason |
|-----------|--------|
| `cme_settlements` | CME blocks scraping |
| `futures_*` | Requires IBKR gateway running |
| `usda_wasde` | Use `python scripts/backfill_fas_psd.py --resume` instead |
| `usda_ams_feedstocks` | Text reports, current week only |
| `usda_ams_ddgs` | Text reports, current week only |

### FAS PSD (WASDE) Backfill

WASDE balance sheet data has its own dedicated backfill script with commodity-level progress tracking:

```bash
python scripts/backfill_fas_psd.py --resume
```

---

## Monitoring

### From the CLI

```bash
python -m src.dispatcher status    # Data freshness table
```

### From the Ops Dashboard

```bash
python -m streamlit run dashboards/ops/app.py
```

### From the LLM Briefing

The MCP tools `get_briefing()` and `get_data_freshness()` surface collection events, failures, and overdue alerts in the LLM session.

---

## Troubleshooting

**Dispatcher shows "Ready" instead of "Running":**
- Check `Get-ScheduledTaskInfo` for the LastTaskResult code
- Code `2147942402` = python not found; ensure the full python path is in the .ps1 script

**A collector fails during backfill:**
- It won't be marked as done — `--resume` will retry it next run
- Check logs: `python -m src.dispatcher backfill --tier X --resume -v`

**Want to re-run a completed task:**
- Delete `data/backfill_progress.json` (or remove the specific key from it)
- Or run without `--resume`

**Rate limit errors:**
- Increase delay: `--delay 30`
- Run specific collector alone: `--collectors census_trade`
