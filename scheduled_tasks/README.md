# RLC-Agent Scheduled Tasks

This folder contains batch files and PowerShell scripts for running RLC-Agent processes via Windows Task Scheduler instead of keeping Claude running.

## Quick Setup

1. **Open PowerShell as Administrator**
   - Right-click Start menu -> "Windows Terminal (Admin)" or "PowerShell (Admin)"

2. **Navigate to this folder**
   ```powershell
   cd C:\RLC-Agent\scheduled_tasks
   ```

3. **Run the setup script**
   ```powershell
   Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser
   .\setup_windows_tasks.ps1
   ```

## Tasks Created

| Task Name | Schedule | Description |
|-----------|----------|-------------|
| RLC Weather Collector | Daily 6:00 AM | Collects weather data for all locations |
| RLC Weather Email - Morning | Weekdays 7:30 AM | Forwards meteorologist emails |
| RLC Weather Email - Midday | Weekdays 1:00 PM | Forwards meteorologist emails |
| RLC Weather Email - Evening | Weekdays 8:00 PM | Forwards meteorologist emails |
| RLC Weather Email - Saturday | Saturday 12:00 PM | Forwards meteorologist emails |
| RLC Weather Email - Sunday AM | Sunday 9:00 AM | Forwards meteorologist emails |
| RLC Weather Email - Sunday PM | Sunday 7:00 PM | Forwards meteorologist emails |
| RLC Data Checker | Daily 7:00 AM | Validates data quality |

## Batch Files

| File | Purpose |
|------|---------|
| run_weather_collector.bat | Runs weather data collection |
| run_weather_email.bat | Runs weather email forwarding |
| run_data_checker.bat | Runs data validation |
| run_agent_scheduler.bat | Runs the main scheduler daemon |

## Logs

All tasks write logs to `C:\RLC-Agent\logs\`:
- `weather_collector.log`
- `weather_email.log`
- `data_checker.log`
- `agent_scheduler.log`

## Managing Tasks

### View Tasks
Open Task Scheduler (taskschd.msc) and look for tasks starting with "RLC"

### Run Manually
Right-click any task -> Run

### Disable/Enable
Right-click any task -> Disable/Enable

### Remove All Tasks
```powershell
Get-ScheduledTask -TaskName "RLC*" | Unregister-ScheduledTask -Confirm:$false
```

## Agent Scheduler Daemon

The `agent_scheduler.py` is a special case - it's a long-running daemon that:
- Tracks USDA release calendars
- Handles federal holiday awareness
- Manages report timing shifts

**Options:**
1. **Run at login** (easiest) - Add to Task Scheduler with AtLogOn trigger
2. **Windows Service** (advanced) - Use NSSM or similar to create a service

For login trigger:
```powershell
$Action = New-ScheduledTaskAction -Execute "C:\RLC-Agent\scheduled_tasks\run_agent_scheduler.bat"
$Trigger = New-ScheduledTaskTrigger -AtLogOn
Register-ScheduledTask -TaskName "RLC Agent Scheduler" -Action $Action -Trigger $Trigger -RunLevel Highest
```

## Troubleshooting

### Task not running?
1. Check logs in `C:\RLC-Agent\logs\`
2. Verify Python path in batch file matches your installation
3. Run batch file manually to see errors

### Python path wrong?
Edit the batch files and update the Python path:
```batch
call C:\Users\torem\AppData\Local\Programs\Python\Python311\python.exe
```
