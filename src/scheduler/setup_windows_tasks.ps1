# RLC Scheduler - Windows Task Setup
# Run this script as Administrator to configure scheduled tasks

# Configuration
$SchedulerDir = "C:\Users\torem\rlc_scheduler"
$PythonPath = "python"  # Update if using a specific Python installation

# Task 1: End of Day Notion Export (5:30 PM daily)
$Action1 = New-ScheduledTaskAction -Execute $PythonPath -Argument "daily_activity_log.py export" -WorkingDirectory $SchedulerDir
$Trigger1 = New-ScheduledTaskTrigger -Daily -At "5:30PM"
$Settings1 = New-ScheduledTaskSettingsSet -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries -StartWhenAvailable

Register-ScheduledTask -TaskName "RLC_DailyNotionExport" -Action $Action1 -Trigger $Trigger1 -Settings $Settings1 -Description "Generate daily Notion sync file for Claude Desktop"

Write-Host "Created: RLC_DailyNotionExport (5:30 PM daily)" -ForegroundColor Green

# Task 2: Agent Scheduler Daemon (runs continuously, starts at login)
$Action2 = New-ScheduledTaskAction -Execute $PythonPath -Argument "agent_scheduler.py run" -WorkingDirectory $SchedulerDir
$Trigger2 = New-ScheduledTaskTrigger -AtLogOn
$Settings2 = New-ScheduledTaskSettingsSet -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries -StartWhenAvailable -ExecutionTimeLimit (New-TimeSpan -Days 365)

Register-ScheduledTask -TaskName "RLC_AgentScheduler" -Action $Action2 -Trigger $Trigger2 -Settings $Settings2 -Description "RLC Agent Scheduler - monitors release calendars and triggers data collection"

Write-Host "Created: RLC_AgentScheduler (starts at login)" -ForegroundColor Green

# Task 3: Weekly USDA Export Sales Alert (Thursday 8:25 AM - 5 min before release)
$Action3 = New-ScheduledTaskAction -Execute "powershell" -Argument "-Command `"Write-Host 'USDA Export Sales releasing in 5 minutes!' -ForegroundColor Yellow; [System.Media.SystemSounds]::Exclamation.Play()`""
$Trigger3 = New-ScheduledTaskTrigger -Weekly -DaysOfWeek Thursday -At "8:25AM"

Register-ScheduledTask -TaskName "RLC_USDAExportSalesAlert" -Action $Action3 -Trigger $Trigger3 -Description "Alert 5 minutes before USDA Export Sales release"

Write-Host "Created: RLC_USDAExportSalesAlert (Thursday 8:25 AM)" -ForegroundColor Green

Write-Host "`nAll tasks created successfully!" -ForegroundColor Cyan
Write-Host "View tasks in Task Scheduler or run: Get-ScheduledTask -TaskName 'RLC_*'" -ForegroundColor Cyan
