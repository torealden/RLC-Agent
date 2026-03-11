#Requires -RunAsAdministrator
<#
.SYNOPSIS
    Registers the Dispatcher Watchdog as a Windows Scheduled Task.
    Runs every 15 minutes to check if the dispatcher is alive and restart if needed.

.USAGE
    powershell -ExecutionPolicy Bypass -File scripts\deployment\setup_watchdog_task.ps1
#>

$TaskName = "RLC Dispatcher Watchdog"
$TaskFolder = "\RLC\"
$ProjectRoot = "C:\dev\RLC-Agent"
$PythonExe = "C:\Users\torem\AppData\Local\Programs\Python\Python311\python.exe"
$ScriptPath = "$ProjectRoot\scripts\deployment\dispatcher_watchdog.py"
$Description = "Checks if RLC dispatcher is alive every 15 minutes and restarts it if dead"

# Remove existing task if present
$existing = Get-ScheduledTask -TaskName $TaskName -TaskPath $TaskFolder -ErrorAction SilentlyContinue
if ($existing) {
    Write-Host "Removing existing task '$TaskName'..."
    Unregister-ScheduledTask -TaskName $TaskName -TaskPath $TaskFolder -Confirm:$false
}

# Trigger: every 15 minutes, starting at system startup
$trigger = New-ScheduledTaskTrigger -Once -At (Get-Date).Date -RepetitionInterval (New-TimeSpan -Minutes 15)

# Action: python dispatcher_watchdog.py
$action = New-ScheduledTaskAction `
    -Execute $PythonExe `
    -Argument $ScriptPath `
    -WorkingDirectory $ProjectRoot

# Settings
$settings = New-ScheduledTaskSettingsSet `
    -AllowStartIfOnBatteries `
    -DontStopIfGoingOnBatteries `
    -StartWhenAvailable `
    -ExecutionTimeLimit (New-TimeSpan -Minutes 5) `
    -MultipleInstances IgnoreNew

# Register the task
Register-ScheduledTask `
    -TaskName $TaskName `
    -TaskPath $TaskFolder `
    -Action $action `
    -Trigger $trigger `
    -Settings $settings `
    -Description $Description `
    -RunLevel Limited

Write-Host ""
Write-Host "Watchdog task '$TaskName' registered successfully." -ForegroundColor Green
Write-Host "  Trigger:    Every 15 minutes"
Write-Host "  Action:     $PythonExe $ScriptPath"
Write-Host ""
Write-Host "To start it now:"
Write-Host "  Start-ScheduledTask -TaskName '$TaskName' -TaskPath '$TaskFolder'"
