#Requires -RunAsAdministrator
<#
.SYNOPSIS
    Registers the RLC Dispatcher as a Windows Task Scheduler task.
    The dispatcher starts at user login and runs continuously, firing
    collectors on their configured schedules via APScheduler.

.USAGE
    Run from an elevated PowerShell:
        powershell -ExecutionPolicy Bypass -File scripts\deployment\setup_dispatcher_task.ps1
#>

$TaskName = "RLC Dispatcher"
$TaskFolder = "\RLC\"
$ProjectRoot = "C:\dev\RLC-Agent"
$PythonExe = "C:\Users\torem\AppData\Local\Programs\Python\Python311\python.exe"
$ScriptArgs = "-m src.dispatcher start"
$Description = "RLC-Agent data collection dispatcher - runs APScheduler daemon that fires collectors on schedule"

# Remove existing task if present
$existing = Get-ScheduledTask -TaskName $TaskName -TaskPath $TaskFolder -ErrorAction SilentlyContinue
if ($existing) {
    Write-Host "Removing existing task '$TaskName'..."
    Unregister-ScheduledTask -TaskName $TaskName -TaskPath $TaskFolder -Confirm:$false
}

# Trigger: at user logon
$trigger = New-ScheduledTaskTrigger -AtLogOn -User $env:USERNAME

# Action: python -m src.dispatcher start, working dir = project root
$action = New-ScheduledTaskAction `
    -Execute $PythonExe `
    -Argument $ScriptArgs `
    -WorkingDirectory $ProjectRoot

# Settings
$settings = New-ScheduledTaskSettingsSet `
    -AllowStartIfOnBatteries `
    -DontStopIfGoingOnBatteries `
    -StartWhenAvailable `
    -RestartCount 3 `
    -RestartInterval (New-TimeSpan -Minutes 5) `
    -ExecutionTimeLimit (New-TimeSpan -Days 0)  # No time limit (runs indefinitely)

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
Write-Host "Task '$TaskName' registered successfully." -ForegroundColor Green
Write-Host "  Trigger:    At logon ($env:USERNAME)"
Write-Host "  Action:     $PythonExe $ScriptArgs"
Write-Host "  WorkDir:    $ProjectRoot"
Write-Host "  Restart:    3 attempts, 5 min apart on failure"
Write-Host ""
Write-Host "To start it now without logging out:"
Write-Host "  Start-ScheduledTask -TaskName '$TaskName' -TaskPath '$TaskFolder'"
Write-Host ""
Write-Host "To check status:"
Write-Host "  Get-ScheduledTask -TaskName '$TaskName' -TaskPath '$TaskFolder' | Select-Object State"
