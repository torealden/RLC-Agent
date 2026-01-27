# RLC-Agent Windows Task Scheduler Setup
# Run this script as Administrator to register all scheduled tasks
#
# Usage: Right-click PowerShell -> Run as Administrator
#        cd C:\RLC-Agent\scheduled_tasks
#        .\setup_windows_tasks.ps1

$ErrorActionPreference = "Stop"
$TaskFolder = "C:\RLC-Agent\scheduled_tasks"

Write-Host "========================================" -ForegroundColor Cyan
Write-Host "RLC-Agent Task Scheduler Setup" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""

# Check if running as admin
$isAdmin = ([Security.Principal.WindowsPrincipal] [Security.Principal.WindowsIdentity]::GetCurrent()).IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)
if (-not $isAdmin) {
    Write-Host "ERROR: This script must be run as Administrator!" -ForegroundColor Red
    Write-Host "Right-click PowerShell and select 'Run as Administrator'" -ForegroundColor Yellow
    exit 1
}

# ============================================
# Task 1: Weather Collector (Daily 6:00 AM)
# ============================================
Write-Host "Creating task: RLC Weather Collector..." -ForegroundColor Yellow

$Action = New-ScheduledTaskAction -Execute "$TaskFolder\run_weather_collector.bat"
$Trigger = New-ScheduledTaskTrigger -Daily -At 6:00AM
$Settings = New-ScheduledTaskSettingsSet -StartWhenAvailable -DontStopOnIdleEnd -AllowStartIfOnBatteries

try {
    Unregister-ScheduledTask -TaskName "RLC Weather Collector" -Confirm:$false -ErrorAction SilentlyContinue
} catch {}

Register-ScheduledTask -TaskName "RLC Weather Collector" `
    -Action $Action `
    -Trigger $Trigger `
    -Settings $Settings `
    -Description "Collects daily weather data for agricultural locations" `
    -RunLevel Highest

Write-Host "  Created: RLC Weather Collector (Daily 6:00 AM)" -ForegroundColor Green

# ============================================
# Task 2: Weather Email Agent (3x daily weekdays)
# ============================================
Write-Host "Creating tasks: RLC Weather Email Agent..." -ForegroundColor Yellow

# Morning (7:30 AM weekdays)
$Action = New-ScheduledTaskAction -Execute "$TaskFolder\run_weather_email.bat"
$Trigger = New-ScheduledTaskTrigger -Weekly -DaysOfWeek Monday,Tuesday,Wednesday,Thursday,Friday -At 7:30AM
$Settings = New-ScheduledTaskSettingsSet -StartWhenAvailable -DontStopOnIdleEnd -AllowStartIfOnBatteries

try {
    Unregister-ScheduledTask -TaskName "RLC Weather Email - Morning" -Confirm:$false -ErrorAction SilentlyContinue
} catch {}

Register-ScheduledTask -TaskName "RLC Weather Email - Morning" `
    -Action $Action `
    -Trigger $Trigger `
    -Settings $Settings `
    -Description "Forwards meteorologist emails - Morning" `
    -RunLevel Highest

Write-Host "  Created: RLC Weather Email - Morning (Weekdays 7:30 AM)" -ForegroundColor Green

# Midday (1:00 PM weekdays)
$Trigger = New-ScheduledTaskTrigger -Weekly -DaysOfWeek Monday,Tuesday,Wednesday,Thursday,Friday -At 1:00PM

try {
    Unregister-ScheduledTask -TaskName "RLC Weather Email - Midday" -Confirm:$false -ErrorAction SilentlyContinue
} catch {}

Register-ScheduledTask -TaskName "RLC Weather Email - Midday" `
    -Action $Action `
    -Trigger $Trigger `
    -Settings $Settings `
    -Description "Forwards meteorologist emails - Midday" `
    -RunLevel Highest

Write-Host "  Created: RLC Weather Email - Midday (Weekdays 1:00 PM)" -ForegroundColor Green

# Evening (8:00 PM weekdays)
$Trigger = New-ScheduledTaskTrigger -Weekly -DaysOfWeek Monday,Tuesday,Wednesday,Thursday,Friday -At 8:00PM

try {
    Unregister-ScheduledTask -TaskName "RLC Weather Email - Evening" -Confirm:$false -ErrorAction SilentlyContinue
} catch {}

Register-ScheduledTask -TaskName "RLC Weather Email - Evening" `
    -Action $Action `
    -Trigger $Trigger `
    -Settings $Settings `
    -Description "Forwards meteorologist emails - Evening" `
    -RunLevel Highest

Write-Host "  Created: RLC Weather Email - Evening (Weekdays 8:00 PM)" -ForegroundColor Green

# Saturday (12:00 PM)
$Trigger = New-ScheduledTaskTrigger -Weekly -DaysOfWeek Saturday -At 12:00PM

try {
    Unregister-ScheduledTask -TaskName "RLC Weather Email - Saturday" -Confirm:$false -ErrorAction SilentlyContinue
} catch {}

Register-ScheduledTask -TaskName "RLC Weather Email - Saturday" `
    -Action $Action `
    -Trigger $Trigger `
    -Settings $Settings `
    -Description "Forwards meteorologist emails - Saturday" `
    -RunLevel Highest

Write-Host "  Created: RLC Weather Email - Saturday (12:00 PM)" -ForegroundColor Green

# Sunday Morning (9:00 AM)
$Trigger = New-ScheduledTaskTrigger -Weekly -DaysOfWeek Sunday -At 9:00AM

try {
    Unregister-ScheduledTask -TaskName "RLC Weather Email - Sunday AM" -Confirm:$false -ErrorAction SilentlyContinue
} catch {}

Register-ScheduledTask -TaskName "RLC Weather Email - Sunday AM" `
    -Action $Action `
    -Trigger $Trigger `
    -Settings $Settings `
    -Description "Forwards meteorologist emails - Sunday Morning" `
    -RunLevel Highest

Write-Host "  Created: RLC Weather Email - Sunday AM (9:00 AM)" -ForegroundColor Green

# Sunday Evening (7:00 PM)
$Trigger = New-ScheduledTaskTrigger -Weekly -DaysOfWeek Sunday -At 7:00PM

try {
    Unregister-ScheduledTask -TaskName "RLC Weather Email - Sunday PM" -Confirm:$false -ErrorAction SilentlyContinue
} catch {}

Register-ScheduledTask -TaskName "RLC Weather Email - Sunday PM" `
    -Action $Action `
    -Trigger $Trigger `
    -Settings $Settings `
    -Description "Forwards meteorologist emails - Sunday Evening" `
    -RunLevel Highest

Write-Host "  Created: RLC Weather Email - Sunday PM (7:00 PM)" -ForegroundColor Green

# ============================================
# Task 3: Data Checker (Daily 7:00 AM)
# ============================================
Write-Host "Creating task: RLC Data Checker..." -ForegroundColor Yellow

$Action = New-ScheduledTaskAction -Execute "$TaskFolder\run_data_checker.bat"
$Trigger = New-ScheduledTaskTrigger -Daily -At 7:00AM
$Settings = New-ScheduledTaskSettingsSet -StartWhenAvailable -DontStopOnIdleEnd -AllowStartIfOnBatteries

try {
    Unregister-ScheduledTask -TaskName "RLC Data Checker" -Confirm:$false -ErrorAction SilentlyContinue
} catch {}

Register-ScheduledTask -TaskName "RLC Data Checker" `
    -Action $Action `
    -Trigger $Trigger `
    -Settings $Settings `
    -Description "Validates bronze/silver/gold data quality" `
    -RunLevel Highest

Write-Host "  Created: RLC Data Checker (Daily 7:00 AM)" -ForegroundColor Green

# ============================================
# Summary
# ============================================
Write-Host ""
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "Setup Complete!" -ForegroundColor Green
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""
Write-Host "Tasks created:" -ForegroundColor White
Write-Host "  - RLC Weather Collector (Daily 6:00 AM)" -ForegroundColor White
Write-Host "  - RLC Weather Email - Morning (Weekdays 7:30 AM)" -ForegroundColor White
Write-Host "  - RLC Weather Email - Midday (Weekdays 1:00 PM)" -ForegroundColor White
Write-Host "  - RLC Weather Email - Evening (Weekdays 8:00 PM)" -ForegroundColor White
Write-Host "  - RLC Weather Email - Saturday (12:00 PM)" -ForegroundColor White
Write-Host "  - RLC Weather Email - Sunday AM (9:00 AM)" -ForegroundColor White
Write-Host "  - RLC Weather Email - Sunday PM (7:00 PM)" -ForegroundColor White
Write-Host "  - RLC Data Checker (Daily 7:00 AM)" -ForegroundColor White
Write-Host ""
Write-Host "Logs will be written to: C:\RLC-Agent\logs\" -ForegroundColor Yellow
Write-Host ""
Write-Host "To view tasks: Open Task Scheduler and look for 'RLC' tasks" -ForegroundColor Yellow
Write-Host "To run manually: Right-click task -> Run" -ForegroundColor Yellow
Write-Host ""

# ============================================
# Optional: Agent Scheduler Daemon
# ============================================
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "OPTIONAL: Agent Scheduler Daemon" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""
Write-Host "The agent_scheduler.py is a long-running daemon that handles" -ForegroundColor White
Write-Host "USDA release calendars and holiday-aware scheduling." -ForegroundColor White
Write-Host ""
Write-Host "Options:" -ForegroundColor Yellow
Write-Host "  1. Run at Windows login (recommended for now)" -ForegroundColor White
Write-Host "  2. Convert to Windows Service (advanced)" -ForegroundColor White
Write-Host ""
Write-Host "To set up login trigger, uncomment and run:" -ForegroundColor Yellow
Write-Host '  $Action = New-ScheduledTaskAction -Execute "C:\RLC-Agent\scheduled_tasks\run_agent_scheduler.bat"' -ForegroundColor Gray
Write-Host '  $Trigger = New-ScheduledTaskTrigger -AtLogOn' -ForegroundColor Gray
Write-Host '  Register-ScheduledTask -TaskName "RLC Agent Scheduler" -Action $Action -Trigger $Trigger -RunLevel Highest' -ForegroundColor Gray
Write-Host ""
