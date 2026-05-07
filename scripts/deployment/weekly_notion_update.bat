@echo off
REM Weekly Notion update: posts a rolling Sat-Fri activity summary to the
REM RLC OS page. Scheduled via Windows Task Scheduler (\RLC\Weekly Notion
REM Update) at 5:00 PM CT every Friday.
REM
REM Why this exists: prior task config invoked `python.exe` directly via
REM the scheduled task Execute field, which failed under rlc-admin
REM (LastResult 0x80070002 — file not found) because python is installed
REM in the torem user profile and isn't on rlc-admin's task-launch PATH.
REM Wrapping in a .bat lets cmd.exe resolve `python` the same way the
REM other working tasks (Market Field Daily, Dispatcher Watchdog) do.

setlocal enabledelayedexpansion
cd /d C:\dev\RLC-Agent

REM Use PowerShell for date (wmic is deprecated on Win11 and returns blank).
for /f "usebackq" %%a in (`powershell -NoProfile -Command "Get-Date -Format yyyy-MM-dd"`) do set TS=%%a
set LOGFILE=logs\notion\weekly_%TS%.log

if not exist logs\notion mkdir logs\notion

echo === Weekly Notion update %TS% === >> %LOGFILE% 2>&1
python scripts\generate_weekly_notion_update.py >> %LOGFILE% 2>&1
echo === Exit code: %ERRORLEVEL% === >> %LOGFILE%

endlocal
