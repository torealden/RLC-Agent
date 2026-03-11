@echo off
REM ================================================================
REM  RLC Dispatcher - Quick Start
REM ================================================================
REM  Double-click this file after a reboot to restart the dispatcher.
REM  The dispatcher runs in this window — closing the window stops it.
REM  For background mode, use the Windows Scheduled Task instead
REM  (see setup_dispatcher_task.ps1).
REM ================================================================

cd /d C:\dev\RLC-Agent

echo ================================================================
echo   RLC-Agent Dispatcher
echo ================================================================
echo.
echo   Starting data collection daemon...
echo   This window must stay open for collectors to run.
echo   Press Ctrl+C to stop.
echo.
echo   After a reboot, just double-click this file again.
echo   Or run:  python -m src.dispatcher start
echo.
echo   To set up auto-start on login:
echo     powershell -ExecutionPolicy Bypass -File scripts\deployment\setup_dispatcher_task.ps1
echo.
echo   To set up the watchdog (auto-restart if it dies):
echo     powershell -ExecutionPolicy Bypass -File scripts\deployment\setup_watchdog_task.ps1
echo ================================================================
echo.

python -m src.dispatcher start
pause
