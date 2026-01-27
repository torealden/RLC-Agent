@echo off
REM RLC Agent Scheduler Startup Script
REM This can be used with Windows Task Scheduler for automatic startup

cd /d "C:\RLC-Agent"
echo Starting RLC Agent Scheduler...
echo Working directory: %CD%
echo.
python rlc_scheduler\agent_scheduler.py run
