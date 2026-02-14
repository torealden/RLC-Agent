@echo off
REM RLC Agent Scheduler - Main daemon (run at login or as service)
REM Manages all USDA release schedules, holiday awareness, etc.

cd /d C:\RLC-Agent
call C:\Users\torem\AppData\Local\Programs\Python\Python311\python.exe rlc_scheduler\agent_scheduler.py >> logs\agent_scheduler.log 2>&1

exit /b %ERRORLEVEL%
