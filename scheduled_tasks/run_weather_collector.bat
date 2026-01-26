@echo off
REM RLC Weather Collector - Daily 6:00 AM
REM Collects weather data for all agricultural locations

cd /d C:\RLC-Agent
call C:\Users\torem\AppData\Local\Programs\Python\Python311\python.exe rlc_scheduler\agents\weather_collector_agent.py >> logs\weather_collector.log 2>&1

exit /b %ERRORLEVEL%
