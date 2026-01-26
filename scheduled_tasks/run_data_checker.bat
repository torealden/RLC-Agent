@echo off
REM RLC Data Checker - Daily 7:00 AM
REM Validates bronze/silver/gold data quality

cd /d C:\RLC-Agent
call C:\Users\torem\AppData\Local\Programs\Python\Python311\python.exe rlc_scheduler\agents\data_checker_agent.py >> logs\data_checker.log 2>&1

exit /b %ERRORLEVEL%
