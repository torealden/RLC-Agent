@echo off
REM RLC Weather Email Agent - 3x daily weekdays (7:30, 13:00, 20:00)
REM Forwards meteorologist emails and generates summaries

cd /d C:\RLC-Agent
call C:\Users\torem\AppData\Local\Programs\Python\Python311\python.exe rlc_scheduler\agents\weather_email_agent.py >> logs\weather_email.log 2>&1

exit /b %ERRORLEVEL%
