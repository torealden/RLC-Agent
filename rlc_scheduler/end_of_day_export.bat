@echo off
REM RLC End of Day Export
REM Runs at scheduled time to generate Notion sync file

cd /d C:\Users\torem\rlc_scheduler
python daily_activity_log.py export

echo.
echo Export complete. Copy the prompt above to Claude Desktop.
pause
