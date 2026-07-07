@echo off
REM Called by the "Update WASDE Data" button in us_wheat_balance_sheet.xlsm.
REM Runs the collector against the sandbox DB, then regenerates
REM us_wheat_wasde.xlsx. Exits non-zero on failure -- caller must check.

set FAS_API_KEY=YweddXaIkadiETDHYVckOyIbznenAWCWsn7rZZKm
set RLC_PG_DATABASE=rlc_commodities_sandbox
set RLC_PG_PASSWORD=SoupBoss1

cd /d "C:\Users\adm\Documents\GitHub\RLC-Agent"
python scripts\collectors\run_wheat_wasde_update.py
exit /b %errorlevel%
