@echo off
cd /d C:\dev\RLC-Agent
python src\tools\generate_cash_prices.py %*
echo.
pause
