@echo off
cd /d C:\dev\RLC-Agent
streamlit run dashboards/market/app.py --server.port 8510
pause
