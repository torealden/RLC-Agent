@echo off
REM RLC Agent Daemon Mode
REM Runs the agent continuously in the background

echo ============================================
echo RLC Agent - Daemon Mode
echo ============================================

REM Check if Ollama is running
curl -s http://localhost:11434/api/tags >nul 2>&1
if %errorlevel% neq 0 (
    echo Ollama is not running. Starting Ollama...
    start /min ollama serve
    timeout /t 5 /nobreak >nul
)

cd /d "%~dp0"

echo Starting RLC Agent in daemon mode...
echo Tasks can be submitted to: agent\tasks\
echo Logs are written to: agent\logs\agent.log
echo.
echo Press Ctrl+C to stop the agent.
echo.

python agent.py --daemon
