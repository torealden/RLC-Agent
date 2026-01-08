@echo off
REM RLC Agent Startup Script for Windows
REM Run this to start the persistent agent

echo ============================================
echo RLC Agent - Starting...
echo ============================================

REM Check if Ollama is running
curl -s http://localhost:11434/api/tags >nul 2>&1
if %errorlevel% neq 0 (
    echo Ollama is not running. Starting Ollama...
    start /min ollama serve
    timeout /t 5 /nobreak >nul
)

REM Change to agent directory
cd /d "%~dp0"

REM Start the agent
echo Starting RLC Agent...
python agent.py --interactive

pause
