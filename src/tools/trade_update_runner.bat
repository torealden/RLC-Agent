@echo off
REM Trade Update Runner - Called by VBA, runs Python, reopens Excel
REM Arguments: %1 = Excel file path, %2 = Sheet name, %3 = mode (latest/all/months), %4 = months (optional)

set SCRIPT_PATH=C:\Users\torem\RLC Dropbox\RLC Team Folder\RLC-Agent\src\tools\excel_trade_updater.py

echo ============================================
echo Trade Data Updater
echo ============================================
echo File: %~1
echo Sheet: %~2
echo Mode: %~3
echo ============================================

REM Wait a moment for Excel to fully close
timeout /t 2 /nobreak > nul

REM Run Python script
if "%~3"=="latest" (
    python "%SCRIPT_PATH%" --file "%~1" --sheet "%~2" --latest
) else if "%~3"=="all" (
    python "%SCRIPT_PATH%" --file "%~1" --sheet "%~2" --all
) else (
    python "%SCRIPT_PATH%" --file "%~1" --sheet "%~2" --months "%~4"
)

REM Check result
if %ERRORLEVEL% EQU 0 (
    echo.
    echo ============================================
    echo Update complete! Reopening workbook...
    echo ============================================
) else (
    echo.
    echo ============================================
    echo ERROR: Python script failed with code %ERRORLEVEL%
    echo Check the log file for details.
    echo ============================================
    pause
)

REM Reopen the Excel file
start "" "%~1"
