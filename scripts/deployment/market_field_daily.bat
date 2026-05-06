@echo off
REM Market Field daily pipeline — collects news, classifies, runs sentiment update.
REM
REM Scheduled via Windows Task Scheduler (\RLC\Market Field Daily) at 5:30am CT.
REM
REM Steps:
REM   1. python -m scripts.collect_news_articles
REM        Fetches RSS + Google News for all active sources, dedups, writes
REM        new articles to bronze.news_article.
REM   2. python -m scripts.classify_news_articles
REM        Sends every unclassified article to Claude (mf-v1 prompt) and
REM        writes structured topic/locality/facility data to silver.news_classified.
REM   3. python -m scripts.update_facility_sentiment --as-of <yesterday>
REM        Runs the DeGroot update for one date and persists to
REM        gold.facility_sentiment_daily.
REM
REM Logs to %REPO%\logs\market_field\daily_<YYYY-MM-DD>.log
REM Failures are non-fatal — each step independent so a transient API hiccup
REM doesn't block the next run.

setlocal enabledelayedexpansion
cd /d C:\dev\RLC-Agent

REM Build a YYYY-MM-DD timestamp for the log filename
for /f "tokens=2 delims==" %%a in ('wmic os get localdatetime /value ^| find "="') do set DT=%%a
set TS=%DT:~0,4%-%DT:~4,2%-%DT:~6,2%
set LOGFILE=logs\market_field\daily_%TS%.log

if not exist logs\market_field mkdir logs\market_field

echo === Market Field daily run %TS% === >> %LOGFILE% 2>&1

echo. >> %LOGFILE%
echo --- Step 1: collect news >> %LOGFILE%
python -m scripts.collect_news_articles >> %LOGFILE% 2>&1

echo. >> %LOGFILE%
echo --- Step 2: classify articles >> %LOGFILE%
python -m scripts.classify_news_articles >> %LOGFILE% 2>&1

echo. >> %LOGFILE%
echo --- Step 3: update facility sentiment >> %LOGFILE%
python -m scripts.update_facility_sentiment >> %LOGFILE% 2>&1

echo === Done %TS% === >> %LOGFILE%
endlocal
