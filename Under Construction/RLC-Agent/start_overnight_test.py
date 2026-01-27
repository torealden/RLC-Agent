#!/usr/bin/env python3
"""
RLC Agent - Overnight Test Runner
=================================
Starts the agent system with comprehensive logging for overnight testing.

Usage:
    python start_overnight_test.py

This will:
1. Initialize logging to capture all activity
2. Start the weather email agent on a schedule
3. Log all operations for morning review
"""

import os
import sys
import time
import logging
import schedule
import subprocess
from datetime import datetime
from pathlib import Path

# Setup paths
PROJECT_ROOT = Path(__file__).parent
sys.path.insert(0, str(PROJECT_ROOT))
os.chdir(PROJECT_ROOT)

# Configure comprehensive logging
LOG_DIR = PROJECT_ROOT / "output" / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)

# Main overnight log file
overnight_log = LOG_DIR / f"overnight_test_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s | %(levelname)-8s | %(name)-30s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.FileHandler(overnight_log, encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger("RLC-OvernightTest")

def check_ollama():
    """Verify Ollama is running."""
    try:
        import requests
        response = requests.get("http://localhost:11434/api/tags", timeout=5)
        if response.ok:
            models = response.json().get('models', [])
            logger.info(f"Ollama is running with {len(models)} models")
            for m in models[:3]:
                logger.info(f"  - {m['name']}")
            return True
    except Exception as e:
        logger.error(f"Ollama not available: {e}")
    return False

def check_database():
    """Verify PostgreSQL connection."""
    try:
        import psycopg2
        from dotenv import load_dotenv
        load_dotenv(PROJECT_ROOT / ".env")

        conn = psycopg2.connect(
            host=os.getenv('DB_HOST', 'localhost'),
            port=os.getenv('DB_PORT', '5432'),
            database=os.getenv('DB_NAME', 'rlc_commodities'),
            user=os.getenv('DB_USER', 'postgres'),
            password=os.getenv('DB_PASSWORD', '')
        )
        conn.close()
        logger.info("PostgreSQL connection successful")
        return True
    except Exception as e:
        logger.warning(f"PostgreSQL not available: {e}")
    return False

def run_weather_email_agent():
    """Run the weather email agent."""
    logger.info("=" * 60)
    logger.info("Starting Weather Email Agent run...")
    logger.info("=" * 60)

    try:
        agent_path = PROJECT_ROOT / "src" / "scheduler" / "agents" / "weather_email_agent.py"

        result = subprocess.run(
            [sys.executable, str(agent_path)],
            capture_output=True,
            text=True,
            timeout=1800,  # 30 minute timeout
            cwd=str(PROJECT_ROOT)
        )

        if result.stdout:
            for line in result.stdout.split('\n'):
                if line.strip():
                    logger.info(f"[WEATHER] {line}")

        if result.stderr:
            for line in result.stderr.split('\n'):
                if line.strip():
                    logger.warning(f"[WEATHER-ERR] {line}")

        if result.returncode == 0:
            logger.info("Weather Email Agent completed successfully")
        else:
            logger.error(f"Weather Email Agent exited with code {result.returncode}")

    except subprocess.TimeoutExpired:
        logger.error("Weather Email Agent timed out after 30 minutes")
    except Exception as e:
        logger.error(f"Error running Weather Email Agent: {e}")

def run_weather_collector():
    """Run the weather collector agent."""
    logger.info("=" * 60)
    logger.info("Starting Weather Collector Agent run...")
    logger.info("=" * 60)

    try:
        agent_path = PROJECT_ROOT / "src" / "scheduler" / "agents" / "weather_collector_agent.py"

        result = subprocess.run(
            [sys.executable, str(agent_path)],
            capture_output=True,
            text=True,
            timeout=600,  # 10 minute timeout
            cwd=str(PROJECT_ROOT)
        )

        if result.stdout:
            for line in result.stdout.split('\n'):
                if line.strip():
                    logger.info(f"[COLLECTOR] {line}")

        if result.returncode == 0:
            logger.info("Weather Collector completed successfully")
        else:
            logger.error(f"Weather Collector exited with code {result.returncode}")

    except Exception as e:
        logger.error(f"Error running Weather Collector: {e}")

def run_data_checker():
    """Run the data checker agent."""
    logger.info("=" * 60)
    logger.info("Starting Data Checker Agent run...")
    logger.info("=" * 60)

    try:
        agent_path = PROJECT_ROOT / "src" / "scheduler" / "agents" / "data_checker_agent.py"

        result = subprocess.run(
            [sys.executable, str(agent_path)],
            capture_output=True,
            text=True,
            timeout=600,
            cwd=str(PROJECT_ROOT)
        )

        if result.stdout:
            for line in result.stdout.split('\n'):
                if line.strip():
                    logger.info(f"[CHECKER] {line}")

        if result.returncode == 0:
            logger.info("Data Checker completed successfully")
        else:
            logger.error(f"Data Checker exited with code {result.returncode}")

    except Exception as e:
        logger.error(f"Error running Data Checker: {e}")

def heartbeat():
    """Log a heartbeat to show the system is running."""
    logger.info(f"[HEARTBEAT] System running - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

def run_notion_export():
    """Export daily activity log for Claude Desktop to sync to Notion."""
    logger.info("=" * 60)
    logger.info("Starting Notion Export...")
    logger.info("=" * 60)

    try:
        export_script = PROJECT_ROOT / "src" / "scheduler" / "daily_activity_log.py"

        result = subprocess.run(
            [sys.executable, str(export_script), "export"],
            capture_output=True,
            text=True,
            timeout=120,
            cwd=str(PROJECT_ROOT)
        )

        if result.stdout:
            for line in result.stdout.split('\n'):
                if line.strip():
                    logger.info(f"[NOTION] {line}")

        if result.returncode == 0:
            logger.info("Notion export completed successfully")
        else:
            logger.warning(f"Notion export exited with code {result.returncode}")

    except Exception as e:
        logger.error(f"Error running Notion export: {e}")

def main():
    """Main entry point for overnight testing."""
    logger.info("=" * 70)
    logger.info("RLC AGENT - OVERNIGHT TEST STARTED")
    logger.info(f"Log file: {overnight_log}")
    logger.info(f"Start time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("=" * 70)

    # System checks
    logger.info("\n--- System Checks ---")
    ollama_ok = check_ollama()
    db_ok = check_database()

    if not ollama_ok:
        logger.error("CRITICAL: Ollama not running. Start with: ollama serve")
        return

    # Schedule tasks
    logger.info("\n--- Scheduling Tasks ---")

    # Weather email agent - run at 7:00 AM, 12:00 PM, 6:00 PM
    schedule.every().day.at("07:00").do(run_weather_email_agent)
    schedule.every().day.at("12:00").do(run_weather_email_agent)
    schedule.every().day.at("18:00").do(run_weather_email_agent)
    logger.info("Scheduled: Weather Email Agent at 07:00, 12:00, 18:00")

    # Weather collector - run at 6:00 AM, 12:00 PM, 8:00 PM
    schedule.every().day.at("06:00").do(run_weather_collector)
    schedule.every().day.at("12:00").do(run_weather_collector)
    schedule.every().day.at("20:00").do(run_weather_collector)
    logger.info("Scheduled: Weather Collector at 06:00, 12:00, 20:00")

    # Data checker - run once at 6:30 AM
    schedule.every().day.at("06:30").do(run_data_checker)
    logger.info("Scheduled: Data Checker at 06:30")

    # Heartbeat every 30 minutes
    schedule.every(30).minutes.do(heartbeat)
    logger.info("Scheduled: Heartbeat every 30 minutes")

    # Notion export - run at end of business day (5:30 PM) to capture daily activities
    schedule.every().day.at("17:30").do(run_notion_export)
    logger.info("Scheduled: Notion Export at 17:30")

    # Run weather email agent immediately for testing
    logger.info("\n--- Running Initial Weather Email Check ---")
    run_weather_email_agent()

    logger.info("\n--- Entering Scheduled Loop ---")
    logger.info("Press Ctrl+C to stop")
    logger.info("-" * 70)

    # Show next scheduled runs
    logger.info("Next scheduled runs:")
    for job in sorted(schedule.get_jobs(), key=lambda x: x.next_run):
        logger.info(f"  {job.next_run.strftime('%Y-%m-%d %H:%M')} - {job.job_func.__name__}")

    # Main loop
    try:
        while True:
            schedule.run_pending()
            time.sleep(60)  # Check every minute
    except KeyboardInterrupt:
        logger.info("\n" + "=" * 70)
        logger.info("OVERNIGHT TEST STOPPED BY USER")
        logger.info(f"End time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info(f"Log file: {overnight_log}")
        logger.info("=" * 70)

if __name__ == "__main__":
    main()
