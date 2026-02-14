#!/usr/bin/env python3
"""
RLC Overnight Runner
====================
Simple overnight monitoring script that runs key data collection and report generation.

Runs:
1. Futures settlement collection (5 PM ET daily)
2. HB Weekly Report generation (6 AM ET daily)
3. Logs all activity for review

Usage:
    python scripts/overnight_runner.py          # Run once (all tasks)
    python scripts/overnight_runner.py futures  # Run futures collection only
    python scripts/overnight_runner.py report   # Run HB report only
    python scripts/overnight_runner.py daemon   # Run as background scheduler
"""

import os
import sys
import time
import logging
from datetime import datetime, timedelta
from pathlib import Path

# Setup paths
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(PROJECT_ROOT / "src"))

# Create logs directory
LOG_DIR = PROJECT_ROOT / "output" / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)

# Setup logging
log_file = LOG_DIR / f"overnight_runner_{datetime.now().strftime('%Y%m%d')}.log"
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(log_file)
    ]
)
logger = logging.getLogger('OvernightRunner')


def run_futures_collection():
    """Run futures settlement collection using Yahoo Finance."""
    logger.info("=" * 60)
    logger.info("FUTURES COLLECTION - Starting")
    logger.info("=" * 60)

    try:
        # Import the collector
        from agents.collectors.market.yahoo_futures_collector import (
            fetch_session_data, save_session_to_bronze, SessionType,
            AG_SYMBOLS, ENERGY_SYMBOLS
        )

        symbols = AG_SYMBOLS + ENERGY_SYMBOLS
        logger.info(f"Collecting settlement data for: {symbols}")

        # Fetch and save settlement data
        quotes = fetch_session_data(symbols, SessionType.SETTLEMENT)

        if quotes:
            count = save_session_to_bronze(quotes, SessionType.SETTLEMENT)
            logger.info(f"SUCCESS: Saved {count} settlement quotes to database")

            # Print summary
            for q in quotes:
                logger.info(f"  {q.symbol}: O={q.open:.2f} H={q.high:.2f} L={q.low:.2f} C={q.close:.2f}")

            return {'success': True, 'records': count}
        else:
            logger.warning("No quotes retrieved")
            return {'success': False, 'error': 'No quotes retrieved'}

    except Exception as e:
        logger.error(f"FAILED: Futures collection error: {e}")
        return {'success': False, 'error': str(e)}


def run_hb_report():
    """Run HB Weekly Report generation."""
    logger.info("=" * 60)
    logger.info("HB REPORT GENERATION - Starting")
    logger.info("=" * 60)

    try:
        # Import and run the V2 report generator
        import importlib.util
        spec = importlib.util.spec_from_file_location(
            "hb_report_v2",
            PROJECT_ROOT / "scripts" / "test_hb_report_generation_v2.py"
        )
        hb_module = importlib.util.module_from_spec(spec)

        # Capture the output
        logger.info("Running HB Report Generation V2...")
        spec.loader.exec_module(hb_module)

        # The script runs on import, check for output files
        output_dir = PROJECT_ROOT / "output" / "reports"
        today = datetime.now().strftime("%Y%m%d")

        report_files = list(output_dir.glob(f"HB_Weekly_Report_V2_{today}*.md"))
        if report_files:
            latest_report = max(report_files, key=lambda p: p.stat().st_mtime)
            logger.info(f"SUCCESS: Report generated at {latest_report}")

            # Log report size
            size = latest_report.stat().st_size
            logger.info(f"  Report size: {size:,} bytes")

            return {'success': True, 'report': str(latest_report)}
        else:
            logger.warning("No report file found after generation")
            return {'success': False, 'error': 'No report file generated'}

    except Exception as e:
        logger.error(f"FAILED: Report generation error: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return {'success': False, 'error': str(e)}


def run_all():
    """Run all overnight tasks."""
    logger.info("=" * 70)
    logger.info("OVERNIGHT RUNNER - Starting all tasks")
    logger.info(f"Time: {datetime.now()}")
    logger.info("=" * 70)

    results = {}

    # 1. Futures Collection
    results['futures'] = run_futures_collection()

    # Small delay between tasks
    time.sleep(5)

    # 2. HB Report
    results['hb_report'] = run_hb_report()

    # Summary
    logger.info("=" * 70)
    logger.info("OVERNIGHT RUNNER - Summary")
    logger.info("=" * 70)
    for task, result in results.items():
        status = "SUCCESS" if result.get('success') else "FAILED"
        logger.info(f"  {task}: {status}")
        if not result.get('success'):
            logger.info(f"    Error: {result.get('error', 'Unknown')}")

    return results


def run_log_review():
    """Run the log review agent to check system health."""
    logger.info("=" * 60)
    logger.info("LOG REVIEW - Starting")
    logger.info("=" * 60)

    try:
        from agents.log_review_agent import LogReviewAgent

        agent = LogReviewAgent()
        success = agent.generate_and_send_report(preview_only=False)

        if success:
            logger.info("SUCCESS: Log review report sent")
            return {'success': True}
        else:
            logger.warning("Log review completed but email may not have sent")
            return {'success': True, 'warning': 'Email may not have sent'}

    except Exception as e:
        logger.error(f"FAILED: Log review error: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return {'success': False, 'error': str(e)}


def run_daemon():
    """Run as background scheduler daemon."""
    logger.info("=" * 70)
    logger.info("OVERNIGHT DAEMON - Starting")
    logger.info("Scheduled tasks:")
    logger.info("  - Log Review: 5:30 AM ET daily")
    logger.info("  - HB Report: 6:00 AM ET daily")
    logger.info("  - Futures settlement: 6:00 PM ET daily")
    logger.info("=" * 70)

    try:
        import schedule
    except ImportError:
        import subprocess
        subprocess.check_call([sys.executable, "-m", "pip", "install", "schedule"])
        import schedule

    # Schedule log review at 5:30 AM ET (before HB report)
    schedule.every().day.at("05:30").do(run_log_review)
    logger.info("Scheduled: Log Review at 05:30")

    # Schedule HB report at 6 AM ET
    schedule.every().day.at("06:00").do(run_hb_report)
    logger.info("Scheduled: HB Report at 06:00")

    # Schedule futures collection at 6 PM ET (after settlements)
    schedule.every().day.at("18:00").do(run_futures_collection)
    logger.info("Scheduled: Futures collection at 18:00")

    # Also run a quick test on startup
    logger.info("Running initial futures collection...")
    run_futures_collection()

    logger.info("Daemon running. Press Ctrl+C to stop.")

    try:
        while True:
            schedule.run_pending()
            time.sleep(60)  # Check every minute
    except KeyboardInterrupt:
        logger.info("Daemon stopped by user")


def main():
    if len(sys.argv) < 2:
        # Default: run all
        run_all()
        return

    command = sys.argv[1].lower()

    if command == 'futures':
        run_futures_collection()
    elif command == 'report':
        run_hb_report()
    elif command == 'daemon':
        run_daemon()
    elif command == 'all':
        run_all()
    else:
        print(__doc__)


if __name__ == '__main__':
    main()
