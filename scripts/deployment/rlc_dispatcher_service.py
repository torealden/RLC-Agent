"""
RLC Dispatcher Service — Keeps the dispatcher daemon running.

Can be run:
  1. Directly: python scripts/deployment/rlc_dispatcher_service.py
  2. Via Windows Task Scheduler (recommended — see setup_dispatcher_task.ps1)
  3. Via CLI: python -m src.dispatcher start

Manual Run:
  cd C:\\dev\\RLC-Agent
  python scripts/deployment/rlc_dispatcher_service.py

Stop:
  The service writes its PID to scripts/deployment/dispatcher.pid.
  Kill it with: taskkill /PID <pid> /F
"""

import logging
import os
import signal
import sys
import time
from pathlib import Path

# Ensure project root is on the path (3 levels: deployment -> scripts -> project root)
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

# Load environment variables so collectors have API keys
from dotenv import load_dotenv
load_dotenv(PROJECT_ROOT / ".env")

# PID file for management
PID_FILE = Path(__file__).parent / 'dispatcher.pid'
LOG_FILE = Path(__file__).parent / 'dispatcher.log'


def setup_logging():
    """Configure logging to both file and console."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s %(name)s %(levelname)s %(message)s',
        handlers=[
            logging.FileHandler(LOG_FILE, encoding='utf-8'),
            logging.StreamHandler(sys.stdout),
        ],
    )


def write_pid():
    """Write current process PID to file."""
    PID_FILE.write_text(str(os.getpid()))


def cleanup_pid():
    """Remove PID file on exit."""
    if PID_FILE.exists():
        PID_FILE.unlink()


def main():
    setup_logging()
    logger = logging.getLogger('rlc_dispatcher_service')

    # Check for existing instance
    if PID_FILE.exists():
        old_pid = PID_FILE.read_text().strip()
        logger.warning(f"Found existing PID file (pid={old_pid}). Overwriting.")

    write_pid()
    logger.info(f"Dispatcher service starting (pid={os.getpid()})")

    # Import and start dispatcher
    try:
        from src.dispatcher.dispatcher import Dispatcher
    except ImportError as e:
        logger.error(f"Failed to import Dispatcher: {e}")
        logger.error("Make sure you're running from the project root directory.")
        cleanup_pid()
        sys.exit(1)

    dispatcher = Dispatcher()

    # Handle graceful shutdown
    def shutdown_handler(signum, frame):
        logger.info(f"Received signal {signum}, shutting down...")
        dispatcher.stop()
        cleanup_pid()
        sys.exit(0)

    signal.signal(signal.SIGINT, shutdown_handler)
    signal.signal(signal.SIGTERM, shutdown_handler)

    try:
        dispatcher.start()
        logger.info("Dispatcher started. Scheduled jobs are running.")
        logger.info(f"Log file: {LOG_FILE}")
        logger.info(f"PID file: {PID_FILE}")
        logger.info("Press Ctrl+C to stop.")

        # Keep the main thread alive
        while True:
            time.sleep(60)

    except KeyboardInterrupt:
        logger.info("Keyboard interrupt received.")
    except Exception as e:
        logger.error(f"Dispatcher crashed: {e}", exc_info=True)
    finally:
        dispatcher.stop()
        cleanup_pid()
        logger.info("Dispatcher service stopped.")


if __name__ == '__main__':
    main()
