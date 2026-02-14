"""
RLC Dispatcher Service — Keeps the dispatcher daemon running.

Can be run:
  1. Directly: python deployment/rlc_dispatcher_service.py
  2. Via Windows Task Scheduler (recommended)
  3. As a background process

Windows Task Scheduler Setup:
  1. Open Task Scheduler
  2. Create Basic Task: "RLC Dispatcher"
  3. Trigger: "When the computer starts"
  4. Action: Start a program
     - Program: python
     - Arguments: deployment/rlc_dispatcher_service.py
     - Start in: C:\\Users\\torem\\RLC Dropbox\\RLC Team Folder\\RLC-Agent
  5. In Properties > Settings:
     - Check "Run whether user is logged on or not"
     - Check "Restart the task if it fails" every 5 minutes, up to 3 times
     - Do NOT check "Stop the task if it runs longer than..."

Manual Run:
  cd "C:\\Users\\torem\\RLC Dropbox\\RLC Team Folder\\RLC-Agent"
  python deployment/rlc_dispatcher_service.py

Stop:
  The service writes its PID to deployment/dispatcher.pid.
  Kill it with: taskkill /PID <pid> /F
"""

import logging
import os
import signal
import sys
import time
from pathlib import Path

# Ensure project root is on the path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

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
