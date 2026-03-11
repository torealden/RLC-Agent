"""
Dispatcher Watchdog

Checks if the RLC dispatcher process is running and restarts it if dead.
Designed to run as a Windows Scheduled Task every 15 minutes.

Usage:
    python scripts/deployment/dispatcher_watchdog.py

Setup:
    Run setup_watchdog_task.ps1 to register the 15-minute scheduled task.
"""

import logging
import os
import subprocess
import sys
from datetime import datetime
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
PID_FILE = Path(__file__).parent / 'dispatcher.pid'
LOG_FILE = Path(__file__).parent / 'watchdog.log'
PYTHON_EXE = sys.executable

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE, encoding='utf-8'),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger('watchdog')


def is_dispatcher_running() -> bool:
    """Check if the dispatcher process is alive."""
    # Method 1: Check PID file (written by both cli.py and rlc_dispatcher_service.py)
    if PID_FILE.exists():
        try:
            pid = int(PID_FILE.read_text().strip())
            # On Windows, check if process with this PID exists
            result = subprocess.run(
                ['tasklist', '/FI', f'PID eq {pid}', '/FO', 'CSV', '/NH'],
                capture_output=True, text=True, timeout=10,
            )
            if str(pid) in result.stdout:
                logger.debug(f"Dispatcher alive (PID {pid})")
                return True
            else:
                logger.info(f"PID {pid} from PID file is not running (stale)")
        except (ValueError, subprocess.TimeoutExpired):
            pass

    # Method 2: Check via PowerShell Get-CimInstance for python running src.dispatcher
    try:
        result = subprocess.run(
            ['powershell', '-NoProfile', '-Command',
             "Get-CimInstance Win32_Process -Filter \"Name='python.exe'\" | "
             "Where-Object { $_.CommandLine -like '*src.dispatcher*' -or "
             "$_.CommandLine -like '*rlc_dispatcher_service*' } | "
             "Select-Object -ExpandProperty ProcessId"],
            capture_output=True, text=True, timeout=15,
        )
        pids = [l.strip() for l in result.stdout.strip().split('\n') if l.strip()]
        if pids:
            logger.debug(f"Dispatcher found via PowerShell (PIDs: {pids})")
            return True
    except (subprocess.TimeoutExpired, FileNotFoundError):
        pass

    return False


def start_dispatcher():
    """Start the dispatcher as a detached background process."""
    logger.info("Starting dispatcher...")

    # Use the service entry point which handles PID file, logging, signals
    service_script = Path(__file__).parent / 'rlc_dispatcher_service.py'

    try:
        # Start detached (CREATE_NEW_PROCESS_GROUP + DETACHED_PROCESS on Windows)
        CREATE_NEW_PROCESS_GROUP = 0x00000200
        DETACHED_PROCESS = 0x00000008

        proc = subprocess.Popen(
            [PYTHON_EXE, str(service_script)],
            cwd=str(PROJECT_ROOT),
            creationflags=CREATE_NEW_PROCESS_GROUP | DETACHED_PROCESS,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )

        logger.info(f"Dispatcher started (PID {proc.pid})")
        log_watchdog_event('restart', f"Watchdog restarted dispatcher (PID {proc.pid})")
        return True

    except Exception as e:
        logger.error(f"Failed to start dispatcher: {e}")
        log_watchdog_event('restart_failed', str(e))
        return False


def log_watchdog_event(action: str, message: str):
    """Log watchdog action to event_log (best effort)."""
    try:
        sys.path.insert(0, str(PROJECT_ROOT))
        from dotenv import load_dotenv
        load_dotenv(PROJECT_ROOT / ".env")
        from src.services.database.db_config import get_connection
        import json

        with get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT core.log_event(%s, %s, %s, %s, %s)",
                ('watchdog_' + action, 'dispatcher_watchdog', message,
                 json.dumps({'action': action, 'timestamp': datetime.now().isoformat()}),
                 2 if action == 'restart' else 4)
            )
            conn.commit()
    except Exception:
        pass  # Watchdog should never crash on logging


def main():
    if is_dispatcher_running():
        logger.info("Dispatcher is running. No action needed.")
    else:
        logger.warning("Dispatcher is NOT running!")

        # Clean up stale PID file
        if PID_FILE.exists():
            logger.info("Removing stale PID file")
            PID_FILE.unlink(missing_ok=True)

        start_dispatcher()


if __name__ == '__main__':
    main()
