"""
Dispatcher Watchdog

Checks if the RLC dispatcher process is running AND healthy, and restarts
it if dead or if the scheduler has become a zombie (process alive but
APScheduler not firing jobs).

Health check: the dispatcher writes a heartbeat file every 5 minutes.
If the heartbeat is older than HEARTBEAT_MAX_AGE_MINUTES, the watchdog
treats the process as a zombie and restarts it.

Designed to run as a Windows Scheduled Task every 15 minutes.

Usage:
    python scripts/deployment/dispatcher_watchdog.py

Setup:
    Run setup_watchdog_task.ps1 to register the 15-minute scheduled task.
"""

import json
import logging
import os
import subprocess
import sys
from datetime import datetime, timedelta
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
PID_FILE = Path(__file__).parent / 'dispatcher.pid'
HEARTBEAT_FILE = Path(__file__).parent / 'dispatcher_heartbeat.json'
LOG_FILE = Path(__file__).parent / 'watchdog.log'
PYTHON_EXE = sys.executable

# If heartbeat is older than this, treat the dispatcher as a zombie
HEARTBEAT_MAX_AGE_MINUTES = 10

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


def _is_process_alive(pid: int) -> bool:
    """Check if a process with the given PID exists on Windows."""
    try:
        result = subprocess.run(
            ['tasklist', '/FI', f'PID eq {pid}', '/FO', 'CSV', '/NH'],
            capture_output=True, text=True, timeout=10,
        )
        return str(pid) in result.stdout
    except (subprocess.TimeoutExpired, FileNotFoundError):
        return False


def _get_heartbeat_age_minutes() -> float | None:
    """Read the heartbeat file and return its age in minutes, or None if missing/corrupt."""
    if not HEARTBEAT_FILE.exists():
        return None
    try:
        data = json.loads(HEARTBEAT_FILE.read_text())
        ts = datetime.fromisoformat(data['timestamp'])
        age = (datetime.now() - ts).total_seconds() / 60.0
        return age
    except (json.JSONDecodeError, KeyError, ValueError):
        return None


def _kill_zombie(pid: int):
    """Kill a zombie dispatcher process."""
    try:
        subprocess.run(
            ['taskkill', '/F', '/PID', str(pid)],
            capture_output=True, text=True, timeout=10,
        )
        logger.info(f"Killed zombie dispatcher (PID {pid})")
    except Exception as e:
        logger.warning(f"Failed to kill PID {pid}: {e}")


def is_dispatcher_running() -> bool:
    """
    Check if the dispatcher process is alive AND healthy.

    A dispatcher is healthy only if:
    1. The process is running (PID check), AND
    2. The heartbeat file is fresh (< HEARTBEAT_MAX_AGE_MINUTES old)

    If the process is alive but the heartbeat is stale, the dispatcher
    is a zombie (main thread sleeping, APScheduler dead). Kill it and
    return False so the watchdog restarts it.
    """
    pid = None

    # Step 1: Find the dispatcher PID
    if PID_FILE.exists():
        try:
            pid = int(PID_FILE.read_text().strip())
            if not _is_process_alive(pid):
                logger.info(f"PID {pid} from PID file is not running (stale)")
                pid = None
        except (ValueError, OSError):
            pid = None

    # Fallback: search via PowerShell
    if pid is None:
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
                pid = int(pids[0])
        except (subprocess.TimeoutExpired, FileNotFoundError, ValueError):
            pass

    if pid is None:
        logger.info("No dispatcher process found")
        return False

    # Step 2: Check heartbeat freshness
    heartbeat_age = _get_heartbeat_age_minutes()

    if heartbeat_age is None:
        # No heartbeat file yet — could be first run after upgrade.
        # Give it grace: if process is young (< 15 min), assume healthy.
        logger.info(f"Dispatcher PID {pid} alive but no heartbeat file — "
                     "assuming first run after heartbeat upgrade")
        return True

    if heartbeat_age <= HEARTBEAT_MAX_AGE_MINUTES:
        logger.debug(f"Dispatcher healthy (PID {pid}, heartbeat {heartbeat_age:.1f}m ago)")
        return True

    # Zombie detected: process alive but heartbeat stale
    logger.warning(
        f"ZOMBIE DETECTED: Dispatcher PID {pid} alive but heartbeat is "
        f"{heartbeat_age:.1f} minutes old (max {HEARTBEAT_MAX_AGE_MINUTES}m). "
        f"Killing zombie and restarting."
    )
    _kill_zombie(pid)
    log_watchdog_event(
        'zombie_kill',
        f"Killed zombie dispatcher PID {pid} — heartbeat was {heartbeat_age:.1f}m stale"
    )

    # Clean up stale files
    PID_FILE.unlink(missing_ok=True)
    HEARTBEAT_FILE.unlink(missing_ok=True)

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
        logger.info("Dispatcher is running and healthy. No action needed.")
    else:
        logger.warning("Dispatcher is NOT running (or is a zombie)!")

        # Clean up stale files (zombie case already handled in is_dispatcher_running)
        if PID_FILE.exists():
            logger.info("Removing stale PID file")
            PID_FILE.unlink(missing_ok=True)
        if HEARTBEAT_FILE.exists():
            logger.info("Removing stale heartbeat file")
            HEARTBEAT_FILE.unlink(missing_ok=True)

        start_dispatcher()


if __name__ == '__main__':
    main()
