"""
Report Scheduler Service

Handles automated scheduling of the weekly report generation.
Supports cron-like scheduling with Tuesday execution.
"""

import logging
import time
import threading
from dataclasses import dataclass, field
from datetime import datetime, date, timedelta, time as dt_time
from typing import Optional, Callable, List, Any

from ..config.settings import HBWeeklyReportConfig, SchedulingConfig

logger = logging.getLogger(__name__)


@dataclass
class ScheduledTask:
    """A scheduled task"""
    name: str
    next_run: datetime
    last_run: Optional[datetime] = None
    last_success: bool = False
    run_count: int = 0
    error_count: int = 0
    last_error: Optional[str] = None


class ReportScheduler:
    """
    Scheduler for automated weekly report generation

    Features:
    - Weekly Tuesday execution
    - Configurable execution time
    - Timezone support
    - Retry logic
    - Manual trigger support
    """

    def __init__(self, config: HBWeeklyReportConfig, run_callback: Callable = None):
        """
        Initialize scheduler

        Args:
            config: HB Weekly Report configuration
            run_callback: Callback function to execute report generation
        """
        self.config = config
        self.schedule_config = config.scheduling
        self.run_callback = run_callback
        self.logger = logging.getLogger(f"{self.__class__.__name__}")

        # State
        self._running = False
        self._thread: Optional[threading.Thread] = None
        self._task: Optional[ScheduledTask] = None

        # Initialize task
        self._init_task()

        self.logger.info(
            f"Initialized ReportScheduler: "
            f"day={self.schedule_config.day_of_week}, "
            f"time={self.schedule_config.execution_time}"
        )

    def _init_task(self):
        """Initialize the scheduled task"""
        next_run = self._calculate_next_run()
        self._task = ScheduledTask(
            name="HB Weekly Report",
            next_run=next_run,
        )
        self.logger.info(f"Next scheduled run: {next_run}")

    def _calculate_next_run(self, from_date: datetime = None) -> datetime:
        """
        Calculate next execution time

        Args:
            from_date: Starting point (default: now)

        Returns:
            Next execution datetime
        """
        try:
            import pytz
            tz = pytz.timezone(self.schedule_config.timezone)
        except ImportError:
            self.logger.warning("pytz not installed, using UTC")
            import datetime as dt
            tz = dt.timezone.utc
        except Exception:
            import datetime as dt
            tz = dt.timezone.utc

        now = from_date or datetime.now(tz) if hasattr(datetime.now(), 'tzinfo') else datetime.now()

        # Find next occurrence of scheduled day
        target_weekday = self.schedule_config.day_of_week
        current_weekday = now.weekday()

        days_until_target = (target_weekday - current_weekday) % 7
        if days_until_target == 0:
            # Same day - check if we've passed execution time
            exec_time = self.schedule_config.execution_time
            if now.time() >= exec_time:
                days_until_target = 7  # Next week

        next_date = now.date() + timedelta(days=days_until_target)

        next_run = datetime.combine(next_date, self.schedule_config.execution_time)

        return next_run

    def start(self):
        """Start the scheduler"""
        if self._running:
            self.logger.warning("Scheduler already running")
            return

        self._running = True
        self._thread = threading.Thread(target=self._run_loop, daemon=True)
        self._thread.start()
        self.logger.info("Scheduler started")

    def stop(self):
        """Stop the scheduler"""
        self._running = False
        if self._thread:
            self._thread.join(timeout=5)
        self.logger.info("Scheduler stopped")

    def _run_loop(self):
        """Main scheduler loop"""
        while self._running:
            try:
                now = datetime.now()

                if self._task and now >= self._task.next_run:
                    self._execute_task()

                # Sleep until next check (every minute)
                time.sleep(60)

            except Exception as e:
                self.logger.error(f"Scheduler error: {e}")
                time.sleep(60)

    def _execute_task(self):
        """Execute the scheduled task"""
        self.logger.info(f"Executing scheduled task: {self._task.name}")

        self._task.last_run = datetime.now()
        self._task.run_count += 1

        try:
            if self.run_callback:
                result = self.run_callback(date.today())
                self._task.last_success = result.success if hasattr(result, 'success') else True
                if not self._task.last_success:
                    self._task.error_count += 1
                    self._task.last_error = str(result.errors[0]) if hasattr(result, 'errors') and result.errors else "Unknown error"

                    # Retry logic
                    if self._task.error_count <= self.schedule_config.max_retries:
                        retry_time = datetime.now() + timedelta(hours=self.schedule_config.retry_delay_hours)
                        self.logger.info(f"Scheduling retry at {retry_time}")
                        self._task.next_run = retry_time
                        return
            else:
                self.logger.warning("No callback configured")
                self._task.last_success = False

        except Exception as e:
            self.logger.error(f"Task execution failed: {e}")
            self._task.last_success = False
            self._task.error_count += 1
            self._task.last_error = str(e)

        # Schedule next run
        self._task.next_run = self._calculate_next_run()
        self.logger.info(f"Next scheduled run: {self._task.next_run}")

    def trigger_now(self) -> bool:
        """
        Manually trigger report generation

        Returns:
            True if triggered successfully
        """
        self.logger.info("Manual trigger requested")

        if not self.run_callback:
            self.logger.error("No callback configured")
            return False

        try:
            result = self.run_callback(date.today())
            return result.success if hasattr(result, 'success') else True
        except Exception as e:
            self.logger.error(f"Manual trigger failed: {e}")
            return False

    def get_status(self) -> dict:
        """Get scheduler status"""
        return {
            "running": self._running,
            "task": {
                "name": self._task.name,
                "next_run": self._task.next_run.isoformat() if self._task.next_run else None,
                "last_run": self._task.last_run.isoformat() if self._task.last_run else None,
                "last_success": self._task.last_success,
                "run_count": self._task.run_count,
                "error_count": self._task.error_count,
                "last_error": self._task.last_error,
            } if self._task else None,
            "config": {
                "day_of_week": self.schedule_config.day_of_week,
                "execution_time": str(self.schedule_config.execution_time),
                "timezone": self.schedule_config.timezone,
            }
        }

    def set_next_run(self, next_run: datetime):
        """Manually set next run time"""
        if self._task:
            self._task.next_run = next_run
            self.logger.info(f"Next run set to: {next_run}")


class CronScheduler:
    """
    Alternative scheduler using cron expressions

    For production use, can integrate with system cron or APScheduler
    """

    def __init__(self, config: HBWeeklyReportConfig):
        self.config = config
        self.logger = logging.getLogger(f"{self.__class__.__name__}")

    def get_cron_expression(self) -> str:
        """
        Get cron expression for scheduled execution

        Returns:
            Cron expression string (e.g., "0 6 * * 2" for Tuesday 6:00 AM)
        """
        exec_time = self.config.scheduling.execution_time
        day = self.config.scheduling.day_of_week

        # Cron format: minute hour day_of_month month day_of_week
        return f"{exec_time.minute} {exec_time.hour} * * {day}"

    def install_system_cron(self, script_path: str) -> bool:
        """
        Install system crontab entry (Linux/Mac only)

        Args:
            script_path: Path to the Python script to run

        Returns:
            True if installed successfully
        """
        import subprocess

        cron_expr = self.get_cron_expression()
        cron_line = f"{cron_expr} cd {script_path.rsplit('/', 1)[0]} && python {script_path} generate\n"

        try:
            # Get current crontab
            result = subprocess.run(['crontab', '-l'], capture_output=True, text=True)
            current_crontab = result.stdout if result.returncode == 0 else ""

            # Check if already installed
            if "hb_weekly_report" in current_crontab:
                self.logger.info("Cron entry already exists")
                return True

            # Add new entry
            new_crontab = current_crontab + f"# HB Weekly Report\n{cron_line}"

            # Install
            process = subprocess.Popen(['crontab', '-'], stdin=subprocess.PIPE, text=True)
            process.communicate(input=new_crontab)

            self.logger.info(f"Installed cron: {cron_expr}")
            return True

        except Exception as e:
            self.logger.error(f"Failed to install cron: {e}")
            return False


def setup_apscheduler(config: HBWeeklyReportConfig, run_callback: Callable):
    """
    Set up APScheduler for production scheduling

    Args:
        config: Configuration
        run_callback: Function to call for report generation

    Returns:
        APScheduler BackgroundScheduler instance
    """
    try:
        from apscheduler.schedulers.background import BackgroundScheduler
        from apscheduler.triggers.cron import CronTrigger

        scheduler = BackgroundScheduler(timezone=config.scheduling.timezone)

        exec_time = config.scheduling.execution_time
        day = config.scheduling.day_of_week

        # Map day number to APScheduler day name
        day_names = ['mon', 'tue', 'wed', 'thu', 'fri', 'sat', 'sun']

        scheduler.add_job(
            run_callback,
            CronTrigger(
                day_of_week=day_names[day],
                hour=exec_time.hour,
                minute=exec_time.minute,
            ),
            id='hb_weekly_report',
            name='HB Weekly Report Generator',
            replace_existing=True,
        )

        logger.info(f"APScheduler configured: {day_names[day]} at {exec_time}")

        return scheduler

    except ImportError:
        logger.error("APScheduler not installed. Run: pip install apscheduler")
        return None
