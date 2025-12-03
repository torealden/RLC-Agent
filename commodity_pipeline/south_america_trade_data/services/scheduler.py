"""
Trade Data Scheduler

Manages scheduled data pulls based on official release calendars:
- Brazil: ~5th-10th of month (for previous month)
- Argentina: Mid-month (~15th)
- Colombia: Mid-month (~15th)
- Uruguay: Mid-month (~15th)
- Paraguay/WITS: 1-2 month lag
"""

import logging
import time
import threading
from dataclasses import dataclass, field
from datetime import datetime, date, timedelta
from typing import Dict, List, Optional, Callable, Any
from enum import Enum

logger = logging.getLogger(__name__)


class ScheduleFrequency(Enum):
    """Schedule frequency options"""
    DAILY = "daily"
    WEEKLY = "weekly"
    MONTHLY = "monthly"


@dataclass
class ScheduledTask:
    """A scheduled data pull task"""
    task_id: str
    country_code: str
    description: str
    frequency: ScheduleFrequency
    day_of_month: int  # For monthly schedules
    day_of_week: int = 0  # For weekly schedules (0=Monday)
    hour: int = 8  # Hour to run (24h format)
    minute: int = 0
    enabled: bool = True
    last_run: Optional[datetime] = None
    last_success: Optional[datetime] = None
    next_run: Optional[datetime] = None
    consecutive_failures: int = 0


@dataclass
class ScheduleConfig:
    """Configuration for a country's release schedule"""
    country_code: str
    release_day_of_month: int
    release_lag_months: int
    timezone: str = "America/Sao_Paulo"
    retry_days: List[int] = field(default_factory=lambda: [1, 2, 3])  # Retry offsets


# Default release schedules
DEFAULT_SCHEDULES = {
    'BRA': ScheduleConfig(
        country_code='BRA',
        release_day_of_month=8,  # Brazil releases around 5th-10th
        release_lag_months=1,
    ),
    'ARG': ScheduleConfig(
        country_code='ARG',
        release_day_of_month=15,  # Argentina mid-month
        release_lag_months=1,
    ),
    'COL': ScheduleConfig(
        country_code='COL',
        release_day_of_month=15,  # Colombia mid-month
        release_lag_months=1,
    ),
    'URY': ScheduleConfig(
        country_code='URY',
        release_day_of_month=15,  # Uruguay mid-month
        release_lag_months=1,
    ),
    'PRY': ScheduleConfig(
        country_code='PRY',
        release_day_of_month=20,  # Paraguay later (WITS lag)
        release_lag_months=2,
    ),
}


class TradeDataScheduler:
    """
    Scheduler for automated trade data pulls

    Features:
    - Cron-like scheduling based on release calendars
    - Automatic retry on failure
    - Incremental tracking (only pull new data)
    - Configurable per-country schedules
    """

    def __init__(self, orchestrator=None, schedules: Dict[str, ScheduleConfig] = None):
        """
        Initialize scheduler

        Args:
            orchestrator: TradeDataOrchestrator instance
            schedules: Custom schedule configurations
        """
        self.orchestrator = orchestrator
        self.schedules = schedules or DEFAULT_SCHEDULES
        self.tasks: Dict[str, ScheduledTask] = {}
        self._running = False
        self._thread: Optional[threading.Thread] = None

        # Initialize tasks from schedules
        self._initialize_tasks()

    def _initialize_tasks(self):
        """Create scheduled tasks from configurations"""
        for country_code, schedule in self.schedules.items():
            task_id = f"monthly_{country_code.lower()}"

            self.tasks[task_id] = ScheduledTask(
                task_id=task_id,
                country_code=country_code,
                description=f"Monthly data pull for {country_code}",
                frequency=ScheduleFrequency.MONTHLY,
                day_of_month=schedule.release_day_of_month,
                hour=8,
                minute=0,
                enabled=True,
            )

            # Calculate next run
            self._calculate_next_run(self.tasks[task_id])

    def _calculate_next_run(self, task: ScheduledTask):
        """Calculate next run time for a task"""
        now = datetime.now()

        if task.frequency == ScheduleFrequency.MONTHLY:
            # Find next occurrence of the specified day
            if now.day < task.day_of_month:
                # Run this month
                next_run = datetime(
                    now.year, now.month, task.day_of_month,
                    task.hour, task.minute
                )
            else:
                # Run next month
                if now.month == 12:
                    next_run = datetime(
                        now.year + 1, 1, task.day_of_month,
                        task.hour, task.minute
                    )
                else:
                    next_run = datetime(
                        now.year, now.month + 1, task.day_of_month,
                        task.hour, task.minute
                    )

            task.next_run = next_run

        elif task.frequency == ScheduleFrequency.WEEKLY:
            days_ahead = task.day_of_week - now.weekday()
            if days_ahead <= 0:
                days_ahead += 7

            next_run = now + timedelta(days=days_ahead)
            next_run = next_run.replace(
                hour=task.hour, minute=task.minute, second=0, microsecond=0
            )
            task.next_run = next_run

        elif task.frequency == ScheduleFrequency.DAILY:
            next_run = now.replace(
                hour=task.hour, minute=task.minute, second=0, microsecond=0
            )
            if next_run <= now:
                next_run += timedelta(days=1)
            task.next_run = next_run

    def get_data_period_for_run(self, country_code: str, run_date: date = None) -> tuple:
        """
        Calculate which period to fetch based on release schedule

        Args:
            country_code: Country code
            run_date: Date of run (default: today)

        Returns:
            Tuple of (year, month) to fetch
        """
        run_date = run_date or date.today()
        schedule = self.schedules.get(country_code)

        if not schedule:
            # Default: previous month
            if run_date.month == 1:
                return run_date.year - 1, 12
            return run_date.year, run_date.month - 1

        # Account for release lag
        target_date = run_date - timedelta(days=30 * schedule.release_lag_months)

        return target_date.year, target_date.month

    def run_task(self, task_id: str) -> Dict:
        """
        Execute a scheduled task

        Args:
            task_id: ID of task to run

        Returns:
            Dictionary with task results
        """
        task = self.tasks.get(task_id)
        if not task:
            return {'success': False, 'error': f'Task not found: {task_id}'}

        if not task.enabled:
            return {'success': False, 'error': 'Task is disabled'}

        logger.info(f"Running task: {task_id}")

        task.last_run = datetime.now()

        try:
            # Calculate period to fetch
            year, month = self.get_data_period_for_run(task.country_code)

            # Run the orchestrator if available
            if self.orchestrator:
                result = self.orchestrator.run_monthly_pipeline(
                    year=year,
                    month=month,
                    countries=[task.country_code]
                )

                if result.success:
                    task.last_success = datetime.now()
                    task.consecutive_failures = 0
                else:
                    task.consecutive_failures += 1

                # Calculate next run
                self._calculate_next_run(task)

                return {
                    'success': result.success,
                    'task_id': task_id,
                    'country': task.country_code,
                    'period': f"{year}-{month:02d}",
                    'records_loaded': result.total_records_loaded,
                    'errors': result.total_errors,
                }
            else:
                return {
                    'success': False,
                    'error': 'No orchestrator configured',
                }

        except Exception as e:
            logger.error(f"Task {task_id} failed: {e}")
            task.consecutive_failures += 1
            self._calculate_next_run(task)

            return {
                'success': False,
                'task_id': task_id,
                'error': str(e),
            }

    def get_pending_tasks(self) -> List[ScheduledTask]:
        """Get tasks that are due to run"""
        now = datetime.now()
        pending = []

        for task in self.tasks.values():
            if task.enabled and task.next_run and task.next_run <= now:
                pending.append(task)

        return pending

    def start(self, check_interval: int = 60):
        """
        Start the scheduler in a background thread

        Args:
            check_interval: Seconds between checks for pending tasks
        """
        if self._running:
            logger.warning("Scheduler already running")
            return

        self._running = True
        self._thread = threading.Thread(
            target=self._scheduler_loop,
            args=(check_interval,),
            daemon=True
        )
        self._thread.start()
        logger.info("Scheduler started")

    def stop(self):
        """Stop the scheduler"""
        self._running = False
        if self._thread:
            self._thread.join(timeout=5)
        logger.info("Scheduler stopped")

    def _scheduler_loop(self, check_interval: int):
        """Main scheduler loop"""
        while self._running:
            try:
                pending = self.get_pending_tasks()

                for task in pending:
                    logger.info(f"Executing pending task: {task.task_id}")
                    result = self.run_task(task.task_id)
                    logger.info(f"Task result: {result}")

            except Exception as e:
                logger.error(f"Scheduler error: {e}")

            time.sleep(check_interval)

    def get_schedule_status(self) -> Dict[str, Any]:
        """Get status of all scheduled tasks"""
        return {
            'running': self._running,
            'tasks': {
                task_id: {
                    'country': task.country_code,
                    'description': task.description,
                    'enabled': task.enabled,
                    'last_run': str(task.last_run) if task.last_run else None,
                    'last_success': str(task.last_success) if task.last_success else None,
                    'next_run': str(task.next_run) if task.next_run else None,
                    'consecutive_failures': task.consecutive_failures,
                }
                for task_id, task in self.tasks.items()
            }
        }

    def add_task(
        self,
        task_id: str,
        country_code: str,
        frequency: ScheduleFrequency = ScheduleFrequency.MONTHLY,
        day_of_month: int = 15,
        hour: int = 8
    ):
        """Add a new scheduled task"""
        task = ScheduledTask(
            task_id=task_id,
            country_code=country_code,
            description=f"Custom task for {country_code}",
            frequency=frequency,
            day_of_month=day_of_month,
            hour=hour,
        )
        self._calculate_next_run(task)
        self.tasks[task_id] = task
        logger.info(f"Added task: {task_id}")

    def enable_task(self, task_id: str):
        """Enable a task"""
        if task_id in self.tasks:
            self.tasks[task_id].enabled = True
            self._calculate_next_run(self.tasks[task_id])
            logger.info(f"Enabled task: {task_id}")

    def disable_task(self, task_id: str):
        """Disable a task"""
        if task_id in self.tasks:
            self.tasks[task_id].enabled = False
            logger.info(f"Disabled task: {task_id}")


# =============================================================================
# CRON EXPRESSION HELPER
# =============================================================================

def generate_cron_expression(schedule: ScheduleConfig) -> str:
    """
    Generate cron expression for a schedule

    Returns:
        Cron expression string (minute hour day-of-month month day-of-week)
    """
    # Default to 8:00 AM on release day
    return f"0 8 {schedule.release_day_of_month} * *"


def get_recommended_cron_schedules() -> Dict[str, str]:
    """
    Get recommended cron schedules for all countries

    Returns:
        Dictionary mapping country codes to cron expressions
    """
    return {
        'BRA': "0 8 8 * *",    # Brazil: 8th of each month at 8 AM
        'ARG': "0 8 15 * *",   # Argentina: 15th of each month at 8 AM
        'COL': "0 8 15 * *",   # Colombia: 15th of each month at 8 AM
        'URY': "0 8 15 * *",   # Uruguay: 15th of each month at 8 AM
        'PRY': "0 8 20 * *",   # Paraguay: 20th of each month at 8 AM (WITS lag)
    }


# =============================================================================
# STANDALONE USAGE
# =============================================================================

def main():
    """Run scheduler from command line"""
    import argparse
    import json

    parser = argparse.ArgumentParser(
        description='South America Trade Data Scheduler'
    )

    parser.add_argument(
        'command',
        choices=['status', 'run', 'start', 'cron'],
        help='Command to execute'
    )

    parser.add_argument('--task', '-t', help='Task ID to run')
    parser.add_argument('--country', '-c', help='Country code')
    parser.add_argument('--interval', type=int, default=60, help='Check interval in seconds')

    args = parser.parse_args()

    scheduler = TradeDataScheduler()

    if args.command == 'status':
        status = scheduler.get_schedule_status()
        print(json.dumps(status, indent=2, default=str))

    elif args.command == 'run':
        if args.task:
            result = scheduler.run_task(args.task)
        elif args.country:
            task_id = f"monthly_{args.country.lower()}"
            result = scheduler.run_task(task_id)
        else:
            print("Please specify --task or --country")
            return

        print(json.dumps(result, indent=2, default=str))

    elif args.command == 'start':
        print("Starting scheduler...")
        scheduler.start(check_interval=args.interval)

        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            scheduler.stop()
            print("\nScheduler stopped")

    elif args.command == 'cron':
        crons = get_recommended_cron_schedules()
        print("Recommended cron schedules:")
        for country, cron in crons.items():
            print(f"  {country}: {cron}")


if __name__ == '__main__':
    main()
