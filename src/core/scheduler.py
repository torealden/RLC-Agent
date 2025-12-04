"""
Master Scheduler for RLC Agent System.

Manages automated task execution based on data source publication schedules.
Triggers data collection agents at optimal times to capture fresh data.
"""

import asyncio
import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Awaitable
import uuid

try:
    import pytz
except ImportError:
    pytz = None

logger = logging.getLogger(__name__)


class TaskFrequency(Enum):
    """Frequency types for scheduled tasks"""
    ONCE = "once"
    DAILY = "daily"
    WEEKLY = "weekly"
    MONTHLY = "monthly"
    CUSTOM = "custom"


class TaskStatus(Enum):
    """Status of a scheduled task"""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"


@dataclass
class ScheduledTask:
    """A task scheduled for execution"""
    task_id: str
    name: str
    source_id: str
    frequency: TaskFrequency
    handler: Optional[Callable[..., Awaitable[Any]]] = None
    params: Dict[str, Any] = field(default_factory=dict)

    # Scheduling parameters
    scheduled_time: Optional[str] = None  # HH:MM format
    day_of_week: Optional[str] = None  # For weekly tasks
    day_of_month: Optional[int] = None  # For monthly tasks
    timezone: str = "America/Chicago"

    # Execution tracking
    next_run: Optional[datetime] = None
    last_run: Optional[datetime] = None
    last_status: TaskStatus = TaskStatus.PENDING
    last_error: Optional[str] = None
    run_count: int = 0

    # Control flags
    enabled: bool = True
    max_retries: int = 3
    retry_count: int = 0

    def __post_init__(self):
        if self.next_run is None:
            self.next_run = self._calculate_next_run()

    def _get_timezone(self):
        """Get timezone object"""
        if pytz:
            return pytz.timezone(self.timezone)
        return None

    def _calculate_next_run(self) -> Optional[datetime]:
        """Calculate the next execution time"""
        if not self.scheduled_time:
            return None

        tz = self._get_timezone()
        now = datetime.now(tz) if tz else datetime.now()

        hour, minute = map(int, self.scheduled_time.split(':'))

        if self.frequency == TaskFrequency.DAILY:
            next_run = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
            if next_run <= now:
                next_run += timedelta(days=1)

        elif self.frequency == TaskFrequency.WEEKLY and self.day_of_week:
            days = ['monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday']
            target_day = days.index(self.day_of_week.lower())
            current_day = now.weekday()

            days_ahead = target_day - current_day
            if days_ahead < 0:
                days_ahead += 7

            next_run = now + timedelta(days=days_ahead)
            next_run = next_run.replace(hour=hour, minute=minute, second=0, microsecond=0)

            if next_run <= now:
                next_run += timedelta(days=7)

        elif self.frequency == TaskFrequency.MONTHLY and self.day_of_month:
            next_run = now.replace(
                day=min(self.day_of_month, 28),  # Safe default
                hour=hour,
                minute=minute,
                second=0,
                microsecond=0
            )
            if next_run <= now:
                # Move to next month
                if now.month == 12:
                    next_run = next_run.replace(year=now.year + 1, month=1)
                else:
                    next_run = next_run.replace(month=now.month + 1)

        else:
            next_run = now + timedelta(hours=1)  # Default to 1 hour from now

        return next_run

    def update_next_run(self):
        """Update next_run after execution"""
        self.last_run = datetime.now()
        self.next_run = self._calculate_next_run()
        self.run_count += 1

    def is_due(self, now: datetime = None) -> bool:
        """Check if task is due for execution"""
        if not self.enabled or self.next_run is None:
            return False

        if now is None:
            tz = self._get_timezone()
            now = datetime.now(tz) if tz else datetime.now()

        return now >= self.next_run


@dataclass
class TaskResult:
    """Result of a task execution"""
    task_id: str
    success: bool
    data: Any = None
    error: Optional[str] = None
    execution_time: float = 0.0
    timestamp: datetime = field(default_factory=datetime.now)


class MasterScheduler:
    """
    Master scheduler coordinating all automated data collection.

    Manages task schedules aligned with data source publication times.
    Executes tasks asynchronously and handles retries on failure.
    """

    def __init__(self, settings=None):
        """
        Initialize the scheduler.

        Args:
            settings: System settings (uses global if None)
        """
        if settings is None:
            from .config import get_settings
            settings = get_settings()

        self.settings = settings
        self._tasks: Dict[str, ScheduledTask] = {}
        self._running = False
        self._task_handlers: Dict[str, Callable] = {}
        self._execution_history: List[TaskResult] = []
        self._max_history = 1000

        # Event callbacks
        self._on_task_complete: List[Callable] = []
        self._on_task_error: List[Callable] = []

        logger.info("MasterScheduler initialized")

    def register_task(self, task: ScheduledTask):
        """
        Register a task with the scheduler.

        Args:
            task: Task to register
        """
        self._tasks[task.task_id] = task
        logger.info(f"Registered task: {task.name} [{task.frequency.value}]")

    def register_handler(self, source_id: str, handler: Callable[..., Awaitable[Any]]):
        """
        Register a handler function for a source type.

        Args:
            source_id: Source identifier
            handler: Async function to execute
        """
        self._task_handlers[source_id] = handler
        logger.info(f"Registered handler for source: {source_id}")

    def register_from_settings(self):
        """Register tasks from settings configuration"""
        for source_id, source_config in self.settings.data_sources.items():
            task = ScheduledTask(
                task_id=f"task_{source_id}_{uuid.uuid4().hex[:6]}",
                name=source_config.source_name,
                source_id=source_id,
                frequency=TaskFrequency(source_config.frequency),
                scheduled_time=source_config.time,
                day_of_week=source_config.day_of_week,
                day_of_month=source_config.day_of_month,
                timezone=source_config.timezone,
                enabled=source_config.enabled,
                params={'reports': source_config.reports}
            )
            self.register_task(task)

    def get_pending_tasks(self, now: datetime = None) -> List[ScheduledTask]:
        """
        Get all tasks that are due for execution.

        Args:
            now: Current time (uses now if None)

        Returns:
            List of due tasks
        """
        return [task for task in self._tasks.values() if task.is_due(now)]

    def get_todays_schedule(self) -> List[ScheduledTask]:
        """
        Get all tasks scheduled for today.

        Returns:
            List of today's tasks
        """
        tz = pytz.timezone(self.settings.scheduler.timezone) if pytz else None
        now = datetime.now(tz) if tz else datetime.now()
        today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        today_end = today_start + timedelta(days=1)

        scheduled_today = []
        for task in self._tasks.values():
            if task.next_run and today_start <= task.next_run < today_end:
                scheduled_today.append(task)

        return sorted(scheduled_today, key=lambda t: t.next_run or datetime.max)

    async def execute_task(self, task: ScheduledTask) -> TaskResult:
        """
        Execute a single task.

        Args:
            task: Task to execute

        Returns:
            TaskResult with execution outcome
        """
        start_time = datetime.now()
        task.last_status = TaskStatus.RUNNING

        logger.info(f"Executing task: {task.name} [id={task.task_id}]")

        try:
            # Get handler
            handler = task.handler or self._task_handlers.get(task.source_id)

            if handler is None:
                raise RuntimeError(f"No handler registered for source: {task.source_id}")

            # Execute
            result_data = await handler(**task.params)

            execution_time = (datetime.now() - start_time).total_seconds()

            task.last_status = TaskStatus.COMPLETED
            task.last_error = None
            task.retry_count = 0
            task.update_next_run()

            result = TaskResult(
                task_id=task.task_id,
                success=True,
                data=result_data,
                execution_time=execution_time
            )

            logger.info(f"Task completed: {task.name} [time={execution_time:.2f}s]")

            # Notify callbacks
            for callback in self._on_task_complete:
                try:
                    await callback(task, result)
                except Exception as e:
                    logger.warning(f"Task complete callback error: {e}")

            return result

        except Exception as e:
            execution_time = (datetime.now() - start_time).total_seconds()

            task.last_status = TaskStatus.FAILED
            task.last_error = str(e)
            task.retry_count += 1

            result = TaskResult(
                task_id=task.task_id,
                success=False,
                error=str(e),
                execution_time=execution_time
            )

            logger.error(f"Task failed: {task.name} - {e}")

            # Check if should retry
            if task.retry_count < task.max_retries:
                # Schedule retry in 5 minutes
                task.next_run = datetime.now() + timedelta(minutes=5 * task.retry_count)
                logger.info(f"Task retry scheduled: {task.name} [attempt {task.retry_count}]")
            else:
                task.update_next_run()  # Move to next scheduled time
                logger.warning(f"Task max retries exceeded: {task.name}")

            # Notify callbacks
            for callback in self._on_task_error:
                try:
                    await callback(task, result)
                except Exception as callback_error:
                    logger.warning(f"Task error callback error: {callback_error}")

            return result

        finally:
            self._record_execution(result)

    def _record_execution(self, result: TaskResult):
        """Record task execution in history"""
        self._execution_history.append(result)
        if len(self._execution_history) > self._max_history:
            self._execution_history = self._execution_history[-self._max_history:]

    async def run_loop(self):
        """
        Main scheduler loop.

        Continuously checks for and executes due tasks.
        """
        self._running = True
        logger.info("Scheduler loop started")

        while self._running:
            try:
                pending_tasks = self.get_pending_tasks()

                if pending_tasks:
                    logger.info(f"Found {len(pending_tasks)} pending tasks")

                    # Execute tasks with concurrency limit
                    semaphore = asyncio.Semaphore(self.settings.scheduler.max_concurrent_tasks)

                    async def execute_with_limit(task):
                        async with semaphore:
                            return await self.execute_task(task)

                    await asyncio.gather(
                        *[execute_with_limit(task) for task in pending_tasks],
                        return_exceptions=True
                    )

            except Exception as e:
                logger.error(f"Scheduler loop error: {e}")

            await asyncio.sleep(self.settings.scheduler.check_interval_seconds)

        logger.info("Scheduler loop stopped")

    def stop(self):
        """Stop the scheduler loop"""
        self._running = False
        logger.info("Scheduler stop requested")

    async def run_now(self, source_id: str, params: Dict[str, Any] = None) -> TaskResult:
        """
        Run a task immediately regardless of schedule.

        Args:
            source_id: Source to execute
            params: Override parameters

        Returns:
            TaskResult from execution
        """
        # Find task by source_id
        task = None
        for t in self._tasks.values():
            if t.source_id == source_id:
                task = t
                break

        if task is None:
            # Create ad-hoc task
            task = ScheduledTask(
                task_id=f"adhoc_{source_id}_{uuid.uuid4().hex[:6]}",
                name=f"Ad-hoc: {source_id}",
                source_id=source_id,
                frequency=TaskFrequency.ONCE,
                params=params or {}
            )
        elif params:
            task.params.update(params)

        return await self.execute_task(task)

    def on_task_complete(self, callback: Callable):
        """Register callback for task completion"""
        self._on_task_complete.append(callback)

    def on_task_error(self, callback: Callable):
        """Register callback for task errors"""
        self._on_task_error.append(callback)

    def get_status(self) -> Dict[str, Any]:
        """Get scheduler status"""
        return {
            'running': self._running,
            'task_count': len(self._tasks),
            'tasks': [
                {
                    'id': t.task_id,
                    'name': t.name,
                    'source': t.source_id,
                    'frequency': t.frequency.value,
                    'enabled': t.enabled,
                    'next_run': t.next_run.isoformat() if t.next_run else None,
                    'last_run': t.last_run.isoformat() if t.last_run else None,
                    'last_status': t.last_status.value,
                    'run_count': t.run_count
                }
                for t in self._tasks.values()
            ],
            'recent_executions': len(self._execution_history)
        }

    def get_next_tasks(self, count: int = 5) -> List[Dict[str, Any]]:
        """Get the next N scheduled tasks"""
        sorted_tasks = sorted(
            [t for t in self._tasks.values() if t.next_run and t.enabled],
            key=lambda t: t.next_run
        )

        return [
            {
                'name': t.name,
                'source': t.source_id,
                'next_run': t.next_run.isoformat(),
                'frequency': t.frequency.value
            }
            for t in sorted_tasks[:count]
        ]
