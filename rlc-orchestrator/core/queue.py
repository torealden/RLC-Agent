"""
RLC Orchestrator - Task Queue Management
==========================================
This module provides the interface for working with the task queue.
It abstracts the database operations into clean, intuitive methods
that the rest of the system can use without worrying about SQL details.

The TaskQueue class is the primary interface. You'll use it to:
- Add new tasks to be executed
- Query for tasks that are ready to run
- Update task status as execution progresses
- Handle human-in-the-loop interactions
"""

from datetime import datetime, timedelta
from typing import List, Optional, Dict, Any
import logging

from sqlalchemy import and_, or_
from sqlalchemy.orm import Session

from .database import Task, TaskStatus, TaskType, HumanInteraction

logger = logging.getLogger(__name__)


class TaskQueue:
    """
    Manages the task queue - adding, querying, and updating tasks.
    
    This class is the main interface between the executor and the database.
    It provides methods that express intent clearly, hiding the complexity
    of database queries behind a clean API.
    
    Example usage:
        queue = TaskQueue(session)
        
        # Add a new task
        task = queue.add_task(
            name="Collect USDA corn prices",
            task_type=TaskType.DATA_COLLECTION,
            payload={"agent": "usda", "commodity": "corn"}
        )
        
        # Get the next task to execute
        task = queue.get_next_pending()
        
        # Mark it complete when done
        queue.complete_task(task, result={"prices": [...]})
    """
    
    def __init__(self, session: Session):
        """
        Initialize with a database session.
        
        The session should be created from your database engine and passed in.
        This allows for proper transaction management and testing.
        """
        self.session = session
    
    def add_task(
        self,
        name: str,
        task_type: TaskType = TaskType.SCRIPT,
        payload: Optional[Dict[str, Any]] = None,
        description: str = None,
        priority: int = 10,
        scheduled_for: datetime = None,
        parent_task_id: int = None,
        max_retries: int = 3
    ) -> Task:
        """
        Add a new task to the queue.
        
        This is the primary way to create work for the orchestrator.
        The task starts in PENDING status and will be picked up by
        the executor when its turn comes.
        
        Args:
            name: Human-readable name for the task (shown in logs, emails)
            task_type: Classification that determines how the task is executed
            payload: Dictionary of data needed to execute the task
            description: Optional longer description
            priority: Lower number = higher priority (like Unix nice values)
            scheduled_for: If set, task won't execute until this time
            parent_task_id: If set, task won't execute until parent completes
            max_retries: How many times to retry on failure
        
        Returns:
            The created Task object
        
        Example:
            task = queue.add_task(
                name="Generate weekly corn report",
                task_type=TaskType.AI_REASONING,
                payload={
                    "prompt": "Analyze this week's corn market data...",
                    "context": {"data": corn_data}
                },
                priority=5  # Higher priority than default
            )
        """
        task = Task(
            name=name,
            task_type=task_type.value if isinstance(task_type, TaskType) else task_type,
            description=description,
            priority=priority,
            scheduled_for=scheduled_for,
            parent_task_id=parent_task_id,
            max_retries=max_retries,
            status=TaskStatus.PENDING.value
        )
        
        if payload:
            task.set_payload(payload)
        
        self.session.add(task)
        self.session.commit()
        
        logger.info(f"Created task {task.id}: {name}")
        return task
    
    def get_next_pending(self) -> Optional[Task]:
        """
        Get the next task that's ready to execute.
        
        This method implements the core scheduling logic:
        1. Only PENDING tasks are considered
        2. Tasks with scheduled_for in the future are skipped
        3. Tasks whose parent hasn't completed are skipped
        4. Tasks are ordered by priority, then by creation time
        
        Returns:
            The next Task to execute, or None if the queue is empty
        """
        now = datetime.utcnow()
        
        # Build the query for pending tasks that are ready to run
        query = self.session.query(Task).filter(
            Task.status == TaskStatus.PENDING.value
        ).filter(
            # Either no scheduled time, or scheduled time has passed
            or_(
                Task.scheduled_for.is_(None),
                Task.scheduled_for <= now
            )
        )
        
        # Order by priority (lower first), then by creation time (older first)
        # This ensures high-priority tasks run first, with FIFO within same priority
        query = query.order_by(Task.priority.asc(), Task.created_at.asc())
        
        # Get all candidates so we can check parent dependencies
        candidates = query.all()
        
        for task in candidates:
            # If task has a parent, check that parent is complete
            if task.parent_task_id:
                parent = self.session.query(Task).get(task.parent_task_id)
                if parent and parent.status != TaskStatus.COMPLETED.value:
                    # Parent not done yet, skip this task
                    continue
            
            # This task is ready to run
            return task
        
        return None
    
    def get_pending_count(self) -> int:
        """
        Count how many tasks are waiting to be executed.
        
        This is useful for monitoring and deciding whether to
        take on new work or wait for the queue to drain.
        """
        return self.session.query(Task).filter(
            Task.status == TaskStatus.PENDING.value
        ).count()
    
    def get_tasks_by_status(self, status: TaskStatus, limit: int = 100) -> List[Task]:
        """
        Get tasks with a specific status.
        
        Useful for:
        - Finding all WAITING_FOR_HUMAN tasks to check for responses
        - Reviewing FAILED tasks for debugging
        - Auditing COMPLETED tasks
        """
        return self.session.query(Task).filter(
            Task.status == status.value
        ).order_by(Task.created_at.desc()).limit(limit).all()
    
    def start_task(self, task: Task) -> None:
        """
        Mark a task as started.
        
        Call this when you begin executing a task. It updates the status
        and records the start time for duration tracking.
        """
        task.mark_started()
        self.session.commit()
        logger.info(f"Started task {task.id}: {task.name}")
    
    def complete_task(self, task: Task, result: Any = None) -> None:
        """
        Mark a task as successfully completed.
        
        Call this when a task finishes successfully. The result
        is stored for later reference and audit purposes.
        """
        task.mark_completed(result)
        self.session.commit()
        logger.info(f"Completed task {task.id}: {task.name}")
    
    def fail_task(self, task: Task, error: str, retry: bool = True) -> None:
        """
        Mark a task as failed.
        
        If retry is True and the task hasn't exceeded max_retries,
        the task is returned to PENDING status for another attempt.
        Otherwise, it's marked as permanently FAILED.
        
        Args:
            task: The task that failed
            error: Description of what went wrong
            retry: Whether to attempt retry (if retries remaining)
        """
        task.retry_count += 1
        
        if retry and task.retry_count < task.max_retries:
            # Return to pending for retry
            task.status = TaskStatus.PENDING.value
            task.error = f"Attempt {task.retry_count} failed: {error}"
            logger.warning(f"Task {task.id} failed, will retry ({task.retry_count}/{task.max_retries}): {error}")
        else:
            # Permanent failure
            task.mark_failed(error)
            logger.error(f"Task {task.id} permanently failed: {error}")
        
        self.session.commit()
    
    def request_human_input(
        self,
        task: Task,
        request: str,
        interaction_type: str = "approval"
    ) -> HumanInteraction:
        """
        Put a task into waiting state and record the interaction.
        
        This creates a HumanInteraction record that tracks the request
        and eventual response. The task moves to WAITING_FOR_HUMAN status
        until provide_human_input() is called.
        
        Args:
            task: The task that needs human input
            request: Description of what input is needed
            interaction_type: Type of interaction (approval, question, alert)
        
        Returns:
            The HumanInteraction record
        """
        task.request_human_input(request)
        
        interaction = HumanInteraction(
            task_id=task.id,
            interaction_type=interaction_type,
            request_body=request
        )
        
        self.session.add(interaction)
        self.session.commit()
        
        logger.info(f"Task {task.id} waiting for human input: {request[:100]}...")
        return interaction
    
    def provide_human_input(
        self,
        task: Task,
        response: str,
        decision: str = None,
        notes: str = None
    ) -> None:
        """
        Provide the human's response to a waiting task.
        
        This updates the most recent HumanInteraction record and
        returns the task to PENDING status so execution can continue.
        
        Args:
            task: The task that was waiting
            response: The raw response text
            decision: Parsed decision (approved, rejected, deferred)
            notes: Any additional notes from the human
        """
        # Update the task
        task.provide_human_input(response)
        
        # Update the interaction record
        interaction = self.session.query(HumanInteraction).filter(
            HumanInteraction.task_id == task.id
        ).order_by(HumanInteraction.id.desc()).first()
        
        if interaction:
            interaction.response_body = response
            interaction.response_received_at = datetime.utcnow()
            interaction.response_decision = decision
            interaction.response_notes = notes
        
        self.session.commit()
        logger.info(f"Task {task.id} received human input, decision: {decision}")
    
    def get_waiting_for_human(self) -> List[Task]:
        """
        Get all tasks currently waiting for human input.
        
        The email handler uses this to check for tasks that need
        reminder emails or that might have received responses.
        """
        return self.session.query(Task).filter(
            Task.status == TaskStatus.WAITING_FOR_HUMAN.value
        ).order_by(Task.created_at.asc()).all()
    
    def cancel_task(self, task: Task, reason: str = None) -> None:
        """
        Cancel a pending task.
        
        Cancelled tasks are not executed and cannot be resumed.
        Use this for tasks that are no longer needed.
        """
        task.status = TaskStatus.CANCELLED.value
        if reason:
            task.error = f"Cancelled: {reason}"
        self.session.commit()
        logger.info(f"Cancelled task {task.id}: {task.name}")
    
    def get_task_by_id(self, task_id: int) -> Optional[Task]:
        """Get a task by its ID."""
        return self.session.query(Task).get(task_id)
    
    def get_recent_tasks(self, limit: int = 20) -> List[Task]:
        """Get the most recently created tasks."""
        return self.session.query(Task).order_by(
            Task.created_at.desc()
        ).limit(limit).all()
    
    def get_failed_tasks(self, since: datetime = None) -> List[Task]:
        """
        Get failed tasks, optionally filtered by time.
        
        Useful for daily reports or debugging sessions.
        """
        query = self.session.query(Task).filter(
            Task.status == TaskStatus.FAILED.value
        )
        
        if since:
            query = query.filter(Task.completed_at >= since)
        
        return query.order_by(Task.completed_at.desc()).all()
    
    def cleanup_old_completed(self, days: int = 30) -> int:
        """
        Remove completed tasks older than the specified days.
        
        This helps keep the database size manageable. Returns
        the number of tasks deleted.
        
        Note: Consider archiving to a separate table instead of
        deleting if you need long-term audit trails.
        """
        cutoff = datetime.utcnow() - timedelta(days=days)
        
        deleted = self.session.query(Task).filter(
            Task.status == TaskStatus.COMPLETED.value,
            Task.completed_at < cutoff
        ).delete()
        
        self.session.commit()
        logger.info(f"Cleaned up {deleted} old completed tasks")
        return deleted


class TaskBuilder:
    """
    Fluent interface for building tasks.
    
    This provides a more readable way to construct complex tasks
    with many options. It's especially useful when you're building
    tasks programmatically.
    
    Example:
        task = TaskBuilder(queue) \
            .named("Generate market report") \
            .of_type(TaskType.AI_REASONING) \
            .with_priority(5) \
            .with_payload({"prompt": "..."}) \
            .scheduled_for(tomorrow_8am) \
            .build()
    """
    
    def __init__(self, queue: TaskQueue):
        self.queue = queue
        self._name = "Unnamed Task"
        self._task_type = TaskType.SCRIPT
        self._payload = None
        self._description = None
        self._priority = 10
        self._scheduled_for = None
        self._parent_task_id = None
        self._max_retries = 3
    
    def named(self, name: str) -> "TaskBuilder":
        """Set the task name."""
        self._name = name
        return self
    
    def described(self, description: str) -> "TaskBuilder":
        """Set the task description."""
        self._description = description
        return self
    
    def of_type(self, task_type: TaskType) -> "TaskBuilder":
        """Set the task type."""
        self._task_type = task_type
        return self
    
    def with_payload(self, payload: Dict[str, Any]) -> "TaskBuilder":
        """Set the task payload."""
        self._payload = payload
        return self
    
    def with_priority(self, priority: int) -> "TaskBuilder":
        """Set the priority (lower = higher priority)."""
        self._priority = priority
        return self
    
    def scheduled_for(self, when: datetime) -> "TaskBuilder":
        """Schedule the task for a specific time."""
        self._scheduled_for = when
        return self
    
    def after_task(self, parent_id: int) -> "TaskBuilder":
        """Make this task depend on another task."""
        self._parent_task_id = parent_id
        return self
    
    def with_retries(self, max_retries: int) -> "TaskBuilder":
        """Set the maximum retry count."""
        self._max_retries = max_retries
        return self
    
    def build(self) -> Task:
        """Create and return the task."""
        return self.queue.add_task(
            name=self._name,
            task_type=self._task_type,
            payload=self._payload,
            description=self._description,
            priority=self._priority,
            scheduled_for=self._scheduled_for,
            parent_task_id=self._parent_task_id,
            max_retries=self._max_retries
        )
