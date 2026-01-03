"""
RLC Orchestrator - Database Models
===================================
This module defines the core data structures for the orchestrator's task queue,
scheduling, and state management. We use SQLAlchemy for database abstraction,
which makes it easy to start with SQLite and migrate to PostgreSQL later.

The key entities are:
- Task: Represents a unit of work to be executed
- Schedule: Defines recurring tasks (like cron jobs)
- AgentConfig: Stores configuration for each agent in the system
- ExecutionLog: Records what happened when tasks ran
"""

from datetime import datetime, timedelta
from enum import Enum
from typing import Optional, Dict, Any
import json

from sqlalchemy import create_engine, Column, Integer, String, Text, DateTime, Boolean, Enum as SQLEnum, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship
from sqlalchemy.pool import StaticPool

Base = declarative_base()


class TaskStatus(str, Enum):
    """
    Tasks move through these states during their lifecycle:
    
    PENDING -> IN_PROGRESS -> COMPLETED (happy path)
                           -> FAILED (if execution fails)
                           -> WAITING_FOR_HUMAN (needs approval/input)
    
    WAITING_FOR_HUMAN -> PENDING (when human responds)
    
    CANCELLED is a terminal state for tasks that were abandoned.
    """
    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    FAILED = "failed"
    WAITING_FOR_HUMAN = "waiting_for_human"
    CANCELLED = "cancelled"


class TaskType(str, Enum):
    """
    Different task types are handled differently by the executor.
    This routing allows us to apply appropriate security controls
    and choose the right execution strategy for each type.
    """
    # Simple script execution - runs a Python function or script
    SCRIPT = "script"
    
    # Requires AI reasoning - calls out to Claude/GPT
    AI_REASONING = "ai_reasoning"
    
    # Code generation - AI writes code, requires human review
    CODE_GENERATION = "code_generation"
    
    # Data collection - fetches data from external sources
    DATA_COLLECTION = "data_collection"
    
    # Analysis - processes data and generates insights
    ANALYSIS = "analysis"
    
    # Email - sends or processes email
    EMAIL = "email"
    
    # Human interaction - explicitly requires human input
    HUMAN_INPUT = "human_input"


class Task(Base):
    """
    A Task represents a single unit of work to be executed.
    
    Tasks are the fundamental building block of the orchestrator.
    Everything that happens - data collection, analysis, emails,
    code generation - is represented as a task in the queue.
    """
    __tablename__ = "tasks"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    
    # Human-readable name for logging and display
    name = Column(String(255), nullable=False)
    
    # Longer description of what this task does
    description = Column(Text, nullable=True)
    
    # Task classification for routing and security
    task_type = Column(String(50), default=TaskType.SCRIPT.value)
    status = Column(String(50), default=TaskStatus.PENDING.value)
    
    # Priority (lower number = higher priority, like Unix nice values)
    priority = Column(Integer, default=10)
    
    # The actual work to be done, stored as JSON
    # For SCRIPT tasks: {"function": "module.function_name", "args": {...}}
    # For AI tasks: {"prompt": "...", "context": {...}}
    # For DATA_COLLECTION: {"agent": "usda", "endpoint": "..."}
    payload = Column(Text, nullable=True)
    
    # Results from execution, stored as JSON
    result = Column(Text, nullable=True)
    
    # Error message if the task failed
    error = Column(Text, nullable=True)
    
    # Number of times we've tried to execute this task
    retry_count = Column(Integer, default=0)
    max_retries = Column(Integer, default=3)
    
    # Timing
    created_at = Column(DateTime, default=datetime.utcnow)
    started_at = Column(DateTime, nullable=True)
    completed_at = Column(DateTime, nullable=True)
    
    # For scheduled/delayed execution
    scheduled_for = Column(DateTime, nullable=True)
    
    # Dependencies - this task won't run until parent is complete
    parent_task_id = Column(Integer, ForeignKey("tasks.id"), nullable=True)
    
    # For WAITING_FOR_HUMAN: what are we waiting for?
    human_input_request = Column(Text, nullable=True)
    human_input_response = Column(Text, nullable=True)
    
    # Link to the schedule that created this task (if any)
    schedule_id = Column(Integer, ForeignKey("schedules.id"), nullable=True)
    
    def set_payload(self, data: Dict[str, Any]) -> None:
        """Store a dictionary as the task payload."""
        self.payload = json.dumps(data)
    
    def get_payload(self) -> Dict[str, Any]:
        """Retrieve the task payload as a dictionary."""
        if self.payload:
            return json.loads(self.payload)
        return {}
    
    def set_result(self, data: Any) -> None:
        """Store the task result."""
        self.result = json.dumps(data)
    
    def get_result(self) -> Any:
        """Retrieve the task result."""
        if self.result:
            return json.loads(self.result)
        return None
    
    def mark_started(self) -> None:
        """Mark the task as in progress."""
        self.status = TaskStatus.IN_PROGRESS.value
        self.started_at = datetime.utcnow()
    
    def mark_completed(self, result: Any = None) -> None:
        """Mark the task as successfully completed."""
        self.status = TaskStatus.COMPLETED.value
        self.completed_at = datetime.utcnow()
        if result is not None:
            self.set_result(result)
    
    def mark_failed(self, error: str) -> None:
        """Mark the task as failed."""
        self.status = TaskStatus.FAILED.value
        self.completed_at = datetime.utcnow()
        self.error = error
    
    def request_human_input(self, request: str) -> None:
        """Put the task in waiting state until human responds."""
        self.status = TaskStatus.WAITING_FOR_HUMAN.value
        self.human_input_request = request
    
    def provide_human_input(self, response: str) -> None:
        """Record human input and return task to pending."""
        self.human_input_response = response
        self.status = TaskStatus.PENDING.value
    
    def __repr__(self) -> str:
        return f"<Task {self.id}: {self.name} [{self.status}]>"


class Schedule(Base):
    """
    A Schedule defines a recurring task pattern.
    
    Schedules use cron-like syntax to define when tasks should be created.
    The scheduler checks every minute and creates Task entries when
    a schedule's conditions are met.
    """
    __tablename__ = "schedules"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    
    # Human-readable name
    name = Column(String(255), nullable=False)
    description = Column(Text, nullable=True)
    
    # Is this schedule active?
    enabled = Column(Boolean, default=True)
    
    # Cron expression: "minute hour day_of_month month day_of_week"
    # Examples:
    #   "0 6 * * *" = 6:00 AM every day
    #   "0 8 * * 1" = 8:00 AM every Monday
    #   "*/15 * * * *" = Every 15 minutes
    cron_expression = Column(String(100), nullable=False)
    
    # Timezone for interpreting the cron expression
    timezone = Column(String(50), default="America/Chicago")
    
    # Template for the task that gets created
    # This is a JSON object with the same structure as Task payload
    task_template = Column(Text, nullable=False)
    task_type = Column(String(50), default=TaskType.SCRIPT.value)
    task_priority = Column(Integer, default=10)
    
    # When was a task last created from this schedule?
    last_run_at = Column(DateTime, nullable=True)
    next_run_at = Column(DateTime, nullable=True)
    
    # Tracking
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Relationship to tasks created from this schedule
    tasks = relationship("Task", backref="schedule", lazy="dynamic")
    
    def set_task_template(self, template: Dict[str, Any]) -> None:
        """Store the task template."""
        self.task_template = json.dumps(template)
    
    def get_task_template(self) -> Dict[str, Any]:
        """Retrieve the task template."""
        return json.loads(self.task_template) if self.task_template else {}
    
    def __repr__(self) -> str:
        status = "enabled" if self.enabled else "disabled"
        return f"<Schedule {self.id}: {self.name} [{status}]>"


class AgentConfig(Base):
    """
    Configuration for each agent in the system.
    
    As the system builds itself, new agents are registered here.
    This provides a central registry of capabilities and their configuration.
    """
    __tablename__ = "agent_configs"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    
    # Unique identifier for this agent
    agent_id = Column(String(100), unique=True, nullable=False)
    
    # Human-readable name and description
    name = Column(String(255), nullable=False)
    description = Column(Text, nullable=True)
    
    # Is this agent active?
    enabled = Column(Boolean, default=True)
    
    # Agent type/category
    agent_type = Column(String(50), nullable=False)  # data_collector, analyzer, reporter, etc.
    
    # Configuration as JSON
    # Structure depends on agent type, but commonly includes:
    # - credentials reference (not the actual credentials!)
    # - API endpoints
    # - collection schedules
    # - processing parameters
    config = Column(Text, nullable=True)
    
    # Path to the agent's Python module
    module_path = Column(String(500), nullable=True)
    
    # Metadata
    version = Column(String(50), default="1.0.0")
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # When was this agent last executed?
    last_run_at = Column(DateTime, nullable=True)
    last_run_status = Column(String(50), nullable=True)
    
    def set_config(self, config: Dict[str, Any]) -> None:
        """Store agent configuration."""
        self.config = json.dumps(config)
    
    def get_config(self) -> Dict[str, Any]:
        """Retrieve agent configuration."""
        return json.loads(self.config) if self.config else {}
    
    def __repr__(self) -> str:
        status = "enabled" if self.enabled else "disabled"
        return f"<Agent {self.agent_id}: {self.name} [{status}]>"


class ExecutionLog(Base):
    """
    Detailed log of task executions.
    
    This provides an audit trail of everything the system does,
    which is essential for debugging and accountability.
    """
    __tablename__ = "execution_logs"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    
    # What task was executed?
    task_id = Column(Integer, ForeignKey("tasks.id"), nullable=False)
    
    # Execution details
    started_at = Column(DateTime, default=datetime.utcnow)
    completed_at = Column(DateTime, nullable=True)
    
    # Outcome
    success = Column(Boolean, nullable=True)
    error_message = Column(Text, nullable=True)
    
    # Detailed log output (can be verbose)
    log_output = Column(Text, nullable=True)
    
    # Resource usage (optional, for monitoring)
    duration_seconds = Column(Integer, nullable=True)
    api_calls_made = Column(Integer, default=0)
    api_tokens_used = Column(Integer, default=0)
    
    # Relationship
    task = relationship("Task", backref="execution_logs")
    
    def __repr__(self) -> str:
        status = "success" if self.success else "failed" if self.success is False else "running"
        return f"<ExecutionLog {self.id} for Task {self.task_id} [{status}]>"


class HumanInteraction(Base):
    """
    Records of human-in-the-loop interactions.
    
    This tracks emails sent, responses received, and the context
    of each approval or input request.
    """
    __tablename__ = "human_interactions"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    
    # Link to the task this interaction is for
    task_id = Column(Integer, ForeignKey("tasks.id"), nullable=False)
    
    # What type of interaction?
    interaction_type = Column(String(50), nullable=False)  # approval, question, alert, status
    
    # The request we sent
    request_subject = Column(String(500), nullable=True)
    request_body = Column(Text, nullable=True)
    request_sent_at = Column(DateTime, nullable=True)
    
    # The response we received
    response_body = Column(Text, nullable=True)
    response_received_at = Column(DateTime, nullable=True)
    
    # Parsed response
    response_decision = Column(String(50), nullable=True)  # approved, rejected, deferred, etc.
    response_notes = Column(Text, nullable=True)
    
    # Email message IDs for threading
    email_message_id = Column(String(255), nullable=True)
    email_thread_id = Column(String(255), nullable=True)
    
    # Relationship
    task = relationship("Task", backref="human_interactions")
    
    def __repr__(self) -> str:
        return f"<HumanInteraction {self.id} [{self.interaction_type}] for Task {self.task_id}>"


# Database initialization
def get_engine(db_path: str = "data/orchestrator.db"):
    """
    Create and return a database engine.
    
    For development/testing, we use SQLite with some special settings
    to handle concurrent access gracefully. In production, you might
    switch to PostgreSQL for better performance and reliability.
    """
    # For SQLite, we use check_same_thread=False to allow multi-threaded access
    # and StaticPool to maintain a single connection (good for SQLite)
    return create_engine(
        f"sqlite:///{db_path}",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
        echo=False  # Set to True for SQL debugging
    )


def init_database(engine) -> None:
    """Create all tables if they don't exist."""
    Base.metadata.create_all(engine)


def get_session(engine):
    """Create a new database session."""
    Session = sessionmaker(bind=engine)
    return Session()


# Convenience function for quick setup
def setup_database(db_path: str = "data/orchestrator.db"):
    """
    One-liner to set up the database and return a session.
    
    Usage:
        session = setup_database()
        # Now you can create/query tasks, schedules, etc.
    """
    engine = get_engine(db_path)
    init_database(engine)
    return get_session(engine)
