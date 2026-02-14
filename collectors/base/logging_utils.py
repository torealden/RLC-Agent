"""
Structured Logging Utilities for RLC Collectors

Provides JSON-lines logging for all collectors and checkers.
Every log entry is a single JSON line with standardized fields.

A separate log-reading script monitors these logs and sends email alerts
when problems are detected. The format MUST be consistent across all programs.
"""

import json
import logging
import uuid
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path


class LogAction(Enum):
    """Standard actions for collector/checker log entries."""
    STARTUP = "STARTUP"
    API_CALL = "API_CALL"
    DATA_SAVE = "DATA_SAVE"
    DATA_UPDATE = "DATA_UPDATE"
    DATA_DELETE = "DATA_DELETE"
    VALIDATION = "VALIDATION"
    ERROR = "ERROR"
    SHUTDOWN = "SHUTDOWN"
    # Checker-specific
    VERIFICATION_START = "VERIFICATION_START"
    VERIFICATION_RESULT = "VERIFICATION_RESULT"


class JsonLineFormatter(logging.Formatter):
    """
    Formats log records as single JSON lines.

    Each line contains: timestamp, level, collector, action, details,
    duration_seconds, run_id.
    """

    def __init__(self, collector_name, run_id):
        super().__init__()
        self.collector_name = collector_name
        self.run_id = run_id

    def format(self, record):
        # Extract structured fields from the record if attached
        details = getattr(record, 'details', {})
        action = getattr(record, 'action', 'INFO')
        duration = getattr(record, 'duration_seconds', None)

        entry = {
            'timestamp': datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z',
            'level': record.levelname,
            'collector': self.collector_name,
            'action': action if isinstance(action, str) else action.value,
            'details': details,
            'duration_seconds': duration,
            'run_id': self.run_id,
        }

        # Include exception info if present
        if record.exc_info and record.exc_info[0] is not None:
            import traceback
            entry['details']['stack_trace'] = ''.join(
                traceback.format_exception(*record.exc_info)
            )

        return json.dumps(entry, default=str)


class ConsoleFormatter(logging.Formatter):
    """Human-readable console output formatter."""

    def format(self, record):
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        action = getattr(record, 'action', '')
        if isinstance(action, LogAction):
            action = action.value

        details = getattr(record, 'details', {})
        description = details.get('description', record.getMessage())

        if action:
            return '[{}] [{}] {}'.format(timestamp, action, description)
        return '[{}] {}'.format(timestamp, description)


def generate_run_id():
    """Generate a unique run ID for a collector/checker execution."""
    return str(uuid.uuid4())[:8]


def setup_collector_logger(name, log_dir, run_id, level=logging.INFO):
    """
    Create a logger with JSON-lines file handler and console handler.

    Args:
        name: Collector/checker name (e.g., 'epa_echo_collector')
        log_dir: Directory for log files
        run_id: Unique run identifier
        level: Logging level

    Returns:
        Configured logger instance
    """
    log_dir = Path(log_dir)
    log_dir.mkdir(parents=True, exist_ok=True)

    # Log file name: {name}_{YYYY-MM-DD}_{HH-MM-SS}.log
    timestamp = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
    log_file = log_dir / '{}_{}.log'.format(name, timestamp)

    logger = logging.getLogger(name)
    logger.setLevel(level)

    # Remove existing handlers to avoid duplicates
    logger.handlers.clear()

    # JSON-lines file handler
    file_handler = logging.FileHandler(str(log_file), encoding='utf-8')
    file_handler.setFormatter(JsonLineFormatter(name, run_id))
    file_handler.setLevel(level)
    logger.addHandler(file_handler)

    # Console handler (human-readable)
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(ConsoleFormatter())
    console_handler.setLevel(level)
    logger.addHandler(console_handler)

    # Store log file path on the logger for later reference
    logger.log_file_path = str(log_file)

    return logger


def make_log_record(logger, level, action, details, duration_seconds=None):
    """
    Create and emit a structured log record.

    Args:
        logger: Logger instance
        level: Logging level (logging.INFO, logging.WARNING, etc.)
        action: LogAction enum value or string
        details: Dict with structured event details
        duration_seconds: Optional timing info
    """
    record = logger.makeRecord(
        name=logger.name,
        level=level,
        fn='',
        lno=0,
        msg=details.get('description', ''),
        args=(),
        exc_info=None,
    )
    record.action = action if isinstance(action, str) else action.value
    record.details = details
    record.duration_seconds = duration_seconds
    logger.handle(record)
