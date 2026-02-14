"""
Census Base Agent
=================
Base class for all Census Bureau pipeline agents with standardized:
- JSON-structured logging
- Database connections
- Pipeline event tracking
- Error handling

All Census agents should extend this class to ensure consistent logging
that can be read by the CensusLogReaderAgent for daily summaries.
"""

import json
import logging
import os
import sys
from abc import ABC, abstractmethod
from dataclasses import dataclass, field, asdict
from datetime import datetime, date
from pathlib import Path
from typing import Dict, List, Optional, Any, Union
from enum import Enum

import psycopg2
from dotenv import load_dotenv

# Project paths
PROJECT_ROOT = Path(__file__).parent.parent.parent.parent
load_dotenv(PROJECT_ROOT / '.env')

# Logs directory
CENSUS_LOGS_DIR = PROJECT_ROOT / 'logs' / 'census'


class PipelineLayer(Enum):
    """Data pipeline layers"""
    BRONZE = "bronze"
    SILVER = "silver"
    GOLD = "gold"


class EventType(Enum):
    """Log event types"""
    # Lifecycle events
    AGENT_START = "agent_start"
    AGENT_COMPLETE = "agent_complete"
    AGENT_ERROR = "agent_error"

    # Data events
    DATA_FETCH = "data_fetch"
    DATA_VALIDATE = "data_validate"
    DATA_TRANSFORM = "data_transform"
    DATA_SAVE = "data_save"

    # Verification events
    VERIFICATION_START = "verification_start"
    VERIFICATION_PASS = "verification_pass"
    VERIFICATION_FAIL = "verification_fail"
    VERIFICATION_WARNING = "verification_warning"

    # View events
    VIEW_REFRESH = "view_refresh"
    VIEW_CREATE = "view_create"

    # General
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"


@dataclass
class LogEntry:
    """Structured log entry for Census pipeline"""
    timestamp: str
    agent: str
    layer: str
    event_type: str
    message: str
    data: Dict[str, Any] = field(default_factory=dict)
    success: bool = True
    duration_ms: Optional[int] = None

    def to_json(self) -> str:
        """Convert to JSON string"""
        return json.dumps(asdict(self), default=str)

    @classmethod
    def from_json(cls, json_str: str) -> 'LogEntry':
        """Create from JSON string"""
        data = json.loads(json_str)
        return cls(**data)


@dataclass
class AgentResult:
    """Result of an agent run"""
    success: bool
    agent_name: str
    layer: str
    started_at: datetime
    completed_at: datetime = field(default_factory=datetime.now)

    # Stats
    records_processed: int = 0
    records_inserted: int = 0
    records_updated: int = 0
    records_failed: int = 0

    # Verification
    checks_passed: int = 0
    checks_failed: int = 0
    warnings: List[str] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)

    # Metadata
    metadata: Dict[str, Any] = field(default_factory=dict)

    @property
    def duration_seconds(self) -> float:
        return (self.completed_at - self.started_at).total_seconds()

    def to_dict(self) -> Dict[str, Any]:
        result = asdict(self)
        result['started_at'] = self.started_at.isoformat()
        result['completed_at'] = self.completed_at.isoformat()
        result['duration_seconds'] = self.duration_seconds
        return result


class CensusJsonFormatter(logging.Formatter):
    """JSON formatter for Census pipeline logs"""

    def __init__(self, agent_name: str, layer: str):
        super().__init__()
        self.agent_name = agent_name
        self.layer = layer

    def format(self, record: logging.LogRecord) -> str:
        """Format log record as JSON"""
        # Get extra data if provided
        extra_data = {}
        if hasattr(record, 'extra_data'):
            extra_data = record.extra_data

        # Map log level to event type
        event_type = EventType.INFO.value
        if record.levelno >= logging.ERROR:
            event_type = EventType.ERROR.value
        elif record.levelno >= logging.WARNING:
            event_type = EventType.WARNING.value
        elif hasattr(record, 'event_type'):
            event_type = record.event_type

        entry = LogEntry(
            timestamp=datetime.now().isoformat(),
            agent=self.agent_name,
            layer=self.layer,
            event_type=event_type,
            message=record.getMessage(),
            data=extra_data,
            success=record.levelno < logging.ERROR,
            duration_ms=extra_data.get('duration_ms')
        )

        return entry.to_json()


class CensusBaseAgent(ABC):
    """
    Base class for all Census Bureau pipeline agents.

    Provides:
    - Structured JSON logging to logs/census/
    - Database connection management
    - Pipeline event tracking
    - Standardized error handling
    - Result collection

    Subclasses must implement:
    - run(): Main agent execution logic
    - get_layer(): Return the pipeline layer (bronze/silver/gold)
    """

    def __init__(
        self,
        agent_name: str = None,
        db_host: str = None,
        db_port: int = None,
        db_name: str = None,
        db_user: str = None,
        db_password: str = None
    ):
        """
        Initialize the Census agent.

        Args:
            agent_name: Name for logging (defaults to class name)
            db_*: Database connection parameters (default from env)
        """
        self.agent_name = agent_name or self.__class__.__name__
        self.started_at = datetime.now()

        # Database config
        self.db_host = db_host or os.environ.get('DB_HOST', 'localhost')
        self.db_port = db_port or int(os.environ.get('DB_PORT', '5432'))
        self.db_name = db_name or os.environ.get('DB_NAME', 'rlc_commodities')
        self.db_user = db_user or os.environ.get('DB_USER', 'postgres')
        self.db_password = db_password or os.environ.get('DB_PASSWORD', '')

        # Connection (lazy loaded)
        self._conn = None

        # Result tracking
        self._result = AgentResult(
            success=True,
            agent_name=self.agent_name,
            layer=self.get_layer().value,
            started_at=self.started_at
        )

        # Setup logging
        self._setup_logging()

        self.log_event(
            EventType.AGENT_START,
            f"{self.agent_name} starting",
            data={'layer': self.get_layer().value}
        )

    def _setup_logging(self):
        """Configure JSON logging to file and console"""
        # Ensure logs directory exists
        CENSUS_LOGS_DIR.mkdir(parents=True, exist_ok=True)

        # Log file path: logs/census/{agent_name}_YYYY-MM-DD.log
        today = date.today().isoformat()
        log_file = CENSUS_LOGS_DIR / f"{self.agent_name}_{today}.log"

        # Create logger
        self.logger = logging.getLogger(f"census.{self.agent_name}")
        self.logger.setLevel(logging.DEBUG)

        # Clear existing handlers
        self.logger.handlers.clear()

        # File handler with JSON formatting
        file_handler = logging.FileHandler(log_file, encoding='utf-8')
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(
            CensusJsonFormatter(self.agent_name, self.get_layer().value)
        )
        self.logger.addHandler(file_handler)

        # Console handler with simple formatting
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(logging.INFO)
        console_handler.setFormatter(
            logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        )
        self.logger.addHandler(console_handler)

        self.log_file = log_file

    @abstractmethod
    def get_layer(self) -> PipelineLayer:
        """Return the pipeline layer this agent operates on"""
        pass

    @abstractmethod
    def run(self, **kwargs) -> AgentResult:
        """
        Execute the agent's main logic.

        Returns:
            AgentResult with success status and statistics
        """
        pass

    # =========================================================================
    # LOGGING METHODS
    # =========================================================================

    def log_event(
        self,
        event_type: EventType,
        message: str,
        data: Dict[str, Any] = None,
        duration_ms: int = None
    ):
        """
        Log a structured event.

        Args:
            event_type: Type of event
            message: Human-readable message
            data: Additional data to include
            duration_ms: Duration in milliseconds
        """
        extra = {
            'event_type': event_type.value,
            'extra_data': data or {}
        }
        if duration_ms is not None:
            extra['extra_data']['duration_ms'] = duration_ms

        # Determine log level from event type
        level = logging.INFO
        if event_type in (EventType.ERROR, EventType.AGENT_ERROR, EventType.VERIFICATION_FAIL):
            level = logging.ERROR
        elif event_type in (EventType.WARNING, EventType.VERIFICATION_WARNING):
            level = logging.WARNING

        self.logger.log(level, message, extra=extra)

    def log_data_event(
        self,
        action: str,
        records_count: int,
        commodity: str = None,
        hs_code: str = None,
        flow: str = None,
        duration_ms: int = None
    ):
        """Log a data processing event"""
        data = {
            'records': records_count,
            'action': action
        }
        if commodity:
            data['commodity'] = commodity
        if hs_code:
            data['hs_code'] = hs_code
        if flow:
            data['flow'] = flow

        event_type = EventType.DATA_SAVE if action == 'save' else EventType.DATA_FETCH
        self.log_event(
            event_type,
            f"{action}: {records_count} records",
            data=data,
            duration_ms=duration_ms
        )

    def log_verification(
        self,
        check_name: str,
        passed: bool,
        expected: Any = None,
        actual: Any = None,
        message: str = None
    ):
        """Log a verification check result"""
        data = {
            'check': check_name,
            'passed': passed
        }
        if expected is not None:
            data['expected'] = expected
        if actual is not None:
            data['actual'] = actual

        if passed:
            self._result.checks_passed += 1
            event_type = EventType.VERIFICATION_PASS
            msg = message or f"Verification passed: {check_name}"
        else:
            self._result.checks_failed += 1
            event_type = EventType.VERIFICATION_FAIL
            msg = message or f"Verification failed: {check_name}"
            self._result.errors.append(msg)

        self.log_event(event_type, msg, data=data)

    def log_warning(self, message: str, data: Dict[str, Any] = None):
        """Log a warning"""
        self._result.warnings.append(message)
        self.log_event(EventType.WARNING, message, data=data)

    def log_error(self, message: str, data: Dict[str, Any] = None):
        """Log an error"""
        self._result.errors.append(message)
        self._result.success = False
        self.log_event(EventType.ERROR, message, data=data)

    # =========================================================================
    # DATABASE METHODS
    # =========================================================================

    @property
    def conn(self):
        """Lazy-load database connection"""
        if self._conn is None or self._conn.closed:
            self._conn = psycopg2.connect(
                host=self.db_host,
                port=self.db_port,
                database=self.db_name,
                user=self.db_user,
                password=self.db_password
            )
        return self._conn

    def close_connection(self):
        """Close database connection"""
        if self._conn and not self._conn.closed:
            self._conn.close()
            self._conn = None

    def execute_query(
        self,
        query: str,
        params: tuple = None,
        fetch: bool = True
    ) -> Optional[List[tuple]]:
        """
        Execute a database query.

        Args:
            query: SQL query string
            params: Query parameters
            fetch: Whether to fetch results

        Returns:
            Query results if fetch=True, else None
        """
        cursor = self.conn.cursor()
        try:
            cursor.execute(query, params)
            if fetch:
                return cursor.fetchall()
            self.conn.commit()
            return None
        except Exception as e:
            self.conn.rollback()
            self.log_error(f"Query error: {e}", data={'query': query[:200]})
            raise
        finally:
            cursor.close()

    # =========================================================================
    # RESULT METHODS
    # =========================================================================

    def add_records_processed(self, count: int):
        """Track records processed"""
        self._result.records_processed += count

    def add_records_inserted(self, count: int):
        """Track records inserted"""
        self._result.records_inserted += count

    def add_records_updated(self, count: int):
        """Track records updated"""
        self._result.records_updated += count

    def add_records_failed(self, count: int):
        """Track records that failed"""
        self._result.records_failed += count

    def set_metadata(self, key: str, value: Any):
        """Set result metadata"""
        self._result.metadata[key] = value

    def complete(self, success: bool = None) -> AgentResult:
        """
        Mark agent as complete and return result.

        Args:
            success: Override success status

        Returns:
            AgentResult with final statistics
        """
        self._result.completed_at = datetime.now()

        if success is not None:
            self._result.success = success
        elif self._result.errors:
            self._result.success = False

        # Log completion
        event_type = (
            EventType.AGENT_COMPLETE if self._result.success
            else EventType.AGENT_ERROR
        )

        self.log_event(
            event_type,
            f"{self.agent_name} completed",
            data=self._result.to_dict(),
            duration_ms=int(self._result.duration_seconds * 1000)
        )

        # Close database connection
        self.close_connection()

        return self._result

    # =========================================================================
    # CONVENIENCE METHODS
    # =========================================================================

    def get_reference_data(self, table: str, columns: List[str] = None) -> List[Dict]:
        """
        Get reference data from silver layer.

        Args:
            table: Table name (e.g., 'trade_commodity_reference')
            columns: Columns to fetch (default: all)

        Returns:
            List of dicts with reference data
        """
        col_str = ', '.join(columns) if columns else '*'
        query = f"SELECT {col_str} FROM silver.{table}"

        cursor = self.conn.cursor()
        cursor.execute(query)

        if columns:
            col_names = columns
        else:
            col_names = [desc[0] for desc in cursor.description]

        results = []
        for row in cursor.fetchall():
            results.append(dict(zip(col_names, row)))

        cursor.close()
        return results

    def get_historical_country_mapping(
        self,
        historical_code: str
    ) -> Optional[Dict[str, Any]]:
        """
        Get current country for a historical code.

        Args:
            historical_code: Historical Census country code

        Returns:
            Dict with current country info or None
        """
        query = """
            SELECT current_code, current_name, is_primary_successor
            FROM silver.trade_country_historical
            WHERE historical_code = %s AND is_primary_successor = TRUE
            LIMIT 1
        """
        results = self.execute_query(query, (historical_code,))

        if results:
            return {
                'current_code': results[0][0],
                'current_name': results[0][1],
                'is_primary': results[0][2]
            }
        return None


# =============================================================================
# UTILITY FUNCTIONS
# =============================================================================

def get_today_log_files() -> List[Path]:
    """Get all Census log files from today"""
    today = date.today().isoformat()
    pattern = f"*_{today}.log"

    if not CENSUS_LOGS_DIR.exists():
        return []

    return list(CENSUS_LOGS_DIR.glob(pattern))


def parse_log_file(log_path: Path) -> List[LogEntry]:
    """
    Parse a Census log file into LogEntry objects.

    Args:
        log_path: Path to log file

    Returns:
        List of LogEntry objects
    """
    entries = []

    with open(log_path, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                entries.append(LogEntry.from_json(line))
            except json.JSONDecodeError:
                # Skip non-JSON lines
                continue

    return entries


def summarize_log_entries(entries: List[LogEntry]) -> Dict[str, Any]:
    """
    Summarize log entries into pipeline statistics.

    Args:
        entries: List of LogEntry objects

    Returns:
        Summary dict with counts and issues
    """
    summary = {
        'total_events': len(entries),
        'agents': {},
        'layers': {
            'bronze': {'events': 0, 'errors': 0, 'warnings': 0},
            'silver': {'events': 0, 'errors': 0, 'warnings': 0},
            'gold': {'events': 0, 'errors': 0, 'warnings': 0}
        },
        'errors': [],
        'warnings': [],
        'verifications': {
            'passed': 0,
            'failed': 0
        }
    }

    for entry in entries:
        # Count by layer
        layer = entry.layer
        if layer in summary['layers']:
            summary['layers'][layer]['events'] += 1

        # Count by agent
        agent = entry.agent
        if agent not in summary['agents']:
            summary['agents'][agent] = {
                'events': 0, 'errors': 0, 'success': None
            }
        summary['agents'][agent]['events'] += 1

        # Track errors and warnings
        if entry.event_type in (EventType.ERROR.value, EventType.AGENT_ERROR.value):
            summary['errors'].append({
                'agent': agent,
                'message': entry.message,
                'timestamp': entry.timestamp
            })
            summary['agents'][agent]['errors'] += 1
            if layer in summary['layers']:
                summary['layers'][layer]['errors'] += 1

        if entry.event_type in (EventType.WARNING.value, EventType.VERIFICATION_WARNING.value):
            summary['warnings'].append({
                'agent': agent,
                'message': entry.message,
                'timestamp': entry.timestamp
            })
            if layer in summary['layers']:
                summary['layers'][layer]['warnings'] += 1

        # Track verifications
        if entry.event_type == EventType.VERIFICATION_PASS.value:
            summary['verifications']['passed'] += 1
        elif entry.event_type == EventType.VERIFICATION_FAIL.value:
            summary['verifications']['failed'] += 1

        # Track agent completion
        if entry.event_type == EventType.AGENT_COMPLETE.value:
            summary['agents'][agent]['success'] = True
        elif entry.event_type == EventType.AGENT_ERROR.value:
            summary['agents'][agent]['success'] = False

    return summary
