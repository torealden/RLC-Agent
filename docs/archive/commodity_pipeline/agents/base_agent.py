"""
Base Agent Class
================
Abstract base class for all commodity pipeline agents.
Provides common functionality for:
- Database connections
- Logging
- Error handling
- Metrics/telemetry

All agents should inherit from this class.
"""

import os
import sys
import json
import logging
from abc import ABC, abstractmethod
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict, Any, List
from dataclasses import dataclass, field

# Third-party imports
try:
    import psycopg2
    from psycopg2.extras import Json
except ImportError:
    psycopg2 = None

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass


# =============================================================================
# Data Classes
# =============================================================================

@dataclass
class AgentResult:
    """Standard result object for agent runs."""
    agent_name: str
    status: str  # 'success', 'partial', 'failed'
    started_at: datetime = field(default_factory=datetime.now)
    completed_at: Optional[datetime] = None
    records_fetched: int = 0
    records_inserted: int = 0
    records_updated: int = 0
    records_failed: int = 0
    error_message: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict:
        return {
            "agent_name": self.agent_name,
            "status": self.status,
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
            "records_fetched": self.records_fetched,
            "records_inserted": self.records_inserted,
            "records_updated": self.records_updated,
            "records_failed": self.records_failed,
            "error_message": self.error_message,
            "metadata": self.metadata
        }


# =============================================================================
# Base Agent
# =============================================================================

class BaseAgent(ABC):
    """
    Abstract base class for all data pipeline agents.

    Subclasses must implement:
    - run(): Main execution logic
    - get_name(): Return agent name

    Optional overrides:
    - setup(): Called before run()
    - teardown(): Called after run()
    - validate(): Validate data after fetch
    """

    def __init__(self, config: Optional[Dict] = None):
        self.config = config or {}
        self.conn = None
        self.logger = self._setup_logging()
        self._result = None

    def _setup_logging(self) -> logging.Logger:
        """Configure logging for this agent."""
        logger = logging.getLogger(self.get_name())
        logger.setLevel(logging.INFO)

        if not logger.handlers:
            # Console handler
            console = logging.StreamHandler()
            console.setFormatter(logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            ))
            logger.addHandler(console)

            # File handler
            log_dir = Path(__file__).parent.parent / "logs"
            log_dir.mkdir(exist_ok=True)
            file_handler = logging.FileHandler(log_dir / f"{self.get_name()}.log")
            file_handler.setFormatter(logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            ))
            logger.addHandler(file_handler)

        return logger

    @abstractmethod
    def get_name(self) -> str:
        """Return the agent name. Used for logging and metrics."""
        pass

    @abstractmethod
    def run(self) -> AgentResult:
        """
        Main agent execution logic.

        Must return an AgentResult object.
        """
        pass

    def setup(self):
        """Called before run(). Override for custom setup."""
        pass

    def teardown(self):
        """Called after run(). Override for custom cleanup."""
        pass

    def validate(self, data: Any) -> bool:
        """Validate fetched data. Override for custom validation."""
        return True

    # =========================================================================
    # Database Methods
    # =========================================================================

    def connect_db(self):
        """Establish database connection."""
        if psycopg2 is None:
            raise ImportError("psycopg2 not installed. Run: pip install psycopg2-binary")

        database_url = os.environ.get("DATABASE_URL")
        if database_url:
            self.conn = psycopg2.connect(database_url)
        else:
            self.conn = psycopg2.connect(
                host=os.environ.get("DB_HOST", "localhost"),
                port=os.environ.get("DB_PORT", "5432"),
                dbname=os.environ.get("DB_NAME", "rlc_commodities"),
                user=os.environ.get("DB_USER", "postgres"),
                password=os.environ.get("DB_PASSWORD", "")
            )
        self.logger.info("Database connection established")

    def close_db(self):
        """Close database connection."""
        if self.conn:
            self.conn.close()
            self.conn = None
            self.logger.debug("Database connection closed")

    def ensure_schemas(self):
        """Ensure required schemas exist."""
        if not self.conn:
            raise RuntimeError("Not connected to database")

        with self.conn.cursor() as cur:
            cur.execute("CREATE SCHEMA IF NOT EXISTS bronze")
            cur.execute("CREATE SCHEMA IF NOT EXISTS silver")
            cur.execute("CREATE SCHEMA IF NOT EXISTS gold")
            cur.execute("CREATE SCHEMA IF NOT EXISTS meta")
        self.conn.commit()

    # =========================================================================
    # Execution Wrapper
    # =========================================================================

    def execute(self) -> AgentResult:
        """
        Full execution wrapper with setup/teardown and error handling.

        This is the main entry point for running an agent.
        """
        self._result = AgentResult(
            agent_name=self.get_name(),
            status="pending",
            started_at=datetime.now()
        )

        try:
            self.logger.info(f"Starting agent: {self.get_name()}")
            self.setup()
            self._result = self.run()
            self._result.completed_at = datetime.now()

            if self._result.status == "pending":
                self._result.status = "success"

        except Exception as e:
            self._result.status = "failed"
            self._result.error_message = str(e)
            self._result.completed_at = datetime.now()
            self.logger.exception(f"Agent failed: {e}")

        finally:
            self.teardown()
            self._log_result()

        self.logger.info(f"Agent completed: {self._result.status}")
        return self._result

    def _log_result(self):
        """Log result to meta table and file."""
        # Log to file
        log_dir = Path(__file__).parent.parent / "logs"
        results_file = log_dir / "agent_results.jsonl"

        with open(results_file, "a") as f:
            f.write(json.dumps(self._result.to_dict(), default=str) + "\n")

        # Log to database if connected
        if self.conn:
            try:
                with self.conn.cursor() as cur:
                    cur.execute("""
                        INSERT INTO meta.agent_execution_log
                        (agent_name, status, started_at, completed_at,
                         records_fetched, records_inserted, records_updated,
                         records_failed, error_message, metadata)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """, (
                        self._result.agent_name,
                        self._result.status,
                        self._result.started_at,
                        self._result.completed_at,
                        self._result.records_fetched,
                        self._result.records_inserted,
                        self._result.records_updated,
                        self._result.records_failed,
                        self._result.error_message,
                        Json(self._result.metadata)
                    ))
                self.conn.commit()
            except Exception as e:
                self.logger.warning(f"Failed to log to database: {e}")


# =============================================================================
# Utility Functions
# =============================================================================

def load_env():
    """Load environment variables from .env file."""
    try:
        from dotenv import load_dotenv
        env_path = Path(__file__).parent.parent / ".env"
        if env_path.exists():
            load_dotenv(env_path)
            return True
    except ImportError:
        pass
    return False
