#!/usr/bin/env python3
"""
RLC Orchestrator - Main Entry Point
=====================================
This is the main entry point for the RLC Orchestrator system. It initializes
all components and starts the main execution loop.

To run the orchestrator:
    python main.py

To run as a systemd service, see scripts/setup_service.sh

The orchestrator will:
1. Initialize the database (creating tables if needed)
2. Load configuration from environment/config files
3. Start the scheduler for recurring tasks
4. Start the executor for processing tasks
5. Run indefinitely until stopped (Ctrl+C or systemd stop)
"""

import os
import sys
import logging
import argparse
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv

# Add project root to path
PROJECT_ROOT = Path(__file__).parent
sys.path.insert(0, str(PROJECT_ROOT))

from core.database import setup_database, get_engine, init_database
from core.queue import TaskQueue, TaskBuilder, TaskType
from core.executor import TaskExecutor
from core.security import SecurityGuard

# Configure logging
def setup_logging(log_level: str = "INFO", log_file: str = None):
    """
    Configure logging for the orchestrator.
    
    Logs go to both console and file (if specified). The format includes
    timestamps, log level, module name, and message - all the information
    needed for debugging production issues.
    """
    handlers = [logging.StreamHandler()]
    
    if log_file:
        # Ensure log directory exists
        log_path = Path(log_file)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        handlers.append(logging.FileHandler(log_file))
    
    logging.basicConfig(
        level=getattr(logging, log_level.upper()),
        format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
        handlers=handlers
    )
    
    # Reduce noise from third-party libraries
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("sqlalchemy").setLevel(logging.WARNING)

logger = logging.getLogger(__name__)


def ensure_directories():
    """
    Ensure all required directories exist.
    
    The orchestrator needs several directories for data, logs, and
    agent code. This function creates them if they don't exist.
    """
    directories = [
        PROJECT_ROOT / "data",
        PROJECT_ROOT / "logs",
        PROJECT_ROOT / "agents" / "installed",
        PROJECT_ROOT / "agents" / "templates",
        PROJECT_ROOT / "sandbox",
    ]
    
    for directory in directories:
        directory.mkdir(parents=True, exist_ok=True)
        logger.debug(f"Ensured directory exists: {directory}")


def create_sample_tasks(queue: TaskQueue):
    """
    Create some sample tasks to demonstrate the system.
    
    This is only run when the database is first initialized
    and is helpful for testing and understanding how tasks work.
    """
    # A simple script task
    queue.add_task(
        name="System Health Check",
        task_type=TaskType.SCRIPT,
        payload={
            "function": "agents.builtin.health_check.run",
            "args": {}
        },
        description="Periodic check that the system is functioning correctly",
        priority=1
    )
    
    logger.info("Created sample tasks for testing")


def main():
    """
    Main function - initializes and runs the orchestrator.
    """
    # Parse command line arguments
    parser = argparse.ArgumentParser(description="RLC Orchestrator")
    parser.add_argument(
        "--config", 
        default=".env",
        help="Path to configuration file (default: .env)"
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging level (default: INFO)"
    )
    parser.add_argument(
        "--db-path",
        default="data/orchestrator.db",
        help="Path to SQLite database (default: data/orchestrator.db)"
    )
    parser.add_argument(
        "--init-only",
        action="store_true",
        help="Initialize database and exit (don't start executor)"
    )
    parser.add_argument(
        "--with-samples",
        action="store_true",
        help="Create sample tasks on first run"
    )
    
    args = parser.parse_args()
    
    # Load environment variables
    if Path(args.config).exists():
        load_dotenv(args.config)
    
    # Setup logging
    log_file = os.getenv("LOG_FILE", "logs/orchestrator.log")
    setup_logging(args.log_level, log_file)
    
    logger.info("=" * 60)
    logger.info("RLC Orchestrator Starting")
    logger.info(f"Python version: {sys.version}")
    logger.info(f"Working directory: {os.getcwd()}")
    logger.info(f"Database: {args.db_path}")
    logger.info("=" * 60)
    
    # Ensure directories exist
    ensure_directories()
    
    # Initialize database
    db_path = Path(args.db_path)
    db_existed = db_path.exists()
    
    engine = get_engine(str(db_path))
    init_database(engine)
    
    if not db_existed:
        logger.info("Database created for the first time")
        if args.with_samples:
            from core.database import get_session
            session = get_session(engine)
            queue = TaskQueue(session)
            create_sample_tasks(queue)
            session.close()
    
    # If init-only, we're done
    if args.init_only:
        logger.info("Initialization complete (--init-only specified)")
        return 0
    
    # Initialize components
    # Note: AI gateway and email client will be added in later phases
    # For now, we run with just the core executor
    
    poll_interval = int(os.getenv("POLL_INTERVAL", "5"))
    
    executor = TaskExecutor(
        db_path=str(db_path),
        poll_interval=poll_interval,
        ai_gateway=None,  # Phase 3
        email_client=None  # Phase 2
    )
    
    # Run the main loop
    try:
        executor.run()
    except KeyboardInterrupt:
        logger.info("Received Ctrl+C, shutting down...")
        executor.stop()
    
    logger.info("Orchestrator stopped")
    return 0


if __name__ == "__main__":
    sys.exit(main())
