"""
RLC Orchestrator - Built-in Health Check Agent
================================================
This is a simple example agent that demonstrates the pattern for
creating agents in the RLC system. It performs a basic health check
of the orchestrator infrastructure.

Every agent follows a similar pattern:
1. A run() function that performs the agent's work
2. Input parameters come from the task payload
3. Results are returned as a dictionary
4. Errors are raised as exceptions (the executor handles them)

This agent is called by the sample "System Health Check" task
that's created when the database is first initialized.
"""

import os
import sys
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, Any

logger = logging.getLogger(__name__)


def run(**kwargs) -> Dict[str, Any]:
    """
    Perform a health check of the orchestrator system.
    
    This function checks:
    - Python environment is correct
    - Required directories exist
    - Database is accessible
    - Disk space is adequate
    
    Returns:
        Dictionary with health check results
    """
    results = {
        "timestamp": datetime.utcnow().isoformat(),
        "status": "healthy",
        "checks": {}
    }
    
    # Check Python version
    python_version = sys.version_info
    results["checks"]["python_version"] = {
        "value": f"{python_version.major}.{python_version.minor}.{python_version.micro}",
        "status": "ok" if python_version.major == 3 and python_version.minor >= 10 else "warning"
    }
    
    # Check required directories
    required_dirs = ["data", "logs", "agents"]
    for dir_name in required_dirs:
        dir_path = Path(dir_name)
        exists = dir_path.exists()
        results["checks"][f"directory_{dir_name}"] = {
            "value": str(dir_path.absolute()) if exists else "MISSING",
            "status": "ok" if exists else "error"
        }
        if not exists:
            results["status"] = "unhealthy"
    
    # Check database file
    db_path = Path("data/orchestrator.db")
    if db_path.exists():
        db_size = db_path.stat().st_size
        results["checks"]["database"] = {
            "value": f"{db_size / 1024:.1f} KB",
            "status": "ok"
        }
    else:
        results["checks"]["database"] = {
            "value": "NOT FOUND",
            "status": "error"
        }
        results["status"] = "unhealthy"
    
    # Check disk space
    try:
        import shutil
        total, used, free = shutil.disk_usage("/")
        free_gb = free / (1024 ** 3)
        results["checks"]["disk_space"] = {
            "value": f"{free_gb:.1f} GB free",
            "status": "ok" if free_gb > 1 else "warning" if free_gb > 0.5 else "error"
        }
        if free_gb <= 0.5:
            results["status"] = "unhealthy"
    except Exception as e:
        results["checks"]["disk_space"] = {
            "value": f"ERROR: {e}",
            "status": "error"
        }
    
    # Check memory (basic)
    try:
        import resource
        usage = resource.getrusage(resource.RUSAGE_SELF)
        mem_mb = usage.ru_maxrss / 1024  # Convert to MB (on Linux)
        results["checks"]["memory_usage"] = {
            "value": f"{mem_mb:.1f} MB",
            "status": "ok" if mem_mb < 500 else "warning"
        }
    except Exception:
        results["checks"]["memory_usage"] = {
            "value": "Unable to check",
            "status": "unknown"
        }
    
    # Log the results
    logger.info(f"Health check completed: {results['status']}")
    for check_name, check_result in results["checks"].items():
        log_level = logging.INFO if check_result["status"] == "ok" else logging.WARNING
        logger.log(log_level, f"  {check_name}: {check_result['value']} ({check_result['status']})")
    
    return results


def get_description() -> str:
    """Return a description of what this agent does."""
    return """
    Health Check Agent
    ==================
    Performs a basic health check of the orchestrator system.
    
    Checks performed:
    - Python version (3.10+ recommended)
    - Required directories exist (data, logs, agents)
    - Database file is present and accessible
    - Adequate disk space (warns below 1GB, errors below 500MB)
    - Memory usage
    
    Run periodically (e.g., every 15 minutes) to catch issues early.
    """


if __name__ == "__main__":
    # Allow running directly for testing
    logging.basicConfig(level=logging.INFO)
    result = run()
    import json
    print(json.dumps(result, indent=2))
