"""
RLC Agent Tools Module

This module provides tools that the Master Agent can call to interact with
the file system, run data collectors, query databases, and more.

Each tool is a callable that takes specific parameters and returns a result
that can be fed back to the LLM for reasoning.
"""

import os
import sys
import json
import subprocess
from pathlib import Path
from datetime import datetime
from typing import Optional, Dict, Any, List
import sqlite3

# Base paths
RLC_ROOT = Path("C:/RLC") if sys.platform == "win32" else Path("/home/user/RLC-Agent")
PROJECT_ROOT = RLC_ROOT / "projects" / "rlc-agent" if sys.platform == "win32" else Path("/home/user/RLC-Agent")
DATA_DIR = PROJECT_ROOT / "data"
COLLECTORS_DIR = PROJECT_ROOT / "commodity_pipeline" / "data_collectors" / "collectors"


class ToolResult:
    """Standardized result from tool execution."""

    def __init__(self, success: bool, data: Any = None, error: str = None):
        self.success = success
        self.data = data
        self.error = error

    def to_dict(self) -> dict:
        return {
            "success": self.success,
            "data": self.data,
            "error": self.error
        }

    def __str__(self) -> str:
        if self.success:
            if isinstance(self.data, str):
                return self.data
            return json.dumps(self.data, indent=2, default=str)
        return f"Error: {self.error}"


# =============================================================================
# FILE SYSTEM TOOLS
# =============================================================================

def read_file(file_path: str, max_lines: int = 100) -> ToolResult:
    """
    Read contents of a file.

    Args:
        file_path: Path to the file (absolute or relative to project)
        max_lines: Maximum lines to return (default 100)

    Returns:
        ToolResult with file contents
    """
    try:
        path = Path(file_path)
        if not path.is_absolute():
            path = PROJECT_ROOT / path

        if not path.exists():
            return ToolResult(False, error=f"File not found: {path}")

        if not path.is_file():
            return ToolResult(False, error=f"Not a file: {path}")

        # Check file size
        size_mb = path.stat().st_size / (1024 * 1024)
        if size_mb > 10:
            return ToolResult(False, error=f"File too large ({size_mb:.1f}MB). Max 10MB.")

        with open(path, 'r', encoding='utf-8', errors='ignore') as f:
            lines = f.readlines()[:max_lines]
            content = ''.join(lines)

            if len(lines) == max_lines:
                content += f"\n... (truncated, showing first {max_lines} lines)"

        return ToolResult(True, data=content)

    except Exception as e:
        return ToolResult(False, error=str(e))


def list_directory(dir_path: str = ".", pattern: str = "*") -> ToolResult:
    """
    List contents of a directory.

    Args:
        dir_path: Directory path (default: project root)
        pattern: Glob pattern to filter (default: all files)

    Returns:
        ToolResult with list of files/folders
    """
    try:
        path = Path(dir_path)
        if not path.is_absolute():
            path = PROJECT_ROOT / path

        if not path.exists():
            return ToolResult(False, error=f"Directory not found: {path}")

        if not path.is_dir():
            return ToolResult(False, error=f"Not a directory: {path}")

        items = []
        for item in sorted(path.glob(pattern)):
            item_type = "DIR" if item.is_dir() else "FILE"
            size = item.stat().st_size if item.is_file() else 0
            items.append({
                "name": item.name,
                "type": item_type,
                "size": size,
                "path": str(item.relative_to(PROJECT_ROOT)) if str(item).startswith(str(PROJECT_ROOT)) else str(item)
            })

        return ToolResult(True, data=items)

    except Exception as e:
        return ToolResult(False, error=str(e))


def write_file(file_path: str, content: str, append: bool = False) -> ToolResult:
    """
    Write content to a file.

    Args:
        file_path: Path to the file
        content: Content to write
        append: If True, append to file; otherwise overwrite

    Returns:
        ToolResult indicating success/failure
    """
    try:
        path = Path(file_path)
        if not path.is_absolute():
            path = PROJECT_ROOT / path

        # Create parent directories if needed
        path.parent.mkdir(parents=True, exist_ok=True)

        mode = 'a' if append else 'w'
        with open(path, mode, encoding='utf-8') as f:
            f.write(content)

        return ToolResult(True, data=f"Successfully wrote to {path}")

    except Exception as e:
        return ToolResult(False, error=str(e))


# =============================================================================
# DATA COLLECTOR TOOLS
# =============================================================================

def list_collectors() -> ToolResult:
    """
    List all available data collectors.

    Returns:
        ToolResult with list of collectors and their status
    """
    collectors = []

    # Check collectors directory
    if COLLECTORS_DIR.exists():
        for f in COLLECTORS_DIR.glob("*_collector.py"):
            collectors.append({
                "name": f.stem.replace("_collector", ""),
                "file": f.name,
                "path": str(f.relative_to(PROJECT_ROOT))
            })

    # Check agent directories
    agent_dirs = [
        PROJECT_ROOT / "commodity_pipeline" / "usda_ams_agent",
        PROJECT_ROOT / "commodity_pipeline" / "export_inspections_agent",
        PROJECT_ROOT / "commodity_pipeline" / "south_america_trade_data",
    ]

    for agent_dir in agent_dirs:
        if agent_dir.exists():
            collectors.append({
                "name": agent_dir.name,
                "type": "agent",
                "path": str(agent_dir.relative_to(PROJECT_ROOT))
            })

    return ToolResult(True, data=collectors)


def run_collector(collector_name: str, **kwargs) -> ToolResult:
    """
    Run a specific data collector.

    Args:
        collector_name: Name of the collector (e.g., 'usda_fas', 'cftc_cot')
        **kwargs: Additional arguments for the collector

    Returns:
        ToolResult with collection results
    """
    try:
        # Map collector names to their scripts
        collector_map = {
            "usda_fas": COLLECTORS_DIR / "usda_fas_collector.py",
            "usda_nass": COLLECTORS_DIR / "usda_nass_collector.py",
            "cftc_cot": COLLECTORS_DIR / "cftc_cot_collector.py",
            "eia_ethanol": COLLECTORS_DIR / "eia_ethanol_collector.py",
            "drought": COLLECTORS_DIR / "drought_collector.py",
            "cme_settlements": COLLECTORS_DIR / "cme_settlements_collector.py",
            "conab": COLLECTORS_DIR / "conab_collector.py",
            "abiove": COLLECTORS_DIR / "abiove_collector.py",
        }

        if collector_name not in collector_map:
            available = ", ".join(collector_map.keys())
            return ToolResult(False, error=f"Unknown collector: {collector_name}. Available: {available}")

        script_path = collector_map[collector_name]
        if not script_path.exists():
            return ToolResult(False, error=f"Collector script not found: {script_path}")

        # Run the collector
        result = subprocess.run(
            [sys.executable, str(script_path)],
            capture_output=True,
            text=True,
            timeout=300,  # 5 minute timeout
            cwd=str(PROJECT_ROOT)
        )

        if result.returncode == 0:
            return ToolResult(True, data={
                "collector": collector_name,
                "output": result.stdout[-2000:] if len(result.stdout) > 2000 else result.stdout,
                "status": "completed"
            })
        else:
            return ToolResult(False, error=f"Collector failed: {result.stderr[-1000:]}")

    except subprocess.TimeoutExpired:
        return ToolResult(False, error="Collector timed out after 5 minutes")
    except Exception as e:
        return ToolResult(False, error=str(e))


# =============================================================================
# DATABASE TOOLS
# =============================================================================

def query_database(sql: str, limit: int = 100) -> ToolResult:
    """
    Execute a SQL query on the commodity database.

    Args:
        sql: SQL query (SELECT only for safety)
        limit: Maximum rows to return

    Returns:
        ToolResult with query results
    """
    try:
        # Only allow SELECT queries for safety
        sql_lower = sql.lower().strip()
        if not sql_lower.startswith("select"):
            return ToolResult(False, error="Only SELECT queries are allowed for safety")

        # Dangerous keywords check
        dangerous = ["drop", "delete", "update", "insert", "alter", "truncate", "exec", ";--"]
        for keyword in dangerous:
            if keyword in sql_lower:
                return ToolResult(False, error=f"Query contains forbidden keyword: {keyword}")

        # Find the database
        db_path = DATA_DIR / "rlc_commodities.db"
        if not db_path.exists():
            # Try alternate locations
            alt_paths = [
                PROJECT_ROOT / "data" / "rlc_commodities.db",
                PROJECT_ROOT / "commodity.db",
            ]
            for alt in alt_paths:
                if alt.exists():
                    db_path = alt
                    break
            else:
                return ToolResult(False, error="Database not found. Run database initialization first.")

        # Add LIMIT if not present
        if "limit" not in sql_lower:
            sql = f"{sql.rstrip(';')} LIMIT {limit}"

        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        cursor.execute(sql)
        rows = cursor.fetchall()

        # Convert to list of dicts
        results = [dict(row) for row in rows]

        conn.close()

        return ToolResult(True, data={
            "query": sql,
            "row_count": len(results),
            "rows": results
        })

    except Exception as e:
        return ToolResult(False, error=str(e))


def list_tables() -> ToolResult:
    """
    List all tables in the commodity database.

    Returns:
        ToolResult with list of tables and their schemas
    """
    try:
        db_path = DATA_DIR / "rlc_commodities.db"
        if not db_path.exists():
            return ToolResult(False, error="Database not found")

        conn = sqlite3.connect(str(db_path))
        cursor = conn.cursor()

        # Get all tables
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
        tables = [row[0] for row in cursor.fetchall()]

        # Get schema for each table
        table_info = []
        for table in tables:
            cursor.execute(f"PRAGMA table_info({table})")
            columns = [{"name": row[1], "type": row[2]} for row in cursor.fetchall()]

            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            row_count = cursor.fetchone()[0]

            table_info.append({
                "name": table,
                "columns": columns,
                "row_count": row_count
            })

        conn.close()

        return ToolResult(True, data=table_info)

    except Exception as e:
        return ToolResult(False, error=str(e))


# =============================================================================
# WEB/SEARCH TOOLS
# =============================================================================

def search_web(query: str, max_results: int = 5) -> ToolResult:
    """
    Search the web for information (using DuckDuckGo).

    Args:
        query: Search query
        max_results: Maximum results to return

    Returns:
        ToolResult with search results
    """
    try:
        # Try to use duckduckgo-search if available
        try:
            from duckduckgo_search import DDGS

            with DDGS() as ddgs:
                results = list(ddgs.text(query, max_results=max_results))

            return ToolResult(True, data={
                "query": query,
                "results": results
            })

        except ImportError:
            return ToolResult(False, error="Web search not available. Install: pip install duckduckgo-search")

    except Exception as e:
        return ToolResult(False, error=str(e))


def fetch_url(url: str) -> ToolResult:
    """
    Fetch content from a URL.

    Args:
        url: URL to fetch

    Returns:
        ToolResult with page content (text extracted)
    """
    try:
        import requests
        from bs4 import BeautifulSoup

        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        }

        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()

        soup = BeautifulSoup(response.text, 'html.parser')

        # Remove script and style elements
        for script in soup(["script", "style"]):
            script.decompose()

        # Get text
        text = soup.get_text(separator='\n', strip=True)

        # Truncate if too long
        if len(text) > 5000:
            text = text[:5000] + "\n... (truncated)"

        return ToolResult(True, data={
            "url": url,
            "content": text
        })

    except Exception as e:
        return ToolResult(False, error=str(e))


# =============================================================================
# SYSTEM TOOLS
# =============================================================================

def get_system_status() -> ToolResult:
    """
    Get current system status.

    Returns:
        ToolResult with system information
    """
    try:
        import platform

        status = {
            "timestamp": datetime.now().isoformat(),
            "platform": platform.system(),
            "python_version": platform.python_version(),
            "project_root": str(PROJECT_ROOT),
            "data_dir_exists": DATA_DIR.exists(),
            "collectors_dir_exists": COLLECTORS_DIR.exists(),
        }

        # Check database
        db_path = DATA_DIR / "rlc_commodities.db"
        if db_path.exists():
            status["database"] = {
                "path": str(db_path),
                "size_mb": round(db_path.stat().st_size / (1024 * 1024), 2)
            }
        else:
            status["database"] = None

        # Check for transcripts
        transcripts_dir = Path("C:/RLC/whisper/transcripts")
        if transcripts_dir.exists():
            today = datetime.now().strftime("%Y-%m-%d")
            today_file = transcripts_dir / f"transcript_{today}.jsonl"
            status["transcripts"] = {
                "dir_exists": True,
                "today_file_exists": today_file.exists()
            }

        return ToolResult(True, data=status)

    except Exception as e:
        return ToolResult(False, error=str(e))


def run_python_code(code: str) -> ToolResult:
    """
    Execute Python code safely.

    Args:
        code: Python code to execute

    Returns:
        ToolResult with execution output
    """
    try:
        # Security check - block dangerous operations
        dangerous_patterns = [
            "import os", "import sys", "import subprocess",
            "exec(", "eval(", "__import__",
            "open(", "file(", "input(",
            "rm ", "del ", "shutil",
        ]

        for pattern in dangerous_patterns:
            if pattern in code:
                return ToolResult(False, error=f"Code contains blocked pattern: {pattern}")

        # Create a restricted namespace
        namespace = {
            "pd": None,
            "np": None,
            "datetime": datetime,
            "json": json,
        }

        # Try to import common data libraries
        try:
            import pandas as pd
            import numpy as np
            namespace["pd"] = pd
            namespace["np"] = np
        except ImportError:
            pass

        # Capture output
        import io
        from contextlib import redirect_stdout

        output = io.StringIO()
        with redirect_stdout(output):
            exec(code, namespace)

        result = output.getvalue()

        return ToolResult(True, data={
            "code": code,
            "output": result if result else "(no output)"
        })

    except Exception as e:
        return ToolResult(False, error=str(e))


# =============================================================================
# TOOL REGISTRY
# =============================================================================

TOOLS = {
    # File system
    "read_file": {
        "function": read_file,
        "description": "Read contents of a file",
        "parameters": {
            "file_path": "Path to the file",
            "max_lines": "Maximum lines to return (default 100)"
        }
    },
    "list_directory": {
        "function": list_directory,
        "description": "List contents of a directory",
        "parameters": {
            "dir_path": "Directory path (default: project root)",
            "pattern": "Glob pattern to filter (default: *)"
        }
    },
    "write_file": {
        "function": write_file,
        "description": "Write content to a file",
        "parameters": {
            "file_path": "Path to the file",
            "content": "Content to write",
            "append": "If True, append; otherwise overwrite"
        }
    },

    # Data collectors
    "list_collectors": {
        "function": list_collectors,
        "description": "List all available data collectors",
        "parameters": {}
    },
    "run_collector": {
        "function": run_collector,
        "description": "Run a specific data collector",
        "parameters": {
            "collector_name": "Name of collector (usda_fas, cftc_cot, etc.)"
        }
    },

    # Database
    "query_database": {
        "function": query_database,
        "description": "Execute a SQL SELECT query on the commodity database",
        "parameters": {
            "sql": "SQL query (SELECT only)",
            "limit": "Maximum rows (default 100)"
        }
    },
    "list_tables": {
        "function": list_tables,
        "description": "List all tables in the commodity database",
        "parameters": {}
    },

    # Web
    "search_web": {
        "function": search_web,
        "description": "Search the web for information",
        "parameters": {
            "query": "Search query",
            "max_results": "Maximum results (default 5)"
        }
    },
    "fetch_url": {
        "function": fetch_url,
        "description": "Fetch and extract text from a URL",
        "parameters": {
            "url": "URL to fetch"
        }
    },

    # System
    "get_system_status": {
        "function": get_system_status,
        "description": "Get current system status",
        "parameters": {}
    },
    "run_python_code": {
        "function": run_python_code,
        "description": "Execute Python code (data analysis only, restricted)",
        "parameters": {
            "code": "Python code to execute"
        }
    }
}


def get_tools_description() -> str:
    """Get a formatted description of all available tools."""
    lines = ["Available tools:\n"]

    for name, info in TOOLS.items():
        lines.append(f"- {name}: {info['description']}")
        if info['parameters']:
            for param, desc in info['parameters'].items():
                lines.append(f"    {param}: {desc}")

    return "\n".join(lines)


def execute_tool(tool_name: str, **kwargs) -> ToolResult:
    """Execute a tool by name with given parameters."""
    if tool_name not in TOOLS:
        return ToolResult(False, error=f"Unknown tool: {tool_name}")

    tool_func = TOOLS[tool_name]["function"]
    return tool_func(**kwargs)
