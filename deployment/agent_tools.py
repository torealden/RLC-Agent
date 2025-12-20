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
# DOCUMENT SEARCH (RAG) TOOLS
# =============================================================================

def search_documents(query: str, top_k: int = 5) -> ToolResult:
    """
    Search indexed documents (Excel, PDF, Markdown) using semantic search.

    Uses local embeddings via Ollama's nomic-embed-text model to find
    relevant content from balance sheets, analysis files, and documentation.

    Args:
        query: Natural language search query
        top_k: Number of results to return (default 5)

    Returns:
        ToolResult with matching document chunks and scores
    """
    try:
        from document_rag import search_documents_sync, get_index_stats

        # Check if index exists
        stats = get_index_stats()
        if not stats.get("indexed"):
            return ToolResult(False, error=stats.get("message", "No index found"))

        # Perform search
        result = search_documents_sync(query, top_k)

        if not result["success"]:
            return ToolResult(False, error=result.get("error", "Search failed"))

        # Format results for display
        formatted_results = []
        for r in result["results"]:
            formatted_results.append({
                "file": r["file_name"],
                "path": r["file_path"],
                "score": round(r["score"], 3),
                "content": r["content"][:500] + "..." if len(r["content"]) > 500 else r["content"]
            })

        return ToolResult(True, data={
            "query": query,
            "result_count": result["result_count"],
            "results": formatted_results
        })

    except ImportError:
        return ToolResult(False, error="RAG module not found. Run: python document_rag.py --index")
    except Exception as e:
        return ToolResult(False, error=str(e))


def get_rag_stats() -> ToolResult:
    """
    Get statistics about the document RAG index.

    Returns:
        ToolResult with index statistics (file counts, chunk counts, etc.)
    """
    try:
        from document_rag import get_index_stats

        stats = get_index_stats()
        return ToolResult(True, data=stats)

    except ImportError:
        return ToolResult(False, error="RAG module not found")
    except Exception as e:
        return ToolResult(False, error=str(e))


# =============================================================================
# EMAIL & CALENDAR TOOLS
# =============================================================================

# Cache for Google services
_gmail_service = None
_calendar_service = None


def _get_google_credentials(service_type: str = "gmail"):
    """Get Google OAuth credentials from token file."""
    import pickle

    # Look for token in multiple locations
    possible_paths = [
        PROJECT_ROOT / "data" / "tokens" / f"{service_type}_token.pickle",
        PROJECT_ROOT / "rlc_master_agent" / "config" / "tokens" / f"{service_type}_token.pickle",
        Path.home() / ".rlc" / "tokens" / f"{service_type}_token.pickle",
    ]

    for token_path in possible_paths:
        if token_path.exists():
            with open(token_path, 'rb') as f:
                creds = pickle.load(f)
                if creds and creds.valid:
                    return creds
                elif creds and creds.expired and creds.refresh_token:
                    try:
                        from google.auth.transport.requests import Request
                        creds.refresh(Request())
                        with open(token_path, 'wb') as f:
                            pickle.dump(creds, f)
                        return creds
                    except Exception:
                        pass

    return None


def _get_gmail_service():
    """Get Gmail API service."""
    global _gmail_service
    if _gmail_service:
        return _gmail_service

    try:
        from googleapiclient.discovery import build
        creds = _get_google_credentials("gmail")
        if creds:
            _gmail_service = build('gmail', 'v1', credentials=creds)
            return _gmail_service
    except ImportError:
        pass
    except Exception:
        pass

    return None


def _get_calendar_service():
    """Get Google Calendar API service."""
    global _calendar_service
    if _calendar_service:
        return _calendar_service

    try:
        from googleapiclient.discovery import build
        creds = _get_google_credentials("calendar")
        if creds:
            _calendar_service = build('calendar', 'v3', credentials=creds)
            return _calendar_service
    except ImportError:
        pass
    except Exception:
        pass

    return None


def check_email(max_results: int = 10, query: str = "is:unread") -> ToolResult:
    """
    Check emails from Gmail.

    Args:
        max_results: Maximum number of emails to return (default 10)
        query: Gmail search query (default: unread emails)

    Returns:
        ToolResult with email summaries
    """
    try:
        service = _get_gmail_service()
        if not service:
            return ToolResult(
                False,
                error="Gmail not connected. Run: python deployment/setup_google_oauth.py"
            )

        # Fetch emails
        results = service.users().messages().list(
            userId='me',
            q=query,
            maxResults=max_results
        ).execute()

        messages = results.get('messages', [])

        if not messages:
            return ToolResult(True, data={
                "count": 0,
                "query": query,
                "emails": [],
                "message": "No emails found matching query"
            })

        emails = []
        for msg_info in messages[:max_results]:
            msg = service.users().messages().get(
                userId='me',
                id=msg_info['id'],
                format='metadata',
                metadataHeaders=['From', 'Subject', 'Date']
            ).execute()

            headers = {h['name']: h['value'] for h in msg.get('payload', {}).get('headers', [])}

            emails.append({
                "id": msg_info['id'],
                "from": headers.get('From', 'Unknown'),
                "subject": headers.get('Subject', '(No Subject)'),
                "date": headers.get('Date', ''),
                "snippet": msg.get('snippet', '')[:150],
                "is_unread": 'UNREAD' in msg.get('labelIds', [])
            })

        return ToolResult(True, data={
            "count": len(emails),
            "query": query,
            "emails": emails
        })

    except ImportError:
        return ToolResult(False, error="Google API packages not installed. Run: pip install google-api-python-client google-auth")
    except Exception as e:
        return ToolResult(False, error=str(e))


def get_email_content(email_id: str) -> ToolResult:
    """
    Get full content of a specific email.

    Args:
        email_id: The email ID to retrieve

    Returns:
        ToolResult with full email content
    """
    try:
        import base64
        service = _get_gmail_service()
        if not service:
            return ToolResult(False, error="Gmail not connected")

        msg = service.users().messages().get(
            userId='me',
            id=email_id,
            format='full'
        ).execute()

        headers = {h['name']: h['value'] for h in msg.get('payload', {}).get('headers', [])}

        # Extract body
        body = ""
        payload = msg.get('payload', {})
        if 'body' in payload and 'data' in payload['body']:
            body = base64.urlsafe_b64decode(payload['body']['data']).decode('utf-8', errors='ignore')
        elif 'parts' in payload:
            for part in payload['parts']:
                if part['mimeType'] == 'text/plain' and 'data' in part.get('body', {}):
                    body = base64.urlsafe_b64decode(part['body']['data']).decode('utf-8', errors='ignore')
                    break

        return ToolResult(True, data={
            "id": email_id,
            "from": headers.get('From', 'Unknown'),
            "to": headers.get('To', ''),
            "subject": headers.get('Subject', '(No Subject)'),
            "date": headers.get('Date', ''),
            "body": body[:2000] if body else "(No text content)",
            "labels": msg.get('labelIds', [])
        })

    except Exception as e:
        return ToolResult(False, error=str(e))


def check_calendar(days_ahead: int = 7) -> ToolResult:
    """
    Check calendar events.

    Args:
        days_ahead: Number of days to look ahead (default 7)

    Returns:
        ToolResult with calendar events
    """
    try:
        from datetime import datetime, timedelta

        service = _get_calendar_service()
        if not service:
            return ToolResult(
                False,
                error="Calendar not connected. Run: python deployment/setup_google_oauth.py"
            )

        now = datetime.utcnow()
        end = now + timedelta(days=days_ahead)

        events_result = service.events().list(
            calendarId='primary',
            timeMin=now.isoformat() + 'Z',
            timeMax=end.isoformat() + 'Z',
            maxResults=50,
            singleEvents=True,
            orderBy='startTime'
        ).execute()

        events = []
        for item in events_result.get('items', []):
            start = item.get('start', {})
            is_all_day = 'date' in start

            events.append({
                "id": item['id'],
                "title": item.get('summary', 'Untitled'),
                "start": start.get('date') if is_all_day else start.get('dateTime', ''),
                "end": item.get('end', {}).get('date') if is_all_day else item.get('end', {}).get('dateTime', ''),
                "location": item.get('location', ''),
                "description": (item.get('description', '') or '')[:200],
                "is_all_day": is_all_day,
                "link": item.get('htmlLink', '')
            })

        # Group by day
        today = now.date()
        today_events = [e for e in events if today.isoformat() in e['start']]

        return ToolResult(True, data={
            "total_events": len(events),
            "days_ahead": days_ahead,
            "today_count": len(today_events),
            "today_events": today_events,
            "all_events": events
        })

    except ImportError:
        return ToolResult(False, error="Google API packages not installed")
    except Exception as e:
        return ToolResult(False, error=str(e))


def get_todays_schedule() -> ToolResult:
    """
    Get today's calendar schedule.

    Returns:
        ToolResult with today's events
    """
    return check_calendar(days_ahead=1)


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
    },

    # Document Search (RAG)
    "search_documents": {
        "function": search_documents,
        "description": "Search indexed documents (Excel balance sheets, PDFs, Markdown) using semantic search",
        "parameters": {
            "query": "Natural language search query (e.g. 'soybean crush margins')",
            "top_k": "Number of results (default 5)"
        }
    },
    "get_rag_stats": {
        "function": get_rag_stats,
        "description": "Get statistics about the document index",
        "parameters": {}
    },

    # Email (Gmail)
    "check_email": {
        "function": check_email,
        "description": "Check Gmail inbox for emails (unread by default)",
        "parameters": {
            "max_results": "Number of emails to return (default 10)",
            "query": "Gmail search query (default: is:unread)"
        }
    },
    "get_email_content": {
        "function": get_email_content,
        "description": "Get the full content of a specific email by ID",
        "parameters": {
            "email_id": "The email ID to retrieve"
        }
    },

    # Calendar (Google Calendar)
    "check_calendar": {
        "function": check_calendar,
        "description": "Check upcoming calendar events",
        "parameters": {
            "days_ahead": "Number of days to look ahead (default 7)"
        }
    },
    "get_todays_schedule": {
        "function": get_todays_schedule,
        "description": "Get today's calendar schedule",
        "parameters": {}
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
