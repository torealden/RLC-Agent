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
    """Get Google OAuth credentials from token file (pickle or JSON format)."""
    import pickle

    # Look for token in multiple locations (pickle format first)
    pickle_paths = [
        PROJECT_ROOT / "data" / "tokens" / f"{service_type}_token.pickle",
        PROJECT_ROOT / "rlc_master_agent" / "config" / "tokens" / f"{service_type}_token.pickle",
        Path.home() / ".rlc" / "tokens" / f"{service_type}_token.pickle",
    ]

    for token_path in pickle_paths:
        if token_path.exists():
            try:
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
            except Exception:
                pass

    # Look for JSON format tokens (from older setup or rlc_master_agent)
    json_paths = [
        PROJECT_ROOT / "data" / "tokens" / f"{service_type}_token.json",
        PROJECT_ROOT / "data" / "tokens" / "work_token.json",
        PROJECT_ROOT / "rlc_master_agent" / "config" / "tokens" / f"{service_type}_token.json",
        PROJECT_ROOT / "Other Files" / "Desktop Assistant" / "token_work.json",
    ]

    for token_path in json_paths:
        if token_path.exists():
            try:
                from google.oauth2.credentials import Credentials
                from google.auth.transport.requests import Request

                with open(token_path, 'r') as f:
                    token_data = json.load(f)

                creds = Credentials(
                    token=token_data.get('token'),
                    refresh_token=token_data.get('refresh_token'),
                    token_uri=token_data.get('token_uri'),
                    client_id=token_data.get('client_id'),
                    client_secret=token_data.get('client_secret'),
                    scopes=token_data.get('scopes')
                )

                if creds and creds.valid:
                    return creds
                elif creds and creds.refresh_token:
                    try:
                        creds.refresh(Request())
                        # Save refreshed token
                        token_data['token'] = creds.token
                        with open(token_path, 'w') as f:
                            json.dump(token_data, f)
                        return creds
                    except Exception:
                        pass
            except Exception:
                pass

    return None


def _get_google_credentials_for_account(account: str = "work"):
    """
    Get Google OAuth credentials for a specific account.

    Args:
        account: "work" or "personal"
    """
    import pickle

    if account == "work":
        # Work account token locations
        paths = [
            PROJECT_ROOT / "data" / "tokens" / "gmail_token.pickle",
            PROJECT_ROOT / "data" / "tokens" / "work_token.json",
            PROJECT_ROOT / "rlc_master_agent" / "config" / "tokens" / "gmail_token.json",
            PROJECT_ROOT / "Other Files" / "Desktop Assistant" / "token_work.json",
        ]
    else:
        # Personal account token locations
        paths = [
            PROJECT_ROOT / "data" / "tokens" / "gmail_personal_token.pickle",
            PROJECT_ROOT / "data" / "tokens" / "personal_token.json",
            PROJECT_ROOT / "Other Files" / "Desktop Assistant" / "token_personal.json",
        ]

    for token_path in paths:
        if not token_path.exists():
            continue

        try:
            # Try pickle format
            if token_path.suffix == '.pickle':
                with open(token_path, 'rb') as f:
                    creds = pickle.load(f)
            else:
                # JSON format
                from google.oauth2.credentials import Credentials
                with open(token_path, 'r') as f:
                    token_data = json.load(f)
                creds = Credentials(
                    token=token_data.get('token'),
                    refresh_token=token_data.get('refresh_token'),
                    token_uri=token_data.get('token_uri'),
                    client_id=token_data.get('client_id'),
                    client_secret=token_data.get('client_secret'),
                    scopes=token_data.get('scopes')
                )

            if creds and creds.valid:
                return creds
            elif creds and creds.refresh_token:
                try:
                    from google.auth.transport.requests import Request
                    creds.refresh(Request())
                    # Save refreshed token
                    if token_path.suffix == '.pickle':
                        with open(token_path, 'wb') as f:
                            pickle.dump(creds, f)
                    return creds
                except Exception:
                    pass
        except Exception:
            pass

    return None


# Service caches for each account
_gmail_services = {}
_calendar_service = None


def _get_gmail_service(account: str = "work"):
    """Get Gmail API service for a specific account."""
    global _gmail_services

    if account in _gmail_services:
        return _gmail_services[account]

    try:
        from googleapiclient.discovery import build
        creds = _get_google_credentials_for_account(account)
        if creds:
            service = build('gmail', 'v1', credentials=creds)
            _gmail_services[account] = service
            return service
    except ImportError:
        pass
    except Exception:
        pass

    return None


def _get_calendar_service():
    """Get Google Calendar API service (always uses WORK account)."""
    global _calendar_service
    if _calendar_service:
        return _calendar_service

    try:
        from googleapiclient.discovery import build
        # Always use work account for calendar
        creds = _get_google_credentials_for_account("work")
        if creds:
            _calendar_service = build('calendar', 'v3', credentials=creds)
            return _calendar_service
    except ImportError:
        pass
    except Exception:
        pass

    return None


def check_email(max_results: int = 10, query: str = "is:unread", account: str = "both") -> ToolResult:
    """
    Check emails from Gmail.

    Args:
        max_results: Maximum number of emails to return (default 10)
        query: Gmail search query (default: unread emails)
        account: Which account - "work", "personal", or "both" (default: both)

    Returns:
        ToolResult with email summaries from specified account(s)
    """
    try:
        all_emails = []
        accounts_checked = []
        errors = []

        accounts_to_check = ["work", "personal"] if account == "both" else [account]

        for acct in accounts_to_check:
            service = _get_gmail_service(acct)
            if not service:
                if account != "both":  # Only error if specifically requested
                    errors.append(f"{acct} account not connected")
                continue

            try:
                # Get account email address
                profile = service.users().getProfile(userId='me').execute()
                email_address = profile.get('emailAddress', acct)
                accounts_checked.append(email_address)

                # Fetch emails
                results = service.users().messages().list(
                    userId='me',
                    q=query,
                    maxResults=max_results
                ).execute()

                messages = results.get('messages', [])

                for msg_info in messages[:max_results]:
                    msg = service.users().messages().get(
                        userId='me',
                        id=msg_info['id'],
                        format='metadata',
                        metadataHeaders=['From', 'Subject', 'Date']
                    ).execute()

                    headers = {h['name']: h['value'] for h in msg.get('payload', {}).get('headers', [])}

                    all_emails.append({
                        "id": msg_info['id'],
                        "account": email_address,
                        "from": headers.get('From', 'Unknown'),
                        "subject": headers.get('Subject', '(No Subject)'),
                        "date": headers.get('Date', ''),
                        "snippet": msg.get('snippet', '')[:150],
                        "is_unread": 'UNREAD' in msg.get('labelIds', [])
                    })
            except Exception as e:
                errors.append(f"{acct}: {str(e)}")

        if not accounts_checked and errors:
            return ToolResult(False, error=f"No email accounts connected. {'; '.join(errors)}")

        # Sort by date (most recent first)
        all_emails.sort(key=lambda x: x.get('date', ''), reverse=True)

        return ToolResult(True, data={
            "accounts_checked": accounts_checked,
            "count": len(all_emails),
            "query": query,
            "emails": all_emails[:max_results * 2] if account == "both" else all_emails[:max_results]
        })

    except ImportError:
        return ToolResult(False, error="Google API packages not installed. Run: pip install google-api-python-client google-auth")
    except Exception as e:
        return ToolResult(False, error=str(e))


def get_email_content(email_id: str, account: str = "work") -> ToolResult:
    """
    Get full content of a specific email.

    Args:
        email_id: The email ID to retrieve
        account: Which account the email is from - "work" or "personal" (default: work)

    Returns:
        ToolResult with full email content
    """
    try:
        import base64
        service = _get_gmail_service(account)
        if not service:
            # Try the other account
            other = "personal" if account == "work" else "work"
            service = _get_gmail_service(other)
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
# NOTION MEMORY TOOLS
# =============================================================================

_notion_client = None
MEMORY_TYPES = ["preference", "fact", "decision", "observation", "process", "contact", "feedback"]
MEMORY_CATEGORIES = ["trading", "ranch", "analytics", "communications", "scheduling", "general"]


def _get_notion_client():
    """Get Notion client if configured."""
    global _notion_client
    if _notion_client:
        return _notion_client

    try:
        from notion_client import Client
        import os

        # Check for token in environment or config file
        token = os.environ.get("NOTION_API_KEY")

        if not token:
            # Try loading from .env
            env_path = PROJECT_ROOT / ".env"
            if env_path.exists():
                with open(env_path) as f:
                    for line in f:
                        if line.startswith("NOTION_API_KEY="):
                            token = line.split("=", 1)[1].strip().strip('"')
                            break

        if token:
            _notion_client = Client(auth=token)
            return _notion_client

    except ImportError:
        pass
    except Exception:
        pass

    return None


def store_memory(
    content: str,
    memory_type: str = "observation",
    category: str = "general",
    source: str = "agent"
) -> ToolResult:
    """
    Store a memory for long-term recall.

    Use this to remember important facts, user preferences, decisions,
    or observations that should persist across sessions.

    Args:
        content: The information to remember
        memory_type: Type of memory (preference, fact, decision, observation, process, contact, feedback)
        category: Category (trading, ranch, analytics, communications, scheduling, general)
        source: Where this information came from

    Returns:
        ToolResult confirming storage
    """
    from datetime import datetime

    if memory_type not in MEMORY_TYPES:
        return ToolResult(False, error=f"Invalid memory_type. Use one of: {MEMORY_TYPES}")

    if category not in MEMORY_CATEGORIES:
        return ToolResult(False, error=f"Invalid category. Use one of: {MEMORY_CATEGORIES}")

    memory_record = {
        "content": content,
        "type": memory_type,
        "category": category,
        "source": source,
        "created_at": datetime.now().isoformat()
    }

    # Try Notion first
    client = _get_notion_client()
    if client:
        try:
            import os
            db_id = os.environ.get("NOTION_MEMORY_DB_ID")

            if db_id:
                properties = {
                    "Name": {"title": [{"text": {"content": content[:100]}}]},
                    "Type": {"select": {"name": memory_type}},
                    "Category": {"select": {"name": category}},
                    "Content": {"rich_text": [{"text": {"content": content}}]},
                    "Source": {"rich_text": [{"text": {"content": source}}]},
                    "Created": {"date": {"start": datetime.now().isoformat()}}
                }

                response = client.pages.create(
                    parent={"database_id": db_id},
                    properties=properties
                )

                return ToolResult(True, data={
                    "stored_in": "notion",
                    "id": response["id"],
                    "content": content[:100] + "..." if len(content) > 100 else content
                })
        except Exception as e:
            # Fall through to local storage
            pass

    # Local fallback
    local_cache_path = DATA_DIR / "memory_cache.json"
    local_cache_path.parent.mkdir(parents=True, exist_ok=True)

    try:
        import json
        cache = {}
        if local_cache_path.exists():
            with open(local_cache_path) as f:
                cache = json.load(f)

        if "memories" not in cache:
            cache["memories"] = []

        memory_record["id"] = f"mem_{datetime.now().timestamp()}"
        cache["memories"].append(memory_record)

        # Keep last 1000 memories
        cache["memories"] = cache["memories"][-1000:]

        with open(local_cache_path, "w") as f:
            json.dump(cache, f, indent=2)

        return ToolResult(True, data={
            "stored_in": "local",
            "id": memory_record["id"],
            "content": content[:100] + "..." if len(content) > 100 else content
        })

    except Exception as e:
        return ToolResult(False, error=str(e))


def recall_memories(
    query: str = None,
    memory_type: str = None,
    category: str = None,
    limit: int = 10
) -> ToolResult:
    """
    Recall stored memories matching criteria.

    Args:
        query: Search term to filter by (searches content)
        memory_type: Filter by type (preference, fact, decision, etc.)
        category: Filter by category (trading, ranch, etc.)
        limit: Maximum memories to return (default 10)

    Returns:
        ToolResult with matching memories
    """
    from datetime import datetime
    import json

    memories = []

    # Try Notion first
    client = _get_notion_client()
    if client:
        try:
            import os
            db_id = os.environ.get("NOTION_MEMORY_DB_ID")

            if db_id:
                filters = []
                if memory_type:
                    filters.append({"property": "Type", "select": {"equals": memory_type}})
                if category:
                    filters.append({"property": "Category", "select": {"equals": category}})

                filter_obj = None
                if filters:
                    filter_obj = {"and": filters} if len(filters) > 1 else filters[0]

                response = client.databases.query(
                    database_id=db_id,
                    filter=filter_obj,
                    page_size=limit,
                    sorts=[{"property": "Created", "direction": "descending"}]
                )

                for page in response.get("results", []):
                    props = page.get("properties", {})
                    content = ""
                    if props.get("Content", {}).get("rich_text"):
                        content = props["Content"]["rich_text"][0].get("plain_text", "")

                    # Filter by query if provided
                    if query and query.lower() not in content.lower():
                        continue

                    memories.append({
                        "id": page["id"],
                        "content": content,
                        "type": props.get("Type", {}).get("select", {}).get("name", ""),
                        "category": props.get("Category", {}).get("select", {}).get("name", ""),
                        "created_at": props.get("Created", {}).get("date", {}).get("start", ""),
                        "source": "notion"
                    })

                if memories:
                    return ToolResult(True, data={
                        "source": "notion",
                        "count": len(memories),
                        "memories": memories[:limit]
                    })
        except Exception:
            pass  # Fall through to local

    # Local fallback
    local_cache_path = DATA_DIR / "memory_cache.json"

    if local_cache_path.exists():
        try:
            with open(local_cache_path) as f:
                cache = json.load(f)

            for mem in cache.get("memories", []):
                # Apply filters
                if memory_type and mem.get("type") != memory_type:
                    continue
                if category and mem.get("category") != category:
                    continue
                if query and query.lower() not in mem.get("content", "").lower():
                    continue
                memories.append(mem)

        except Exception as e:
            return ToolResult(False, error=str(e))

    # Sort by created_at descending
    memories.sort(key=lambda x: x.get("created_at", ""), reverse=True)

    return ToolResult(True, data={
        "source": "local",
        "count": len(memories[:limit]),
        "memories": memories[:limit]
    })


def get_memory_stats() -> ToolResult:
    """
    Get statistics about stored memories.

    Returns:
        ToolResult with memory statistics
    """
    import json

    stats = {
        "notion_configured": _get_notion_client() is not None,
        "local_memories": 0,
        "by_type": {},
        "by_category": {}
    }

    # Check local cache
    local_cache_path = DATA_DIR / "memory_cache.json"
    if local_cache_path.exists():
        try:
            with open(local_cache_path) as f:
                cache = json.load(f)

            memories = cache.get("memories", [])
            stats["local_memories"] = len(memories)

            for mem in memories:
                t = mem.get("type", "unknown")
                c = mem.get("category", "unknown")
                stats["by_type"][t] = stats["by_type"].get(t, 0) + 1
                stats["by_category"][c] = stats["by_category"].get(c, 0) + 1

        except Exception:
            pass

    return ToolResult(True, data=stats)


# =============================================================================
# ENHANCED DATABASE TOOLS
# =============================================================================

# Path to the main commodity database
COMMODITY_DB_PATH = DATA_DIR / "rlc_commodities.db"


def get_data_catalog(
    commodity: str = None,
    category: str = None,
    search: str = None
) -> ToolResult:
    """
    Browse the data catalog to see what data series are available.

    Args:
        commodity: Filter by commodity (corn, soybeans, etc.)
        category: Filter by category (biofuels, oilseeds, etc.)
        search: Search term to find series by name

    Returns:
        ToolResult with available data series
    """
    try:
        if not COMMODITY_DB_PATH.exists():
            return ToolResult(False, error="Database not initialized. Run: python deployment/excel_to_database.py --init --load")

        conn = sqlite3.connect(str(COMMODITY_DB_PATH))
        cursor = conn.cursor()

        # Check if data_catalog table exists
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='data_catalog'")
        if not cursor.fetchone():
            # Fall back to listing what's in other tables
            tables_info = []
            for table in ['balance_sheets', 'price_history', 'excel_time_series']:
                try:
                    cursor.execute(f"SELECT COUNT(*) FROM {table}")
                    count = cursor.fetchone()[0]
                    if count > 0:
                        cursor.execute(f"SELECT DISTINCT source_file FROM {table} LIMIT 20")
                        files = [r[0] for r in cursor.fetchall()]
                        tables_info.append({
                            "table": table,
                            "rows": count,
                            "source_files": files[:10]
                        })
                except:
                    pass

            conn.close()
            return ToolResult(True, data={
                "note": "Data catalog not yet built. Here's what's in the database:",
                "tables": tables_info
            })

        # Build query with filters
        conditions = []
        params = []

        if commodity:
            conditions.append("LOWER(commodity) LIKE ?")
            params.append(f"%{commodity.lower()}%")
        if category:
            conditions.append("LOWER(category) LIKE ?")
            params.append(f"%{category.lower()}%")
        if search:
            conditions.append("(LOWER(series_name) LIKE ? OR LOWER(description) LIKE ?)")
            params.extend([f"%{search.lower()}%", f"%{search.lower()}%"])

        where_clause = f"WHERE {' AND '.join(conditions)}" if conditions else ""

        cursor.execute(f"""
            SELECT series_id, series_name, description, commodity, country,
                   category, frequency, start_date, end_date, row_count
            FROM data_catalog
            {where_clause}
            ORDER BY category, commodity, series_name
            LIMIT 50
        """, params)

        series = []
        for row in cursor.fetchall():
            series.append({
                "id": row[0],
                "name": row[1],
                "description": row[2],
                "commodity": row[3],
                "country": row[4],
                "category": row[5],
                "frequency": row[6],
                "date_range": f"{row[7]} to {row[8]}",
                "rows": row[9]
            })

        conn.close()

        return ToolResult(True, data={
            "count": len(series),
            "series": series
        })

    except Exception as e:
        return ToolResult(False, error=str(e))


def get_balance_sheet(
    commodity: str,
    country: str = None,
    year: str = None
) -> ToolResult:
    """
    Get balance sheet data for a commodity.

    Args:
        commodity: Commodity name (soybeans, corn, etc.)
        country: Country/region filter (optional)
        year: Marketing year filter (e.g. "2024/25")

    Returns:
        ToolResult with balance sheet data in a readable format
    """
    try:
        if not COMMODITY_DB_PATH.exists():
            return ToolResult(False, error="Database not initialized")

        conn = sqlite3.connect(str(COMMODITY_DB_PATH))
        cursor = conn.cursor()

        # Build query
        conditions = ["LOWER(commodity) LIKE ?"]
        params = [f"%{commodity.lower()}%"]

        if country:
            conditions.append("LOWER(country) LIKE ?")
            params.append(f"%{country.lower()}%")
        if year:
            conditions.append("marketing_year = ?")
            params.append(year)

        where_clause = " AND ".join(conditions)

        cursor.execute(f"""
            SELECT commodity, country, marketing_year, metric, value, unit, source_file
            FROM balance_sheets
            WHERE {where_clause}
            ORDER BY country, marketing_year DESC, metric
            LIMIT 200
        """, params)

        rows = cursor.fetchall()
        conn.close()

        if not rows:
            return ToolResult(True, data={
                "commodity": commodity,
                "message": "No data found. Try searching with get_data_catalog first."
            })

        # Organize by country/year
        by_country_year = {}
        for row in rows:
            key = f"{row[1]}|{row[2]}"
            if key not in by_country_year:
                by_country_year[key] = {
                    "commodity": row[0],
                    "country": row[1],
                    "marketing_year": row[2],
                    "metrics": {},
                    "source": row[6]
                }
            by_country_year[key]["metrics"][row[3]] = row[4]

        return ToolResult(True, data={
            "commodity": commodity,
            "balance_sheets": list(by_country_year.values())
        })

    except Exception as e:
        return ToolResult(False, error=str(e))


def get_price_history(
    symbol: str = None,
    commodity: str = None,
    start_date: str = None,
    end_date: str = None,
    limit: int = 100
) -> ToolResult:
    """
    Get historical price data.

    Args:
        symbol: Specific symbol (e.g., "ZS" for soybeans)
        commodity: Commodity name (alternative to symbol)
        start_date: Start date (YYYY-MM-DD)
        end_date: End date (YYYY-MM-DD)
        limit: Maximum rows (default 100)

    Returns:
        ToolResult with price history
    """
    try:
        if not COMMODITY_DB_PATH.exists():
            return ToolResult(False, error="Database not initialized")

        conn = sqlite3.connect(str(COMMODITY_DB_PATH))
        cursor = conn.cursor()

        conditions = []
        params = []

        if symbol:
            conditions.append("LOWER(symbol) LIKE ?")
            params.append(f"%{symbol.lower()}%")
        if commodity:
            conditions.append("LOWER(commodity) LIKE ?")
            params.append(f"%{commodity.lower()}%")
        if start_date:
            conditions.append("date_value >= ?")
            params.append(start_date)
        if end_date:
            conditions.append("date_value <= ?")
            params.append(end_date)

        where_clause = f"WHERE {' AND '.join(conditions)}" if conditions else ""

        cursor.execute(f"""
            SELECT symbol, commodity, price_type, date_value,
                   open, high, low, close, settle, volume
            FROM price_history
            {where_clause}
            ORDER BY date_value DESC
            LIMIT ?
        """, params + [limit])

        prices = []
        for row in cursor.fetchall():
            prices.append({
                "symbol": row[0],
                "commodity": row[1],
                "type": row[2],
                "date": row[3],
                "open": row[4],
                "high": row[5],
                "low": row[6],
                "close": row[7],
                "settle": row[8],
                "volume": row[9]
            })

        conn.close()

        return ToolResult(True, data={
            "count": len(prices),
            "prices": prices
        })

    except Exception as e:
        return ToolResult(False, error=str(e))


def get_time_series(
    series_name: str = None,
    category: str = None,
    search: str = None,
    limit: int = 100
) -> ToolResult:
    """
    Get time series data from Excel imports.

    Args:
        series_name: Exact series name
        category: Category filter (biofuels, oilseeds, etc.)
        search: Search term
        limit: Maximum rows (default 100)

    Returns:
        ToolResult with time series data
    """
    try:
        if not COMMODITY_DB_PATH.exists():
            return ToolResult(False, error="Database not initialized")

        conn = sqlite3.connect(str(COMMODITY_DB_PATH))
        cursor = conn.cursor()

        conditions = []
        params = []

        if series_name:
            conditions.append("series_name = ?")
            params.append(series_name)
        if category:
            conditions.append("LOWER(category) LIKE ?")
            params.append(f"%{category.lower()}%")
        if search:
            conditions.append("LOWER(series_name) LIKE ?")
            params.append(f"%{search.lower()}%")

        where_clause = f"WHERE {' AND '.join(conditions)}" if conditions else ""

        # First get distinct series
        cursor.execute(f"""
            SELECT DISTINCT series_name, category, source_file
            FROM excel_time_series
            {where_clause}
            LIMIT 20
        """, params)

        series_list = [{"name": r[0], "category": r[1], "source": r[2]} for r in cursor.fetchall()]

        # Then get data for first matching series
        if series_list:
            target_series = series_list[0]["name"]
            cursor.execute("""
                SELECT date_value, numeric_value
                FROM excel_time_series
                WHERE series_name = ?
                ORDER BY date_value DESC
                LIMIT ?
            """, [target_series, limit])

            data_points = [{"date": r[0], "value": r[1]} for r in cursor.fetchall()]
        else:
            data_points = []

        conn.close()

        return ToolResult(True, data={
            "available_series": series_list,
            "data_series": series_list[0]["name"] if series_list else None,
            "data_points": data_points
        })

    except Exception as e:
        return ToolResult(False, error=str(e))


def analyze_data_relationships(commodity: str) -> ToolResult:
    """
    Analyze relationships between data series for a commodity.
    Helps understand what data is connected.

    Args:
        commodity: Commodity to analyze (soybeans, corn, etc.)

    Returns:
        ToolResult with relationship analysis
    """
    try:
        if not COMMODITY_DB_PATH.exists():
            return ToolResult(False, error="Database not initialized")

        conn = sqlite3.connect(str(COMMODITY_DB_PATH))
        cursor = conn.cursor()

        analysis = {
            "commodity": commodity,
            "balance_sheet_countries": [],
            "balance_sheet_metrics": [],
            "time_series_available": [],
            "price_data": [],
            "source_files": []
        }

        # Get balance sheet info
        cursor.execute("""
            SELECT DISTINCT country FROM balance_sheets
            WHERE LOWER(commodity) LIKE ?
        """, [f"%{commodity.lower()}%"])
        analysis["balance_sheet_countries"] = [r[0] for r in cursor.fetchall()]

        cursor.execute("""
            SELECT DISTINCT metric FROM balance_sheets
            WHERE LOWER(commodity) LIKE ?
        """, [f"%{commodity.lower()}%"])
        analysis["balance_sheet_metrics"] = [r[0] for r in cursor.fetchall()]

        # Get time series info
        cursor.execute("""
            SELECT DISTINCT series_name, category FROM excel_time_series
            WHERE LOWER(series_name) LIKE ? OR LOWER(category) LIKE ?
            LIMIT 20
        """, [f"%{commodity.lower()}%", f"%{commodity.lower()}%"])
        analysis["time_series_available"] = [{"name": r[0], "category": r[1]} for r in cursor.fetchall()]

        # Get price data info
        cursor.execute("""
            SELECT DISTINCT symbol, price_type FROM price_history
            WHERE LOWER(commodity) LIKE ?
        """, [f"%{commodity.lower()}%"])
        analysis["price_data"] = [{"symbol": r[0], "type": r[1]} for r in cursor.fetchall()]

        # Get source files
        cursor.execute("""
            SELECT DISTINCT source_file FROM balance_sheets
            WHERE LOWER(commodity) LIKE ?
            UNION
            SELECT DISTINCT source_file FROM excel_time_series
            WHERE LOWER(series_name) LIKE ?
            LIMIT 20
        """, [f"%{commodity.lower()}%", f"%{commodity.lower()}%"])
        analysis["source_files"] = [r[0] for r in cursor.fetchall()]

        conn.close()

        return ToolResult(True, data=analysis)

    except Exception as e:
        return ToolResult(False, error=str(e))


def get_database_status() -> ToolResult:
    """
    Get comprehensive status of the commodity database.

    Returns:
        ToolResult with database statistics
    """
    try:
        if not COMMODITY_DB_PATH.exists():
            return ToolResult(True, data={
                "exists": False,
                "message": "Database not initialized. Run: python deployment/excel_to_database.py --init --load"
            })

        conn = sqlite3.connect(str(COMMODITY_DB_PATH))
        cursor = conn.cursor()

        status = {
            "exists": True,
            "path": str(COMMODITY_DB_PATH),
            "size_mb": COMMODITY_DB_PATH.stat().st_size / (1024*1024),
            "tables": {}
        }

        # Get all tables
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = [r[0] for r in cursor.fetchall()]

        for table in tables:
            try:
                cursor.execute(f"SELECT COUNT(*) FROM {table}")
                count = cursor.fetchone()[0]
                status["tables"][table] = count
            except:
                status["tables"][table] = "error"

        # Get summary stats
        try:
            cursor.execute("SELECT COUNT(DISTINCT commodity) FROM balance_sheets")
            status["commodities_count"] = cursor.fetchone()[0]
        except:
            pass

        try:
            cursor.execute("SELECT COUNT(DISTINCT source_file) FROM excel_imports WHERE import_status='success'")
            status["files_imported"] = cursor.fetchone()[0]
        except:
            pass

        conn.close()

        return ToolResult(True, data=status)

    except Exception as e:
        return ToolResult(False, error=str(e))


def import_excel_to_database(file_path: str = None, scan_only: bool = False) -> ToolResult:
    """
    Import Excel files into the database or scan available files.

    Args:
        file_path: Specific file to import (optional)
        scan_only: If True, just scan and report what's available

    Returns:
        ToolResult with import status
    """
    try:
        import subprocess

        script_path = PROJECT_ROOT / "deployment" / "excel_to_database.py"
        if not script_path.exists():
            return ToolResult(False, error="excel_to_database.py not found")

        if scan_only:
            cmd = [sys.executable, str(script_path), "--scan"]
        elif file_path:
            cmd = [sys.executable, str(script_path), "--file", file_path]
        else:
            cmd = [sys.executable, str(script_path), "--init", "--load"]

        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=600  # 10 minute timeout for full import
        )

        output = result.stdout + result.stderr

        return ToolResult(
            success=result.returncode == 0,
            data={"output": output[:5000]},  # Truncate long output
            error=result.stderr[:500] if result.returncode != 0 else None
        )

    except subprocess.TimeoutExpired:
        return ToolResult(False, error="Import timed out after 10 minutes")
    except Exception as e:
        return ToolResult(False, error=str(e))


def discover_data_sources(
    commodity: str,
    data_type: str = "all",
    region: str = "global"
) -> ToolResult:
    """
    Search for potential new data sources on the internet.
    Uses web search to find APIs, datasets, and data portals.

    Args:
        commodity: Commodity to find data for
        data_type: Type of data (prices, production, trade, stocks, etc.)
        region: Geographic focus (US, Brazil, Global, etc.)

    Returns:
        ToolResult with potential data sources
    """
    # Known commodity data sources to recommend
    known_sources = {
        "government": [
            {"name": "USDA FAS", "url": "https://apps.fas.usda.gov/psdonline/app/index.html",
             "data": ["production", "consumption", "trade", "stocks"], "regions": ["global"]},
            {"name": "USDA NASS", "url": "https://quickstats.nass.usda.gov",
             "data": ["production", "prices", "crop progress"], "regions": ["US"]},
            {"name": "USDA ERS", "url": "https://www.ers.usda.gov/data-products/",
             "data": ["prices", "costs", "farm economy"], "regions": ["US"]},
            {"name": "EIA", "url": "https://api.eia.gov",
             "data": ["ethanol", "biodiesel", "petroleum", "energy"], "regions": ["US", "global"]},
            {"name": "CFTC", "url": "https://www.cftc.gov/MarketReports/CommitmentsofTraders/index.htm",
             "data": ["COT positions", "speculator positions"], "regions": ["US"]},
            {"name": "CONAB Brazil", "url": "https://www.conab.gov.br",
             "data": ["production", "stocks", "crop progress"], "regions": ["Brazil"]},
            {"name": "ABIOVE Brazil", "url": "https://abiove.org.br",
             "data": ["crush", "meal production", "oil production"], "regions": ["Brazil"]},
            {"name": "MAGyP Argentina", "url": "https://datos.agroindustria.gob.ar",
             "data": ["production", "exports", "trade"], "regions": ["Argentina"]},
            {"name": "EU EUROSTAT", "url": "https://ec.europa.eu/eurostat",
             "data": ["production", "trade", "prices"], "regions": ["EU"]},
        ],
        "exchanges": [
            {"name": "CME Group", "url": "https://www.cmegroup.com",
             "data": ["futures prices", "options", "settlements"], "regions": ["global"]},
            {"name": "ICE", "url": "https://www.theice.com",
             "data": ["futures prices", "energy"], "regions": ["global"]},
            {"name": "B3 Brazil", "url": "https://www.b3.com.br",
             "data": ["futures prices", "soybean", "corn"], "regions": ["Brazil"]},
        ],
        "industry": [
            {"name": "Oil World", "url": "https://www.oilworld.biz",
             "data": ["oilseeds", "vegetable oils", "fats"], "regions": ["global"], "paid": True},
            {"name": "OPIS", "url": "https://www.opisnet.com",
             "data": ["biofuels prices", "RINs", "biodiesel"], "regions": ["US"], "paid": True},
            {"name": "Argus Media", "url": "https://www.argusmedia.com",
             "data": ["biofuels", "feedstocks", "prices"], "regions": ["global"], "paid": True},
            {"name": "EPA EMTS", "url": "https://www.epa.gov/fuels-registration-reporting-and-compliance-help/rin-data-emts",
             "data": ["RINs", "biofuel credits"], "regions": ["US"]},
        ],
        "free_apis": [
            {"name": "USDA AMS Market News", "url": "https://marsapi.ams.usda.gov",
             "data": ["daily prices", "cash markets", "basis"], "regions": ["US"]},
            {"name": "Quandl/Nasdaq", "url": "https://data.nasdaq.com",
             "data": ["futures", "commodities", "economic"], "regions": ["global"]},
            {"name": "FRED", "url": "https://fred.stlouisfed.org",
             "data": ["economic", "interest rates", "currencies"], "regions": ["US", "global"]},
        ]
    }

    # Filter by commodity, data_type, and region
    commodity_lower = commodity.lower()
    data_type_lower = data_type.lower()
    region_lower = region.lower()

    recommended = []

    for category, sources in known_sources.items():
        for source in sources:
            # Check if source matches criteria
            data_match = data_type_lower == "all" or any(
                data_type_lower in d.lower() for d in source["data"]
            )
            region_match = region_lower == "global" or any(
                region_lower in r.lower() for r in source["regions"]
            ) or "global" in source["regions"]

            if data_match and region_match:
                recommended.append({
                    "category": category,
                    "name": source["name"],
                    "url": source["url"],
                    "data_types": source["data"],
                    "regions": source["regions"],
                    "paid": source.get("paid", False)
                })

    # Build search queries for web search
    search_queries = [
        f"{commodity} {data_type} API free data",
        f"{commodity} {region} statistics data download",
        f"{commodity} market data source {data_type}",
    ]

    return ToolResult(True, data={
        "commodity": commodity,
        "data_type": data_type,
        "region": region,
        "recommended_sources": recommended,
        "search_suggestions": search_queries,
        "note": "Use search_web tool with the search_suggestions to find additional sources"
    })


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

    # Email (Gmail - checks BOTH work and personal accounts by default)
    "check_email": {
        "function": check_email,
        "description": "Check Gmail inbox for emails from BOTH work and personal accounts",
        "parameters": {
            "max_results": "Number of emails to return per account (default 10)",
            "query": "Gmail search query (default: is:unread)",
            "account": "Which account: 'both', 'work', or 'personal' (default: both)"
        }
    },
    "get_email_content": {
        "function": get_email_content,
        "description": "Get the full content of a specific email by ID",
        "parameters": {
            "email_id": "The email ID to retrieve",
            "account": "Which account: 'work' or 'personal' (default: work)"
        }
    },

    # Calendar (Google Calendar - ALWAYS uses work account)
    "check_calendar": {
        "function": check_calendar,
        "description": "Check upcoming calendar events (uses work calendar: tore.alden@roundlakescommodities.com)",
        "parameters": {
            "days_ahead": "Number of days to look ahead (default 7)"
        }
    },
    "get_todays_schedule": {
        "function": get_todays_schedule,
        "description": "Get today's schedule (uses work calendar)",
        "parameters": {}
    },

    # Memory (Notion + local fallback)
    "store_memory": {
        "function": store_memory,
        "description": "Store a memory for long-term recall (facts, preferences, decisions, observations)",
        "parameters": {
            "content": "The information to remember",
            "memory_type": "Type: preference, fact, decision, observation, process, contact, feedback",
            "category": "Category: trading, ranch, analytics, communications, scheduling, general",
            "source": "Where this came from (default: agent)"
        }
    },
    "recall_memories": {
        "function": recall_memories,
        "description": "Recall stored memories matching criteria",
        "parameters": {
            "query": "Search term to filter by",
            "memory_type": "Filter by type (optional)",
            "category": "Filter by category (optional)",
            "limit": "Max memories to return (default 10)"
        }
    },
    "get_memory_stats": {
        "function": get_memory_stats,
        "description": "Get statistics about stored memories",
        "parameters": {}
    },

    # Enhanced Database Tools
    "get_data_catalog": {
        "function": get_data_catalog,
        "description": "Browse the data catalog to see what data series are available in the database",
        "parameters": {
            "commodity": "Filter by commodity (corn, soybeans, etc.)",
            "category": "Filter by category (biofuels, oilseeds, etc.)",
            "search": "Search term to find series by name"
        }
    },
    "get_balance_sheet": {
        "function": get_balance_sheet,
        "description": "Get balance sheet data for a commodity (production, exports, crush, stocks, etc.)",
        "parameters": {
            "commodity": "Commodity name (soybeans, corn, wheat, etc.) - REQUIRED",
            "country": "Country/region filter (US, Brazil, World, etc.)",
            "year": "Marketing year filter (e.g. 2024/25)"
        }
    },
    "get_price_history": {
        "function": get_price_history,
        "description": "Get historical price data for a commodity or symbol",
        "parameters": {
            "symbol": "Specific symbol (e.g., ZS for soybeans)",
            "commodity": "Commodity name (alternative to symbol)",
            "start_date": "Start date (YYYY-MM-DD)",
            "end_date": "End date (YYYY-MM-DD)",
            "limit": "Maximum rows (default 100)"
        }
    },
    "get_time_series": {
        "function": get_time_series,
        "description": "Get time series data from imported Excel files",
        "parameters": {
            "series_name": "Exact series name",
            "category": "Category filter (biofuels, oilseeds, etc.)",
            "search": "Search term to find series",
            "limit": "Maximum rows (default 100)"
        }
    },
    "analyze_data_relationships": {
        "function": analyze_data_relationships,
        "description": "Analyze what data is available for a commodity and how it's connected",
        "parameters": {
            "commodity": "Commodity to analyze (soybeans, corn, biodiesel, etc.)"
        }
    },
    "get_database_status": {
        "function": get_database_status,
        "description": "Get comprehensive status of the commodity database (tables, row counts, etc.)",
        "parameters": {}
    },
    "import_excel_to_database": {
        "function": import_excel_to_database,
        "description": "Import Excel files into the commodity database (builds the fundamental data)",
        "parameters": {
            "file_path": "Specific file to import (optional - imports all if not specified)",
            "scan_only": "If True, just scan and report what files are available"
        }
    },
    "discover_data_sources": {
        "function": discover_data_sources,
        "description": "Search for potential new data sources on the internet for a commodity or metric",
        "parameters": {
            "commodity": "Commodity to find data for (soybeans, biodiesel, etc.)",
            "data_type": "Type of data needed (prices, production, trade, etc.)",
            "region": "Geographic focus (US, Brazil, Global, etc.)"
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
