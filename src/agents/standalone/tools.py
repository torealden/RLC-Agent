"""
RLC Agent Tools
Tools that the LLM agent can use to interact with the world.
"""

import os
import json
import logging
import subprocess
from pathlib import Path
from datetime import datetime
from typing import Optional, List, Dict, Any

import pandas as pd

from config import DATABASE, PROJECT_ROOT, SEARCH_BACKEND, NOTION_API_KEY

# Try to import TAVILY_API_KEY if it exists
try:
    from config import TAVILY_API_KEY
except ImportError:
    TAVILY_API_KEY = None

logger = logging.getLogger(__name__)

# ============================================================================
# WEB SEARCH TOOLS
# ============================================================================

def web_search(query: str, max_results: int = 5) -> List[Dict[str, str]]:
    """
    Search the web using Tavily (primary) or DuckDuckGo (fallback).

    Args:
        query: Search query string
        max_results: Maximum number of results to return

    Returns:
        List of {"title": ..., "url": ..., "snippet": ...}
    """
    # Try Tavily first if configured
    if SEARCH_BACKEND == "tavily" and TAVILY_API_KEY:
        try:
            from tavily import TavilyClient

            client = TavilyClient(api_key=TAVILY_API_KEY)
            response = client.search(query=query, max_results=max_results)

            results = []
            for r in response.get("results", []):
                results.append({
                    "title": r.get("title", ""),
                    "url": r.get("url", ""),
                    "snippet": r.get("content", "")
                })

            logger.info(f"Tavily search for '{query}' returned {len(results)} results")
            return results

        except ImportError:
            logger.warning("tavily-python not installed, falling back to DuckDuckGo")
        except Exception as e:
            logger.warning(f"Tavily search error: {e}, falling back to DuckDuckGo")

    # Fallback to DuckDuckGo (now renamed to ddgs package)
    try:
        from ddgs import DDGS

        results = []
        with DDGS() as ddgs:
            for r in ddgs.text(query, max_results=max_results):
                results.append({
                    "title": r.get("title", ""),
                    "url": r.get("href", ""),
                    "snippet": r.get("body", "")
                })

        logger.info(f"DuckDuckGo search for '{query}' returned {len(results)} results")
        return results

    except ImportError:
        logger.error("ddgs not installed. Run: pip install ddgs")
        return [{"error": "Search not available - ddgs not installed"}]
    except Exception as e:
        logger.error(f"Web search error: {e}")
        return [{"error": str(e)}]


def web_search_news(query: str, max_results: int = 5) -> List[Dict[str, str]]:
    """
    Search for recent news articles using Tavily (primary) or DuckDuckGo (fallback).

    Args:
        query: Search query string
        max_results: Maximum number of results to return

    Returns:
        List of {"title": ..., "url": ..., "snippet": ..., "date": ..., "source": ...}
    """
    # Try Tavily first if configured
    if SEARCH_BACKEND == "tavily" and TAVILY_API_KEY:
        try:
            from tavily import TavilyClient

            client = TavilyClient(api_key=TAVILY_API_KEY)
            response = client.search(
                query=query,
                max_results=max_results,
                topic="news"  # Tavily news topic filter
            )

            results = []
            for r in response.get("results", []):
                results.append({
                    "title": r.get("title", ""),
                    "url": r.get("url", ""),
                    "snippet": r.get("content", ""),
                    "date": r.get("published_date", ""),
                    "source": r.get("source", "")
                })

            logger.info(f"Tavily news search for '{query}' returned {len(results)} results")
            return results

        except ImportError:
            logger.warning("tavily-python not installed, falling back to DuckDuckGo")
        except Exception as e:
            logger.warning(f"Tavily news search error: {e}, falling back to DuckDuckGo")

    # Fallback to DuckDuckGo (now renamed to ddgs package)
    try:
        from ddgs import DDGS

        results = []
        with DDGS() as ddgs:
            for r in ddgs.news(query, max_results=max_results):
                results.append({
                    "title": r.get("title", ""),
                    "url": r.get("url", ""),
                    "snippet": r.get("body", ""),
                    "date": r.get("date", ""),
                    "source": r.get("source", "")
                })

        logger.info(f"DuckDuckGo news search for '{query}' returned {len(results)} results")
        return results

    except ImportError:
        logger.error("ddgs not installed. Run: pip install ddgs")
        return [{"error": "News search not available - ddgs not installed"}]
    except Exception as e:
        logger.error(f"News search error: {e}")
        return [{"error": str(e)}]


def fetch_webpage(url: str) -> str:
    """
    Fetch and extract text content from a webpage.

    Args:
        url: URL to fetch

    Returns:
        Extracted text content
    """
    try:
        import requests
        from html import unescape
        import re

        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        }

        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()

        # Simple HTML to text conversion
        text = response.text
        # Remove scripts and styles
        text = re.sub(r'<script[^>]*>.*?</script>', '', text, flags=re.DOTALL)
        text = re.sub(r'<style[^>]*>.*?</style>', '', text, flags=re.DOTALL)
        # Remove HTML tags
        text = re.sub(r'<[^>]+>', ' ', text)
        # Clean up whitespace
        text = re.sub(r'\s+', ' ', text)
        text = unescape(text).strip()

        # Limit length
        if len(text) > 10000:
            text = text[:10000] + "... [truncated]"

        logger.info(f"Fetched {len(text)} chars from {url}")
        return text

    except Exception as e:
        logger.error(f"Webpage fetch error: {e}")
        return f"Error fetching {url}: {e}"


# ============================================================================
# FILE SYSTEM TOOLS
# ============================================================================

def read_file(file_path: str) -> str:
    """
    Read contents of a file.

    Args:
        file_path: Path to file (relative to project root or absolute)

    Returns:
        File contents as string
    """
    try:
        path = Path(file_path)
        if not path.is_absolute():
            path = PROJECT_ROOT / path

        if not path.exists():
            return f"Error: File not found: {path}"

        with open(path, 'r', encoding='utf-8') as f:
            content = f.read()

        logger.info(f"Read {len(content)} chars from {path}")
        return content

    except Exception as e:
        logger.error(f"File read error: {e}")
        return f"Error reading file: {e}"


def write_file(file_path: str, content: str, require_approval: bool = True) -> str:
    """
    Write content to a file.

    Args:
        file_path: Path to file
        content: Content to write
        require_approval: If True, return approval request instead of writing

    Returns:
        Success message or approval request
    """
    try:
        path = Path(file_path)
        if not path.is_absolute():
            path = PROJECT_ROOT / path

        if require_approval:
            return f"APPROVAL_REQUIRED: Write {len(content)} chars to {path}"

        # Create parent directories if needed
        path.parent.mkdir(parents=True, exist_ok=True)

        with open(path, 'w', encoding='utf-8') as f:
            f.write(content)

        logger.info(f"Wrote {len(content)} chars to {path}")
        return f"Successfully wrote to {path}"

    except Exception as e:
        logger.error(f"File write error: {e}")
        return f"Error writing file: {e}"


def list_directory(dir_path: str = ".", pattern: str = "*") -> List[str]:
    """
    List files in a directory.

    Args:
        dir_path: Directory path
        pattern: Glob pattern to match

    Returns:
        List of file paths
    """
    try:
        path = Path(dir_path)
        if not path.is_absolute():
            path = PROJECT_ROOT / path

        files = sorted([str(f.relative_to(PROJECT_ROOT)) for f in path.glob(pattern)])
        logger.info(f"Listed {len(files)} files in {path}")
        return files

    except Exception as e:
        logger.error(f"Directory list error: {e}")
        return [f"Error: {e}"]


def find_files(pattern: str, directory: str = ".") -> List[str]:
    """
    Find files matching a pattern recursively.

    Args:
        pattern: Glob pattern (e.g., "**/*.py")
        directory: Starting directory

    Returns:
        List of matching file paths
    """
    try:
        path = Path(directory)
        if not path.is_absolute():
            path = PROJECT_ROOT / path

        files = sorted([str(f.relative_to(PROJECT_ROOT)) for f in path.glob(pattern)])
        logger.info(f"Found {len(files)} files matching {pattern}")
        return files

    except Exception as e:
        logger.error(f"Find files error: {e}")
        return [f"Error: {e}"]


# ============================================================================
# DATABASE TOOLS
# ============================================================================

def query_database(sql: str, params: tuple = None) -> str:
    """
    Execute a SELECT query on the database.

    Args:
        sql: SQL SELECT query
        params: Query parameters

    Returns:
        Query results as formatted string
    """
    try:
        import psycopg2

        # Only allow SELECT queries for safety
        if not sql.strip().upper().startswith("SELECT"):
            return "Error: Only SELECT queries allowed. Use execute_database for modifications."

        conn = psycopg2.connect(**DATABASE)
        df = pd.read_sql(sql, conn, params=params)
        conn.close()

        result = f"Query returned {len(df)} rows:\n\n{df.to_string()}"
        logger.info(f"Database query returned {len(df)} rows")
        return result

    except ImportError:
        return "Error: psycopg2 not installed"
    except Exception as e:
        logger.error(f"Database query error: {e}")
        return f"Database error: {e}"


def execute_database(sql: str, params: tuple = None, require_approval: bool = True) -> str:
    """
    Execute a modifying query (INSERT, UPDATE, DELETE) on the database.

    Args:
        sql: SQL query
        params: Query parameters
        require_approval: If True, return approval request

    Returns:
        Success message or approval request
    """
    try:
        import psycopg2

        if require_approval:
            return f"APPROVAL_REQUIRED: Execute SQL: {sql[:200]}..."

        conn = psycopg2.connect(**DATABASE)
        cur = conn.cursor()
        cur.execute(sql, params)
        affected = cur.rowcount
        conn.commit()
        conn.close()

        logger.info(f"Database execute affected {affected} rows")
        return f"Query executed successfully. {affected} rows affected."

    except Exception as e:
        logger.error(f"Database execute error: {e}")
        return f"Database error: {e}"


def get_database_schema() -> str:
    """Get the database schema (tables, columns, types)."""
    try:
        import psycopg2

        conn = psycopg2.connect(**DATABASE)
        cur = conn.cursor()

        schema_info = []

        for schema in ['bronze', 'silver', 'gold']:
            schema_info.append(f"\n## {schema.upper()} Schema\n")

            # Get tables
            cur.execute("""
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = %s AND table_type = 'BASE TABLE'
                ORDER BY table_name
            """, (schema,))

            for (table,) in cur.fetchall():
                cur.execute("""
                    SELECT column_name, data_type
                    FROM information_schema.columns
                    WHERE table_schema = %s AND table_name = %s
                    ORDER BY ordinal_position
                """, (schema, table))

                columns = [f"{col} ({dtype})" for col, dtype in cur.fetchall()]
                schema_info.append(f"\n**{table}**")
                schema_info.append("  " + ", ".join(columns))

        conn.close()
        return "\n".join(schema_info)

    except Exception as e:
        logger.error(f"Schema retrieval error: {e}")
        return f"Error getting schema: {e}"


# ============================================================================
# SCRIPT EXECUTION TOOLS
# ============================================================================

def run_python_script(script_path: str, args: List[str] = None, require_approval: bool = True) -> str:
    """
    Run a Python script from the project.

    Args:
        script_path: Path to script relative to project root
        args: Command line arguments
        require_approval: If True, return approval request

    Returns:
        Script output or approval request
    """
    try:
        path = Path(script_path)
        if not path.is_absolute():
            path = PROJECT_ROOT / path

        if not path.exists():
            return f"Error: Script not found: {path}"

        if require_approval:
            return f"APPROVAL_REQUIRED: Run script {path} with args {args}"

        cmd = ["python", str(path)]
        if args:
            cmd.extend(args)

        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            cwd=str(PROJECT_ROOT),
            timeout=300
        )

        output = result.stdout
        if result.stderr:
            output += f"\n\nSTDERR:\n{result.stderr}"

        logger.info(f"Script {path} completed with return code {result.returncode}")
        return f"Return code: {result.returncode}\n\n{output}"

    except subprocess.TimeoutExpired:
        return "Error: Script timed out after 5 minutes"
    except Exception as e:
        logger.error(f"Script execution error: {e}")
        return f"Error running script: {e}"


def list_available_scripts() -> List[Dict[str, str]]:
    """List Python scripts in the project with their docstrings."""
    scripts = []

    for script_path in PROJECT_ROOT.glob("scripts/*.py"):
        try:
            with open(script_path, 'r') as f:
                content = f.read()

            # Extract docstring
            docstring = ""
            if content.startswith('"""'):
                end = content.find('"""', 3)
                if end > 0:
                    docstring = content[3:end].strip()
            elif content.startswith("'''"):
                end = content.find("'''", 3)
                if end > 0:
                    docstring = content[3:end].strip()

            scripts.append({
                "name": script_path.name,
                "path": f"scripts/{script_path.name}",
                "description": docstring[:200] if docstring else "No description"
            })
        except:
            pass

    return scripts


# ============================================================================
# UTILITY TOOLS
# ============================================================================

def get_current_time() -> str:
    """Get current date and time."""
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def calculate(expression: str) -> str:
    """
    Safely evaluate a mathematical expression.

    Args:
        expression: Math expression like "2 + 2" or "100 * 1.15"

    Returns:
        Result as string
    """
    try:
        # Only allow safe characters
        allowed = set("0123456789+-*/.() ")
        if not all(c in allowed for c in expression):
            return "Error: Invalid characters in expression"

        result = eval(expression)
        return str(result)

    except Exception as e:
        return f"Error: {e}"


# ============================================================================
# NOTION TOOLS
# ============================================================================

NOTION_BASE_URL = "https://api.notion.com/v1"
NOTION_VERSION = "2022-06-28"


def _notion_headers() -> Dict[str, str]:
    """Get headers for Notion API requests."""
    return {
        "Authorization": f"Bearer {NOTION_API_KEY}",
        "Content-Type": "application/json",
        "Notion-Version": NOTION_VERSION
    }


def notion_search(query: str, filter_type: str = None) -> str:
    """
    Search Notion for pages and databases.

    Args:
        query: Search query
        filter_type: Optional filter - "page" or "database"

    Returns:
        Search results
    """
    if not NOTION_API_KEY:
        return "Error: NOTION_API_KEY not configured. Add your key to agent/config.py"

    try:
        import requests

        payload = {"query": query}
        if filter_type:
            payload["filter"] = {"property": "object", "value": filter_type}

        response = requests.post(
            f"{NOTION_BASE_URL}/search",
            headers=_notion_headers(),
            json=payload,
            timeout=30
        )

        if response.status_code != 200:
            return f"Error: {response.status_code} - {response.text}"

        data = response.json()
        results = []

        for item in data.get("results", []):
            obj_type = item.get("object")
            title = ""

            # Extract title based on object type
            if obj_type == "page":
                props = item.get("properties", {})
                for prop in props.values():
                    if prop.get("type") == "title":
                        title_content = prop.get("title", [])
                        if title_content:
                            title = title_content[0].get("plain_text", "Untitled")
                        break
            elif obj_type == "database":
                title_list = item.get("title", [])
                if title_list:
                    title = title_list[0].get("plain_text", "Untitled")

            results.append({
                "type": obj_type,
                "id": item.get("id"),
                "title": title or "Untitled",
                "url": item.get("url", "")
            })

        logger.info(f"Notion search for '{query}' returned {len(results)} results")
        return json.dumps(results, indent=2)

    except Exception as e:
        logger.error(f"Notion search error: {e}")
        return f"Error: {e}"


def notion_get_page(page_id: str) -> str:
    """
    Get content from a Notion page.

    Args:
        page_id: The page ID (from URL or search results)

    Returns:
        Page content as text
    """
    if not NOTION_API_KEY:
        return "Error: NOTION_API_KEY not configured. Add your key to agent/config.py"

    try:
        import requests

        # Get page metadata
        response = requests.get(
            f"{NOTION_BASE_URL}/pages/{page_id}",
            headers=_notion_headers(),
            timeout=30
        )

        if response.status_code != 200:
            return f"Error: {response.status_code} - {response.text}"

        page_data = response.json()

        # Get page blocks (content)
        blocks_response = requests.get(
            f"{NOTION_BASE_URL}/blocks/{page_id}/children",
            headers=_notion_headers(),
            timeout=30
        )

        content_parts = []

        if blocks_response.status_code == 200:
            blocks = blocks_response.json().get("results", [])

            for block in blocks:
                block_type = block.get("type")
                block_content = block.get(block_type, {})

                # Extract text from rich_text blocks
                if "rich_text" in block_content:
                    text = "".join([t.get("plain_text", "") for t in block_content["rich_text"]])
                    if block_type.startswith("heading"):
                        content_parts.append(f"\n## {text}\n")
                    elif block_type == "bulleted_list_item":
                        content_parts.append(f"• {text}")
                    elif block_type == "numbered_list_item":
                        content_parts.append(f"- {text}")
                    else:
                        content_parts.append(text)

        logger.info(f"Retrieved Notion page {page_id}")
        return "\n".join(content_parts) if content_parts else "Page has no text content"

    except Exception as e:
        logger.error(f"Notion get page error: {e}")
        return f"Error: {e}"


def notion_query_database(database_id: str, filter_json: str = None) -> str:
    """
    Query a Notion database.

    Args:
        database_id: The database ID
        filter_json: Optional JSON filter string

    Returns:
        Database entries
    """
    if not NOTION_API_KEY:
        return "Error: NOTION_API_KEY not configured. Add your key to agent/config.py"

    try:
        import requests

        payload = {}
        if filter_json:
            payload["filter"] = json.loads(filter_json)

        response = requests.post(
            f"{NOTION_BASE_URL}/databases/{database_id}/query",
            headers=_notion_headers(),
            json=payload,
            timeout=30
        )

        if response.status_code != 200:
            return f"Error: {response.status_code} - {response.text}"

        data = response.json()
        results = []

        for item in data.get("results", []):
            entry = {"id": item.get("id")}

            for prop_name, prop_data in item.get("properties", {}).items():
                prop_type = prop_data.get("type")

                # Extract value based on property type
                if prop_type == "title":
                    title_content = prop_data.get("title", [])
                    entry[prop_name] = title_content[0].get("plain_text", "") if title_content else ""
                elif prop_type == "rich_text":
                    text_content = prop_data.get("rich_text", [])
                    entry[prop_name] = text_content[0].get("plain_text", "") if text_content else ""
                elif prop_type == "number":
                    entry[prop_name] = prop_data.get("number")
                elif prop_type == "select":
                    select = prop_data.get("select")
                    entry[prop_name] = select.get("name") if select else ""
                elif prop_type == "multi_select":
                    entry[prop_name] = [s.get("name") for s in prop_data.get("multi_select", [])]
                elif prop_type == "date":
                    date = prop_data.get("date")
                    entry[prop_name] = date.get("start") if date else ""
                elif prop_type == "checkbox":
                    entry[prop_name] = prop_data.get("checkbox", False)
                elif prop_type == "url":
                    entry[prop_name] = prop_data.get("url", "")

            results.append(entry)

        logger.info(f"Queried Notion database {database_id}, {len(results)} results")
        return json.dumps(results, indent=2)

    except Exception as e:
        logger.error(f"Notion query error: {e}")
        return f"Error: {e}"


def notion_add_page(database_id: str, title: str, properties_json: str = None) -> str:
    """
    Add a new page to a Notion database.

    Args:
        database_id: The database ID to add to
        title: Title for the new page
        properties_json: Optional JSON string with additional properties

    Returns:
        Result message
    """
    if not NOTION_API_KEY:
        return "Error: NOTION_API_KEY not configured. Add your key to agent/config.py"

    try:
        import requests

        # Build properties - start with title
        properties = {
            "Name": {
                "title": [{"text": {"content": title}}]
            }
        }

        # Add any additional properties
        if properties_json:
            extra_props = json.loads(properties_json)
            properties.update(extra_props)

        payload = {
            "parent": {"database_id": database_id},
            "properties": properties
        }

        response = requests.post(
            f"{NOTION_BASE_URL}/pages",
            headers=_notion_headers(),
            json=payload,
            timeout=30
        )

        if response.status_code != 200:
            return f"Error: {response.status_code} - {response.text}"

        data = response.json()
        logger.info(f"Created Notion page in database {database_id}")
        return f"Page created successfully. ID: {data.get('id')}, URL: {data.get('url')}"

    except Exception as e:
        logger.error(f"Notion add page error: {e}")
        return f"Error: {e}"


# ============================================================================
# TOOL REGISTRY
# ============================================================================

TOOLS = {
    # Web tools
    "web_search": {
        "function": web_search,
        "description": "Search the web for information",
        "parameters": {"query": "Search query", "max_results": "Number of results (default 5)"}
    },
    "web_search_news": {
        "function": web_search_news,
        "description": "Search for recent news articles",
        "parameters": {"query": "Search query", "max_results": "Number of results (default 5)"}
    },
    "fetch_webpage": {
        "function": fetch_webpage,
        "description": "Fetch and extract text from a webpage",
        "parameters": {"url": "URL to fetch"}
    },

    # File tools
    "read_file": {
        "function": read_file,
        "description": "Read contents of a file",
        "parameters": {"file_path": "Path to file"}
    },
    "write_file": {
        "function": write_file,
        "description": "Write content to a file (requires approval)",
        "parameters": {"file_path": "Path to file", "content": "Content to write"}
    },
    "list_directory": {
        "function": list_directory,
        "description": "List files in a directory",
        "parameters": {"dir_path": "Directory path", "pattern": "Glob pattern"}
    },
    "find_files": {
        "function": find_files,
        "description": "Find files matching a pattern",
        "parameters": {"pattern": "Glob pattern", "directory": "Starting directory"}
    },

    # Database tools
    "query_database": {
        "function": query_database,
        "description": "Execute a SELECT query on the database",
        "parameters": {"sql": "SQL SELECT query"}
    },
    "execute_database": {
        "function": execute_database,
        "description": "Execute INSERT/UPDATE/DELETE (requires approval)",
        "parameters": {"sql": "SQL query"}
    },
    "get_database_schema": {
        "function": get_database_schema,
        "description": "Get database schema information",
        "parameters": {}
    },

    # Script tools
    "run_python_script": {
        "function": run_python_script,
        "description": "Run a Python script (requires approval)",
        "parameters": {"script_path": "Path to script", "args": "Command line arguments"}
    },
    "list_available_scripts": {
        "function": list_available_scripts,
        "description": "List available Python scripts",
        "parameters": {}
    },

    # Utility tools
    "get_current_time": {
        "function": get_current_time,
        "description": "Get current date and time",
        "parameters": {}
    },
    "calculate": {
        "function": calculate,
        "description": "Evaluate a math expression",
        "parameters": {"expression": "Math expression"}
    },

    # Notion tools
    "notion_search": {
        "function": notion_search,
        "description": "Search Notion for pages and databases",
        "parameters": {"query": "Search query", "filter_type": "Optional: 'page' or 'database'"}
    },
    "notion_get_page": {
        "function": notion_get_page,
        "description": "Get content from a Notion page",
        "parameters": {"page_id": "Page ID from search results or URL"}
    },
    "notion_query_database": {
        "function": notion_query_database,
        "description": "Query a Notion database for entries",
        "parameters": {"database_id": "Database ID", "filter_json": "Optional JSON filter"}
    },
    "notion_add_page": {
        "function": notion_add_page,
        "description": "Add a new page to a Notion database",
        "parameters": {"database_id": "Database ID", "title": "Page title", "properties_json": "Optional properties"}
    }
}


def get_tools_description() -> str:
    """Get a formatted description of all available tools."""
    lines = ["# Available Tools\n"]

    for name, tool in TOOLS.items():
        lines.append(f"\n## {name}")
        lines.append(f"{tool['description']}")
        if tool['parameters']:
            lines.append("Parameters:")
            for param, desc in tool['parameters'].items():
                lines.append(f"  - {param}: {desc}")

    return "\n".join(lines)


def execute_tool(tool_name: str, **kwargs) -> str:
    """Execute a tool by name with given arguments."""
    if tool_name not in TOOLS:
        return f"Error: Unknown tool '{tool_name}'"

    try:
        func = TOOLS[tool_name]["function"]
        result = func(**kwargs)
        return result if isinstance(result, str) else json.dumps(result, indent=2)
    except Exception as e:
        logger.error(f"Tool execution error: {e}")
        return f"Error executing {tool_name}: {e}"
