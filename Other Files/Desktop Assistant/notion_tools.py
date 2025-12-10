# notion_tools.py
import os
from typing import List, Dict, Any, Optional
from dataclasses import dataclass

from notion_client import Client
from langchain.tools import tool
import dateparser
from datetime import datetime
import pytz

NOTION_API_KEY = os.getenv("NOTION_API_KEY")
NOTION_DB_ID = os.getenv("NOTION_DATABASE_ID")

client = Client(auth=NOTION_API_KEY)

# Adjust these to your database schema
TITLE_PROP = "Name"        # Title property
STATUS_PROP = "Status"     # Status property (e.g., Not Started / In Progress / Done)
DUE_PROP = "Due"           # Date property
PROJECT_PROP = "Project"   # Optional relation or select
PRIORITY_PROP = "Priority" # Optional select

def _iso_from_natural(text: str, tz: str = "America/New_York") -> Optional[str]:
    if not text:
        return None
    dt = dateparser.parse(text)
    if not dt:
        return None
    tzinfo = pytz.timezone(tz)
    if not dt.tzinfo:
        dt = tzinfo.localize(dt)
    return dt.isoformat()

def _find_page_id_by_title(title: str) -> Optional[str]:
    res = client.databases.query(
        database_id=NOTION_DB_ID,
        filter={"property": TITLE_PROP, "title": {"contains": title}}
    )
    results = res.get("results", [])
    return results[0]["id"] if results else None

@tool("notion_find_tasks", return_direct=False)
def notion_find_tasks(query_text: str = "", status: str = "", limit: int = 10) -> List[Dict[str, Any]]:
    """
    Search tasks in the Notion database by title contains and/or status.
    Returns: [{id, title, status, due}]
    """
    filters = []
    if query_text:
        filters.append({"property": TITLE_PROP, "title": {"contains": query_text}})
    if status:
        filters.append({"property": STATUS_PROP, "status": {"equals": status}})

    compound = None
    if len(filters) == 1:
        compound = filters[0]
    elif len(filters) > 1:
        compound = {"and": filters}

    res = client.databases.query(database_id=NOTION_DB_ID, filter=compound, page_size=limit)
    items = []
    for page in res.get("results", []):
        props = page["properties"]
        title = "".join([t["plain_text"] for t in props[TITLE_PROP]["title"]]) if props.get(TITLE_PROP) else ""
        status_val = props.get(STATUS_PROP, {}).get("status", {}).get("name")
        due_val = props.get(DUE_PROP, {}).get("date", {}).get("start") if props.get(DUE_PROP) else None
        items.append({"id": page["id"], "title": title, "status": status_val, "due": due_val})
    return items

@tool("notion_add_task", return_direct=False)
def notion_add_task(title: str, due_natural: str = "", status: str = "Not Started", project: str = "", priority: str = "") -> Dict[str, Any]:
    """
    Add a task to the Notion database. Accepts natural due date (e.g., 'next Friday 5pm').
    """
    due_iso = _iso_from_natural(due_natural) if due_natural else None

    props = {
        TITLE_PROP: {"title": [{"type": "text", "text": {"content": title}}]},
        STATUS_PROP: {"status": {"name": status}},
    }
    if due_iso:
        props[DUE_PROP] = {"date": {"start": due_iso}}
    if project:
        # If your Project property is a select:
        props[PROJECT_PROP] = {"select": {"name": project}}
    if priority:
        props[PRIORITY_PROP] = {"select": {"name": priority}}

    page = client.pages.create(parent={"database_id": NOTION_DB_ID}, properties=props)
    return {"id": page["id"], "title": title, "due": due_iso, "status": status}

@tool("notion_update_task_status", return_direct=False)
def notion_update_task_status(title_or_id: str, new_status: str) -> str:
    """
    Update a task's status by title match (first result) or by direct Notion page ID.
    """
    page_id = title_or_id
    if not page_id.startswith("~") and "-" not in page_id:  # crude heuristic; try lookup by title
        maybe = _find_page_id_by_title(title_or_id)
        if not maybe:
            return f"No task found matching title: {title_or_id}"
        page_id = maybe

    client.pages.update(page_id=page_id, properties={STATUS_PROP: {"status": {"name": new_status}}})
    return f"Updated task '{title_or_id}' → {new_status}"
