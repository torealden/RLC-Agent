"""
RLC Daily Activity Logger
=========================
Log activities throughout the day. Run the export at end of day to generate
a JSON file for Claude Desktop to sync to Notion.

Usage:
  python daily_activity_log.py log "Built new agent X" --category agent --status Live
  python daily_activity_log.py log "Fixed OAuth bug" --category lesson --status Fixed
  python daily_activity_log.py export  # Generates JSON for Claude Desktop
  python daily_activity_log.py clear   # Clear today's log after export
"""

import json
import os
import sys
from datetime import datetime, date
from pathlib import Path

# Configuration
LOG_DIR = Path(os.environ.get("RLC_LOG_DIR", Path.home() / "rlc_scheduler" / "logs"))
EXPORT_DIR = Path(os.environ.get("RLC_EXPORT_DIR", Path.home() / "rlc_scheduler" / "exports"))

# Notion Database IDs
NOTION_DBS = {
    "agent_registry": "2dbead02-3dee-804a-b611-000b7fe5b299",
    "data_sources_registry": "2dbead02-3dee-8062-ae13-000ba10e3beb",
    "architecture_decisions": "2dbead02-3dee-802f-a0a7-000b20d183ca",
    "runbooks": "2dbead02-3dee-804d-b167-000b11e5f92f",
    "lessons_learned": "2e6ead02-3dee-80d1-a7d7-000bf28e86d6",
    "master_timeline": "2dcead02-3dee-80ae-8990-000b75ea7d59",
    "reconciliation_log": "2dbead02-3dee-8050-ae40-000bd8ff835c"
}

# Category to database mapping
CATEGORY_MAP = {
    "agent": "agent_registry",
    "source": "data_sources_registry",
    "decision": "architecture_decisions",
    "runbook": "runbooks",
    "lesson": "lessons_learned",
    "timeline": "master_timeline",
    "issue": "reconciliation_log"
}


def ensure_dirs():
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    EXPORT_DIR.mkdir(parents=True, exist_ok=True)


def get_today_log_path():
    return LOG_DIR / f"activity_{date.today().isoformat()}.json"


def load_today_log():
    log_path = get_today_log_path()
    if log_path.exists():
        with open(log_path, "r") as f:
            return json.load(f)
    return {"date": date.today().isoformat(), "entries": []}


def save_today_log(log_data):
    ensure_dirs()
    with open(get_today_log_path(), "w") as f:
        json.dump(log_data, f, indent=2)


def log_activity(description, category="general", status=None, priority=None, details=None):
    """Log an activity for today."""
    log_data = load_today_log()

    entry = {
        "timestamp": datetime.now().isoformat(),
        "description": description,
        "category": category,
        "database": CATEGORY_MAP.get(category, "master_timeline"),
        "status": status,
        "priority": priority,
        "details": details or {}
    }

    log_data["entries"].append(entry)
    save_today_log(log_data)
    print(f"Logged: [{category}] {description}")
    return entry


def export_for_claude_desktop():
    """Export today's log as a JSON file for Claude Desktop to process."""
    ensure_dirs()
    log_data = load_today_log()

    if not log_data["entries"]:
        print("No activities logged today.")
        return None

    # Group entries by database
    grouped = {}
    for entry in log_data["entries"]:
        db = entry["database"]
        if db not in grouped:
            grouped[db] = []
        grouped[db].append(entry)

    # Create export structure
    export_data = {
        "export_date": datetime.now().isoformat(),
        "activity_date": log_data["date"],
        "notion_databases": NOTION_DBS,
        "entries_by_database": {},
        "raw_entries": log_data["entries"],
        "claude_desktop_prompt": generate_prompt(grouped)
    }

    for db_name, entries in grouped.items():
        export_data["entries_by_database"][db_name] = {
            "database_id": NOTION_DBS.get(db_name),
            "entries": entries
        }

    # Save export file
    export_path = EXPORT_DIR / f"notion_sync_{log_data['date']}.json"
    with open(export_path, "w") as f:
        json.dump(export_data, f, indent=2)

    print(f"\nExported to: {export_path}")
    print(f"\nPrompt for Claude Desktop:\n")
    print("-" * 50)
    print(export_data["claude_desktop_prompt"])
    print("-" * 50)

    return export_path


def generate_prompt(grouped_entries):
    """Generate a prompt for Claude Desktop to process the export."""
    lines = [
        f"Please read the daily activity export and update my Notion databases.",
        f"Export file: C:\\Users\\torem\\rlc_scheduler\\exports\\notion_sync_{date.today().isoformat()}.json",
        f"",
        f"Summary of updates needed:"
    ]

    for db_name, entries in grouped_entries.items():
        db_id = NOTION_DBS.get(db_name, "unknown")
        lines.append(f"\n**{db_name}** (ID: {db_id}):")
        for entry in entries:
            status_str = f" [{entry['status']}]" if entry.get('status') else ""
            lines.append(f"  - {entry['description']}{status_str}")

    lines.append(f"\nFor each entry, create or update the appropriate Notion page with all relevant properties.")

    return "\n".join(lines)


def clear_today_log():
    """Clear today's log after successful export."""
    log_path = get_today_log_path()
    if log_path.exists():
        # Archive instead of delete
        archive_path = LOG_DIR / "archive" / log_path.name
        archive_path.parent.mkdir(parents=True, exist_ok=True)
        log_path.rename(archive_path)
        print(f"Archived to: {archive_path}")
    else:
        print("No log to clear.")


def show_today():
    """Show today's logged activities."""
    log_data = load_today_log()
    if not log_data["entries"]:
        print("No activities logged today.")
        return

    print(f"\nActivities for {log_data['date']}:")
    print("-" * 40)
    for i, entry in enumerate(log_data["entries"], 1):
        time = entry["timestamp"].split("T")[1][:8]
        status = f" [{entry['status']}]" if entry.get('status') else ""
        print(f"{i}. [{time}] [{entry['category']}] {entry['description']}{status}")


def main():
    if len(sys.argv) < 2:
        print(__doc__)
        return

    command = sys.argv[1].lower()

    if command == "log":
        if len(sys.argv) < 3:
            print("Usage: python daily_activity_log.py log \"description\" --category agent --status Live")
            return

        description = sys.argv[2]
        category = "general"
        status = None
        priority = None

        # Parse optional arguments
        args = sys.argv[3:]
        i = 0
        while i < len(args):
            if args[i] == "--category" and i + 1 < len(args):
                category = args[i + 1]
                i += 2
            elif args[i] == "--status" and i + 1 < len(args):
                status = args[i + 1]
                i += 2
            elif args[i] == "--priority" and i + 1 < len(args):
                priority = args[i + 1]
                i += 2
            else:
                i += 1

        log_activity(description, category, status, priority)

    elif command == "export":
        export_for_claude_desktop()

    elif command == "clear":
        clear_today_log()

    elif command == "show":
        show_today()

    else:
        print(f"Unknown command: {command}")
        print(__doc__)


if __name__ == "__main__":
    main()
