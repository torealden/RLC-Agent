"""
Submit a task to the RLC Agent.

Usage:
    python agent/submit_task.py "Search for new oilseed data APIs"
    python agent/submit_task.py "Analyze soybean crush margins" --priority 3
"""

import sys
import json
import argparse
from pathlib import Path
from datetime import datetime

TASKS_DIR = Path(__file__).parent / "tasks"
TASKS_DIR.mkdir(exist_ok=True)


def submit_task(description: str, priority: int = 5) -> str:
    """Submit a task to the agent queue."""
    task_id = datetime.now().strftime("%Y%m%d_%H%M%S_%f")

    task = {
        "id": task_id,
        "description": description,
        "priority": priority,
        "status": "pending",
        "created_at": datetime.now().isoformat(),
        "started_at": None,
        "completed_at": None,
        "result": None,
        "conversation": []
    }

    task_file = TASKS_DIR / f"{task_id}.json"
    with open(task_file, 'w') as f:
        json.dump(task, f, indent=2)

    print(f"Task submitted successfully!")
    print(f"  ID: {task_id}")
    print(f"  Description: {description}")
    print(f"  Priority: {priority}")
    print(f"  File: {task_file}")

    return task_id


def list_tasks():
    """List all tasks."""
    tasks = []
    for task_file in TASKS_DIR.glob("*.json"):
        with open(task_file, 'r') as f:
            tasks.append(json.load(f))

    # Sort by status then priority
    tasks.sort(key=lambda t: (t['status'] != 'pending', t['priority']))

    if not tasks:
        print("No tasks found.")
        return

    print("\n" + "="*70)
    print(f"{'ID':<25} {'Status':<12} {'Pri':<4} Description")
    print("="*70)

    for task in tasks:
        desc = task['description'][:30] + "..." if len(task['description']) > 30 else task['description']
        print(f"{task['id']:<25} {task['status']:<12} {task['priority']:<4} {desc}")

    print("="*70)


def view_task(task_id: str):
    """View details of a specific task."""
    task_file = TASKS_DIR / f"{task_id}.json"

    if not task_file.exists():
        print(f"Task not found: {task_id}")
        return

    with open(task_file, 'r') as f:
        task = json.load(f)

    print("\n" + "="*70)
    print(f"Task: {task['id']}")
    print("="*70)
    print(f"Status: {task['status']}")
    print(f"Priority: {task['priority']}")
    print(f"Created: {task['created_at']}")
    print(f"Started: {task['started_at'] or 'Not started'}")
    print(f"Completed: {task['completed_at'] or 'Not completed'}")
    print(f"\nDescription:\n{task['description']}")

    if task['result']:
        print(f"\nResult:\n{task['result']}")

    if task['conversation']:
        print(f"\nConversation ({len(task['conversation'])} messages)")


def main():
    parser = argparse.ArgumentParser(description="Submit tasks to RLC Agent")
    parser.add_argument("task", nargs="?", help="Task description")
    parser.add_argument("--priority", "-p", type=int, default=5, help="Priority (1-10, lower=higher)")
    parser.add_argument("--list", "-l", action="store_true", help="List all tasks")
    parser.add_argument("--view", "-v", type=str, help="View task details")

    args = parser.parse_args()

    if args.list:
        list_tasks()
    elif args.view:
        view_task(args.view)
    elif args.task:
        submit_task(args.task, args.priority)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
