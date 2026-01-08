"""
RLC Persistent Agent
A background-running LLM agent that can perform tasks autonomously.

Usage:
    python agent/agent.py                  # Run in foreground
    python agent/agent.py --daemon         # Run as background daemon
    python agent/agent.py --task "query"   # Submit a single task
"""

import os
import sys
import json
import time
import logging
import argparse
import re
from pathlib import Path
from datetime import datetime
from typing import Optional, List, Dict, Generator
from queue import Queue
import threading

import requests

# Add agent directory to path
sys.path.insert(0, str(Path(__file__).parent))

from config import (
    OLLAMA_HOST, DEFAULT_MODEL, SYSTEM_PROMPT,
    TASK_CHECK_INTERVAL, MAX_TOKENS, LOGS_DIR, TASKS_DIR,
    LOG_LEVEL, LOG_FILE, REQUIRE_APPROVAL
)
from tools import TOOLS, execute_tool, get_tools_description

# ============================================================================
# LOGGING SETUP
# ============================================================================

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("RLC-Agent")

# ============================================================================
# TASK MANAGEMENT
# ============================================================================

class Task:
    """Represents a task for the agent to complete."""

    def __init__(self, description: str, priority: int = 5, task_id: str = None):
        self.id = task_id or datetime.now().strftime("%Y%m%d_%H%M%S_%f")
        self.description = description
        self.priority = priority
        self.status = "pending"
        self.created_at = datetime.now()
        self.started_at = None
        self.completed_at = None
        self.result = None
        self.conversation = []

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "description": self.description,
            "priority": self.priority,
            "status": self.status,
            "created_at": self.created_at.isoformat(),
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
            "result": self.result,
            "conversation": self.conversation
        }

    def save(self):
        """Save task to file."""
        task_file = TASKS_DIR / f"{self.id}.json"
        with open(task_file, 'w') as f:
            json.dump(self.to_dict(), f, indent=2)

    @classmethod
    def load(cls, task_id: str) -> 'Task':
        """Load task from file."""
        task_file = TASKS_DIR / f"{task_id}.json"
        with open(task_file, 'r') as f:
            data = json.load(f)

        task = cls(data['description'], data['priority'], data['id'])
        task.status = data['status']
        task.result = data.get('result')
        task.conversation = data.get('conversation', [])
        return task


class TaskQueue:
    """Manages the queue of tasks for the agent."""

    def __init__(self):
        self.tasks: List[Task] = []
        self.load_pending_tasks()

    def load_pending_tasks(self):
        """Load any pending tasks from disk."""
        for task_file in TASKS_DIR.glob("*.json"):
            try:
                task = Task.load(task_file.stem)
                if task.status == "pending":
                    self.tasks.append(task)
            except Exception as e:
                logger.error(f"Error loading task {task_file}: {e}")

        # Sort by priority
        self.tasks.sort(key=lambda t: t.priority)
        logger.info(f"Loaded {len(self.tasks)} pending tasks")

    def add_task(self, description: str, priority: int = 5) -> Task:
        """Add a new task to the queue."""
        task = Task(description, priority)
        task.save()
        self.tasks.append(task)
        self.tasks.sort(key=lambda t: t.priority)
        logger.info(f"Added task {task.id}: {description[:50]}...")
        return task

    def get_next_task(self) -> Optional[Task]:
        """Get the next pending task."""
        for task in self.tasks:
            if task.status == "pending":
                return task
        return None

    def complete_task(self, task: Task, result: str):
        """Mark a task as completed."""
        task.status = "completed"
        task.completed_at = datetime.now()
        task.result = result
        task.save()
        logger.info(f"Completed task {task.id}")


# ============================================================================
# LLM INTERACTION
# ============================================================================

def check_ollama() -> bool:
    """Check if Ollama is running."""
    try:
        response = requests.get(f"{OLLAMA_HOST}/api/tags", timeout=5)
        return response.status_code == 200
    except:
        return False


def chat_with_llm(messages: List[Dict], model: str = DEFAULT_MODEL) -> str:
    """
    Send messages to the LLM and get a response.

    Args:
        messages: List of {"role": "...", "content": "..."}
        model: Model to use

    Returns:
        LLM response text
    """
    try:
        response = requests.post(
            f"{OLLAMA_HOST}/api/chat",
            json={
                "model": model,
                "messages": messages,
                "stream": False,
                "options": {
                    "num_predict": MAX_TOKENS
                }
            },
            timeout=300
        )

        if response.status_code != 200:
            return f"Error: {response.status_code}"

        data = response.json()
        return data.get("message", {}).get("content", "No response")

    except requests.exceptions.Timeout:
        return "Error: Request timed out"
    except Exception as e:
        return f"Error: {e}"


def stream_chat(messages: List[Dict], model: str = DEFAULT_MODEL) -> Generator[str, None, None]:
    """Stream chat response for interactive use."""
    try:
        response = requests.post(
            f"{OLLAMA_HOST}/api/chat",
            json={
                "model": model,
                "messages": messages,
                "stream": True
            },
            stream=True,
            timeout=300
        )

        for line in response.iter_lines():
            if line:
                data = json.loads(line)
                if "message" in data and "content" in data["message"]:
                    yield data["message"]["content"]

    except Exception as e:
        yield f"Error: {e}"


# ============================================================================
# TOOL PARSING AND EXECUTION
# ============================================================================

def parse_tool_calls(response: str) -> List[Dict]:
    """
    Parse tool calls from LLM response.

    Expected format:
    <tool_call>
    tool: tool_name
    param1: value1
    param2: value2
    </tool_call>
    """
    tool_calls = []

    pattern = r'<tool_call>(.*?)</tool_call>'
    matches = re.findall(pattern, response, re.DOTALL)

    for match in matches:
        lines = match.strip().split('\n')
        tool_call = {}

        for line in lines:
            if ':' in line:
                key, value = line.split(':', 1)
                key = key.strip().lower()
                value = value.strip()

                if key == 'tool':
                    tool_call['tool'] = value
                else:
                    if 'params' not in tool_call:
                        tool_call['params'] = {}
                    tool_call['params'][key] = value

        if 'tool' in tool_call:
            tool_calls.append(tool_call)

    return tool_calls


def build_tool_prompt() -> str:
    """Build the tool usage instructions for the LLM."""
    return f"""
{get_tools_description()}

## How to Use Tools

When you need to use a tool, format your request like this:

<tool_call>
tool: tool_name
param1: value1
param2: value2
</tool_call>

You can include multiple tool calls in one response. I will execute them and provide the results.

## Examples

Search the web:
<tool_call>
tool: web_search
query: USDA soybean export data API
max_results: 5
</tool_call>

Read a file:
<tool_call>
tool: read_file
file_path: scripts/extract_trade_data.py
</tool_call>

Query the database:
<tool_call>
tool: query_database
sql: SELECT COUNT(*) FROM bronze.trade_data_raw
</tool_call>

Always explain what you're doing and why before making tool calls.
"""


# ============================================================================
# AGENT CORE
# ============================================================================

class RLCAgent:
    """The main agent that processes tasks."""

    def __init__(self, model: str = DEFAULT_MODEL):
        self.model = model
        self.task_queue = TaskQueue()
        self.running = False
        self.approval_queue = Queue()

    def build_system_message(self) -> str:
        """Build the full system message with tools."""
        return f"{SYSTEM_PROMPT}\n\n{build_tool_prompt()}"

    def process_task(self, task: Task) -> str:
        """
        Process a single task.

        Args:
            task: Task to process

        Returns:
            Final result/response
        """
        task.status = "in_progress"
        task.started_at = datetime.now()
        task.save()

        logger.info(f"Processing task: {task.description[:100]}...")

        # Initialize conversation
        messages = [
            {"role": "system", "content": self.build_system_message()},
            {"role": "user", "content": task.description}
        ]

        max_iterations = 10
        iteration = 0

        while iteration < max_iterations:
            iteration += 1
            logger.debug(f"Task iteration {iteration}")

            # Get LLM response
            response = chat_with_llm(messages, self.model)
            task.conversation.append({"role": "assistant", "content": response})
            task.save()

            # Check for tool calls
            tool_calls = parse_tool_calls(response)

            if not tool_calls:
                # No tool calls - task is complete
                logger.info(f"Task completed in {iteration} iterations")
                return response

            # Execute tool calls
            tool_results = []
            for tc in tool_calls:
                tool_name = tc.get('tool')
                params = tc.get('params', {})

                logger.info(f"Executing tool: {tool_name}")
                result = execute_tool(tool_name, **params)

                # Check for approval requirements
                if result.startswith("APPROVAL_REQUIRED:"):
                    logger.warning(f"Approval required: {result}")
                    # For now, just note it - in production, would wait for approval
                    result = f"[Approval pending] {result}"

                tool_results.append({
                    "tool": tool_name,
                    "result": result
                })

            # Add tool results to conversation
            results_text = "## Tool Results\n\n"
            for tr in tool_results:
                results_text += f"### {tr['tool']}\n{tr['result']}\n\n"

            messages.append({"role": "assistant", "content": response})
            messages.append({"role": "user", "content": results_text + "\nPlease continue with the task based on these results."})

            task.conversation.append({"role": "tool_results", "content": results_text})
            task.save()

        return "Task reached maximum iterations. Last response:\n\n" + response

    def run_once(self):
        """Process a single task from the queue."""
        task = self.task_queue.get_next_task()

        if task:
            try:
                result = self.process_task(task)
                self.task_queue.complete_task(task, result)
                return True
            except Exception as e:
                logger.error(f"Error processing task: {e}")
                task.status = "error"
                task.result = str(e)
                task.save()
        return False

    def run_continuous(self):
        """Run continuously, processing tasks as they arrive."""
        logger.info("Starting continuous agent loop...")
        self.running = True

        while self.running:
            try:
                # Check for new task files
                self.task_queue.load_pending_tasks()

                # Process any pending tasks
                if not self.run_once():
                    # No tasks - wait before checking again
                    time.sleep(TASK_CHECK_INTERVAL)

            except KeyboardInterrupt:
                logger.info("Received shutdown signal")
                self.running = False
            except Exception as e:
                logger.error(f"Agent loop error: {e}")
                time.sleep(5)

        logger.info("Agent stopped")

    def interactive_mode(self):
        """Run in interactive mode for direct conversation."""
        print("\n" + "="*60)
        print("RLC Agent - Interactive Mode")
        print("="*60)
        print("Type 'quit' to exit, 'tools' to list tools")
        print("="*60 + "\n")

        messages = [{"role": "system", "content": self.build_system_message()}]

        while True:
            try:
                user_input = input("\nYou: ").strip()

                if not user_input:
                    continue

                if user_input.lower() == 'quit':
                    break

                if user_input.lower() == 'tools':
                    print(get_tools_description())
                    continue

                messages.append({"role": "user", "content": user_input})

                # Show thinking indicator
                print("\nAgent: [Thinking...] ", end="", flush=True)

                full_response = ""
                first_chunk = True
                for chunk in stream_chat(messages, self.model):
                    if first_chunk:
                        # Clear the "Thinking..." indicator
                        print("\r" + " "*30 + "\r", end="", flush=True)
                        print("Agent: ", end="", flush=True)
                        first_chunk = False
                    print(chunk, end="", flush=True)
                    full_response += chunk

                print()

                # Check for tool calls
                tool_calls = parse_tool_calls(full_response)

                if tool_calls:
                    print("\n[Executing tools...]")

                    for tc in tool_calls:
                        tool_name = tc.get('tool')
                        params = tc.get('params', {})

                        print(f"\n> {tool_name}: ", end="")
                        result = execute_tool(tool_name, **params)
                        print(result[:500] + "..." if len(result) > 500 else result)

                        messages.append({"role": "assistant", "content": full_response})
                        messages.append({"role": "user", "content": f"Tool result for {tool_name}:\n{result}"})

                        # Get follow-up response
                        print("\nAgent: [Thinking...] ", end="", flush=True)
                        full_response = ""
                        first_chunk = True
                        for chunk in stream_chat(messages, self.model):
                            if first_chunk:
                                print("\r" + " "*30 + "\r", end="", flush=True)
                                print("Agent: ", end="", flush=True)
                                first_chunk = False
                            print(chunk, end="", flush=True)
                            full_response += chunk
                        print()

                messages.append({"role": "assistant", "content": full_response})

            except KeyboardInterrupt:
                print("\n\nInterrupted. Type 'quit' to exit.")


# ============================================================================
# MAIN
# ============================================================================

def submit_task(description: str, priority: int = 5):
    """Submit a task to the queue."""
    task_queue = TaskQueue()
    task = task_queue.add_task(description, priority)
    print(f"Task submitted: {task.id}")
    return task.id


def main():
    parser = argparse.ArgumentParser(description="RLC Persistent Agent")
    parser.add_argument("--daemon", action="store_true", help="Run as background daemon")
    parser.add_argument("--interactive", action="store_true", help="Run in interactive mode")
    parser.add_argument("--task", type=str, help="Submit a task")
    parser.add_argument("--priority", type=int, default=5, help="Task priority (1-10, lower=higher)")
    parser.add_argument("--model", default=DEFAULT_MODEL, help="LLM model to use")
    parser.add_argument("--status", action="store_true", help="Show agent status")

    args = parser.parse_args()

    # Check Ollama
    if not check_ollama():
        print("Error: Ollama is not running. Start it with: ollama serve")
        sys.exit(1)

    if args.status:
        task_queue = TaskQueue()
        pending = [t for t in task_queue.tasks if t.status == "pending"]
        print(f"Ollama: Running")
        print(f"Pending tasks: {len(pending)}")
        for task in pending:
            print(f"  - [{task.id}] {task.description[:50]}...")
        return

    if args.task:
        submit_task(args.task, args.priority)
        return

    agent = RLCAgent(model=args.model)

    if args.daemon:
        print("Starting agent in daemon mode...")
        agent.run_continuous()
    elif args.interactive:
        agent.interactive_mode()
    else:
        # Default: interactive mode
        agent.interactive_mode()


if __name__ == "__main__":
    main()
