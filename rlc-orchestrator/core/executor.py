"""
RLC Orchestrator - Task Executor
=================================
This is the workhorse of the orchestrator. The executor runs in a continuous
loop, pulling tasks from the queue and executing them. It implements the
"heartbeat" of the system - even when there's nothing to do, it keeps
running, ready to pick up new work.

The executor is designed to be resilient:
- It handles errors gracefully without crashing
- It respects task dependencies and scheduling
- It routes different task types to appropriate handlers
- It maintains detailed logs for debugging

Think of the executor as a diligent employee who never sleeps, never
complains, and always follows the rules you've set.
"""

import time
import logging
import importlib
import traceback
from datetime import datetime
from typing import Callable, Dict, Any, Optional

from .database import Task, TaskType, TaskStatus, ExecutionLog, get_session, get_engine
from .queue import TaskQueue
from .security import SecurityGuard

logger = logging.getLogger(__name__)


class TaskExecutor:
    """
    The main execution engine for the orchestrator.
    
    The executor runs a loop that continuously checks for pending tasks
    and executes them. It's designed to run as a daemon process, starting
    when the system boots and running indefinitely.
    
    Key features:
    - Continuous polling with configurable sleep between checks
    - Task routing based on task_type
    - Error handling with retries
    - Execution logging for audit trails
    - Security checks before executing anything
    
    Example usage:
        # In main.py or a systemd service
        executor = TaskExecutor(
            db_path="data/orchestrator.db",
            poll_interval=5  # Check every 5 seconds
        )
        executor.run()  # This runs forever
    """
    
    def __init__(
        self,
        db_path: str = "data/orchestrator.db",
        poll_interval: int = 5,
        ai_gateway = None,
        email_client = None
    ):
        """
        Initialize the executor.
        
        Args:
            db_path: Path to the SQLite database
            poll_interval: Seconds to wait between queue checks
            ai_gateway: Optional AI gateway for reasoning tasks
            email_client: Optional email client for notifications
        """
        self.db_path = db_path
        self.poll_interval = poll_interval
        self.ai_gateway = ai_gateway
        self.email_client = email_client
        
        # Security guard checks all operations before execution
        self.security = SecurityGuard()
        
        # Custom handlers for different task types
        # Users can register their own handlers for extensibility
        self.handlers: Dict[str, Callable] = {}
        
        # Flag to control the main loop (set False to stop gracefully)
        self.running = False
        
        # Statistics for monitoring
        self.stats = {
            "tasks_executed": 0,
            "tasks_succeeded": 0,
            "tasks_failed": 0,
            "start_time": None
        }
        
        # Register default handlers
        self._register_default_handlers()
    
    def _register_default_handlers(self):
        """
        Register the built-in handlers for common task types.
        
        Each handler is a function that takes (task, context) and returns
        a result. Handlers can raise exceptions on failure.
        """
        self.handlers[TaskType.SCRIPT.value] = self._handle_script_task
        self.handlers[TaskType.AI_REASONING.value] = self._handle_ai_task
        self.handlers[TaskType.CODE_GENERATION.value] = self._handle_code_generation_task
        self.handlers[TaskType.EMAIL.value] = self._handle_email_task
        self.handlers[TaskType.HUMAN_INPUT.value] = self._handle_human_input_task
    
    def register_handler(self, task_type: str, handler: Callable) -> None:
        """
        Register a custom handler for a task type.
        
        This allows you to extend the executor with new capabilities
        without modifying the core code.
        
        Args:
            task_type: The task type string this handler processes
            handler: Function that takes (task, context) and returns result
        
        Example:
            def my_custom_handler(task, context):
                payload = task.get_payload()
                # Do something with payload...
                return {"status": "done"}
            
            executor.register_handler("custom_type", my_custom_handler)
        """
        self.handlers[task_type] = handler
        logger.info(f"Registered handler for task type: {task_type}")
    
    def run(self) -> None:
        """
        Start the main execution loop.
        
        This method runs forever (until self.running is set to False).
        It's designed to be run as the main process in a systemd service
        or similar daemon manager.
        
        The loop:
        1. Checks for pending tasks
        2. Executes the next available task
        3. Handles any errors gracefully
        4. Sleeps briefly before checking again
        """
        self.running = True
        self.stats["start_time"] = datetime.utcnow()
        
        logger.info("=" * 60)
        logger.info("RLC Orchestrator starting up")
        logger.info(f"Database: {self.db_path}")
        logger.info(f"Poll interval: {self.poll_interval} seconds")
        logger.info("=" * 60)
        
        # Create database session for this thread
        engine = get_engine(self.db_path)
        session = get_session(engine)
        queue = TaskQueue(session)
        
        while self.running:
            try:
                # Check for pending tasks
                task = queue.get_next_pending()
                
                if task:
                    self._execute_task(task, queue, session)
                else:
                    # No tasks ready, sleep before checking again
                    time.sleep(self.poll_interval)
                
            except KeyboardInterrupt:
                logger.info("Received shutdown signal")
                self.running = False
                
            except Exception as e:
                # Log the error but don't crash - we want to keep running
                logger.error(f"Unexpected error in main loop: {e}")
                logger.error(traceback.format_exc())
                time.sleep(self.poll_interval)
        
        logger.info("Orchestrator shutting down gracefully")
        session.close()
    
    def _execute_task(self, task: Task, queue: TaskQueue, session) -> None:
        """
        Execute a single task.
        
        This method handles the full lifecycle of task execution:
        1. Security check
        2. Status update (mark as in progress)
        3. Create execution log
        4. Route to appropriate handler
        5. Handle success or failure
        6. Update execution log
        """
        logger.info(f"Executing task {task.id}: {task.name}")
        
        # Security check - make sure this task is safe to execute
        payload = task.get_payload()
        security_result = self.security.check_task(task.task_type, payload)
        
        if not security_result["allowed"]:
            queue.fail_task(task, f"Security check failed: {security_result['reason']}", retry=False)
            self._send_security_alert(task, security_result["reason"])
            return
        
        # Mark task as started
        queue.start_task(task)
        
        # Create execution log entry
        exec_log = ExecutionLog(task_id=task.id)
        session.add(exec_log)
        session.commit()
        
        # Track timing
        start_time = datetime.utcnow()
        
        try:
            # Find the appropriate handler
            handler = self.handlers.get(task.task_type)
            
            if not handler:
                raise ValueError(f"No handler registered for task type: {task.task_type}")
            
            # Build execution context
            context = {
                "session": session,
                "queue": queue,
                "ai_gateway": self.ai_gateway,
                "email_client": self.email_client,
                "security": self.security
            }
            
            # Execute the handler
            result = handler(task, context)
            
            # Success!
            queue.complete_task(task, result)
            
            # Update execution log
            exec_log.success = True
            exec_log.completed_at = datetime.utcnow()
            exec_log.duration_seconds = int((exec_log.completed_at - start_time).total_seconds())
            
            # Update stats
            self.stats["tasks_executed"] += 1
            self.stats["tasks_succeeded"] += 1
            
            logger.info(f"Task {task.id} completed successfully")
            
        except Exception as e:
            # Task failed
            error_msg = str(e)
            logger.error(f"Task {task.id} failed: {error_msg}")
            logger.error(traceback.format_exc())
            
            queue.fail_task(task, error_msg)
            
            # Update execution log
            exec_log.success = False
            exec_log.error_message = error_msg
            exec_log.completed_at = datetime.utcnow()
            exec_log.log_output = traceback.format_exc()
            
            # Update stats
            self.stats["tasks_executed"] += 1
            self.stats["tasks_failed"] += 1
        
        session.commit()
    
    def _handle_script_task(self, task: Task, context: Dict) -> Any:
        """
        Handle SCRIPT type tasks.
        
        Script tasks execute a Python function specified in the payload.
        The function should be importable and callable.
        
        Payload format:
        {
            "function": "module.path.function_name",
            "args": {"arg1": value1, ...},
            "kwargs": {"kwarg1": value1, ...}
        }
        """
        payload = task.get_payload()
        
        function_path = payload.get("function")
        if not function_path:
            raise ValueError("Script task missing 'function' in payload")
        
        # Parse module and function name
        parts = function_path.rsplit(".", 1)
        if len(parts) != 2:
            raise ValueError(f"Invalid function path: {function_path}")
        
        module_path, function_name = parts
        
        # Import the module and get the function
        try:
            module = importlib.import_module(module_path)
            function = getattr(module, function_name)
        except (ImportError, AttributeError) as e:
            raise ValueError(f"Could not import {function_path}: {e}")
        
        # Execute the function
        args = payload.get("args", {})
        kwargs = payload.get("kwargs", {})
        
        return function(**args, **kwargs) if args else function(**kwargs)
    
    def _handle_ai_task(self, task: Task, context: Dict) -> Any:
        """
        Handle AI_REASONING type tasks.
        
        These tasks require calling out to an AI API (Claude, GPT-4, etc.)
        for reasoning, analysis, or generation that requires intelligence.
        
        Payload format:
        {
            "prompt": "The prompt to send to the AI",
            "system_prompt": "Optional system context",
            "context": {...additional context...},
            "model": "claude-3-opus" (optional, uses default if not specified)
        }
        """
        ai_gateway = context.get("ai_gateway")
        
        if not ai_gateway:
            raise ValueError("AI task requires ai_gateway but none configured")
        
        payload = task.get_payload()
        prompt = payload.get("prompt")
        
        if not prompt:
            raise ValueError("AI task missing 'prompt' in payload")
        
        # Call the AI gateway
        result = ai_gateway.complete(
            prompt=prompt,
            system_prompt=payload.get("system_prompt"),
            context=payload.get("context"),
            model=payload.get("model")
        )
        
        return result
    
    def _handle_code_generation_task(self, task: Task, context: Dict) -> Any:
        """
        Handle CODE_GENERATION type tasks.
        
        Code generation is special because it always requires human review
        before the generated code is deployed. This handler generates the
        code and then puts the task into WAITING_FOR_HUMAN status.
        
        Payload format:
        {
            "description": "What code should be generated",
            "requirements": ["list", "of", "requirements"],
            "template": "optional template to start from",
            "language": "python" (default)
        }
        """
        ai_gateway = context.get("ai_gateway")
        queue = context.get("queue")
        
        if not ai_gateway:
            raise ValueError("Code generation requires ai_gateway but none configured")
        
        payload = task.get_payload()
        description = payload.get("description")
        
        if not description:
            raise ValueError("Code generation task missing 'description' in payload")
        
        # Build the code generation prompt
        prompt = f"""Generate Python code for the following requirement:

{description}

Requirements:
{chr(10).join('- ' + r for r in payload.get('requirements', []))}

Please provide:
1. The complete, working code
2. Comments explaining key sections
3. Any configuration or setup needed
4. Example usage

Format your response as a code block.
"""
        
        # Generate the code
        result = ai_gateway.complete(
            prompt=prompt,
            system_prompt="You are an expert Python developer. Write clean, well-documented code.",
            model=payload.get("model")
        )
        
        # Store the generated code in the task result
        task.set_result({"generated_code": result})
        
        # Request human review
        queue.request_human_input(
            task,
            f"Code generated for: {description}\n\nPlease review the attached code and reply APPROVED to deploy, or CHANGES: [notes] to request modifications.",
            interaction_type="approval"
        )
        
        # Return partial result - task will continue after human approval
        return {"status": "awaiting_review", "code": result}
    
    def _handle_email_task(self, task: Task, context: Dict) -> Any:
        """
        Handle EMAIL type tasks.
        
        These tasks send emails, typically for notifications, approvals,
        or human-in-the-loop communications.
        
        Payload format:
        {
            "to": "recipient@example.com",
            "subject": "Email subject",
            "body": "Email body text",
            "html_body": "Optional HTML version",
            "attachments": [{"filename": "...", "data": "..."}]
        }
        """
        email_client = context.get("email_client")
        
        if not email_client:
            raise ValueError("Email task requires email_client but none configured")
        
        payload = task.get_payload()
        
        return email_client.send(
            to=payload.get("to"),
            subject=payload.get("subject"),
            body=payload.get("body"),
            html_body=payload.get("html_body"),
            attachments=payload.get("attachments", [])
        )
    
    def _handle_human_input_task(self, task: Task, context: Dict) -> Any:
        """
        Handle HUMAN_INPUT type tasks.
        
        These tasks explicitly require human input before they can proceed.
        The task immediately enters WAITING_FOR_HUMAN status.
        
        Payload format:
        {
            "question": "What to ask the human",
            "context": "Additional context to include",
            "options": ["optional", "list", "of", "choices"]
        }
        """
        queue = context.get("queue")
        email_client = context.get("email_client")
        
        payload = task.get_payload()
        question = payload.get("question", "Input needed")
        
        # Build the request message
        message = question
        if payload.get("context"):
            message = f"{question}\n\nContext:\n{payload['context']}"
        if payload.get("options"):
            message += f"\n\nOptions: {', '.join(payload['options'])}"
        
        # Put task in waiting state
        interaction = queue.request_human_input(
            task,
            message,
            interaction_type="question"
        )
        
        # Send email notification if email client is available
        if email_client:
            email_client.send(
                to=email_client.default_recipient,
                subject=f"[RLC-QUESTION] {task.name}",
                body=f"""A task needs your input:

Task: {task.name}

{message}

Reply to this email with your response."""
            )
        
        return {"status": "waiting_for_human", "interaction_id": interaction.id}
    
    def _send_security_alert(self, task: Task, reason: str) -> None:
        """Send an alert when a security check fails."""
        if self.email_client:
            self.email_client.send(
                to=self.email_client.default_recipient,
                subject=f"[RLC-ALERT] Security check failed for task {task.id}",
                body=f"""A task was blocked by the security guard:

Task ID: {task.id}
Task Name: {task.name}
Reason: {reason}

This may indicate a bug in task creation or a potential security issue.
Please review the task and determine the cause."""
            )
    
    def stop(self) -> None:
        """Signal the executor to stop gracefully."""
        self.running = False
        logger.info("Stop signal received, will exit after current task")
    
    def get_stats(self) -> Dict[str, Any]:
        """Get execution statistics."""
        stats = dict(self.stats)
        if stats["start_time"]:
            stats["uptime_seconds"] = int((datetime.utcnow() - stats["start_time"]).total_seconds())
        return stats
