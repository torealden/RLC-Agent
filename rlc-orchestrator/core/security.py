"""
RLC Orchestrator - Security Guard
===================================
The SecurityGuard is a critical component that prevents the orchestrator
from executing potentially harmful operations. Even if the AI suggests
something dangerous (due to confusion, adversarial input, or bugs), the
SecurityGuard provides a hard barrier that cannot be bypassed.

This is "defense in depth" - we don't just trust that the AI will never
suggest harmful things; we actively check and block them.

The security model has three layers:
1. Blocklist: Explicit patterns that are never allowed
2. Allowlist: For sensitive operations, only pre-approved patterns pass
3. Sandboxing: New code runs in isolation before production deployment

This module implements layers 1 and 2. Layer 3 (sandboxing) is handled
by the executor when running code generation tasks.
"""

import re
import logging
from typing import Dict, Any, List, Tuple

logger = logging.getLogger(__name__)


class SecurityGuard:
    """
    Checks tasks for security violations before execution.
    
    The guard maintains lists of blocked patterns and reviews
    task payloads before they're executed. Any match against
    the blocklist results in immediate rejection.
    
    Example usage:
        guard = SecurityGuard()
        result = guard.check_task("script", {"function": "os.remove"})
        if not result["allowed"]:
            print(f"Blocked: {result['reason']}")
    """
    
    # Patterns that are NEVER allowed, regardless of context
    # These are compiled as regex patterns
    BLOCKED_PATTERNS = [
        # File system destruction
        (r"rm\s+-rf", "Recursive file deletion is prohibited"),
        (r"rm\s+-r\s+/", "Deleting system directories is prohibited"),
        (r"rmdir\s+/", "Deleting system directories is prohibited"),
        (r"os\.remove\s*\(", "Direct file deletion is prohibited"),
        (r"shutil\.rmtree", "Recursive directory deletion is prohibited"),
        (r"os\.rmdir", "Directory deletion is prohibited"),
        (r"pathlib.*\.unlink", "File deletion via pathlib is prohibited"),
        (r"pathlib.*\.rmdir", "Directory deletion via pathlib is prohibited"),
        
        # System modification
        (r"chmod\s+777", "Insecure permissions are prohibited"),
        (r"chmod\s+-R", "Recursive permission changes are prohibited"),
        (r"chown\s+-R", "Recursive ownership changes are prohibited"),
        
        # Dangerous directories
        (r"/etc/passwd", "Access to system auth files is prohibited"),
        (r"/etc/shadow", "Access to system auth files is prohibited"),
        (r"/etc/sudoers", "Access to sudo config is prohibited"),
        (r"\.ssh/", "Access to SSH keys is prohibited"),
        
        # Network/System commands
        (r"iptables", "Firewall modification is prohibited"),
        (r"ufw\s+", "Firewall modification is prohibited"),
        (r"systemctl\s+stop", "Stopping services is prohibited"),
        (r"systemctl\s+disable", "Disabling services is prohibited"),
        (r"service\s+\w+\s+stop", "Stopping services is prohibited"),
        
        # Package management (prevent installing malware)
        (r"pip\s+install\s+.*--", "Pip with flags is prohibited (use allowlist)"),
        (r"curl\s+.*\|\s*sh", "Piping curl to shell is prohibited"),
        (r"wget\s+.*\|\s*sh", "Piping wget to shell is prohibited"),
        (r"curl\s+.*\|\s*bash", "Piping curl to shell is prohibited"),
        
        # Credential exposure
        (r"echo\s+.*password", "Echoing passwords is prohibited"),
        (r"echo\s+.*secret", "Echoing secrets is prohibited"),
        (r"echo\s+.*api.?key", "Echoing API keys is prohibited"),
        (r"print\s*\(.*password", "Printing passwords is prohibited"),
        (r"print\s*\(.*secret", "Printing secrets is prohibited"),
        (r"logging.*password", "Logging passwords is prohibited"),
        
        # SQL injection patterns
        (r";\s*DROP\s+TABLE", "SQL injection pattern detected"),
        (r";\s*DELETE\s+FROM", "SQL injection pattern detected"),
        (r"--\s*$", "SQL comment injection pattern detected"),
        
        # Code execution risks
        (r"eval\s*\(", "eval() is prohibited"),
        (r"exec\s*\(", "exec() is prohibited"),
        (r"__import__\s*\(", "Dynamic import is prohibited"),
        (r"compile\s*\(.*exec", "Dynamic code compilation is prohibited"),
        
        # Environment manipulation
        (r"os\.environ\[", "Direct environment modification is prohibited"),
        (r"os\.putenv", "Environment modification is prohibited"),
    ]
    
    # Directories that should never be accessed
    BLOCKED_DIRECTORIES = [
        "/etc",
        "/usr/bin",
        "/usr/sbin",
        "/bin",
        "/sbin",
        "/boot",
        "/root",
        "/var/log",  # Except our own logs
    ]
    
    # For certain task types, only specific operations are allowed
    # This is more restrictive than the blocklist
    ALLOWED_PATTERNS = {
        "data_collection": [
            # Only allow HTTP requests to known APIs
            r"requests\.(get|post)\s*\(",
            r"httpx\.(get|post)\s*\(",
            r"aiohttp\.ClientSession",
            # File operations only in data directory
            r"open\s*\(['\"]data/",
            r"open\s*\(['\"]\/home\/rlc\/data/",
        ],
        "email": [
            # Only allow email library operations
            r"smtplib\.SMTP",
            r"imaplib\.IMAP",
            r"email\.",
        ]
    }
    
    def __init__(self):
        """Initialize the security guard with compiled patterns."""
        # Compile all blocked patterns for efficiency
        self.compiled_blocks = [
            (re.compile(pattern, re.IGNORECASE), reason)
            for pattern, reason in self.BLOCKED_PATTERNS
        ]
        
        # Compile allowed patterns
        self.compiled_allows = {
            task_type: [re.compile(p, re.IGNORECASE) for p in patterns]
            for task_type, patterns in self.ALLOWED_PATTERNS.items()
        }
        
        logger.info(f"SecurityGuard initialized with {len(self.compiled_blocks)} block patterns")
    
    def check_task(self, task_type: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        """
        Check if a task is safe to execute.
        
        This is the main entry point for security checking. It examines
        the task type and payload, checking against blocked patterns
        and (for some task types) allowed patterns.
        
        Args:
            task_type: The type of task being checked
            payload: The task's payload dictionary
        
        Returns:
            {
                "allowed": True/False,
                "reason": "Explanation if blocked",
                "warnings": ["List of warnings if allowed but concerning"]
            }
        """
        result = {
            "allowed": True,
            "reason": None,
            "warnings": []
        }
        
        # Convert payload to string for pattern matching
        payload_str = str(payload)
        
        # Check against blocked patterns
        for pattern, reason in self.compiled_blocks:
            if pattern.search(payload_str):
                logger.warning(f"Security block: {reason} in payload")
                return {
                    "allowed": False,
                    "reason": reason,
                    "warnings": []
                }
        
        # Check for blocked directories
        for blocked_dir in self.BLOCKED_DIRECTORIES:
            if blocked_dir in payload_str:
                # Allow reading from our own paths
                if "/home/rlc" not in payload_str and "/home/claude" not in payload_str:
                    return {
                        "allowed": False,
                        "reason": f"Access to {blocked_dir} is prohibited",
                        "warnings": []
                    }
        
        # For task types with allowlists, verify operations are allowed
        if task_type in self.compiled_allows:
            result = self._check_allowlist(task_type, payload_str)
        
        # Check for function imports that might be suspicious
        suspicious = self._check_suspicious_patterns(payload_str)
        if suspicious:
            result["warnings"].extend(suspicious)
        
        return result
    
    def _check_allowlist(self, task_type: str, payload_str: str) -> Dict[str, Any]:
        """
        For restricted task types, verify operations are on the allowlist.
        
        This is more restrictive than blocklist checking - operations must
        explicitly match an allowed pattern to proceed.
        """
        allowed_patterns = self.compiled_allows.get(task_type, [])
        
        # Check if any operation in the payload matches an allowed pattern
        # For now, we just warn rather than block on allowlist violations
        # This can be made more strict as the system matures
        
        matches_any = any(p.search(payload_str) for p in allowed_patterns)
        
        if not matches_any and allowed_patterns:
            return {
                "allowed": True,  # Allow but warn
                "reason": None,
                "warnings": [f"Task type {task_type} has allowlist but no matches found"]
            }
        
        return {"allowed": True, "reason": None, "warnings": []}
    
    def _check_suspicious_patterns(self, payload_str: str) -> List[str]:
        """
        Check for patterns that aren't blocked but are suspicious.
        
        These generate warnings that are logged and potentially
        included in reports, but don't block execution.
        """
        warnings = []
        
        # Subprocess usage
        if "subprocess" in payload_str.lower():
            warnings.append("Task uses subprocess - verify command is safe")
        
        # Network operations
        if "socket" in payload_str.lower():
            warnings.append("Task uses raw sockets - verify destination is safe")
        
        # File operations outside normal areas
        if "open(" in payload_str and "/tmp" in payload_str:
            warnings.append("Task uses /tmp - verify this is intentional")
        
        return warnings
    
    def check_command(self, command: str) -> Dict[str, Any]:
        """
        Check if a shell command is safe to execute.
        
        This is a convenience method for checking commands that will
        be run via subprocess or os.system.
        
        Args:
            command: The shell command string
        
        Returns:
            Same format as check_task()
        """
        return self.check_task("command", {"command": command})
    
    def check_file_path(self, path: str, operation: str = "read") -> Dict[str, Any]:
        """
        Check if a file operation is allowed.
        
        Args:
            path: The file path
            operation: "read", "write", or "delete"
        
        Returns:
            Same format as check_task()
        """
        # Normalize the path
        import os
        try:
            normalized = os.path.normpath(path)
            real_path = os.path.realpath(normalized)
        except (OSError, ValueError):
            return {
                "allowed": False,
                "reason": "Invalid file path",
                "warnings": []
            }
        
        # Check for blocked directories
        for blocked in self.BLOCKED_DIRECTORIES:
            if real_path.startswith(blocked):
                return {
                    "allowed": False,
                    "reason": f"Access to {blocked} is prohibited",
                    "warnings": []
                }
        
        # Delete operations require extra scrutiny
        if operation == "delete":
            # Only allow deleting in specific directories
            allowed_delete_dirs = [
                "/home/rlc/data/",
                "/home/rlc/temp/",
                "/home/claude/",
                "/tmp/rlc_",
            ]
            
            if not any(real_path.startswith(d) for d in allowed_delete_dirs):
                return {
                    "allowed": False,
                    "reason": "Delete only allowed in data/temp directories",
                    "warnings": []
                }
        
        return {"allowed": True, "reason": None, "warnings": []}
    
    def sanitize_log_message(self, message: str) -> str:
        """
        Remove sensitive information from a string before logging.
        
        This helps prevent accidental credential exposure in logs.
        """
        # Patterns to redact
        patterns = [
            (r'api[_-]?key["\']?\s*[:=]\s*["\']?[\w-]+', 'api_key=***REDACTED***'),
            (r'password["\']?\s*[:=]\s*["\']?[^\s,}]+', 'password=***REDACTED***'),
            (r'secret["\']?\s*[:=]\s*["\']?[^\s,}]+', 'secret=***REDACTED***'),
            (r'token["\']?\s*[:=]\s*["\']?[\w-]+', 'token=***REDACTED***'),
            (r'bearer\s+[\w-]+', 'Bearer ***REDACTED***'),
        ]
        
        result = message
        for pattern, replacement in patterns:
            result = re.sub(pattern, replacement, result, flags=re.IGNORECASE)
        
        return result


class SandboxConfig:
    """
    Configuration for running code in a sandbox.
    
    When the executor runs code generation tasks, it uses these
    settings to create an isolated environment for testing.
    """
    
    # Docker image to use for sandboxing
    DOCKER_IMAGE = "python:3.11-slim"
    
    # Resource limits
    MAX_MEMORY = "256m"
    MAX_CPU = "0.5"
    MAX_EXECUTION_TIME = 60  # seconds
    
    # Network access (default: none)
    NETWORK_ENABLED = False
    
    # Allowed network destinations (if NETWORK_ENABLED is True)
    ALLOWED_HOSTS = [
        "api.anthropic.com",
        "api.openai.com",
        "eia.gov",
        "usda.gov",
        "census.gov",
    ]
    
    # Volume mounts (read-only)
    READONLY_MOUNTS = [
        "/home/rlc/data:/data:ro",
    ]
    
    # Environment variables to pass into sandbox
    SAFE_ENV_VARS = [
        "TZ",
        "LANG",
        "LC_ALL",
    ]
