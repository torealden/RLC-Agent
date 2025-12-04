"""
Tool Registry - Central registry for all tools available to agents.

Tools are modular, reusable capabilities that encapsulate specific operations.
Agents call tools through the registry to perform actions.
"""

import asyncio
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional, Type
import time

logger = logging.getLogger(__name__)


@dataclass
class ToolResult:
    """Result from a tool execution"""
    success: bool
    data: Any = None
    error: Optional[str] = None
    execution_time: float = 0.0
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            'success': self.success,
            'data': self.data,
            'error': self.error,
            'execution_time': round(self.execution_time, 4),
            'metadata': self.metadata
        }


class Tool(ABC):
    """
    Abstract base class for tools.

    Tools are stateless, reusable operations that agents can invoke.
    Each tool should:
    - Have a clear, single responsibility
    - Be idempotent when possible
    - Return structured ToolResult
    - Handle errors gracefully
    """

    @property
    @abstractmethod
    def name(self) -> str:
        """Unique name for this tool"""
        pass

    @property
    @abstractmethod
    def description(self) -> str:
        """Human-readable description of what this tool does"""
        pass

    @property
    def parameters(self) -> Dict[str, Any]:
        """
        Description of expected parameters.

        Override to provide parameter documentation.

        Returns:
            Dict describing each parameter
        """
        return {}

    @abstractmethod
    async def execute(self, params: Dict[str, Any]) -> ToolResult:
        """
        Execute the tool with given parameters.

        Args:
            params: Dictionary of parameters

        Returns:
            ToolResult with execution outcome
        """
        pass

    async def validate_params(self, params: Dict[str, Any]) -> Optional[str]:
        """
        Validate input parameters.

        Override to add custom validation.

        Args:
            params: Parameters to validate

        Returns:
            Error message if invalid, None if valid
        """
        return None

    def __repr__(self):
        return f"<Tool({self.name})>"


class ToolRegistry:
    """
    Central registry for all tools in the system.

    Provides:
    - Tool registration and discovery
    - Parameter validation
    - Execution tracking
    - Error handling
    """

    def __init__(self):
        """Initialize the tool registry"""
        self._tools: Dict[str, Tool] = {}
        self._execution_count: Dict[str, int] = {}
        self._error_count: Dict[str, int] = {}
        self._last_execution: Dict[str, datetime] = {}

        logger.info("ToolRegistry initialized")

    def register(self, tool: Tool):
        """
        Register a tool with the registry.

        Args:
            tool: Tool instance to register
        """
        if tool.name in self._tools:
            logger.warning(f"Tool '{tool.name}' already registered, replacing")

        self._tools[tool.name] = tool
        self._execution_count[tool.name] = 0
        self._error_count[tool.name] = 0
        logger.info(f"Registered tool: {tool.name}")

    def register_class(self, tool_class: Type[Tool], **kwargs):
        """
        Register a tool by class, instantiating it.

        Args:
            tool_class: Tool class to instantiate
            **kwargs: Arguments to pass to constructor
        """
        tool = tool_class(**kwargs)
        self.register(tool)

    def unregister(self, tool_name: str):
        """
        Remove a tool from the registry.

        Args:
            tool_name: Name of tool to remove
        """
        if tool_name in self._tools:
            del self._tools[tool_name]
            logger.info(f"Unregistered tool: {tool_name}")

    def get(self, tool_name: str) -> Optional[Tool]:
        """
        Get a tool by name.

        Args:
            tool_name: Name of the tool

        Returns:
            Tool instance or None
        """
        return self._tools.get(tool_name)

    async def execute(self, tool_name: str, params: Dict[str, Any] = None) -> ToolResult:
        """
        Execute a tool by name.

        Args:
            tool_name: Name of the tool to execute
            params: Parameters to pass to the tool

        Returns:
            ToolResult with execution outcome
        """
        params = params or {}

        # Get tool
        tool = self._tools.get(tool_name)
        if tool is None:
            logger.error(f"Tool not found: {tool_name}")
            return ToolResult(
                success=False,
                error=f"Tool '{tool_name}' not found in registry"
            )

        # Validate parameters
        validation_error = await tool.validate_params(params)
        if validation_error:
            logger.warning(f"Tool {tool_name} parameter validation failed: {validation_error}")
            return ToolResult(
                success=False,
                error=f"Parameter validation failed: {validation_error}"
            )

        # Execute with timing
        start_time = time.time()

        try:
            result = await tool.execute(params)
            result.execution_time = time.time() - start_time

            self._execution_count[tool_name] += 1
            self._last_execution[tool_name] = datetime.now()

            if not result.success:
                self._error_count[tool_name] += 1

            logger.debug(
                f"Tool {tool_name} executed "
                f"[success={result.success}, time={result.execution_time:.3f}s]"
            )

            return result

        except Exception as e:
            execution_time = time.time() - start_time
            self._execution_count[tool_name] += 1
            self._error_count[tool_name] += 1
            self._last_execution[tool_name] = datetime.now()

            logger.error(f"Tool {tool_name} execution failed: {e}")

            return ToolResult(
                success=False,
                error=str(e),
                execution_time=execution_time,
                metadata={'exception_type': type(e).__name__}
            )

    def list_tools(self) -> List[Dict[str, Any]]:
        """
        List all registered tools.

        Returns:
            List of tool information dictionaries
        """
        return [
            {
                'name': tool.name,
                'description': tool.description,
                'parameters': tool.parameters,
                'execution_count': self._execution_count.get(tool.name, 0),
                'error_count': self._error_count.get(tool.name, 0),
                'last_execution': self._last_execution.get(tool.name)
            }
            for tool in self._tools.values()
        ]

    def get_stats(self) -> Dict[str, Any]:
        """Get registry statistics"""
        return {
            'tool_count': len(self._tools),
            'total_executions': sum(self._execution_count.values()),
            'total_errors': sum(self._error_count.values()),
            'tools': self.list_tools()
        }

    @property
    def tool_names(self) -> List[str]:
        """List of all registered tool names"""
        return list(self._tools.keys())

    def __contains__(self, tool_name: str) -> bool:
        return tool_name in self._tools

    def __len__(self) -> int:
        return len(self._tools)


# Global registry instance
_registry: Optional[ToolRegistry] = None


def get_tool_registry() -> ToolRegistry:
    """Get the global tool registry"""
    global _registry
    if _registry is None:
        _registry = ToolRegistry()
    return _registry


def register_default_tools(registry: ToolRegistry = None):
    """
    Register the default set of tools.

    Call this during system initialization.
    """
    if registry is None:
        registry = get_tool_registry()

    # Import and register tools
    from .database_tools import DatabaseQueryTool, DatabaseInsertTool
    from .data_tools import USDAFetchTool

    registry.register(DatabaseQueryTool())
    registry.register(DatabaseInsertTool())
    registry.register(USDAFetchTool())

    logger.info(f"Registered {len(registry)} default tools")
