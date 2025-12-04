"""
Base Agent - Abstract base class for all agents in the RLC system.

All agents inherit from BaseAgent and implement specific capabilities.
Provides common functionality for logging, status tracking, and tool access.
"""

import asyncio
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional, Callable
import uuid


class AgentStatus(Enum):
    """Status states for an agent"""
    IDLE = "idle"
    RUNNING = "running"
    PAUSED = "paused"
    ERROR = "error"
    COMPLETED = "completed"


@dataclass
class AgentResult:
    """Result of an agent operation"""
    success: bool
    data: Any = None
    error: Optional[str] = None
    execution_time: float = 0.0
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict:
        return {
            'success': self.success,
            'data': self.data,
            'error': self.error,
            'execution_time': round(self.execution_time, 3),
            'metadata': self.metadata
        }


@dataclass
class AgentContext:
    """Context passed to agent during execution"""
    request_id: str
    timestamp: datetime
    params: Dict[str, Any]
    parent_agent: Optional[str] = None
    tools: Optional[Any] = None  # ToolRegistry reference

    @classmethod
    def create(cls, params: Dict[str, Any] = None, parent: str = None, tools: Any = None):
        return cls(
            request_id=str(uuid.uuid4())[:8],
            timestamp=datetime.now(),
            params=params or {},
            parent_agent=parent,
            tools=tools
        )


class BaseAgent(ABC):
    """
    Abstract base class for all agents in the RLC system.

    Agents are autonomous units that:
    - Have a specific role/responsibility
    - Can execute tasks independently
    - Report results back to orchestrators
    - Can use tools from the tool registry

    Subclasses must implement:
    - name: property returning agent's name
    - execute(): the main agent logic
    """

    def __init__(self, config: Dict[str, Any] = None):
        """
        Initialize the agent.

        Args:
            config: Agent-specific configuration
        """
        self.config = config or {}
        self._status = AgentStatus.IDLE
        self._last_run: Optional[datetime] = None
        self._run_count = 0
        self._error_count = 0
        self._tools = None

        # Set up logging for this agent
        self.logger = logging.getLogger(f"agent.{self.name}")

    @property
    @abstractmethod
    def name(self) -> str:
        """Return the agent's unique name"""
        pass

    @property
    def status(self) -> AgentStatus:
        """Current status of the agent"""
        return self._status

    @property
    def stats(self) -> Dict[str, Any]:
        """Return agent statistics"""
        return {
            'name': self.name,
            'status': self._status.value,
            'last_run': self._last_run.isoformat() if self._last_run else None,
            'run_count': self._run_count,
            'error_count': self._error_count
        }

    def set_tools(self, tools: Any):
        """Set the tool registry for this agent"""
        self._tools = tools

    async def use_tool(self, tool_name: str, params: Dict[str, Any] = None) -> Any:
        """
        Use a tool from the registry.

        Args:
            tool_name: Name of the tool to use
            params: Parameters to pass to the tool

        Returns:
            Result from the tool execution
        """
        if self._tools is None:
            raise RuntimeError(f"Agent {self.name} has no tool registry configured")

        return await self._tools.execute(tool_name, params or {})

    @abstractmethod
    async def execute(self, context: AgentContext) -> AgentResult:
        """
        Execute the agent's main task.

        This is the core method that subclasses must implement.

        Args:
            context: Execution context with params and metadata

        Returns:
            AgentResult with success status and data
        """
        pass

    async def run(self, params: Dict[str, Any] = None,
                  parent: str = None,
                  tools: Any = None) -> AgentResult:
        """
        Run the agent with given parameters.

        This is the main entry point for executing an agent.
        Handles status tracking, timing, and error handling.

        Args:
            params: Parameters for this execution
            parent: Name of parent agent/orchestrator
            tools: Tool registry (optional, uses self._tools if not provided)

        Returns:
            AgentResult with execution results
        """
        start_time = datetime.now()
        self._status = AgentStatus.RUNNING
        self._run_count += 1

        # Create context
        context = AgentContext.create(
            params=params,
            parent=parent,
            tools=tools or self._tools
        )

        self.logger.info(f"Starting execution [request_id={context.request_id}]")

        try:
            # Call the subclass implementation
            result = await self.execute(context)

            # Calculate execution time
            result.execution_time = (datetime.now() - start_time).total_seconds()
            result.metadata['request_id'] = context.request_id
            result.metadata['agent'] = self.name

            if result.success:
                self._status = AgentStatus.COMPLETED
                self.logger.info(
                    f"Execution completed successfully "
                    f"[time={result.execution_time:.2f}s]"
                )
            else:
                self._status = AgentStatus.ERROR
                self._error_count += 1
                self.logger.warning(
                    f"Execution completed with failure: {result.error}"
                )

            return result

        except Exception as e:
            self._status = AgentStatus.ERROR
            self._error_count += 1
            execution_time = (datetime.now() - start_time).total_seconds()

            self.logger.error(f"Execution failed with exception: {e}")

            return AgentResult(
                success=False,
                error=str(e),
                execution_time=execution_time,
                metadata={
                    'request_id': context.request_id,
                    'agent': self.name,
                    'exception_type': type(e).__name__
                }
            )
        finally:
            self._last_run = datetime.now()

    async def health_check(self) -> Dict[str, Any]:
        """
        Perform a health check on this agent.

        Override in subclasses for agent-specific health checks.

        Returns:
            Dictionary with health status
        """
        return {
            'agent': self.name,
            'status': self._status.value,
            'healthy': self._status not in (AgentStatus.ERROR,),
            'last_run': self._last_run.isoformat() if self._last_run else None,
            'error_rate': self._error_count / max(self._run_count, 1)
        }

    def reset(self):
        """Reset agent to initial state"""
        self._status = AgentStatus.IDLE
        self.logger.info("Agent reset to IDLE state")

    def __repr__(self):
        return f"<{self.__class__.__name__}(name='{self.name}', status={self._status.value})>"


class CompositeAgent(BaseAgent):
    """
    An agent that coordinates multiple sub-agents.

    Use this as a base for orchestrator-type agents that
    delegate work to specialized sub-agents.
    """

    def __init__(self, config: Dict[str, Any] = None):
        super().__init__(config)
        self._sub_agents: Dict[str, BaseAgent] = {}

    def register_agent(self, agent: BaseAgent):
        """Register a sub-agent"""
        self._sub_agents[agent.name] = agent
        agent.set_tools(self._tools)
        self.logger.info(f"Registered sub-agent: {agent.name}")

    def get_agent(self, name: str) -> Optional[BaseAgent]:
        """Get a registered sub-agent by name"""
        return self._sub_agents.get(name)

    @property
    def sub_agents(self) -> List[str]:
        """List of registered sub-agent names"""
        return list(self._sub_agents.keys())

    async def run_sub_agent(self, name: str, params: Dict[str, Any] = None) -> AgentResult:
        """
        Run a registered sub-agent.

        Args:
            name: Name of the sub-agent
            params: Parameters to pass

        Returns:
            Result from the sub-agent
        """
        agent = self._sub_agents.get(name)
        if agent is None:
            return AgentResult(
                success=False,
                error=f"Sub-agent '{name}' not found"
            )

        return await agent.run(params=params, parent=self.name, tools=self._tools)

    async def run_parallel(self,
                          agent_params: Dict[str, Dict[str, Any]]) -> Dict[str, AgentResult]:
        """
        Run multiple sub-agents in parallel.

        Args:
            agent_params: Dict mapping agent names to their parameters

        Returns:
            Dict mapping agent names to their results
        """
        tasks = {}
        for agent_name, params in agent_params.items():
            if agent_name in self._sub_agents:
                tasks[agent_name] = self.run_sub_agent(agent_name, params)

        results = await asyncio.gather(*tasks.values(), return_exceptions=True)

        return {
            name: result if not isinstance(result, Exception)
                  else AgentResult(success=False, error=str(result))
            for name, result in zip(tasks.keys(), results)
        }

    async def health_check(self) -> Dict[str, Any]:
        """Health check including all sub-agents"""
        base_health = await super().health_check()

        sub_health = {}
        for name, agent in self._sub_agents.items():
            sub_health[name] = await agent.health_check()

        base_health['sub_agents'] = sub_health
        base_health['sub_agent_count'] = len(self._sub_agents)

        return base_health
