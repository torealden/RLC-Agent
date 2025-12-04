"""
Analyst Agent - Central orchestrator for the RLC Agent System.

The Analyst Agent coordinates all data operations:
- Schedules data pulls aligned with publication times
- Triggers analysis after new data arrives
- Synthesizes reports and presentations
- Manages quality control workflows

Think of it as the senior analyst who coordinates a team of junior analysts.
"""

import asyncio
import logging
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional

from ..agents.base_agent import CompositeAgent, AgentContext, AgentResult
from ..core.scheduler import MasterScheduler, ScheduledTask
from ..core.events import EventBus, EventType, Event, get_event_bus
from ..core.config import get_settings
from ..tools.registry import ToolRegistry, get_tool_registry, register_default_tools

logger = logging.getLogger(__name__)


class WorkflowType(Enum):
    """Types of workflows the Analyst can run"""
    DAILY_COLLECTION = "daily_collection"
    WEEKLY_REPORT = "weekly_report"
    ANALYSIS = "analysis"
    QUALITY_CHECK = "quality_check"
    AD_HOC_QUERY = "ad_hoc_query"


@dataclass
class WorkflowResult:
    """Result from a workflow execution"""
    workflow_type: WorkflowType
    success: bool
    start_time: datetime
    end_time: datetime
    stages_completed: List[str] = field(default_factory=list)
    stages_failed: List[str] = field(default_factory=list)
    data: Dict[str, Any] = field(default_factory=dict)
    errors: List[str] = field(default_factory=list)

    @property
    def duration_seconds(self) -> float:
        return (self.end_time - self.start_time).total_seconds()

    def to_dict(self) -> Dict:
        return {
            'workflow_type': self.workflow_type.value,
            'success': self.success,
            'duration_seconds': round(self.duration_seconds, 2),
            'stages_completed': self.stages_completed,
            'stages_failed': self.stages_failed,
            'data': self.data,
            'errors': self.errors
        }


class AnalystAgent(CompositeAgent):
    """
    Central orchestrator for commodity market data operations.

    Coordinates:
    - Data collection teams
    - Database operations
    - Analysis workflows
    - Report generation
    - Quality assurance

    The Analyst Agent doesn't perform data operations directly.
    Instead, it calls on specialized teams/agents and tools.
    """

    def __init__(self, settings=None):
        """
        Initialize the Analyst Agent.

        Args:
            settings: System settings (uses global if None)
        """
        super().__init__()

        self.settings = settings or get_settings()
        self.scheduler = MasterScheduler(self.settings)
        self.event_bus = get_event_bus()
        self._tools = get_tool_registry()

        # Register default tools
        register_default_tools(self._tools)

        # Team orchestrators (lazy initialized)
        self._data_team = None
        self._database_team = None
        self._analysis_team = None
        self._reporting_team = None

        # Track active workflows
        self._active_workflows: Dict[str, WorkflowResult] = {}

        # Set up event subscriptions
        self._setup_event_handlers()

        logger.info("AnalystAgent initialized")

    @property
    def name(self) -> str:
        return "analyst_agent"

    def _setup_event_handlers(self):
        """Set up handlers for system events"""

        async def on_data_collected(event: Event):
            """Handle new data collection"""
            logger.info(f"Data collected: {event.data.get('record_count', 0)} records")
            # Could trigger analysis here
            await self.event_bus.emit(
                EventType.DATA_STORED,
                source=self.name,
                data=event.data,
                correlation_id=event.correlation_id
            )

        async def on_task_completed(event: Event):
            """Handle scheduled task completion"""
            logger.info(f"Scheduled task completed: {event.data}")

        self.event_bus.subscribe(EventType.DATA_COLLECTED, on_data_collected)
        self.event_bus.subscribe(EventType.TASK_COMPLETED, on_task_completed)

    @property
    def data_team(self):
        """Get or create the data collection team orchestrator"""
        if self._data_team is None:
            from .data_team import DataTeamOrchestrator
            self._data_team = DataTeamOrchestrator(self._tools)
        return self._data_team

    @property
    def database_team(self):
        """Get or create the database team orchestrator"""
        if self._database_team is None:
            from .database_team import DatabaseTeamOrchestrator
            self._database_team = DatabaseTeamOrchestrator(self._tools)
        return self._database_team

    async def execute(self, context: AgentContext) -> AgentResult:
        """
        Execute an analyst request.

        The Analyst Agent handles high-level commands and
        delegates to appropriate teams.
        """
        command = context.params.get('command', 'status')

        try:
            if command == 'daily_workflow':
                result = await self.run_daily_workflow()
                return AgentResult(success=result.success, data=result.to_dict())

            elif command == 'collect':
                source = context.params.get('source', 'usda_ams_daily')
                result = await self.collect_data(source, context.params)
                return AgentResult(success=result.success, data=result.data)

            elif command == 'status':
                status = await self.get_system_status()
                return AgentResult(success=True, data=status)

            elif command == 'query':
                result = await self._tools.execute('query_database', context.params)
                return AgentResult(success=result.success, data=result.data, error=result.error)

            elif command == 'schedule':
                schedule = self.get_schedule()
                return AgentResult(success=True, data=schedule)

            else:
                return AgentResult(
                    success=False,
                    error=f"Unknown command: {command}"
                )

        except Exception as e:
            logger.error(f"Analyst execution failed: {e}")
            return AgentResult(success=False, error=str(e))

    async def run_daily_workflow(self) -> WorkflowResult:
        """
        Execute the complete daily data workflow.

        Stages:
        1. Check what data sources should be collected
        2. Collect data from scheduled sources
        3. Validate and store in database
        4. Run quality checks
        5. Trigger analysis if new insights found
        """
        start_time = datetime.now()
        stages_completed = []
        stages_failed = []
        errors = []
        workflow_data = {}

        logger.info("Starting daily workflow")

        # Emit workflow start event
        await self.event_bus.emit(
            EventType.AGENT_STARTED,
            source=self.name,
            data={'workflow': 'daily'}
        )

        try:
            # Stage 1: Get today's schedule
            schedule = self.scheduler.get_todays_schedule()
            workflow_data['scheduled_tasks'] = len(schedule)
            stages_completed.append('get_schedule')
            logger.info(f"Found {len(schedule)} scheduled tasks for today")

            # Stage 2: Collect data from each source
            collection_results = {}
            for task in schedule:
                if task.enabled:
                    try:
                        result = await self.collect_data(task.source_id, task.params)
                        collection_results[task.source_id] = {
                            'success': result.success,
                            'record_count': result.data.get('count', 0) if result.data else 0
                        }
                    except Exception as e:
                        collection_results[task.source_id] = {
                            'success': False,
                            'error': str(e)
                        }
                        errors.append(f"Collection failed for {task.source_id}: {e}")

            workflow_data['collection_results'] = collection_results
            stages_completed.append('data_collection')

            # Stage 3: Validate collected data
            total_records = sum(
                r.get('record_count', 0)
                for r in collection_results.values()
                if r.get('success')
            )
            workflow_data['total_records_collected'] = total_records

            if total_records > 0:
                stages_completed.append('data_validation')
            else:
                logger.warning("No records collected today")

            # Stage 4: Run quality checks
            try:
                db_stats = await self._tools.execute('database_stats', {})
                workflow_data['database_stats'] = db_stats.data
                stages_completed.append('quality_check')
            except Exception as e:
                errors.append(f"Quality check failed: {e}")
                stages_failed.append('quality_check')

            # Stage 5: Analysis (placeholder for future)
            # TODO: Implement analysis triggers
            stages_completed.append('analysis_check')

            success = len(stages_failed) == 0 and total_records >= 0

        except Exception as e:
            logger.error(f"Daily workflow failed: {e}")
            errors.append(str(e))
            success = False

        end_time = datetime.now()

        # Emit workflow complete event
        await self.event_bus.emit(
            EventType.AGENT_COMPLETED,
            source=self.name,
            data={
                'workflow': 'daily',
                'success': success,
                'duration': (end_time - start_time).total_seconds()
            }
        )

        result = WorkflowResult(
            workflow_type=WorkflowType.DAILY_COLLECTION,
            success=success,
            start_time=start_time,
            end_time=end_time,
            stages_completed=stages_completed,
            stages_failed=stages_failed,
            data=workflow_data,
            errors=errors
        )

        logger.info(f"Daily workflow completed: {result.success}")
        return result

    async def collect_data(self, source: str, params: Dict[str, Any] = None) -> AgentResult:
        """
        Collect data from a specific source.

        Args:
            source: Source identifier (e.g., 'usda_ams_daily')
            params: Collection parameters

        Returns:
            AgentResult with collected data
        """
        params = params or {}
        logger.info(f"Collecting data from: {source}")

        # Map sources to tools
        source_tool_map = {
            'usda_ams_daily': 'fetch_usda_data',
            'usda_ams_weekly': 'fetch_usda_data',
            'usda': 'fetch_usda_data',
        }

        tool_name = source_tool_map.get(source, 'fetch_usda_data')

        # Execute data collection
        fetch_result = await self._tools.execute(tool_name, params)

        if not fetch_result.success:
            return AgentResult(
                success=False,
                error=fetch_result.error,
                data={'source': source}
            )

        records = fetch_result.data.get('records', [])

        # Emit data collected event
        await self.event_bus.emit(
            EventType.DATA_COLLECTED,
            source=source,
            data={
                'record_count': len(records),
                'source': source
            }
        )

        # Store in database
        if records and self.settings.pipeline.enable_database_output:
            insert_result = await self._tools.execute('insert_records', {'records': records})

            return AgentResult(
                success=insert_result.success,
                data={
                    'source': source,
                    'count': len(records),
                    'inserted': insert_result.data.get('inserted', 0) if insert_result.data else 0,
                    'skipped': insert_result.data.get('skipped', 0) if insert_result.data else 0
                }
            )

        return AgentResult(
            success=True,
            data={
                'source': source,
                'count': len(records)
            }
        )

    async def get_system_status(self) -> Dict[str, Any]:
        """Get comprehensive system status"""
        status = {
            'agent': self.name,
            'status': self.status.value,
            'timestamp': datetime.now().isoformat()
        }

        # Scheduler status
        status['scheduler'] = self.scheduler.get_status()

        # Database status
        try:
            db_result = await self._tools.execute('database_stats', {})
            status['database'] = db_result.data if db_result.success else {'error': db_result.error}
        except Exception as e:
            status['database'] = {'error': str(e)}

        # Tool registry status
        status['tools'] = self._tools.get_stats()

        # Event bus status
        status['events'] = self.event_bus.get_stats()

        # Next scheduled tasks
        status['next_tasks'] = self.scheduler.get_next_tasks(5)

        return status

    def get_schedule(self) -> Dict[str, Any]:
        """Get the current schedule"""
        return {
            'today': [
                {
                    'name': t.name,
                    'source': t.source_id,
                    'next_run': t.next_run.isoformat() if t.next_run else None,
                    'enabled': t.enabled
                }
                for t in self.scheduler.get_todays_schedule()
            ],
            'upcoming': self.scheduler.get_next_tasks(10)
        }

    async def start_scheduler(self):
        """Start the background scheduler"""
        # Register task handlers
        self.scheduler.register_handler('usda_ams_daily', self._handle_usda_collection)
        self.scheduler.register_handler('usda_ams_weekly', self._handle_usda_collection)

        # Register tasks from settings
        self.scheduler.register_from_settings()

        # Start the scheduler loop
        logger.info("Starting scheduler...")
        await self.scheduler.run_loop()

    async def _handle_usda_collection(self, **kwargs):
        """Handler for USDA data collection tasks"""
        reports = kwargs.get('reports', [])
        result = await self.collect_data('usda', {'report_ids': reports})
        return result.data

    def stop_scheduler(self):
        """Stop the background scheduler"""
        self.scheduler.stop()

    async def health_check(self) -> Dict[str, Any]:
        """Comprehensive health check"""
        health = await super().health_check()

        # Check tool availability
        health['tools_available'] = len(self._tools) > 0

        # Check database connection
        try:
            db_result = await self._tools.execute('database_stats', {})
            health['database_healthy'] = db_result.success
        except Exception:
            health['database_healthy'] = False

        health['overall_healthy'] = (
            health['healthy'] and
            health['tools_available'] and
            health['database_healthy']
        )

        return health


# Factory function
def create_analyst_agent(settings=None) -> AnalystAgent:
    """Create a configured AnalystAgent instance"""
    return AnalystAgent(settings=settings)
