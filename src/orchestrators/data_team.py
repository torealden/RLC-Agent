"""
Data Team Orchestrator - Coordinates data collection agents.

Manages all data collection operations across different sources:
- USDA AMS reports
- South America trade data
- Export inspections
- Future: Eurostat, other sources
"""

import asyncio
import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional

from ..agents.base_agent import CompositeAgent, AgentContext, AgentResult
from ..tools.registry import ToolRegistry

logger = logging.getLogger(__name__)


@dataclass
class CollectionResult:
    """Result from a data collection operation"""
    source: str
    success: bool
    record_count: int = 0
    inserted: int = 0
    skipped: int = 0
    errors: List[str] = field(default_factory=list)
    execution_time: float = 0.0
    timestamp: datetime = field(default_factory=datetime.now)

    def to_dict(self) -> Dict:
        return {
            'source': self.source,
            'success': self.success,
            'record_count': self.record_count,
            'inserted': self.inserted,
            'skipped': self.skipped,
            'errors': self.errors,
            'execution_time': round(self.execution_time, 2),
            'timestamp': self.timestamp.isoformat()
        }


@dataclass
class BatchCollectionResult:
    """Result from collecting multiple sources"""
    total_sources: int
    successful_sources: int
    failed_sources: int
    total_records: int
    results: Dict[str, CollectionResult] = field(default_factory=dict)
    execution_time: float = 0.0

    @property
    def success(self) -> bool:
        return self.failed_sources == 0

    def to_dict(self) -> Dict:
        return {
            'total_sources': self.total_sources,
            'successful_sources': self.successful_sources,
            'failed_sources': self.failed_sources,
            'total_records': self.total_records,
            'success': self.success,
            'execution_time': round(self.execution_time, 2),
            'results': {k: v.to_dict() for k, v in self.results.items()}
        }


class DataTeamOrchestrator(CompositeAgent):
    """
    Orchestrates all data collection agents.

    Coordinates:
    - USDA AMS data collection
    - South America trade data
    - Export inspections
    - Other data sources

    Supports both sequential and parallel collection.
    """

    def __init__(self, tools: ToolRegistry = None):
        """
        Initialize the Data Team Orchestrator.

        Args:
            tools: Tool registry for data operations
        """
        super().__init__()
        self._tools = tools

        # Source configurations
        self.sources = {
            'usda_ams': {
                'name': 'USDA AMS Reports',
                'tool': 'fetch_usda_data',
                'enabled': True
            },
            'usda_historical': {
                'name': 'USDA Historical',
                'tool': 'fetch_usda_historical',
                'enabled': True
            }
            # Future sources:
            # 'south_america': {...},
            # 'export_inspections': {...},
            # 'eurostat': {...}
        }

        logger.info("DataTeamOrchestrator initialized")

    @property
    def name(self) -> str:
        return "data_team"

    async def execute(self, context: AgentContext) -> AgentResult:
        """Execute a data team operation"""
        command = context.params.get('command', 'collect')

        if command == 'collect':
            source = context.params.get('source', 'usda_ams')
            result = await self.collect_source(source, context.params)
            return AgentResult(
                success=result.success,
                data=result.to_dict()
            )

        elif command == 'collect_all':
            result = await self.collect_all(context.params)
            return AgentResult(
                success=result.success,
                data=result.to_dict()
            )

        elif command == 'status':
            return AgentResult(
                success=True,
                data=self.get_status()
            )

        else:
            return AgentResult(
                success=False,
                error=f"Unknown command: {command}"
            )

    async def collect_source(self,
                             source_id: str,
                             params: Dict[str, Any] = None) -> CollectionResult:
        """
        Collect data from a single source.

        Args:
            source_id: Source identifier
            params: Collection parameters

        Returns:
            CollectionResult with operation details
        """
        params = params or {}
        start_time = datetime.now()

        logger.info(f"Collecting from source: {source_id}")

        # Get source configuration
        source_config = self.sources.get(source_id)
        if source_config is None:
            return CollectionResult(
                source=source_id,
                success=False,
                errors=[f"Unknown source: {source_id}"]
            )

        if not source_config.get('enabled', True):
            return CollectionResult(
                source=source_id,
                success=False,
                errors=["Source is disabled"]
            )

        tool_name = source_config.get('tool')

        try:
            # Execute collection
            result = await self._tools.execute(tool_name, params)

            if not result.success:
                return CollectionResult(
                    source=source_id,
                    success=False,
                    errors=[result.error or "Collection failed"],
                    execution_time=(datetime.now() - start_time).total_seconds()
                )

            record_count = result.data.get('count', 0) if result.data else 0

            return CollectionResult(
                source=source_id,
                success=True,
                record_count=record_count,
                inserted=result.data.get('inserted', 0) if result.data else 0,
                skipped=result.data.get('skipped', 0) if result.data else 0,
                execution_time=(datetime.now() - start_time).total_seconds()
            )

        except Exception as e:
            logger.error(f"Collection failed for {source_id}: {e}")
            return CollectionResult(
                source=source_id,
                success=False,
                errors=[str(e)],
                execution_time=(datetime.now() - start_time).total_seconds()
            )

    async def collect_all(self,
                          params: Dict[str, Any] = None,
                          parallel: bool = True) -> BatchCollectionResult:
        """
        Collect from all enabled sources.

        Args:
            params: Parameters to pass to each collector
            parallel: Whether to collect in parallel

        Returns:
            BatchCollectionResult with all results
        """
        params = params or {}
        start_time = datetime.now()

        enabled_sources = [
            source_id for source_id, config in self.sources.items()
            if config.get('enabled', True)
        ]

        logger.info(f"Collecting from {len(enabled_sources)} sources")

        results = {}

        if parallel:
            # Collect in parallel
            tasks = {
                source_id: self.collect_source(source_id, params)
                for source_id in enabled_sources
            }

            task_results = await asyncio.gather(
                *tasks.values(),
                return_exceptions=True
            )

            for source_id, result in zip(tasks.keys(), task_results):
                if isinstance(result, Exception):
                    results[source_id] = CollectionResult(
                        source=source_id,
                        success=False,
                        errors=[str(result)]
                    )
                else:
                    results[source_id] = result

        else:
            # Collect sequentially
            for source_id in enabled_sources:
                results[source_id] = await self.collect_source(source_id, params)

        # Aggregate results
        successful = sum(1 for r in results.values() if r.success)
        failed = len(results) - successful
        total_records = sum(r.record_count for r in results.values())

        return BatchCollectionResult(
            total_sources=len(enabled_sources),
            successful_sources=successful,
            failed_sources=failed,
            total_records=total_records,
            results=results,
            execution_time=(datetime.now() - start_time).total_seconds()
        )

    async def collect_scheduled(self,
                                 schedule: List[Dict[str, Any]]) -> BatchCollectionResult:
        """
        Collect data based on a schedule.

        Args:
            schedule: List of scheduled collection tasks

        Returns:
            BatchCollectionResult
        """
        start_time = datetime.now()
        results = {}

        for task in schedule:
            source_id = task.get('source_id', task.get('source', 'usda_ams'))
            params = task.get('params', {})

            result = await self.collect_source(source_id, params)
            results[source_id] = result

        successful = sum(1 for r in results.values() if r.success)
        failed = len(results) - successful
        total_records = sum(r.record_count for r in results.values())

        return BatchCollectionResult(
            total_sources=len(schedule),
            successful_sources=successful,
            failed_sources=failed,
            total_records=total_records,
            results=results,
            execution_time=(datetime.now() - start_time).total_seconds()
        )

    def get_status(self) -> Dict[str, Any]:
        """Get data team status"""
        return {
            'name': self.name,
            'status': self.status.value,
            'sources': {
                source_id: {
                    'name': config.get('name'),
                    'enabled': config.get('enabled', True),
                    'tool': config.get('tool')
                }
                for source_id, config in self.sources.items()
            },
            'stats': self.stats
        }

    def enable_source(self, source_id: str):
        """Enable a data source"""
        if source_id in self.sources:
            self.sources[source_id]['enabled'] = True
            logger.info(f"Enabled source: {source_id}")

    def disable_source(self, source_id: str):
        """Disable a data source"""
        if source_id in self.sources:
            self.sources[source_id]['enabled'] = False
            logger.info(f"Disabled source: {source_id}")

    def add_source(self, source_id: str, name: str, tool: str, enabled: bool = True):
        """
        Add a new data source.

        Args:
            source_id: Unique identifier
            name: Human-readable name
            tool: Tool to use for collection
            enabled: Whether source is enabled
        """
        self.sources[source_id] = {
            'name': name,
            'tool': tool,
            'enabled': enabled
        }
        logger.info(f"Added source: {source_id} ({name})")
