"""
Database Tools - Tools for database operations.

Provides tools for querying, inserting, and managing database records.
These wrap the existing DatabaseAgent functionality.
"""

import logging
from typing import Any, Dict, List, Optional
import sys
from pathlib import Path

from .registry import Tool, ToolResult

logger = logging.getLogger(__name__)


class DatabaseQueryTool(Tool):
    """Tool for querying the commodity database"""

    @property
    def name(self) -> str:
        return "query_database"

    @property
    def description(self) -> str:
        return "Query commodity price data from the database with optional filters"

    @property
    def parameters(self) -> Dict[str, Any]:
        return {
            'source_report': {
                'type': 'string',
                'required': False,
                'description': 'Filter by source report name'
            },
            'commodity': {
                'type': 'string',
                'required': False,
                'description': 'Filter by commodity name'
            },
            'start_date': {
                'type': 'string',
                'required': False,
                'description': 'Start date (YYYY-MM-DD)'
            },
            'end_date': {
                'type': 'string',
                'required': False,
                'description': 'End date (YYYY-MM-DD)'
            },
            'limit': {
                'type': 'integer',
                'required': False,
                'default': 100,
                'description': 'Maximum records to return'
            }
        }

    def _get_database_agent(self):
        """Get or create a database agent instance"""
        # Try to import from the existing module
        try:
            # Add the commodity pipeline path
            pipeline_path = Path(__file__).parent.parent.parent / 'commodity_pipeline' / 'usda_ams_agent'
            if str(pipeline_path) not in sys.path:
                sys.path.insert(0, str(pipeline_path))

            from agents.database_agent import DatabaseAgent
            return DatabaseAgent()
        except ImportError as e:
            logger.warning(f"Could not import DatabaseAgent: {e}")
            return None

    async def execute(self, params: Dict[str, Any]) -> ToolResult:
        """Execute the database query"""
        try:
            db = self._get_database_agent()
            if db is None:
                return ToolResult(
                    success=False,
                    error="DatabaseAgent not available"
                )

            # Initialize schema if needed
            db.initialize_schema()

            # Execute query
            records = db.get_records(
                source_report=params.get('source_report'),
                commodity=params.get('commodity'),
                start_date=params.get('start_date'),
                end_date=params.get('end_date'),
                limit=params.get('limit', 100)
            )

            return ToolResult(
                success=True,
                data={
                    'records': records,
                    'count': len(records)
                },
                metadata={
                    'filters': {k: v for k, v in params.items() if v is not None}
                }
            )

        except Exception as e:
            logger.error(f"Database query failed: {e}")
            return ToolResult(
                success=False,
                error=str(e)
            )


class DatabaseInsertTool(Tool):
    """Tool for inserting records into the database"""

    @property
    def name(self) -> str:
        return "insert_records"

    @property
    def description(self) -> str:
        return "Insert commodity price records into the database"

    @property
    def parameters(self) -> Dict[str, Any]:
        return {
            'records': {
                'type': 'array',
                'required': True,
                'description': 'List of records to insert'
            }
        }

    async def validate_params(self, params: Dict[str, Any]) -> Optional[str]:
        """Validate that records is provided and is a list"""
        records = params.get('records')
        if records is None:
            return "Parameter 'records' is required"
        if not isinstance(records, list):
            return "Parameter 'records' must be a list"
        return None

    def _get_database_agent(self):
        """Get or create a database agent instance"""
        try:
            pipeline_path = Path(__file__).parent.parent.parent / 'commodity_pipeline' / 'usda_ams_agent'
            if str(pipeline_path) not in sys.path:
                sys.path.insert(0, str(pipeline_path))

            from agents.database_agent import DatabaseAgent
            return DatabaseAgent()
        except ImportError as e:
            logger.warning(f"Could not import DatabaseAgent: {e}")
            return None

    async def execute(self, params: Dict[str, Any]) -> ToolResult:
        """Execute the database insert"""
        try:
            db = self._get_database_agent()
            if db is None:
                return ToolResult(
                    success=False,
                    error="DatabaseAgent not available"
                )

            # Initialize schema if needed
            db.initialize_schema()

            records = params['records']

            # Insert records
            insert_result = db.insert_price_records(records)

            return ToolResult(
                success=insert_result.success_rate > 0.5,
                data={
                    'inserted': insert_result.inserted,
                    'skipped': insert_result.skipped,
                    'errors': insert_result.errors,
                    'total_attempted': insert_result.total_attempted,
                    'success_rate': insert_result.success_rate
                },
                metadata={
                    'error_messages': insert_result.error_messages[:5]
                }
            )

        except Exception as e:
            logger.error(f"Database insert failed: {e}")
            return ToolResult(
                success=False,
                error=str(e)
            )


class DatabaseStatsTool(Tool):
    """Tool for getting database statistics"""

    @property
    def name(self) -> str:
        return "database_stats"

    @property
    def description(self) -> str:
        return "Get statistics about the commodity database"

    @property
    def parameters(self) -> Dict[str, Any]:
        return {}

    def _get_database_agent(self):
        """Get or create a database agent instance"""
        try:
            pipeline_path = Path(__file__).parent.parent.parent / 'commodity_pipeline' / 'usda_ams_agent'
            if str(pipeline_path) not in sys.path:
                sys.path.insert(0, str(pipeline_path))

            from agents.database_agent import DatabaseAgent
            return DatabaseAgent()
        except ImportError as e:
            logger.warning(f"Could not import DatabaseAgent: {e}")
            return None

    async def execute(self, params: Dict[str, Any]) -> ToolResult:
        """Get database statistics"""
        try:
            db = self._get_database_agent()
            if db is None:
                return ToolResult(
                    success=False,
                    error="DatabaseAgent not available"
                )

            db.initialize_schema()
            stats = db.get_statistics()

            return ToolResult(
                success=True,
                data=stats
            )

        except Exception as e:
            logger.error(f"Failed to get database stats: {e}")
            return ToolResult(
                success=False,
                error=str(e)
            )
