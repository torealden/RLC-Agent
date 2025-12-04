"""
Database Team Orchestrator - Coordinates database operations.

Manages:
- Data storage and retrieval
- Data validation and integrity checks
- Source vs database comparison
- Database health monitoring
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
class StorageResult:
    """Result from a storage operation"""
    success: bool
    records_attempted: int = 0
    records_inserted: int = 0
    records_skipped: int = 0
    records_failed: int = 0
    validation_passed: bool = True
    errors: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict:
        return {
            'success': self.success,
            'records_attempted': self.records_attempted,
            'records_inserted': self.records_inserted,
            'records_skipped': self.records_skipped,
            'records_failed': self.records_failed,
            'validation_passed': self.validation_passed,
            'errors': self.errors
        }


@dataclass
class ValidationResult:
    """Result from data validation"""
    valid: bool
    total_records: int = 0
    valid_records: int = 0
    invalid_records: int = 0
    issues: List[Dict[str, Any]] = field(default_factory=list)

    @property
    def validity_rate(self) -> float:
        if self.total_records == 0:
            return 1.0
        return self.valid_records / self.total_records

    def to_dict(self) -> Dict:
        return {
            'valid': self.valid,
            'total_records': self.total_records,
            'valid_records': self.valid_records,
            'invalid_records': self.invalid_records,
            'validity_rate': round(self.validity_rate, 4),
            'issues': self.issues[:10]  # Limit issues
        }


class DatabaseTeamOrchestrator(CompositeAgent):
    """
    Orchestrates database operations and data integrity.

    Coordinates:
    - Storage Agent: Data insertion and retrieval
    - Checker Agent: Data validation and integrity
    - Quality monitoring

    Ensures data quality before and after storage.
    """

    def __init__(self, tools: ToolRegistry = None):
        """
        Initialize the Database Team Orchestrator.

        Args:
            tools: Tool registry for database operations
        """
        super().__init__()
        self._tools = tools

        logger.info("DatabaseTeamOrchestrator initialized")

    @property
    def name(self) -> str:
        return "database_team"

    async def execute(self, context: AgentContext) -> AgentResult:
        """Execute a database team operation"""
        command = context.params.get('command', 'status')

        if command == 'store':
            records = context.params.get('records', [])
            result = await self.store_with_validation(records)
            return AgentResult(
                success=result.success,
                data=result.to_dict()
            )

        elif command == 'query':
            result = await self.query(context.params)
            return result

        elif command == 'validate':
            records = context.params.get('records', [])
            result = await self.validate_records(records)
            return AgentResult(
                success=result.valid,
                data=result.to_dict()
            )

        elif command == 'status':
            status = await self.get_database_status()
            return AgentResult(success=True, data=status)

        elif command == 'health_check':
            health = await self.check_database_health()
            return AgentResult(success=health.get('healthy', False), data=health)

        else:
            return AgentResult(
                success=False,
                error=f"Unknown command: {command}"
            )

    async def store_with_validation(self,
                                     records: List[Dict[str, Any]],
                                     validate_first: bool = True) -> StorageResult:
        """
        Store records with pre and post validation.

        Args:
            records: Records to store
            validate_first: Whether to validate before storing

        Returns:
            StorageResult with operation details
        """
        if not records:
            return StorageResult(
                success=True,
                records_attempted=0
            )

        logger.info(f"Storing {len(records)} records with validation")
        errors = []

        # Pre-validation
        if validate_first:
            validation = await self.validate_records(records)
            if not validation.valid:
                logger.warning(f"Pre-validation failed: {validation.invalid_records} invalid records")
                # Filter to only valid records
                if validation.issues:
                    invalid_indices = {issue.get('record_index') for issue in validation.issues}
                    records = [
                        r for i, r in enumerate(records)
                        if i not in invalid_indices
                    ]
                    if not records:
                        return StorageResult(
                            success=False,
                            records_attempted=validation.total_records,
                            validation_passed=False,
                            errors=["All records failed validation"]
                        )

        # Store records
        try:
            result = await self._tools.execute('insert_records', {'records': records})

            if not result.success:
                return StorageResult(
                    success=False,
                    records_attempted=len(records),
                    errors=[result.error or "Insert failed"]
                )

            data = result.data or {}

            return StorageResult(
                success=True,
                records_attempted=len(records),
                records_inserted=data.get('inserted', 0),
                records_skipped=data.get('skipped', 0),
                records_failed=data.get('errors', 0),
                validation_passed=True,
                errors=data.get('error_messages', [])
            )

        except Exception as e:
            logger.error(f"Storage failed: {e}")
            return StorageResult(
                success=False,
                records_attempted=len(records),
                errors=[str(e)]
            )

    async def validate_records(self,
                                records: List[Dict[str, Any]],
                                required_fields: List[str] = None) -> ValidationResult:
        """
        Validate a batch of records.

        Args:
            records: Records to validate
            required_fields: Fields that must be present

        Returns:
            ValidationResult
        """
        if not records:
            return ValidationResult(valid=True)

        required_fields = required_fields or ['commodity', 'report_date']

        try:
            result = await self._tools.execute('validate_data', {
                'records': records,
                'required_fields': required_fields,
                'check_prices': True
            })

            if not result.success:
                return ValidationResult(
                    valid=False,
                    total_records=len(records),
                    issues=[{'error': result.error}]
                )

            data = result.data or {}

            return ValidationResult(
                valid=data.get('validity_rate', 0) > 0.8,
                total_records=data.get('total_records', len(records)),
                valid_records=data.get('valid_records', 0),
                invalid_records=data.get('invalid_records', 0),
                issues=data.get('issues', [])
            )

        except Exception as e:
            logger.error(f"Validation failed: {e}")
            return ValidationResult(
                valid=False,
                total_records=len(records),
                issues=[{'error': str(e)}]
            )

    async def query(self, params: Dict[str, Any]) -> AgentResult:
        """
        Query the database.

        Args:
            params: Query parameters (source_report, commodity, dates, limit)

        Returns:
            AgentResult with query results
        """
        try:
            result = await self._tools.execute('query_database', params)
            return AgentResult(
                success=result.success,
                data=result.data,
                error=result.error
            )
        except Exception as e:
            logger.error(f"Query failed: {e}")
            return AgentResult(success=False, error=str(e))

    async def get_database_status(self) -> Dict[str, Any]:
        """Get comprehensive database status"""
        try:
            result = await self._tools.execute('database_stats', {})

            if result.success:
                return {
                    'healthy': True,
                    'stats': result.data,
                    'timestamp': datetime.now().isoformat()
                }
            else:
                return {
                    'healthy': False,
                    'error': result.error,
                    'timestamp': datetime.now().isoformat()
                }

        except Exception as e:
            return {
                'healthy': False,
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            }

    async def check_database_health(self) -> Dict[str, Any]:
        """
        Run database health checks.

        Returns:
            Health check results
        """
        checks = {
            'connection': False,
            'has_records': False,
            'recent_data': False
        }
        issues = []

        try:
            # Check connection and get stats
            status = await self.get_database_status()

            if status.get('healthy'):
                checks['connection'] = True

                stats = status.get('stats', {})

                # Check for records
                if stats.get('total_records', 0) > 0:
                    checks['has_records'] = True
                else:
                    issues.append("Database has no records")

                # Check for recent data
                date_range = stats.get('date_range', (None, None))
                if date_range[1]:
                    # Parse and check if within last 7 days
                    # (simplified check)
                    checks['recent_data'] = True
                else:
                    issues.append("No recent data in database")

            else:
                issues.append(status.get('error', 'Connection failed'))

        except Exception as e:
            issues.append(f"Health check error: {e}")

        overall_healthy = all(checks.values())

        return {
            'healthy': overall_healthy,
            'checks': checks,
            'issues': issues,
            'timestamp': datetime.now().isoformat()
        }

    async def compare_with_source(self,
                                   source: str,
                                   source_records: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Compare database records with source data.

        Args:
            source: Source identifier
            source_records: Records from source

        Returns:
            Comparison results
        """
        if not source_records:
            return {
                'match': True,
                'source_count': 0,
                'db_count': 0
            }

        # Get database records for same criteria
        # Extract date range from source records
        dates = [r.get('report_date') for r in source_records if r.get('report_date')]
        if dates:
            start_date = min(dates)
            end_date = max(dates)
        else:
            start_date = end_date = None

        try:
            db_result = await self.query({
                'source_report': source,
                'start_date': start_date,
                'end_date': end_date,
                'limit': len(source_records) * 2
            })

            if not db_result.success:
                return {
                    'match': False,
                    'error': db_result.error
                }

            db_records = db_result.data.get('records', [])

            return {
                'match': len(db_records) >= len(source_records) * 0.9,
                'source_count': len(source_records),
                'db_count': len(db_records),
                'coverage': len(db_records) / max(len(source_records), 1)
            }

        except Exception as e:
            return {
                'match': False,
                'error': str(e)
            }
