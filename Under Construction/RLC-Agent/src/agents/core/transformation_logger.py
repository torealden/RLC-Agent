"""
Transformation Logger Agent for RLC Database

Provides logging for agent interactions with data across the Bronze/Silver/Gold
medallion architecture. Implements a "checkout/checkin" pattern for data sessions.

Usage:
    from src.agents.core.transformation_logger import TransformationLogger

    # As context manager (recommended)
    with TransformationLogger(
        agent_id='wasde_transformer',
        session_type='BRONZE_TO_SILVER',
        source_tables=['bronze.wasde_cell'],
        purpose='Transform WASDE cells to observations'
    ) as logger:
        # Perform transformation
        logger.log_operation(
            operation_type='AGGREGATE',
            input_tables=['bronze.wasde_cell'],
            transformation_logic='SELECT commodity, SUM(value) GROUP BY commodity',
            output_table='silver.observation',
            input_row_count=50000,
            output_row_count=150
        )

        # Register output artifact
        logger.register_output(
            artifact_type='TABLE',
            artifact_name='silver.observation',
            row_count=150
        )

    # Or manual lifecycle
    logger = TransformationLogger(...)
    session_id = logger.checkout()
    try:
        logger.log_operation(...)
        logger.checkin()
    except Exception as e:
        logger.checkin(status='FAILED', error_message=str(e))
"""

import logging
import time
from dataclasses import dataclass, field
from datetime import date, datetime
from enum import Enum
from typing import Any, Dict, List, Optional
from uuid import UUID

# Database configuration
import sys
from pathlib import Path

# Add project root to path if needed
PROJECT_ROOT = Path(__file__).parent.parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.services.database.db_config import get_connection, DB_TYPE

logger = logging.getLogger(__name__)


class SessionType(str, Enum):
    """Valid transformation session types."""
    BRONZE_TO_SILVER = 'BRONZE_TO_SILVER'
    SILVER_AGGREGATE = 'SILVER_AGGREGATE'
    SILVER_TO_GOLD = 'SILVER_TO_GOLD'
    GOLD_VISUALIZATION = 'GOLD_VISUALIZATION'
    CROSS_LAYER_ANALYSIS = 'CROSS_LAYER_ANALYSIS'
    AD_HOC_QUERY = 'AD_HOC_QUERY'
    DATA_EXPORT = 'DATA_EXPORT'
    DATA_CORRECTION = 'DATA_CORRECTION'


class SessionStatus(str, Enum):
    """Valid session status values."""
    ACTIVE = 'ACTIVE'
    COMPLETED = 'COMPLETED'
    FAILED = 'FAILED'
    ABANDONED = 'ABANDONED'
    ROLLED_BACK = 'ROLLED_BACK'


class SourceLayer(str, Enum):
    """Valid source data layers."""
    BRONZE = 'BRONZE'
    SILVER = 'SILVER'
    GOLD = 'GOLD'


class OperationType(str, Enum):
    """Valid transformation operation types."""
    SELECT = 'SELECT'
    INSERT = 'INSERT'
    UPDATE = 'UPDATE'
    DELETE = 'DELETE'
    UPSERT = 'UPSERT'
    AGGREGATE = 'AGGREGATE'
    JOIN = 'JOIN'
    FILTER = 'FILTER'
    CALCULATE = 'CALCULATE'
    PIVOT = 'PIVOT'
    UNPIVOT = 'UNPIVOT'
    NORMALIZE = 'NORMALIZE'
    CLEAN = 'CLEAN'
    DEDUPLICATE = 'DEDUPLICATE'
    VALIDATE = 'VALIDATE'
    TRANSFORM = 'TRANSFORM'
    EXPORT = 'EXPORT'
    REFRESH = 'REFRESH'


class ArtifactType(str, Enum):
    """Valid output artifact types."""
    TABLE = 'TABLE'
    VIEW = 'VIEW'
    MATERIALIZED_VIEW = 'MATERIALIZED_VIEW'
    TEMP_TABLE = 'TEMP_TABLE'
    DATAFRAME = 'DATAFRAME'
    CSV_FILE = 'CSV_FILE'
    EXCEL_FILE = 'EXCEL_FILE'
    JSON_FILE = 'JSON_FILE'
    PARQUET_FILE = 'PARQUET_FILE'
    CHART = 'CHART'
    DASHBOARD = 'DASHBOARD'
    REPORT = 'REPORT'
    API_RESPONSE = 'API_RESPONSE'
    CACHED_QUERY = 'CACHED_QUERY'


class RelationshipType(str, Enum):
    """Valid lineage relationship types."""
    DERIVES_FROM = 'DERIVES_FROM'
    COPIES = 'COPIES'
    AGGREGATES = 'AGGREGATES'
    JOINS = 'JOINS'
    FILTERS = 'FILTERS'
    TRANSFORMS = 'TRANSFORMS'
    ENRICHES = 'ENRICHES'
    VALIDATES = 'VALIDATES'
    REFERENCES = 'REFERENCES'


@dataclass
class OperationResult:
    """Result of logging an operation."""
    operation_id: int
    operation_order: int
    execution_time_ms: Optional[int] = None


@dataclass
class ArtifactResult:
    """Result of registering an artifact."""
    artifact_id: UUID
    artifact_name: str


@dataclass
class LineageEdgeResult:
    """Result of adding a lineage edge."""
    edge_id: int
    is_new: bool = True


class TransformationLogger:
    """
    Logging agent for tracking data transformations across the medallion architecture.

    Implements a checkout/checkin pattern:
    1. checkout() - Start a session, register source tables
    2. log_operation() - Record transformation steps
    3. register_output() - Record output artifacts
    4. checkin() - Complete the session

    Can be used as a context manager for automatic checkin on success/failure.

    Attributes:
        session_id: UUID of the current session (set after checkout)
        agent_id: Identifier of the agent performing transformations
        agent_type: Type of agent (COLLECTOR, TRANSFORMER, ANALYST, VISUALIZATION)
        session_type: Type of transformation session
        source_layer: Which data layer is being accessed
        source_tables: List of tables being accessed
    """

    def __init__(
        self,
        agent_id: str,
        session_type: SessionType | str = SessionType.BRONZE_TO_SILVER,
        source_layer: SourceLayer | str = SourceLayer.BRONZE,
        source_tables: Optional[List[str]] = None,
        purpose: Optional[str] = None,
        agent_type: Optional[str] = None,
        agent_version: Optional[str] = None,
        source_filters: Optional[Dict[str, Any]] = None,
        data_start_date: Optional[date] = None,
        data_end_date: Optional[date] = None,
        parent_session_id: Optional[UUID] = None,
        ticket_id: Optional[str] = None,
        auto_checkout: bool = True
    ):
        """
        Initialize the transformation logger.

        Args:
            agent_id: Unique identifier for the agent
            session_type: Type of transformation session
            source_layer: Which data layer is being accessed (BRONZE, SILVER, GOLD)
            source_tables: List of tables being accessed
            purpose: Human-readable description of the transformation
            agent_type: Type of agent (COLLECTOR, TRANSFORMER, ANALYST, VISUALIZATION)
            agent_version: Version of the agent
            source_filters: Any WHERE clauses/filters applied to source data
            data_start_date: Start date of source data range
            data_end_date: End date of source data range
            parent_session_id: ID of parent session for chained transformations
            ticket_id: Optional link to Jira/issue tracker
            auto_checkout: Whether to automatically checkout when used as context manager
        """
        self.agent_id = agent_id
        self.session_type = session_type if isinstance(session_type, str) else session_type.value
        self.source_layer = source_layer if isinstance(source_layer, str) else source_layer.value
        self.source_tables = source_tables or []
        self.purpose = purpose
        self.agent_type = agent_type
        self.agent_version = agent_version
        self.source_filters = source_filters
        self.data_start_date = data_start_date
        self.data_end_date = data_end_date
        self.parent_session_id = parent_session_id
        self.ticket_id = ticket_id
        self.auto_checkout = auto_checkout

        self.session_id: Optional[UUID] = None
        self._operation_count = 0
        self._output_count = 0
        self._is_active = False

    def __enter__(self) -> 'TransformationLogger':
        """Context manager entry - checkout if auto_checkout is True."""
        if self.auto_checkout:
            self.checkout()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - checkin with appropriate status."""
        if self._is_active:
            if exc_type is not None:
                # Exception occurred - mark as failed
                self.checkin(
                    status=SessionStatus.FAILED,
                    error_message=str(exc_val) if exc_val else f"{exc_type.__name__}"
                )
            else:
                # Success
                self.checkin(status=SessionStatus.COMPLETED)
        return False  # Don't suppress exceptions

    def checkout(self) -> UUID:
        """
        Start a transformation session (checkout data).

        Returns:
            UUID of the created session

        Raises:
            RuntimeError: If session is already active
            DatabaseError: If database operation fails
        """
        if self._is_active:
            raise RuntimeError(f"Session already active: {self.session_id}")

        if not self.source_tables:
            logger.warning("No source_tables specified for transformation session")

        try:
            with get_connection() as conn:
                cursor = conn.cursor()

                if DB_TYPE == "postgresql":
                    # Use the stored function
                    cursor.execute("""
                        SELECT audit.start_transformation_session(
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                        ) AS session_id
                    """, (
                        self.session_type,
                        self.agent_id,
                        self.source_layer,
                        self.source_tables,
                        self.purpose,
                        self.agent_type,
                        self.agent_version,
                        self.source_filters if self.source_filters else None,
                        self.data_start_date,
                        self.data_end_date,
                        str(self.parent_session_id) if self.parent_session_id else None,
                        self.ticket_id
                    ))
                    result = cursor.fetchone()
                    self.session_id = result['session_id'] if isinstance(result, dict) else result[0]
                else:
                    # SQLite fallback - simplified logging
                    import uuid
                    self.session_id = uuid.uuid4()
                    logger.info(
                        f"Transformation session started (SQLite mode): {self.session_id} "
                        f"- {self.session_type} by {self.agent_id}"
                    )

                conn.commit()

            self._is_active = True
            logger.info(
                f"Transformation session checkout: {self.session_id} "
                f"({self.session_type}, {self.source_layer})"
            )
            return self.session_id

        except Exception as e:
            logger.error(f"Failed to start transformation session: {e}")
            raise

    def log_operation(
        self,
        operation_type: OperationType | str,
        input_tables: Optional[List[str]] = None,
        transformation_logic: Optional[str] = None,
        output_table: Optional[str] = None,
        input_row_count: Optional[int] = None,
        output_row_count: Optional[int] = None,
        transformation_type: str = 'SQL',
        parameters: Optional[Dict[str, Any]] = None,
        input_columns: Optional[List[str]] = None,
        output_columns: Optional[List[str]] = None,
        warnings: Optional[List[str]] = None,
        execution_time_ms: Optional[int] = None
    ) -> OperationResult:
        """
        Log an operation within the current session.

        Args:
            operation_type: Type of operation (AGGREGATE, JOIN, FILTER, etc.)
            input_tables: Tables/views used as input
            transformation_logic: SQL, formula, or description of operation
            output_table: Target table/view name
            input_row_count: Approximate row count processed
            output_row_count: Rows produced by operation
            transformation_type: 'SQL', 'PYTHON', 'PANDAS', 'FORMULA', 'MANUAL'
            parameters: Configuration parameters (not data values)
            input_columns: Columns accessed from input
            output_columns: Columns produced in output
            warnings: Any issues encountered
            execution_time_ms: Operation duration in milliseconds

        Returns:
            OperationResult with operation ID and order

        Raises:
            RuntimeError: If no active session
        """
        if not self._is_active:
            raise RuntimeError("No active session. Call checkout() first.")

        op_type = operation_type if isinstance(operation_type, str) else operation_type.value

        try:
            with get_connection() as conn:
                cursor = conn.cursor()

                if DB_TYPE == "postgresql":
                    import json
                    cursor.execute("""
                        SELECT audit.log_transformation_operation(
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                        ) AS operation_id
                    """, (
                        str(self.session_id),
                        op_type,
                        input_tables,
                        transformation_logic,
                        output_table,
                        input_row_count,
                        output_row_count,
                        transformation_type,
                        json.dumps(parameters) if parameters else None,
                        input_columns,
                        output_columns,
                        warnings,
                        execution_time_ms
                    ))
                    result = cursor.fetchone()
                    operation_id = result['operation_id'] if isinstance(result, dict) else result[0]
                else:
                    # SQLite fallback
                    operation_id = self._operation_count + 1
                    logger.info(
                        f"Operation logged (SQLite mode): {op_type} "
                        f"- {input_tables} -> {output_table}"
                    )

                conn.commit()

            self._operation_count += 1
            logger.debug(
                f"Logged operation {self._operation_count}: {op_type} "
                f"({input_row_count or '?'} -> {output_row_count or '?'} rows)"
            )

            return OperationResult(
                operation_id=operation_id,
                operation_order=self._operation_count,
                execution_time_ms=execution_time_ms
            )

        except Exception as e:
            logger.error(f"Failed to log operation: {e}")
            raise

    def register_output(
        self,
        artifact_type: ArtifactType | str,
        artifact_name: str,
        artifact_location: Optional[str] = None,
        source_tables: Optional[List[str]] = None,
        row_count: Optional[int] = None,
        column_count: Optional[int] = None,
        data_as_of: Optional[datetime] = None,
        data_start_date: Optional[date] = None,
        data_end_date: Optional[date] = None,
        description: Optional[str] = None,
        expires_at: Optional[datetime] = None,
        metadata: Optional[Dict[str, Any]] = None
    ) -> ArtifactResult:
        """
        Register an output artifact created during the session.

        Args:
            artifact_type: Type of artifact (TABLE, VIEW, CHART, etc.)
            artifact_name: Name of the artifact
            artifact_location: Schema.table, file path, or URL
            source_tables: Which tables feed this artifact
            row_count: Number of rows in artifact
            column_count: Number of columns in artifact
            data_as_of: Point-in-time snapshot timestamp
            data_start_date: Earliest data in artifact
            data_end_date: Latest data in artifact
            description: Human-readable description
            expires_at: Optional TTL for temporary artifacts
            metadata: Additional attributes

        Returns:
            ArtifactResult with artifact ID and name

        Raises:
            RuntimeError: If no active session
        """
        if not self._is_active:
            raise RuntimeError("No active session. Call checkout() first.")

        art_type = artifact_type if isinstance(artifact_type, str) else artifact_type.value

        try:
            with get_connection() as conn:
                cursor = conn.cursor()

                if DB_TYPE == "postgresql":
                    import json
                    cursor.execute("""
                        SELECT audit.register_output_artifact(
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                        ) AS artifact_id
                    """, (
                        str(self.session_id),
                        art_type,
                        artifact_name,
                        artifact_location,
                        source_tables,
                        row_count,
                        column_count,
                        data_as_of,
                        data_start_date,
                        data_end_date,
                        description,
                        expires_at,
                        json.dumps(metadata) if metadata else None
                    ))
                    result = cursor.fetchone()
                    artifact_id = result['artifact_id'] if isinstance(result, dict) else result[0]
                else:
                    # SQLite fallback
                    import uuid
                    artifact_id = uuid.uuid4()
                    logger.info(
                        f"Artifact registered (SQLite mode): {art_type} - {artifact_name}"
                    )

                conn.commit()

            self._output_count += 1
            logger.debug(f"Registered artifact: {artifact_name} ({art_type})")

            return ArtifactResult(
                artifact_id=artifact_id,
                artifact_name=artifact_name
            )

        except Exception as e:
            logger.error(f"Failed to register artifact: {e}")
            raise

    def add_lineage(
        self,
        source_schema: str,
        source_name: str,
        target_schema: str,
        target_name: str,
        relationship_type: RelationshipType | str,
        source_type: str = 'TABLE',
        target_type: str = 'TABLE',
        source_column: Optional[str] = None,
        target_column: Optional[str] = None,
        transformation_description: Optional[str] = None
    ) -> LineageEdgeResult:
        """
        Add a lineage edge between two data entities.

        Args:
            source_schema: Schema of source entity (bronze, silver, gold)
            source_name: Name of source table/view
            target_schema: Schema of target entity
            target_name: Name of target table/view
            relationship_type: Type of relationship (DERIVES_FROM, AGGREGATES, etc.)
            source_type: Type of source (TABLE, VIEW, COLUMN, FILE, API)
            target_type: Type of target
            source_column: Optional column for column-level lineage
            target_column: Optional column for column-level lineage
            transformation_description: Human-readable description

        Returns:
            LineageEdgeResult with edge ID
        """
        rel_type = relationship_type if isinstance(relationship_type, str) else relationship_type.value

        try:
            with get_connection() as conn:
                cursor = conn.cursor()

                if DB_TYPE == "postgresql":
                    cursor.execute("""
                        SELECT audit.add_lineage_edge(
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                        ) AS edge_id
                    """, (
                        source_type,
                        source_schema,
                        source_name,
                        target_type,
                        target_schema,
                        target_name,
                        rel_type,
                        str(self.session_id) if self.session_id else None,
                        source_column,
                        target_column,
                        transformation_description
                    ))
                    result = cursor.fetchone()
                    edge_id = result['edge_id'] if isinstance(result, dict) else result[0]
                else:
                    # SQLite fallback
                    edge_id = hash(f"{source_schema}.{source_name}->{target_schema}.{target_name}")
                    logger.info(
                        f"Lineage edge added (SQLite mode): "
                        f"{source_schema}.{source_name} -> {target_schema}.{target_name}"
                    )

                conn.commit()

            logger.debug(
                f"Added lineage: {source_schema}.{source_name} "
                f"--[{rel_type}]--> {target_schema}.{target_name}"
            )

            return LineageEdgeResult(edge_id=edge_id)

        except Exception as e:
            logger.error(f"Failed to add lineage edge: {e}")
            raise

    def checkin(
        self,
        status: SessionStatus | str = SessionStatus.COMPLETED,
        error_message: Optional[str] = None,
        error_details: Optional[Dict[str, Any]] = None
    ) -> None:
        """
        Complete the transformation session (checkin).

        Args:
            status: Final status of the session
            error_message: Error message if status is FAILED
            error_details: Additional error details as JSONB

        Raises:
            RuntimeError: If no active session
        """
        if not self._is_active:
            logger.warning("checkin() called but no active session")
            return

        stat = status if isinstance(status, str) else status.value

        try:
            with get_connection() as conn:
                cursor = conn.cursor()

                if DB_TYPE == "postgresql":
                    import json
                    cursor.execute("""
                        SELECT audit.complete_transformation_session(
                            %s, %s, %s, %s
                        )
                    """, (
                        str(self.session_id),
                        stat,
                        error_message,
                        json.dumps(error_details) if error_details else None
                    ))
                else:
                    logger.info(
                        f"Transformation session completed (SQLite mode): "
                        f"{self.session_id} - {stat}"
                    )

                conn.commit()

            self._is_active = False
            logger.info(
                f"Transformation session checkin: {self.session_id} "
                f"({stat}, {self._operation_count} ops, {self._output_count} outputs)"
            )

        except Exception as e:
            logger.error(f"Failed to complete transformation session: {e}")
            raise

    def timed_operation(self, operation_type: OperationType | str, **kwargs):
        """
        Context manager for timing an operation.

        Usage:
            with logger.timed_operation('AGGREGATE', input_tables=['bronze.wasde_cell']) as op:
                # Do the work
                result = process_data()
                op.set_output(output_table='silver.observation', output_row_count=len(result))
        """
        return _TimedOperation(self, operation_type, **kwargs)


class _TimedOperation:
    """Helper context manager for timed operations."""

    def __init__(self, logger: TransformationLogger, operation_type: str, **kwargs):
        self._logger = logger
        self._operation_type = operation_type
        self._kwargs = kwargs
        self._start_time = None
        self._output_table = None
        self._output_row_count = None
        self._output_columns = None

    def __enter__(self) -> '_TimedOperation':
        self._start_time = time.time()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        execution_time_ms = int((time.time() - self._start_time) * 1000)

        if exc_type is not None:
            # Operation failed
            self._logger.log_operation(
                operation_type=self._operation_type,
                execution_time_ms=execution_time_ms,
                warnings=[f"Operation failed: {exc_val}"],
                **self._kwargs
            )
        else:
            # Operation succeeded
            self._logger.log_operation(
                operation_type=self._operation_type,
                output_table=self._output_table,
                output_row_count=self._output_row_count,
                output_columns=self._output_columns,
                execution_time_ms=execution_time_ms,
                **self._kwargs
            )
        return False

    def set_output(
        self,
        output_table: Optional[str] = None,
        output_row_count: Optional[int] = None,
        output_columns: Optional[List[str]] = None
    ) -> None:
        """Set output details to be logged when the context exits."""
        self._output_table = output_table
        self._output_row_count = output_row_count
        self._output_columns = output_columns


# =============================================================================
# Convenience functions for common patterns
# =============================================================================

def log_bronze_to_silver(
    agent_id: str,
    bronze_tables: List[str],
    silver_table: str,
    transformation_logic: str,
    input_row_count: Optional[int] = None,
    output_row_count: Optional[int] = None,
    purpose: Optional[str] = None
) -> UUID:
    """
    Convenience function to log a Bronze → Silver transformation.

    Returns the session ID.
    """
    with TransformationLogger(
        agent_id=agent_id,
        session_type=SessionType.BRONZE_TO_SILVER,
        source_layer=SourceLayer.BRONZE,
        source_tables=bronze_tables,
        purpose=purpose or f"Transform {bronze_tables} to {silver_table}"
    ) as log:
        log.log_operation(
            operation_type=OperationType.TRANSFORM,
            input_tables=bronze_tables,
            transformation_logic=transformation_logic,
            output_table=silver_table,
            input_row_count=input_row_count,
            output_row_count=output_row_count
        )

        # Add lineage
        for bronze_table in bronze_tables:
            bronze_schema, bronze_name = bronze_table.split('.') if '.' in bronze_table else ('bronze', bronze_table)
            silver_schema, silver_name = silver_table.split('.') if '.' in silver_table else ('silver', silver_table)

            log.add_lineage(
                source_schema=bronze_schema,
                source_name=bronze_name,
                target_schema=silver_schema,
                target_name=silver_name,
                relationship_type=RelationshipType.TRANSFORMS,
                transformation_description=transformation_logic[:200] if transformation_logic else None
            )

        log.register_output(
            artifact_type=ArtifactType.TABLE,
            artifact_name=silver_table,
            source_tables=bronze_tables,
            row_count=output_row_count
        )

        return log.session_id


def log_silver_to_gold(
    agent_id: str,
    silver_tables: List[str],
    gold_view: str,
    transformation_logic: str,
    purpose: Optional[str] = None
) -> UUID:
    """
    Convenience function to log a Silver → Gold view creation/refresh.

    Returns the session ID.
    """
    with TransformationLogger(
        agent_id=agent_id,
        session_type=SessionType.SILVER_TO_GOLD,
        source_layer=SourceLayer.SILVER,
        source_tables=silver_tables,
        purpose=purpose or f"Create/refresh {gold_view}"
    ) as log:
        log.log_operation(
            operation_type=OperationType.REFRESH,
            input_tables=silver_tables,
            transformation_logic=transformation_logic,
            output_table=gold_view
        )

        # Add lineage
        for silver_table in silver_tables:
            silver_schema, silver_name = silver_table.split('.') if '.' in silver_table else ('silver', silver_table)
            gold_schema, gold_name = gold_view.split('.') if '.' in gold_view else ('gold', gold_view)

            log.add_lineage(
                source_schema=silver_schema,
                source_name=silver_name,
                target_schema=gold_schema,
                target_name=gold_name,
                relationship_type=RelationshipType.DERIVES_FROM,
                target_type='VIEW'
            )

        log.register_output(
            artifact_type=ArtifactType.VIEW,
            artifact_name=gold_view,
            source_tables=silver_tables
        )

        return log.session_id
