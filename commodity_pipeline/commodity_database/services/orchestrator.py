"""
Database Pipeline Orchestrator

Coordinates the data ingestion pipeline including:
- Data collection from various sources (Dropbox, APIs)
- Data loading via DatabaseAgent
- Verification via VerificationAgent
- Notifications and alerting
"""

import json
import logging
import smtplib
from dataclasses import dataclass, field
from datetime import datetime, date, timedelta
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from pathlib import Path
from typing import Dict, List, Optional, Any, Callable
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
from sqlalchemy.orm import sessionmaker

from ..agents.database_agent import DatabaseAgent, IngestionResult
from ..agents.verification_agent import VerificationAgent, VerificationResult, DataQualityReport
from ..database.models import (
    DataSourceType, DataLoadLog, QualityAlert, SchemaChange,
    AlertSeverity, LoadStatus
)
from ..config.settings import (
    CommodityDatabaseConfig, IngestionMode, AlertChannel,
    default_config
)


logger = logging.getLogger(__name__)


# =============================================================================
# RESULT DATACLASSES
# =============================================================================

@dataclass
class SourceFetchResult:
    """Result of fetching data from a source."""
    success: bool
    source_name: str
    source_type: DataSourceType
    data: Optional[Any] = None
    record_count: int = 0
    error_message: Optional[str] = None
    duration_seconds: float = 0.0


@dataclass
class PipelineResult:
    """Result of a complete pipeline run."""
    success: bool
    started_at: datetime = field(default_factory=datetime.utcnow)
    completed_at: Optional[datetime] = None
    duration_seconds: float = 0.0
    sources_processed: int = 0
    sources_failed: int = 0
    total_records_loaded: int = 0
    total_records_failed: int = 0
    fetch_results: List[SourceFetchResult] = field(default_factory=list)
    ingestion_results: List[IngestionResult] = field(default_factory=list)
    verification_results: List[VerificationResult] = field(default_factory=list)
    alerts_sent: int = 0
    errors: List[str] = field(default_factory=list)


@dataclass
class ScheduledTask:
    """Definition of a scheduled data fetch task."""
    name: str
    source_type: DataSourceType
    fetch_function: Callable
    schedule_cron: str  # Cron expression
    enabled: bool = True
    last_run: Optional[datetime] = None
    next_run: Optional[datetime] = None
    parameters: Dict[str, Any] = field(default_factory=dict)


# =============================================================================
# ORCHESTRATOR
# =============================================================================

class DatabasePipelineOrchestrator:
    """
    Orchestrates the commodity data pipeline.

    Coordinates:
    1. Collector agents (fetch data from sources)
    2. Database agent (load data into database)
    3. Verification agent (verify data integrity)
    4. Notification service (send alerts)

    Features:
    - Parallel data fetching
    - Retry logic with exponential backoff
    - Schema change approval workflow
    - Quality alerting
    - Email/Slack notifications
    """

    def __init__(
        self,
        config: Optional[CommodityDatabaseConfig] = None,
        session_factory: Optional[sessionmaker] = None
    ):
        """Initialize the orchestrator."""
        self.config = config or default_config
        self._session_factory = session_factory
        self._database_agent: Optional[DatabaseAgent] = None
        self._verification_agent: Optional[VerificationAgent] = None
        self._scheduled_tasks: Dict[str, ScheduledTask] = {}

    @property
    def database_agent(self) -> DatabaseAgent:
        """Lazy initialization of database agent."""
        if self._database_agent is None:
            self._database_agent = DatabaseAgent(
                self.config, self._session_factory
            )
        return self._database_agent

    @property
    def verification_agent(self) -> VerificationAgent:
        """Lazy initialization of verification agent."""
        if self._verification_agent is None:
            self._verification_agent = VerificationAgent(
                self.config, self._session_factory
            )
        return self._verification_agent

    # =========================================================================
    # MAIN PIPELINE EXECUTION
    # =========================================================================

    def run_full_pipeline(
        self,
        sources: List[Dict[str, Any]] = None,
        verify: bool = True,
        notify: bool = True
    ) -> PipelineResult:
        """
        Run the complete data pipeline.

        Args:
            sources: List of source configurations to process
            verify: Whether to run verification after loading
            notify: Whether to send notifications

        Returns:
            PipelineResult with all statistics
        """
        result = PipelineResult(success=True)
        logger.info("Starting full pipeline run")

        try:
            # Check for pending schema changes
            pending_changes = self.database_agent.get_pending_schema_changes()
            if pending_changes:
                logger.warning(f"Found {len(pending_changes)} pending schema changes")
                if notify:
                    self._send_schema_change_notification(pending_changes)

            # Fetch data from all sources
            if sources:
                fetch_results = self._fetch_all_sources(sources)
                result.fetch_results = fetch_results

                for fetch_result in fetch_results:
                    if fetch_result.success:
                        result.sources_processed += 1
                    else:
                        result.sources_failed += 1
                        result.errors.append(
                            f"Failed to fetch {fetch_result.source_name}: {fetch_result.error_message}"
                        )

                # Load fetched data
                for fetch_result in fetch_results:
                    if fetch_result.success and fetch_result.data is not None:
                        try:
                            ingestion_result = self._load_data(
                                fetch_result.data,
                                fetch_result.source_type,
                                fetch_result.source_name
                            )
                            result.ingestion_results.append(ingestion_result)

                            result.total_records_loaded += (
                                ingestion_result.records_inserted +
                                ingestion_result.records_updated
                            )
                            result.total_records_failed += ingestion_result.records_errored

                            # Verify if configured
                            if verify and ingestion_result.load_id:
                                verification = self.verification_agent.verify_load(
                                    ingestion_result.load_id,
                                    source_count=fetch_result.record_count
                                )
                                result.verification_results.append(verification)

                                if not verification.success:
                                    logger.warning(
                                        f"Verification failed for {fetch_result.source_name}"
                                    )

                        except Exception as e:
                            logger.exception(f"Failed to load data from {fetch_result.source_name}")
                            result.errors.append(str(e))
                            result.success = False

            # Send notifications
            if notify:
                result.alerts_sent = self._send_pipeline_notifications(result)

        except Exception as e:
            result.success = False
            result.errors.append(f"Pipeline error: {str(e)}")
            logger.exception("Pipeline failed")

        finally:
            result.completed_at = datetime.utcnow()
            result.duration_seconds = (result.completed_at - result.started_at).total_seconds()
            logger.info(
                f"Pipeline completed: {result.total_records_loaded} loaded, "
                f"{result.total_records_failed} failed, "
                f"duration: {result.duration_seconds:.2f}s"
            )

        return result

    def run_incremental_update(
        self,
        data_type: str,
        source_type: DataSourceType,
        source_name: str,
        data: Any
    ) -> PipelineResult:
        """
        Run an incremental data update for a single source.

        Args:
            data_type: Type of data (price, fundamental, crop_progress, trade_flow)
            source_type: Type of source
            source_name: Name of source
            data: Data to load

        Returns:
            PipelineResult
        """
        result = PipelineResult(success=True)

        try:
            # Load data
            ingestion_result = self._load_data(
                data, source_type, source_name,
                mode=IngestionMode.INCREMENTAL
            )
            result.ingestion_results.append(ingestion_result)

            result.total_records_loaded = (
                ingestion_result.records_inserted +
                ingestion_result.records_updated
            )
            result.total_records_failed = ingestion_result.records_errored
            result.success = ingestion_result.success

            # Verify
            if ingestion_result.load_id:
                verification = self.verification_agent.verify_load(
                    ingestion_result.load_id
                )
                result.verification_results.append(verification)

        except Exception as e:
            result.success = False
            result.errors.append(str(e))
            logger.exception("Incremental update failed")

        finally:
            result.completed_at = datetime.utcnow()
            result.duration_seconds = (result.completed_at - result.started_at).total_seconds()

        return result

    # =========================================================================
    # DATA FETCHING
    # =========================================================================

    def _fetch_all_sources(
        self,
        sources: List[Dict[str, Any]],
        parallel: bool = True,
        max_workers: int = 4
    ) -> List[SourceFetchResult]:
        """Fetch data from all configured sources."""
        results = []

        if parallel and len(sources) > 1:
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = {
                    executor.submit(self._fetch_source, source): source
                    for source in sources
                }

                for future in as_completed(futures):
                    source = futures[future]
                    try:
                        result = future.result()
                        results.append(result)
                    except Exception as e:
                        results.append(SourceFetchResult(
                            success=False,
                            source_name=source.get('name', 'unknown'),
                            source_type=source.get('type', DataSourceType.API),
                            error_message=str(e)
                        ))
        else:
            for source in sources:
                result = self._fetch_source(source)
                results.append(result)

        return results

    def _fetch_source(self, source: Dict[str, Any]) -> SourceFetchResult:
        """Fetch data from a single source."""
        start_time = datetime.utcnow()
        source_name = source.get('name', 'unknown')
        source_type = source.get('type', DataSourceType.API)

        try:
            fetch_function = source.get('fetch_function')
            if fetch_function:
                data = fetch_function(**source.get('parameters', {}))
            elif source.get('file_path'):
                data = self._load_from_file(source['file_path'])
            else:
                raise ValueError(f"No fetch method specified for source: {source_name}")

            record_count = len(data) if isinstance(data, (list, pd.DataFrame)) else 1

            return SourceFetchResult(
                success=True,
                source_name=source_name,
                source_type=source_type,
                data=data,
                record_count=record_count,
                duration_seconds=(datetime.utcnow() - start_time).total_seconds()
            )

        except Exception as e:
            logger.exception(f"Failed to fetch from {source_name}")
            return SourceFetchResult(
                success=False,
                source_name=source_name,
                source_type=source_type,
                error_message=str(e),
                duration_seconds=(datetime.utcnow() - start_time).total_seconds()
            )

    def _load_from_file(self, file_path: str) -> pd.DataFrame:
        """Load data from a file."""
        path = Path(file_path)

        if path.suffix.lower() in ['.xlsx', '.xls']:
            return pd.read_excel(path)
        elif path.suffix.lower() == '.csv':
            return pd.read_csv(path)
        elif path.suffix.lower() == '.json':
            return pd.read_json(path)
        else:
            raise ValueError(f"Unsupported file format: {path.suffix}")

    # =========================================================================
    # DATA LOADING
    # =========================================================================

    def _load_data(
        self,
        data: Any,
        source_type: DataSourceType,
        source_name: str,
        mode: IngestionMode = None
    ) -> IngestionResult:
        """Load data using the appropriate method based on data structure."""
        mode = mode or self.config.ingestion.default_mode

        # Detect data type from structure
        if isinstance(data, pd.DataFrame):
            columns = set(data.columns)
        elif isinstance(data, list) and len(data) > 0:
            columns = set(data[0].keys())
        elif isinstance(data, dict):
            columns = set(data.keys())
        else:
            columns = set()

        # Route to appropriate loader
        if 'price' in columns or 'observation_date' in columns:
            return self.database_agent.ingest_price_data(
                data, source_type, source_name, mode
            )
        elif 'field_name' in columns or 'production' in columns or 'ending_stocks' in columns:
            return self.database_agent.ingest_fundamental_data(
                data, source_type, source_name, mode
            )
        elif 'week_ending' in columns or 'pct_planted' in columns:
            return self.database_agent.ingest_crop_progress(
                data, source_type, source_name, mode
            )
        elif 'flow_type' in columns or 'partner_country' in columns:
            return self.database_agent.ingest_trade_flows(
                data, source_type, source_name, mode
            )
        else:
            # Default to fundamental data
            return self.database_agent.ingest_fundamental_data(
                data, source_type, source_name, mode
            )

    # =========================================================================
    # NOTIFICATIONS
    # =========================================================================

    def _send_pipeline_notifications(self, result: PipelineResult) -> int:
        """Send notifications based on pipeline results."""
        alerts_sent = 0

        # Determine notification severity
        if result.success and result.total_records_failed == 0:
            severity = "success"
            recipients = self.config.notifications.success_recipients
        elif result.sources_failed > 0 or result.total_records_failed > 0:
            severity = "warning"
            recipients = self.config.notifications.quality_alert_recipients
        else:
            severity = "error"
            recipients = self.config.notifications.error_recipients

        if not recipients:
            return 0

        # Build message
        subject = f"[Commodity DB] Pipeline {severity.upper()}: {result.total_records_loaded} records loaded"

        body = self._format_pipeline_report(result)

        # Send via configured channels
        if self.config.notifications.default_channel == AlertChannel.EMAIL:
            if self._send_email(recipients, subject, body):
                alerts_sent += 1

        if self.config.notifications.slack_webhook_url:
            if self._send_slack_notification(subject, body):
                alerts_sent += 1

        return alerts_sent

    def _send_schema_change_notification(self, changes: List[SchemaChange]) -> bool:
        """Send notification about pending schema changes."""
        if not self.config.notifications.schema_change_recipients:
            return False

        subject = f"[Commodity DB] {len(changes)} Schema Changes Pending Approval"

        body = "The following schema changes require approval:\n\n"
        for change in changes:
            body += f"- {change.change_type}: {change.proposed_change}\n"
            body += f"  Table: {change.table_name}\n"
            body += f"  Created: {change.created_at}\n\n"

        return self._send_email(
            self.config.notifications.schema_change_recipients,
            subject,
            body
        )

    def _format_pipeline_report(self, result: PipelineResult) -> str:
        """Format pipeline result as text report."""
        lines = [
            "=" * 50,
            "COMMODITY DATABASE PIPELINE REPORT",
            "=" * 50,
            "",
            f"Started: {result.started_at}",
            f"Completed: {result.completed_at}",
            f"Duration: {result.duration_seconds:.2f} seconds",
            "",
            "SUMMARY",
            "-" * 30,
            f"Sources Processed: {result.sources_processed}",
            f"Sources Failed: {result.sources_failed}",
            f"Records Loaded: {result.total_records_loaded}",
            f"Records Failed: {result.total_records_failed}",
            "",
        ]

        if result.fetch_results:
            lines.append("FETCH RESULTS")
            lines.append("-" * 30)
            for fetch in result.fetch_results:
                status = "OK" if fetch.success else "FAILED"
                lines.append(f"  {fetch.source_name}: {status} ({fetch.record_count} records)")
            lines.append("")

        if result.ingestion_results:
            lines.append("INGESTION RESULTS")
            lines.append("-" * 30)
            for ingestion in result.ingestion_results:
                lines.append(f"  Load ID {ingestion.load_id}:")
                lines.append(f"    Inserted: {ingestion.records_inserted}")
                lines.append(f"    Updated: {ingestion.records_updated}")
                lines.append(f"    Skipped: {ingestion.records_skipped}")
                lines.append(f"    Errors: {ingestion.records_errored}")
            lines.append("")

        if result.verification_results:
            lines.append("VERIFICATION RESULTS")
            lines.append("-" * 30)
            for verification in result.verification_results:
                status = "PASSED" if verification.success else "FAILED"
                lines.append(
                    f"  Load {verification.load_id}: {status} "
                    f"({verification.checks_passed} passed, {verification.checks_failed} failed)"
                )
            lines.append("")

        if result.errors:
            lines.append("ERRORS")
            lines.append("-" * 30)
            for error in result.errors:
                lines.append(f"  - {error}")
            lines.append("")

        return "\n".join(lines)

    def _send_email(
        self,
        recipients: List[str],
        subject: str,
        body: str
    ) -> bool:
        """Send email notification."""
        if not self.config.notifications.smtp_server:
            logger.warning("SMTP not configured, skipping email")
            return False

        try:
            msg = MIMEMultipart()
            msg['From'] = self.config.notifications.from_email
            msg['To'] = ', '.join(recipients)
            msg['Subject'] = subject
            msg.attach(MIMEText(body, 'plain'))

            with smtplib.SMTP(
                self.config.notifications.smtp_server,
                self.config.notifications.smtp_port
            ) as server:
                server.starttls()
                if self.config.notifications.smtp_username:
                    server.login(
                        self.config.notifications.smtp_username,
                        self.config.notifications.smtp_password
                    )
                server.send_message(msg)

            logger.info(f"Email sent to {len(recipients)} recipients")
            return True

        except Exception as e:
            logger.exception("Failed to send email")
            return False

    def _send_slack_notification(self, title: str, message: str) -> bool:
        """Send Slack notification via webhook."""
        if not self.config.notifications.slack_webhook_url:
            return False

        try:
            import requests

            payload = {
                "channel": self.config.notifications.slack_channel,
                "username": "Commodity Database",
                "text": f"*{title}*\n```{message[:2000]}```",
            }

            response = requests.post(
                self.config.notifications.slack_webhook_url,
                json=payload,
                timeout=10
            )
            response.raise_for_status()

            logger.info("Slack notification sent")
            return True

        except Exception as e:
            logger.exception("Failed to send Slack notification")
            return False

    # =========================================================================
    # SCHEDULING
    # =========================================================================

    def register_scheduled_task(self, task: ScheduledTask):
        """Register a scheduled task."""
        self._scheduled_tasks[task.name] = task
        logger.info(f"Registered scheduled task: {task.name}")

    def get_due_tasks(self) -> List[ScheduledTask]:
        """Get tasks that are due to run."""
        now = datetime.utcnow()
        due_tasks = []

        for task in self._scheduled_tasks.values():
            if not task.enabled:
                continue

            if task.next_run is None or task.next_run <= now:
                due_tasks.append(task)

        return due_tasks

    def run_scheduled_tasks(self) -> Dict[str, PipelineResult]:
        """Run all due scheduled tasks."""
        results = {}
        due_tasks = self.get_due_tasks()

        for task in due_tasks:
            logger.info(f"Running scheduled task: {task.name}")
            try:
                source = {
                    'name': task.name,
                    'type': task.source_type,
                    'fetch_function': task.fetch_function,
                    'parameters': task.parameters
                }

                result = self.run_full_pipeline(sources=[source])
                results[task.name] = result

                task.last_run = datetime.utcnow()
                # TODO: Calculate next_run based on cron expression

            except Exception as e:
                logger.exception(f"Scheduled task {task.name} failed")
                results[task.name] = PipelineResult(
                    success=False,
                    errors=[str(e)]
                )

        return results

    # =========================================================================
    # QUALITY REPORTING
    # =========================================================================

    def generate_daily_report(self) -> DataQualityReport:
        """Generate daily data quality report."""
        return self.verification_agent.generate_quality_report()

    def get_pipeline_status(self) -> Dict[str, Any]:
        """Get current pipeline status."""
        session = self.database_agent.get_session()
        try:
            # Recent load statistics
            recent_loads = session.query(DataLoadLog).order_by(
                DataLoadLog.started_at.desc()
            ).limit(10).all()

            # Unresolved alerts
            alerts = session.query(QualityAlert).filter_by(
                is_resolved=False
            ).count()

            # Pending schema changes
            pending_changes = session.query(SchemaChange).filter_by(
                status="pending"
            ).count()

            return {
                "last_load": recent_loads[0].started_at if recent_loads else None,
                "recent_loads": [
                    {
                        "load_id": load.load_id,
                        "type": load.load_type,
                        "source": load.source_name,
                        "status": load.status.value,
                        "records": load.records_inserted + load.records_updated,
                        "started_at": str(load.started_at)
                    }
                    for load in recent_loads
                ],
                "unresolved_alerts": alerts,
                "pending_schema_changes": pending_changes,
                "scheduled_tasks": len(self._scheduled_tasks),
                "database_connected": True
            }

        except Exception as e:
            return {
                "error": str(e),
                "database_connected": False
            }

        finally:
            session.close()
