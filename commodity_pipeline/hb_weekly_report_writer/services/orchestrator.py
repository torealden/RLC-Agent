"""
HB Report Orchestrator

Main orchestration service that coordinates the complete weekly report generation
workflow from data collection through document delivery.
"""

import logging
import json
from dataclasses import dataclass, field, asdict
from datetime import datetime, date, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple

from ..config.settings import HBWeeklyReportConfig, default_config
from ..agents.report_writer_agent import ReportWriterAgent, WriterResult
from ..database.models import (
    init_database,
    WeeklyReport,
    Question,
    QuestionStatus,
    ReportStatus,
    get_session,
)
from .document_builder import DocumentBuilder, PlaceholderDocumentBuilder, DocumentResult

logger = logging.getLogger(__name__)


@dataclass
class OrchestratorResult:
    """Result of complete orchestration run"""
    success: bool
    report_date: date

    # Timing
    started_at: datetime = field(default_factory=datetime.utcnow)
    completed_at: Optional[datetime] = None
    total_duration_seconds: float = 0.0

    # Report info
    report_id: Optional[int] = None
    document_path: Optional[Path] = None
    dropbox_path: Optional[str] = None

    # Status
    data_complete: bool = False
    content_generated: bool = False
    document_created: bool = False
    uploaded: bool = False

    # Questions
    questions_raised: int = 0
    questions_answered: int = 0
    questions_timeout: int = 0

    # Quality metrics
    completeness_score: float = 0.0
    placeholders_count: int = 0
    llm_estimates_count: int = 0

    # Errors
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)


class HBReportOrchestrator:
    """
    Orchestrates the complete HB Weekly Report generation workflow

    Workflow Steps:
    1. Initialize and validate configuration
    2. Gather data from all sources
    3. Identify missing data and raise questions
    4. Wait for answers (if configured)
    5. Generate report content
    6. Build Word document
    7. Upload to Dropbox
    8. Send notifications
    9. Log results
    """

    def __init__(self, config: HBWeeklyReportConfig = None, db_session_factory=None):
        """
        Initialize the orchestrator

        Args:
            config: Configuration (default: from environment)
            db_session_factory: Database session factory (optional)
        """
        self.config = config or default_config
        self.logger = logging.getLogger(f"{self.__class__.__name__}")

        # Initialize database if needed
        if db_session_factory:
            self.db_session_factory = db_session_factory
        else:
            self.db_session_factory = self._init_database()

        # Initialize components
        self.report_writer = ReportWriterAgent(self.config, self.db_session_factory)
        self.document_builder = self._init_document_builder()

        self.logger.info(f"Initialized {self.config.agent_name} v{self.config.agent_version}")

    def _init_database(self):
        """Initialize database connection"""
        try:
            connection_string = self.config.database.get_connection_string()
            _, session_factory = init_database(connection_string)
            return session_factory
        except Exception as e:
            self.logger.warning(f"Database initialization failed: {e}. Running without persistence.")
            return None

    def _init_document_builder(self) -> DocumentBuilder:
        """Initialize appropriate document builder"""
        try:
            import docx
            return DocumentBuilder(self.config)
        except ImportError:
            self.logger.warning("python-docx not available, using HTML fallback")
            return PlaceholderDocumentBuilder(self.config)

    def run_weekly_report(self, report_date: date = None) -> OrchestratorResult:
        """
        Run the complete weekly report generation workflow

        Args:
            report_date: Date for the report (default: today, typically Tuesday)

        Returns:
            OrchestratorResult with complete status
        """
        report_date = report_date or date.today()

        result = OrchestratorResult(
            success=False,
            report_date=report_date,
        )

        self.logger.info(f"Starting weekly report generation for {report_date}")

        try:
            # Create database record
            report_record = self._create_report_record(report_date)
            if report_record:
                result.report_id = report_record.id

            # Step 1: Generate report content
            self.logger.info("Step 1: Generating report content...")
            writer_result = self.report_writer.generate_report(report_date)

            if not writer_result.success:
                result.errors.append("Content generation failed")
                result.errors.extend(writer_result.errors)
                return self._finalize_result(result, report_record)

            result.content_generated = True

            # Step 2: Handle pending questions
            if writer_result.pending_questions:
                result.questions_raised = len(writer_result.pending_questions)
                self.logger.info(f"Step 2: Handling {result.questions_raised} questions...")

                # Record questions
                self._record_questions(report_record, writer_result.pending_questions)

                # Wait for answers if configured
                if self.config.question_wait_timeout_hours > 0:
                    answered, timeout = self._wait_for_answers(
                        report_record,
                        self.config.question_wait_timeout_hours
                    )
                    result.questions_answered = answered
                    result.questions_timeout = timeout

            # Step 3: Build document
            self.logger.info("Step 3: Building document...")
            doc_result = self.document_builder.build_document(writer_result.content)

            if not doc_result.success:
                result.errors.append(f"Document generation failed: {doc_result.error_message}")
                result.warnings.append("Content was generated but document creation failed")
            else:
                result.document_created = True
                result.document_path = doc_result.document_path

            # Step 4: Upload to Dropbox
            if result.document_created and self.config.dropbox.enabled:
                self.logger.info("Step 4: Uploading to Dropbox...")
                dropbox_path = self._upload_to_dropbox(doc_result.document_path)
                if dropbox_path:
                    result.uploaded = True
                    result.dropbox_path = dropbox_path
                else:
                    result.warnings.append("Dropbox upload failed - document saved locally only")

            # Step 5: Record metrics
            result.data_complete = writer_result.content.is_complete if writer_result.content else False
            result.completeness_score = writer_result.content.completeness_score if writer_result.content else 0
            result.placeholders_count = len(writer_result.content.placeholders) if writer_result.content else 0
            result.llm_estimates_count = len(writer_result.content.llm_estimates) if writer_result.content else 0

            # Success if document was created
            result.success = result.document_created

            # Step 6: Send notifications
            if result.success:
                self._send_completion_notification(result)
            else:
                self._send_error_notification(result)

        except Exception as e:
            self.logger.error(f"Orchestration failed: {e}", exc_info=True)
            result.errors.append(str(e))

        return self._finalize_result(result, report_record if 'report_record' in locals() else None)

    def _create_report_record(self, report_date: date) -> Optional[WeeklyReport]:
        """Create database record for the report"""
        if not self.db_session_factory:
            return None

        try:
            session = get_session(self.db_session_factory)

            # Calculate week ending
            days_since_friday = (report_date.weekday() - 4) % 7
            if days_since_friday == 0 and report_date.weekday() != 4:
                days_since_friday = 7
            week_ending = report_date - timedelta(days=days_since_friday)

            report = WeeklyReport(
                report_date=report_date,
                week_ending=week_ending,
                status=ReportStatus.DRAFT,
            )
            session.add(report)
            session.commit()

            self.logger.info(f"Created report record ID: {report.id}")
            return report

        except Exception as e:
            self.logger.error(f"Failed to create report record: {e}")
            return None

    def _record_questions(self, report: Optional[WeeklyReport], questions: List[Dict]):
        """Record questions in database"""
        if not self.db_session_factory or not report:
            # Log questions even without database
            for q in questions:
                self.logger.info(f"Question: {q.get('question', 'Unknown')}")
            return

        try:
            session = get_session(self.db_session_factory)

            for q in questions:
                question = Question(
                    report_id=report.id,
                    question_text=q.get('question', ''),
                    category=q.get('category'),
                    context=json.dumps(q) if q else None,
                )
                session.add(question)

            session.commit()
            self.logger.info(f"Recorded {len(questions)} questions")

        except Exception as e:
            self.logger.error(f"Failed to record questions: {e}")

    def _wait_for_answers(
        self,
        report: Optional[WeeklyReport],
        timeout_hours: float
    ) -> Tuple[int, int]:
        """
        Wait for answers to questions

        Returns:
            Tuple of (answered_count, timeout_count)
        """
        import time

        if not self.db_session_factory or not report:
            self.logger.info("No database - skipping question wait")
            return 0, 0

        timeout_seconds = timeout_hours * 3600
        check_interval = 300  # Check every 5 minutes
        start_time = datetime.utcnow()

        self.logger.info(f"Waiting up to {timeout_hours} hours for answers...")

        answered = 0
        pending = 0

        while (datetime.utcnow() - start_time).total_seconds() < timeout_seconds:
            try:
                session = get_session(self.db_session_factory)

                # Check question status
                questions = session.query(Question).filter(
                    Question.report_id == report.id
                ).all()

                answered = sum(1 for q in questions if q.status == QuestionStatus.ANSWERED)
                pending = sum(1 for q in questions if q.status == QuestionStatus.PENDING)

                if pending == 0:
                    self.logger.info(f"All questions resolved: {answered} answered")
                    return answered, 0

                self.logger.debug(f"Waiting... {answered} answered, {pending} pending")
                time.sleep(check_interval)

            except Exception as e:
                self.logger.error(f"Error checking questions: {e}")
                time.sleep(check_interval)

        # Timeout reached
        self.logger.info(f"Timeout reached. {answered} answered, {pending} timed out")

        # Mark remaining as timeout
        if self.db_session_factory:
            try:
                session = get_session(self.db_session_factory)
                session.query(Question).filter(
                    Question.report_id == report.id,
                    Question.status == QuestionStatus.PENDING
                ).update({"status": QuestionStatus.TIMEOUT})
                session.commit()
            except Exception as e:
                self.logger.error(f"Failed to update timeout status: {e}")

        return answered, pending

    def _upload_to_dropbox(self, local_path: Path) -> Optional[str]:
        """Upload document to Dropbox"""
        try:
            import dropbox

            dbx_config = self.config.dropbox

            # Create client
            if dbx_config.access_token:
                client = dropbox.Dropbox(dbx_config.access_token)
            elif dbx_config.refresh_token:
                client = dropbox.Dropbox(
                    oauth2_refresh_token=dbx_config.refresh_token,
                    app_key=dbx_config.app_key,
                    app_secret=dbx_config.app_secret,
                )
            else:
                self.logger.warning("Dropbox credentials not configured")
                return None

            # Upload file
            dropbox_path = f"{dbx_config.reports_folder}/{local_path.name}"

            with open(local_path, 'rb') as f:
                client.files_upload(
                    f.read(),
                    dropbox_path,
                    mode=dropbox.files.WriteMode.overwrite
                )

            self.logger.info(f"Uploaded to Dropbox: {dropbox_path}")
            return dropbox_path

        except ImportError:
            self.logger.warning("dropbox package not installed")
            return None
        except Exception as e:
            self.logger.error(f"Dropbox upload failed: {e}")
            return None

    def _send_completion_notification(self, result: OrchestratorResult):
        """Send notification that report is complete"""
        if not self.config.notifications.enabled:
            return

        try:
            message = f"""HB Weekly Report Generated Successfully

Report Date: {result.report_date}
Document: {result.document_path}
Completeness: {result.completeness_score:.1f}%

Questions: {result.questions_raised} raised, {result.questions_answered} answered
Placeholders: {result.placeholders_count}
LLM Estimates: {result.llm_estimates_count}

{f'Dropbox: {result.dropbox_path}' if result.dropbox_path else 'Not uploaded to Dropbox'}
"""

            # Send email if configured
            if self.config.notifications.smtp_server:
                self._send_email(
                    subject=f"HB Weekly Report Ready - {result.report_date}",
                    body=message,
                    recipients=self.config.notifications.report_complete_recipients
                )

            # Send Slack if configured
            if self.config.notifications.slack_webhook_url:
                self._send_slack(message)

        except Exception as e:
            self.logger.error(f"Failed to send completion notification: {e}")

    def _send_error_notification(self, result: OrchestratorResult):
        """Send notification about errors"""
        if not self.config.notifications.enabled:
            return

        try:
            message = f"""HB Weekly Report Generation FAILED

Report Date: {result.report_date}

Errors:
{chr(10).join(f'- {e}' for e in result.errors)}

Warnings:
{chr(10).join(f'- {w}' for w in result.warnings)}

Please investigate and retry manually if needed.
"""

            if self.config.notifications.smtp_server:
                self._send_email(
                    subject=f"[ALERT] HB Weekly Report Failed - {result.report_date}",
                    body=message,
                    recipients=self.config.notifications.error_recipients
                )

            if self.config.notifications.slack_webhook_url:
                self._send_slack(f":warning: HB Report Failed: {result.errors[0] if result.errors else 'Unknown error'}")

        except Exception as e:
            self.logger.error(f"Failed to send error notification: {e}")

    def _send_email(self, subject: str, body: str, recipients: List[str]):
        """Send email notification"""
        import smtplib
        from email.mime.text import MIMEText
        from email.mime.multipart import MIMEMultipart

        if not recipients:
            return

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

            self.logger.info(f"Sent email to {recipients}")

        except Exception as e:
            self.logger.error(f"Email send failed: {e}")

    def _send_slack(self, message: str):
        """Send Slack notification"""
        import requests

        try:
            response = requests.post(
                self.config.notifications.slack_webhook_url,
                json={"text": message},
                timeout=10
            )
            if response.status_code == 200:
                self.logger.info("Sent Slack notification")
            else:
                self.logger.warning(f"Slack notification failed: {response.status_code}")

        except Exception as e:
            self.logger.error(f"Slack send failed: {e}")

    def _finalize_result(
        self,
        result: OrchestratorResult,
        report_record: Optional[WeeklyReport]
    ) -> OrchestratorResult:
        """Finalize result and update database"""
        result.completed_at = datetime.utcnow()
        result.total_duration_seconds = (result.completed_at - result.started_at).total_seconds()

        # Update database record
        if self.db_session_factory and report_record:
            try:
                session = get_session(self.db_session_factory)
                report = session.query(WeeklyReport).get(report_record.id)

                if report:
                    report.status = ReportStatus.PENDING_REVIEW if result.success else ReportStatus.DRAFT
                    report.generation_time_seconds = result.total_duration_seconds
                    report.document_path = str(result.document_path) if result.document_path else None
                    report.dropbox_path = result.dropbox_path
                    report.placeholders_count = result.placeholders_count
                    report.llm_estimates_count = result.llm_estimates_count
                    report.data_completeness_score = result.completeness_score
                    report.has_errors = len(result.errors) > 0
                    report.error_summary = '; '.join(result.errors) if result.errors else None

                    session.commit()

            except Exception as e:
                self.logger.error(f"Failed to update report record: {e}")

        self.logger.info(
            f"Report generation {'succeeded' if result.success else 'failed'} "
            f"in {result.total_duration_seconds:.1f}s"
        )

        return result

    def get_status(self) -> Dict[str, Any]:
        """Get orchestrator status"""
        return {
            "agent_name": self.config.agent_name,
            "agent_version": self.config.agent_version,
            "data_source": self.config.internal_data_source.value,
            "dropbox_enabled": self.config.dropbox.enabled,
            "notifications_enabled": self.config.notifications.enabled,
            "llm_enabled": self.config.llm.enabled,
            "llm_provider": self.config.llm.provider if self.config.llm.enabled else None,
        }
