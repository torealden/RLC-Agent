"""
Report Publisher

Orchestrates report distribution across multiple channels (email, Notion, file).
Accepts a Report object and routes it through formatting and delivery.

Usage:
    from src.agents.publishing.publisher import Publisher, Report

    report = Report(
        title="WASDE Monthly Summary — March 2026",
        report_type="wasde",
        narrative="Corn ending stocks were revised...",
        charts=["/path/to/chart1.png"],
        metadata={"triggered_by": "usda_wasde", "marketing_year": "2025/26"},
    )

    publisher = Publisher()
    result = publisher.publish(report, channels=["email", "file"])
"""

import logging
import os
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent.parent


@dataclass
class Report:
    """A report ready for publishing."""
    title: str
    report_type: str                          # wasde, crop_progress, ethanol, positioning, weekly
    narrative: str                             # LLM-generated text (markdown)
    charts: List[str] = field(default_factory=list)      # Paths to chart images
    tables: List[str] = field(default_factory=list)      # Markdown tables
    metadata: Dict = field(default_factory=dict)
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


@dataclass
class PublishResult:
    """Result of a publish operation."""
    success: bool
    channels_attempted: List[str]
    channels_succeeded: List[str] = field(default_factory=list)
    channels_failed: Dict[str, str] = field(default_factory=dict)  # channel -> error
    file_paths: List[str] = field(default_factory=list)
    error_message: Optional[str] = None


class Publisher:
    """
    Distributes reports across configured channels.

    Channels:
        - 'file'    Save as .md and/or .html to output/reports/
        - 'email'   Send via SMTP
        - 'notion'  Create Notion page (requires Notion MCP)
        - 'docx'    Generate Word document
    """

    def __init__(self, config: Dict = None):
        self.config = config or self._load_config()
        self._channels = {}

    def _load_config(self) -> Dict:
        """Load publishing config from environment."""
        return {
            'smtp_host': os.environ.get('SMTP_HOST', 'smtp.gmail.com'),
            'smtp_port': int(os.environ.get('SMTP_PORT', '587')),
            'smtp_user': os.environ.get('SMTP_USER', ''),
            'smtp_password': os.environ.get('SMTP_PASSWORD', ''),
            'from_email': os.environ.get('PUBLISH_FROM_EMAIL', ''),
            'to_emails': [e.strip() for e in os.environ.get('PUBLISH_TO_EMAILS', '').split(',') if e.strip()],
            'output_dir': str(PROJECT_ROOT / 'output' / 'reports'),
            'notion_parent_page': os.environ.get('NOTION_PUBLISH_PAGE', ''),
        }

    def publish(self, report: Report, channels: List[str] = None) -> PublishResult:
        """
        Publish a report to specified channels.

        Args:
            report: The Report object to publish
            channels: List of channel names. Default: ['file']

        Returns:
            PublishResult with success/failure per channel
        """
        channels = channels or ['file']
        result = PublishResult(
            success=False,
            channels_attempted=channels,
        )

        for channel in channels:
            try:
                if channel == 'file':
                    paths = self._publish_file(report)
                    result.channels_succeeded.append('file')
                    result.file_paths.extend(paths)

                elif channel == 'email':
                    self._publish_email(report)
                    result.channels_succeeded.append('email')

                elif channel == 'notion':
                    self._publish_notion(report)
                    result.channels_succeeded.append('notion')

                elif channel == 'docx':
                    path = self._publish_docx(report)
                    result.channels_succeeded.append('docx')
                    result.file_paths.append(path)

                else:
                    result.channels_failed[channel] = f"Unknown channel: {channel}"

            except Exception as e:
                logger.error(f"Publish to {channel} failed: {e}", exc_info=True)
                result.channels_failed[channel] = str(e)

        result.success = len(result.channels_succeeded) > 0
        return result

    # ------------------------------------------------------------------
    # Channel: File
    # ------------------------------------------------------------------

    def _publish_file(self, report: Report) -> List[str]:
        """Save report as markdown and HTML files."""
        output_dir = Path(self.config['output_dir'])
        output_dir.mkdir(parents=True, exist_ok=True)

        timestamp = report.created_at.strftime('%Y%m%d_%H%M%S')
        slug = report.report_type.replace(' ', '_')
        base_name = f"{slug}_{timestamp}"

        paths = []

        # Markdown
        md_path = output_dir / f"{base_name}.md"
        md_content = self._format_markdown(report)
        md_path.write_text(md_content, encoding='utf-8')
        paths.append(str(md_path))
        logger.info(f"Published to file: {md_path}")

        # HTML
        try:
            from src.agents.publishing.formatters.html_formatter import format_report_html
            html_path = output_dir / f"{base_name}.html"
            html_content = format_report_html(report)
            html_path.write_text(html_content, encoding='utf-8')
            paths.append(str(html_path))
        except ImportError:
            logger.debug("HTML formatter not available, skipping HTML output")

        return paths

    def _format_markdown(self, report: Report) -> str:
        """Format report as markdown."""
        lines = [
            f"# {report.title}",
            f"*Generated: {report.created_at.strftime('%Y-%m-%d %H:%M UTC')}*\n",
        ]

        if report.metadata:
            meta = report.metadata
            if 'marketing_year' in meta:
                lines.append(f"**Marketing Year:** {meta['marketing_year']}\n")

        lines.append(report.narrative)

        if report.tables:
            lines.append("\n---\n")
            for table in report.tables:
                lines.append(table)
                lines.append("")

        if report.charts:
            lines.append("\n## Charts\n")
            for chart_path in report.charts:
                name = Path(chart_path).stem
                lines.append(f"![{name}]({chart_path})")

        return "\n".join(lines)

    # ------------------------------------------------------------------
    # Channel: Email
    # ------------------------------------------------------------------

    def _publish_email(self, report: Report):
        """Send report via SMTP email."""
        import smtplib
        from email.mime.multipart import MIMEMultipart
        from email.mime.text import MIMEText
        from email.mime.image import MIMEImage

        smtp_host = self.config['smtp_host']
        smtp_port = self.config['smtp_port']
        smtp_user = self.config['smtp_user']
        smtp_pass = self.config['smtp_password']
        from_email = self.config['from_email'] or smtp_user
        to_emails = self.config['to_emails']

        if not smtp_user or not to_emails:
            raise ValueError(
                "Email not configured. Set SMTP_USER, SMTP_PASSWORD, "
                "PUBLISH_FROM_EMAIL, and PUBLISH_TO_EMAILS in .env"
            )

        # Build email
        msg = MIMEMultipart('related')
        msg['Subject'] = report.title
        msg['From'] = from_email
        msg['To'] = ', '.join(to_emails)

        # HTML body
        try:
            from src.agents.publishing.formatters.html_formatter import format_report_html
            html_body = format_report_html(report)
        except ImportError:
            # Fallback: wrap markdown in basic HTML
            html_body = f"<html><body><pre>{report.narrative}</pre></body></html>"

        msg_alt = MIMEMultipart('alternative')
        msg_alt.attach(MIMEText(report.narrative, 'plain'))
        msg_alt.attach(MIMEText(html_body, 'html'))
        msg.attach(msg_alt)

        # Attach chart images
        for i, chart_path in enumerate(report.charts):
            try:
                with open(chart_path, 'rb') as f:
                    img = MIMEImage(f.read())
                    img.add_header('Content-ID', f'<chart{i}>')
                    img.add_header('Content-Disposition', 'inline',
                                   filename=Path(chart_path).name)
                    msg.attach(img)
            except FileNotFoundError:
                logger.warning(f"Chart not found: {chart_path}")

        # Send
        with smtplib.SMTP(smtp_host, smtp_port) as server:
            server.starttls()
            server.login(smtp_user, smtp_pass)
            server.sendmail(from_email, to_emails, msg.as_string())

        logger.info(f"Email sent to {len(to_emails)} recipients: {report.title}")

    # ------------------------------------------------------------------
    # Channel: Notion
    # ------------------------------------------------------------------

    def _publish_notion(self, report: Report):
        """Create a Notion page with the report content.

        Uses the Notion MCP tools if available, otherwise raises.
        """
        # This will be called via MCP Notion tools by Claude
        # For programmatic use, we save a file that can be imported
        raise NotImplementedError(
            "Notion publishing requires Claude + Notion MCP. "
            "Use generate_report MCP tool and ask Claude to publish to Notion."
        )

    # ------------------------------------------------------------------
    # Channel: DOCX
    # ------------------------------------------------------------------

    def _publish_docx(self, report: Report) -> str:
        """Generate a Word document."""
        from src.agents.publishing.formatters.docx_formatter import format_report_docx

        output_dir = Path(self.config['output_dir'])
        output_dir.mkdir(parents=True, exist_ok=True)

        timestamp = report.created_at.strftime('%Y%m%d_%H%M%S')
        slug = report.report_type.replace(' ', '_')
        docx_path = output_dir / f"{slug}_{timestamp}.docx"

        format_report_docx(report, str(docx_path))
        logger.info(f"Published DOCX: {docx_path}")
        return str(docx_path)
