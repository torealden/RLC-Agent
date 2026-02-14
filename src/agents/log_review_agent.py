#!/usr/bin/env python3
"""
HARVEST Log Review Agent
========================
Daily system health monitor that reviews logs and sends status reports.

Runs at 5:30 AM ET daily to:
1. Review all logs from the past 24 hours
2. Check for errors, warnings, and successes
3. Verify data freshness in the database
4. Generate a comprehensive status report
5. Send email summary to configured recipients

Usage:
    python src/agents/log_review_agent.py              # Run full review and send email
    python src/agents/log_review_agent.py --preview    # Preview report without sending
    python src/agents/log_review_agent.py --daemon     # Run as scheduled daemon
"""

import os
import sys
import re
import json
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Tuple, Optional
from dataclasses import dataclass, field
from collections import defaultdict
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

# Setup paths
SCRIPT_DIR = Path(__file__).parent
PROJECT_ROOT = SCRIPT_DIR.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(PROJECT_ROOT / "src"))

# Directories
LOG_DIR = PROJECT_ROOT / "output" / "logs"
REPORT_DIR = PROJECT_ROOT / "output" / "reports"

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('LogReviewAgent')

# =============================================================================
# CONFIGURATION
# =============================================================================

@dataclass
class LogReviewConfig:
    """Configuration for log review agent."""
    # Email settings
    smtp_server: str = "smtp.gmail.com"
    smtp_port: int = 587
    sender_email: str = ""
    sender_password: str = ""  # App password for Gmail
    recipients: List[str] = field(default_factory=lambda: [
        "tore.alden@roundlakescommodities.com"
    ])

    # Review settings
    lookback_hours: int = 24
    log_patterns: List[str] = field(default_factory=lambda: [
        "overnight_runner_*.log",
        "weather_collector.log",
        "weather_email_agent.log",
        "weather_email.log",
        "hb_report_*.log",
        "scheduler.log",
        "data_checker.log"
    ])

    # Thresholds
    error_threshold: int = 0  # Alert if any errors
    warning_threshold: int = 5  # Alert if more than 5 warnings

    # Database connection
    db_host: str = "localhost"
    db_port: str = "5432"
    db_name: str = "rlc_commodities"
    db_user: str = "postgres"
    db_password: str = "SoupBoss1"


# =============================================================================
# DATA CLASSES
# =============================================================================

@dataclass
class LogEntry:
    """Represents a parsed log entry."""
    timestamp: datetime
    level: str
    source: str
    message: str
    file: str


@dataclass
class LogSummary:
    """Summary of a single log file."""
    file_name: str
    total_lines: int
    errors: List[LogEntry] = field(default_factory=list)
    warnings: List[LogEntry] = field(default_factory=list)
    info_highlights: List[LogEntry] = field(default_factory=list)
    success_indicators: List[str] = field(default_factory=list)


@dataclass
class DataFreshness:
    """Data freshness for a database table."""
    schema: str
    table: str
    row_count: int
    latest_record: Optional[datetime]
    freshness_hours: Optional[float]
    status: str  # 'FRESH', 'STALE', 'EMPTY', 'ERROR'


@dataclass
class SystemStatus:
    """Overall system status."""
    review_time: datetime
    lookback_start: datetime
    log_summaries: List[LogSummary] = field(default_factory=list)
    data_freshness: List[DataFreshness] = field(default_factory=list)
    overall_status: str = "UNKNOWN"  # 'HEALTHY', 'WARNING', 'ERROR', 'CRITICAL'
    total_errors: int = 0
    total_warnings: int = 0


# =============================================================================
# LOG PARSING
# =============================================================================

class LogParser:
    """Parse various log file formats."""

    # Common log patterns
    PATTERNS = [
        # Standard Python logging: 2026-01-28 06:00:59,235 - OvernightRunner - INFO - message
        re.compile(r'^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},?\d*)\s*[-|]\s*(\w+)\s*[-|]\s*(\w+)\s*[-|]\s*(.*)$'),
        # Pipe format: 2026-01-27 05:53:32 | INFO | HB-ReportV2 | message
        re.compile(r'^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})\s*\|\s*(\w+)\s*\|\s*([\w-]+)\s*\|\s*(.*)$'),
        # Bracket format: [2026-01-19 08:08:28] message
        re.compile(r'^\[(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})\]\s*(.*)$'),
    ]

    # Keywords to identify log levels
    ERROR_KEYWORDS = ['ERROR', 'FAILED', 'CRITICAL', 'EXCEPTION', 'Error:', 'error:']
    WARNING_KEYWORDS = ['WARNING', 'WARN', 'Warning:', 'warning:']
    SUCCESS_KEYWORDS = ['SUCCESS', 'COMPLETE', 'successfully', 'Saved', 'Generated']

    @classmethod
    def parse_file(cls, file_path: Path, since: datetime) -> LogSummary:
        """Parse a log file and return summary."""
        summary = LogSummary(
            file_name=file_path.name,
            total_lines=0
        )

        if not file_path.exists():
            return summary

        try:
            with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                lines = f.readlines()
        except Exception as e:
            logger.error(f"Error reading {file_path}: {e}")
            return summary

        summary.total_lines = len(lines)

        for line in lines:
            line = line.strip()
            if not line:
                continue

            entry = cls._parse_line(line, file_path.name)
            if not entry:
                continue

            # Filter by time
            if entry.timestamp < since:
                continue

            # Categorize
            if entry.level == 'ERROR' or any(kw in line for kw in cls.ERROR_KEYWORDS):
                summary.errors.append(entry)
            elif entry.level == 'WARNING' or any(kw in line for kw in cls.WARNING_KEYWORDS):
                summary.warnings.append(entry)
            elif any(kw in line for kw in cls.SUCCESS_KEYWORDS):
                summary.success_indicators.append(line[:200])

        return summary

    @classmethod
    def _parse_line(cls, line: str, file_name: str) -> Optional[LogEntry]:
        """Parse a single log line."""
        for pattern in cls.PATTERNS:
            match = pattern.match(line)
            if match:
                groups = match.groups()

                # Parse timestamp
                try:
                    ts_str = groups[0].replace(',', '.')
                    if '.' in ts_str:
                        timestamp = datetime.strptime(ts_str[:23], '%Y-%m-%d %H:%M:%S.%f')
                    else:
                        timestamp = datetime.strptime(ts_str[:19], '%Y-%m-%d %H:%M:%S')
                except:
                    timestamp = datetime.now()

                # Handle different formats
                if len(groups) == 4:
                    # Full format with level and source
                    return LogEntry(
                        timestamp=timestamp,
                        level=groups[2].upper() if groups[1].isalpha() and len(groups[1]) < 20 else groups[1].upper(),
                        source=groups[1] if len(groups[1]) < 30 else 'unknown',
                        message=groups[3],
                        file=file_name
                    )
                elif len(groups) == 2:
                    # Simple bracket format
                    level = 'INFO'
                    message = groups[1]
                    for kw in cls.ERROR_KEYWORDS:
                        if kw.lower() in message.lower():
                            level = 'ERROR'
                            break
                    for kw in cls.WARNING_KEYWORDS:
                        if kw.lower() in message.lower():
                            level = 'WARNING'
                            break
                    return LogEntry(
                        timestamp=timestamp,
                        level=level,
                        source='unknown',
                        message=message,
                        file=file_name
                    )

        return None


# =============================================================================
# DATABASE CHECKER
# =============================================================================

class DatabaseChecker:
    """Check database table freshness."""

    # Tables to monitor with their timestamp columns
    TABLES_TO_MONITOR = [
        # Weather
        ('bronze', 'weather_raw', 'collected_at'),
        ('bronze', 'weather_email_extract', 'collected_at'),
        ('bronze', 'weather_alerts_raw', 'collected_at'),
        ('silver', 'weather_observation', 'updated_at'),
        # USDA NASS
        ('bronze', 'nass_crop_progress', 'collected_at'),
        ('bronze', 'nass_crop_condition', 'collected_at'),
        ('bronze', 'nass_production', 'collected_at'),
        # Trade & Positioning
        ('bronze', 'census_trade', 'collected_at'),
        ('bronze', 'cftc_cot', 'collected_at'),
        # EIA Energy
        ('bronze', 'eia_raw_ingestion', 'ingestion_ts'),
        ('silver', 'eia_petroleum_weekly', 'updated_ts'),
        # Futures
        ('bronze', 'futures_daily_settlement', 'collected_at'),
        # Brazil
        ('bronze', 'conab_production', 'created_at'),
    ]

    # Freshness thresholds (hours)
    FRESHNESS_THRESHOLDS = {
        'weather_raw': 6,  # 6 hours
        'weather_observation': 6,
        'weather_alerts_raw': 48,  # 2 days (may not always have alerts)
        'weather_email_extract': 24,
        'futures_daily_settlement': 24,
        'nass_crop_progress': 168,  # Weekly
        'nass_crop_condition': 168,
        'nass_production': 720,  # Monthly
        'census_trade': 720,  # Monthly
        'cftc_cot': 168,  # Weekly
        'eia_raw_ingestion': 168,  # Weekly
        'eia_petroleum_weekly': 168,
        'conab_production': 720,  # Monthly
    }

    def __init__(self, config: LogReviewConfig):
        self.config = config
        self.conn = None

    def connect(self):
        """Connect to database."""
        try:
            import psycopg2
            self.conn = psycopg2.connect(
                host=self.config.db_host,
                port=self.config.db_port,
                database=self.config.db_name,
                user=self.config.db_user,
                password=self.config.db_password
            )
            return True
        except Exception as e:
            logger.error(f"Database connection failed: {e}")
            return False

    def close(self):
        """Close database connection."""
        if self.conn:
            self.conn.close()

    def check_all_tables(self) -> List[DataFreshness]:
        """Check freshness of all monitored tables."""
        results = []

        if not self.conn:
            if not self.connect():
                return results

        cur = self.conn.cursor()

        for schema, table, ts_col in self.TABLES_TO_MONITOR:
            freshness = self._check_table(cur, schema, table, ts_col)
            results.append(freshness)

        cur.close()
        return results

    def _check_table(self, cur, schema: str, table: str, ts_col: str) -> DataFreshness:
        """Check a single table's freshness."""
        try:
            # Check if table exists
            cur.execute(f"""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables
                    WHERE table_schema = %s AND table_name = %s
                )
            """, (schema, table))

            if not cur.fetchone()[0]:
                return DataFreshness(
                    schema=schema,
                    table=table,
                    row_count=0,
                    latest_record=None,
                    freshness_hours=None,
                    status='MISSING'
                )

            # Get row count and latest timestamp
            cur.execute(f"""
                SELECT COUNT(*), MAX({ts_col})
                FROM {schema}.{table}
            """)

            row_count, latest_ts = cur.fetchone()

            if row_count == 0 or latest_ts is None:
                return DataFreshness(
                    schema=schema,
                    table=table,
                    row_count=row_count or 0,
                    latest_record=None,
                    freshness_hours=None,
                    status='EMPTY'
                )

            # Calculate freshness
            now = datetime.now()
            if latest_ts.tzinfo:
                latest_ts = latest_ts.replace(tzinfo=None)
            freshness_hours = (now - latest_ts).total_seconds() / 3600

            # Determine status
            threshold = self.FRESHNESS_THRESHOLDS.get(table, 24)
            if freshness_hours <= threshold:
                status = 'FRESH'
            elif freshness_hours <= threshold * 2:
                status = 'STALE'
            else:
                status = 'VERY_STALE'

            return DataFreshness(
                schema=schema,
                table=table,
                row_count=row_count,
                latest_record=latest_ts,
                freshness_hours=round(freshness_hours, 1),
                status=status
            )

        except Exception as e:
            logger.error(f"Error checking {schema}.{table}: {e}")
            # Rollback to clear the aborted transaction state
            try:
                self.conn.rollback()
            except:
                pass
            return DataFreshness(
                schema=schema,
                table=table,
                row_count=0,
                latest_record=None,
                freshness_hours=None,
                status='ERROR'
            )


# =============================================================================
# REPORT GENERATOR
# =============================================================================

class ReportGenerator:
    """Generate status reports."""

    @staticmethod
    def generate_text_report(status: SystemStatus) -> str:
        """Generate a plain text status report."""
        lines = []

        # Header
        lines.append("=" * 70)
        lines.append("HARVEST DAILY SYSTEM STATUS REPORT")
        lines.append("=" * 70)
        lines.append(f"Report Time: {status.review_time.strftime('%Y-%m-%d %H:%M:%S')}")
        lines.append(f"Review Period: {status.lookback_start.strftime('%Y-%m-%d %H:%M')} to now")
        lines.append(f"Overall Status: {status.overall_status}")
        lines.append("")

        # Summary
        lines.append("-" * 70)
        lines.append("SUMMARY")
        lines.append("-" * 70)
        lines.append(f"Total Errors: {status.total_errors}")
        lines.append(f"Total Warnings: {status.total_warnings}")
        lines.append(f"Log Files Reviewed: {len(status.log_summaries)}")
        lines.append(f"Database Tables Checked: {len(status.data_freshness)}")
        lines.append("")

        # Log file summaries
        lines.append("-" * 70)
        lines.append("LOG FILE ANALYSIS")
        lines.append("-" * 70)

        for summary in status.log_summaries:
            status_icon = "✅" if not summary.errors else "❌"
            lines.append(f"\n{status_icon} {summary.file_name}")
            lines.append(f"   Lines: {summary.total_lines} | Errors: {len(summary.errors)} | Warnings: {len(summary.warnings)}")

            # Show errors
            if summary.errors:
                lines.append("   ERRORS:")
                for err in summary.errors[:5]:  # Limit to 5
                    lines.append(f"      [{err.timestamp.strftime('%H:%M:%S')}] {err.message[:80]}")
                if len(summary.errors) > 5:
                    lines.append(f"      ... and {len(summary.errors) - 5} more errors")

            # Show success indicators
            if summary.success_indicators:
                lines.append("   SUCCESSES:")
                for success in summary.success_indicators[:3]:
                    lines.append(f"      ✓ {success[:70]}")

        lines.append("")

        # Data freshness
        lines.append("-" * 70)
        lines.append("DATABASE FRESHNESS")
        lines.append("-" * 70)

        for df in status.data_freshness:
            if df.status == 'FRESH':
                icon = "🟢"
            elif df.status == 'STALE':
                icon = "🟡"
            elif df.status in ['VERY_STALE', 'EMPTY']:
                icon = "🔴"
            else:
                icon = "⚪"

            freshness_str = f"{df.freshness_hours}h ago" if df.freshness_hours else "N/A"
            lines.append(f"{icon} {df.schema}.{df.table}")
            lines.append(f"   Rows: {df.row_count:,} | Last Update: {freshness_str} | Status: {df.status}")

        lines.append("")

        # Footer
        lines.append("-" * 70)
        lines.append("END OF REPORT")
        lines.append("-" * 70)

        return "\n".join(lines)

    @staticmethod
    def generate_html_report(status: SystemStatus) -> str:
        """Generate an HTML status report for email."""

        # Status colors
        status_colors = {
            'HEALTHY': '#28a745',
            'WARNING': '#ffc107',
            'ERROR': '#dc3545',
            'CRITICAL': '#dc3545',
            'FRESH': '#28a745',
            'STALE': '#ffc107',
            'VERY_STALE': '#dc3545',
            'EMPTY': '#6c757d',
            'MISSING': '#6c757d',
            'ERROR': '#dc3545'
        }

        html = f"""
<!DOCTYPE html>
<html>
<head>
    <style>
        body {{ font-family: Arial, sans-serif; margin: 20px; background: #f5f5f5; }}
        .container {{ max-width: 800px; margin: 0 auto; background: white; padding: 20px; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }}
        h1 {{ color: #1f4e79; border-bottom: 2px solid #1f4e79; padding-bottom: 10px; }}
        h2 {{ color: #2e75b6; margin-top: 30px; }}
        .status-badge {{ display: inline-block; padding: 5px 15px; border-radius: 20px; color: white; font-weight: bold; }}
        .summary-grid {{ display: grid; grid-template-columns: repeat(4, 1fr); gap: 15px; margin: 20px 0; }}
        .summary-card {{ background: #f8f9fa; padding: 15px; border-radius: 8px; text-align: center; }}
        .summary-card .number {{ font-size: 32px; font-weight: bold; color: #1f4e79; }}
        .summary-card .label {{ color: #6c757d; font-size: 12px; text-transform: uppercase; }}
        table {{ width: 100%; border-collapse: collapse; margin: 15px 0; }}
        th, td {{ padding: 10px; text-align: left; border-bottom: 1px solid #dee2e6; }}
        th {{ background: #f8f9fa; font-weight: 600; }}
        .error {{ color: #dc3545; }}
        .warning {{ color: #ffc107; }}
        .success {{ color: #28a745; }}
        .status-dot {{ display: inline-block; width: 12px; height: 12px; border-radius: 50%; margin-right: 8px; }}
        .footer {{ margin-top: 30px; padding-top: 20px; border-top: 1px solid #dee2e6; color: #6c757d; font-size: 12px; }}
    </style>
</head>
<body>
    <div class="container">
        <h1>🌾 HARVEST Daily Status Report</h1>

        <p><strong>Report Time:</strong> {status.review_time.strftime('%Y-%m-%d %H:%M:%S')}</p>
        <p><strong>Review Period:</strong> {status.lookback_start.strftime('%Y-%m-%d %H:%M')} to now</p>
        <p><strong>Overall Status:</strong>
            <span class="status-badge" style="background: {status_colors.get(status.overall_status, '#6c757d')}">
                {status.overall_status}
            </span>
        </p>

        <div class="summary-grid">
            <div class="summary-card">
                <div class="number" style="color: {'#dc3545' if status.total_errors > 0 else '#28a745'}">{status.total_errors}</div>
                <div class="label">Errors</div>
            </div>
            <div class="summary-card">
                <div class="number" style="color: {'#ffc107' if status.total_warnings > 5 else '#28a745'}">{status.total_warnings}</div>
                <div class="label">Warnings</div>
            </div>
            <div class="summary-card">
                <div class="number">{len(status.log_summaries)}</div>
                <div class="label">Logs Reviewed</div>
            </div>
            <div class="summary-card">
                <div class="number">{len([d for d in status.data_freshness if d.status == 'FRESH'])}/{len(status.data_freshness)}</div>
                <div class="label">Data Fresh</div>
            </div>
        </div>

        <h2>📋 Log File Analysis</h2>
        <table>
            <tr>
                <th>Log File</th>
                <th>Lines</th>
                <th>Errors</th>
                <th>Warnings</th>
                <th>Status</th>
            </tr>
"""

        for summary in status.log_summaries:
            status_icon = "✅" if not summary.errors else "❌"
            row_class = "error" if summary.errors else ""
            html += f"""
            <tr class="{row_class}">
                <td>{summary.file_name}</td>
                <td>{summary.total_lines:,}</td>
                <td class="{'error' if summary.errors else ''}">{len(summary.errors)}</td>
                <td class="{'warning' if summary.warnings else ''}">{len(summary.warnings)}</td>
                <td>{status_icon}</td>
            </tr>
"""

        html += """
        </table>

        <h2>🗄️ Database Freshness</h2>
        <table>
            <tr>
                <th>Table</th>
                <th>Rows</th>
                <th>Last Update</th>
                <th>Status</th>
            </tr>
"""

        for df in status.data_freshness:
            freshness_str = f"{df.freshness_hours}h ago" if df.freshness_hours else "N/A"
            color = status_colors.get(df.status, '#6c757d')
            html += f"""
            <tr>
                <td>{df.schema}.{df.table}</td>
                <td>{df.row_count:,}</td>
                <td>{freshness_str}</td>
                <td><span class="status-dot" style="background: {color}"></span>{df.status}</td>
            </tr>
"""

        # Add errors section if any
        all_errors = []
        for summary in status.log_summaries:
            all_errors.extend(summary.errors)

        if all_errors:
            html += """
        </table>

        <h2>❌ Errors (Last 24h)</h2>
        <table>
            <tr>
                <th>Time</th>
                <th>Source</th>
                <th>Message</th>
            </tr>
"""
            for err in all_errors[:10]:
                html += f"""
            <tr class="error">
                <td>{err.timestamp.strftime('%H:%M:%S')}</td>
                <td>{err.file}</td>
                <td>{err.message[:100]}{'...' if len(err.message) > 100 else ''}</td>
            </tr>
"""

        html += f"""
        </table>

        <div class="footer">
            <p>Generated by HARVEST Log Review Agent</p>
            <p>Project: RLC-Agent | Location: C:\\Users\\torem\\RLC Dropbox\\RLC Team Folder\\RLC-Agent</p>
        </div>
    </div>
</body>
</html>
"""
        return html


# =============================================================================
# EMAIL SENDER
# =============================================================================

class EmailSender:
    """Send status reports via email using Gmail API."""

    def __init__(self, config: LogReviewConfig):
        self.config = config
        self.service = None

    def _get_gmail_service(self):
        """Get Gmail API service using existing OAuth credentials."""
        if self.service:
            return self.service

        try:
            from google.oauth2.credentials import Credentials
            from google.auth.transport.requests import Request
            from googleapiclient.discovery import build

            # Use the same credentials location as weather_email_agent
            CREDENTIALS_DIR = Path(r"C:\Users\torem\RLC Dropbox\RLC Team Folder\RLC Documents\LLM Model and Documents\Projects\Desktop Assistant")
            token_path = CREDENTIALS_DIR / "token_work.json"

            if not token_path.exists():
                logger.error(f"Gmail token not found: {token_path}")
                return None

            creds = Credentials.from_authorized_user_file(str(token_path))

            if creds.expired and creds.refresh_token:
                logger.info("Refreshing expired Gmail token...")
                creds.refresh(Request())
                with open(token_path, 'w') as f:
                    f.write(creds.to_json())

            self.service = build('gmail', 'v1', credentials=creds)
            return self.service

        except Exception as e:
            logger.error(f"Failed to get Gmail service: {e}")
            return None

    def send_report(self, status: SystemStatus, text_report: str, html_report: str) -> bool:
        """Send the status report via email using Gmail API."""

        service = self._get_gmail_service()
        if not service:
            logger.warning("Gmail service not available, skipping email send")
            return False

        try:
            import base64

            # Create message
            msg = MIMEMultipart('alternative')
            msg['Subject'] = f"HARVEST Status: {status.overall_status} - {status.review_time.strftime('%Y-%m-%d')}"
            msg['To'] = ", ".join(self.config.recipients)

            # Attach both text and HTML versions
            msg.attach(MIMEText(text_report, 'plain', 'utf-8'))
            msg.attach(MIMEText(html_report, 'html', 'utf-8'))

            # Encode the message
            raw_message = base64.urlsafe_b64encode(msg.as_bytes()).decode('utf-8')

            # Send via Gmail API
            service.users().messages().send(
                userId='me',
                body={'raw': raw_message}
            ).execute()

            logger.info(f"Status report sent to {self.config.recipients}")
            return True

        except Exception as e:
            logger.error(f"Failed to send email: {e}")
            return False


# =============================================================================
# MAIN AGENT
# =============================================================================

class LogReviewAgent:
    """Main log review agent."""

    def __init__(self, config: Optional[LogReviewConfig] = None):
        self.config = config or LogReviewConfig()
        self.db_checker = DatabaseChecker(self.config)
        self.email_sender = EmailSender(self.config)

    def run_review(self) -> SystemStatus:
        """Run a full system review."""
        logger.info("Starting log review...")

        now = datetime.now()
        lookback_start = now - timedelta(hours=self.config.lookback_hours)

        status = SystemStatus(
            review_time=now,
            lookback_start=lookback_start
        )

        # Review log files
        status.log_summaries = self._review_logs(lookback_start)

        # Check database freshness
        status.data_freshness = self.db_checker.check_all_tables()
        self.db_checker.close()

        # Calculate totals
        for summary in status.log_summaries:
            status.total_errors += len(summary.errors)
            status.total_warnings += len(summary.warnings)

        # Determine overall status
        status.overall_status = self._determine_overall_status(status)

        logger.info(f"Review complete. Status: {status.overall_status}")
        return status

    def _review_logs(self, since: datetime) -> List[LogSummary]:
        """Review all configured log files."""
        summaries = []

        if not LOG_DIR.exists():
            logger.warning(f"Log directory not found: {LOG_DIR}")
            return summaries

        # Find matching log files
        for pattern in self.config.log_patterns:
            for log_file in LOG_DIR.glob(pattern):
                # Only review recent files
                if log_file.stat().st_mtime > since.timestamp():
                    summary = LogParser.parse_file(log_file, since)
                    summaries.append(summary)
                    logger.info(f"Reviewed {log_file.name}: {len(summary.errors)} errors, {len(summary.warnings)} warnings")

        return summaries

    def _determine_overall_status(self, status: SystemStatus) -> str:
        """Determine overall system status."""
        # Check for critical issues
        critical_tables = ['weather_raw', 'futures_quotes']
        for df in status.data_freshness:
            if df.table in critical_tables and df.status in ['VERY_STALE', 'EMPTY', 'ERROR']:
                return 'CRITICAL'

        # Check error count
        if status.total_errors > 10:
            return 'ERROR'
        elif status.total_errors > 0:
            return 'WARNING'

        # Check stale data
        stale_count = sum(1 for df in status.data_freshness if df.status in ['STALE', 'VERY_STALE'])
        if stale_count > len(status.data_freshness) / 2:
            return 'WARNING'

        return 'HEALTHY'

    def generate_and_send_report(self, preview_only: bool = False) -> bool:
        """Run review, generate report, and optionally send it."""
        status = self.run_review()

        # Generate reports
        text_report = ReportGenerator.generate_text_report(status)
        html_report = ReportGenerator.generate_html_report(status)

        # Save reports locally
        report_date = status.review_time.strftime('%Y%m%d_%H%M%S')
        text_file = REPORT_DIR / f"system_status_{report_date}.txt"
        html_file = REPORT_DIR / f"system_status_{report_date}.html"

        REPORT_DIR.mkdir(parents=True, exist_ok=True)

        with open(text_file, 'w', encoding='utf-8') as f:
            f.write(text_report)
        with open(html_file, 'w', encoding='utf-8') as f:
            f.write(html_report)

        logger.info(f"Reports saved to {text_file}")

        if preview_only:
            # Handle Windows console encoding issues
            try:
                print("\n" + text_report)
            except UnicodeEncodeError:
                # Fallback: replace emojis for console output
                safe_report = text_report.encode('ascii', 'replace').decode('ascii')
                print("\n" + safe_report)
            return True

        # Send email
        return self.email_sender.send_report(status, text_report, html_report)


# =============================================================================
# DAEMON MODE
# =============================================================================

def run_daemon():
    """Run as a scheduled daemon."""
    import schedule

    agent = LogReviewAgent()

    # Schedule daily review at 5:30 AM
    schedule.every().day.at("05:30").do(agent.generate_and_send_report)

    logger.info("Log Review Agent daemon started")
    logger.info("Scheduled: Daily review at 05:30 AM")

    # Run initial review
    logger.info("Running initial review...")
    agent.generate_and_send_report()

    # Keep running
    while True:
        schedule.run_pending()
        import time
        time.sleep(60)


# =============================================================================
# CLI
# =============================================================================

def main():
    import argparse

    parser = argparse.ArgumentParser(description="HARVEST Log Review Agent")
    parser.add_argument('--preview', action='store_true', help="Preview report without sending email")
    parser.add_argument('--daemon', action='store_true', help="Run as scheduled daemon")
    parser.add_argument('--hours', type=int, default=24, help="Hours to look back (default: 24)")

    args = parser.parse_args()

    config = LogReviewConfig(lookback_hours=args.hours)
    agent = LogReviewAgent(config)

    if args.daemon:
        run_daemon()
    else:
        success = agent.generate_and_send_report(preview_only=args.preview)
        sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
