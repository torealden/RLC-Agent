"""
Census Email Summary Agent
==========================
Agent for sending daily Census pipeline summary emails.

Uses Gmail OAuth to send formatted summary reports with:
1. Pipeline status (SUCCESS/PARTIAL/FAILED)
2. Record counts by layer
3. Error/warning list
4. Link to detailed logs

Reads summary from CensusLogReaderAgent output.
"""

import json
import os
import sys
from datetime import datetime, date
from pathlib import Path
from typing import Dict, List, Optional, Any

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT / 'src'))

from agents.base.census_base_agent import (
    CensusBaseAgent, PipelineLayer, EventType, CENSUS_LOGS_DIR
)

# Try to import email functionality
try:
    from utils.email_sender import send_email
    EMAIL_AVAILABLE = True
except ImportError:
    EMAIL_AVAILABLE = False


class CensusEmailSummaryAgent(CensusBaseAgent):
    """
    Agent for sending daily Census pipeline summary emails.

    Sends HTML-formatted email with pipeline status and statistics.
    """

    def __init__(
        self,
        recipient_email: str = None,
        sender_email: str = None,
        **kwargs
    ):
        super().__init__(agent_name='CensusEmailSummary', **kwargs)

        self.recipient_email = recipient_email or os.environ.get('ALERT_EMAIL')
        self.sender_email = sender_email or os.environ.get('GMAIL_USER')

        if not self.recipient_email:
            self.log_warning("No recipient email configured")

    def get_layer(self) -> PipelineLayer:
        return PipelineLayer.GOLD

    def run(
        self,
        target_date: date = None,
        force_send: bool = False
    ):
        """
        Send Census pipeline summary email.

        Args:
            target_date: Date of summary (default: today)
            force_send: Send even if no issues

        Returns:
            AgentResult with email status
        """
        target_date = target_date or date.today()

        self.log_event(
            EventType.INFO,
            f"Preparing email summary for {target_date}"
        )

        # Load daily summary
        summary = self._load_summary(target_date)

        if not summary:
            self.log_warning(f"No summary found for {target_date}")
            return self.complete()

        # Check if we should send
        pipeline_status = summary.get('pipeline_status', 'UNKNOWN')
        should_send = (
            force_send or
            pipeline_status in ('FAILED', 'WARNING') or
            len(summary.get('errors', [])) > 0
        )

        # Always send on the first of the month for a monthly report
        if target_date.day == 1:
            should_send = True

        if not should_send:
            self.log_event(
                EventType.INFO,
                "No issues detected, skipping email",
                data={'status': pipeline_status}
            )
            return self.complete()

        # Generate email content
        subject, body = self._generate_email(summary, target_date)

        # Send email
        if self.recipient_email:
            success = self._send_email(subject, body)

            if success:
                self.log_event(
                    EventType.INFO,
                    f"Email sent to {self.recipient_email}",
                    data={'subject': subject}
                )
                self.set_metadata('email_sent', True)
            else:
                self.log_error("Failed to send email")
                self.set_metadata('email_sent', False)
        else:
            self.log_warning("No recipient configured, email not sent")
            # Still save the email content for reference
            self._save_email_content(subject, body, target_date)

        self.set_metadata('summary_status', pipeline_status)
        self.set_metadata('subject', subject)

        return self.complete()

    def _load_summary(self, target_date: date) -> Optional[Dict]:
        """Load daily summary from file"""
        summary_path = CENSUS_LOGS_DIR / 'summaries' / f"daily_summary_{target_date}.json"

        if not summary_path.exists():
            return None

        with open(summary_path, 'r', encoding='utf-8') as f:
            return json.load(f)

    def _generate_email(
        self,
        summary: Dict,
        target_date: date
    ) -> tuple:
        """Generate email subject and HTML body"""
        status = summary.get('pipeline_status', 'UNKNOWN')

        # Status emoji and color
        status_info = {
            'SUCCESS': ('✅', 'green'),
            'WARNING': ('⚠️', 'orange'),
            'FAILED': ('❌', 'red'),
            'NO_DATA': ('📭', 'gray'),
            'UNKNOWN': ('❓', 'gray')
        }

        emoji, color = status_info.get(status, ('❓', 'gray'))

        subject = f"{emoji} Census Pipeline {status} - {target_date}"

        # Build HTML body
        records = summary.get('records', {})
        verifications = summary.get('verifications', {})
        errors = summary.get('errors', [])
        warnings = summary.get('warnings', [])
        agents = summary.get('agents', {})

        body = f"""
<!DOCTYPE html>
<html>
<head>
    <style>
        body {{ font-family: Arial, sans-serif; max-width: 600px; margin: 0 auto; }}
        .header {{ background-color: {color}; color: white; padding: 20px; text-align: center; }}
        .section {{ padding: 15px; border-bottom: 1px solid #eee; }}
        .metric {{ display: inline-block; margin: 10px 20px; text-align: center; }}
        .metric-value {{ font-size: 24px; font-weight: bold; }}
        .metric-label {{ font-size: 12px; color: #666; }}
        .error {{ color: red; margin: 5px 0; }}
        .warning {{ color: orange; margin: 5px 0; }}
        table {{ width: 100%; border-collapse: collapse; }}
        th, td {{ padding: 8px; text-align: left; border-bottom: 1px solid #ddd; }}
        th {{ background-color: #f5f5f5; }}
        .success {{ color: green; }}
        .failure {{ color: red; }}
    </style>
</head>
<body>
    <div class="header">
        <h1>{emoji} Census Pipeline {status}</h1>
        <p>{target_date}</p>
    </div>

    <div class="section">
        <h2>📊 Record Counts</h2>
        <div class="metric">
            <div class="metric-value">{records.get('bronze_collected', 0):,}</div>
            <div class="metric-label">Bronze Collected</div>
        </div>
        <div class="metric">
            <div class="metric-value">{records.get('silver_transformed', 0):,}</div>
            <div class="metric-label">Silver Transformed</div>
        </div>
        <div class="metric">
            <div class="metric-value">{records.get('gold_created', 0):,}</div>
            <div class="metric-label">Gold Views</div>
        </div>
    </div>

    <div class="section">
        <h2>✓ Verifications</h2>
        <div class="metric">
            <div class="metric-value success">{verifications.get('passed', 0)}</div>
            <div class="metric-label">Passed</div>
        </div>
        <div class="metric">
            <div class="metric-value failure">{verifications.get('failed', 0)}</div>
            <div class="metric-label">Failed</div>
        </div>
    </div>

    <div class="section">
        <h2>🤖 Agent Status</h2>
        <table>
            <tr><th>Agent</th><th>Events</th><th>Status</th></tr>
            {"".join(f'''<tr>
                <td>{name}</td>
                <td>{info.get('events', 0)}</td>
                <td class="{'success' if info.get('success') else 'failure'}">
                    {'✅' if info.get('success') else '❌' if info.get('success') is False else '—'}
                </td>
            </tr>''' for name, info in agents.items())}
        </table>
    </div>
"""

        if errors:
            body += f"""
    <div class="section">
        <h2>❌ Errors ({len(errors)})</h2>
        {"".join(f'<div class="error">• [{e.get("agent", "?")}] {e.get("message", "Unknown error")}</div>' for e in errors[:10])}
        {'<p><em>... and ' + str(len(errors) - 10) + ' more</em></p>' if len(errors) > 10 else ''}
    </div>
"""

        if warnings:
            body += f"""
    <div class="section">
        <h2>⚠️ Warnings ({len(warnings)})</h2>
        {"".join(f'<div class="warning">• [{w.get("agent", "?")}] {w.get("message", "Unknown warning")}</div>' for w in warnings[:10])}
        {'<p><em>... and ' + str(len(warnings) - 10) + ' more</em></p>' if len(warnings) > 10 else ''}
    </div>
"""

        body += f"""
    <div class="section" style="text-align: center; color: #666; font-size: 12px;">
        <p>Generated at {summary.get('generated_at', datetime.now().isoformat())}</p>
        <p>Logs: logs/census/</p>
    </div>
</body>
</html>
"""

        return subject, body

    def _send_email(self, subject: str, body: str) -> bool:
        """Send email using Gmail"""
        if not EMAIL_AVAILABLE:
            self.log_warning("Email functionality not available")
            return self._send_email_simple(subject, body)

        try:
            send_email(
                to=self.recipient_email,
                subject=subject,
                body=body,
                html=True
            )
            return True
        except Exception as e:
            self.log_error(f"Email send failed: {e}")
            return False

    def _send_email_simple(self, subject: str, body: str) -> bool:
        """Simple email fallback using smtplib"""
        import smtplib
        from email.mime.text import MIMEText
        from email.mime.multipart import MIMEMultipart

        gmail_user = os.environ.get('GMAIL_USER')
        gmail_password = os.environ.get('GMAIL_APP_PASSWORD')

        if not gmail_user or not gmail_password:
            self.log_warning("Gmail credentials not configured")
            return False

        try:
            msg = MIMEMultipart('alternative')
            msg['Subject'] = subject
            msg['From'] = gmail_user
            msg['To'] = self.recipient_email

            msg.attach(MIMEText(body, 'html'))

            with smtplib.SMTP_SSL('smtp.gmail.com', 465) as server:
                server.login(gmail_user, gmail_password)
                server.sendmail(gmail_user, self.recipient_email, msg.as_string())

            return True

        except Exception as e:
            self.log_error(f"Simple email failed: {e}")
            return False

    def _save_email_content(
        self,
        subject: str,
        body: str,
        target_date: date
    ):
        """Save email content to file for reference"""
        email_dir = CENSUS_LOGS_DIR / 'emails'
        email_dir.mkdir(parents=True, exist_ok=True)

        email_path = email_dir / f"email_{target_date}.html"

        with open(email_path, 'w', encoding='utf-8') as f:
            f.write(f"<!-- Subject: {subject} -->\n")
            f.write(body)

        self.log_event(
            EventType.DATA_SAVE,
            f"Saved email content to {email_path.name}"
        )


# =============================================================================
# CLI
# =============================================================================

def main():
    """CLI for Census Email Summary Agent"""
    import argparse
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    parser = argparse.ArgumentParser(description='Census Email Summary Agent')

    parser.add_argument(
        '--date',
        help='Target date (YYYY-MM-DD), defaults to today'
    )
    parser.add_argument(
        '--recipient',
        help='Override recipient email'
    )
    parser.add_argument(
        '--force',
        action='store_true',
        help='Force send even if no issues'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Generate email but do not send'
    )

    args = parser.parse_args()

    # Parse date
    target_date = None
    if args.date:
        target_date = datetime.strptime(args.date, '%Y-%m-%d').date()

    agent = CensusEmailSummaryAgent(recipient_email=args.recipient)

    if args.dry_run:
        # Just generate and save, don't send
        agent.recipient_email = None

    result = agent.run(target_date=target_date, force_send=args.force)

    print(f"\nResult: {'SUCCESS' if result.success else 'FAILED'}")
    print(f"Email sent: {result.metadata.get('email_sent', False)}")
    print(f"Status: {result.metadata.get('summary_status', 'N/A')}")


if __name__ == '__main__':
    main()
