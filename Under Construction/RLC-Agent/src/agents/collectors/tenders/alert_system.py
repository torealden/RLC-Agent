"""
Wheat Tender Alert System

Handles notifications for wheat tender events via:
- Email (SendGrid)
- Slack (Webhooks)
- SMS (Twilio)

Configuration is stored in the database (tender_alert_config table)
or can be provided via environment variables.

Required Environment Variables:
- SENDGRID_API_KEY: For email notifications
- TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN, TWILIO_FROM_NUMBER: For SMS
- SLACK_WEBHOOK_URL: For Slack notifications
"""

import os
import logging
import json
from datetime import datetime
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, field
from enum import Enum

try:
    import requests
    REQUESTS_AVAILABLE = True
except ImportError:
    REQUESTS_AVAILABLE = False

logger = logging.getLogger(__name__)


# =============================================================================
# CONFIGURATION
# =============================================================================

class AlertChannel(Enum):
    """Available notification channels"""
    EMAIL = "email"
    SLACK = "slack"
    SMS = "sms"


@dataclass
class AlertConfig:
    """Configuration for a single alert rule"""
    name: str
    description: str = ""

    # Trigger conditions
    country_codes: List[str] = field(default_factory=list)  # Empty = all countries
    agency_codes: List[str] = field(default_factory=list)   # Empty = all agencies
    volume_threshold_mt: Optional[float] = None              # Min volume to trigger

    # Notification channels
    notify_email: bool = False
    notify_slack: bool = False
    notify_sms: bool = False

    # Recipients
    email_recipients: List[str] = field(default_factory=list)
    slack_channels: List[str] = field(default_factory=list)
    sms_recipients: List[str] = field(default_factory=list)

    is_active: bool = True


@dataclass
class AlertMessage:
    """A single alert message to send"""
    alert_name: str
    tender_id: Optional[int] = None
    subject: str = ""
    body: str = ""
    html_body: str = ""
    data: Dict[str, Any] = field(default_factory=dict)
    priority: str = "normal"  # low, normal, high, urgent


# =============================================================================
# NOTIFICATION SENDERS
# =============================================================================

class EmailSender:
    """
    Send email notifications via SendGrid.

    Requires:
    - SENDGRID_API_KEY environment variable
    - sendgrid Python package (optional, uses raw API if not installed)

    Registration: https://sendgrid.com/
    """

    def __init__(self, api_key: str = None, from_email: str = None):
        self.api_key = api_key or os.environ.get('SENDGRID_API_KEY')
        self.from_email = from_email or os.environ.get(
            'SENDGRID_FROM_EMAIL', 'alerts@rlc-commodities.com'
        )
        self.api_url = "https://api.sendgrid.com/v3/mail/send"
        self.logger = logging.getLogger(f"{self.__class__.__name__}")

    def send(self, to_emails: List[str], message: AlertMessage) -> bool:
        """
        Send email notification.

        Args:
            to_emails: List of recipient email addresses
            message: AlertMessage to send

        Returns:
            True if successful, False otherwise
        """
        if not self.api_key:
            self.logger.warning("SendGrid API key not configured")
            return False

        if not to_emails:
            self.logger.warning("No email recipients specified")
            return False

        try:
            payload = {
                "personalizations": [
                    {"to": [{"email": email} for email in to_emails]}
                ],
                "from": {"email": self.from_email, "name": "RLC Tender Alerts"},
                "subject": message.subject or "Wheat Tender Alert",
                "content": []
            }

            # Add plain text content
            if message.body:
                payload["content"].append({
                    "type": "text/plain",
                    "value": message.body
                })

            # Add HTML content
            if message.html_body:
                payload["content"].append({
                    "type": "text/html",
                    "value": message.html_body
                })
            elif message.body:
                # Convert plain text to simple HTML
                payload["content"].append({
                    "type": "text/html",
                    "value": f"<pre>{message.body}</pre>"
                })

            response = requests.post(
                self.api_url,
                headers={
                    "Authorization": f"Bearer {self.api_key}",
                    "Content-Type": "application/json"
                },
                json=payload,
                timeout=30
            )

            if response.status_code in (200, 201, 202):
                self.logger.info(f"Email sent to {len(to_emails)} recipients")
                return True
            else:
                self.logger.error(
                    f"SendGrid error: {response.status_code} - {response.text}"
                )
                return False

        except Exception as e:
            self.logger.error(f"Failed to send email: {e}")
            return False


class SlackSender:
    """
    Send notifications to Slack via webhooks.

    Requires:
    - SLACK_WEBHOOK_URL environment variable or explicit webhook URL

    Setup: https://api.slack.com/messaging/webhooks
    """

    def __init__(self, webhook_url: str = None):
        self.webhook_url = webhook_url or os.environ.get('SLACK_WEBHOOK_URL')
        self.logger = logging.getLogger(f"{self.__class__.__name__}")

    def send(self, channels: List[str], message: AlertMessage) -> bool:
        """
        Send Slack notification.

        Args:
            channels: List of channel webhooks or names (ignored if using single webhook)
            message: AlertMessage to send

        Returns:
            True if successful, False otherwise
        """
        if not self.webhook_url:
            self.logger.warning("Slack webhook URL not configured")
            return False

        try:
            # Build Slack message with rich formatting
            blocks = self._build_slack_blocks(message)

            payload = {
                "text": message.subject or "Wheat Tender Alert",
                "blocks": blocks
            }

            response = requests.post(
                self.webhook_url,
                json=payload,
                timeout=30
            )

            if response.status_code == 200:
                self.logger.info("Slack notification sent")
                return True
            else:
                self.logger.error(
                    f"Slack error: {response.status_code} - {response.text}"
                )
                return False

        except Exception as e:
            self.logger.error(f"Failed to send Slack notification: {e}")
            return False

    def _build_slack_blocks(self, message: AlertMessage) -> List[Dict]:
        """Build Slack block kit message"""
        blocks = []

        # Header
        if message.subject:
            blocks.append({
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": f":wheat: {message.subject}",
                    "emoji": True
                }
            })

        # Main content
        if message.body:
            blocks.append({
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": message.body
                }
            })

        # Data fields
        if message.data:
            fields = []
            for key, value in message.data.items():
                if value is not None:
                    fields.append({
                        "type": "mrkdwn",
                        "text": f"*{key}:*\n{value}"
                    })

            # Slack limits to 10 fields per section
            for i in range(0, len(fields), 10):
                blocks.append({
                    "type": "section",
                    "fields": fields[i:i+10]
                })

        # Footer with timestamp
        blocks.append({
            "type": "context",
            "elements": [{
                "type": "mrkdwn",
                "text": f"Alert: {message.alert_name} | {datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')}"
            }]
        })

        return blocks


class SMSSender:
    """
    Send SMS notifications via Twilio.

    Requires:
    - TWILIO_ACCOUNT_SID
    - TWILIO_AUTH_TOKEN
    - TWILIO_FROM_NUMBER

    Registration: https://www.twilio.com/
    """

    def __init__(
        self,
        account_sid: str = None,
        auth_token: str = None,
        from_number: str = None
    ):
        self.account_sid = account_sid or os.environ.get('TWILIO_ACCOUNT_SID')
        self.auth_token = auth_token or os.environ.get('TWILIO_AUTH_TOKEN')
        self.from_number = from_number or os.environ.get('TWILIO_FROM_NUMBER')
        self.logger = logging.getLogger(f"{self.__class__.__name__}")

    def send(self, to_numbers: List[str], message: AlertMessage) -> bool:
        """
        Send SMS notification.

        Args:
            to_numbers: List of phone numbers (E.164 format)
            message: AlertMessage to send

        Returns:
            True if all successful, False otherwise
        """
        if not all([self.account_sid, self.auth_token, self.from_number]):
            self.logger.warning("Twilio credentials not configured")
            return False

        if not to_numbers:
            self.logger.warning("No SMS recipients specified")
            return False

        # SMS has 160 char limit - create concise message
        sms_text = self._format_sms(message)

        success = True
        for number in to_numbers:
            try:
                url = f"https://api.twilio.com/2010-04-01/Accounts/{self.account_sid}/Messages.json"

                response = requests.post(
                    url,
                    auth=(self.account_sid, self.auth_token),
                    data={
                        "From": self.from_number,
                        "To": number,
                        "Body": sms_text
                    },
                    timeout=30
                )

                if response.status_code in (200, 201):
                    self.logger.info(f"SMS sent to {number}")
                else:
                    self.logger.error(
                        f"Twilio error for {number}: {response.status_code}"
                    )
                    success = False

            except Exception as e:
                self.logger.error(f"Failed to send SMS to {number}: {e}")
                success = False

        return success

    def _format_sms(self, message: AlertMessage) -> str:
        """Format message for SMS (160 char limit)"""
        # Build concise SMS
        parts = [message.subject or "Tender Alert"]

        if message.data:
            if message.data.get('country'):
                parts.append(message.data['country'])
            if message.data.get('volume_mt'):
                parts.append(f"{message.data['volume_mt']:,.0f}MT")
            if message.data.get('price_usd_mt'):
                parts.append(f"${message.data['price_usd_mt']:.2f}/MT")

        text = " | ".join(parts)

        # Truncate if too long
        if len(text) > 155:
            text = text[:152] + "..."

        return text


# =============================================================================
# ALERT MANAGER
# =============================================================================

class TenderAlertManager:
    """
    Manages tender alert rules and sends notifications.

    Loads configuration from database or config file.
    Processes tender events and triggers appropriate alerts.
    """

    # Default alert configurations
    DEFAULT_ALERTS = [
        AlertConfig(
            name="egypt_tender",
            description="Egypt wheat tender alerts",
            country_codes=["EG"],
            volume_threshold_mt=50000,
            notify_email=True,
            notify_slack=True,
        ),
        AlertConfig(
            name="algeria_tender",
            description="Algeria wheat tender alerts",
            country_codes=["DZ"],
            volume_threshold_mt=200000,
            notify_email=True,
        ),
        AlertConfig(
            name="large_tender",
            description="Any tender over 500K MT",
            volume_threshold_mt=500000,
            notify_email=True,
            notify_slack=True,
            notify_sms=True,
        ),
        AlertConfig(
            name="all_tenders",
            description="All wheat tender activity",
            notify_email=True,
        ),
    ]

    def __init__(
        self,
        configs: List[AlertConfig] = None,
        db_connection = None
    ):
        self.configs = configs or self.DEFAULT_ALERTS
        self.db_connection = db_connection
        self.logger = logging.getLogger(f"{self.__class__.__name__}")

        # Initialize senders
        self.email_sender = EmailSender()
        self.slack_sender = SlackSender()
        self.sms_sender = SMSSender()

        # Alert history (in-memory, should use DB in production)
        self.alert_history: List[Dict] = []

    def load_configs_from_db(self):
        """Load alert configurations from database"""
        if not self.db_connection:
            self.logger.warning("No database connection for loading configs")
            return

        try:
            # Would query tender_alert_config table
            # For now, use defaults
            pass
        except Exception as e:
            self.logger.error(f"Failed to load configs from DB: {e}")

    def process_tender(self, tender_data: Dict) -> List[str]:
        """
        Process a tender and send alerts if conditions are met.

        Args:
            tender_data: Dictionary with tender information

        Returns:
            List of alert names that were triggered
        """
        triggered_alerts = []

        for config in self.configs:
            if not config.is_active:
                continue

            if self._should_trigger(config, tender_data):
                message = self._build_message(config, tender_data)
                self._send_notifications(config, message)
                triggered_alerts.append(config.name)

                # Record in history
                self.alert_history.append({
                    'alert_name': config.name,
                    'tender_data': tender_data,
                    'triggered_at': datetime.now().isoformat(),
                })

        return triggered_alerts

    def _should_trigger(self, config: AlertConfig, tender_data: Dict) -> bool:
        """Check if alert should trigger for this tender"""
        # Check country filter
        if config.country_codes:
            tender_country = tender_data.get('country_code', '')
            if tender_country not in config.country_codes:
                # Also check country name
                country_raw = (tender_data.get('country_raw', '') or '').lower()
                country_matches = any(
                    code.lower() in country_raw
                    for code in config.country_codes
                )
                if not country_matches:
                    return False

        # Check agency filter
        if config.agency_codes:
            tender_agency = tender_data.get('agency_code', '')
            if tender_agency not in config.agency_codes:
                return False

        # Check volume threshold
        if config.volume_threshold_mt:
            tender_volume = tender_data.get('volume_value') or 0
            if tender_volume < config.volume_threshold_mt:
                return False

        return True

    def _build_message(self, config: AlertConfig, tender_data: Dict) -> AlertMessage:
        """Build alert message from tender data"""
        country = tender_data.get('country_raw', 'Unknown')
        volume = tender_data.get('volume_value')
        price = tender_data.get('price_value')
        agency = tender_data.get('agency_raw', 'Unknown')
        origins = tender_data.get('origins_raw', '')
        headline = tender_data.get('headline', '')

        # Build subject
        subject_parts = [f"Wheat Tender: {country}"]
        if volume:
            subject_parts.append(f"{volume:,.0f} MT")
        subject = " - ".join(subject_parts)

        # Build body
        body_lines = [
            f"Wheat Tender Alert: {config.name}",
            "",
            f"Country: {country}",
            f"Agency: {agency}",
        ]

        if volume:
            body_lines.append(f"Volume: {volume:,.0f} MT")
        if price:
            price_type = tender_data.get('price_type', '')
            body_lines.append(f"Price: ${price:.2f}/MT {price_type}")
        if origins:
            body_lines.append(f"Origins: {origins}")
        if headline:
            body_lines.extend(["", f"Headline: {headline}"])

        body_lines.extend([
            "",
            f"Source: {tender_data.get('source_name', 'Unknown')}",
            f"URL: {tender_data.get('article_url', 'N/A')}",
        ])

        # Build HTML body
        html_body = f"""
        <h2>Wheat Tender Alert</h2>
        <p><strong>Alert:</strong> {config.name}</p>
        <table style="border-collapse: collapse;">
            <tr><td style="padding: 5px; border: 1px solid #ddd;"><strong>Country</strong></td>
                <td style="padding: 5px; border: 1px solid #ddd;">{country}</td></tr>
            <tr><td style="padding: 5px; border: 1px solid #ddd;"><strong>Agency</strong></td>
                <td style="padding: 5px; border: 1px solid #ddd;">{agency}</td></tr>
            {'<tr><td style="padding: 5px; border: 1px solid #ddd;"><strong>Volume</strong></td><td style="padding: 5px; border: 1px solid #ddd;">' + f"{volume:,.0f} MT" + '</td></tr>' if volume else ''}
            {'<tr><td style="padding: 5px; border: 1px solid #ddd;"><strong>Price</strong></td><td style="padding: 5px; border: 1px solid #ddd;">$' + f"{price:.2f}/MT" + '</td></tr>' if price else ''}
            {'<tr><td style="padding: 5px; border: 1px solid #ddd;"><strong>Origins</strong></td><td style="padding: 5px; border: 1px solid #ddd;">' + origins + '</td></tr>' if origins else ''}
        </table>
        {f'<p><strong>Headline:</strong> {headline}</p>' if headline else ''}
        <p><a href="{tender_data.get('article_url', '#')}">View Source Article</a></p>
        """

        return AlertMessage(
            alert_name=config.name,
            subject=subject,
            body="\n".join(body_lines),
            html_body=html_body,
            data={
                'country': country,
                'agency': agency,
                'volume_mt': volume,
                'price_usd_mt': price,
                'origins': origins,
            },
            priority="high" if volume and volume >= 500000 else "normal"
        )

    def _send_notifications(self, config: AlertConfig, message: AlertMessage):
        """Send notifications via configured channels"""
        if config.notify_email and config.email_recipients:
            self.email_sender.send(config.email_recipients, message)

        if config.notify_slack and config.slack_channels:
            self.slack_sender.send(config.slack_channels, message)
        elif config.notify_slack:
            # Use default webhook if no specific channels
            self.slack_sender.send([], message)

        if config.notify_sms and config.sms_recipients:
            self.sms_sender.send(config.sms_recipients, message)


# =============================================================================
# CONVENIENCE FUNCTIONS
# =============================================================================

def send_tender_alert(
    tender_data: Dict,
    channels: List[str] = None
) -> List[str]:
    """
    Quick function to send tender alert.

    Args:
        tender_data: Tender information dictionary
        channels: List of channels to use (default: all configured)

    Returns:
        List of triggered alert names
    """
    manager = TenderAlertManager()
    return manager.process_tender(tender_data)


def test_notifications():
    """Test notification channels with sample data"""
    test_tender = {
        'country_raw': 'Egypt',
        'country_code': 'EG',
        'agency_raw': 'Mostakbal Misr',
        'agency_code': 'MOSTAKBAL_MISR',
        'volume_value': 60000,
        'price_value': 275.50,
        'price_type': 'C&F',
        'origins_raw': 'Russia, France',
        'headline': 'Egypt books 60,000 MT wheat from Russia',
        'source_name': 'Test',
        'article_url': 'https://example.com/test',
    }

    manager = TenderAlertManager()

    # Create test alert config
    test_config = AlertConfig(
        name="test_alert",
        description="Test alert",
        notify_email=True,
        notify_slack=True,
        email_recipients=["test@example.com"],
    )

    message = manager._build_message(test_config, test_tender)
    print(f"Subject: {message.subject}")
    print(f"Body:\n{message.body}")

    return message


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='Tender Alert System')
    parser.add_argument('command', choices=['test', 'status'])

    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO)

    if args.command == 'test':
        test_notifications()
    elif args.command == 'status':
        manager = TenderAlertManager()
        print(f"Configured alerts: {len(manager.configs)}")
        for config in manager.configs:
            status = "Active" if config.is_active else "Inactive"
            channels = []
            if config.notify_email:
                channels.append("Email")
            if config.notify_slack:
                channels.append("Slack")
            if config.notify_sms:
                channels.append("SMS")
            print(f"  - {config.name}: {status} ({', '.join(channels)})")
