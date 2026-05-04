#!/usr/bin/env python3
"""
Weather Intelligence Agent

Enhanced weather email processing with:
1. Email type classification
2. Type-specific data extraction
3. LLM-powered synthesis
4. Domain knowledge integration

This replaces the simple regex-based weather_email_agent.py
with a sophisticated intelligence pipeline.

Usage:
    python weather_intelligence_agent.py              # Process and send summary
    python weather_intelligence_agent.py --test       # Test mode (no email send)
    python weather_intelligence_agent.py --hours 12   # Look back 12 hours
"""

import os
import sys
import json
import base64
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional, List, Dict, Any
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.utils import parsedate_to_datetime

# Add parent to path for imports
sys.path.insert(0, str(Path(__file__).parent))

from weather_email_classifier import WeatherEmailClassifier, EmailClassification
from weather_data_models import ExtractedWeatherData, WeatherSummaryBatch, AttachmentInfo
from weather_extractors import TextExtractor, PDFExtractor, GraphicsHandler
from weather_synthesizer import WeatherSynthesizer, format_brief_as_email

# Gmail API imports
try:
    from google.oauth2.credentials import Credentials
    from google.auth.transport.requests import Request
    from googleapiclient.discovery import build
    from googleapiclient.errors import HttpError
    GMAIL_AVAILABLE = True
except ImportError:
    GMAIL_AVAILABLE = False

# Configuration
BASE_DIR = Path(__file__).parent.parent
CONFIG_DIR = BASE_DIR / "config"
DATA_DIR = BASE_DIR / "data"
CREDENTIALS_DIR = Path(r"C:\Users\torem\RLC Dropbox\RLC Team Folder\RLC Documents\LLM Model and Documents\Projects\Desktop Assistant")

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class WeatherIntelligenceAgent:
    """
    Enhanced weather email agent with classification, extraction, and synthesis.
    """

    def __init__(self, config_path: Optional[str] = None):
        """Initialize the agent with all components."""
        self.config = self._load_config(config_path)

        # Initialize components
        self.classifier = WeatherEmailClassifier()
        self.text_extractor = TextExtractor()
        self.pdf_extractor = PDFExtractor()
        self.graphics_handler = GraphicsHandler()
        self.synthesizer = WeatherSynthesizer(
            use_claude=self.config.get("use_claude", True)
        )

        # Gmail connection
        self.credentials = None
        self.gmail_service = None

        # Tracking
        self.processed_ids = self._load_processed_ids()

        logger.info("Weather Intelligence Agent initialized")

    def _load_config(self, config_path: Optional[str]) -> Dict:
        """Load configuration."""
        default_config = {
            "meteorologist_senders": [
                "worldweather@bizkc.rr.com",
                "akarst_worldweather@bizkc.rr.com",
                "scarlett_worldweather@bizkc.rr.com",
                "brad_worldweather@bizkc.rr.com"
            ],
            "forward_recipients": [
                "felipe.baptista@roundlakescommodities.com"
            ],
            "summary_recipients": [
                "tore.alden@roundlakescommodities.com",
                "felipe.baptista@roundlakescommodities.com"
            ],
            "token_file": "token_work.json",
            "check_hours_back": 24,
            "processed_ids_file": "weather_emails_processed.json",
            "use_claude": True,
            "save_attachments": True,
            "min_emails_for_summary": 1
        }

        # Try to load from file
        if config_path:
            config_file = Path(config_path)
        else:
            config_file = CONFIG_DIR / "weather_email_config.json"

        if config_file.exists():
            try:
                with open(config_file, 'r') as f:
                    user_config = json.load(f)
                    default_config.update(user_config)
            except Exception as e:
                logger.warning(f"Could not load config: {e}")

        return default_config

    def _load_processed_ids(self) -> set:
        """Load previously processed email IDs."""
        path = CONFIG_DIR / self.config["processed_ids_file"]
        if path.exists():
            try:
                with open(path, 'r') as f:
                    return set(json.load(f))
            except Exception:
                pass
        return set()

    def _save_processed_ids(self):
        """Save processed email IDs."""
        path = CONFIG_DIR / self.config["processed_ids_file"]
        with open(path, 'w') as f:
            # Keep only last 1000 IDs
            recent_ids = list(self.processed_ids)[-1000:]
            json.dump(recent_ids, f)

    def connect_gmail(self) -> bool:
        """Connect to Gmail API."""
        if not GMAIL_AVAILABLE:
            logger.error("Gmail API not available. Install google-api-python-client")
            return False

        token_path = CREDENTIALS_DIR / self.config["token_file"]

        if not token_path.exists():
            logger.error(f"Token file not found: {token_path}")
            return False

        try:
            self.credentials = Credentials.from_authorized_user_file(str(token_path))

            if self.credentials.expired and self.credentials.refresh_token:
                logger.info("Refreshing expired token...")
                self.credentials.refresh(Request())
                with open(token_path, 'w') as f:
                    f.write(self.credentials.to_json())

            self.gmail_service = build('gmail', 'v1', credentials=self.credentials)

            # Verify connection
            profile = self.gmail_service.users().getProfile(userId='me').execute()
            logger.info(f"Connected to Gmail as: {profile['emailAddress']}")
            return True

        except Exception as e:
            logger.error(f"Gmail connection failed: {e}")
            return False

    def fetch_weather_emails(self, hours_back: int = None) -> List[Dict]:
        """Fetch weather emails from Gmail."""
        if not self.gmail_service:
            logger.error("Not connected to Gmail")
            return []

        hours = hours_back or self.config["check_hours_back"]
        after_date = datetime.now() - timedelta(hours=hours)
        after_str = after_date.strftime("%Y/%m/%d")

        emails = []

        for sender in self.config["meteorologist_senders"]:
            try:
                query = f"from:{sender} after:{after_str}"
                logger.info(f"Searching: {query}")

                results = self.gmail_service.users().messages().list(
                    userId='me',
                    q=query,
                    maxResults=50
                ).execute()

                messages = results.get('messages', [])
                logger.info(f"Found {len(messages)} emails from {sender}")

                for msg in messages:
                    if msg['id'] not in self.processed_ids:
                        email_data = self._get_email_details(msg['id'])
                        if email_data:
                            emails.append(email_data)

            except HttpError as e:
                logger.error(f"Gmail API error for {sender}: {e}")
            except Exception as e:
                logger.error(f"Error fetching from {sender}: {e}")

        return emails

    def _get_email_details(self, msg_id: str) -> Optional[Dict]:
        """Get full email details including attachments."""
        try:
            msg = self.gmail_service.users().messages().get(
                userId='me',
                id=msg_id,
                format='full'
            ).execute()

            headers = {h['name']: h['value'] for h in msg['payload'].get('headers', [])}

            # Extract body and attachments
            body_text, attachments = self._extract_body_and_attachments(msg['payload'], msg_id)

            # Parse date — RFC 2822 format from Gmail header
            date_str = headers.get('Date', '')
            try:
                received_at = parsedate_to_datetime(date_str) if date_str else datetime.now(timezone.utc)
                if received_at.tzinfo is None:
                    received_at = received_at.replace(tzinfo=timezone.utc)
            except Exception:
                received_at = datetime.now(timezone.utc)

            return {
                'id': msg_id,
                'subject': headers.get('Subject', 'No Subject'),
                'from': headers.get('From', 'Unknown'),
                'date': date_str,
                'received_at': received_at,
                'body': body_text,
                'attachments': attachments,
                'snippet': msg.get('snippet', '')
            }

        except Exception as e:
            logger.error(f"Error getting email {msg_id}: {e}")
            return None

    def _extract_body_and_attachments(
        self,
        payload: Dict,
        msg_id: str
    ) -> tuple:
        """Extract body text and attachment info from email payload."""
        body_text = ""
        attachments = []

        def process_part(part):
            nonlocal body_text, attachments

            mime_type = part.get('mimeType', '')
            filename = part.get('filename', '')

            # Check for attachment
            if filename and part.get('body', {}).get('attachmentId'):
                attachments.append({
                    'filename': filename,
                    'mime_type': mime_type,
                    'attachment_id': part['body']['attachmentId'],
                    'size': part['body'].get('size', 0)
                })

            # Extract text body
            elif mime_type == 'text/plain' and not body_text:
                data = part.get('body', {}).get('data')
                if data:
                    body_text = base64.urlsafe_b64decode(data).decode('utf-8', errors='ignore')

            elif mime_type == 'text/html' and not body_text:
                data = part.get('body', {}).get('data')
                if data:
                    import re
                    html = base64.urlsafe_b64decode(data).decode('utf-8', errors='ignore')
                    body_text = re.sub(r'<[^>]+>', ' ', html)
                    body_text = re.sub(r'\s+', ' ', body_text).strip()

            # Handle multipart
            if 'parts' in part:
                for subpart in part['parts']:
                    process_part(subpart)

        # Check for simple body
        if 'body' in payload and payload['body'].get('data'):
            body_text = base64.urlsafe_b64decode(payload['body']['data']).decode('utf-8', errors='ignore')

        # Process parts
        if 'parts' in payload:
            for part in payload['parts']:
                process_part(part)

        return body_text, attachments

    def _get_db_connection(self):
        """Lazy-import DB helper. Returns None if unavailable (so the agent
        can still run a brief without DB persistence)."""
        try:
            project_root = str(Path(__file__).parent.parent.parent)
            if project_root not in sys.path:
                sys.path.insert(0, project_root)
            from src.services.database.db_config import get_connection
            return get_connection
        except Exception as e:
            logger.warning(f"DB persistence unavailable: {e}")
            return None

    def _save_extract_to_bronze(self, extracted, processing_time_ms: int = 0) -> bool:
        """Persist a single extracted email to bronze.weather_email_extract.

        ON CONFLICT (email_id) updates so this is idempotent — same Gmail
        message id never duplicates rows.
        """
        get_conn = self._get_db_connection()
        if get_conn is None:
            return False

        try:
            locations = sorted({r.region_name for r in extracted.regions.values()
                                if r.region_name})
            summary_lines = []
            if extracted.headline_summary:
                summary_lines.append(extracted.headline_summary)
            if extracted.key_points:
                summary_lines.extend(f"- {p}" for p in extracted.key_points[:10])
            weather_summary = "\n".join(summary_lines) or extracted.full_text_summary or ""
            extracted_conditions = json.dumps(extracted.to_dict(), default=str)
            llm_model_used = ('claude' if (self.synthesizer.use_claude
                                            and self.synthesizer.anthropic_available)
                              else self.synthesizer.ollama_model)

            with get_conn() as conn:
                cur = conn.cursor()
                cur.execute("""
                    INSERT INTO bronze.weather_email_extract
                        (email_id, email_subject, email_from, email_date,
                         extracted_locations, weather_summary,
                         extracted_conditions, llm_model, processing_time_ms,
                         collected_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s::jsonb, %s, %s, NOW())
                    ON CONFLICT (email_id) DO UPDATE SET
                        email_subject = EXCLUDED.email_subject,
                        email_from = EXCLUDED.email_from,
                        email_date = EXCLUDED.email_date,
                        extracted_locations = EXCLUDED.extracted_locations,
                        weather_summary = EXCLUDED.weather_summary,
                        extracted_conditions = EXCLUDED.extracted_conditions,
                        llm_model = EXCLUDED.llm_model,
                        processing_time_ms = EXCLUDED.processing_time_ms
                """, (
                    extracted.email_id,
                    extracted.subject[:1000] if extracted.subject else None,
                    extracted.sender[:500] if extracted.sender else None,
                    extracted.received_at,
                    locations or None,
                    weather_summary,
                    extracted_conditions,
                    llm_model_used,
                    processing_time_ms,
                ))
                conn.commit()
            return True
        except Exception as e:
            logger.warning(f"Failed to save extract for {extracted.email_id}: {e}")
            return False

    def _load_recent_extracts_from_bronze(
        self, days_back: int = 7, exclude_email_ids: Optional[set] = None
    ) -> List[Dict]:
        """Pull recent emails out of bronze.weather_email_extract for synthesis
        context. Excludes any email_ids passed in (so we don't double-count
        emails we already added to today's batch)."""
        get_conn = self._get_db_connection()
        if get_conn is None:
            return []

        exclude_email_ids = exclude_email_ids or set()
        cutoff = datetime.now(timezone.utc) - timedelta(days=days_back)

        try:
            with get_conn() as conn:
                cur = conn.cursor()
                cur.execute("""
                    SELECT email_id, email_subject, email_from, email_date,
                           extracted_locations, weather_summary
                    FROM bronze.weather_email_extract
                    WHERE email_date >= %s
                    ORDER BY email_date DESC
                    LIMIT 100
                """, (cutoff,))
                rows = cur.fetchall()

            recents = []
            for r in rows:
                # tolerate either tuple or RealDictRow
                if isinstance(r, dict):
                    email_id = r['email_id']
                    subj = r['email_subject']; sender = r['email_from']
                    edate = r['email_date']; locs = r['extracted_locations']
                    summary = r['weather_summary']
                else:
                    email_id, subj, sender, edate, locs, summary = r
                if email_id in exclude_email_ids:
                    continue
                recents.append({
                    'email_id': email_id, 'subject': subj or '',
                    'sender': sender or '', 'email_date': edate,
                    'locations': list(locs or []),
                    'summary': summary or '',
                })
            return recents
        except Exception as e:
            logger.warning(f"Failed to load recent extracts: {e}")
            return []

    @staticmethod
    def _format_history_context(recents: List[Dict]) -> str:
        """Build a 'PRIOR WEEK CONTEXT' block from recent extract rows."""
        if not recents:
            return ""
        lines = ["", "=" * 60,
                 "PRIOR WEEK CONTEXT (already-processed emails for trend reference)",
                 "Use this to assess week-over-week changes — do NOT just restate it.",
                 "=" * 60]
        for r in recents:
            d = r['email_date']
            d_str = d.strftime('%Y-%m-%d %H:%M') if hasattr(d, 'strftime') else str(d)
            sender_short = (r['sender'].split('<')[0].strip() or r['sender'])[:35]
            subj = r['subject'][:90]
            lines.append(f"\n[{d_str}] {sender_short} — {subj}")
            if r['summary']:
                # First 400 chars of summary is enough for trend tracking
                snippet = r['summary'][:400].strip()
                lines.append(snippet)
        return "\n".join(lines)

    def process_emails(
        self,
        hours_back: int = None,
        send_summary: bool = True,
        forward_emails: bool = True,
        test_mode: bool = False
    ) -> Dict[str, Any]:
        """
        Main processing pipeline.

        1. Fetch emails
        2. Classify each email
        3. Extract structured data
        4. Persist each extract to bronze.weather_email_extract
        5. Load last 7 days from bronze for synthesis context
        6. Synthesize into brief
        7. Send summary email
        """
        result = {
            'success': False,
            'emails_found': 0,
            'emails_processed': 0,
            'summary_sent': False,
            'brief': None
        }

        # Connect to Gmail
        if not self.connect_gmail():
            return result

        # Fetch emails
        emails = self.fetch_weather_emails(hours_back)
        result['emails_found'] = len(emails)

        if not emails:
            logger.info("No new weather emails found")
            result['success'] = True
            return result

        logger.info(f"Processing {len(emails)} emails")

        # Create batch for synthesis
        batch = WeatherSummaryBatch()

        # Process each email
        for email in emails:
            try:
                # 1. Classify
                classification = self.classifier.classify(
                    subject=email['subject'],
                    body_preview=email['body'][:500] if email['body'] else "",
                    attachments=[a['filename'] for a in email.get('attachments', [])]
                )

                logger.info(f"Classified '{email['subject'][:50]}...' as {classification.email_type}")

                # 2. Extract based on type
                extracted = self.text_extractor.extract(
                    email_id=email['id'],
                    subject=email['subject'],
                    body=email['body'],
                    sender=email['from'],
                    received_at=email['received_at'],
                    email_type=classification.email_type,
                    classification=classification.to_dict()
                )

                # 3. Handle attachments if present
                for att in email.get('attachments', []):
                    att_info = AttachmentInfo(
                        filename=att['filename'],
                        file_type=att['mime_type'].split('/')[-1] if '/' in att['mime_type'] else 'unknown'
                    )
                    extracted.attachments.append(att_info)

                # 4. Add to batch
                batch.add_email(extracted)
                result['emails_processed'] += 1

                # 5. Persist to bronze (idempotent on email_id)
                self._save_extract_to_bronze(extracted)

                # Mark as processed
                self.processed_ids.add(email['id'])

            except Exception as e:
                logger.error(f"Error processing email {email['id']}: {e}")

        # Save processed IDs
        self._save_processed_ids()

        # 6. Synthesize with research + 7-day rolling context
        # We synthesize even on a 0-new-email day if there is recent history,
        # so the brief reflects the current state of Drew's commentary stream.
        new_extract_ids = {e.email_id for e in batch.emails}
        recent_history = self._load_recent_extracts_from_bronze(
            days_back=7, exclude_email_ids=new_extract_ids
        )
        result['recent_history_count'] = len(recent_history)

        min_for_summary = self.config.get("min_emails_for_summary", 1)
        have_enough = (result['emails_processed'] >= min_for_summary
                       or len(recent_history) >= min_for_summary)
        if have_enough:
            logger.info(
                f"Generating brief: {result['emails_processed']} new emails, "
                f"{len(recent_history)} from prior 7 days"
            )

            batch_data = batch.to_dict()
            base_context = batch.get_llm_context()
            history_block = self._format_history_context(recent_history)
            batch_data["llm_context"] = base_context + "\n" + history_block

            # Use research-enhanced synthesis
            brief = self.synthesizer.synthesize_with_research(
                batch_data,
                regions=batch.regions_covered
            )
            result['brief'] = brief

            # Add header/footer (counts include the 7-day rolling window
            # used for synthesis, not just today's new emails)
            full_brief = self._format_full_brief(brief, batch, recent_history)

            if not test_mode:
                # Save brief
                brief_path = CONFIG_DIR / f"weather_brief_{datetime.now().strftime('%Y%m%d_%H%M')}.txt"
                with open(brief_path, 'w', encoding='utf-8') as f:
                    f.write(full_brief)
                logger.info(f"Brief saved to {brief_path}")

                # Send email
                if send_summary:
                    result['summary_sent'] = self._send_summary_email(full_brief)
            else:
                print("\n" + "=" * 60)
                print("GENERATED BRIEF (Test Mode)")
                print("=" * 60)
                print(full_brief)

        result['success'] = True
        return result

    def _format_full_brief(self, brief: str, batch: WeatherSummaryBatch,
                           recent_history: Optional[List[Dict]] = None) -> str:
        """Format the full brief with header and footer.

        Counts and region list reflect the full 7-day synthesis window
        (today's new emails + prior-week history), not just today's batch.
        """
        recent_history = recent_history or []
        history_regions = {loc for r in recent_history for loc in (r.get('locations') or [])}
        total_regions = sorted(set(batch.regions_covered) | history_regions)
        total_emails = batch.emails_processed + len(recent_history)

        lines = [
            "=" * 60,
            "WEATHER INTELLIGENCE BRIEF",
            f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M')} ET",
            f"Emails analyzed: {total_emails} "
            f"({batch.emails_processed} new today, {len(recent_history)} from prior 7 days)",
            "=" * 60,
            "",
            brief,
            "",
            "-" * 60,
            f"Regions: {', '.join(total_regions) if total_regions else '(none parsed)'}",
            f"Sentiment: {batch.overall_sentiment}",
            "Source: World Weather Inc.",
            "Analysis by: Claude (Anthropic)" if self.synthesizer.use_claude else f"Analysis by: Ollama ({self.synthesizer.ollama_model})",
            "=" * 60
        ]

        return "\n".join(lines)

    def _send_summary_email(self, brief: str) -> bool:
        """Send the summary email."""
        recipients = self.config.get("summary_recipients", [])
        if not recipients:
            recipients = self.config.get("forward_recipients", [])

        if not recipients:
            logger.warning("No recipients configured for summary")
            return False

        try:
            email_data = format_brief_as_email(brief, "Weather Intelligence Brief")

            message = MIMEText(brief, 'plain')
            message['To'] = ', '.join(recipients)
            message['Subject'] = email_data['subject']

            raw = base64.urlsafe_b64encode(message.as_bytes()).decode()

            self.gmail_service.users().messages().send(
                userId='me',
                body={'raw': raw}
            ).execute()

            logger.info(f"Summary sent to {', '.join(recipients)}")
            return True

        except Exception as e:
            logger.error(f"Error sending summary: {e}")
            return False


def main():
    """CLI interface."""
    import argparse

    parser = argparse.ArgumentParser(description='Weather Intelligence Agent')
    parser.add_argument('--test', action='store_true', help='Test mode - no email sending')
    parser.add_argument('--hours', type=int, default=24, help='Hours to look back')
    parser.add_argument('--no-forward', action='store_true', help='Skip forwarding emails')
    parser.add_argument('--no-summary', action='store_true', help='Skip summary generation')
    parser.add_argument('--config', help='Path to config file')

    args = parser.parse_args()

    print("=" * 60)
    print("Weather Intelligence Agent")
    print("=" * 60)

    agent = WeatherIntelligenceAgent(config_path=args.config)

    result = agent.process_emails(
        hours_back=args.hours,
        send_summary=not args.no_summary,
        forward_emails=not args.no_forward,
        test_mode=args.test
    )

    print("\n" + "-" * 60)
    print("RESULTS")
    print("-" * 60)
    print(f"Emails found: {result['emails_found']}")
    print(f"Emails processed: {result['emails_processed']}")
    print(f"Summary sent: {result['summary_sent']}")
    print(f"Success: {result['success']}")


if __name__ == "__main__":
    main()
