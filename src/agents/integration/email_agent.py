"""
Email Management Agent for RLC Master Agent
Handles Gmail integration, email triage, and response drafting
Round Lakes Commodities
"""

import os
import json
import base64
import logging
from typing import Dict, Any, Optional, List, Tuple
from datetime import datetime
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from pathlib import Path
from dataclasses import dataclass, field
from enum import Enum

logger = logging.getLogger('rlc_master_agent.email_agent')


class EmailPriority(Enum):
    """Email priority levels"""
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"
    SPAM = "spam"


class EmailAction(Enum):
    """Suggested actions for emails"""
    REPLY = "reply"
    ARCHIVE = "archive"
    DELETE = "delete"
    SCHEDULE_MEETING = "schedule_meeting"
    FORWARD = "forward"
    FLAG = "flag"
    READ_LATER = "read_later"


@dataclass
class EmailMessage:
    """Represents an email message"""
    id: str
    thread_id: str
    subject: str
    sender: str
    sender_name: str
    recipients: List[str]
    date: datetime
    snippet: str
    body: str
    labels: List[str]
    is_unread: bool
    has_attachments: bool
    priority: EmailPriority = EmailPriority.MEDIUM
    suggested_action: Optional[EmailAction] = None
    summary: Optional[str] = None


@dataclass
class EmailDraft:
    """Represents an email draft"""
    to: List[str]
    subject: str
    body: str
    cc: List[str] = field(default_factory=list)
    bcc: List[str] = field(default_factory=list)
    reply_to_id: Optional[str] = None
    thread_id: Optional[str] = None


class EmailAgent:
    """
    Agent responsible for Gmail integration and email management.

    Features:
    - Fetch and summarize emails
    - Prioritize and categorize emails
    - Draft responses
    - Archive/delete emails
    - Send emails (with approval)
    """

    def __init__(
        self,
        settings: Optional[Any] = None,
        preferences_path: Optional[Path] = None,
        approval_manager: Optional[Any] = None
    ):
        """
        Initialize Email Agent

        Args:
            settings: Application settings
            preferences_path: Path to email preferences JSON
            approval_manager: Approval manager for send operations
        """
        self.settings = settings
        self.approval_manager = approval_manager
        self._gmail_service = None
        self._credentials = None

        # Load preferences
        self.preferences = self._load_preferences(preferences_path)

        # Cache for emails
        self._email_cache: Dict[str, EmailMessage] = {}

        logger.info("Email Agent initialized")

    def _load_preferences(self, path: Optional[Path]) -> Dict[str, Any]:
        """Load email preferences from file"""
        default_prefs = {
            'priority_senders': {'high': [], 'low': []},
            'auto_archive': {'enabled': False, 'patterns': []},
            'urgent_keywords': ['urgent', 'asap', 'immediately'],
            'categories': {},
            'daily_summary': {'enabled': True, 'time': '08:00'}
        }

        if path and path.exists():
            try:
                with open(path, 'r') as f:
                    prefs = json.load(f)
                    default_prefs.update(prefs)
                    logger.debug(f"Loaded email preferences from {path}")
            except Exception as e:
                logger.warning(f"Could not load preferences: {e}")

        return default_prefs

    def _get_gmail_service(self):
        """Get or create Gmail API service"""
        if self._gmail_service is not None:
            return self._gmail_service

        try:
            from google.oauth2.credentials import Credentials
            from google_auth_oauthlib.flow import InstalledAppFlow
            from google.auth.transport.requests import Request
            from googleapiclient.discovery import build
            import pickle

            SCOPES = [
                'https://www.googleapis.com/auth/gmail.readonly',
                'https://www.googleapis.com/auth/gmail.send',
                'https://www.googleapis.com/auth/gmail.modify'
            ]

            creds = None
            token_path = None
            credentials_path = None

            if self.settings and hasattr(self.settings, 'google'):
                token_dir = Path(self.settings.google.token_dir)
                token_path = token_dir / 'gmail_token.pickle'
                credentials_path = self.settings.google.get_credentials_path('gmail_work')

            # Try to load existing token
            if token_path and token_path.exists():
                with open(token_path, 'rb') as token:
                    creds = pickle.load(token)

            # Refresh or get new credentials
            if not creds or not creds.valid:
                if creds and creds.expired and creds.refresh_token:
                    creds.refresh(Request())
                elif credentials_path and credentials_path.exists():
                    flow = InstalledAppFlow.from_client_secrets_file(
                        str(credentials_path), SCOPES
                    )
                    creds = flow.run_local_server(port=0)

                # Save the token
                if token_path and creds:
                    token_path.parent.mkdir(parents=True, exist_ok=True)
                    with open(token_path, 'wb') as token:
                        pickle.dump(creds, token)

            if creds:
                self._gmail_service = build('gmail', 'v1', credentials=creds)
                self._credentials = creds
                logger.info("Gmail service initialized")
                return self._gmail_service

        except ImportError as e:
            logger.error(f"Gmail dependencies not installed: {e}")
        except Exception as e:
            logger.error(f"Failed to initialize Gmail service: {e}")

        return None

    # -------------------------------------------------------------------------
    # Email Fetching
    # -------------------------------------------------------------------------

    def get_unread_emails(self, max_results: int = 20) -> List[EmailMessage]:
        """
        Get unread emails from inbox

        Args:
            max_results: Maximum number of emails to fetch

        Returns:
            List of EmailMessage objects
        """
        return self._fetch_emails(query='is:unread', max_results=max_results)

    def get_inbox(self, max_results: int = 20) -> List[EmailMessage]:
        """
        Get emails from inbox

        Args:
            max_results: Maximum number of emails to fetch

        Returns:
            List of EmailMessage objects
        """
        return self._fetch_emails(query='in:inbox', max_results=max_results)

    def get_important_emails(self, max_results: int = 10) -> List[EmailMessage]:
        """Get important/starred emails"""
        return self._fetch_emails(query='is:important OR is:starred', max_results=max_results)

    def search_emails(self, query: str, max_results: int = 20) -> List[EmailMessage]:
        """
        Search emails with Gmail query

        Args:
            query: Gmail search query
            max_results: Maximum results

        Returns:
            List of matching emails
        """
        return self._fetch_emails(query=query, max_results=max_results)

    def _fetch_emails(self, query: str, max_results: int = 20) -> List[EmailMessage]:
        """Internal method to fetch emails"""
        service = self._get_gmail_service()
        if not service:
            logger.warning("Gmail service not available")
            return []

        try:
            results = service.users().messages().list(
                userId='me',
                q=query,
                maxResults=max_results
            ).execute()

            messages = results.get('messages', [])
            emails = []

            for msg_info in messages:
                email = self._get_email_details(msg_info['id'])
                if email:
                    self._analyze_email(email)
                    emails.append(email)
                    self._email_cache[email.id] = email

            return emails

        except Exception as e:
            logger.error(f"Failed to fetch emails: {e}")
            return []

    def _get_email_details(self, msg_id: str) -> Optional[EmailMessage]:
        """Get full details of an email"""
        service = self._get_gmail_service()
        if not service:
            return None

        try:
            msg = service.users().messages().get(
                userId='me',
                id=msg_id,
                format='full'
            ).execute()

            headers = {h['name']: h['value'] for h in msg['payload'].get('headers', [])}

            # Extract body
            body = self._extract_body(msg['payload'])

            # Parse sender
            sender = headers.get('From', '')
            sender_name = sender.split('<')[0].strip().strip('"')
            sender_email = sender.split('<')[-1].replace('>', '').strip() if '<' in sender else sender

            # Parse date
            date_str = headers.get('Date', '')
            try:
                # Parse various date formats
                from email.utils import parsedate_to_datetime
                date = parsedate_to_datetime(date_str)
            except:
                date = datetime.now()

            return EmailMessage(
                id=msg_id,
                thread_id=msg.get('threadId', msg_id),
                subject=headers.get('Subject', '(No Subject)'),
                sender=sender_email,
                sender_name=sender_name,
                recipients=headers.get('To', '').split(','),
                date=date,
                snippet=msg.get('snippet', ''),
                body=body,
                labels=msg.get('labelIds', []),
                is_unread='UNREAD' in msg.get('labelIds', []),
                has_attachments=self._has_attachments(msg['payload'])
            )

        except Exception as e:
            logger.error(f"Failed to get email details for {msg_id}: {e}")
            return None

    def _extract_body(self, payload: Dict) -> str:
        """Extract body text from email payload"""
        body = ''

        if 'body' in payload and 'data' in payload['body']:
            body = base64.urlsafe_b64decode(payload['body']['data']).decode('utf-8', errors='ignore')
        elif 'parts' in payload:
            for part in payload['parts']:
                if part['mimeType'] == 'text/plain':
                    if 'data' in part.get('body', {}):
                        body = base64.urlsafe_b64decode(part['body']['data']).decode('utf-8', errors='ignore')
                        break
                elif 'parts' in part:
                    body = self._extract_body(part)
                    if body:
                        break

        return body

    def _has_attachments(self, payload: Dict) -> bool:
        """Check if email has attachments"""
        if 'parts' in payload:
            for part in payload['parts']:
                if part.get('filename'):
                    return True
                if 'parts' in part:
                    if self._has_attachments(part):
                        return True
        return False

    # -------------------------------------------------------------------------
    # Email Analysis
    # -------------------------------------------------------------------------

    def _analyze_email(self, email: EmailMessage):
        """Analyze email for priority and suggested action"""
        # Determine priority
        priority = EmailPriority.MEDIUM

        # Check high priority senders
        high_priority_senders = self.preferences.get('priority_senders', {}).get('high', [])
        for sender in high_priority_senders:
            if sender.lower() in email.sender.lower():
                priority = EmailPriority.HIGH
                break

        # Check for urgent keywords
        urgent_keywords = self.preferences.get('urgent_keywords', [])
        text_to_check = f"{email.subject} {email.snippet}".lower()
        for keyword in urgent_keywords:
            if keyword.lower() in text_to_check:
                priority = EmailPriority.HIGH
                break

        # Check low priority patterns
        low_priority_senders = self.preferences.get('priority_senders', {}).get('low', [])
        for sender in low_priority_senders:
            if sender.lower() in email.sender.lower():
                priority = EmailPriority.LOW
                break

        email.priority = priority

        # Suggest action
        email.suggested_action = self._suggest_action(email)

        # Generate summary
        email.summary = self._summarize_email(email)

    def _suggest_action(self, email: EmailMessage) -> EmailAction:
        """Suggest an action for the email"""
        text = f"{email.subject} {email.body}".lower()

        # Check for meeting-related content
        meeting_keywords = ['meeting', 'schedule', 'calendar', 'call', 'discuss']
        if any(kw in text for kw in meeting_keywords):
            return EmailAction.SCHEDULE_MEETING

        # Check for auto-archive patterns
        auto_archive = self.preferences.get('auto_archive', {})
        if auto_archive.get('enabled'):
            for pattern in auto_archive.get('patterns', []):
                if pattern.lower() in email.sender.lower():
                    return EmailAction.ARCHIVE

        # Check priority
        if email.priority == EmailPriority.HIGH:
            return EmailAction.REPLY

        if email.priority == EmailPriority.LOW:
            return EmailAction.READ_LATER

        return EmailAction.FLAG

    def _summarize_email(self, email: EmailMessage) -> str:
        """Generate a brief summary of the email"""
        # Simple summarization - take first few sentences
        body = email.body.strip()
        if not body:
            return email.snippet

        # Split into sentences and take first 2
        sentences = body.replace('\n', ' ').split('.')
        summary = '. '.join(s.strip() for s in sentences[:2] if s.strip())

        if len(summary) > 200:
            summary = summary[:197] + '...'

        return summary or email.snippet

    def summarize_inbox(self, max_emails: int = 10) -> Dict[str, Any]:
        """
        Get a summary of the inbox

        Returns:
            Dictionary with inbox summary
        """
        emails = self.get_unread_emails(max_results=max_emails)

        summary = {
            'timestamp': datetime.now().isoformat(),
            'unread_count': len(emails),
            'by_priority': {
                'high': [],
                'medium': [],
                'low': []
            },
            'suggested_actions': {}
        }

        for email in emails:
            priority_key = email.priority.value
            summary['by_priority'][priority_key].append({
                'id': email.id,
                'subject': email.subject,
                'from': email.sender_name or email.sender,
                'date': email.date.isoformat(),
                'summary': email.summary,
                'suggested_action': email.suggested_action.value if email.suggested_action else None
            })

            # Count actions
            if email.suggested_action:
                action = email.suggested_action.value
                summary['suggested_actions'][action] = summary['suggested_actions'].get(action, 0) + 1

        return summary

    # -------------------------------------------------------------------------
    # Email Actions
    # -------------------------------------------------------------------------

    def draft_reply(
        self,
        email_id: str,
        content: str,
        include_original: bool = True
    ) -> EmailDraft:
        """
        Draft a reply to an email

        Args:
            email_id: ID of email to reply to
            content: Reply content
            include_original: Include original message

        Returns:
            EmailDraft object
        """
        original = self._email_cache.get(email_id) or self._get_email_details(email_id)
        if not original:
            raise ValueError(f"Email not found: {email_id}")

        subject = original.subject
        if not subject.lower().startswith('re:'):
            subject = f"Re: {subject}"

        body = content
        if include_original:
            body += f"\n\n---\nOn {original.date.strftime('%B %d, %Y')}, {original.sender_name} wrote:\n"
            body += '\n'.join(f"> {line}" for line in original.body.split('\n')[:20])

        return EmailDraft(
            to=[original.sender],
            subject=subject,
            body=body,
            reply_to_id=email_id,
            thread_id=original.thread_id
        )

    def send_email(
        self,
        draft: EmailDraft,
        require_approval: bool = True
    ) -> Tuple[bool, str]:
        """
        Send an email

        Args:
            draft: EmailDraft to send
            require_approval: Whether to require approval

        Returns:
            Tuple of (success, message)
        """
        # Check approval
        if require_approval and self.approval_manager:
            from approval_manager import ActionType
            approved, reason = self.approval_manager.request_approval(
                action_type=ActionType.EMAIL_SEND,
                description=f"Send email to {', '.join(draft.to)}",
                details={
                    'to': draft.to,
                    'subject': draft.subject,
                    'body_preview': draft.body[:200]
                }
            )
            if not approved:
                return False, f"Email not sent: {reason}"

        service = self._get_gmail_service()
        if not service:
            return False, "Gmail service not available"

        try:
            message = MIMEMultipart()
            message['to'] = ', '.join(draft.to)
            message['subject'] = draft.subject

            if draft.cc:
                message['cc'] = ', '.join(draft.cc)
            if draft.bcc:
                message['bcc'] = ', '.join(draft.bcc)

            message.attach(MIMEText(draft.body, 'plain'))

            raw = base64.urlsafe_b64encode(message.as_bytes()).decode()

            send_params = {'userId': 'me', 'body': {'raw': raw}}
            if draft.thread_id:
                send_params['body']['threadId'] = draft.thread_id

            result = service.users().messages().send(**send_params).execute()

            logger.info(f"Email sent: {draft.subject} to {draft.to}")
            return True, f"Email sent successfully (ID: {result.get('id')})"

        except Exception as e:
            logger.error(f"Failed to send email: {e}")
            return False, f"Failed to send email: {str(e)}"

    def archive_email(self, email_id: str) -> Tuple[bool, str]:
        """Archive an email"""
        service = self._get_gmail_service()
        if not service:
            return False, "Gmail service not available"

        try:
            service.users().messages().modify(
                userId='me',
                id=email_id,
                body={'removeLabelIds': ['INBOX']}
            ).execute()

            logger.info(f"Email archived: {email_id}")
            return True, "Email archived"

        except Exception as e:
            logger.error(f"Failed to archive email: {e}")
            return False, f"Failed to archive: {str(e)}"

    def mark_as_read(self, email_id: str) -> Tuple[bool, str]:
        """Mark email as read"""
        service = self._get_gmail_service()
        if not service:
            return False, "Gmail service not available"

        try:
            service.users().messages().modify(
                userId='me',
                id=email_id,
                body={'removeLabelIds': ['UNREAD']}
            ).execute()

            return True, "Marked as read"

        except Exception as e:
            return False, f"Failed: {str(e)}"

    def add_label(self, email_id: str, label: str) -> Tuple[bool, str]:
        """Add a label to an email"""
        service = self._get_gmail_service()
        if not service:
            return False, "Gmail service not available"

        try:
            # First, get or create the label
            labels_response = service.users().labels().list(userId='me').execute()
            labels = {l['name']: l['id'] for l in labels_response.get('labels', [])}

            if label not in labels:
                # Create the label
                new_label = service.users().labels().create(
                    userId='me',
                    body={'name': label}
                ).execute()
                label_id = new_label['id']
            else:
                label_id = labels[label]

            # Add the label
            service.users().messages().modify(
                userId='me',
                id=email_id,
                body={'addLabelIds': [label_id]}
            ).execute()

            return True, f"Label '{label}' added"

        except Exception as e:
            return False, f"Failed: {str(e)}"

    # -------------------------------------------------------------------------
    # Health & Status
    # -------------------------------------------------------------------------

    def health_check(self) -> Dict[str, Any]:
        """Check email agent health"""
        service = self._get_gmail_service()

        status = {
            'gmail_connected': service is not None,
            'cached_emails': len(self._email_cache),
            'preferences_loaded': bool(self.preferences)
        }

        if service:
            try:
                profile = service.users().getProfile(userId='me').execute()
                status['email_address'] = profile.get('emailAddress')
                status['total_messages'] = profile.get('messagesTotal')
            except Exception as e:
                status['profile_error'] = str(e)

        return status
