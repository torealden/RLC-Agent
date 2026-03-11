"""
Email Channel

Standalone email sending utility for the publishing agent.
Supports both SMTP (Gmail/Outlook) and Gmail API via OAuth.

Configuration via environment variables:
    SMTP_HOST=smtp.gmail.com
    SMTP_PORT=587
    SMTP_USER=your@gmail.com
    SMTP_PASSWORD=app_password
    PUBLISH_FROM_EMAIL=your@gmail.com
    PUBLISH_TO_EMAILS=recipient1@example.com,recipient2@example.com
"""

import logging
import os
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.image import MIMEImage
from pathlib import Path
from typing import List, Optional

logger = logging.getLogger(__name__)


def send_email(
    subject: str,
    html_body: str,
    text_body: str = None,
    to_emails: List[str] = None,
    from_email: str = None,
    attachments: List[str] = None,
    inline_images: List[str] = None,
) -> bool:
    """
    Send an email via SMTP.

    Args:
        subject: Email subject line
        html_body: HTML content
        text_body: Plain text fallback (default: strip HTML)
        to_emails: Recipient list (default from env)
        from_email: Sender address (default from env)
        attachments: File paths to attach
        inline_images: Image paths to inline (referenced as cid:imageN)

    Returns:
        True if sent successfully
    """
    smtp_host = os.environ.get('SMTP_HOST', 'smtp.gmail.com')
    smtp_port = int(os.environ.get('SMTP_PORT', '587'))
    smtp_user = os.environ.get('SMTP_USER', '')
    smtp_pass = os.environ.get('SMTP_PASSWORD', '')
    from_email = from_email or os.environ.get('PUBLISH_FROM_EMAIL', smtp_user)
    to_emails = to_emails or [
        e.strip() for e in os.environ.get('PUBLISH_TO_EMAILS', '').split(',')
        if e.strip()
    ]

    if not smtp_user or not to_emails:
        raise ValueError("SMTP not configured. Set SMTP_USER, SMTP_PASSWORD, PUBLISH_TO_EMAILS")

    text_body = text_body or html_body  # Fallback

    msg = MIMEMultipart('related')
    msg['Subject'] = subject
    msg['From'] = from_email
    msg['To'] = ', '.join(to_emails)

    # Multipart alternative: text + html
    alt = MIMEMultipart('alternative')
    alt.attach(MIMEText(text_body, 'plain'))
    alt.attach(MIMEText(html_body, 'html'))
    msg.attach(alt)

    # Inline images
    for i, img_path in enumerate(inline_images or []):
        try:
            with open(img_path, 'rb') as f:
                img = MIMEImage(f.read())
                img.add_header('Content-ID', f'<image{i}>')
                img.add_header('Content-Disposition', 'inline',
                               filename=Path(img_path).name)
                msg.attach(img)
        except FileNotFoundError:
            logger.warning(f"Inline image not found: {img_path}")

    # Attachments
    from email.mime.base import MIMEBase
    from email import encoders
    for att_path in (attachments or []):
        try:
            with open(att_path, 'rb') as f:
                part = MIMEBase('application', 'octet-stream')
                part.set_payload(f.read())
                encoders.encode_base64(part)
                part.add_header('Content-Disposition', 'attachment',
                                filename=Path(att_path).name)
                msg.attach(part)
        except FileNotFoundError:
            logger.warning(f"Attachment not found: {att_path}")

    # Send
    with smtplib.SMTP(smtp_host, smtp_port) as server:
        server.starttls()
        server.login(smtp_user, smtp_pass)
        server.sendmail(from_email, to_emails, msg.as_string())

    logger.info(f"Email sent: '{subject}' to {len(to_emails)} recipients")
    return True
