"""
Weekly Cash Prices Export -> Felipe

Pipeline:
  1. Generate Cash Prices - MMDDYYYY.xlsx via src.tools.generate_cash_prices
  2. Copy file to RLC Dropbox\HigbyBarrett\weekly_cash_prices\ (Felipe has access)
  3. Email Felipe with the file as an attachment

Designed to run weekly via Windows Scheduled Task (Wed evening, after
the existing cash_prices_generation job that runs at 18:00 ET).

Usage:
    python scripts/email_cash_prices_to_felipe.py                 # use today's date
    python scripts/email_cash_prices_to_felipe.py --date 2026-05-21
    python scripts/email_cash_prices_to_felipe.py --dry-run       # build but don't email
"""
import argparse
import base64
import logging
import os
import shutil
import sys
from datetime import date, datetime
from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

try:
    from dotenv import load_dotenv
    load_dotenv(PROJECT_ROOT / '.env')
except ImportError:
    pass

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build

from src.tools.generate_cash_prices import generate_cash_prices

logger = logging.getLogger(__name__)

# Shared Dropbox folder Felipe already has access to via the HigbyBarrett share.
SHARED_DROPBOX = Path(
    r"C:\Users\torem\RLC Dropbox\Tore Alden\HigbyBarrett\weekly_cash_prices"
)
FELIPE_EMAIL = "felipe.baptista@roundlakescommodities.com"
TORE_EMAIL = "tore.alden@roundlakescommodities.com"

GMAIL_TOKEN = Path(
    r"C:\Users\torem\RLC Dropbox\RLC Team Folder\RLC Documents"
    r"\LLM Model and Documents\Projects\Desktop Assistant\token_work.json"
)


def _gmail_service():
    creds = Credentials.from_authorized_user_file(str(GMAIL_TOKEN))
    if creds.expired and creds.refresh_token:
        creds.refresh(Request())
        GMAIL_TOKEN.write_text(creds.to_json())
    return build('gmail', 'v1', credentials=creds)


def _gmail_send(subject: str, body_text: str, body_html: str,
                to_emails: list, attachment: Path,
                cc_emails: list = None) -> None:
    msg = MIMEMultipart('mixed')
    msg['Subject'] = subject
    msg['To'] = ', '.join(to_emails)
    if cc_emails:
        msg['Cc'] = ', '.join(cc_emails)

    alt = MIMEMultipart('alternative')
    alt.attach(MIMEText(body_text, 'plain'))
    alt.attach(MIMEText(body_html, 'html'))
    msg.attach(alt)

    with open(attachment, 'rb') as f:
        part = MIMEBase('application',
                        'vnd.openxmlformats-officedocument.spreadsheetml.sheet')
        part.set_payload(f.read())
    encoders.encode_base64(part)
    part.add_header('Content-Disposition', 'attachment',
                    filename=attachment.name)
    msg.attach(part)

    raw = base64.urlsafe_b64encode(msg.as_bytes()).decode()
    service = _gmail_service()
    service.users().messages().send(userId='me', body={'raw': raw}).execute()
    recipients = list(to_emails) + list(cc_emails or [])
    logger.info(f"Sent '{subject}' to {recipients}")


def run(report_date: date, dry_run: bool = False) -> Path:
    # 1. Generate the xlsx into output/reports/
    generated = generate_cash_prices(report_date)
    logger.info(f"Generated: {generated}")

    # 2. Copy to shared Dropbox folder
    SHARED_DROPBOX.mkdir(parents=True, exist_ok=True)
    shared_path = SHARED_DROPBOX / generated.name
    shutil.copy2(generated, shared_path)
    logger.info(f"Copied to shared: {shared_path}")

    if dry_run:
        logger.info("--dry-run: skipping email")
        return shared_path

    # 3. Email Felipe with attachment, cc Tore
    date_str = report_date.strftime("%B %d, %Y")
    subject = f"Weekly Cash Prices - {date_str}"
    body_text = (
        f"Hi Felipe,\n\n"
        f"Attached are this week's cash prices for the HB Weekly Report, "
        f"dated {date_str}.\n\n"
        f"Also saved to: HigbyBarrett\\weekly_cash_prices\\{generated.name}\n\n"
        f"This is an automated send — let Tore know if anything looks off, "
        f"in particular any blank rows where the DB couldn't fill a value.\n\n"
        f"-RLC Data Pipeline"
    )
    body_html = body_text.replace("\n", "<br>")
    _gmail_send(
        subject=subject,
        body_text=body_text,
        body_html=body_html,
        to_emails=[FELIPE_EMAIL],
        cc_emails=[TORE_EMAIL],
        attachment=shared_path,
    )
    return shared_path


def main():
    p = argparse.ArgumentParser(description="Email weekly cash prices to Felipe")
    p.add_argument("--date", help="Report date YYYY-MM-DD (default: today)")
    p.add_argument("--dry-run", action="store_true",
                   help="Generate + copy but do not send email")
    p.add_argument("--verbose", "-v", action="store_true")
    args = p.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    report_date = (
        datetime.strptime(args.date, "%Y-%m-%d").date()
        if args.date else date.today()
    )
    run(report_date, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
