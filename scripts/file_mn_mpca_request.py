"""
File the bundled MN MPCA Information Request.

Steps:
  1. Render the request body (everything BEFORE the "Suggested message" section)
     to a clean PDF at the Dropbox Helios-adjacent folder.
  2. Email the PDF to Sean Bryant (MPCA intake) with cc to onlineservices.pca
     and Tore, using the body text from the "Suggested message" section.

The PDF is the artifact for the file; the email is the filing channel.
"""
import base64
import os
import re
import sys
from datetime import datetime
from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path

sys.stdout.reconfigure(encoding='utf-8', line_buffering=True)

from dotenv import load_dotenv
load_dotenv()

# PDF rendering via docx -> ReportLab. Simplest path that handles tables:
# use reportlab for layout from scratch given we control the source markdown.
from reportlab.lib.pagesizes import LETTER
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from reportlab.lib import colors
from reportlab.platypus import (
    SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, PageBreak,
)
from reportlab.lib.enums import TA_LEFT

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build

# ---------------------------------------------------------------------------
# Paths and constants
# ---------------------------------------------------------------------------

SRC = Path(r"C:\dev\RLC-Agent\docs\specs\mn_mpca_information_request.md")
OUT_DIR = Path(r"C:\Users\torem\RLC Dropbox\Tore Alden\Misc Personal Stuff\Helios").parent / "MN MPCA"
OUT_DIR.mkdir(parents=True, exist_ok=True)
PDF_PATH = OUT_DIR / f"MN_MPCA_Information_Request_Bundle_{datetime.now():%Y%m%d}.pdf"

GMAIL_TOKEN = Path(
    r"C:\Users\torem\RLC Dropbox\RLC Team Folder\RLC Documents"
    r"\LLM Model and Documents\Projects\Desktop Assistant\token_work.json"
)
SEAN_EMAIL = "sean.bryant@state.mn.us"
MPCA_E_SERVICES = "onlineservices.pca@state.mn.us"
TORE_EMAIL = "tore.alden@roundlakescommodities.com"

# ---------------------------------------------------------------------------
# Read source markdown and split into request body vs. suggested email message
# ---------------------------------------------------------------------------

md = SRC.read_text(encoding="utf-8")

# Split at "## Suggested message to paste"
SPLIT_MARKER = "## Suggested message to paste"
if SPLIT_MARKER not in md:
    raise RuntimeError(f"Split marker not found in source: {SPLIT_MARKER!r}")
body_md, rest_md = md.split(SPLIT_MARKER, 1)

# Extract the blockquoted suggested message
m = re.search(r"^>\s*\*\*Subject:\*\*\s*(.+?)$", rest_md, re.M)
if not m:
    raise RuntimeError("Could not extract Subject from suggested message")
subject = m.group(1).strip()

# Pull quoted body lines (lines starting with "> ")
quoted = [ln[2:] for ln in rest_md.splitlines() if ln.startswith("> ")]
# Drop the Subject line itself; that's separate
quoted = [ln for ln in quoted if not ln.startswith("**Subject:")]
# Strip leading bold/markdown artifacts
email_body_text = "\n".join(quoted).strip()
# Convert markdown bold/italic markers out for plain text
email_body_text = re.sub(r"\*\*(.+?)\*\*", r"\1", email_body_text)
email_body_text = re.sub(r"\*(.+?)\*", r"\1", email_body_text)
# Convert links: [text](url) -> text (url)
email_body_text = re.sub(r"\[([^\]]+)\]\(([^)]+)\)", r"\1 (\2)", email_body_text)

# ---------------------------------------------------------------------------
# Build PDF from body_md
# ---------------------------------------------------------------------------

styles = getSampleStyleSheet()
h1 = ParagraphStyle('H1', parent=styles['Heading1'], fontName='Times-Bold',
                    fontSize=16, spaceAfter=6, spaceBefore=0)
h2 = ParagraphStyle('H2', parent=styles['Heading2'], fontName='Times-Bold',
                    fontSize=12, spaceAfter=4, spaceBefore=10)
h3 = ParagraphStyle('H3', parent=styles['Heading3'], fontName='Times-Bold',
                    fontSize=11, spaceAfter=3, spaceBefore=8)
body = ParagraphStyle('Body', parent=styles['BodyText'], fontName='Times-Roman',
                      fontSize=10, leading=13, spaceAfter=4, alignment=TA_LEFT)
small = ParagraphStyle('Small', parent=body, fontSize=9, leading=11)


def md_inline(s):
    """Render markdown bold + italic to ReportLab markup."""
    s = re.sub(r"\*\*(.+?)\*\*", r"<b>\1</b>", s)
    s = re.sub(r"\*(.+?)\*", r"<i>\1</i>", s)
    s = re.sub(r"`(.+?)`", r"<font face='Courier'>\1</font>", s)
    s = re.sub(r"\[([^\]]+)\]\(([^)]+)\)", r'<a href="\2">\1</a>', s)
    return s


flowables = []
lines = body_md.splitlines()
i = 0
while i < len(lines):
    line = lines[i]
    stripped = line.strip()

    if not stripped:
        i += 1
        continue

    # H1: # Title
    if stripped.startswith("# "):
        flowables.append(Paragraph(md_inline(stripped[2:]), h1))
        i += 1
        continue
    # H2: ## Section
    if stripped.startswith("## "):
        flowables.append(Paragraph(md_inline(stripped[3:]), h2))
        i += 1
        continue
    # H3: ### Subsection
    if stripped.startswith("### "):
        flowables.append(Paragraph(md_inline(stripped[4:]), h3))
        i += 1
        continue
    # Horizontal rule
    if stripped == "---":
        flowables.append(Spacer(1, 0.1 * inch))
        i += 1
        continue
    # Markdown table starting with header row
    if stripped.startswith("|") and i + 1 < len(lines) and "---" in lines[i + 1]:
        # Read header row
        header = [c.strip() for c in stripped.strip("|").split("|")]
        i += 2  # skip separator
        rows = []
        while i < len(lines) and lines[i].strip().startswith("|"):
            rows.append([c.strip() for c in lines[i].strip().strip("|").split("|")])
            i += 1
        data = [[Paragraph(md_inline(c), small) for c in header]]
        for r in rows:
            data.append([Paragraph(md_inline(c), small) for c in r])
        # Compute column widths — distribute over usable width (6.5 in)
        ncols = len(header)
        usable = 6.5 * inch
        widths = [usable / ncols] * ncols
        t = Table(data, colWidths=widths, repeatRows=1)
        t.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor("#E8E8E8")),
            ('FONTNAME', (0, 0), (-1, 0), 'Times-Bold'),
            ('GRID', (0, 0), (-1, -1), 0.25, colors.grey),
            ('VALIGN', (0, 0), (-1, -1), 'TOP'),
            ('LEFTPADDING', (0, 0), (-1, -1), 4),
            ('RIGHTPADDING', (0, 0), (-1, -1), 4),
            ('TOPPADDING', (0, 0), (-1, -1), 3),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 3),
        ]))
        flowables.append(t)
        flowables.append(Spacer(1, 0.08 * inch))
        continue
    # Numbered list
    m = re.match(r"^(\d+)\.\s+(.+)", stripped)
    if m:
        flowables.append(Paragraph(f"{m.group(1)}. {md_inline(m.group(2))}", body))
        i += 1
        continue
    # Bullet
    if stripped.startswith("- "):
        flowables.append(Paragraph(f"&bull; {md_inline(stripped[2:])}", body))
        i += 1
        continue
    # Plain paragraph
    flowables.append(Paragraph(md_inline(stripped), body))
    i += 1


doc = SimpleDocTemplate(
    str(PDF_PATH),
    pagesize=LETTER,
    leftMargin=0.75 * inch,
    rightMargin=0.75 * inch,
    topMargin=0.7 * inch,
    bottomMargin=0.7 * inch,
    title="MN MPCA Information Request — Round Lake Companies",
    author="Tore Alden",
)
doc.build(flowables)
print(f"Wrote PDF: {PDF_PATH}")
print(f"  size: {PDF_PATH.stat().st_size:,} bytes")

# ---------------------------------------------------------------------------
# Send the email via Gmail API
# ---------------------------------------------------------------------------

creds = Credentials.from_authorized_user_file(str(GMAIL_TOKEN))
if creds.expired and creds.refresh_token:
    creds.refresh(Request())
    GMAIL_TOKEN.write_text(creds.to_json())
svc = build('gmail', 'v1', credentials=creds)

msg = MIMEMultipart('mixed')
msg['Subject'] = subject
msg['To'] = SEAN_EMAIL
msg['Cc'] = f"{MPCA_E_SERVICES}, {TORE_EMAIL}"

# Body
alt = MIMEMultipart('alternative')
alt.attach(MIMEText(email_body_text, 'plain'))
alt.attach(MIMEText(email_body_text.replace("\n", "<br>"), 'html'))
msg.attach(alt)

# Attachment
with open(PDF_PATH, 'rb') as f:
    part = MIMEBase('application', 'pdf')
    part.set_payload(f.read())
encoders.encode_base64(part)
part.add_header('Content-Disposition', 'attachment', filename=PDF_PATH.name)
msg.attach(part)

raw = base64.urlsafe_b64encode(msg.as_bytes()).decode()
result = svc.users().messages().send(userId='me', body={'raw': raw}).execute()
print(f"\nSent email:")
print(f"  Message-Id: {result.get('id')}")
print(f"  Subject:    {subject}")
print(f"  To:         {SEAN_EMAIL}")
print(f"  Cc:         {MPCA_E_SERVICES}, {TORE_EMAIL}")
print(f"  Attachment: {PDF_PATH.name}")
