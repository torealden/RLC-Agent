"""Read recent inbox messages from the work Gmail account.
Filters: MPCA / MN / state environmental / regulatory replies first.
"""
import base64
import os
import sys
from pathlib import Path

sys.stdout.reconfigure(encoding="utf-8", line_buffering=True)
from dotenv import load_dotenv; load_dotenv()
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build

TOKEN = Path(r"C:\Users\torem\RLC Dropbox\RLC Team Folder\RLC Documents"
             r"\LLM Model and Documents\Projects\Desktop Assistant\token_work.json")

creds = Credentials.from_authorized_user_file(str(TOKEN))
if creds.expired and creds.refresh_token:
    creds.refresh(Request())
    TOKEN.write_text(creds.to_json())
svc = build('gmail', 'v1', credentials=creds)

# Search MPCA-like senders + general recent
queries = [
    ('MPCA / MN Pollution Control', 'from:(mn.gov OR mpca OR pca.state.mn.us) newer_than:7d'),
    ('Other state env (.gov, .us)',  'from:(.gov OR .us) newer_than:2d'),
    ('Yesterday + today INBOX',       'in:inbox newer_than:2d'),
]


def show_message(msg_id, prefix=""):
    msg = svc.users().messages().get(userId='me', id=msg_id, format='full').execute()
    headers = {h['name']: h['value'] for h in msg['payload']['headers']}
    print(f"\n{prefix}---")
    print(f"{prefix}From:    {headers.get('From','?')}")
    print(f"{prefix}To:      {headers.get('To','?')[:80]}")
    print(f"{prefix}Subject: {headers.get('Subject','?')}")
    print(f"{prefix}Date:    {headers.get('Date','?')}")

    # Extract body (plain text part if present)
    def find_text(part):
        if part.get('mimeType') == 'text/plain' and 'data' in part.get('body', {}):
            return base64.urlsafe_b64decode(part['body']['data']).decode('utf-8', errors='ignore')
        for p in part.get('parts', []) or []:
            t = find_text(p)
            if t:
                return t
        return None

    body = find_text(msg['payload']) or msg.get('snippet', '')
    body = body.replace('\r\n', '\n').strip()
    # Trim quoted history at the obvious markers
    for marker in ['\n-----Original Message-----', '\nFrom:', '\n________________________________']:
        idx = body.find(marker)
        if idx > 200:
            body = body[:idx]
            break
    print(f"{prefix}Body:")
    for ln in body.splitlines()[:60]:
        print(f"{prefix}    {ln}")


for label, q in queries:
    print(f"\n========= {label}  ({q})")
    try:
        result = svc.users().messages().list(userId='me', q=q, maxResults=10).execute()
        msgs = result.get('messages', [])
        if not msgs:
            print("  (no messages)")
            continue
        for m in msgs:
            show_message(m['id'], prefix="  ")
    except Exception as e:
        print(f"  ERROR: {type(e).__name__}: {e}")
