import os
import base64
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
import requests  # For Ollama API

# Scopes for read-only access (update if your token needs refreshing)
SCOPES = ['https://www.googleapis.com/auth/gmail.readonly']

def get_gmail_service(token_file='token.json', creds_file='credentials.json'):
    creds = None
    if os.path.exists(token_file):
        creds = Credentials.from_authorized_user_file(token_file, SCOPES)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(creds_file, SCOPES)
            creds = flow.run_local_server(port=0)
        with open(token_file, 'w') as token:
            token.write(creds.to_json())
    return build('gmail', 'v1', credentials=creds)

def get_email_body(service, msg_id):
    msg = service.users().messages().get(userId='me', id=msg_id, format='full').execute()
    parts = msg['payload'].get('parts', [])
    body = ''
    for part in parts:
        if part['mimeType'] == 'text/plain':
            body = base64.urlsafe_b64decode(part['body']['data']).decode('utf-8')
            break
    return body.strip()[:1000]  # Limit to ~1000 chars to avoid overwhelming the model

def summarize_with_ollama(text, model='gemma3:4b'):
    prompt = f"Summarize this email in 2-3 sentences: {text}"
    response = requests.post("http://localhost:11434/api/generate", json={"model": model, "prompt": prompt, "stream": False})
    if response.status_code == 200:
        return response.json()['response'].strip()
    return "Summary error—check Ollama server."

# Fetch and summarize for an account
def fetch_and_summarize_emails(service, label='INBOX', max_results=5, header='Recent emails:'):
    results = service.users().messages().list(userId='me', labelIds=[label], maxResults=max_results).execute()
    messages = results.get('messages', [])
    print(header)
    for msg in messages:
        msg_data = service.users().messages().get(userId='me', id=msg['id'], format='metadata', metadataHeaders=['Subject', 'From']).execute()
        subject = next((h['value'] for h in msg_data['payload']['headers'] if h['name'] == 'Subject'), 'No Subject')
        from_email = next((h['value'] for h in msg_data['payload']['headers'] if h['name'] == 'From'), 'Unknown')
        body = get_email_body(service, msg['id'])
        summary = summarize_with_ollama(body)
        print(f"From: {from_email}\nSubject: {subject}\nSummary: {summary}\n---")

# Main execution (update with your personal/work emails if separate services)
personal_service = get_gmail_service('token_personal.json', credentials_personal.json) # Adjust file names
workspace_service = get_gmail_service('token_workspace.json', 'credentials.json')

fetch_and_summarize_emails(workspace_service, header="Recent Workspace emails:")
fetch_and_summarize_emails(personal_service, header="Recent Personal emails:")