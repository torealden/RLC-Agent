import os
import base64
import json
import csv
from datetime import datetime
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from langchain.agents import initialize_agent, Tool, AgentType
from langchain_community.llms import Ollama
from langchain_community.tools.gmail.utils import build_resource_service
from langchain_community.tools.gmail.search import GmailSearch
from langchain_community.tools.gmail.get_message import GmailGetMessage
from langchain_google_community import CalendarToolkit
import re
import html as ihtml

# Constants for credentials and scopes
CREDENTIALS_PERSONAL_FILE = "credentials_personal.json"
CREDENTIALS_WORK_FILE = "credentials.json"
TOKEN_PERSONAL_FILE = "token_personal.json"
TOKEN_WORK_FILE = "token_workspace.json"
SCOPES_GMAIL = ['https://www.googleapis.com/auth/gmail.readonly',
                'https://www.googleapis.com/auth/gmail.modify',
                'https://www.googleapis.com/auth/gmail.metadata']
SCOPES_WORK = SCOPES_GMAIL + ['https://www.googleapis.com/auth/calendar.events']
SCOPES_PERSONAL = SCOPES_GMAIL

# Ollama model
OLLAMA_MODEL = "gemma:7b"  # Adjust as needed

def setup_credentials(credentials_file, scopes, token_file, port=8080):
    creds = None
    if os.path.exists(token_file):
        try:
            creds = Credentials.from_authorized_user_file(token_file, scopes)
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
        except ValueError:
            creds = None  # Force reauth on invalid token
    if not creds or not creds.valid:
        flow = InstalledAppFlow.from_client_secrets_file(credentials_file, scopes)
        flow.redirect_uri = f'http://localhost:{port}/'
        auth_url, _ = flow.authorization_url(access_type='offline', prompt='select_account consent')
        print("Please visit this URL to authorize this application: " + auth_url)
        creds = flow.run_local_server(port=port, open_browser=False)
        with open(token_file, 'w') as token:
            token.write(creds.to_json())
    return creds

# Setup credentials
print("Authorizing workspace account...")
creds_workspace = setup_credentials(CREDENTIALS_WORK_FILE, SCOPES_WORK, TOKEN_WORK_FILE, port=8080)
print("Workspace account authorized successfully.")

print("Authorizing personal account...")
creds_personal = setup_credentials(CREDENTIALS_PERSONAL_FILE, SCOPES_PERSONAL, TOKEN_PERSONAL_FILE, port=8081)
print("Personal account authorized successfully.")

# Build API resources
api_workspace = build_resource_service(credentials=creds_workspace)
api_personal = build_resource_service(credentials=creds_personal)

# Gmail tools
workspace_search = GmailSearch(api_resource=api_workspace)
workspace_search.name = "workspace_gmail_search"
workspace_search.description = "Search for recent emails in workspace Gmail account."

workspace_get = GmailGetMessage(api_resource=api_workspace)
workspace_get.name = "workspace_gmail_get_message"
workspace_get.description = "Get full details of an email by ID from workspace Gmail."

personal_search = GmailSearch(api_resource=api_personal)
personal_search.name = "personal_gmail_search"
personal_search.description = "Search for recent emails in personal Gmail account."

personal_get = GmailGetMessage(api_resource=api_personal)
personal_get.name = "personal_gmail_get_message"
personal_get.description = "Get full details of an email by ID from personal Gmail."

# Calendar tools (workspace only)
calendar_toolkit = CalendarToolkit(credentials=creds_workspace)
calendar_tools = calendar_toolkit.get_tools()

# All tools for agent
tools = [
    workspace_search,
    workspace_get,
    personal_search,
    personal_get,
] + calendar_tools

# Ollama LLM
llm = Ollama(model=OLLAMA_MODEL)

# Initialize agent
agent = initialize_agent(
    tools, 
    llm, 
    agent_type=AgentType.ZERO_SHOT_REACT_DESCRIPTION,
    verbose=True
)

def strip_html_tags(html):
    """Remove HTML tags and decode HTML entities from a string."""
    text = re.sub('<[^<]+?>', '', html)  # remove HTML tags
    text = ihtml.unescape(text)          # convert HTML entities
    return text

# Enhanced prompt for agent to handle summarization and detailed proposals
prompt_template = """
Check the last 5 emails from {account_type} Gmail account.
For each email, retrieve the ID, subject, sender, and a 1-2 sentence summary of the body.
Then, propose an action: 
- Delete the email
- Mark as spam
- No action needed
- Draft a response (provide the draft text)
- Schedule event (provide event details as JSON: {{summary: str, start: str (ISO), end: str (ISO)}})
- Other (specify)
Output in structured JSON format for each email, e.g.:
[{{"id": "msg_id", "sender": "name", "subject": "text", "summary": "1-2 sentences", "proposed_action": "action type", "details": "rationale or draft/event JSON"}}]
Do not execute actions; propose for approval.
If an email mentions a meeting, use calendar tools to check availability if needed.
"""

def process_emails(account_type, api_resource):
    """Process emails for a given account using the agent."""
    prompt = prompt_template.format(account_type=account_type)
    result = agent.run(prompt)
    try:
        emails = json.loads(result)  # Assume agent outputs JSON list
    except json.JSONDecodeError:
        print(f"Error parsing agent output for {account_type}: {result}")
        emails = []
    return emails

def prompt_user_for_approval(email):
    """Prompt user for action approval."""
    print(f"\nEmail from {email['sender']} – Subject: {email['subject']}")
    print(f"Summary: {email['summary']}")
    print(f"Proposed Action: {email['proposed_action']} ({email['details']})")
    choice = input("Approve this action? [y/n]: ").strip().lower()
    return choice == 'y'

# Action execution stubs (implement full API calls as needed)
def execute_delete(service, msg_id):
    """Stub: Delete email (move to Trash)."""
    # TODO: service.users().messages().trash(userId='me', id=msg_id).execute()
    print(f"(Would delete email ID: {msg_id})")

def execute_mark_spam(service, msg_id):
    """Stub: Mark as spam."""
    # TODO: service.users().messages().modify(userId='me', id=msg_id, body={'addLabelIds': ['SPAM']}).execute()
    print(f"(Would mark email ID: {msg_id} as spam)")

def execute_draft_response(service, msg_id, draft_text):
    """Stub: Create draft response."""
    # TODO: Use Gmail API to create draft (e.g., build message and service.users().drafts().create(...))
    print(f"(Would create draft for email ID: {msg_id} with text: {draft_text})")

def execute_schedule_event(creds, event_details):
    """Stub: Schedule calendar event (workspace only)."""
    # TODO: calendar_service = build('calendar', 'v3', credentials=creds)
    # calendar_service.events().insert(calendarId='primary', body=event_details).execute()
    print(f"(Would schedule event: {event_details})")

def execute_no_action():
    """No action."""
    print("(No action taken)")

def log_decision(log_file, email, approved):
    """Log decision to CSV."""
    decision_str = "approved" if approved else "rejected"
    file_exists = os.path.exists(log_file)
    with open(log_file, 'a', newline='', encoding='utf-8') as csvfile:
        fieldnames = ["timestamp", "sender", "subject", "summary", "proposed_action", "details", "decision"]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        if not file_exists:
            writer.writeheader()
        writer.writerow({
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "sender": email["sender"],
            "subject": email["subject"],
            "summary": email["summary"],
            "proposed_action": email["proposed_action"],
            "details": email["details"],
            "decision": decision_str
        })

def main():
    log_file = "email_actions_log.csv"
    
    # Process personal emails
    print("\nProcessing Personal Account Emails...")
    personal_emails = process_emails("personal", api_personal)
    for email in personal_emails:
        approved = prompt_user_for_approval(email)
        service = build('gmail', 'v1', credentials=creds_personal)  # For execution
        if approved:
            action = email['proposed_action'].lower()
            if action == "delete":
                execute_delete(service, email['id'])
            elif action == "mark as spam":
                execute_mark_spam(service, email['id'])
            elif action.startswith("draft"):
                execute_draft_response(service, email['id'], email['details'])
            elif action == "schedule event":
                print("Scheduling not available for personal; skipping.")
                execute_no_action()
            else:
                execute_no_action()
        else:
            print(f"Action rejected for email ID: {email['id']}")
        log_decision(log_file, email, approved)
    
    # Process workspace emails
    print("\nProcessing Work Account Emails...")
    workspace_emails = process_emails("workspace", api_workspace)
    for email in workspace_emails:
        approved = prompt_user_for_approval(email)
        service = build('gmail', 'v1', credentials=creds_workspace)
        if approved:
            action = email['proposed_action'].lower()
            if action == "delete":
                execute_delete(service, email['id'])
            elif action == "mark as spam":
                execute_mark_spam(service, email['id'])
            elif action.startswith("draft"):
                execute_draft_response(service, email['id'], email['details'])
            elif action == "schedule event":
                try:
                    event_details = json.loads(email['details'])
                    execute_schedule_event(creds_workspace, event_details)
                except json.JSONDecodeError:
                    print("Invalid event details; skipping.")
            else:
                execute_no_action()
        else:
            print(f"Action rejected for email ID: {email['id']}")
        log_decision(log_file, email, approved)

if __name__ == "__main__":
    main()

# Note: For full execution, implement TODOs with actual API calls. Integrate with master agent by exposing process_emails() or running on schedule. Use logged CSV to train/fine-tune agent preferences over time.