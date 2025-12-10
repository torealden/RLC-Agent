import os
import base64
import json
import csv
from datetime import datetime, timedelta
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import pickle
import re
import html as ihtml

#For LangChain - use updated imports
from langchain.agents import initialize_agent, Tool, AgentType
from langchain_community.llms import Ollama
from langchain_community.tools.gmail.utils import build_resource_service
from langchain_community.tools.gmail.search import GmailSearch
from langchain_community.tools.gmail.get_message import GmailGetMessage

# Constants for credentials and scopes
CREDENTIALS_PERSONAL_FILE = "credentials_personal.json"
CREDENTIALS_WORK_FILE = "credentials.json"
TOKEN_PERSONAL_FILE = "token_personal.json"
TOKEN_WORK_FILE = "token_workspace.json"

#Fixed SCOPES with all necessary scopes
SCOPES = [
    'https://www.googleapis.com/auth/gmail.readonly',
    'https://www.googleapis.com/auth/gmail.modify',
    'https://www.googleapis.com/auth/gmail.metadata',
    'https://www.googleapis.com/auth/calendar.events',
    'openid', #add this for basic auth
    'https://www.googleapis.com/auth/userinfo.email',#add this for email info
    ]

# Ollama model
OLLAMA_MODEL = "gemma:7b"  # Adjust as needed

def verify_credentials_file(credentials_file):
    """Verify the credentials file has correct client type."""
    try:
        with open(credentials_file, 'r') as f:
            creds_data = json.load(f)
            
        # Check if it's the right type of credentials
        if 'installed' not in creds_data and 'web' not in creds_data:
            print(f"ERROR: {credentials_file} doesn't appear to be a valid OAuth client credentials file.")
            print("Make sure you downloaded the OAuth 2.0 Client ID (not Service Account key)")
            return False
            
        # For installed apps, check redirect URIs
        if 'installed' in creds_data:
            print(f"✓ Found installed app credentials in {credentials_file}")
        elif 'web' in creds_data:
            print(f"✓ Found web app credentials in {credentials_file}")
            redirect_uris = creds_data['web'].get('redirect_uris', [])
            print(f"  Configured redirect URIs: {redirect_uris}")
            
        return True
    except Exception as e:
        print(f"ERROR reading {credentials_file}: {e}")
        return False
    
def setup_credentials(credentials_file, scopes, token_file, port=8080):
    """Setup OAuth2 credentials with detailed error reporting."""
    
    # First verify the credentials file
    if not verify_credentials_file(credentials_file):
        raise ValueError(f"Invalid credentials file: {credentials_file}")
    
    creds = None
    
    # Try to load existing token
    if os.path.exists(token_file):
        try:
            creds = Credentials.from_authorized_user_file(token_file, scopes)
            print(f"✓ Loaded existing token from {token_file}")
        except Exception as e:
            print(f"⚠ Could not load token from {token_file}: {e}")
            creds = None
    
    # Check if credentials are valid
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            try:
                print("Attempting to refresh expired token...")
                creds.refresh(Request())
                print("✓ Token refreshed successfully")
            except Exception as e:
                print(f"⚠ Could not refresh token: {e}")
                creds = None
        
        # If still no valid credentials, run OAuth flow
        if not creds:
            try:
                print(f"\nStarting OAuth flow on port {port}...")
                print(f"Requesting scopes: {scopes}")
                
                flow = InstalledAppFlow.from_client_secrets_file(
                    credentials_file, scopes
                )
                
                # Run the local server
                creds = flow.run_local_server(
                    port=port,
                    authorization_prompt_message='Please visit this URL to authorize: {url}',
                    success_message='Authorization complete! You may close this window.',
                    open_browser=True,
                    timeout_seconds=120  # Add timeout
                )
                
                # Save the credentials
                with open(token_file, 'w') as token:
                    token.write(creds.to_json())
                print(f"✓ New token saved to {token_file}")
                
            except Exception as e:
                print(f"\n❌ OAuth flow failed: {e}")
                print("\nTroubleshooting steps:")
                print("1. Check that Gmail API is enabled in Google Cloud Console")
                print("2. Check that Calendar API is enabled in Google Cloud Console")
                print("3. Verify OAuth consent screen is configured with the required scopes")
                print("4. Make sure your email is added as a test user (if app is in testing mode)")
                print("5. Try using a different port if {port} is blocked")
                raise
    
    # Verify the credentials work
    try:
        # Test Gmail API
        gmail_service = build('gmail', 'v1', credentials=creds)
        profile = gmail_service.users().getProfile(userId='me').execute()
        print(f"✓ Successfully authenticated as: {profile['emailAddress']}")
        
        # Test Calendar API if calendar scope is included
        if 'https://www.googleapis.com/auth/calendar.events' in scopes:
            cal_service = build('calendar', 'v3', credentials=creds)
            cal_list = cal_service.calendarList().list(maxResults=1).execute()
            print(f"✓ Calendar API access confirmed")
            
    except HttpError as e:
        print(f"⚠ API test failed: {e}")
        if e.resp.status == 403:
            print("This usually means the API is not enabled or scopes are not authorized")
    
    return creds

#Calendar tools function (simplified without langchain_google_community)
def create_calendar_tools(credentials):
    """Create calendar tools manually without langchain_google_community."""
    calendar_service = build('calendar', 'v3', credentials=credentials)
    
    def check_availability(date_string):
        """Check calendar availability for a given date."""
        try:
            #Parse the date string and create time bounds
            date = datetime.fromisoformat(date_string)
            time_min = date.isoformat() + 'Z'
            time_max = (date + timedelta(days=1)).isoformat() + 'Z'
            
            events_result = calendar_service.events().list(
                calendarId='primary',
                timeMin=time_min,
                timeMax=time_max,
                singleEvents=True,
                orderBy='startTime'
            ).execute()
            
            events = events_result.get('items', [])
            return f"Found {len(events)} events on {date_string}"
        except Exception as e:
            return f"Error checking calendar: {str(e)}"
        
    def create_event(event_data):
        """Create a calendar event."""
        try:
            event = calendar_service.events().insert(
                calendarId='primary',
                body=event_data
            ).execute()
            return f"Event created: {event.get('htmlLink')}"
        except Exception as e:
            return f"Error creating event. {str(e)}"
        
    return [
        Tool(
            name="check_calendar_availability",
            description="Check calendar availability for a specific date",
            func=check_availability
        ),
        Tool(
            name="create_calendar_event",
            description="Create a new calendar event",
            func=create_event
        )
    ]            
    
#Enhanced prompt with better structure for Gemma
prompt_template = """
Task: Check the last 5 emails from {account_type} Gmail account.

For each email, provide:
1. Email ID
2. Sender name/email
3. Subject line
4. Brief summary (1-2 sentences)
5. Proposed action (choose one):
    - delete
    - spam
    - no_action
    - draft_response
    - schedule_event
    - other
    
Format your response as a list with clear separators.
Example:
Email 1:
ID: abc123
SENDER: john@example.com
SUBJECT: Meeting Tomorrow
SUMMARY: Reminder about tomorrow's meeting at 3pm.
ACTION: schedule_event
DETAILS: Meeting at 3pm tomorrow

Do not execute any actions, only propose them.
"""
            
def parse_agent_output(output):
    """Parse agent output more robustly."""
    emails = []
    
    # Split by EMAIL markers
    email_blocks = re.split(r'EMAIL \d+:', output)
    
    for block in email_blocks[1:]:  # Skip first empty element
        try:
            lines = block.strip().split('\n')
            email_data = {}
            
            for line in lines:
                if line.startswith('ID:'):
                    email_data['id'] = line.split(':', 1)[1].strip()
                elif line.startswith('SENDER:'):
                    email_data['sender'] = line.split(':', 1)[1].strip()
                elif line.startswith('SUBJECT:'):
                    email_data['subject'] = line.split(':', 1)[1].strip()
                elif line.startswith('SUMMARY:'):
                    email_data['summary'] = line.split(':', 1)[1].strip()
                elif line.startswith('ACTION:'):
                    email_data['proposed_action'] = line.split(':', 1)[1].strip()
                elif line.startswith('DETAILS:'):
                    email_data['details'] = line.split(':', 1)[1].strip()
            
            if 'id' in email_data:
                emails.append(email_data)
        except Exception as e:
            print(f"Error parsing email block: {e}")
            continue
    
    return emails

def prompt_user_for_approval(email):
    """Prompt user for action approval."""
    print(f"\nEmail from {email.get('sender', 'Unknown')} - Subject: {email.get('subject', 'No subject')}")
    print(f"Summary: {email.get('summary', 'No summary')}")
    print(f"Proposed Action: {email.get('proposed_action', 'none')} ({email.get('details', 'No details')})")
    choice = input("Approve this action? [y/n]: ").strip().lower()
    return choice == 'y'

def main():
    log_file = "email_actions_log.csv"
    
    try:
        # Setup credentials with better error handling
        print("Authorizing workspace account...")
        creds_workspace = setup_credentials(
            CREDENTIALS_WORK_FILE, 
            SCOPES, 
            TOKEN_WORK_FILE, 
            port=8080
        )
        print("Workspace account authorized successfully.")
        
        print("\nAuthorizing personal account...")
        creds_personal = setup_credentials(
            CREDENTIALS_PERSONAL_FILE, 
            SCOPES, 
            TOKEN_PERSONAL_FILE, 
            port=8081
        )
        print("Personal account authorized successfully.")
        
        # Build API resources
        api_workspace = build_resource_service(credentials=creds_workspace)
        api_personal = build_resource_service(credentials=creds_personal)
        
        # Create Gmail tools
        workspace_search = GmailSearch(api_resource=api_workspace)
        workspace_search.name = "workspace_gmail_search"
        workspace_search.description = "Search for emails in workspace Gmail."
        
        personal_search = GmailSearch(api_resource=api_personal)
        personal_search.name = "personal_gmail_search"
        personal_search.description = "Search for emails in personal Gmail."
        
        # Create calendar tools
        calendar_tools = create_calendar_tools(creds_workspace)
        
        # All tools for agent
        tools = [
            workspace_search,
            personal_search,
        ] + calendar_tools
        
        # Initialize Ollama LLM
        llm = Ollama(model=OLLAMA_MODEL)
        
        # Initialize agent
        agent = initialize_agent(
            tools, 
            llm, 
            agent_type=AgentType.ZERO_SHOT_REACT_DESCRIPTION,
            verbose=True,
            handle_parsing_errors=True
        )
        
        # Process emails
        print("\nProcessing emails...")
        
        # Run the agent with the prompt
        prompt = prompt_template.format(account_type="workspace")
        result = agent.run(prompt)
        
        # Parse the output
        emails = parse_agent_output(result)
        
        for email in emails:
            approved = prompt_user_for_approval(email)
            if approved:
                print(f"Action approved for email: {email.get('id', 'unknown')}")
            else:
                print(f"Action rejected for email: {email.get('id', 'unknown')}")
        
    except Exception as e:
        print(f"Error in main execution: {e}")
        import traceback
        traceback.print_exc()

# Quick test script
if __name__ == "__main__":
    # Test with just Gmail first
    GMAIL_SCOPES = [
        'https://www.googleapis.com/auth/gmail.readonly',
        'openid',
        'https://www.googleapis.com/auth/userinfo.email'
    ]
    
    print("Testing Gmail authentication...")
    try:
        creds = setup_credentials(
            "credentials.json",  # Your credentials file
            GMAIL_SCOPES,
            "token_test.json",
            port=8080
        )
        print("\n✅ Authentication successful!")
    except Exception as e:
        print(f"\n❌ Authentication failed: {e}")