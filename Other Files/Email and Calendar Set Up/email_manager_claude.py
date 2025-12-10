import os
import base64
import json
import csv
from datetime import datetime
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
SCOPES = [
    'https://www.googleapis.com/auth/gmail.readonly',
    'https://www.googleapis.com/auth/gmail.modify',
    'https://www.googleapis.com/auth/gmail.metadata'
    'https://www.googleapis.com/auth/calendar.events'
    ]

# Ollama model
OLLAMA_MODEL = "gemma:7b"  # Adjust as needed

def setup_credentials(credentials_file, scopes, token_file, port=8080):
    """Setup OAuth2 credentials with better error handling"""
    creds = None
    
    #Try to load existing token
    if os.path.exists(token_file):
        try:
            creds = Credentials.from_authorized_user_file(token_file, scopes)
        except Exception as e:
            print(f"Error loading token from {token_file}: {e}")
            creds = None
            
    #Check validity of credentials
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            try:
                creds.refresh(Request())
                print ("Token refreshed successfully")
            except Exception as e:
                print(f"Error refreshing token:{e}")
                creds = None
                
        #If credentials are still not valid, run OAuth flow
        if not creds:
            try:
                flow = InstalledAppFlow.from_client_secrets_file(
                    credentials_file, scopes
                )
                #Fix: Use separate prompt values
                creds = flow.run_local_server(
                    port=port,
                    authorization_prompt_message='Please visit this URL to authorize this application: {url}',
                    success_message='The auth flow is complete; you may close this window.',
                    open_browser=True
                    )
            
                #Save the credentials for next run
                with open(token_file, 'w') as token:
                    token.write(creds.to_json())
                print(f"New token save to {token_file}")
                
            except Exception as e:
                print(f"Error during OAuth flow: {e}")
                raise
        
    return creds

#Calendar tools function (simplified without langchain_google_community)
def create_calendar_tools(credentials):
    """Create calendar tools manually without langchain_google_community."""
    calendar_service = build('calendar', 'v3', credentials=credentials)
    
    def check_availability(date_string):
        """Check calendar availability for a given date."""
        try:
            #Parse the date string and create time bounds
            from datetime import datetime, timedelta
            date = datetime.fromisoformat(date_string)
            time_min = date.isoformat() + 'Z'
            time_max = (date + timedelta(days=1)).isoformat() + 'Z'
            
            events_result = calendar_service.events().list(
                calendarID='primary',
                timeMin=time_min,
                timeMax=time_max,
                singleEvents=True,
                orderBy='starttime'
            ).execute()
            
            events = events_result.get('items', [])
            return f"Found {len(events)} events on {date_string}"
        except Exception as e:
            return f"Error checking calendar: {str(e)}"
        
    def create_event(event_data):
        """Create a calendar event."""
        try:
            event = calendar_service.events().insert(
                calendarID='primary',
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

if __name__ == "__main__":
    main()           
  