#!/usr/bin/env python3
"""
Email Manager with Gmail and Calendar Integration
Fixed version with improved error handling and setup verification
"""

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
import sys

# For LangChain - use updated imports
try:
    from langchain.agents import initialize_agent, Tool, AgentType
    from langchain_community.llms import Ollama
    from langchain_community.tools.gmail.utils import build_resource_service
    from langchain_community.tools.gmail.search import GmailSearch
    from langchain_community.tools.gmail.get_message import GmailGetMessage
    LANGCHAIN_AVAILABLE = True
except ImportError as e:
    LANGCHAIN_AVAILABLE = False
    print(f"⚠ LangChain not fully installed: {e}")
    print("Install with: pip install langchain langchain-community")

# Constants for credentials and scopes
CREDENTIALS_PERSONAL_FILE = "credentials_personal.json"
CREDENTIALS_WORK_FILE = "credentials_desktop.json"
TOKEN_PERSONAL_FILE = "token_personal.json"
TOKEN_WORK_FILE = "token_workspace.json"

# Fixed SCOPES with all necessary scopes
SCOPES = [
    'https://www.googleapis.com/auth/gmail.readonly',
    'https://www.googleapis.com/auth/gmail.modify',
    'https://www.googleapis.com/auth/gmail.metadata',
    'https://www.googleapis.com/auth/calendar.events',
    'openid',
    'https://www.googleapis.com/auth/userinfo.email',
]

# Ollama model
OLLAMA_MODEL = "gemma:7b"  # Adjust as needed

def verify_setup():
    """Verify the setup before running the main application."""
    print("\n" + "="*60)
    print("Verifying Setup")
    print("="*60)
    
    issues = []
    
    # Check for credentials files
    if not os.path.exists(CREDENTIALS_WORK_FILE):
        issues.append(f"Missing work credentials file: {CREDENTIALS_WORK_FILE}")
    
    # Personal credentials are optional
    if not os.path.exists(CREDENTIALS_PERSONAL_FILE):
        print(f"⚠ Optional: Personal credentials file not found: {CREDENTIALS_PERSONAL_FILE}")
    
    # Check for Ollama
    if LANGCHAIN_AVAILABLE:
        try:
            from langchain_community.llms import Ollama
            test_llm = Ollama(model=OLLAMA_MODEL)
            print(f"✓ Ollama model configured: {OLLAMA_MODEL}")
        except Exception as e:
            issues.append(f"Ollama not available: {e}")
            print("  Install Ollama from: https://ollama.ai")
            print(f"  Then run: ollama pull {OLLAMA_MODEL}")
    
    if issues:
        print("\n❌ Setup issues found:")
        for issue in issues:
            print(f"  - {issue}")
        return False
    
    print("\n✓ Setup verification passed")
    return True

def verify_credentials_file(credentials_file):
    """Verify the credentials file has correct client type."""
    try:
        if not os.path.exists(credentials_file):
            print(f"⚠ Credentials file not found: {credentials_file}")
            return False
            
        with open(credentials_file, 'r') as f:
            creds_data = json.load(f)
            
        # Check if it's the right type of credentials
        if 'installed' not in creds_data and 'web' not in creds_data:
            print(f"ERROR: {credentials_file} doesn't appear to be a valid OAuth client credentials file.")
            print("Make sure you downloaded the OAuth 2.0 Client ID (not Service Account key)")
            return False
            
        # For installed apps, check redirect URIs
        if 'installed' in creds_data:
            print(f"✓ Found desktop app credentials in {credentials_file}")
        elif 'web' in creds_data:
            print(f"✓ Found web app credentials in {credentials_file}")
            redirect_uris = creds_data['web'].get('redirect_uris', [])
            print(f"  Configured redirect URIs: {redirect_uris}")
            
            # Check for localhost URIs
            if not any('localhost' in uri for uri in redirect_uris):
                print("  ⚠ WARNING: No localhost redirect URIs found")
                print("    Add these in Google Cloud Console:")
                print("    - http://localhost:8080")
                print("    - http://localhost:8081")
            
        return True
    except json.JSONDecodeError:
        print(f"ERROR: Invalid JSON in {credentials_file}")
        return False
    except Exception as e:
        print(f"ERROR reading {credentials_file}: {e}")
        return False

def setup_credentials(credentials_file, scopes, token_file, port=8080):
    """Setup OAuth2 credentials with detailed error reporting."""
    
    print(f"\n" + "-"*40)
    print(f"Setting up: {credentials_file}")
    print("-"*40)
    
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
            print("  Will create new token...")
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
                print(f"\n🌐 Starting OAuth flow on port {port}...")
                print(f"Requesting scopes: {len(scopes)} scopes")
                
                flow = InstalledAppFlow.from_client_secrets_file(
                    credentials_file, scopes
                )
                
                print("\n" + "!"*60)
                print("IMPORTANT: A browser window will open for authentication")
                print("Make sure to:")
                print("1. Use the Google account that's added as a test user")
                print("2. Review and accept all requested permissions")
                print("3. If you see a warning, click 'Advanced' > 'Go to [app]'")
                print("!"*60 + "\n")
                
                # Run the local server
                creds = flow.run_local_server(
                    port=port,
                    authorization_prompt_message='Please visit this URL to authorize: {url}',
                    success_message='Authorization complete! You may close this window.',
                    open_browser=True,
                    timeout_seconds=120
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
                print("4. Make sure your email is added as a test user")
                print(f"5. Try using a different port if {port} is blocked")
                print("6. Check redirect URIs in Google Cloud Console")
                raise
    
    # Verify the credentials work
    try:
        print("\nVerifying API access...")
        
        # Test Gmail API
        gmail_service = build('gmail', 'v1', credentials=creds)
        profile = gmail_service.users().getProfile(userId='me').execute()
        print(f"✓ Gmail API: Authenticated as {profile['emailAddress']}")
        
        # Test Calendar API if calendar scope is included
        if 'https://www.googleapis.com/auth/calendar.events' in scopes:
            cal_service = build('calendar', 'v3', credentials=creds)
            cal_list = cal_service.calendarList().list(maxResults=1).execute()
            print(f"✓ Calendar API: Access confirmed")
            
    except HttpError as e:
        print(f"⚠ API verification failed: {e}")
        if e.resp.status == 403:
            print("   The API might not be enabled or scopes not authorized")
        elif e.resp.status == 401:
            print("   Authentication failed - try deleting the token file")
    
    return creds

def test_gmail_connection(credentials):
    """Test Gmail connection and show recent emails."""
    print("\n" + "-"*40)
    print("Testing Gmail Connection")
    print("-"*40)
    
    try:
        service = build('gmail', 'v1', credentials=credentials)
        
        # Get recent messages
        results = service.users().messages().list(
            userId='me',
            maxResults=5,
            q='is:unread'
        ).execute()
        
        messages = results.get('messages', [])
        
        if not messages:
            print('No unread messages found.')
        else:
            print(f"Found {len(messages)} unread messages:")
            
            for msg in messages[:3]:  # Show first 3
                msg_data = service.users().messages().get(
                    userId='me',
                    id=msg['id']
                ).execute()
                
                # Extract headers
                headers = msg_data['payload'].get('headers', [])
                subject = next((h['value'] for h in headers if h['name'] == 'Subject'), 'No Subject')
                sender = next((h['value'] for h in headers if h['name'] == 'From'), 'Unknown')
                
                print(f"  - From: {sender[:50]}")
                print(f"    Subject: {subject[:50]}")
                print()
        
        return True
        
    except Exception as e:
        print(f"❌ Gmail test failed: {e}")
        return False

def create_calendar_tools(credentials):
    """Create calendar tools manually."""
    calendar_service = build('calendar', 'v3', credentials=credentials)
    
    def check_availability(date_string):
        """Check calendar availability for a given date."""
        try:
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
            return f"Error creating event: {str(e)}"
    
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

def main():
    """Main application entry point."""
    print("\n" + "="*60)
    print("Email Manager with Gmail and Calendar Integration")
    print("="*60)
    
    # Verify setup first
    if not verify_setup():
        print("\n❌ Please fix setup issues before proceeding")
        return
    
    try:
        # Setup workspace account
        print("\n📧 Setting up workspace account...")
        if os.path.exists(CREDENTIALS_WORK_FILE):
            creds_workspace = setup_credentials(
                CREDENTIALS_WORK_FILE,
                SCOPES,
                TOKEN_WORK_FILE,
                port=8080
            )
            print("✓ Workspace account ready")
            
            # Test the connection
            test_gmail_connection(creds_workspace)
        else:
            print(f"⚠ Skipping workspace - {CREDENTIALS_WORK_FILE} not found")
            creds_workspace = None
        
        # Setup personal account (optional)
        print("\n📧 Setting up personal account...")
        if os.path.exists(CREDENTIALS_PERSONAL_FILE):
            creds_personal = setup_credentials(
                CREDENTIALS_PERSONAL_FILE,
                SCOPES,
                TOKEN_PERSONAL_FILE,
                port=8081
            )
            print("✓ Personal account ready")
            
            # Test the connection
            test_gmail_connection(creds_personal)
        else:
            print(f"⚠ Skipping personal - {CREDENTIALS_PERSONAL_FILE} not found")
            creds_personal = None
        
        if not creds_workspace and not creds_personal:
            print("\n❌ No accounts configured successfully")
            return
        
        print("\n" + "="*60)
        print("Setup Complete!")
        print("="*60)
        print("\n✓ Your Gmail and Calendar integration is ready to use")
        print("\nYou can now:")
        print("1. Use the credentials to access Gmail and Calendar APIs")
        print("2. Run email processing with LangChain agents")
        print("3. Automate email management tasks")
        
        # Only try LangChain features if available and user wants to
        if LANGCHAIN_AVAILABLE and creds_workspace:
            response = input("\nWould you like to test LangChain email processing? (y/n): ")
            if response.lower() == 'y':
                print("\n🤖 Initializing LangChain agent...")
                
                # Build API resources
                api_workspace = build_resource_service(credentials=creds_workspace)
                
                # Create Gmail tools
                workspace_search = GmailSearch(api_resource=api_workspace)
                workspace_search.name = "workspace_gmail_search"
                workspace_search.description = "Search for emails in workspace Gmail."
                
                # Create calendar tools
                calendar_tools = create_calendar_tools(creds_workspace)
                
                # All tools for agent
                tools = [workspace_search] + calendar_tools
                
                # Initialize Ollama LLM
                try:
                    llm = Ollama(model=OLLAMA_MODEL)
                    
                    # Initialize agent
                    agent = initialize_agent(
                        tools,
                        llm,
                        agent_type=AgentType.ZERO_SHOT_REACT_DESCRIPTION,
                        verbose=True,
                        handle_parsing_errors=True
                    )
                    
                    print("✓ LangChain agent initialized")
                    print("\nAgent is ready to process emails!")
                    
                except Exception as e:
                    print(f"⚠ LangChain initialization failed: {e}")
                    print("Make sure Ollama is running with the correct model")
        
    except Exception as e:
        print(f"\n❌ Error in main execution: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()