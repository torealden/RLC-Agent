#!/usr/bin/env python3
"""
Email Manager with Desktop OAuth - Both Accounts
Updated to use Desktop OAuth credentials for work and personal accounts
"""

import os
import json
from datetime import datetime, timedelta
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# LangChain imports (if available)
try:
    from langchain.agents import initialize_agent, Tool, AgentType
    from langchain_community.llms import Ollama
    from langchain_community.tools.gmail.utils import build_resource_service
    from langchain_community.tools.gmail.search import GmailSearch
    from langchain_community.tools.gmail.get_message import GmailGetMessage
    LANGCHAIN_AVAILABLE = True
except ImportError:
    LANGCHAIN_AVAILABLE = False
    print("⚠ LangChain not installed - basic features only")

# Use Desktop OAuth credentials for BOTH accounts
DESKTOP_CREDENTIALS = "credentials_desktop.json"

# Separate token files for each account
TOKEN_WORK = "token_roundlakes.json"      # For Round Lakes Commodities
TOKEN_PERSONAL = "token_personal.json"    # For toremalden@gmail.com

# Scopes needed
SCOPES = [
    'https://www.googleapis.com/auth/gmail.readonly',
    'https://www.googleapis.com/auth/gmail.modify',
    'https://www.googleapis.com/auth/gmail.metadata',
    'https://www.googleapis.com/auth/calendar.events',
    'openid',
    'https://www.googleapis.com/auth/userinfo.email',
]

def setup_account(account_type="work"):
    """Setup OAuth for a specific account using Desktop credentials."""
    
    if account_type == "work":
        token_file = TOKEN_WORK
        account_name = "Round Lakes Commodities"
    else:
        token_file = TOKEN_PERSONAL
        account_name = "Personal"
    
    print(f"\nSetting up {account_name} account...")
    
    creds = None
    
    # Try to load existing token
    if os.path.exists(token_file):
        try:
            creds = Credentials.from_authorized_user_file(token_file, SCOPES)
            if creds and creds.valid:
                print(f"✓ Using existing token for {account_name}")
            elif creds and creds.expired and creds.refresh_token:
                print(f"Refreshing token for {account_name}...")
                creds.refresh(Request())
                # Save refreshed token
                with open(token_file, 'w') as f:
                    f.write(creds.to_json())
                print("✓ Token refreshed")
        except Exception as e:
            print(f"Could not load token: {e}")
            creds = None
    
    # If no valid credentials, need to authenticate
    if not creds or not creds.valid:
        print(f"\nNeed to authenticate {account_name} account")
        print("="*40)
        if account_type == "work":
            print("⚠️  IMPORTANT: Choose your WORK account")
            print("   NOT toremalden@gmail.com")
        else:
            print("Choose your PERSONAL account (toremalden@gmail.com)")
        print("="*40)
        
        flow = InstalledAppFlow.from_client_secrets_file(
            DESKTOP_CREDENTIALS, SCOPES
        )
        
        creds = flow.run_local_server(
            port=0,
            authorization_prompt_message=f'Authorize {account_name}: {{url}}',
            success_message=f'{account_name} authorized! Close this window.'
        )
        
        # Save token
        with open(token_file, 'w') as f:
            f.write(creds.to_json())
        print(f"✓ New token saved for {account_name}")
    
    # Verify which account was authenticated
    try:
        service = build('gmail', 'v1', credentials=creds)
        profile = service.users().getProfile(userId='me').execute()
        email = profile['emailAddress']
        print(f"✓ Authenticated as: {email}")
        
        if account_type == "work" and email == "toremalden@gmail.com":
            print("\n⚠️  WARNING: This is your personal account!")
            print("   Run again and choose your work account")
            return None
            
        return creds
        
    except Exception as e:
        print(f"Error verifying account: {e}")
        return None

def process_emails(credentials, account_name):
    """Process emails for a specific account."""
    
    print(f"\n" + "="*60)
    print(f"Processing {account_name} Emails")
    print("="*60)
    
    try:
        service = build('gmail', 'v1', credentials=credentials)
        
        # Get unread emails
        results = service.users().messages().list(
            userId='me',
            q='is:unread',
            maxResults=10
        ).execute()
        
        messages = results.get('messages', [])
        
        if not messages:
            print('No unread messages')
            return
        
        print(f"Found {len(messages)} unread messages")
        
        # Process first 5 messages
        for msg in messages[:5]:
            msg_data = service.users().messages().get(
                userId='me',
                id=msg['id']
            ).execute()
            
            headers = msg_data['payload'].get('headers', [])
            subject = next((h['value'] for h in headers if h['name'] == 'Subject'), 'No Subject')
            sender = next((h['value'] for h in headers if h['name'] == 'From'), 'Unknown')
            date = next((h['value'] for h in headers if h['name'] == 'Date'), 'Unknown')
            
            print(f"\n📧 From: {sender[:60]}")
            print(f"   Subject: {subject[:60]}")
            print(f"   Date: {date[:30]}")
            
            # Here you can add logic to:
            # - Delete spam
            # - Archive promotional emails
            # - Flag important messages
            # - Create calendar events
            # - Draft responses
            
    except Exception as e:
        print(f"Error processing emails: {e}")

def create_calendar_event(credentials, event_data):
    """Create a calendar event using the authenticated credentials."""
    
    try:
        service = build('calendar', 'v3', credentials=credentials)
        
        event = service.events().insert(
            calendarId='primary',
            body=event_data
        ).execute()
        
        print(f"✓ Event created: {event.get('htmlLink')}")
        return event
        
    except Exception as e:
        print(f"Error creating event: {e}")
        return None

def main():
    """Main application."""
    
    print("\n" + "="*60)
    print("Email Manager - Desktop OAuth Edition")
    print("="*60)
    
    # Check for Desktop credentials
    if not os.path.exists(DESKTOP_CREDENTIALS):
        print(f"❌ {DESKTOP_CREDENTIALS} not found!")
        print("Run setup_desktop_oauth.py first")
        return
    
    print("\nWhich accounts to process?")
    print("1. Work account only")
    print("2. Personal account only")
    print("3. Both accounts")
    
    choice = input("\nYour choice (1-3): ").strip()
    
    accounts_to_process = []
    
    if choice in ["1", "3"]:
        print("\nSetting up work account...")
        work_creds = setup_account("work")
        if work_creds:
            accounts_to_process.append(("Work", work_creds))
    
    if choice in ["2", "3"]:
        print("\nSetting up personal account...")
        personal_creds = setup_account("personal")
        if personal_creds:
            accounts_to_process.append(("Personal", personal_creds))
    
    # Process each account
    for account_name, creds in accounts_to_process:
        process_emails(creds, account_name)
    
    print("\n" + "="*60)
    print("Email Processing Complete")
    print("="*60)
    
    # If LangChain is available, offer advanced features
    if LANGCHAIN_AVAILABLE and accounts_to_process:
        print("\n🤖 LangChain is available for advanced processing")
        use_ai = input("Use AI for email categorization? (y/n): ").lower()
        
        if use_ai == 'y':
            print("Initializing AI agent...")
            # Add LangChain processing here
            
    print("\n✅ All done!")

if __name__ == "__main__":
    main()