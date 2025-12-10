#!/usr/bin/env python3
"""
Multi-Account OAuth Setup
Authenticate both Work and Personal Gmail accounts
"""

import os
import json
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build

# Desktop OAuth works for BOTH accounts!
DESKTOP_CREDENTIALS = "credentials_desktop.json"

# Separate token files for each account
TOKEN_WORK = "token_work.json"
TOKEN_PERSONAL = "token_personal.json"

# Scopes needed
SCOPES = [
    'https://www.googleapis.com/auth/gmail.readonly',
    'https://www.googleapis.com/auth/gmail.modify',
    'https://www.googleapis.com/auth/gmail.metadata',
    'https://www.googleapis.com/auth/calendar.events',
    'openid',
    'https://www.googleapis.com/auth/userinfo.email',
]

def authenticate_account(account_type):
    """Authenticate a specific account."""
    
    print("\n" + "="*60)
    print(f"Setting up {account_type} Account")
    print("="*60)
    
    if account_type == "WORK":
        token_file = TOKEN_WORK
        print("You need to authenticate with your WORK account")
        print("This should be your @roundlakescommodities.com email")
        print("or the work Gmail account you use")
    else:
        token_file = TOKEN_PERSONAL
        print("You need to authenticate with your PERSONAL account")
        print("This should be toremalden@gmail.com")
    
    # Remove existing token to force re-authentication
    if os.path.exists(token_file):
        print(f"Removing old token: {token_file}")
        os.remove(token_file)
    
    print("\n🌐 Starting authentication...")
    print("-"*40)
    print("IMPORTANT: When the browser opens:")
    if account_type == "WORK":
        print("1. Choose your WORK/RLC account")
        print("2. NOT your personal toremalden@gmail.com")
    else:
        print("1. Choose your PERSONAL account")
        print("2. This should be toremalden@gmail.com")
    print("3. Click 'Continue' if you see a warning")
    print("4. Allow all permissions")
    print("-"*40)
    
    input(f"\nPress Enter to authenticate your {account_type} account...")
    
    try:
        # Create flow with Desktop credentials
        flow = InstalledAppFlow.from_client_secrets_file(
            DESKTOP_CREDENTIALS, SCOPES
        )
        
        # Run authentication
        creds = flow.run_local_server(
            port=0,  # Use any available port
            authorization_prompt_message=f'Authorize {account_type} account: {{url}}',
            success_message=f'{account_type} account authorized! You can close this window.'
        )
        
        # Save the token
        with open(token_file, 'w') as token:
            token.write(creds.to_json())
        
        # Test the connection
        service = build('gmail', 'v1', credentials=creds)
        profile = service.users().getProfile(userId='me').execute()
        email = profile['emailAddress']
        
        print(f"\n✅ SUCCESS! {account_type} account authenticated")
        print(f"📧 Email: {email}")
        print(f"📊 Messages: {profile.get('messagesTotal', 'N/A')}")
        print(f"✓ Token saved to: {token_file}")
        
        return email
        
    except Exception as e:
        print(f"❌ Error authenticating {account_type} account: {e}")
        return None

def check_existing_tokens():
    """Check what tokens already exist."""
    print("\n" + "="*60)
    print("Checking Existing Tokens")
    print("="*60)
    
    accounts = {}
    
    for token_file in [TOKEN_WORK, TOKEN_PERSONAL, "token_desktop.json"]:
        if os.path.exists(token_file):
            try:
                creds = Credentials.from_authorized_user_file(token_file, SCOPES)
                service = build('gmail', 'v1', credentials=creds)
                profile = service.users().getProfile(userId='me').execute()
                email = profile['emailAddress']
                accounts[token_file] = email
                print(f"✓ {token_file}: {email}")
            except:
                print(f"✗ {token_file}: Invalid or expired")
        else:
            print(f"✗ {token_file}: Not found")
    
    return accounts

def main():
    print("\n" + "="*60)
    print("Multi-Account Gmail/Calendar Setup")
    print("="*60)
    print("\nThis will set up BOTH your work and personal accounts")
    print("Using the same Desktop OAuth credentials for both")
    
    # Check what's already set up
    existing = check_existing_tokens()
    
    print("\n" + "="*60)
    print("Setup Options")
    print("="*60)
    print("\n1. Set up WORK account (Round Lakes Commodities)")
    print("2. Set up PERSONAL account (toremalden@gmail.com)")
    print("3. Set up BOTH accounts")
    print("4. Test existing accounts")
    
    choice = input("\nYour choice (1-4): ").strip()
    
    if choice == "1":
        authenticate_account("WORK")
    elif choice == "2":
        authenticate_account("PERSONAL")
    elif choice == "3":
        print("\nSetting up both accounts...")
        print("We'll do them one at a time")
        
        work_email = authenticate_account("WORK")
        if work_email:
            print(f"\n✓ Work account ready: {work_email}")
        
        personal_email = authenticate_account("PERSONAL")
        if personal_email:
            print(f"\n✓ Personal account ready: {personal_email}")
        
        if work_email and personal_email:
            print("\n" + "="*60)
            print("✅ Both accounts are set up!")
            print("="*60)
            print(f"Work: {work_email} -> token_work.json")
            print(f"Personal: {personal_email} -> token_personal.json")
    
    elif choice == "4":
        print("\nTesting existing accounts...")
        for token_file, email in existing.items():
            print(f"\n{token_file}: {email}")
    
    print("\n" + "="*60)
    print("Next Steps")
    print("="*60)
    print("\nYour email manager can now use:")
    print("- token_work.json for Round Lakes Commodities")
    print("- token_personal.json for personal Gmail")
    print("\nUpdate your email_manager_claude.py to use these token files")

if __name__ == "__main__":
    main()