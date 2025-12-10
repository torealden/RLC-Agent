#!/usr/bin/env python3
"""
Test Desktop OAuth Authentication
Using your newly created Desktop OAuth client
"""

import os
import json
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build

# Your new Desktop OAuth credentials
CREDENTIALS_FILE = "credentials_desktop.json"
TOKEN_FILE = "token_desktop.json"

# Start with minimal scopes to test
SCOPES = [
    'https://www.googleapis.com/auth/gmail.readonly',
    'openid',
    'https://www.googleapis.com/auth/userinfo.email'
]

def test_desktop_auth():
    """Test authentication with Desktop OAuth client."""
    
    print("="*60)
    print("Testing Desktop OAuth Authentication")
    print("="*60)
    
    # Check credentials file exists
    if not os.path.exists(CREDENTIALS_FILE):
        print(f"❌ Error: {CREDENTIALS_FILE} not found!")
        print("Make sure the file is in the same directory as this script")
        return
    
    print(f"✓ Found {CREDENTIALS_FILE}")
    
    # Verify the credentials file structure
    with open(CREDENTIALS_FILE, 'r') as f:
        creds_data = json.load(f)
    
    if 'installed' in creds_data:
        print("✓ Valid Desktop application credentials")
        print(f"  Client ID: {creds_data['installed']['client_id'][:50]}...")
        print(f"  Client Secret: {'*' * 20} (present)")
    else:
        print("❌ Invalid credentials format")
        return
    
    print("\n" + "-"*60)
    print("Starting OAuth Flow")
    print("-"*60)
    
    creds = None
    
    # Check for existing token
    if os.path.exists(TOKEN_FILE):
        print(f"Found existing token file: {TOKEN_FILE}")
        try:
            creds = Credentials.from_authorized_user_file(TOKEN_FILE, SCOPES)
            print("✓ Loaded existing token")
        except Exception as e:
            print(f"⚠ Could not load token: {e}")
            creds = None
    
    # Check if credentials are valid
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            print("Token expired, refreshing...")
            try:
                creds.refresh(Request())
                print("✓ Token refreshed")
            except Exception as e:
                print(f"Could not refresh token: {e}")
                creds = None
        
        # Need new authorization
        if not creds:
            print("\n🌐 Starting new authorization flow...")
            print("A browser window will open for authorization")
            print("Make sure to:")
            print("1. Use the Google account that's added as a test user")
            print("2. Click 'Continue' if you see a warning")
            print("3. Allow all requested permissions\n")
            
            try:
                flow = InstalledAppFlow.from_client_secrets_file(
                    CREDENTIALS_FILE, SCOPES
                )
                
                # Desktop apps work great with localhost!
                creds = flow.run_local_server(
                    port=0,  # Use any available port
                    authorization_prompt_message='Please authorize in your browser: {url}',
                    success_message='Authorization successful! You can close this window.',
                    open_browser=True
                )
                
                print("✅ Authorization successful!")
                
                # Save the token for next time
                with open(TOKEN_FILE, 'w') as token:
                    token.write(creds.to_json())
                print(f"✓ Token saved to {TOKEN_FILE}")
                
            except Exception as e:
                print(f"❌ Authorization failed: {e}")
                return
    
    # Test the Gmail API
    print("\n" + "-"*60)
    print("Testing Gmail API Access")
    print("-"*60)
    
    try:
        service = build('gmail', 'v1', credentials=creds)
        
        # Get user profile
        profile = service.users().getProfile(userId='me').execute()
        email = profile['emailAddress']
        
        print(f"✅ Successfully connected to Gmail!")
        print(f"📧 Authenticated as: {email}")
        print(f"📊 Total messages: {profile.get('messagesTotal', 'Unknown')}")
        print(f"📊 Total threads: {profile.get('threadsTotal', 'Unknown')}")
        
        # Get a few recent messages
        print("\n" + "-"*60)
        print("Recent Messages Test")
        print("-"*60)
        
        results = service.users().messages().list(
            userId='me',
            maxResults=3
        ).execute()
        
        messages = results.get('messages', [])
        
        if messages:
            print(f"✓ Found {len(messages)} recent messages")
            
            for msg in messages:
                msg_data = service.users().messages().get(
                    userId='me',
                    id=msg['id']
                ).execute()
                
                headers = msg_data['payload'].get('headers', [])
                subject = next((h['value'] for h in headers if h['name'] == 'Subject'), 'No Subject')
                sender = next((h['value'] for h in headers if h['name'] == 'From'), 'Unknown')
                
                print(f"\n  📨 From: {sender[:50]}")
                print(f"     Subject: {subject[:50]}")
        else:
            print("No messages found")
        
        print("\n" + "="*60)
        print("✅ SUCCESS! Your Desktop OAuth is working perfectly!")
        print("="*60)
        print("\nYou can now:")
        print("1. Use these credentials in your email manager script")
        print("2. Add more scopes for Calendar access")
        print("3. Run your full email automation")
        
        return True
        
    except Exception as e:
        print(f"❌ Gmail API test failed: {e}")
        return False

def main():
    """Main entry point."""
    print("\n🚀 Desktop OAuth Client Test")
    print("Using: credentials_desktop.json")
    print("\nThis test will:")
    print("1. Verify your Desktop OAuth credentials")
    print("2. Authenticate with Google")
    print("3. Test Gmail API access")
    print("4. Save a token for future use")
    
    input("\nPress Enter to start...")
    
    success = test_desktop_auth()
    
    if success:
        print("\n✨ Everything is working! Your OAuth setup is complete.")
        print("\nNext steps:")
        print("1. Update your email_manager_claude.py to use 'credentials_desktop.json'")
        print("2. Test with full scopes (Calendar + Gmail)")
        print("3. Run your email automation")
    else:
        print("\n⚠ Something went wrong. Check the error messages above.")
        print("\nCommon fixes:")
        print("1. Make sure your email is in the test users list")
        print("2. Check that Gmail API is enabled")
        print("3. Try deleting token_desktop.json and re-authenticating")

if __name__ == "__main__":
    main()