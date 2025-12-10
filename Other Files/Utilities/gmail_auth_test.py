#!/usr/bin/env python3
"""
Gmail Authentication Test Script
Tests OAuth2 authentication flow for Gmail API
"""

import os
import json
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

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
    except FileNotFoundError:
        print(f"ERROR: Could not find {credentials_file}")
        print("Please make sure the file exists in the current directory")
        return False
    except Exception as e:
        print(f"ERROR reading {credentials_file}: {e}")
        return False

def setup_credentials(credentials_file, scopes, token_file, port=8080):
    """Setup OAuth2 credentials with detailed error reporting."""
    
    print(f"\n{'='*60}")
    print(f"Setting up OAuth for: {credentials_file}")
    print(f"{'='*60}")
    
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
                print(f"Requesting scopes:")
                for scope in scopes:
                    print(f"  - {scope}")
                
                flow = InstalledAppFlow.from_client_secrets_file(
                    credentials_file, scopes
                )
                
                print(f"\n🌐 Opening browser for authorization...")
                print(f"   If browser doesn't open, manually visit the URL shown")
                print(f"   Make sure to use the account added as test user\n")
                
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
                print(f"5. Try using a different port if {port} is blocked")
                print("6. Check that redirect URI includes http://localhost:{port}")
                raise
    
    # Verify the credentials work
    try:
        print("\n" + "="*60)
        print("Testing API Access")
        print("="*60)
        
        # Test Gmail API
        gmail_service = build('gmail', 'v1', credentials=creds)
        profile = gmail_service.users().getProfile(userId='me').execute()
        email_address = profile['emailAddress']
        print(f"✓ Gmail API: Successfully authenticated as {email_address}")
        
        # Get message count
        messages = gmail_service.users().messages().list(userId='me', maxResults=1).execute()
        total_messages = messages.get('resultSizeEstimate', 0)
        print(f"  - Total messages accessible: ~{total_messages}")
        
        # Test Calendar API if calendar scope is included
        if 'https://www.googleapis.com/auth/calendar' in scopes or \
           'https://www.googleapis.com/auth/calendar.events' in scopes:
            cal_service = build('calendar', 'v3', credentials=creds)
            cal_list = cal_service.calendarList().list(maxResults=1).execute()
            print(f"✓ Calendar API: Access confirmed")
            calendars = cal_list.get('items', [])
            if calendars:
                print(f"  - Primary calendar: {calendars[0].get('summary', 'Unknown')}")
                
    except HttpError as e:
        print(f"⚠ API test failed: {e}")
        if e.resp.status == 403:
            print("This usually means the API is not enabled or scopes are not authorized")
            print("Check your Google Cloud Console settings")
        elif e.resp.status == 401:
            print("Authentication failed - try deleting the token file and re-authenticating")
    
    return creds

def main():
    """Main test function."""
    
    print("\n" + "="*60)
    print("Gmail OAuth Authentication Test")
    print("="*60)
    
    # Define test configurations
    tests = [
        {
            "name": "Gmail Only (Minimal Scopes)",
            "credentials_file": "credentials.json",
            "token_file": "token_test_gmail.json",
            "scopes": [
                'https://www.googleapis.com/auth/gmail.readonly',
                'openid',
                'https://www.googleapis.com/auth/userinfo.email'
            ],
            "port": 8080
        },
        {
            "name": "Gmail + Calendar (Full Scopes)",
            "credentials_file": "credentials.json",
            "token_file": "token_test_full.json",
            "scopes": [
                'https://www.googleapis.com/auth/gmail.readonly',
                'https://www.googleapis.com/auth/gmail.modify',
                'https://www.googleapis.com/auth/gmail.metadata',
                'https://www.googleapis.com/auth/calendar.events',
                'openid',
                'https://www.googleapis.com/auth/userinfo.email',
            ],
            "port": 8081
        }
    ]
    
    # Let user choose which test to run
    print("\nAvailable tests:")
    for i, test in enumerate(tests, 1):
        print(f"{i}. {test['name']}")
    
    try:
        choice = input("\nWhich test would you like to run? (1-2, or 'all'): ").strip()
        
        if choice.lower() == 'all':
            tests_to_run = tests
        else:
            idx = int(choice) - 1
            if 0 <= idx < len(tests):
                tests_to_run = [tests[idx]]
            else:
                print("Invalid choice")
                return
    except (ValueError, IndexError):
        print("Invalid input")
        return
    
    # Run selected tests
    for test in tests_to_run:
        print(f"\n{'='*60}")
        print(f"Running: {test['name']}")
        print(f"{'='*60}")
        
        try:
            # Check if credentials file exists
            if not os.path.exists(test['credentials_file']):
                print(f"❌ Credentials file not found: {test['credentials_file']}")
                print("\nTo fix this:")
                print("1. Go to Google Cloud Console")
                print("2. Navigate to APIs & Services > Credentials")
                print("3. Click on your OAuth 2.0 Client ID")
                print("4. Download the JSON file")
                print(f"5. Save it as '{test['credentials_file']}' in this directory")
                continue
            
            creds = setup_credentials(
                test['credentials_file'],
                test['scopes'],
                test['token_file'],
                test['port']
            )
            
            print(f"\n✅ Test '{test['name']}' completed successfully!")
            
        except Exception as e:
            print(f"\n❌ Test '{test['name']}' failed: {e}")
    
    print("\n" + "="*60)
    print("Testing Complete")
    print("="*60)
    print("\nNext steps:")
    print("1. If all tests passed, your OAuth setup is working correctly")
    print("2. You can now use these credentials in your main application")
    print("3. Token files are saved and will be reused in future runs")

if __name__ == "__main__":
    # Check for required libraries
    try:
        import google.auth
        import google_auth_oauthlib
        import googleapiclient
    except ImportError as e:
        print("Missing required libraries. Please install:")
        print("pip install google-auth google-auth-oauthlib google-auth-httplib2 google-api-python-client")
        exit(1)
    
    main()