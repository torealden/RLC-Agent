#!/usr/bin/env python3
"""
Simple Desktop OAuth Test
The fastest way to verify your new Desktop credentials work
"""

from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build

print("\n🚀 Testing your new Desktop OAuth credentials...")
print("="*60)

try:
    # Create flow with Desktop credentials
    flow = InstalledAppFlow.from_client_secrets_file(
        'credentials_desktop.json',
        ['https://www.googleapis.com/auth/gmail.readonly']
    )
    
    print("Opening browser for authorization...")
    print("Please approve the permissions when prompted.\n")
    
    # Run auth - Desktop apps work great with port=0 (any available port)
    creds = flow.run_local_server(port=0)
    
    # Test Gmail access
    service = build('gmail', 'v1', credentials=creds)
    profile = service.users().getProfile(userId='me').execute()
    
    print("="*60)
    print("✅ SUCCESS! Desktop OAuth is working!")
    print("="*60)
    print(f"Authenticated as: {profile['emailAddress']}")
    print(f"Total messages: {profile.get('messagesTotal', 'N/A')}")
    print("\nYour OAuth setup is complete! 🎉")
    
except FileNotFoundError:
    print("❌ Error: credentials_desktop.json not found")
    print("Make sure the file is in the current directory")
    
except Exception as e:
    print(f"❌ Error: {e}")
    print("\nTroubleshooting:")
    print("1. Make sure your email is in the test users list")
    print("2. Check that Gmail API is enabled")
    print("3. Try again - sometimes the browser needs a moment to open")