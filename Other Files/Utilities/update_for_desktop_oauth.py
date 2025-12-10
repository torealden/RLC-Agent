#!/usr/bin/env python3
"""
Update Email Manager to use Desktop OAuth
This updates your existing scripts to use the new Desktop credentials
"""

import os

def update_email_manager():
    """Update the email manager script to use Desktop credentials."""
    
    print("="*60)
    print("Updating Email Manager for Desktop OAuth")
    print("="*60)
    
    # Files to potentially update
    files_to_check = [
        "email_manager_claude.py",
        "email_manager_fixed.py",
        "gmail_auth_test.py"
    ]
    
    print("\nLooking for scripts to update...")
    
    for filename in files_to_check:
        if os.path.exists(filename):
            print(f"\n✓ Found: {filename}")
            
            # Read the file
            with open(filename, 'r') as f:
                content = f.read()
            
            # Check if it uses the old credentials
            if 'credentials.json' in content or 'credentials_personal.json' in content:
                print(f"  Updating credential references...")
                
                # Create a backup
                backup_name = f"{filename}.backup"
                with open(backup_name, 'w') as f:
                    f.write(content)
                print(f"  Created backup: {backup_name}")
                
                # Update the content
                updated = content.replace(
                    'CREDENTIALS_WORK_FILE = "credentials.json"',
                    'CREDENTIALS_WORK_FILE = "credentials_desktop.json"'
                ).replace(
                    '"credentials.json"',
                    '"credentials_desktop.json"  # Updated to use Desktop OAuth'
                )
                
                # Write the updated file
                with open(filename, 'w') as f:
                    f.write(updated)
                print(f"  ✓ Updated to use credentials_desktop.json")
            else:
                print(f"  No updates needed")
        else:
            print(f"✗ Not found: {filename}")
    
    print("\n" + "="*60)
    print("Quick Start Guide")
    print("="*60)
    
    print("""
1. TEST THE DESKTOP OAUTH:
   python test_desktop_auth.py

2. TEST WITH FULL SCOPES:
   python test_full_scopes.py

3. RUN YOUR EMAIL MANAGER:
   python email_manager_claude.py

Your Desktop OAuth client will work much better than Web clients!
No redirect URI configuration needed - it just works.
""")

def create_full_scope_test():
    """Create a test script with all scopes."""
    
    script_content = '''#!/usr/bin/env python3
"""
Test Desktop OAuth with Full Scopes
Tests Gmail + Calendar access
"""

from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
import os

# Desktop OAuth credentials
CREDENTIALS_FILE = "credentials_desktop.json"
TOKEN_FILE = "token_full_scopes.json"

# Full scopes for email manager
SCOPES = [
    'https://www.googleapis.com/auth/gmail.readonly',
    'https://www.googleapis.com/auth/gmail.modify',
    'https://www.googleapis.com/auth/gmail.metadata',
    'https://www.googleapis.com/auth/calendar.events',
    'openid',
    'https://www.googleapis.com/auth/userinfo.email',
]

def test_full_access():
    """Test with all required scopes."""
    
    print("Testing Desktop OAuth with Full Scopes")
    print("="*60)
    
    creds = None
    
    # Force new authentication to get all scopes
    if os.path.exists(TOKEN_FILE):
        os.remove(TOKEN_FILE)
        print("Removed old token to get fresh permissions")
    
    print("Requesting all scopes needed for email manager...")
    print(f"Total scopes: {len(SCOPES)}")
    
    flow = InstalledAppFlow.from_client_secrets_file(
        CREDENTIALS_FILE, SCOPES
    )
    
    creds = flow.run_local_server(port=0)
    
    # Save token
    with open(TOKEN_FILE, 'w') as token:
        token.write(creds.to_json())
    
    print("✓ Authorization successful!")
    
    # Test Gmail
    print("\\nTesting Gmail API...")
    gmail = build('gmail', 'v1', credentials=creds)
    profile = gmail.users().getProfile(userId='me').execute()
    print(f"✓ Gmail access: {profile['emailAddress']}")
    
    # Test Calendar
    print("\\nTesting Calendar API...")
    calendar = build('calendar', 'v3', credentials=creds)
    calendars = calendar.calendarList().list(maxResults=1).execute()
    print(f"✓ Calendar access confirmed")
    
    print("\\n✅ All APIs working with Desktop OAuth!")
    print("You're ready to run your email manager!")

if __name__ == "__main__":
    test_full_access()
'''
    
    with open('test_full_scopes.py', 'w') as f:
        f.write(script_content)
    
    print("✓ Created test_full_scopes.py")

if __name__ == "__main__":
    update_email_manager()
    create_full_scope_test()