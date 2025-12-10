#!/usr/bin/env python3
"""
Fix Scope Issues and Re-authenticate with Full Permissions
This will ensure both accounts have all necessary scopes
"""

import os
import json
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build

# Desktop OAuth credentials
DESKTOP_CREDENTIALS = "credentials_desktop.json"

# Token files
TOKENS = {
    "Work": "token_work.json",
    "Personal": "token_personal.json",
    "Workspace": "token_workspace.json"  # Old token that might exist
}

# FULL scopes needed for everything to work
FULL_SCOPES = [
    'https://www.googleapis.com/auth/gmail.readonly',
    'https://www.googleapis.com/auth/gmail.modify',
    'https://www.googleapis.com/auth/gmail.compose',  # For drafts
    'https://www.googleapis.com/auth/calendar',  # Full calendar access
    'openid',
    'https://www.googleapis.com/auth/userinfo.email',
]

def check_token_scopes(token_file):
    """Check what scopes a token has."""
    if not os.path.exists(token_file):
        return None
    
    try:
        with open(token_file, 'r') as f:
            token_data = json.load(f)
        
        scopes = token_data.get('scopes', [])
        return scopes
    except:
        return None

def test_apis(credentials):
    """Test what APIs work with current credentials."""
    results = {}
    
    # Test Gmail
    try:
        gmail = build('gmail', 'v1', credentials=credentials)
        profile = gmail.users().getProfile(userId='me').execute()
        results['gmail_profile'] = f"✓ {profile['emailAddress']}"
        
        # Test different Gmail operations
        try:
            # Test list with query (needs full scope)
            gmail.users().messages().list(userId='me', q='is:unread', maxResults=1).execute()
            results['gmail_search'] = "✓ Can search messages"
        except Exception as e:
            results['gmail_search'] = f"✗ Cannot search: {str(e)[:50]}"
        
        try:
            # Test list without query (works with metadata scope)
            gmail.users().messages().list(userId='me', maxResults=1).execute()
            results['gmail_list'] = "✓ Can list messages"
        except Exception as e:
            results['gmail_list'] = f"✗ Cannot list: {str(e)[:50]}"
            
    except Exception as e:
        results['gmail_profile'] = f"✗ Gmail failed: {str(e)[:50]}"
    
    # Test Calendar
    try:
        calendar = build('calendar', 'v3', credentials=credentials)
        calendar.calendarList().list(maxResults=1).execute()
        results['calendar'] = "✓ Calendar access works"
    except Exception as e:
        results['calendar'] = f"✗ Calendar failed: {str(e)[:50]}"
    
    return results

def reauthenticate_account(account_name, token_file):
    """Re-authenticate an account with full scopes."""
    
    print(f"\n{'='*60}")
    print(f"Re-authenticating {account_name} with FULL scopes")
    print('='*60)
    
    # Remove old token
    if os.path.exists(token_file):
        os.remove(token_file)
        print(f"Removed old token: {token_file}")
    
    print("\nRequesting these scopes:")
    for scope in FULL_SCOPES:
        print(f"  • {scope.split('/')[-1]}")
    
    print(f"\n⚠️  IMPORTANT:")
    if account_name == "Work":
        print("Select: tore.alden@roundlakescommodities.com")
    else:
        print("Select: toremalden@gmail.com")
    
    input(f"\nPress Enter to authenticate {account_name} account...")
    
    try:
        flow = InstalledAppFlow.from_client_secrets_file(
            DESKTOP_CREDENTIALS, FULL_SCOPES
        )
        
        creds = flow.run_local_server(
            port=0,
            authorization_prompt_message=f'Re-authenticate {account_name}: {{url}}',
            success_message=f'{account_name} re-authenticated with full permissions!'
        )
        
        # Save token
        with open(token_file, 'w') as f:
            f.write(creds.to_json())
        
        # Test the APIs
        print(f"\nTesting {account_name} account APIs...")
        results = test_apis(creds)
        
        for api, result in results.items():
            print(f"  {api}: {result}")
        
        # Verify email
        gmail = build('gmail', 'v1', credentials=creds)
        profile = gmail.users().getProfile(userId='me').execute()
        email = profile['emailAddress']
        
        print(f"\n✅ {account_name} ready: {email}")
        
        return True
        
    except Exception as e:
        print(f"❌ Error: {e}")
        return False

def main():
    print("\n" + "="*60)
    print("OAuth Scope Fixer")
    print("="*60)
    print("\nThis will fix the scope issues you're experiencing")
    
    # Check existing tokens
    print("\nChecking existing tokens...")
    print("-"*40)
    
    token_status = {}
    for name, file in TOKENS.items():
        if os.path.exists(file):
            scopes = check_token_scopes(file)
            if scopes:
                print(f"\n{name} ({file}):")
                print(f"  Current scopes: {len(scopes)}")
                missing = []
                for scope in FULL_SCOPES:
                    if scope not in scopes:
                        missing.append(scope.split('/')[-1])
                if missing:
                    print(f"  ⚠️  Missing: {', '.join(missing)}")
                    token_status[name] = "needs_update"
                else:
                    print(f"  ✓ Has all scopes")
                    token_status[name] = "ok"
            else:
                print(f"{name} ({file}): ✗ Invalid")
                token_status[name] = "invalid"
        else:
            if name != "Workspace":  # Don't worry about old workspace token
                print(f"{name} ({file}): ✗ Not found")
                token_status[name] = "missing"
    
    # Decide what to do
    print("\n" + "="*60)
    print("Action Required")
    print("="*60)
    
    needs_fix = False
    for name, status in token_status.items():
        if status != "ok" and name != "Workspace":
            needs_fix = True
            break
    
    if not needs_fix:
        print("\n✅ All tokens have correct scopes!")
        print("Your setup is complete!")
        return
    
    print("\nYour tokens need to be updated with full scopes.")
    print("This will allow:")
    print("  • Full Gmail access (search, read, modify)")
    print("  • Full Calendar access")
    print("  • No more 'q parameter' errors")
    
    print("\nOptions:")
    print("1. Fix Work account only")
    print("2. Fix Personal account only")
    print("3. Fix BOTH accounts (recommended)")
    print("4. Skip")
    
    choice = input("\nYour choice (1-4): ").strip()
    
    if choice == "1":
        reauthenticate_account("Work", TOKENS["Work"])
    elif choice == "2":
        reauthenticate_account("Personal", TOKENS["Personal"])
    elif choice == "3":
        # Fix both
        success = []
        if reauthenticate_account("Work", TOKENS["Work"]):
            success.append("Work")
        if reauthenticate_account("Personal", TOKENS["Personal"]):
            success.append("Personal")
        
        if len(success) == 2:
            print("\n" + "="*60)
            print("✅ BOTH accounts fixed and ready!")
            print("="*60)
            print("\nYou can now run your email manager without errors!")
    
    print("\n" + "="*60)
    print("Next Steps")
    print("="*60)
    print("\n1. Run the working email manager:")
    print("   python email_manager_working.py")
    print("\n2. Both accounts will work without scope errors")
    print("3. Calendar access will work properly")

if __name__ == "__main__":
    main()