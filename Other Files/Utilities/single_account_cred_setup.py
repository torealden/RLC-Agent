#!/usr/bin/env python3
"""
Quick Setup for Round Lakes Commodities Work Account
Uses the working Desktop OAuth credentials
"""

from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
import os

print("\n" + "="*60)
print("Setting up WORK Account (Round Lakes Commodities)")
print("="*60)

# Use the Desktop credentials that are already working
CREDENTIALS_FILE = "credentials_desktop.json"
TOKEN_FILE = "token_roundlakes.json"

# Full scopes for email manager
SCOPES = [
    'https://www.googleapis.com/auth/gmail.readonly',
    'https://www.googleapis.com/auth/gmail.modify',
    'https://www.googleapis.com/auth/gmail.metadata',
    'https://www.googleapis.com/auth/calendar.events',
    'openid',
    'https://www.googleapis.com/auth/userinfo.email',
]

# Remove old token to force fresh authentication
if os.path.exists(TOKEN_FILE):
    os.remove(TOKEN_FILE)
    print(f"Removed old token file")

print("\n⚠️  IMPORTANT INSTRUCTIONS:")
print("="*40)
print("When the browser opens:")
print("")
print("1. ❌ DO NOT choose toremalden@gmail.com")
print("2. ✅ CHOOSE your Round Lakes Commodities account")
print("   (This might be @roundlakescommodities.com")
print("   or another work Gmail account)")
print("")
print("3. If you see 'Google hasn't verified this app'")
print("   Click 'Continue' or 'Advanced' > 'Go to app'")
print("")
print("4. Allow all requested permissions")
print("="*40)

input("\nPress Enter when ready to authenticate your WORK account...")

try:
    # Create flow
    flow = InstalledAppFlow.from_client_secrets_file(
        CREDENTIALS_FILE, SCOPES
    )
    
    print("\n🌐 Opening browser for WORK account authentication...")
    
    # Authenticate
    creds = flow.run_local_server(
        port=0,
        authorization_prompt_message='Authenticate your WORK account: {url}',
        success_message='Work account authorized! You can close this window.'
    )
    
    # Save token
    with open(TOKEN_FILE, 'w') as f:
        f.write(creds.to_json())
    
    # Test and show which account was authenticated
    service = build('gmail', 'v1', credentials=creds)
    profile = service.users().getProfile(userId='me').execute()
    email = profile['emailAddress']
    
    print("\n" + "="*60)
    
    if 'roundlakes' in email.lower() or email != 'toremalden@gmail.com':
        print("✅ SUCCESS! Work account authenticated!")
        print("="*60)
        print(f"📧 Work Email: {email}")
        print(f"📊 Total messages: {profile.get('messagesTotal', 'N/A')}")
        print(f"✓ Token saved to: {TOKEN_FILE}")
        print("\nYour work account is ready for the email manager!")
        
        # Test Calendar access too
        try:
            cal_service = build('calendar', 'v3', credentials=creds)
            calendars = cal_service.calendarList().list(maxResults=1).execute()
            print("✓ Calendar access confirmed for work account")
        except:
            pass
            
    else:
        print("⚠️  WARNING: You authenticated with your PERSONAL account!")
        print("="*60)
        print(f"Email: {email}")
        print("\nYou need to run this again and choose your WORK account")
        print("Look for your Round Lakes Commodities email option")
        
except Exception as e:
    print(f"\n❌ Error: {e}")
    print("\nTroubleshooting:")
    print("1. Make sure your work email is added as a test user")
    print("2. Try using a different browser or incognito mode")
    print("3. Clear your browser cookies for accounts.google.com")