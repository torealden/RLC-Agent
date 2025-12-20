#!/usr/bin/env python3
"""
Google OAuth Setup for RLC Agent

This script sets up OAuth authentication for Gmail and Google Calendar.
Run this once on your RLC-SERVER to authorize the agent to access your email and calendar.

Prerequisites:
1. Go to https://console.cloud.google.com/
2. Create a project (or select existing)
3. Enable Gmail API and Google Calendar API
4. Go to APIs & Services > Credentials
5. Create OAuth 2.0 Client ID (Desktop application)
6. Download the JSON file
7. Save it as 'credentials.json' in this directory

Usage:
    python setup_google_oauth.py
"""

import os
import sys
import pickle
from pathlib import Path

# Determine paths
SCRIPT_DIR = Path(__file__).parent
PROJECT_ROOT = SCRIPT_DIR.parent

# Check for Windows RLC structure
if sys.platform == "win32":
    RLC_ROOT = Path("C:/RLC")
    if (RLC_ROOT / "projects" / "rlc-agent").exists():
        PROJECT_ROOT = RLC_ROOT / "projects" / "rlc-agent"

# Token storage location
TOKEN_DIR = PROJECT_ROOT / "data" / "tokens"
CREDENTIALS_FILE = SCRIPT_DIR / "credentials.json"

# Also check project root for credentials
if not CREDENTIALS_FILE.exists():
    CREDENTIALS_FILE = PROJECT_ROOT / "credentials.json"


def check_dependencies():
    """Check if required packages are installed."""
    try:
        from google.oauth2.credentials import Credentials
        from google_auth_oauthlib.flow import InstalledAppFlow
        from google.auth.transport.requests import Request
        from googleapiclient.discovery import build
        return True
    except ImportError as e:
        print("\n❌ Missing required packages!")
        print("\nPlease install them with:")
        print("  pip install google-api-python-client google-auth-oauthlib google-auth")
        print(f"\nError: {e}")
        return False


def setup_gmail():
    """Set up Gmail OAuth."""
    print("\n" + "=" * 50)
    print("📧 Setting up Gmail Access")
    print("=" * 50)

    from google.oauth2.credentials import Credentials
    from google_auth_oauthlib.flow import InstalledAppFlow
    from google.auth.transport.requests import Request
    from googleapiclient.discovery import build

    SCOPES = [
        'https://www.googleapis.com/auth/gmail.readonly',
        'https://www.googleapis.com/auth/gmail.modify'
    ]

    token_path = TOKEN_DIR / "gmail_token.pickle"
    creds = None

    # Check existing token
    if token_path.exists():
        print(f"Found existing token at {token_path}")
        with open(token_path, 'rb') as f:
            creds = pickle.load(f)

    # Refresh or get new credentials
    if creds and creds.valid:
        print("✅ Gmail credentials are valid!")
    elif creds and creds.expired and creds.refresh_token:
        print("Refreshing expired credentials...")
        try:
            creds.refresh(Request())
            print("✅ Credentials refreshed!")
        except Exception as e:
            print(f"Could not refresh: {e}")
            creds = None

    if not creds or not creds.valid:
        if not CREDENTIALS_FILE.exists():
            print(f"\n❌ Credentials file not found!")
            print(f"   Expected: {CREDENTIALS_FILE}")
            print("\nTo fix this:")
            print("1. Go to https://console.cloud.google.com/")
            print("2. Create/select a project")
            print("3. Enable the Gmail API")
            print("4. Go to APIs & Services > Credentials")
            print("5. Create OAuth 2.0 Client ID (Desktop app)")
            print("6. Download the JSON and save as credentials.json")
            return False

        print("\n🌐 Opening browser for authentication...")
        print("   Please sign in with your Google account.")
        print("   Grant access to Gmail.\n")

        flow = InstalledAppFlow.from_client_secrets_file(
            str(CREDENTIALS_FILE), SCOPES
        )
        creds = flow.run_local_server(port=0)
        print("✅ Authentication successful!")

    # Save token
    TOKEN_DIR.mkdir(parents=True, exist_ok=True)
    with open(token_path, 'wb') as f:
        pickle.dump(creds, f)
    print(f"Token saved to: {token_path}")

    # Test connection
    try:
        service = build('gmail', 'v1', credentials=creds)
        profile = service.users().getProfile(userId='me').execute()
        print(f"\n✅ Connected as: {profile.get('emailAddress')}")
        print(f"   Total messages: {profile.get('messagesTotal')}")
        return True
    except Exception as e:
        print(f"\n⚠️  Could not verify: {e}")
        return True


def setup_calendar():
    """Set up Google Calendar OAuth."""
    print("\n" + "=" * 50)
    print("📅 Setting up Google Calendar Access")
    print("=" * 50)

    from google.oauth2.credentials import Credentials
    from google_auth_oauthlib.flow import InstalledAppFlow
    from google.auth.transport.requests import Request
    from googleapiclient.discovery import build

    SCOPES = [
        'https://www.googleapis.com/auth/calendar.readonly',
        'https://www.googleapis.com/auth/calendar.events'
    ]

    token_path = TOKEN_DIR / "calendar_token.pickle"
    creds = None

    # Check existing token
    if token_path.exists():
        print(f"Found existing token at {token_path}")
        with open(token_path, 'rb') as f:
            creds = pickle.load(f)

    # Refresh or get new credentials
    if creds and creds.valid:
        print("✅ Calendar credentials are valid!")
    elif creds and creds.expired and creds.refresh_token:
        print("Refreshing expired credentials...")
        try:
            creds.refresh(Request())
            print("✅ Credentials refreshed!")
        except Exception as e:
            print(f"Could not refresh: {e}")
            creds = None

    if not creds or not creds.valid:
        if not CREDENTIALS_FILE.exists():
            print(f"\n❌ Credentials file not found: {CREDENTIALS_FILE}")
            return False

        print("\n🌐 Opening browser for authentication...")
        print("   Please sign in with your Google account.")
        print("   Grant access to Google Calendar.\n")

        flow = InstalledAppFlow.from_client_secrets_file(
            str(CREDENTIALS_FILE), SCOPES
        )
        creds = flow.run_local_server(port=0)
        print("✅ Authentication successful!")

    # Save token
    TOKEN_DIR.mkdir(parents=True, exist_ok=True)
    with open(token_path, 'wb') as f:
        pickle.dump(creds, f)
    print(f"Token saved to: {token_path}")

    # Test connection
    try:
        service = build('calendar', 'v3', credentials=creds)
        calendar = service.calendars().get(calendarId='primary').execute()
        print(f"\n✅ Connected to: {calendar.get('summary')}")
        print(f"   Calendar ID: {calendar.get('id')}")
        return True
    except Exception as e:
        print(f"\n⚠️  Could not verify: {e}")
        return True


def main():
    """Main setup routine."""
    print("\n" + "=" * 60)
    print("  RLC Agent - Google OAuth Setup")
    print("  Email & Calendar Integration")
    print("=" * 60)

    print(f"\nProject root: {PROJECT_ROOT}")
    print(f"Token storage: {TOKEN_DIR}")
    print(f"Credentials file: {CREDENTIALS_FILE}")

    # Check dependencies
    if not check_dependencies():
        return 1

    print("\nThis script will authorize the RLC Agent to access:")
    print("  1. Gmail (read and organize emails)")
    print("  2. Google Calendar (view events)")
    print("\nA browser window will open for each service.")
    print("Sign in with your Google account and grant access.")

    input("\nPress Enter to continue...")

    gmail_ok = False
    calendar_ok = False

    # Setup Gmail
    try:
        gmail_ok = setup_gmail()
    except Exception as e:
        print(f"\n❌ Gmail setup failed: {e}")

    # Setup Calendar
    try:
        calendar_ok = setup_calendar()
    except Exception as e:
        print(f"\n❌ Calendar setup failed: {e}")

    # Summary
    print("\n" + "=" * 60)
    print("  Setup Complete!")
    print("=" * 60)

    print(f"\n  Gmail:    {'✅ Connected' if gmail_ok else '❌ Not connected'}")
    print(f"  Calendar: {'✅ Connected' if calendar_ok else '❌ Not connected'}")

    if gmail_ok or calendar_ok:
        print("\n  Your agent can now access:")
        if gmail_ok:
            print("    - check_email: View your inbox")
            print("    - get_email_content: Read email contents")
        if calendar_ok:
            print("    - check_calendar: View upcoming events")
            print("    - get_todays_schedule: See today's schedule")

        print("\n  Test it by asking your agent:")
        print('    "Check my email for anything important"')
        print('    "What\'s on my calendar today?"')

    print()
    return 0 if (gmail_ok and calendar_ok) else 1


if __name__ == "__main__":
    sys.exit(main())
