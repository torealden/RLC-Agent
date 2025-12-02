#!/usr/bin/env python3
"""
RLC Master Agent - Google OAuth Setup Script
Sets up OAuth authentication for Gmail and Google Calendar
Round Lakes Commodities
"""

import sys
import os
import pickle
from pathlib import Path

# Ensure the package is in the path
sys.path.insert(0, str(Path(__file__).parent))

from config.settings import get_settings


def setup_gmail_auth(settings, account_type: str = 'work'):
    """
    Set up Gmail OAuth authentication

    Args:
        settings: Application settings
        account_type: 'work' or 'personal'
    """
    print(f"\n{'='*60}")
    print(f"Setting up Gmail OAuth ({account_type} account)")
    print('='*60)

    try:
        from google.oauth2.credentials import Credentials
        from google_auth_oauthlib.flow import InstalledAppFlow
        from google.auth.transport.requests import Request
        from googleapiclient.discovery import build
    except ImportError:
        print("\nError: Google API packages not installed.")
        print("Run: pip install google-auth google-auth-oauthlib google-api-python-client")
        return False

    SCOPES = [
        'https://www.googleapis.com/auth/gmail.readonly',
        'https://www.googleapis.com/auth/gmail.send',
        'https://www.googleapis.com/auth/gmail.modify'
    ]

    # Paths
    token_dir = Path(settings.google.token_dir)
    token_dir.mkdir(parents=True, exist_ok=True)
    token_path = token_dir / f'gmail_{account_type}_token.pickle'

    if account_type == 'work':
        credentials_path = settings.google.get_credentials_path('gmail_work')
    else:
        credentials_path = settings.google.get_credentials_path('gmail_personal')

    # Check for credentials file
    if not credentials_path.exists():
        print(f"\nError: Credentials file not found at {credentials_path}")
        print("\nTo fix this:")
        print("1. Go to https://console.cloud.google.com/")
        print("2. Create or select a project")
        print("3. Enable the Gmail API")
        print("4. Create OAuth 2.0 credentials (Desktop app)")
        print("5. Download the JSON file")
        print(f"6. Save it as: {credentials_path}")
        return False

    creds = None

    # Check for existing token
    if token_path.exists():
        print(f"Found existing token at {token_path}")
        with open(token_path, 'rb') as token:
            creds = pickle.load(token)

    # Validate or refresh credentials
    if creds and creds.valid:
        print("Existing credentials are valid!")
    elif creds and creds.expired and creds.refresh_token:
        print("Refreshing expired credentials...")
        try:
            creds.refresh(Request())
            print("Credentials refreshed successfully!")
        except Exception as e:
            print(f"Could not refresh credentials: {e}")
            creds = None

    # Get new credentials if needed
    if not creds or not creds.valid:
        print("\nStarting OAuth flow...")
        print("A browser window will open for authentication.")
        print("Please sign in with your Google account and grant permissions.\n")

        try:
            flow = InstalledAppFlow.from_client_secrets_file(
                str(credentials_path),
                SCOPES
            )
            creds = flow.run_local_server(port=0)
            print("\nAuthentication successful!")
        except Exception as e:
            print(f"\nAuthentication failed: {e}")
            return False

    # Save the token
    with open(token_path, 'wb') as token:
        pickle.dump(creds, token)
    print(f"Token saved to {token_path}")

    # Test the connection
    try:
        service = build('gmail', 'v1', credentials=creds)
        profile = service.users().getProfile(userId='me').execute()
        print(f"\nConnected as: {profile.get('emailAddress')}")
        print(f"Total messages: {profile.get('messagesTotal')}")
        return True
    except Exception as e:
        print(f"\nWarning: Could not verify connection: {e}")
        return True  # Token was still saved


def setup_calendar_auth(settings):
    """
    Set up Google Calendar OAuth authentication
    """
    print(f"\n{'='*60}")
    print("Setting up Google Calendar OAuth")
    print('='*60)

    try:
        from google.oauth2.credentials import Credentials
        from google_auth_oauthlib.flow import InstalledAppFlow
        from google.auth.transport.requests import Request
        from googleapiclient.discovery import build
    except ImportError:
        print("\nError: Google API packages not installed.")
        print("Run: pip install google-auth google-auth-oauthlib google-api-python-client")
        return False

    SCOPES = [
        'https://www.googleapis.com/auth/calendar',
        'https://www.googleapis.com/auth/calendar.events'
    ]

    # Paths
    token_dir = Path(settings.google.token_dir)
    token_dir.mkdir(parents=True, exist_ok=True)
    token_path = token_dir / 'calendar_token.pickle'
    credentials_path = settings.google.get_credentials_path('calendar')

    # Check for credentials file
    if not credentials_path.exists():
        print(f"\nError: Credentials file not found at {credentials_path}")
        print("\nTo fix this:")
        print("1. Go to https://console.cloud.google.com/")
        print("2. Create or select a project")
        print("3. Enable the Google Calendar API")
        print("4. Create OAuth 2.0 credentials (Desktop app)")
        print("5. Download the JSON file")
        print(f"6. Save it as: {credentials_path}")
        return False

    creds = None

    # Check for existing token
    if token_path.exists():
        print(f"Found existing token at {token_path}")
        with open(token_path, 'rb') as token:
            creds = pickle.load(token)

    # Validate or refresh credentials
    if creds and creds.valid:
        print("Existing credentials are valid!")
    elif creds and creds.expired and creds.refresh_token:
        print("Refreshing expired credentials...")
        try:
            creds.refresh(Request())
            print("Credentials refreshed successfully!")
        except Exception as e:
            print(f"Could not refresh credentials: {e}")
            creds = None

    # Get new credentials if needed
    if not creds or not creds.valid:
        print("\nStarting OAuth flow...")
        print("A browser window will open for authentication.")
        print("Please sign in with your Google account and grant permissions.\n")

        try:
            flow = InstalledAppFlow.from_client_secrets_file(
                str(credentials_path),
                SCOPES
            )
            creds = flow.run_local_server(port=0)
            print("\nAuthentication successful!")
        except Exception as e:
            print(f"\nAuthentication failed: {e}")
            return False

    # Save the token
    with open(token_path, 'wb') as token:
        pickle.dump(creds, token)
    print(f"Token saved to {token_path}")

    # Test the connection
    try:
        service = build('calendar', 'v3', credentials=creds)
        calendar = service.calendars().get(calendarId='primary').execute()
        print(f"\nConnected to calendar: {calendar.get('summary')}")
        print(f"Calendar ID: {calendar.get('id')}")
        return True
    except Exception as e:
        print(f"\nWarning: Could not verify connection: {e}")
        return True  # Token was still saved


def main():
    """Main setup routine"""
    print("\n" + "=" * 60)
    print("RLC Master Agent - Google OAuth Setup")
    print("Round Lakes Commodities")
    print("=" * 60)

    # Load settings
    settings = get_settings()

    print("\nThis script will set up OAuth authentication for:")
    print("1. Gmail (work account)")
    print("2. Gmail (personal account - optional)")
    print("3. Google Calendar")
    print("\nMake sure you have downloaded the OAuth credentials JSON files")
    print("from Google Cloud Console and placed them in the config/ directory.")

    # Check for .env file
    env_path = Path(__file__).parent / '.env'
    if not env_path.exists():
        example_path = Path(__file__).parent / '.env.example'
        if example_path.exists():
            print("\nNote: .env file not found. Creating from .env.example...")
            import shutil
            shutil.copy(example_path, env_path)
            print(f"Created {env_path}")
            print("Please edit this file with your configuration before running the agent.")

    input("\nPress Enter to continue...")

    success = True

    # Set up Gmail (work)
    print("\n" + "-" * 40)
    print("Step 1: Gmail (Work Account)")
    print("-" * 40)
    work_creds = settings.google.get_credentials_path('gmail_work')
    if work_creds.exists():
        if not setup_gmail_auth(settings, 'work'):
            print("Gmail (work) setup failed or skipped.")
            success = False
    else:
        print(f"\nGmail work credentials not found at {work_creds}")
        print("Skipping Gmail (work) setup.")
        print("Add the credentials file and run this script again to set up Gmail.")

    # Set up Gmail (personal) - optional
    print("\n" + "-" * 40)
    print("Step 2: Gmail (Personal Account) - Optional")
    print("-" * 40)
    personal_creds = settings.google.get_credentials_path('gmail_personal')
    if personal_creds.exists():
        response = input("Set up personal Gmail account? [y/N]: ").strip().lower()
        if response == 'y':
            if not setup_gmail_auth(settings, 'personal'):
                print("Gmail (personal) setup failed or skipped.")
    else:
        print("Personal Gmail credentials not found. Skipping.")

    # Set up Calendar
    print("\n" + "-" * 40)
    print("Step 3: Google Calendar")
    print("-" * 40)
    calendar_creds = settings.google.get_credentials_path('calendar')
    if calendar_creds.exists():
        if not setup_calendar_auth(settings):
            print("Calendar setup failed or skipped.")
            success = False
    else:
        print(f"\nCalendar credentials not found at {calendar_creds}")
        print("Skipping Calendar setup.")
        print("Add the credentials file and run this script again to set up Calendar.")

    # Summary
    print("\n" + "=" * 60)
    print("Setup Complete!")
    print("=" * 60)

    if success:
        print("\nGoogle OAuth setup completed successfully.")
        print("\nNext steps:")
        print("1. Run: python initialize_system.py   # Verify all configurations")
        print("2. Run: python launch.py              # Start the agent")
    else:
        print("\nSome services were not configured.")
        print("You can run this script again after adding the required credentials.")

    print()
    return 0 if success else 1


if __name__ == '__main__':
    sys.exit(main())
