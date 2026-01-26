#!/usr/bin/env python3
"""
Gmail Authentication Setup for Weather Email Agent

Run this script to authenticate with Gmail and get fresh tokens.
This will open a browser window for Google OAuth.

Required: credentials.json from Google Cloud Console
"""

import os
import sys
from pathlib import Path

from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build

# Scopes needed for the weather email agent
# - gmail.readonly: Read emails
# - gmail.send: Send/forward emails
# - gmail.modify: Mark as read, archive
SCOPES = [
    'https://www.googleapis.com/auth/gmail.readonly',
    'https://www.googleapis.com/auth/gmail.send',
    'https://www.googleapis.com/auth/gmail.modify',
]

# Paths
CREDENTIALS_DIR = Path(r"C:\Users\torem\RLC Dropbox\RLC Team Folder\RLC Documents\LLM Model and Documents\Projects\Desktop Assistant")
CREDENTIALS_FILE = CREDENTIALS_DIR / "credentials.json"  # or credentials_desktop.json
TOKEN_FILE = CREDENTIALS_DIR / "token_work.json"


def setup_credentials():
    """Set up Gmail credentials with proper scopes."""

    creds = None

    # Check if token exists and is valid
    if TOKEN_FILE.exists():
        creds = Credentials.from_authorized_user_file(str(TOKEN_FILE), SCOPES)

        if creds and creds.valid:
            print(f"Existing token is valid")
            return creds

        if creds and creds.expired and creds.refresh_token:
            print("Attempting to refresh token...")
            try:
                creds.refresh(Request())
                with open(TOKEN_FILE, 'w') as f:
                    f.write(creds.to_json())
                print("Token refreshed successfully")
                return creds
            except Exception as e:
                print(f"Refresh failed: {e}")
                print("Will need to re-authenticate...")

    # Need to authenticate from scratch
    if not CREDENTIALS_FILE.exists():
        print(f"\nError: credentials.json not found at {CREDENTIALS_FILE}")
        print("\nTo fix this:")
        print("1. Go to https://console.cloud.google.com/")
        print("2. Create or select a project")
        print("3. Enable the Gmail API")
        print("4. Go to Credentials > Create Credentials > OAuth Client ID")
        print("5. Select 'Desktop Application'")
        print("6. Download the JSON file and save as credentials.json")
        return None

    print("\nStarting OAuth flow...")
    print("A browser window will open for Google authentication.")
    print(f"Make sure to sign in with the account you want to use.\n")

    flow = InstalledAppFlow.from_client_secrets_file(
        str(CREDENTIALS_FILE),
        SCOPES
    )

    creds = flow.run_local_server(port=8080)

    # Save the token
    with open(TOKEN_FILE, 'w') as f:
        f.write(creds.to_json())

    print(f"\nToken saved to {TOKEN_FILE}")
    return creds


def test_connection(creds):
    """Test the Gmail connection."""

    print("\nTesting Gmail connection...")

    try:
        service = build('gmail', 'v1', credentials=creds)

        # Get profile
        profile = service.users().getProfile(userId='me').execute()
        email = profile['emailAddress']

        print(f"Connected as: {email}")

        # Test reading
        results = service.users().messages().list(
            userId='me',
            maxResults=3
        ).execute()

        messages = results.get('messages', [])
        print(f"Can read emails: Yes ({len(messages)} recent messages)")

        # Test labels (indicates modify access)
        labels = service.users().labels().list(userId='me').execute()
        print(f"Can access labels: Yes ({len(labels.get('labels', []))} labels)")

        print("\nSetup complete! The weather email agent should now work.")
        return True

    except Exception as e:
        print(f"Error: {e}")
        return False


def main():
    print("=" * 60)
    print("Gmail Authentication Setup")
    print("=" * 60)

    print(f"\nCredentials file: {CREDENTIALS_FILE}")
    print(f"Token file: {TOKEN_FILE}")
    print(f"Scopes requested: {', '.join(SCOPES)}")

    # Delete existing token to force re-auth
    if '--force' in sys.argv and TOKEN_FILE.exists():
        print("\n--force specified, deleting existing token...")
        os.remove(TOKEN_FILE)

    creds = setup_credentials()

    if creds:
        test_connection(creds)
    else:
        print("\nAuthentication failed.")
        sys.exit(1)


if __name__ == "__main__":
    main()
