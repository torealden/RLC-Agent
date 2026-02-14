#!/usr/bin/env python3
"""
Calendar Transfer Script

Transfers future events from personal Google Calendar to work calendar.
Run this after setting up OAuth for both accounts.

Usage:
    python transfer_calendar.py
"""

import os
import sys
import pickle
from pathlib import Path
from datetime import datetime, timedelta

# Paths
SCRIPT_DIR = Path(__file__).parent
PROJECT_ROOT = SCRIPT_DIR.parent

if sys.platform == "win32":
    RLC_ROOT = Path("C:/RLC")
    if (RLC_ROOT / "projects" / "rlc-agent").exists():
        PROJECT_ROOT = RLC_ROOT / "projects" / "rlc-agent"

TOKEN_DIR = PROJECT_ROOT / "data" / "tokens"


def load_credentials(token_file):
    """Load credentials from pickle file."""
    token_path = TOKEN_DIR / token_file
    if not token_path.exists():
        return None

    with open(token_path, 'rb') as f:
        creds = pickle.load(f)

    # Refresh if needed
    if creds and creds.expired and creds.refresh_token:
        from google.auth.transport.requests import Request
        creds.refresh(Request())
        with open(token_path, 'wb') as f:
            pickle.dump(creds, f)

    return creds


def get_calendar_service(creds):
    """Build Calendar API service."""
    from googleapiclient.discovery import build
    return build('calendar', 'v3', credentials=creds)


def get_future_events(service, max_results=500):
    """Get all future events from a calendar."""
    now = datetime.utcnow().isoformat() + 'Z'

    events = []
    page_token = None

    while True:
        response = service.events().list(
            calendarId='primary',
            timeMin=now,
            maxResults=min(250, max_results - len(events)),
            singleEvents=True,
            orderBy='startTime',
            pageToken=page_token
        ).execute()

        events.extend(response.get('items', []))
        page_token = response.get('nextPageToken')

        if not page_token or len(events) >= max_results:
            break

    return events


def copy_event(source_event, dest_service):
    """Copy an event to destination calendar."""
    # Build new event (exclude source-specific fields)
    new_event = {
        'summary': source_event.get('summary', 'Untitled'),
        'description': source_event.get('description', ''),
        'location': source_event.get('location', ''),
        'start': source_event.get('start'),
        'end': source_event.get('end'),
    }

    # Copy recurrence if present
    if 'recurrence' in source_event:
        new_event['recurrence'] = source_event['recurrence']

    # Copy reminders if present
    if 'reminders' in source_event:
        new_event['reminders'] = source_event['reminders']

    # Add note about source
    if new_event['description']:
        new_event['description'] += '\n\n---\n[Transferred from personal calendar]'
    else:
        new_event['description'] = '[Transferred from personal calendar]'

    # Create in destination calendar
    created = dest_service.events().insert(
        calendarId='primary',
        body=new_event
    ).execute()

    return created


def main():
    print("\n" + "=" * 60)
    print("  Calendar Transfer: Personal → Work")
    print("=" * 60)

    # Check dependencies
    try:
        from googleapiclient.discovery import build
        from google.auth.transport.requests import Request
    except ImportError:
        print("\n❌ Google API packages not installed.")
        print("Run: pip install google-api-python-client google-auth")
        return 1

    # Load credentials
    print("\n📂 Loading credentials...")

    personal_creds = load_credentials('calendar_personal_token.pickle')
    work_creds = load_credentials('calendar_token.pickle')

    if not personal_creds:
        print("\n❌ Personal calendar token not found!")
        print("   Expected: data/tokens/calendar_personal_token.pickle")
        print("\n   If you haven't set up the personal account yet:")
        print("   1. Run: python deployment/setup_google_oauth.py")
        print("   2. Sign in with toremalden@gmail.com")
        print("   3. Rename the token: Rename-Item .\\data\\tokens\\calendar_token.pickle calendar_personal_token.pickle")
        return 1

    if not work_creds:
        print("\n❌ Work calendar token not found!")
        print("   Expected: data/tokens/calendar_token.pickle")
        print("\n   Run: python deployment/setup_google_oauth.py")
        print("   Sign in with tore.alden@roundlakescommodities.com")
        return 1

    # Build services
    print("✅ Credentials loaded")

    try:
        personal_service = get_calendar_service(personal_creds)
        work_service = get_calendar_service(work_creds)

        # Get calendar info
        personal_cal = personal_service.calendars().get(calendarId='primary').execute()
        work_cal = work_service.calendars().get(calendarId='primary').execute()

        print(f"\n📅 Source (Personal): {personal_cal.get('summary', 'Primary')}")
        print(f"📅 Destination (Work): {work_cal.get('summary', 'Primary')}")

    except Exception as e:
        print(f"\n❌ Error connecting to calendars: {e}")
        return 1

    # Get future events from personal calendar
    print("\n🔍 Fetching future events from personal calendar...")

    try:
        events = get_future_events(personal_service)
        print(f"   Found {len(events)} future events")
    except Exception as e:
        print(f"\n❌ Error fetching events: {e}")
        return 1

    if not events:
        print("\n✅ No future events to transfer!")
        return 0

    # Show preview
    print("\n📋 Events to transfer:")
    print("-" * 50)

    for i, event in enumerate(events[:10]):
        start = event['start'].get('dateTime', event['start'].get('date'))
        title = event.get('summary', 'Untitled')
        print(f"   {i+1}. {start[:10]} - {title[:40]}")

    if len(events) > 10:
        print(f"   ... and {len(events) - 10} more events")

    print("-" * 50)

    # Confirm
    response = input(f"\nTransfer {len(events)} events to work calendar? [y/N]: ").strip().lower()

    if response != 'y':
        print("\n❌ Transfer cancelled.")
        return 0

    # Transfer events
    print("\n🔄 Transferring events...")

    success = 0
    failed = 0

    for i, event in enumerate(events):
        title = event.get('summary', 'Untitled')
        try:
            copy_event(event, work_service)
            success += 1
            print(f"   ✅ [{i+1}/{len(events)}] {title[:40]}")
        except Exception as e:
            failed += 1
            print(f"   ❌ [{i+1}/{len(events)}] {title[:40]} - {str(e)[:30]}")

    # Summary
    print("\n" + "=" * 60)
    print("  Transfer Complete!")
    print("=" * 60)
    print(f"\n  ✅ Transferred: {success}")
    print(f"  ❌ Failed: {failed}")

    if success > 0:
        print("\n  Your work calendar now has all future events.")
        print("  The agent will use tore.alden@roundlakescommodities.com")
        print("  for all scheduling going forward.")

    print()
    return 0


if __name__ == "__main__":
    sys.exit(main())
