#!/usr/bin/env python3
"""
Working Email Manager - Compatible with Current Setup
Handles both accounts without scope errors
"""

import os
import json
from datetime import datetime
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# Token files for each account
TOKEN_WORK = "token_work.json"
TOKEN_PERSONAL = "token_personal.json"

def load_credentials(token_file, account_name):
    """Load credentials from token file."""
    if not os.path.exists(token_file):
        print(f"❌ {token_file} not found for {account_name}")
        return None
    
    try:
        creds = Credentials.from_authorized_user_file(token_file)
        
        # Check if token needs refresh
        if creds.expired and creds.refresh_token:
            print(f"Refreshing token for {account_name}...")
            creds.refresh(Request())
            # Save refreshed token
            with open(token_file, 'w') as f:
                f.write(creds.to_json())
        
        return creds
        
    except Exception as e:
        print(f"Error loading {account_name} credentials: {e}")
        return None

def process_emails_simple(credentials, account_name):
    """Process emails without using 'q' parameter to avoid scope errors."""
    
    print(f"\n{'='*60}")
    print(f"Processing {account_name} Emails")
    print('='*60)
    
    try:
        service = build('gmail', 'v1', credentials=credentials)
        
        # Get profile
        profile = service.users().getProfile(userId='me').execute()
        email = profile['emailAddress']
        total = profile.get('messagesTotal', 0)
        
        print(f"📧 Account: {email}")
        print(f"📊 Total messages: {total}")
        
        # Get recent messages WITHOUT using 'q' parameter
        # This works with metadata scope
        print("\nFetching recent messages...")
        results = service.users().messages().list(
            userId='me',
            maxResults=10
        ).execute()
        
        messages = results.get('messages', [])
        
        if not messages:
            print("No messages found")
            return
        
        print(f"Found {len(messages)} recent messages\n")
        
        unread_count = 0
        important_senders = []
        
        # Process each message
        for i, msg in enumerate(messages[:5], 1):
            try:
                # Get full message
                msg_data = service.users().messages().get(
                    userId='me',
                    id=msg['id']
                ).execute()
                
                # Extract headers
                headers = msg_data['payload'].get('headers', [])
                subject = next((h['value'] for h in headers if h['name'] == 'Subject'), 'No Subject')
                sender = next((h['value'] for h in headers if h['name'] == 'From'), 'Unknown')
                date = next((h['value'] for h in headers if h['name'] == 'Date'), 'Unknown')
                
                # Check if unread
                labels = msg_data.get('labelIds', [])
                is_unread = 'UNREAD' in labels
                is_important = 'IMPORTANT' in labels
                
                # Display message info
                status = "🔴 UNREAD" if is_unread else "✓ Read"
                importance = "⭐" if is_important else ""
                
                print(f"{i}. {status} {importance}")
                print(f"   From: {sender[:60]}")
                print(f"   Subject: {subject[:60]}")
                print(f"   Date: {date[:30]}")
                print()
                
                if is_unread:
                    unread_count += 1
                
                if is_important and sender not in important_senders:
                    important_senders.append(sender[:40])
                
            except Exception as e:
                print(f"   Error processing message: {e}")
        
        # Summary
        print(f"\n📊 Summary for {account_name}:")
        print(f"   • Unread in last 5: {unread_count}")
        if important_senders:
            print(f"   • Important from: {', '.join(important_senders[:3])}")
        
        # Actions you could take
        print(f"\n🤖 Suggested Actions:")
        if unread_count > 3:
            print("   • Many unread messages - consider bulk processing")
        if important_senders:
            print(f"   • Priority response needed for important senders")
        
        return True
        
    except HttpError as e:
        print(f"❌ API Error: {e}")
        if "Metadata scope" in str(e):
            print("\n⚠️  Limited scope detected. Run fix_scope_issues.py for full access")
        return False
    except Exception as e:
        print(f"❌ Error: {e}")
        return False

def check_calendar_access(credentials, account_name):
    """Check if calendar access works."""
    
    print(f"\n📅 Checking Calendar for {account_name}...")
    
    try:
        service = build('calendar', 'v3', credentials=credentials)
        
        # Try to list calendars
        calendar_list = service.calendarList().list(maxResults=1).execute()
        calendars = calendar_list.get('items', [])
        
        if calendars:
            print(f"   ✓ Calendar access works")
            print(f"   Primary calendar: {calendars[0].get('summary', 'Unknown')}")
            
            # Get upcoming events
            now = datetime.utcnow().isoformat() + 'Z'
            events_result = service.events().list(
                calendarId='primary',
                timeMin=now,
                maxResults=3,
                singleEvents=True,
                orderBy='startTime'
            ).execute()
            
            events = events_result.get('items', [])
            
            if events:
                print(f"   Upcoming events: {len(events)}")
                for event in events:
                    start = event['start'].get('dateTime', event['start'].get('date'))
                    summary = event.get('summary', 'No title')
                    print(f"     • {summary[:40]} - {start[:16]}")
            else:
                print("   No upcoming events")
                
            return True
            
        else:
            print(f"   ⚠️  No calendars found")
            return False
            
    except HttpError as e:
        if "insufficientPermissions" in str(e):
            print(f"   ✗ Calendar access denied - need to re-authenticate")
            print(f"     Run: python fix_scope_issues.py")
        else:
            print(f"   ✗ Calendar error: {str(e)[:50]}")
        return False
    except Exception as e:
        print(f"   ✗ Error: {str(e)[:50]}")
        return False

def email_categorizer(messages):
    """Simple email categorization without AI."""
    
    categories = {
        'newsletters': [],
        'personal': [],
        'work': [],
        'promotional': [],
        'automated': []
    }
    
    # Simple keyword-based categorization
    newsletter_keywords = ['newsletter', 'digest', 'weekly', 'daily', 'update']
    promo_keywords = ['sale', 'discount', 'offer', 'deal', '% off', 'save']
    automated_keywords = ['no-reply', 'noreply', 'automated', 'notification']
    
    for msg in messages:
        sender = msg.get('sender', '').lower()
        subject = msg.get('subject', '').lower()
        
        if any(kw in sender or kw in subject for kw in automated_keywords):
            categories['automated'].append(msg)
        elif any(kw in subject for kw in newsletter_keywords):
            categories['newsletters'].append(msg)
        elif any(kw in subject for kw in promo_keywords):
            categories['promotional'].append(msg)
        elif '@roundlakes' in sender or 'commodities' in subject:
            categories['work'].append(msg)
        else:
            categories['personal'].append(msg)
    
    return categories

def main():
    """Main application."""
    
    print("\n" + "="*60)
    print("📧 Email Manager - Working Version")
    print("="*60)
    print("\nThis version works with your current token setup")
    
    # Load credentials
    accounts = []
    
    # Work account
    work_creds = load_credentials(TOKEN_WORK, "Work")
    if work_creds:
        accounts.append(("Work (RLC)", work_creds))
        print("✓ Work account loaded")
    else:
        print("✗ Work account not available")
    
    # Personal account
    personal_creds = load_credentials(TOKEN_PERSONAL, "Personal")
    if personal_creds:
        accounts.append(("Personal", personal_creds))
        print("✓ Personal account loaded")
    else:
        print("✗ Personal account not available")
    
    if not accounts:
        print("\n❌ No accounts available!")
        print("Run: python setup_both_accounts.py")
        return
    
    print(f"\n✓ {len(accounts)} account(s) ready")
    
    # Menu
    print("\n" + "="*60)
    print("What would you like to do?")
    print("="*60)
    print("1. Check recent emails (all accounts)")
    print("2. Check Work account only")
    print("3. Check Personal account only")
    print("4. Test Calendar access")
    print("5. Exit")
    
    choice = input("\nYour choice (1-5): ").strip()
    
    if choice == "1":
        # Process all accounts
        for account_name, creds in accounts:
            process_emails_simple(creds, account_name)
            
    elif choice == "2":
        # Work only
        if work_creds:
            process_emails_simple(work_creds, "Work (RLC)")
            check_calendar_access(work_creds, "Work")
        else:
            print("Work account not available")
            
    elif choice == "3":
        # Personal only
        if personal_creds:
            process_emails_simple(personal_creds, "Personal")
            check_calendar_access(personal_creds, "Personal")
        else:
            print("Personal account not available")
            
    elif choice == "4":
        # Test calendar for all
        for account_name, creds in accounts:
            check_calendar_access(creds, account_name)
    
    print("\n" + "="*60)
    print("Session Complete")
    print("="*60)
    
    # Check for scope issues
    print("\n💡 Tips:")
    print("• If you see 'Metadata scope' errors, run: python fix_scope_issues.py")
    print("• If Calendar doesn't work, you need to re-authenticate with full scopes")
    print("• For AI email processing, install: pip install langchain-ollama")

if __name__ == "__main__":
    main()