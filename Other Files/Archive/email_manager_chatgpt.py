import os
import base64
import json
import csv
from datetime import datetime

# Google API client libraries
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build

# If using the Ollama HTTP API for LLM calls
import requests

# Constants for credentials and scopes
CREDENTIALS_PERSONAL_FILE = "credentials_personal.json"  # OAuth client secrets for personal account
CREDENTIALS_WORK_FILE = "credentials.json"               # OAuth client secrets for work account (rename if needed)
TOKEN_PERSONAL_FILE = "token_personal.json"              # Stored user token for personal Gmail
TOKEN_WORK_FILE = "token_workspace.json"                 # Stored user token for work Gmail

# Define scopes for Gmail (modify includes read, send, delete) and Calendar (for work account scheduling)
SCOPES_GMAIL = ["https://www.googleapis.com/auth/gmail.modify"]
SCOPES_WORK = SCOPES_GMAIL + ["https://www.googleapis.com/auth/calendar"]  # include Calendar scope for work if needed
SCOPES_PERSONAL = SCOPES_GMAIL  # personal email doesn't include Calendar in this example

# Ollama model name for local LLM
OLLAMA_MODEL = "llama2"  # Change this to the model you have loaded in Ollama

def get_gmail_service(creds_file, token_file, scopes):
    """Authenticate to Gmail API and return a service client for the given account."""
    creds = None
    # Load existing tokens if available
    if os.path.exists(token_file):
        creds = Credentials.from_authorized_user_file(token_file, scopes)
    # If no valid credentials, run OAuth flow to get new token
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            # Refresh expired credentials
            creds.refresh(Request())
        else:
            # Run OAuth authorization flow for new token
            flow = InstalledAppFlow.from_client_secrets_file(creds_file, scopes)
            creds = flow.run_local_server(port=0)
        # Save the new credentials for next time
        with open(token_file, "w") as token:
            token.write(creds.to_json())
    # Build the Gmail API service
    service = build("gmail", "v1", credentials=creds)
    return service

def fetch_emails(service, max_emails=5):
    """Fetch the latest emails (up to max_emails) from the Gmail inbox using the given service."""
    emails = []
    try:
        # List messages in inbox
        results = service.users().messages().list(userId="me", labelIds=["INBOX"], maxResults=max_emails).execute()
        messages = results.get("messages", [])
    except Exception as e:
        print(f"Error fetching email list: {e}")
        return emails
    if not messages:
        return emails  # No messages found
    # Get each message detail
    for msg in messages:
        msg_id = msg["id"]
        try:
            # Fetch full message data (including headers and body)
            msg_data = service.users().messages().get(userId="me", id=msg_id, format="full").execute()
        except Exception as e:
            print(f"Error fetching message ID {msg_id}: {e}")
            continue
        # Extract headers
        headers = msg_data.get("payload", {}).get("headers", [])
        sender = next((h["value"] for h in headers if h["name"] == "From"), "Unknown Sender")
        subject = next((h["value"] for h in headers if h["name"] == "Subject"), "(No Subject)")
        # Parse sender to get just the name or email address for display
        sender_name = sender
        if "<" in sender:
            # Format "Name <email>" -> just "Name"
            sender_name = sender.split("<")[0].strip().strip('"')
            if sender_name == "":
                sender_name = sender  # if name was empty, use full sender string
        # Extract email body (plain text if available, else HTML)
        body_text = ""
        payload = msg_data.get("payload", {})
        data = payload.get("body", {}).get("data")
        if data:
            # This is the case for non-multipart emails
            body_bytes = base64.urlsafe_b64decode(data)
            body_text = body_bytes.decode("utf-8", errors="ignore")
        else:
            # Multipart email - find text/plain part
            parts = payload.get("parts", [])
            for part in parts:
                mime_type = part.get("mimeType")
                part_data = part.get("body", {}).get("data")
                if mime_type == "text/plain" and part_data:
                    body_bytes = base64.urlsafe_b64decode(part_data)
                    body_text = body_bytes.decode("utf-8", errors="ignore")
                    break
                # If no plain text found, use HTML part and strip tags
                if mime_type == "text/html" and part_data and not body_text:
                    body_bytes = base64.urlsafe_b64decode(part_data)
                    html_text = body_bytes.decode("utf-8", errors="ignore")
                    # Basic HTML tag removal
                    body_text = strip_html_tags(html_text)
        # Truncate body to approximately 1000 characters for processing
        if len(body_text) > 1000:
            body_text = body_text[:1000] + "..."
        emails.append({
            "id": msg_id,
            "sender": sender_name,
            "subject": subject,
            "body": body_text
        })
    return emails

def strip_html_tags(html):
    """Remove HTML tags and decode HTML entities from a string."""
    # Simple tag stripper (could be improved or use an HTML parser if needed)
    import re, html as ihtml
    text = re.sub('<[^<]+?>', '', html)        # remove HTML tags
    text = ihtml.unescape(text)               # convert HTML entities to plain text
    return text

def summarize_email(body_text):
    """Summarize the email body in 1-2 sentences using the local LLM."""
    prompt = f"Summarize the following email in 1-2 sentences:\n\"\"\"\n{body_text}\n\"\"\""
    try:
        # Call the Ollama API to generate a summary
        response = requests.post(
            "http://localhost:11434/api/generate",
            json={"model": OLLAMA_MODEL, "prompt": prompt}
        )
        response.raise_for_status()
        # The Ollama API returns a streaming response; get the full content
        result = response.json()  # expecting the final output as JSON
        # Depending on Ollama's API, adjust parsing:
        summary_text = result.get("output") or result.get("response") or ""
        summary = summary_text.strip()
    except Exception as e:
        print(f"LLM summarization error: {e}")
        summary = "(Summary not available)"
    return summary

def propose_action(email_summary):
    """Use LLM to propose an action (reply, delete, spam, schedule, or nothing) for the email, with rationale."""
    prompt = (
        f"You are an email assistant. Given the following email summary, suggest one of the actions: "
        f"reply, delete, mark as spam, schedule event, or do nothing. Provide the action and a brief rationale.\n"
        f"Email Summary: \"{email_summary}\"\n"
        f"Answer in the format: <action> (<rationale>)"
    )
    try:
        response = requests.post(
            "http://localhost:11434/api/generate",
            json={"model": OLLAMA_MODEL, "prompt": prompt}
        )
        response.raise_for_status()
        result = response.json()
        output_text = result.get("output") or result.get("response") or ""
        recommendation = output_text.strip()
    except Exception as e:
        print(f"LLM action proposal error: {e}")
        recommendation = "do nothing (unable to determine action)"
    # Parse the output to separate action and rationale
    action = None
    rationale = ""
    if "(" in recommendation:
        # Expect format like "reply (because ...)" or "reply (...)."
        action = recommendation.split("(")[0].strip().lower()
        rationale = recommendation.split("(", 1)[1].rstrip(")")
    else:
        action = recommendation.strip().lower()
    # Normalize action to one of expected values
    valid_actions = {"reply", "delete", "mark as spam", "schedule event", "do nothing"}
    if action not in valid_actions:
        # If LLM gives something not in our list, default to "do nothing"
        rationale = recommendation  # treat the whole output as rationale
        action = "do nothing"
    return action, rationale

def prompt_user_for_action(email_info, summary, proposed_action, rationale):
    """Display the email summary and proposed action, and prompt user for approval."""
    sender = email_info["sender"]
    subject = email_info["subject"]
    # Prepare the prompt message
    print(f"\nEmail from {sender} – Subject: {subject}")
    print(f"Summary: {summary}")
    print(f"Proposed Action: {proposed_action} ({rationale})")
    choice = input("Approve this action? [y/n]: ").strip().lower()
    approved = (choice == 'y')
    return approved

# Action execution stub functions
def reply_to_email(email):
    """Stub for replying to an email (to be implemented with Gmail API)."""
    # TODO: Implement actual reply using Gmail API (create draft or send message)
    print(f"(Would send a reply to email from {email['sender']} with subject '{email['subject']}')")

def delete_email(email):
    """Stub for deleting an email (to be implemented with Gmail API)."""
    # TODO: Implement actual deletion using Gmail API (move to Trash)
    print(f"(Would delete email from {email['sender']} with subject '{email['subject']}')")

def mark_as_spam(email):
    """Stub for marking an email as spam (to be implemented with Gmail API)."""
    # TODO: Implement marking as spam using Gmail API (modifyLabels to add SPAM label)
    print(f"(Would mark email from {email['sender']} with subject '{email['subject']}' as spam)")

def schedule_event(email):
    """Stub for scheduling an event (to be implemented with Google Calendar API)."""
    # TODO: Implement actual event scheduling using Calendar API
    print(f"(Would create a calendar event for email from {email['sender']} - subject '{email['subject']}')")

def do_nothing(email):
    """No action to be taken for this email."""
    print("(No action taken for this email)")

def log_decision(log_file, email, summary, action, approved):
    """Append the decision details to a CSV log file with timestamp."""
    decision_str = "approved" if approved else "rejected"
    # Ensure we have a header if file is new
    file_exists = os.path.exists(log_file)
    with open(log_file, 'a', newline='', encoding='utf-8') as csvfile:
        fieldnames = ["timestamp", "sender", "subject", "summary", "action", "decision"]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        if not file_exists:
            writer.writeheader()
        writer.writerow({
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "sender": email["sender"],
            "subject": email["subject"],
            "summary": summary,
            "action": action,
            "decision": decision_str
        })

def main():
    # Authenticate and get Gmail service for both personal and work accounts
    personal_service = get_gmail_service(CREDENTIALS_PERSONAL_FILE, TOKEN_PERSONAL_FILE, SCOPES_PERSONAL)
    work_service = get_gmail_service(CREDENTIALS_WORK_FILE, TOKEN_WORK_FILE, SCOPES_WORK)

    # Fetch the last 5 emails from each account
    personal_emails = fetch_emails(personal_service, max_emails=5)
    work_emails = fetch_emails(work_service, max_emails=5)

    # Process emails from Personal account
    if personal_emails:
        print("\nProcessing Personal Account Emails...")
    for email in personal_emails:
        summary = summarize_email(email["body"])
        action, rationale = propose_action(summary)
        approved = prompt_user_for_action(email, summary, action, rationale)
        if approved:
            # Execute the approved action (simulate via stub)
            if action == "reply":
                reply_to_email(email)
            elif action == "delete":
                delete_email(email)
            elif action == "mark as spam":
                mark_as_spam(email)
            elif action == "schedule event":
                schedule_event(email)
            else:
                do_nothing(email)
        else:
            # User rejected the suggested action
            print(f"Action '{action}' was not approved. Skipping this email.")
        # Log the decision
        log_decision("email_actions_log.csv", email, summary, action, approved)

    # Process emails from Work account
    if work_emails:
        print("\nProcessing Work Account Emails...")
    for email in work_emails:
        summary = summarize_email(email["body"])
        action, rationale = propose_action(summary)
        approved = prompt_user_for_action(email, summary, action, rationale)
        if approved:
            if action == "reply":
                reply_to_email(email)
            elif action == "delete":
                delete_email(email)
            elif action == "mark as spam":
                mark_as_spam(email)
            elif action == "schedule event":
                schedule_event(email)
            else:
                do_nothing(email)
        else:
            print(f"Action '{action}' was not approved. Skipping this email.")
        log_decision("email_actions_log.csv", email, summary, action, approved)

if __name__ == "__main__":
    main()
