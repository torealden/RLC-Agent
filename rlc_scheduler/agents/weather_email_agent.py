#!/usr/bin/env python3
"""
Weather Email Agent - Meteorologist Email Forwarding and Summarization

This agent:
1. Searches for emails from configured meteorologist senders
2. Forwards them to Felipe and other configured recipients
3. Parses weather content and generates summary reports
4. Extracts city/location mentions for weather tracking
"""

import os
import sys
import json
import re
import base64
from datetime import datetime, timedelta
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from pathlib import Path
from typing import Optional, List, Dict, Any

from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# Configuration paths
BASE_DIR = Path(__file__).parent.parent
CONFIG_DIR = BASE_DIR / "config"
CREDENTIALS_DIR = Path(r"C:\Users\torem\Dropbox\RLC Documents\LLM Model and Documents\Projects\Desktop Assistant")

# Default configuration
DEFAULT_CONFIG = {
    "meteorologist_senders": [
        # Add meteorologist email addresses here
        # "meteorologist@example.com"
    ],
    "forward_recipients": [
        # "felipe@roundlakes.com"  # Add Felipe's email
    ],
    "summary_recipients": [
        # Recipients for the daily weather summary
    ],
    "token_file": "token_work.json",  # or "token_personal.json"
    "check_hours_back": 24,  # How far back to look for emails
    "processed_ids_file": "weather_emails_processed.json",
    "cities_file": "weather_cities.json",
    "log_file": "weather_email_agent.log"
}


class WeatherEmailAgent:
    """Agent for handling meteorologist emails."""

    def __init__(self, config_path: Optional[str] = None):
        """Initialize the agent with configuration."""
        self.config = self._load_config(config_path)
        self.credentials = None
        self.service = None
        self.processed_ids = self._load_processed_ids()
        self.extracted_cities = self._load_cities()

    def _load_config(self, config_path: Optional[str]) -> dict:
        """Load configuration from file or use defaults."""
        config = DEFAULT_CONFIG.copy()

        if config_path and Path(config_path).exists():
            with open(config_path, 'r') as f:
                user_config = json.load(f)
                config.update(user_config)
        else:
            # Try default config location
            default_path = CONFIG_DIR / "weather_email_config.json"
            if default_path.exists():
                with open(default_path, 'r') as f:
                    user_config = json.load(f)
                    config.update(user_config)

        return config

    def _load_processed_ids(self) -> set:
        """Load set of already-processed email IDs."""
        path = CONFIG_DIR / self.config["processed_ids_file"]
        if path.exists():
            with open(path, 'r') as f:
                return set(json.load(f))
        return set()

    def _save_processed_ids(self):
        """Save processed email IDs."""
        path = CONFIG_DIR / self.config["processed_ids_file"]
        with open(path, 'w') as f:
            json.dump(list(self.processed_ids), f)

    def _load_cities(self) -> List[str]:
        """Load list of tracked cities."""
        path = CONFIG_DIR / self.config["cities_file"]
        if path.exists():
            with open(path, 'r') as f:
                return json.load(f)
        return []

    def _save_cities(self):
        """Save tracked cities list."""
        path = CONFIG_DIR / self.config["cities_file"]
        with open(path, 'w') as f:
            json.dump(self.extracted_cities, f, indent=2)

    def _log(self, message: str):
        """Log message to file and stdout."""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_line = f"[{timestamp}] {message}"
        print(log_line)

        log_path = CONFIG_DIR / self.config["log_file"]
        with open(log_path, 'a') as f:
            f.write(log_line + "\n")

    def connect(self) -> bool:
        """Connect to Gmail API using stored credentials."""
        token_path = CREDENTIALS_DIR / self.config["token_file"]

        if not token_path.exists():
            self._log(f"Token file not found: {token_path}")
            return False

        try:
            self.credentials = Credentials.from_authorized_user_file(str(token_path))

            # Refresh if expired
            if self.credentials.expired and self.credentials.refresh_token:
                self._log("Refreshing expired token...")
                self.credentials.refresh(Request())
                with open(token_path, 'w') as f:
                    f.write(self.credentials.to_json())

            self.service = build('gmail', 'v1', credentials=self.credentials)

            # Verify connection
            profile = self.service.users().getProfile(userId='me').execute()
            self._log(f"Connected to Gmail as: {profile['emailAddress']}")
            return True

        except Exception as e:
            self._log(f"Failed to connect: {e}")
            return False

    def search_meteorologist_emails(self, hours_back: Optional[int] = None) -> List[Dict[str, Any]]:
        """Search for emails from configured meteorologist senders."""
        if not self.service:
            self._log("Not connected to Gmail")
            return []

        hours = hours_back or self.config["check_hours_back"]
        after_date = datetime.now() - timedelta(hours=hours)
        after_str = after_date.strftime("%Y/%m/%d")

        emails = []

        for sender in self.config["meteorologist_senders"]:
            try:
                query = f"from:{sender} after:{after_str}"
                self._log(f"Searching: {query}")

                results = self.service.users().messages().list(
                    userId='me',
                    q=query,
                    maxResults=50
                ).execute()

                messages = results.get('messages', [])
                self._log(f"Found {len(messages)} emails from {sender}")

                for msg in messages:
                    if msg['id'] not in self.processed_ids:
                        email_data = self._get_email_details(msg['id'])
                        if email_data:
                            emails.append(email_data)

            except HttpError as e:
                if "Metadata scope" in str(e):
                    self._log(f"Scope error - need full Gmail access. Run fix_scope_issues.py")
                else:
                    self._log(f"Error searching for {sender}: {e}")
            except Exception as e:
                self._log(f"Error searching for {sender}: {e}")

        return emails

    def _get_email_details(self, msg_id: str) -> Optional[Dict[str, Any]]:
        """Get full details of an email."""
        try:
            msg = self.service.users().messages().get(
                userId='me',
                id=msg_id,
                format='full'
            ).execute()

            headers = {h['name']: h['value'] for h in msg['payload'].get('headers', [])}

            # Extract body
            body_text = self._extract_body(msg['payload'])

            return {
                'id': msg_id,
                'thread_id': msg.get('threadId'),
                'subject': headers.get('Subject', 'No Subject'),
                'from': headers.get('From', 'Unknown'),
                'to': headers.get('To', ''),
                'date': headers.get('Date', ''),
                'body': body_text,
                'labels': msg.get('labelIds', []),
                'snippet': msg.get('snippet', '')
            }

        except Exception as e:
            self._log(f"Error getting email {msg_id}: {e}")
            return None

    def _extract_body(self, payload: dict) -> str:
        """Extract text body from email payload."""
        body_text = ""

        # Check for simple body
        if 'body' in payload and payload['body'].get('data'):
            body_text = base64.urlsafe_b64decode(payload['body']['data']).decode('utf-8', errors='ignore')

        # Check for multipart
        if 'parts' in payload:
            for part in payload['parts']:
                mime_type = part.get('mimeType', '')

                if mime_type == 'text/plain':
                    if part.get('body', {}).get('data'):
                        body_text = base64.urlsafe_b64decode(part['body']['data']).decode('utf-8', errors='ignore')
                        break
                elif mime_type == 'text/html' and not body_text:
                    if part.get('body', {}).get('data'):
                        html = base64.urlsafe_b64decode(part['body']['data']).decode('utf-8', errors='ignore')
                        # Simple HTML to text conversion
                        body_text = re.sub(r'<[^>]+>', ' ', html)
                        body_text = re.sub(r'\s+', ' ', body_text).strip()
                elif mime_type.startswith('multipart/'):
                    # Recursive extraction
                    nested = self._extract_body(part)
                    if nested:
                        body_text = nested

        return body_text

    def forward_email(self, email: Dict[str, Any], recipients: Optional[List[str]] = None) -> bool:
        """Forward an email to configured recipients."""
        if not self.service:
            self._log("Not connected to Gmail")
            return False

        to_list = recipients or self.config["forward_recipients"]
        if not to_list:
            self._log("No forward recipients configured")
            return False

        try:
            # Create forwarded message
            message = MIMEMultipart()
            message['To'] = ', '.join(to_list)
            message['Subject'] = f"Fwd: {email['subject']}"

            # Build forwarded body
            fwd_header = f"""
---------- Forwarded message ---------
From: {email['from']}
Date: {email['date']}
Subject: {email['subject']}

"""
            body = fwd_header + email['body']
            message.attach(MIMEText(body, 'plain'))

            # Encode and send
            raw = base64.urlsafe_b64encode(message.as_bytes()).decode()

            self.service.users().messages().send(
                userId='me',
                body={'raw': raw}
            ).execute()

            self._log(f"Forwarded '{email['subject']}' to {', '.join(to_list)}")
            return True

        except Exception as e:
            self._log(f"Error forwarding email: {e}")
            return False

    def extract_cities(self, text: str) -> List[str]:
        """Extract city/location names from weather text."""
        # Common US agricultural regions and cities
        known_cities = [
            # Corn Belt
            "Des Moines", "Omaha", "Kansas City", "Sioux Falls", "Minneapolis",
            "Chicago", "Indianapolis", "Columbus", "Springfield", "Cedar Rapids",
            # Wheat Belt
            "Amarillo", "Dodge City", "Wichita", "Oklahoma City", "Lubbock",
            # Southern
            "Memphis", "Little Rock", "Dallas", "Houston", "New Orleans",
            # Brazil
            "Sao Paulo", "Mato Grosso", "Parana", "Rio Grande do Sul", "Goias",
            # Argentina
            "Buenos Aires", "Cordoba", "Rosario", "Santa Fe",
            # General
            "Gulf Coast", "Midwest", "Great Plains", "Delta", "Corn Belt"
        ]

        found_cities = []
        text_lower = text.lower()

        for city in known_cities:
            if city.lower() in text_lower:
                if city not in found_cities:
                    found_cities.append(city)

        # Also look for state abbreviations with weather context
        state_pattern = r'\b([A-Z]{2})\b(?=.*(?:rain|dry|drought|temp|degrees|inches))'
        state_matches = re.findall(state_pattern, text, re.IGNORECASE)

        us_states = ['IA', 'IL', 'NE', 'KS', 'MN', 'SD', 'ND', 'MO', 'OH', 'IN',
                     'TX', 'OK', 'CO', 'MT', 'WY', 'AR', 'LA', 'MS', 'TN', 'KY']

        for state in state_matches:
            if state.upper() in us_states and state.upper() not in found_cities:
                found_cities.append(state.upper())

        return found_cities

    def generate_summary(self, emails: List[Dict[str, Any]]) -> str:
        """Generate a weather summary from collected emails."""
        if not emails:
            return "No new weather emails to summarize."

        summary_parts = [
            "=" * 60,
            "WEATHER SUMMARY REPORT",
            f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M')}",
            f"Emails processed: {len(emails)}",
            "=" * 60,
            ""
        ]

        all_cities = []

        for i, email in enumerate(emails, 1):
            summary_parts.append(f"--- Email {i}: {email['subject']} ---")
            summary_parts.append(f"From: {email['from']}")
            summary_parts.append(f"Date: {email['date']}")
            summary_parts.append("")

            # Extract key weather information
            body = email['body']

            # Look for temperature mentions
            temps = re.findall(r'(\d{1,3})\s*(?:degrees?|°|F)', body, re.IGNORECASE)
            if temps:
                summary_parts.append(f"Temperatures mentioned: {', '.join(temps[:5])}°F")

            # Look for precipitation
            precip = re.findall(r'(\d+\.?\d*)\s*(?:inch|in|mm|cm)', body, re.IGNORECASE)
            if precip:
                summary_parts.append(f"Precipitation amounts: {', '.join(precip[:5])}")

            # Look for key weather words
            weather_words = ['drought', 'dry', 'rain', 'wet', 'flood', 'frost',
                          'freeze', 'heat', 'cold', 'storm', 'snow', 'wind']
            found_weather = [w for w in weather_words if w.lower() in body.lower()]
            if found_weather:
                summary_parts.append(f"Weather conditions: {', '.join(found_weather)}")

            # Extract cities
            cities = self.extract_cities(body)
            if cities:
                summary_parts.append(f"Locations: {', '.join(cities)}")
                all_cities.extend(cities)

            # Add snippet
            summary_parts.append(f"Preview: {email['snippet'][:200]}...")
            summary_parts.append("")

        # Overall summary
        summary_parts.append("=" * 60)
        summary_parts.append("LOCATIONS MENTIONED (for weather tracking):")
        unique_cities = list(set(all_cities))
        summary_parts.append(", ".join(unique_cities) if unique_cities else "None identified")
        summary_parts.append("=" * 60)

        return "\n".join(summary_parts)

    def send_summary(self, summary: str, recipients: Optional[List[str]] = None) -> bool:
        """Send summary report via email."""
        if not self.service:
            self._log("Not connected to Gmail")
            return False

        to_list = recipients or self.config["summary_recipients"]
        if not to_list:
            # Default to forward recipients if no summary recipients
            to_list = self.config["forward_recipients"]

        if not to_list:
            self._log("No recipients for summary")
            return False

        try:
            message = MIMEText(summary, 'plain')
            message['To'] = ', '.join(to_list)
            message['Subject'] = f"Weather Summary - {datetime.now().strftime('%Y-%m-%d')}"

            raw = base64.urlsafe_b64encode(message.as_bytes()).decode()

            self.service.users().messages().send(
                userId='me',
                body={'raw': raw}
            ).execute()

            self._log(f"Sent summary to {', '.join(to_list)}")
            return True

        except Exception as e:
            self._log(f"Error sending summary: {e}")
            return False

    def process_emails(self, forward: bool = True, summarize: bool = True) -> Dict[str, Any]:
        """Main processing loop - search, forward, and summarize."""
        result = {
            'success': False,
            'emails_found': 0,
            'emails_forwarded': 0,
            'summary_sent': False,
            'cities_extracted': []
        }

        if not self.connect():
            self._log("Failed to connect to Gmail")
            return result

        # Search for meteorologist emails
        self._log("Searching for meteorologist emails...")
        emails = self.search_meteorologist_emails()
        result['emails_found'] = len(emails)

        if not emails:
            self._log("No new meteorologist emails found")
            result['success'] = True
            return result

        self._log(f"Found {len(emails)} new emails to process")

        # Forward emails
        if forward and self.config["forward_recipients"]:
            for email in emails:
                if self.forward_email(email):
                    result['emails_forwarded'] += 1
                    self.processed_ids.add(email['id'])

        # Generate and send summary
        if summarize:
            summary = self.generate_summary(emails)

            # Save summary to file
            summary_path = CONFIG_DIR / f"weather_summary_{datetime.now().strftime('%Y%m%d_%H%M')}.txt"
            with open(summary_path, 'w') as f:
                f.write(summary)
            self._log(f"Summary saved to {summary_path}")

            # Print summary
            print("\n" + summary)

            # Send if recipients configured
            if self.config["summary_recipients"] or self.config["forward_recipients"]:
                result['summary_sent'] = self.send_summary(summary)

        # Extract and save cities
        for email in emails:
            cities = self.extract_cities(email['body'])
            for city in cities:
                if city not in self.extracted_cities:
                    self.extracted_cities.append(city)
                    result['cities_extracted'].append(city)

        if result['cities_extracted']:
            self._save_cities()
            self._log(f"New cities added: {', '.join(result['cities_extracted'])}")

        # Save processed IDs
        self._save_processed_ids()

        result['success'] = True
        self._log("Processing complete")
        return result


def main():
    """Command-line interface."""
    import argparse

    parser = argparse.ArgumentParser(description='Weather Email Agent')
    parser.add_argument('--config', help='Path to config file')
    parser.add_argument('--no-forward', action='store_true', help='Skip forwarding')
    parser.add_argument('--no-summary', action='store_true', help='Skip summary')
    parser.add_argument('--hours', type=int, default=24, help='Hours to look back')
    parser.add_argument('--test', action='store_true', help='Test mode - just search, no send')

    args = parser.parse_args()

    print("=" * 60)
    print("Weather Email Agent")
    print("=" * 60)

    agent = WeatherEmailAgent(args.config)

    if args.test:
        print("\n[TEST MODE - No emails will be sent]\n")
        if agent.connect():
            emails = agent.search_meteorologist_emails(args.hours)
            if emails:
                print(f"\nFound {len(emails)} emails:")
                for email in emails:
                    print(f"  - {email['subject']}")
                    print(f"    From: {email['from']}")
                    print(f"    Date: {email['date']}")
                print("\nSummary preview:")
                print(agent.generate_summary(emails))
            else:
                print("No emails found from meteorologist senders")
                print(f"Configured senders: {agent.config['meteorologist_senders']}")
    else:
        result = agent.process_emails(
            forward=not args.no_forward,
            summarize=not args.no_summary
        )

        print("\n" + "=" * 60)
        print("RESULTS")
        print("=" * 60)
        print(f"Emails found: {result['emails_found']}")
        print(f"Emails forwarded: {result['emails_forwarded']}")
        print(f"Summary sent: {result['summary_sent']}")
        print(f"New cities: {', '.join(result['cities_extracted']) if result['cities_extracted'] else 'None'}")


if __name__ == "__main__":
    main()
