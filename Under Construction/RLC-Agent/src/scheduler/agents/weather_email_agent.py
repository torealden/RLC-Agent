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
import requests
from datetime import datetime, timedelta
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from pathlib import Path
from typing import Optional, List, Dict, Any, Tuple

from dotenv import load_dotenv
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# Database connectivity (optional)
try:
    import psycopg2
    from psycopg2.extras import Json
    DB_AVAILABLE = True
except ImportError:
    DB_AVAILABLE = False

# Location service (optional)
try:
    from src.services.location_service import LocationService, get_location_service
    LOCATION_SERVICE_AVAILABLE = True
except ImportError:
    LOCATION_SERVICE_AVAILABLE = False

# City enrollment service (optional)
try:
    from src.services.city_enrollment_service import CityEnrollmentService, get_enrollment_service
    ENROLLMENT_SERVICE_AVAILABLE = True
except ImportError:
    ENROLLMENT_SERVICE_AVAILABLE = False

# Configuration paths
BASE_DIR = Path(__file__).parent.parent
CONFIG_DIR = BASE_DIR / "config"
CREDENTIALS_DIR = Path(r"C:\Users\torem\RLC Dropbox\RLC Team Folder\RLC Documents\LLM Model and Documents\Projects\Desktop Assistant")

# Load environment variables for API keys
PROJECT_ROOT = Path(__file__).parent.parent.parent
load_dotenv(PROJECT_ROOT / ".env")
load_dotenv(PROJECT_ROOT / "config" / "credentials.env")

# Graphics output directory
DATA_DIR = PROJECT_ROOT / "data"
WEATHER_GRAPHICS_DIR = DATA_DIR / "weather_graphics"

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
    "log_file": "weather_email_agent.log",
    # Graphics extraction
    "extract_graphics": True,
    "graphics_dir": str(WEATHER_GRAPHICS_DIR),
    # LLM Configuration
    "llm_enabled": True,
    "llm_provider": "ollama",  # "ollama", "openai", or "anthropic"
    "ollama_url": "http://localhost:11434",
    "ollama_model": "llama3",
    "openai_model": "gpt-4o-mini",
    "anthropic_model": "claude-3-haiku-20240307",
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

        # Database config
        self.db_config = {
            'host': os.getenv('DB_HOST', 'localhost'),
            'port': int(os.getenv('DB_PORT', 5432)),
            'database': os.getenv('DB_NAME', 'rlc_commodities'),
            'user': os.getenv('DB_USER', 'postgres'),
            'password': os.getenv('DB_PASSWORD', '')
        }

        # Location service for matching cities to weather locations
        self.location_service = None
        if LOCATION_SERVICE_AVAILABLE:
            try:
                self.location_service = get_location_service()
            except Exception as e:
                self._log(f"Location service not available: {e}")

        # Enrollment service for auto-enrolling new cities
        self.enrollment_service = None
        if ENROLLMENT_SERVICE_AVAILABLE:
            try:
                self.enrollment_service = get_enrollment_service()
            except Exception as e:
                self._log(f"Enrollment service not available: {e}")

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

    def _get_db_connection(self):
        """Get database connection if available."""
        if not DB_AVAILABLE:
            return None
        try:
            return psycopg2.connect(**self.db_config)
        except Exception as e:
            self._log(f"Database connection failed: {e}")
            return None

    def match_cities_to_locations(self, cities: List[str]) -> Tuple[List[str], List[str]]:
        """
        Match extracted city names to known weather location IDs.

        Args:
            cities: List of city/region names from text extraction

        Returns:
            Tuple of (matched_location_ids, unmatched_city_names)
        """
        if not self.location_service:
            return [], cities

        matched_ids = []
        unmatched_cities = []

        for city in cities:
            # Skip generic region names (not specific locations)
            generic_terms = ['midwest', 'corn belt', 'wheat belt', 'delta',
                           'great plains', 'gulf coast', 'south america']
            if city.lower() in generic_terms:
                continue

            # Try exact alias match first
            location_id = self.location_service.resolve_alias(city)
            if location_id and location_id not in matched_ids:
                matched_ids.append(location_id)
                continue

            # Try fuzzy match
            matches = self.location_service.fuzzy_match(city, threshold=0.75)
            if matches:
                best_match = matches[0][0]  # (location_id, score)
                if best_match not in matched_ids:
                    matched_ids.append(best_match)
            else:
                # City not found - mark for potential enrollment
                if city not in unmatched_cities:
                    unmatched_cities.append(city)

        return matched_ids, unmatched_cities

    def auto_enroll_cities(self, city_names: List[str]) -> List[str]:
        """
        Automatically enroll unmatched cities in the weather system.

        This triggers:
        1. Geocoding to get coordinates
        2. Region classification
        3. Addition to location registry (JSON + database)
        4. Historical data collection (1 year backfill)

        Args:
            city_names: List of city names to enroll

        Returns:
            List of newly enrolled location IDs
        """
        if not self.enrollment_service:
            self._log("Enrollment service not available - cannot auto-enroll cities")
            return []

        enrolled_ids = []
        for city in city_names:
            try:
                self._log(f"Auto-enrolling new city: {city}")
                location_id = self.enrollment_service.enroll_city(
                    city_name=city,
                    pull_historical=True  # Triggers 1-year backfill
                )
                if location_id:
                    enrolled_ids.append(location_id)
                    self._log(f"Successfully enrolled: {city} -> {location_id}")
                else:
                    self._log(f"Failed to enroll city: {city}")
            except Exception as e:
                self._log(f"Error enrolling {city}: {e}")

        return enrolled_ids

    def save_email_extract_to_db(
        self,
        email_id: str,
        email_subject: str,
        email_from: str,
        email_date: datetime,
        extracted_locations: List[str],
        matched_location_ids: List[str],
        weather_summary: str,
        llm_model: str = None
    ) -> bool:
        """
        Save extracted weather email data to bronze.weather_email_extract.

        Args:
            email_id: Gmail message ID
            email_subject: Email subject
            email_from: Sender email
            email_date: Email timestamp
            extracted_locations: Raw city names from text
            matched_location_ids: Resolved location IDs
            weather_summary: LLM-generated summary
            llm_model: Model used for extraction

        Returns:
            True if saved successfully
        """
        conn = self._get_db_connection()
        if not conn:
            return False

        try:
            cursor = conn.cursor()

            sql = """
                INSERT INTO bronze.weather_email_extract (
                    email_id, email_subject, email_from, email_date,
                    extracted_locations, matched_location_ids,
                    weather_summary, llm_model, collected_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (email_id)
                DO UPDATE SET
                    extracted_locations = EXCLUDED.extracted_locations,
                    matched_location_ids = EXCLUDED.matched_location_ids,
                    weather_summary = EXCLUDED.weather_summary,
                    llm_model = EXCLUDED.llm_model,
                    collected_at = EXCLUDED.collected_at
            """

            cursor.execute(sql, (
                email_id,
                email_subject[:500] if email_subject else None,
                email_from[:200] if email_from else None,
                email_date,
                extracted_locations,
                matched_location_ids,
                weather_summary,
                llm_model,
                datetime.now()
            ))

            conn.commit()
            cursor.close()
            conn.close()
            self._log(f"Saved email extract to database: {email_id[:20]}...")
            return True

        except Exception as e:
            self._log(f"Error saving email extract: {e}")
            if conn:
                conn.rollback()
                conn.close()
            return False

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

            # Extract and save images if enabled
            extracted_images = []
            if self.config.get("extract_graphics", True):
                # Parse date for directory organization
                try:
                    from email.utils import parsedate_to_datetime
                    email_datetime = parsedate_to_datetime(headers.get('Date', ''))
                    date_str = email_datetime.strftime('%Y-%m-%d')
                except Exception:
                    date_str = datetime.now().strftime('%Y-%m-%d')

                extracted_images = self._extract_and_save_images(
                    msg['payload'], msg_id, date_str
                )

            return {
                'id': msg_id,
                'thread_id': msg.get('threadId'),
                'subject': headers.get('Subject', 'No Subject'),
                'from': headers.get('From', 'Unknown'),
                'to': headers.get('To', ''),
                'date': headers.get('Date', ''),
                'body': body_text,
                'labels': msg.get('labelIds', []),
                'snippet': msg.get('snippet', ''),
                'extracted_images': extracted_images
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

    def _extract_and_save_images(self, payload: dict, email_id: str, email_date: str) -> List[str]:
        """
        Extract image attachments and inline images from email payload.

        Args:
            payload: Email payload from Gmail API
            email_id: Gmail message ID
            email_date: Date string for organizing files (YYYY-MM-DD)

        Returns:
            List of saved image file paths
        """
        if not self.config.get("extract_graphics", True):
            return []

        saved_images = []
        graphics_dir = Path(self.config.get("graphics_dir", WEATHER_GRAPHICS_DIR))

        # Create date-specific directory
        date_dir = graphics_dir / email_date
        date_dir.mkdir(parents=True, exist_ok=True)

        image_counter = 0

        def process_part(part, depth=0):
            nonlocal image_counter

            mime_type = part.get('mimeType', '')
            filename = part.get('filename', '')

            # Check if this is an image
            is_image = (
                mime_type.startswith('image/') or
                filename.lower().endswith(('.png', '.jpg', '.jpeg', '.gif', '.bmp', '.webp'))
            )

            if is_image and part.get('body'):
                body = part['body']

                # Get attachment data
                if 'attachmentId' in body:
                    # Need to fetch attachment separately
                    try:
                        attachment = self.service.users().messages().attachments().get(
                            userId='me',
                            messageId=email_id,
                            id=body['attachmentId']
                        ).execute()
                        data = attachment.get('data', '')
                    except Exception as e:
                        self._log(f"Error fetching attachment: {e}")
                        return
                elif 'data' in body:
                    data = body['data']
                else:
                    return

                if data:
                    try:
                        # Decode base64
                        image_data = base64.urlsafe_b64decode(data)

                        # Generate filename
                        if filename:
                            # Sanitize filename
                            safe_filename = re.sub(r'[^\w\-_\.]', '_', filename)
                        else:
                            # Generate filename from mime type
                            ext = mime_type.split('/')[-1] if '/' in mime_type else 'png'
                            ext = ext.split(';')[0]  # Handle mime type parameters
                            image_counter += 1
                            safe_filename = f"weather_image_{email_id[:8]}_{image_counter:03d}.{ext}"

                        # Save image
                        image_path = date_dir / safe_filename
                        with open(image_path, 'wb') as f:
                            f.write(image_data)

                        saved_images.append(str(image_path))
                        self._log(f"Saved image: {image_path}")

                    except Exception as e:
                        self._log(f"Error saving image: {e}")

            # Recurse into multipart
            if 'parts' in part:
                for subpart in part['parts']:
                    process_part(subpart, depth + 1)

        # Start processing
        process_part(payload)

        return saved_images

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

    def _call_llm(self, prompt: str, system_prompt: str = None) -> Optional[str]:
        """Call configured LLM provider and return response."""
        provider = self.config.get("llm_provider", "ollama")

        try:
            if provider == "ollama":
                return self._call_ollama(prompt, system_prompt)
            elif provider == "openai":
                return self._call_openai(prompt, system_prompt)
            elif provider == "anthropic":
                return self._call_anthropic(prompt, system_prompt)
            else:
                self._log(f"Unknown LLM provider: {provider}")
                return None
        except Exception as e:
            self._log(f"LLM call failed: {e}")
            return None

    def _call_ollama(self, prompt: str, system_prompt: str = None) -> Optional[str]:
        """Call Ollama local LLM."""
        url = self.config.get("ollama_url", "http://localhost:11434")
        model = self.config.get("ollama_model", "llama3")

        full_prompt = prompt
        if system_prompt:
            full_prompt = f"{system_prompt}\n\n{prompt}"

        try:
            response = requests.post(
                f"{url}/api/generate",
                json={
                    "model": model,
                    "prompt": full_prompt,
                    "stream": False,
                },
                timeout=300  # 5 minutes for long weather emails
            )
            response.raise_for_status()
            return response.json().get("response", "").strip()
        except requests.exceptions.ConnectionError:
            self._log(f"Ollama not available at {url}")
            return None
        except Exception as e:
            self._log(f"Ollama error: {e}")
            return None

    def _call_openai(self, prompt: str, system_prompt: str = None) -> Optional[str]:
        """Call OpenAI API."""
        api_key = os.getenv("OPENAI_API_KEY")
        if not api_key:
            self._log("OPENAI_API_KEY not found in environment")
            return None

        model = self.config.get("openai_model", "gpt-4o-mini")

        try:
            import openai
            client = openai.OpenAI(api_key=api_key)

            messages = []
            if system_prompt:
                messages.append({"role": "system", "content": system_prompt})
            messages.append({"role": "user", "content": prompt})

            response = client.chat.completions.create(
                model=model,
                messages=messages,
                temperature=0.3,
                max_tokens=1500
            )
            return response.choices[0].message.content.strip()
        except ImportError:
            self._log("openai package not installed")
            return None
        except Exception as e:
            self._log(f"OpenAI error: {e}")
            return None

    def _call_anthropic(self, prompt: str, system_prompt: str = None) -> Optional[str]:
        """Call Anthropic API."""
        api_key = os.getenv("ANTHROPIC_API_KEY")
        if not api_key:
            self._log("ANTHROPIC_API_KEY not found in environment")
            return None

        model = self.config.get("anthropic_model", "claude-3-haiku-20240307")

        try:
            import anthropic
            client = anthropic.Anthropic(api_key=api_key)

            response = client.messages.create(
                model=model,
                max_tokens=1500,
                system=system_prompt or "",
                messages=[{"role": "user", "content": prompt}]
            )
            return response.content[0].text.strip()
        except ImportError:
            self._log("anthropic package not installed")
            return None
        except Exception as e:
            self._log(f"Anthropic error: {e}")
            return None

    def _chunk_text(self, text: str, chunk_size: int = 2000, overlap: int = 200) -> List[str]:
        """Split text into overlapping chunks for LLM processing."""
        if len(text) <= chunk_size:
            return [text]

        chunks = []
        start = 0
        while start < len(text):
            end = start + chunk_size
            # Try to break at a sentence boundary
            if end < len(text):
                # Look for sentence endings near the chunk boundary
                for boundary in ['. ', '.\n', '\n\n', '\n']:
                    last_boundary = text.rfind(boundary, start + chunk_size - 300, end)
                    if last_boundary > start:
                        end = last_boundary + len(boundary)
                        break

            chunks.append(text[start:end].strip())
            start = end - overlap  # Overlap to maintain context

        return chunks

    def _extract_weather_data_from_chunk(self, chunk: str, email_subject: str) -> Optional[str]:
        """Extract key weather data from a single chunk using LLM."""
        system_prompt = "Extract weather data concisely. List only: locations, temperatures, precipitation amounts, and conditions."

        prompt = f"""From this weather report section, extract key data:

Subject: {email_subject}
---
{chunk}
---

List in this format:
LOCATIONS: [cities/regions mentioned]
TEMPS: [any temperature values]
PRECIP: [rainfall/snow amounts]
CONDITIONS: [drought/wet/storm/frost etc]

Be brief - just the facts."""

        return self._call_llm(prompt, system_prompt)

    def _summarize_email_extractions(self, extractions: List[str], email_subject: str) -> Optional[str]:
        """Combine multiple chunk extractions into one email summary."""
        if not extractions:
            return None

        if len(extractions) == 1:
            return extractions[0]

        combined = "\n---\n".join(extractions)

        system_prompt = "Combine these weather data extracts into one concise summary. Remove duplicates."

        prompt = f"""Combine these extracts from "{email_subject}" into one summary:

{combined}

Provide a single consolidated summary with:
- Key locations and their conditions
- Temperature ranges
- Precipitation outlook
- Notable weather events"""

        return self._call_llm(prompt, system_prompt)

    def generate_llm_summary(self, emails: List[Dict[str, Any]]) -> Optional[str]:
        """Generate an intelligent weather summary using LLM with chunking for long emails."""
        if not emails:
            return None

        self._log(f"Generating LLM summary for {len(emails)} emails...")

        # Process each email separately with chunking
        email_summaries = []
        all_cities = []

        for i, email in enumerate(emails, 1):
            self._log(f"Processing email {i}/{len(emails)}: {email['subject'][:50]}...")

            body = email['body']
            body_length = len(body)

            # Chunk long emails
            if body_length > 2500:
                self._log(f"  Email is {body_length} chars, chunking...")
                chunks = self._chunk_text(body, chunk_size=2000, overlap=200)
                self._log(f"  Split into {len(chunks)} chunks")

                # Extract data from each chunk
                chunk_extractions = []
                for j, chunk in enumerate(chunks):
                    self._log(f"  Processing chunk {j+1}/{len(chunks)}...")
                    extraction = self._extract_weather_data_from_chunk(chunk, email['subject'])
                    if extraction:
                        chunk_extractions.append(extraction)
                        # Extract cities from the extraction
                        cities = self.extract_cities(extraction)
                        all_cities.extend(cities)

                # Combine chunk extractions
                if chunk_extractions:
                    email_summary = self._summarize_email_extractions(chunk_extractions, email['subject'])
                    if email_summary:
                        email_summaries.append(f"[{email['subject']}]\n{email_summary}")
            else:
                # Short email - process directly
                extraction = self._extract_weather_data_from_chunk(body, email['subject'])
                if extraction:
                    email_summaries.append(f"[{email['subject']}]\n{extraction}")
                    cities = self.extract_cities(extraction)
                    all_cities.extend(cities)

        if not email_summaries:
            self._log("No extractions obtained from emails")
            return None

        # Final synthesis of all email summaries
        self._log("Generating final synthesis...")
        combined_summaries = "\n\n".join(email_summaries)

        system_prompt = """You are an agricultural commodity weather analyst. Create actionable weather summaries for grain traders. Be concise and specific."""

        prompt = f"""Create a trading-focused weather brief from these summaries:

{combined_summaries}

Format your response as:

KEY POINTS:
- (3-4 bullet points for trading decisions)

US CONDITIONS:
- Corn Belt: (temps, precip, crop impact)
- Wheat Belt: (temps, precip, crop impact)
- Delta/South: (conditions)

SOUTH AMERICA:
- Brazil: (Mato Grosso, Parana, RS conditions)
- Argentina: (Buenos Aires, Cordoba conditions)

WATCH LIST:
- (2-3 key items to monitor)

Be specific about temperatures (°F) and precipitation (inches) where available."""

        final_summary = self._call_llm(prompt, system_prompt)

        if final_summary:
            # Add header and metadata
            provider = self.config.get('llm_provider', 'ollama')
            model_key = f"{provider}_model"
            model_name = self.config.get(model_key, provider)

            # Add unique cities found
            unique_cities = list(set(all_cities))
            cities_line = f"Locations tracked: {', '.join(unique_cities)}" if unique_cities else ""

            summary = [
                "=" * 60,
                "WEATHER INTELLIGENCE BRIEF",
                f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M')}",
                f"Emails analyzed: {len(emails)}",
                f"Source: World Weather Inc.",
                "=" * 60,
                "",
                final_summary,
                "",
                "=" * 60,
                cities_line,
                f"Analysis by: {provider.upper()} ({model_name})",
                "=" * 60,
            ]
            return "\n".join(summary)

        return None

    def generate_summary(self, emails: List[Dict[str, Any]]) -> str:
        """Generate a weather summary from collected emails.

        Tries LLM-powered summary first if enabled, falls back to regex extraction.
        """
        if not emails:
            return "No new weather emails to summarize."

        # Try LLM summary if enabled
        if self.config.get("llm_enabled", False):
            llm_summary = self.generate_llm_summary(emails)
            if llm_summary:
                self._log("Using LLM-generated summary")
                return llm_summary
            self._log("LLM summary failed, falling back to regex extraction")

        # Fallback: regex-based extraction
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

    def process_emails(self, forward: bool = True, summarize: bool = True, auto_enroll: bool = True) -> Dict[str, Any]:
        """Main processing loop - search, forward, and summarize.

        Args:
            forward: Whether to forward emails to recipients
            summarize: Whether to generate and send summary
            auto_enroll: Whether to auto-enroll unmatched cities (default: True)

        Returns:
            Dict with processing results
        """
        result = {
            'success': False,
            'emails_found': 0,
            'emails_forwarded': 0,
            'summary_sent': False,
            'cities_extracted': [],
            'locations_matched': [],
            'cities_enrolled': [],
            'emails_saved_to_db': 0,
            'images_extracted': 0,
            'image_paths': []
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

        # Extract and save cities, match to locations, and save to database
        all_matched_locations = set()
        all_unmatched_cities = set()

        for email in emails:
            cities = self.extract_cities(email['body'])

            # Track new cities
            for city in cities:
                if city not in self.extracted_cities:
                    self.extracted_cities.append(city)
                    result['cities_extracted'].append(city)

            # Match to weather location IDs (returns matched, unmatched)
            matched_ids, unmatched = self.match_cities_to_locations(cities)
            all_matched_locations.update(matched_ids)
            all_unmatched_cities.update(unmatched)

            # Track extracted images
            extracted_images = email.get('extracted_images', [])
            if extracted_images:
                result['images_extracted'] += len(extracted_images)
                result['image_paths'].extend(extracted_images)

        # Auto-enroll unmatched cities if enabled
        if auto_enroll and all_unmatched_cities:
            self._log(f"Found {len(all_unmatched_cities)} unmatched cities: {', '.join(all_unmatched_cities)}")
            enrolled_ids = self.auto_enroll_cities(list(all_unmatched_cities))
            result['cities_enrolled'] = enrolled_ids
            all_matched_locations.update(enrolled_ids)

        # Now process each email with final matched locations
        for email in emails:
            cities = self.extract_cities(email['body'])
            matched_ids, _ = self.match_cities_to_locations(cities)

            # Save email extract to database (with LLM summary if available)
            email_summary = None
            if summarize and self.config.get("llm_enabled", False):
                # Get a concise summary for this specific email
                email_summary = self._extract_weather_data_from_chunk(
                    email['body'][:3000],
                    email['subject']
                )

            # Parse email date
            email_date = None
            try:
                from email.utils import parsedate_to_datetime
                email_date = parsedate_to_datetime(email['date'])
            except Exception:
                email_date = datetime.now()

            # Save to database
            saved = self.save_email_extract_to_db(
                email_id=email['id'],
                email_subject=email['subject'],
                email_from=email['from'],
                email_date=email_date,
                extracted_locations=cities,
                matched_location_ids=matched_ids,
                weather_summary=email_summary,
                llm_model=self.config.get(f"{self.config.get('llm_provider', 'ollama')}_model")
            )
            if saved:
                result['emails_saved_to_db'] += 1

        result['locations_matched'] = list(all_matched_locations)

        if result['cities_extracted']:
            self._save_cities()
            self._log(f"New cities added: {', '.join(result['cities_extracted'])}")

        if result['locations_matched']:
            self._log(f"Matched to locations: {', '.join(result['locations_matched'])}")

        if result.get('cities_enrolled'):
            self._log(f"Auto-enrolled new cities: {', '.join(result['cities_enrolled'])}")

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
        print(f"Saved to DB: {result.get('emails_saved_to_db', 0)}")
        print(f"New cities: {', '.join(result['cities_extracted']) if result['cities_extracted'] else 'None'}")
        print(f"Matched locations: {', '.join(result.get('locations_matched', [])) if result.get('locations_matched') else 'None'}")
        print(f"Cities enrolled: {', '.join(result.get('cities_enrolled', [])) if result.get('cities_enrolled') else 'None'}")
        print(f"Images extracted: {result.get('images_extracted', 0)}")
        if result.get('image_paths'):
            print(f"Image locations:")
            for img_path in result['image_paths'][:5]:  # Show first 5
                print(f"  - {img_path}")
            if len(result['image_paths']) > 5:
                print(f"  ... and {len(result['image_paths']) - 5} more")


if __name__ == "__main__":
    main()
