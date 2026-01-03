"""
Calendar Management Agent for RLC Master Agent
Handles Google Calendar integration and scheduling
Round Lakes Commodities
"""

import logging
from typing import Dict, Any, Optional, List, Tuple
from datetime import datetime, timedelta
from pathlib import Path
from dataclasses import dataclass, field
from enum import Enum

logger = logging.getLogger('rlc_master_agent.calendar_agent')


class EventType(Enum):
    """Types of calendar events"""
    MEETING = "meeting"
    CALL = "call"
    TASK = "task"
    REMINDER = "reminder"
    BLOCKED = "blocked"
    TRAVEL = "travel"


@dataclass
class CalendarEvent:
    """Represents a calendar event"""
    id: str
    title: str
    start: datetime
    end: datetime
    description: str = ""
    location: str = ""
    attendees: List[str] = field(default_factory=list)
    event_type: EventType = EventType.MEETING
    is_all_day: bool = False
    recurrence: Optional[str] = None
    status: str = "confirmed"
    organizer: str = ""
    link: str = ""


@dataclass
class TimeSlot:
    """Represents an available time slot"""
    start: datetime
    end: datetime
    duration_minutes: int = 0

    def __post_init__(self):
        if self.duration_minutes == 0:
            self.duration_minutes = int((self.end - self.start).total_seconds() / 60)


class CalendarAgent:
    """
    Agent responsible for Google Calendar integration.

    Features:
    - View calendar events
    - Find available time slots
    - Create/modify/delete events
    - Smart scheduling based on preferences
    - Conflict detection
    """

    def __init__(
        self,
        settings: Optional[Any] = None,
        approval_manager: Optional[Any] = None,
        verification_agent: Optional[Any] = None
    ):
        """
        Initialize Calendar Agent

        Args:
            settings: Application settings
            approval_manager: Approval manager for event operations
            verification_agent: Verification agent for event validation
        """
        self.settings = settings
        self.approval_manager = approval_manager
        self.verification_agent = verification_agent
        self._calendar_service = None
        self._credentials = None

        # User preferences
        self.working_hours_start = 8
        self.working_hours_end = 18
        self.preferred_duration = 30  # minutes
        self.timezone = 'America/Chicago'

        if settings and hasattr(settings, 'user'):
            self.working_hours_start = settings.user.working_hours_start
            self.working_hours_end = settings.user.working_hours_end
            self.preferred_duration = settings.user.preferred_meeting_duration
            self.timezone = settings.user.timezone

        logger.info("Calendar Agent initialized")

    def _get_calendar_service(self):
        """Get or create Google Calendar API service"""
        if self._calendar_service is not None:
            return self._calendar_service

        try:
            from google.oauth2.credentials import Credentials
            from google_auth_oauthlib.flow import InstalledAppFlow
            from google.auth.transport.requests import Request
            from googleapiclient.discovery import build
            import pickle

            SCOPES = [
                'https://www.googleapis.com/auth/calendar',
                'https://www.googleapis.com/auth/calendar.events'
            ]

            creds = None
            token_path = None
            credentials_path = None

            if self.settings and hasattr(self.settings, 'google'):
                token_dir = Path(self.settings.google.token_dir)
                token_path = token_dir / 'calendar_token.pickle'
                credentials_path = self.settings.google.get_credentials_path('calendar')

            # Try to load existing token
            if token_path and token_path.exists():
                with open(token_path, 'rb') as token:
                    creds = pickle.load(token)

            # Refresh or get new credentials
            if not creds or not creds.valid:
                if creds and creds.expired and creds.refresh_token:
                    creds.refresh(Request())
                elif credentials_path and credentials_path.exists():
                    flow = InstalledAppFlow.from_client_secrets_file(
                        str(credentials_path), SCOPES
                    )
                    creds = flow.run_local_server(port=0)

                # Save the token
                if token_path and creds:
                    token_path.parent.mkdir(parents=True, exist_ok=True)
                    with open(token_path, 'wb') as token:
                        pickle.dump(creds, token)

            if creds:
                self._calendar_service = build('calendar', 'v3', credentials=creds)
                self._credentials = creds
                logger.info("Calendar service initialized")
                return self._calendar_service

        except ImportError as e:
            logger.error(f"Calendar dependencies not installed: {e}")
        except Exception as e:
            logger.error(f"Failed to initialize Calendar service: {e}")

        return None

    # -------------------------------------------------------------------------
    # Event Fetching
    # -------------------------------------------------------------------------

    def get_events(
        self,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        max_results: int = 50
    ) -> List[CalendarEvent]:
        """
        Get calendar events

        Args:
            start_date: Start of range (defaults to now)
            end_date: End of range (defaults to 7 days from now)
            max_results: Maximum events to return

        Returns:
            List of CalendarEvent objects
        """
        service = self._get_calendar_service()
        if not service:
            logger.warning("Calendar service not available")
            return []

        if start_date is None:
            start_date = datetime.now()
        if end_date is None:
            end_date = start_date + timedelta(days=7)

        try:
            events_result = service.events().list(
                calendarId='primary',
                timeMin=start_date.isoformat() + 'Z',
                timeMax=end_date.isoformat() + 'Z',
                maxResults=max_results,
                singleEvents=True,
                orderBy='startTime'
            ).execute()

            events = []
            for item in events_result.get('items', []):
                event = self._parse_event(item)
                if event:
                    events.append(event)

            return events

        except Exception as e:
            logger.error(f"Failed to fetch events: {e}")
            return []

    def get_today_events(self) -> List[CalendarEvent]:
        """Get today's events"""
        today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        tomorrow = today + timedelta(days=1)
        return self.get_events(start_date=today, end_date=tomorrow)

    def get_upcoming_events(self, hours: int = 24) -> List[CalendarEvent]:
        """Get events in the next N hours"""
        now = datetime.now()
        end = now + timedelta(hours=hours)
        return self.get_events(start_date=now, end_date=end)

    def _parse_event(self, item: Dict) -> Optional[CalendarEvent]:
        """Parse Google Calendar API event item"""
        try:
            # Parse start/end times
            start_data = item.get('start', {})
            end_data = item.get('end', {})

            is_all_day = 'date' in start_data

            if is_all_day:
                start = datetime.strptime(start_data['date'], '%Y-%m-%d')
                end = datetime.strptime(end_data['date'], '%Y-%m-%d')
            else:
                start = datetime.fromisoformat(start_data['dateTime'].replace('Z', '+00:00'))
                end = datetime.fromisoformat(end_data['dateTime'].replace('Z', '+00:00'))

            # Extract attendees
            attendees = [
                a.get('email', '') for a in item.get('attendees', [])
            ]

            # Determine event type from title
            title = item.get('summary', 'Untitled')
            event_type = EventType.MEETING
            title_lower = title.lower()
            if 'call' in title_lower or 'phone' in title_lower:
                event_type = EventType.CALL
            elif 'blocked' in title_lower or 'focus' in title_lower:
                event_type = EventType.BLOCKED
            elif 'travel' in title_lower:
                event_type = EventType.TRAVEL
            elif 'reminder' in title_lower:
                event_type = EventType.REMINDER

            return CalendarEvent(
                id=item['id'],
                title=title,
                start=start,
                end=end,
                description=item.get('description', ''),
                location=item.get('location', ''),
                attendees=attendees,
                event_type=event_type,
                is_all_day=is_all_day,
                recurrence=item.get('recurrence', [None])[0] if item.get('recurrence') else None,
                status=item.get('status', 'confirmed'),
                organizer=item.get('organizer', {}).get('email', ''),
                link=item.get('htmlLink', '')
            )

        except Exception as e:
            logger.error(f"Failed to parse event: {e}")
            return None

    # -------------------------------------------------------------------------
    # Availability & Scheduling
    # -------------------------------------------------------------------------

    def find_free_slots(
        self,
        date: Optional[datetime] = None,
        duration_minutes: int = 30,
        within_days: int = 7
    ) -> List[TimeSlot]:
        """
        Find available time slots

        Args:
            date: Starting date (defaults to today)
            duration_minutes: Required slot duration
            within_days: How many days to search

        Returns:
            List of available TimeSlot objects
        """
        if date is None:
            date = datetime.now()

        start_date = date.replace(hour=0, minute=0, second=0, microsecond=0)
        end_date = start_date + timedelta(days=within_days)

        # Get existing events
        events = self.get_events(start_date=start_date, end_date=end_date)

        free_slots = []
        current_date = start_date

        while current_date < end_date:
            # Skip weekends if configured
            if current_date.weekday() < 5:  # Monday-Friday
                day_slots = self._find_day_slots(current_date, events, duration_minutes)
                free_slots.extend(day_slots)

            current_date += timedelta(days=1)

        return free_slots

    def _find_day_slots(
        self,
        date: datetime,
        events: List[CalendarEvent],
        duration_minutes: int
    ) -> List[TimeSlot]:
        """Find free slots for a specific day"""
        slots = []

        # Set working hours for this day
        day_start = date.replace(hour=self.working_hours_start, minute=0)
        day_end = date.replace(hour=self.working_hours_end, minute=0)

        # Skip if day is in the past
        now = datetime.now()
        if day_end < now:
            return []

        if day_start < now:
            day_start = now.replace(second=0, microsecond=0)
            # Round up to next 15-minute mark
            minutes = (day_start.minute // 15 + 1) * 15
            if minutes >= 60:
                day_start = day_start.replace(hour=day_start.hour + 1, minute=0)
            else:
                day_start = day_start.replace(minute=minutes)

        # Filter events for this day
        day_events = [
            e for e in events
            if e.start.date() == date.date() and not e.is_all_day
        ]

        # Sort by start time
        day_events.sort(key=lambda e: e.start)

        # Find gaps between events
        current = day_start

        for event in day_events:
            if event.start > current:
                slot_duration = (event.start - current).total_seconds() / 60
                if slot_duration >= duration_minutes:
                    slots.append(TimeSlot(
                        start=current,
                        end=event.start,
                        duration_minutes=int(slot_duration)
                    ))

            # Move current past this event
            if event.end > current:
                current = event.end

        # Check for slot after last event
        if current < day_end:
            slot_duration = (day_end - current).total_seconds() / 60
            if slot_duration >= duration_minutes:
                slots.append(TimeSlot(
                    start=current,
                    end=day_end,
                    duration_minutes=int(slot_duration)
                ))

        return slots

    def suggest_meeting_time(
        self,
        duration_minutes: int = 30,
        preferred_days: Optional[List[int]] = None,
        within_days: int = 7
    ) -> Optional[TimeSlot]:
        """
        Suggest the best meeting time

        Args:
            duration_minutes: Meeting duration
            preferred_days: Preferred weekdays (0=Monday, 4=Friday)
            within_days: Search range

        Returns:
            Best available TimeSlot or None
        """
        slots = self.find_free_slots(duration_minutes=duration_minutes, within_days=within_days)

        if not slots:
            return None

        # Filter by preferred days if specified
        if preferred_days:
            preferred_slots = [s for s in slots if s.start.weekday() in preferred_days]
            if preferred_slots:
                slots = preferred_slots

        # Prefer morning slots (before noon)
        morning_slots = [s for s in slots if s.start.hour < 12]
        if morning_slots:
            return morning_slots[0]

        return slots[0]

    # -------------------------------------------------------------------------
    # Event Creation
    # -------------------------------------------------------------------------

    def create_event(
        self,
        title: str,
        start: datetime,
        end: datetime,
        description: str = "",
        location: str = "",
        attendees: Optional[List[str]] = None,
        require_approval: bool = True
    ) -> Tuple[bool, str, Optional[CalendarEvent]]:
        """
        Create a calendar event

        Args:
            title: Event title
            start: Start datetime
            end: End datetime
            description: Event description
            location: Event location
            attendees: List of attendee emails
            require_approval: Whether to require approval

        Returns:
            Tuple of (success, message, created_event)
        """
        attendees = attendees or []

        # Verify the event if verification agent available
        if self.verification_agent:
            existing_events = self.get_events(
                start_date=start - timedelta(hours=2),
                end_date=end + timedelta(hours=2)
            )

            user_prefs = {
                'working_hours_start': self.working_hours_start,
                'working_hours_end': self.working_hours_end,
                'preferred_meeting_duration': self.preferred_duration
            }

            verification = self.verification_agent.verify_calendar_event(
                title=title,
                start_time=start,
                end_time=end,
                attendees=attendees,
                existing_events=[{'start': e.start, 'end': e.end, 'title': e.title} for e in existing_events],
                user_preferences=user_prefs
            )

            if not verification.is_acceptable():
                issues = '; '.join(verification.issues)
                return False, f"Event verification failed: {issues}", None

        # Check approval
        if require_approval and self.approval_manager:
            from approval_manager import ActionType
            approved, reason = self.approval_manager.request_approval(
                action_type=ActionType.CALENDAR_CREATE,
                description=f"Create event: {title}",
                details={
                    'title': title,
                    'start': start.isoformat(),
                    'end': end.isoformat(),
                    'attendees': attendees
                }
            )
            if not approved:
                return False, f"Event not created: {reason}", None

        service = self._get_calendar_service()
        if not service:
            return False, "Calendar service not available", None

        try:
            event_body = {
                'summary': title,
                'description': description,
                'location': location,
                'start': {'dateTime': start.isoformat(), 'timeZone': self.timezone},
                'end': {'dateTime': end.isoformat(), 'timeZone': self.timezone},
            }

            if attendees:
                event_body['attendees'] = [{'email': a} for a in attendees]

            created = service.events().insert(
                calendarId='primary',
                body=event_body,
                sendUpdates='all' if attendees else 'none'
            ).execute()

            event = self._parse_event(created)
            logger.info(f"Event created: {title} at {start}")

            return True, f"Event created: {created.get('htmlLink', '')}", event

        except Exception as e:
            logger.error(f"Failed to create event: {e}")
            return False, f"Failed to create event: {str(e)}", None

    def quick_meeting(
        self,
        title: str,
        duration_minutes: int = 30,
        attendees: Optional[List[str]] = None,
        within_days: int = 7
    ) -> Tuple[bool, str, Optional[CalendarEvent]]:
        """
        Create a meeting at the next available slot

        Args:
            title: Meeting title
            duration_minutes: Meeting duration
            attendees: List of attendee emails
            within_days: How far to look for availability

        Returns:
            Tuple of (success, message, created_event)
        """
        slot = self.suggest_meeting_time(duration_minutes=duration_minutes, within_days=within_days)

        if not slot:
            return False, "No available time slots found", None

        return self.create_event(
            title=title,
            start=slot.start,
            end=slot.start + timedelta(minutes=duration_minutes),
            attendees=attendees
        )

    # -------------------------------------------------------------------------
    # Event Modification
    # -------------------------------------------------------------------------

    def update_event(
        self,
        event_id: str,
        title: Optional[str] = None,
        start: Optional[datetime] = None,
        end: Optional[datetime] = None,
        description: Optional[str] = None,
        require_approval: bool = True
    ) -> Tuple[bool, str]:
        """
        Update an existing event

        Args:
            event_id: Event ID to update
            title: New title
            start: New start time
            end: New end time
            description: New description
            require_approval: Whether to require approval

        Returns:
            Tuple of (success, message)
        """
        if require_approval and self.approval_manager:
            from approval_manager import ActionType
            approved, reason = self.approval_manager.request_approval(
                action_type=ActionType.CALENDAR_MODIFY,
                description=f"Update event: {event_id}",
                details={'event_id': event_id, 'changes': {
                    'title': title, 'start': str(start), 'end': str(end)
                }}
            )
            if not approved:
                return False, f"Event not updated: {reason}"

        service = self._get_calendar_service()
        if not service:
            return False, "Calendar service not available"

        try:
            # Get existing event
            event = service.events().get(calendarId='primary', eventId=event_id).execute()

            # Apply updates
            if title:
                event['summary'] = title
            if description:
                event['description'] = description
            if start:
                event['start'] = {'dateTime': start.isoformat(), 'timeZone': self.timezone}
            if end:
                event['end'] = {'dateTime': end.isoformat(), 'timeZone': self.timezone}

            service.events().update(
                calendarId='primary',
                eventId=event_id,
                body=event
            ).execute()

            logger.info(f"Event updated: {event_id}")
            return True, "Event updated successfully"

        except Exception as e:
            logger.error(f"Failed to update event: {e}")
            return False, f"Failed to update: {str(e)}"

    def delete_event(self, event_id: str, require_approval: bool = True) -> Tuple[bool, str]:
        """
        Delete a calendar event

        Args:
            event_id: Event ID to delete
            require_approval: Whether to require approval

        Returns:
            Tuple of (success, message)
        """
        if require_approval and self.approval_manager:
            from approval_manager import ActionType
            approved, reason = self.approval_manager.request_approval(
                action_type=ActionType.CALENDAR_DELETE,
                description=f"Delete event: {event_id}",
                details={'event_id': event_id}
            )
            if not approved:
                return False, f"Event not deleted: {reason}"

        service = self._get_calendar_service()
        if not service:
            return False, "Calendar service not available"

        try:
            service.events().delete(calendarId='primary', eventId=event_id).execute()
            logger.info(f"Event deleted: {event_id}")
            return True, "Event deleted successfully"

        except Exception as e:
            logger.error(f"Failed to delete event: {e}")
            return False, f"Failed to delete: {str(e)}"

    # -------------------------------------------------------------------------
    # Summary & Reporting
    # -------------------------------------------------------------------------

    def get_schedule_summary(self, days: int = 7) -> Dict[str, Any]:
        """
        Get a summary of the schedule

        Args:
            days: Number of days to summarize

        Returns:
            Schedule summary dictionary
        """
        events = self.get_events(
            start_date=datetime.now(),
            end_date=datetime.now() + timedelta(days=days)
        )

        summary = {
            'period_days': days,
            'total_events': len(events),
            'by_day': {},
            'by_type': {},
            'busy_hours': 0,
            'free_slots': []
        }

        for event in events:
            # Group by day
            day_key = event.start.strftime('%Y-%m-%d')
            if day_key not in summary['by_day']:
                summary['by_day'][day_key] = []
            summary['by_day'][day_key].append({
                'title': event.title,
                'time': event.start.strftime('%H:%M'),
                'duration': int((event.end - event.start).total_seconds() / 60)
            })

            # Group by type
            type_key = event.event_type.value
            summary['by_type'][type_key] = summary['by_type'].get(type_key, 0) + 1

            # Calculate busy hours
            if not event.is_all_day:
                summary['busy_hours'] += (event.end - event.start).total_seconds() / 3600

        # Find free slots
        free_slots = self.find_free_slots(within_days=days)
        summary['free_slots'] = [
            {
                'start': s.start.isoformat(),
                'end': s.end.isoformat(),
                'duration_minutes': s.duration_minutes
            }
            for s in free_slots[:10]  # Top 10 slots
        ]

        return summary

    def get_next_event(self) -> Optional[CalendarEvent]:
        """Get the next upcoming event"""
        events = self.get_upcoming_events(hours=24)
        return events[0] if events else None

    # -------------------------------------------------------------------------
    # Health & Status
    # -------------------------------------------------------------------------

    def health_check(self) -> Dict[str, Any]:
        """Check calendar agent health"""
        service = self._get_calendar_service()

        status = {
            'calendar_connected': service is not None,
            'working_hours': f"{self.working_hours_start}:00 - {self.working_hours_end}:00",
            'timezone': self.timezone
        }

        if service:
            try:
                calendar = service.calendars().get(calendarId='primary').execute()
                status['calendar_name'] = calendar.get('summary')
                status['calendar_id'] = calendar.get('id')
            except Exception as e:
                status['calendar_error'] = str(e)

        return status
