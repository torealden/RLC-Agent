"""
Event Bus for RLC Agent System.

Provides pub/sub messaging for inter-agent communication.
Enables reactive patterns where agents respond to system events.
"""

import asyncio
import logging
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Awaitable
import uuid

logger = logging.getLogger(__name__)


class EventType(Enum):
    """Standard event types in the system"""
    # Data events
    DATA_COLLECTED = "data.collected"
    DATA_STORED = "data.stored"
    DATA_VALIDATED = "data.validated"
    DATA_QUALITY_ISSUE = "data.quality_issue"

    # Agent events
    AGENT_STARTED = "agent.started"
    AGENT_COMPLETED = "agent.completed"
    AGENT_ERROR = "agent.error"

    # Scheduler events
    TASK_SCHEDULED = "scheduler.task_scheduled"
    TASK_STARTED = "scheduler.task_started"
    TASK_COMPLETED = "scheduler.task_completed"
    TASK_FAILED = "scheduler.task_failed"

    # Analysis events
    ANALYSIS_STARTED = "analysis.started"
    ANALYSIS_COMPLETED = "analysis.completed"
    INSIGHT_GENERATED = "analysis.insight"
    ANOMALY_DETECTED = "analysis.anomaly"

    # Report events
    REPORT_REQUESTED = "report.requested"
    REPORT_GENERATED = "report.generated"

    # System events
    SYSTEM_STARTUP = "system.startup"
    SYSTEM_SHUTDOWN = "system.shutdown"
    SYSTEM_ERROR = "system.error"
    HEALTH_CHECK = "system.health_check"

    # Custom events
    CUSTOM = "custom"


@dataclass
class Event:
    """An event in the system"""
    event_type: EventType
    source: str  # Agent/component that generated the event
    data: Dict[str, Any] = field(default_factory=dict)
    event_id: str = field(default_factory=lambda: str(uuid.uuid4())[:12])
    timestamp: datetime = field(default_factory=datetime.now)
    correlation_id: Optional[str] = None  # For linking related events

    def to_dict(self) -> Dict[str, Any]:
        return {
            'event_id': self.event_id,
            'type': self.event_type.value,
            'source': self.source,
            'data': self.data,
            'timestamp': self.timestamp.isoformat(),
            'correlation_id': self.correlation_id
        }

    @classmethod
    def create(cls,
               event_type: EventType,
               source: str,
               data: Dict[str, Any] = None,
               correlation_id: str = None) -> 'Event':
        """Factory method for creating events"""
        return cls(
            event_type=event_type,
            source=source,
            data=data or {},
            correlation_id=correlation_id
        )


# Type alias for event handlers
EventHandler = Callable[[Event], Awaitable[None]]


class EventBus:
    """
    Central event bus for the RLC Agent System.

    Provides:
    - Publish/subscribe pattern
    - Event filtering by type
    - Async event handling
    - Event history tracking
    """

    def __init__(self, max_history: int = 1000):
        """
        Initialize the event bus.

        Args:
            max_history: Maximum events to keep in history
        """
        self._subscribers: Dict[EventType, List[EventHandler]] = {}
        self._global_subscribers: List[EventHandler] = []
        self._history: List[Event] = []
        self._max_history = max_history
        self._running = False

        logger.info("EventBus initialized")

    def subscribe(self,
                  event_type: EventType,
                  handler: EventHandler):
        """
        Subscribe to a specific event type.

        Args:
            event_type: Type of event to subscribe to
            handler: Async function to call when event occurs
        """
        if event_type not in self._subscribers:
            self._subscribers[event_type] = []

        self._subscribers[event_type].append(handler)
        logger.debug(f"Subscribed to {event_type.value}")

    def subscribe_all(self, handler: EventHandler):
        """
        Subscribe to all events.

        Args:
            handler: Async function to call for any event
        """
        self._global_subscribers.append(handler)
        logger.debug("Subscribed to all events")

    def unsubscribe(self,
                    event_type: EventType,
                    handler: EventHandler):
        """
        Unsubscribe from an event type.

        Args:
            event_type: Type of event
            handler: Handler to remove
        """
        if event_type in self._subscribers:
            try:
                self._subscribers[event_type].remove(handler)
            except ValueError:
                pass

    async def publish(self, event: Event):
        """
        Publish an event to all subscribers.

        Args:
            event: Event to publish
        """
        logger.debug(f"Publishing event: {event.event_type.value} from {event.source}")

        # Record in history
        self._history.append(event)
        if len(self._history) > self._max_history:
            self._history = self._history[-self._max_history:]

        # Get handlers
        handlers = list(self._global_subscribers)
        if event.event_type in self._subscribers:
            handlers.extend(self._subscribers[event.event_type])

        # Execute handlers concurrently
        if handlers:
            results = await asyncio.gather(
                *[self._safe_handle(handler, event) for handler in handlers],
                return_exceptions=True
            )

            # Log any errors
            for result in results:
                if isinstance(result, Exception):
                    logger.error(f"Event handler error: {result}")

    async def _safe_handle(self, handler: EventHandler, event: Event):
        """Safely execute a handler with error catching"""
        try:
            await handler(event)
        except Exception as e:
            logger.error(f"Handler {handler.__name__} failed: {e}")
            raise

    async def emit(self,
                   event_type: EventType,
                   source: str,
                   data: Dict[str, Any] = None,
                   correlation_id: str = None):
        """
        Convenience method to create and publish an event.

        Args:
            event_type: Type of event
            source: Event source
            data: Event data
            correlation_id: Correlation ID for linking events
        """
        event = Event.create(
            event_type=event_type,
            source=source,
            data=data,
            correlation_id=correlation_id
        )
        await self.publish(event)

    def get_history(self,
                    event_type: EventType = None,
                    source: str = None,
                    limit: int = 100) -> List[Event]:
        """
        Get event history with optional filtering.

        Args:
            event_type: Filter by event type
            source: Filter by source
            limit: Maximum events to return

        Returns:
            List of matching events
        """
        events = self._history

        if event_type:
            events = [e for e in events if e.event_type == event_type]

        if source:
            events = [e for e in events if e.source == source]

        return events[-limit:]

    def get_recent_by_correlation(self, correlation_id: str) -> List[Event]:
        """Get all events with a specific correlation ID"""
        return [e for e in self._history if e.correlation_id == correlation_id]

    def clear_history(self):
        """Clear event history"""
        self._history = []

    @property
    def subscriber_count(self) -> int:
        """Total number of subscriptions"""
        count = len(self._global_subscribers)
        for handlers in self._subscribers.values():
            count += len(handlers)
        return count

    def get_stats(self) -> Dict[str, Any]:
        """Get event bus statistics"""
        event_counts = {}
        for event in self._history:
            type_name = event.event_type.value
            event_counts[type_name] = event_counts.get(type_name, 0) + 1

        return {
            'total_events': len(self._history),
            'subscriber_count': self.subscriber_count,
            'event_types_seen': len(event_counts),
            'event_counts': event_counts
        }


# Global event bus instance
_event_bus: Optional[EventBus] = None


def get_event_bus() -> EventBus:
    """Get the global event bus instance"""
    global _event_bus
    if _event_bus is None:
        _event_bus = EventBus()
    return _event_bus


# Convenience functions for common event patterns
async def emit_data_collected(source: str, record_count: int, source_report: str = None):
    """Emit a data collected event"""
    await get_event_bus().emit(
        EventType.DATA_COLLECTED,
        source=source,
        data={
            'record_count': record_count,
            'source_report': source_report,
            'timestamp': datetime.now().isoformat()
        }
    )


async def emit_agent_error(agent_name: str, error: str, context: Dict = None):
    """Emit an agent error event"""
    await get_event_bus().emit(
        EventType.AGENT_ERROR,
        source=agent_name,
        data={
            'error': error,
            'context': context or {}
        }
    )


async def emit_analysis_insight(source: str, insight_type: str, data: Dict):
    """Emit an analysis insight event"""
    await get_event_bus().emit(
        EventType.INSIGHT_GENERATED,
        source=source,
        data={
            'insight_type': insight_type,
            **data
        }
    )
