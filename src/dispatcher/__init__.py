"""
RLC-Agent Dispatcher Package

The CNS Dispatcher Daemon — makes everything run on time, automatically.

Components:
- CollectorRegistry: Maps collector names to classes (COLLECTOR_MAP)
- CollectorRunner: Executes collectors with status logging and retry
- Dispatcher: APScheduler daemon that fires collectors on schedule
"""

from src.dispatcher.collector_registry import CollectorRegistry
from src.dispatcher.collector_runner import CollectorRunner
from src.dispatcher.dispatcher import Dispatcher

__all__ = ['CollectorRegistry', 'CollectorRunner', 'Dispatcher']
