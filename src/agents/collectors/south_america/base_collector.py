"""
Base Collector Re-exports

Re-exports the base collector classes for use by South America collectors.
"""

from src.agents.base.base_collector import (
    BaseCollector,
    CollectorConfig,
    CollectorResult,
    DataFrequency,
    AuthType
)

__all__ = [
    'BaseCollector',
    'CollectorConfig',
    'CollectorResult',
    'DataFrequency',
    'AuthType',
]
