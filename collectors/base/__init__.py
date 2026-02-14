"""
Base classes for RLC data collectors and verification agents.

All collectors inherit from BaseCollector which provides:
- Structured JSON-lines logging
- HTTP requests with retry/backoff
- Rate limiting
- Raw response archiving
- Multi-sheet Excel output

All checkers inherit from BaseChecker which provides:
- Collector log reading and parsing
- Verification target extraction
- Severity classification
- Sample/full verification modes
"""

from .base_collector import BaseCollector, CollectorConfig, CollectorResult
from .base_checker import BaseChecker, CheckerConfig, CheckerResult
from .logging_utils import setup_collector_logger, LogAction

__all__ = [
    'BaseCollector', 'CollectorConfig', 'CollectorResult',
    'BaseChecker', 'CheckerConfig', 'CheckerResult',
    'setup_collector_logger', 'LogAction',
]
