"""
Base Collector re-export for South America collectors.

This module re-exports the base collector classes from src.agents.base.
"""

import sys
from pathlib import Path

# Ensure project root is in path
PROJECT_ROOT = Path(__file__).parent.parent.parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# Import and re-export from the main base_collector
try:
    from src.agents.base.base_collector import (
        BaseCollector,
        CollectorConfig,
        CollectorResult,
        DataFrequency,
        AuthType,
    )
except ImportError:
    # Fallback: define minimal classes if import fails
    from dataclasses import dataclass, field
    from datetime import datetime
    from typing import Optional, Any, List
    from enum import Enum
    import logging

    class DataFrequency(Enum):
        REALTIME = "realtime"
        DAILY = "daily"
        WEEKLY = "weekly"
        MONTHLY = "monthly"
        QUARTERLY = "quarterly"
        ANNUAL = "annual"

    class AuthType(Enum):
        NONE = "none"
        API_KEY = "api_key"
        OAUTH = "oauth"
        PAID = "paid"

    @dataclass
    class CollectorConfig:
        source_name: str = ""
        source_url: str = ""
        auth_type: AuthType = AuthType.NONE
        api_key: Optional[str] = None
        timeout: int = 30
        retry_attempts: int = 3
        rate_limit_per_minute: int = 60
        frequency: DataFrequency = DataFrequency.DAILY
        commodities: List[str] = field(default_factory=list)

    @dataclass
    class CollectorResult:
        success: bool
        source: str
        collected_at: datetime = field(default_factory=datetime.now)
        records_fetched: int = 0
        data: Optional[Any] = None
        response_time_ms: int = 0
        error_message: Optional[str] = None
        warnings: List[str] = field(default_factory=list)
        period_start: Optional[str] = None
        period_end: Optional[str] = None
        data_as_of: Optional[str] = None

    class BaseCollector:
        """Minimal base collector"""
        def __init__(self, config: CollectorConfig = None):
            import requests
            self.config = config or CollectorConfig()
            self.logger = logging.getLogger(self.__class__.__name__)
            self.session = requests.Session()

        def _make_request(self, url, **kwargs):
            try:
                response = self.session.get(url, timeout=self.config.timeout, **kwargs)
                return response, None
            except Exception as e:
                return None, str(e)

        def collect(self, **kwargs):
            return self.fetch_data(**kwargs)

        def fetch_data(self, **kwargs):
            raise NotImplementedError

        def test_connection(self):
            try:
                response, error = self._make_request(self.config.source_url)
                if error:
                    return False, error
                return response.status_code == 200, f"HTTP {response.status_code}"
            except Exception as e:
                return False, str(e)


__all__ = [
    'BaseCollector',
    'CollectorConfig',
    'CollectorResult',
    'DataFrequency',
    'AuthType',
]
