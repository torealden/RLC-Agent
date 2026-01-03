"""
Base classes for RLC agents and collectors.

Classes:
- BaseCollector: Abstract base for all data collectors
- BaseTradeAgent: Base for trade data agents
- BaseLineupAgent: Base for port lineup agents
"""

from .base_collector import (
    BaseCollector,
    CollectorConfig,
    CollectorResult,
    DataFrequency,
    AuthType,
)

from .base_trade_agent import (
    BaseTradeAgent,
    FetchResult,
    LoadResult,
)

from .base_lineup_agent import (
    BaseLineupAgent,
)

__all__ = [
    # Collector base
    "BaseCollector",
    "CollectorConfig",
    "CollectorResult",
    "DataFrequency",
    "AuthType",
    # Trade agent base
    "BaseTradeAgent",
    "FetchResult",
    "LoadResult",
    # Lineup agent base
    "BaseLineupAgent",
]
