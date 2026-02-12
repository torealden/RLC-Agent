"""
Base Trade Agent Re-exports

Re-exports the base trade agent classes for use by South America collectors.
"""

from src.agents.base.base_trade_agent import (
    BaseTradeAgent,
    FetchResult,
    LoadResult
)

__all__ = [
    'BaseTradeAgent',
    'FetchResult',
    'LoadResult',
]
