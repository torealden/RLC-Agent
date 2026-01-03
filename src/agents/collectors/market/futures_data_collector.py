"""
Unified Futures Data Collector with Fallback

Attempts IBKR first, falls back to TradeStation if unavailable.
Provides a single interface for historical futures data.

Usage:
    from futures_data_collector import FuturesDataCollector

    collector = FuturesDataCollector()
    result = collector.collect()  # Uses best available source
"""

import os
import logging
from datetime import datetime, date, timedelta
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any
from pathlib import Path

try:
    import pandas as pd
    PANDAS_AVAILABLE = True
except ImportError:
    PANDAS_AVAILABLE = False

from .base_collector import (
    BaseCollector,
    CollectorConfig,
    CollectorResult,
    DataFrequency,
    AuthType
)
from .ibkr_collector import IBKRCollector, IBKRConfig
from .tradestation_collector import TradeStationCollector, TradeStationConfig


@dataclass
class FuturesDataConfig(CollectorConfig):
    """Configuration for unified futures data collector"""
    source_name: str = "Futures Data"
    source_url: str = ""
    auth_type: AuthType = AuthType.API_KEY

    # Source preference
    primary_source: str = "ibkr"  # 'ibkr' or 'tradestation'
    enable_fallback: bool = True

    # Data settings
    frequency: DataFrequency = DataFrequency.DAILY
    lookback_days: int = 365

    commodities: List[str] = field(default_factory=lambda: [
        'corn', 'wheat_srw', 'wheat_hrw', 'soybeans', 'soybean_meal', 'soybean_oil',
        'crude_oil', 'natural_gas', 'ethanol'
    ])


class FuturesDataCollector(BaseCollector):
    """
    Unified futures data collector with automatic failover.

    Tries IBKR first (if configured), falls back to TradeStation.
    """

    def __init__(self, config: FuturesDataConfig = None):
        if config is None:
            config = FuturesDataConfig()
        super().__init__(config)

        self.config: FuturesDataConfig = config
        self.ibkr_collector = None
        self.tradestation_collector = None
        self.active_source = None

        # Initialize available collectors
        self._init_collectors()

    def _init_collectors(self):
        """Initialize available collectors based on configuration"""
        # Check IBKR availability
        ibkr_account = os.environ.get('IBKR_ACCOUNT_ID')
        if ibkr_account:
            try:
                self.ibkr_collector = IBKRCollector(IBKRConfig())
                self.logger.info("IBKR collector initialized")
            except Exception as e:
                self.logger.warning(f"Could not initialize IBKR: {e}")

        # Check TradeStation availability
        ts_client = os.environ.get('TRADESTATION_CLIENT_ID')
        ts_secret = os.environ.get('TRADESTATION_CLIENT_SECRET')
        ts_refresh = os.environ.get('TRADESTATION_REFRESH_TOKEN')

        if ts_client and ts_secret and ts_refresh:
            try:
                self.tradestation_collector = TradeStationCollector(TradeStationConfig())
                self.logger.info("TradeStation collector initialized")
            except Exception as e:
                self.logger.warning(f"Could not initialize TradeStation: {e}")

    def get_table_name(self) -> str:
        return "futures_prices"

    def _check_ibkr_available(self) -> bool:
        """Check if IBKR is available and authenticated"""
        if not self.ibkr_collector:
            return False

        success, _ = self.ibkr_collector.test_connection()
        return success

    def _check_tradestation_available(self) -> bool:
        """Check if TradeStation is available"""
        if not self.tradestation_collector:
            return False

        success, _ = self.tradestation_collector.test_connection()
        return success

    def _select_source(self) -> Optional[str]:
        """Select best available data source"""
        primary = self.config.primary_source

        # Try primary first
        if primary == 'ibkr' and self._check_ibkr_available():
            self.logger.info("Using IBKR as data source")
            return 'ibkr'
        elif primary == 'tradestation' and self._check_tradestation_available():
            self.logger.info("Using TradeStation as data source")
            return 'tradestation'

        # Fallback if enabled
        if self.config.enable_fallback:
            if primary == 'ibkr' and self._check_tradestation_available():
                self.logger.info("IBKR unavailable, falling back to TradeStation")
                return 'tradestation'
            elif primary == 'tradestation' and self._check_ibkr_available():
                self.logger.info("TradeStation unavailable, falling back to IBKR")
                return 'ibkr'

        return None

    def fetch_data(
        self,
        start_date: date = None,
        end_date: date = None,
        **kwargs
    ) -> CollectorResult:
        """
        Fetch historical futures data from best available source.
        """
        commodities = kwargs.get('commodities', self.config.commodities)

        # Select data source
        source = self._select_source()

        if not source:
            return CollectorResult(
                success=False,
                source=self.config.source_name,
                error_message="No futures data source available",
                warnings=[
                    "Neither IBKR nor TradeStation is configured/available",
                    "",
                    "For IBKR:",
                    "  1. Set IBKR_ACCOUNT_ID in .env",
                    "  2. Run Client Portal Gateway",
                    "  3. Log in at https://localhost:5000",
                    "",
                    "For TradeStation:",
                    "  1. Set TRADESTATION_CLIENT_ID",
                    "  2. Set TRADESTATION_CLIENT_SECRET",
                    "  3. Set TRADESTATION_REFRESH_TOKEN",
                ]
            )

        self.active_source = source

        # Fetch from selected source
        if source == 'ibkr':
            result = self.ibkr_collector.fetch_data(
                start_date=start_date,
                end_date=end_date,
                commodities=commodities
            )
        else:
            result = self.tradestation_collector.fetch_data(
                start_date=start_date,
                end_date=end_date,
                commodities=commodities
            )

        # Add source info to result
        if result.data is not None and PANDAS_AVAILABLE and isinstance(result.data, pd.DataFrame):
            result.data['data_source'] = source.upper()

        return result

    def parse_response(self, response_data: Any) -> Any:
        """Parse response data"""
        return response_data

    def test_connection(self) -> tuple:
        """Test connection to available sources"""
        sources_status = []

        # Test IBKR
        if self.ibkr_collector:
            ibkr_ok, ibkr_msg = self.ibkr_collector.test_connection()
            sources_status.append(f"IBKR: {'OK' if ibkr_ok else ibkr_msg}")
        else:
            sources_status.append("IBKR: Not configured")

        # Test TradeStation
        if self.tradestation_collector:
            ts_ok, ts_msg = self.tradestation_collector.test_connection()
            sources_status.append(f"TradeStation: {'OK' if ts_ok else ts_msg}")
        else:
            sources_status.append("TradeStation: Not configured")

        # Determine overall status
        any_available = (
            (self.ibkr_collector and self._check_ibkr_available()) or
            (self.tradestation_collector and self._check_tradestation_available())
        )

        return any_available, " | ".join(sources_status)

    def get_status(self) -> Dict[str, Any]:
        """Get detailed status of all sources"""
        status = {
            'primary_source': self.config.primary_source,
            'fallback_enabled': self.config.enable_fallback,
            'active_source': self.active_source,
            'sources': {}
        }

        # IBKR status
        if self.ibkr_collector:
            ibkr_ok, ibkr_msg = self.ibkr_collector.test_connection()
            status['sources']['ibkr'] = {
                'configured': True,
                'available': ibkr_ok,
                'message': ibkr_msg,
            }
        else:
            status['sources']['ibkr'] = {
                'configured': False,
                'available': False,
                'message': 'Not configured - set IBKR_ACCOUNT_ID',
            }

        # TradeStation status
        if self.tradestation_collector:
            ts_ok, ts_msg = self.tradestation_collector.test_connection()
            status['sources']['tradestation'] = {
                'configured': True,
                'available': ts_ok,
                'message': ts_msg,
            }
        else:
            status['sources']['tradestation'] = {
                'configured': False,
                'available': False,
                'message': 'Not configured - set TRADESTATION_* variables',
            }

        return status


# Convenience function for quick data fetch
def fetch_futures_data(
    commodities: List[str] = None,
    lookback_days: int = 365,
    primary_source: str = 'ibkr'
) -> CollectorResult:
    """
    Quick function to fetch futures data with automatic source selection.

    Args:
        commodities: List of commodities to fetch (default: major ags + energy)
        lookback_days: Number of days of history
        primary_source: Preferred source ('ibkr' or 'tradestation')

    Returns:
        CollectorResult with historical price data
    """
    config = FuturesDataConfig(
        primary_source=primary_source,
        lookback_days=lookback_days,
    )

    if commodities:
        config.commodities = commodities

    collector = FuturesDataCollector(config)

    end_date = date.today()
    start_date = end_date - timedelta(days=lookback_days)

    return collector.collect(start_date=start_date, end_date=end_date)


if __name__ == "__main__":
    # Test the unified collector
    print("Futures Data Collector Status")
    print("=" * 50)

    collector = FuturesDataCollector()
    status = collector.get_status()

    print(f"Primary Source: {status['primary_source']}")
    print(f"Fallback Enabled: {status['fallback_enabled']}")
    print()

    for source, info in status['sources'].items():
        print(f"{source.upper()}:")
        print(f"  Configured: {info['configured']}")
        print(f"  Available: {info['available']}")
        print(f"  Message: {info['message']}")
        print()
