"""
Interactive Brokers (IBKR) Historical Data Collector

Fetches historical futures prices via the IBKR Client Portal API or TWS API.

Options:
1. Client Portal API (Web) - REST API, requires authentication
2. TWS API - Requires TWS/IB Gateway running locally

For Client Portal API setup:
1. Log into Client Portal: https://www.interactivebrokers.com/portal
2. Enable API access in Account Management > Settings > API
3. Download and run the Client Portal Gateway

For TWS API setup:
1. Install TWS or IB Gateway
2. Enable API in TWS: Edit > Global Configuration > API > Settings
3. Allow connections from localhost

Environment Variables:
    IBKR_ACCOUNT_ID - Your IBKR account ID
    IBKR_GATEWAY_HOST - Gateway host (default: localhost)
    IBKR_GATEWAY_PORT - Gateway port (default: 5000 for Client Portal, 7497 for TWS)
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


# IBKR Contract IDs for common commodity futures
IBKR_FUTURES_CONTRACTS = {
    # Grains - CBOT
    'corn': {'symbol': 'ZC', 'exchange': 'CBOT', 'currency': 'USD', 'multiplier': 50},
    'wheat_srw': {'symbol': 'ZW', 'exchange': 'CBOT', 'currency': 'USD', 'multiplier': 50},
    'wheat_hrw': {'symbol': 'KE', 'exchange': 'CBOT', 'currency': 'USD', 'multiplier': 50},
    'soybeans': {'symbol': 'ZS', 'exchange': 'CBOT', 'currency': 'USD', 'multiplier': 50},
    'soybean_meal': {'symbol': 'ZM', 'exchange': 'CBOT', 'currency': 'USD', 'multiplier': 100},
    'soybean_oil': {'symbol': 'ZL', 'exchange': 'CBOT', 'currency': 'USD', 'multiplier': 600},
    'oats': {'symbol': 'ZO', 'exchange': 'CBOT', 'currency': 'USD', 'multiplier': 50},

    # Energy - NYMEX
    'crude_oil': {'symbol': 'CL', 'exchange': 'NYMEX', 'currency': 'USD', 'multiplier': 1000},
    'natural_gas': {'symbol': 'NG', 'exchange': 'NYMEX', 'currency': 'USD', 'multiplier': 10000},
    'gasoline': {'symbol': 'RB', 'exchange': 'NYMEX', 'currency': 'USD', 'multiplier': 42000},
    'heating_oil': {'symbol': 'HO', 'exchange': 'NYMEX', 'currency': 'USD', 'multiplier': 42000},

    # Softs - ICE
    'cotton': {'symbol': 'CT', 'exchange': 'NYBOT', 'currency': 'USD', 'multiplier': 500},
    'sugar': {'symbol': 'SB', 'exchange': 'NYBOT', 'currency': 'USD', 'multiplier': 1120},
    'coffee': {'symbol': 'KC', 'exchange': 'NYBOT', 'currency': 'USD', 'multiplier': 375},
    'cocoa': {'symbol': 'CC', 'exchange': 'NYBOT', 'currency': 'USD', 'multiplier': 10},

    # Livestock - CME
    'live_cattle': {'symbol': 'LE', 'exchange': 'CME', 'currency': 'USD', 'multiplier': 400},
    'lean_hogs': {'symbol': 'HE', 'exchange': 'CME', 'currency': 'USD', 'multiplier': 400},
    'feeder_cattle': {'symbol': 'GF', 'exchange': 'CME', 'currency': 'USD', 'multiplier': 500},

    # Ethanol
    'ethanol': {'symbol': 'EH', 'exchange': 'CBOT', 'currency': 'USD', 'multiplier': 29000},
}


# Contract month codes
MONTH_CODES = {
    1: 'F', 2: 'G', 3: 'H', 4: 'J', 5: 'K', 6: 'M',
    7: 'N', 8: 'Q', 9: 'U', 10: 'V', 11: 'X', 12: 'Z'
}


@dataclass
class IBKRConfig(CollectorConfig):
    """Configuration for IBKR collector"""
    source_name: str = "IBKR"
    source_url: str = "https://localhost:5000"  # Client Portal Gateway
    auth_type: AuthType = AuthType.API_KEY

    # IBKR specific settings
    account_id: str = field(default_factory=lambda: os.environ.get('IBKR_ACCOUNT_ID', ''))
    gateway_host: str = field(default_factory=lambda: os.environ.get('IBKR_GATEWAY_HOST', 'localhost'))
    gateway_port: int = field(default_factory=lambda: int(os.environ.get('IBKR_GATEWAY_PORT', '5000')))
    use_tws: bool = False  # If True, use TWS API instead of Client Portal

    # Data settings
    frequency: DataFrequency = DataFrequency.DAILY
    bar_size: str = "1 day"  # Options: 1 min, 5 mins, 15 mins, 1 hour, 1 day
    what_to_show: str = "TRADES"  # TRADES, MIDPOINT, BID, ASK

    commodities: List[str] = field(default_factory=lambda: [
        'corn', 'wheat_srw', 'wheat_hrw', 'soybeans', 'soybean_meal', 'soybean_oil',
        'crude_oil', 'natural_gas', 'ethanol'
    ])


class IBKRCollector(BaseCollector):
    """
    Collector for Interactive Brokers historical futures data.

    Uses the Client Portal API (REST) by default.
    Can also use TWS API if configured.
    """

    def __init__(self, config: IBKRConfig = None):
        if config is None:
            config = IBKRConfig()
        super().__init__(config)

        self.config: IBKRConfig = config
        self.base_url = f"https://{config.gateway_host}:{config.gateway_port}/v1/api"
        self.authenticated = False

    def get_table_name(self) -> str:
        return "ibkr_futures_prices"

    def _get_front_month_contract(self, commodity: str, as_of_date: date = None) -> str:
        """Get the front month contract symbol"""
        if as_of_date is None:
            as_of_date = date.today()

        contract_info = IBKR_FUTURES_CONTRACTS.get(commodity)
        if not contract_info:
            return None

        # Simple front month logic - actual implementation would use roll dates
        # Most ag contracts roll around the 15th of the month before expiry
        current_month = as_of_date.month
        current_year = as_of_date.year

        # Move to next month if past the 15th
        if as_of_date.day > 15:
            current_month += 1
            if current_month > 12:
                current_month = 1
                current_year += 1

        month_code = MONTH_CODES[current_month]
        year_code = str(current_year)[-1]  # Last digit of year

        return f"{contract_info['symbol']}{month_code}{year_code}"

    def _check_auth(self) -> bool:
        """Check if authenticated with Client Portal"""
        try:
            response, error = self._make_request(
                f"{self.base_url}/iserver/auth/status",
                timeout=5
            )
            if error:
                return False

            if response and response.status_code == 200:
                data = response.json()
                return data.get('authenticated', False)

            return False
        except Exception:
            return False

    def _get_contract_id(self, symbol: str, exchange: str) -> Optional[int]:
        """Look up IBKR contract ID for a symbol"""
        try:
            params = {
                'symbols': symbol,
                'secType': 'FUT'
            }
            response, error = self._make_request(
                f"{self.base_url}/iserver/secdef/search",
                params=params
            )

            if error or not response:
                return None

            data = response.json()
            if data and len(data) > 0:
                # Find matching exchange
                for contract in data:
                    if contract.get('exchange') == exchange:
                        return contract.get('conid')
                # Return first match if no exchange match
                return data[0].get('conid')

            return None
        except Exception as e:
            self.logger.error(f"Error looking up contract: {e}")
            return None

    def _fetch_historical_data(
        self,
        conid: int,
        period: str = "1y",
        bar_size: str = "1d"
    ) -> Optional[List[Dict]]:
        """Fetch historical bars for a contract"""
        try:
            params = {
                'conid': conid,
                'period': period,
                'bar': bar_size,
                'outsideRth': False
            }

            response, error = self._make_request(
                f"{self.base_url}/iserver/marketdata/history",
                params=params
            )

            if error:
                self.logger.warning(f"Error fetching history: {error}")
                return None

            if response and response.status_code == 200:
                data = response.json()
                return data.get('data', [])

            return None
        except Exception as e:
            self.logger.error(f"Error fetching historical data: {e}")
            return None

    def fetch_data(
        self,
        start_date: date = None,
        end_date: date = None,
        **kwargs
    ) -> CollectorResult:
        """
        Fetch historical futures data from IBKR.

        Note: Requires Client Portal Gateway running and authenticated.
        """
        commodities = kwargs.get('commodities', self.config.commodities)
        all_records = []
        warnings = []

        # Check authentication
        if not self._check_auth():
            self.logger.warning(
                "Not authenticated with IBKR Client Portal. "
                "Please log in at https://localhost:5000 and try again."
            )
            return CollectorResult(
                success=False,
                source=self.config.source_name,
                error_message="Not authenticated. Start Client Portal Gateway and log in.",
                warnings=[
                    "IBKR requires Client Portal Gateway running",
                    "1. Download gateway from IBKR website",
                    "2. Run: bin/run.sh (or run.bat on Windows)",
                    "3. Log in at https://localhost:5000",
                    "4. Run this collector again"
                ]
            )

        for commodity in commodities:
            contract_info = IBKR_FUTURES_CONTRACTS.get(commodity)
            if not contract_info:
                warnings.append(f"Unknown commodity: {commodity}")
                continue

            self.logger.info(f"Fetching {commodity} ({contract_info['symbol']})")

            # Get front month contract
            front_month = self._get_front_month_contract(commodity, end_date)

            # Look up contract ID
            conid = self._get_contract_id(
                contract_info['symbol'],
                contract_info['exchange']
            )

            if not conid:
                warnings.append(f"{commodity}: Could not find contract ID")
                continue

            # Fetch historical data
            bars = self._fetch_historical_data(conid, period="1y", bar_size="1d")

            if not bars:
                warnings.append(f"{commodity}: No historical data returned")
                continue

            # Parse bars into records
            for bar in bars:
                record = {
                    'commodity': commodity,
                    'symbol': contract_info['symbol'],
                    'exchange': contract_info['exchange'],
                    'date': datetime.fromtimestamp(bar['t'] / 1000).strftime('%Y-%m-%d'),
                    'open': bar.get('o'),
                    'high': bar.get('h'),
                    'low': bar.get('l'),
                    'close': bar.get('c'),
                    'volume': bar.get('v'),
                    'contract': front_month,
                    'source': 'IBKR',
                }
                all_records.append(record)

            self.logger.info(f"  Retrieved {len(bars)} bars for {commodity}")

        if PANDAS_AVAILABLE and all_records:
            df = pd.DataFrame(all_records)
            df['date'] = pd.to_datetime(df['date'])
            df = df.sort_values(['commodity', 'date'])
            data = df
        else:
            data = all_records

        return CollectorResult(
            success=len(all_records) > 0,
            source=self.config.source_name,
            records_fetched=len(all_records),
            data=data,
            warnings=warnings,
            period_start=str(start_date) if start_date else None,
            period_end=str(end_date) if end_date else None,
        )

    def parse_response(self, response_data: Any) -> Any:
        """Parse IBKR API response"""
        return response_data

    def test_connection(self) -> tuple:
        """Test connection to IBKR gateway"""
        try:
            # Try to reach the gateway
            response, error = self._make_request(
                f"{self.base_url}/iserver/auth/status",
                timeout=5
            )

            if error:
                if "Connection refused" in str(error):
                    return False, "Client Portal Gateway not running"
                return False, error

            if response and response.status_code == 200:
                data = response.json()
                if data.get('authenticated'):
                    return True, "Connected and authenticated"
                return False, "Gateway running but not authenticated - please log in"

            return False, f"HTTP {response.status_code}"

        except Exception as e:
            return False, f"Connection error: {str(e)}"


# Provide setup instructions when module is run directly
if __name__ == "__main__":
    print("""
IBKR Historical Data Collector Setup
=====================================

Option 1: Client Portal Gateway (Recommended)
----------------------------------------------
1. Download Client Portal Gateway:
   https://www.interactivebrokers.com/en/trading/ib-api.php
   (Look for "Client Portal API Gateway")

2. Extract and run:
   Linux/Mac: ./bin/run.sh
   Windows: bin\\run.bat

3. Open https://localhost:5000 in browser and log in

4. Set environment variables:
   export IBKR_ACCOUNT_ID=your_account_id
   export IBKR_GATEWAY_HOST=localhost
   export IBKR_GATEWAY_PORT=5000

5. Run your data collection script


Option 2: TWS API (For existing TWS users)
-------------------------------------------
1. In TWS: Edit > Global Configuration > API > Settings
2. Enable "Enable ActiveX and Socket Clients"
3. Add localhost to "Trusted IPs"
4. Note the port (default: 7497 for paper, 7496 for live)

5. Install ib_insync:
   pip install ib_insync

6. Use IBKRCollector with use_tws=True


Available Commodities:
""")
    for name, info in IBKR_FUTURES_CONTRACTS.items():
        print(f"  {name}: {info['symbol']} on {info['exchange']}")
