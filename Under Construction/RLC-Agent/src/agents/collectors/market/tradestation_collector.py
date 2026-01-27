"""
TradeStation Historical Data Collector

Fetches historical futures prices via the TradeStation API.

TradeStation API Setup:
1. Log into TradeStation developer portal: https://developer.tradestation.com
2. Create an application to get Client ID and Secret
3. Configure redirect URI (can use http://localhost for testing)

Environment Variables:
    TRADESTATION_CLIENT_ID - Your API Client ID
    TRADESTATION_CLIENT_SECRET - Your API Client Secret
    TRADESTATION_REFRESH_TOKEN - OAuth refresh token (after initial auth)

API Documentation:
    https://api.tradestation.com/docs/fundamentals/http-api
"""

import os
import logging
import base64
from datetime import datetime, date, timedelta
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any
from pathlib import Path
import json

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


# TradeStation futures symbols
TRADESTATION_FUTURES = {
    # Grains - CBOT
    'corn': {'symbol': 'ZC', 'exchange': 'CME', 'description': 'Corn Futures'},
    'wheat_srw': {'symbol': 'ZW', 'exchange': 'CME', 'description': 'Chicago SRW Wheat'},
    'wheat_hrw': {'symbol': 'KE', 'exchange': 'CME', 'description': 'KC HRW Wheat'},
    'soybeans': {'symbol': 'ZS', 'exchange': 'CME', 'description': 'Soybean Futures'},
    'soybean_meal': {'symbol': 'ZM', 'exchange': 'CME', 'description': 'Soybean Meal'},
    'soybean_oil': {'symbol': 'ZL', 'exchange': 'CME', 'description': 'Soybean Oil'},
    'oats': {'symbol': 'ZO', 'exchange': 'CME', 'description': 'Oats Futures'},

    # Energy - NYMEX
    'crude_oil': {'symbol': 'CL', 'exchange': 'NYMEX', 'description': 'WTI Crude Oil'},
    'natural_gas': {'symbol': 'NG', 'exchange': 'NYMEX', 'description': 'Natural Gas'},
    'gasoline': {'symbol': 'RB', 'exchange': 'NYMEX', 'description': 'RBOB Gasoline'},
    'heating_oil': {'symbol': 'HO', 'exchange': 'NYMEX', 'description': 'Heating Oil/ULSD'},

    # Softs - ICE
    'cotton': {'symbol': 'CT', 'exchange': 'ICE', 'description': 'Cotton #2'},
    'sugar': {'symbol': 'SB', 'exchange': 'ICE', 'description': 'Sugar #11'},
    'coffee': {'symbol': 'KC', 'exchange': 'ICE', 'description': 'Coffee C'},

    # Livestock - CME
    'live_cattle': {'symbol': 'LE', 'exchange': 'CME', 'description': 'Live Cattle'},
    'lean_hogs': {'symbol': 'HE', 'exchange': 'CME', 'description': 'Lean Hogs'},
    'feeder_cattle': {'symbol': 'GF', 'exchange': 'CME', 'description': 'Feeder Cattle'},

    # Ethanol
    'ethanol': {'symbol': 'EH', 'exchange': 'CME', 'description': 'Chicago Ethanol'},
}

# Contract month codes
MONTH_CODES = {
    1: 'F', 2: 'G', 3: 'H', 4: 'J', 5: 'K', 6: 'M',
    7: 'N', 8: 'Q', 9: 'U', 10: 'V', 11: 'X', 12: 'Z'
}


@dataclass
class TradeStationConfig(CollectorConfig):
    """Configuration for TradeStation collector"""
    source_name: str = "TradeStation"
    source_url: str = "https://api.tradestation.com/v3"
    auth_type: AuthType = AuthType.OAUTH

    # TradeStation API credentials
    client_id: str = field(default_factory=lambda: os.environ.get('TRADESTATION_CLIENT_ID', ''))
    client_secret: str = field(default_factory=lambda: os.environ.get('TRADESTATION_CLIENT_SECRET', ''))
    refresh_token: str = field(default_factory=lambda: os.environ.get('TRADESTATION_REFRESH_TOKEN', ''))

    # OAuth settings
    auth_url: str = "https://signin.tradestation.com/oauth/token"
    redirect_uri: str = "http://localhost"

    # Data settings
    frequency: DataFrequency = DataFrequency.DAILY
    bar_unit: str = "Daily"  # Options: Minute, Daily, Weekly, Monthly
    bars_back: int = 365  # Number of bars to fetch

    commodities: List[str] = field(default_factory=lambda: [
        'corn', 'wheat_srw', 'wheat_hrw', 'soybeans', 'soybean_meal', 'soybean_oil',
        'crude_oil', 'natural_gas', 'ethanol'
    ])


class TradeStationCollector(BaseCollector):
    """
    Collector for TradeStation historical futures data.

    Uses OAuth 2.0 for authentication.
    """

    def __init__(self, config: TradeStationConfig = None):
        if config is None:
            config = TradeStationConfig()
        super().__init__(config)

        self.config: TradeStationConfig = config
        self.access_token = None
        self.token_expiry = None

    def get_table_name(self) -> str:
        return "tradestation_futures_prices"

    def _get_front_month_symbol(self, commodity: str, as_of_date: date = None) -> str:
        """Get the front month continuous contract symbol"""
        if as_of_date is None:
            as_of_date = date.today()

        contract_info = TRADESTATION_FUTURES.get(commodity)
        if not contract_info:
            return None

        symbol = contract_info['symbol']

        # TradeStation uses @<symbol> for continuous front month
        # Or specific contracts like ZCH24 for March 2024 Corn
        return f"@{symbol}"  # Continuous front month

    def _authenticate(self) -> bool:
        """Authenticate with TradeStation API using refresh token"""
        if not self.config.client_id or not self.config.client_secret:
            self.logger.error("Missing client_id or client_secret")
            return False

        if not self.config.refresh_token:
            self.logger.error(
                "Missing refresh_token. Complete initial OAuth flow first. "
                "See setup instructions below."
            )
            return False

        try:
            # Prepare auth header
            credentials = f"{self.config.client_id}:{self.config.client_secret}"
            auth_header = base64.b64encode(credentials.encode()).decode()

            headers = {
                'Authorization': f'Basic {auth_header}',
                'Content-Type': 'application/x-www-form-urlencoded'
            }

            data = {
                'grant_type': 'refresh_token',
                'refresh_token': self.config.refresh_token,
            }

            # Use requests directly for auth
            import requests
            response = requests.post(
                self.config.auth_url,
                headers=headers,
                data=data,
                timeout=30
            )

            if response.status_code == 200:
                token_data = response.json()
                self.access_token = token_data.get('access_token')
                expires_in = token_data.get('expires_in', 1200)
                self.token_expiry = datetime.now() + timedelta(seconds=expires_in)

                # Update session headers
                self.session.headers['Authorization'] = f'Bearer {self.access_token}'

                self.logger.info("Successfully authenticated with TradeStation")
                return True
            else:
                self.logger.error(f"Auth failed: {response.status_code} - {response.text}")
                return False

        except Exception as e:
            self.logger.error(f"Authentication error: {e}")
            return False

    def _ensure_authenticated(self) -> bool:
        """Ensure we have a valid access token"""
        if self.access_token and self.token_expiry:
            if datetime.now() < self.token_expiry - timedelta(minutes=5):
                return True

        return self._authenticate()

    def _fetch_bars(
        self,
        symbol: str,
        unit: str = "Daily",
        bars_back: int = 365
    ) -> Optional[List[Dict]]:
        """Fetch historical bars for a symbol"""
        try:
            params = {
                'unit': unit,
                'barsback': bars_back,
            }

            response, error = self._make_request(
                f"{self.config.source_url}/marketdata/barcharts/{symbol}",
                params=params
            )

            if error:
                self.logger.warning(f"Error fetching bars for {symbol}: {error}")
                return None

            if response and response.status_code == 200:
                data = response.json()
                return data.get('Bars', [])
            elif response and response.status_code == 401:
                self.logger.warning("Token expired, re-authenticating...")
                if self._authenticate():
                    return self._fetch_bars(symbol, unit, bars_back)

            return None

        except Exception as e:
            self.logger.error(f"Error fetching bars: {e}")
            return None

    def _fetch_quote(self, symbol: str) -> Optional[Dict]:
        """Fetch current quote for a symbol"""
        try:
            response, error = self._make_request(
                f"{self.config.source_url}/marketdata/quotes/{symbol}"
            )

            if error:
                return None

            if response and response.status_code == 200:
                data = response.json()
                quotes = data.get('Quotes', [])
                return quotes[0] if quotes else None

            return None
        except Exception:
            return None

    def fetch_data(
        self,
        start_date: date = None,
        end_date: date = None,
        **kwargs
    ) -> CollectorResult:
        """
        Fetch historical futures data from TradeStation.
        """
        commodities = kwargs.get('commodities', self.config.commodities)
        all_records = []
        warnings = []

        # Authenticate
        if not self._ensure_authenticated():
            return CollectorResult(
                success=False,
                source=self.config.source_name,
                error_message="Authentication failed",
                warnings=[
                    "TradeStation requires OAuth authentication.",
                    "1. Register app at https://developer.tradestation.com",
                    "2. Complete OAuth flow to get refresh token",
                    "3. Set TRADESTATION_CLIENT_ID, TRADESTATION_CLIENT_SECRET, TRADESTATION_REFRESH_TOKEN"
                ]
            )

        for commodity in commodities:
            contract_info = TRADESTATION_FUTURES.get(commodity)
            if not contract_info:
                warnings.append(f"Unknown commodity: {commodity}")
                continue

            # Get continuous contract symbol
            symbol = self._get_front_month_symbol(commodity, end_date)
            self.logger.info(f"Fetching {commodity} ({symbol})")

            # Fetch historical bars
            bars = self._fetch_bars(
                symbol,
                unit=self.config.bar_unit,
                bars_back=self.config.bars_back
            )

            if not bars:
                warnings.append(f"{commodity}: No data returned")
                continue

            # Parse bars into records
            for bar in bars:
                record = {
                    'commodity': commodity,
                    'symbol': contract_info['symbol'],
                    'exchange': contract_info['exchange'],
                    'date': bar.get('TimeStamp', '')[:10],  # Extract date part
                    'open': bar.get('Open'),
                    'high': bar.get('High'),
                    'low': bar.get('Low'),
                    'close': bar.get('Close'),
                    'volume': bar.get('TotalVolume'),
                    'open_interest': bar.get('OpenInterest'),
                    'source': 'TradeStation',
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
        """Parse TradeStation API response"""
        return response_data

    def test_connection(self) -> tuple:
        """Test connection to TradeStation API"""
        if not self.config.client_id:
            return False, "Missing TRADESTATION_CLIENT_ID"
        if not self.config.client_secret:
            return False, "Missing TRADESTATION_CLIENT_SECRET"
        if not self.config.refresh_token:
            return False, "Missing TRADESTATION_REFRESH_TOKEN - complete OAuth flow first"

        if self._ensure_authenticated():
            return True, "Connected and authenticated"
        return False, "Authentication failed"


def generate_auth_url(client_id: str, redirect_uri: str = "http://localhost") -> str:
    """Generate OAuth authorization URL for initial token setup"""
    base_url = "https://signin.tradestation.com/authorize"
    params = {
        'response_type': 'code',
        'client_id': client_id,
        'redirect_uri': redirect_uri,
        'audience': 'https://api.tradestation.com',
        'scope': 'MarketData ReadAccount',
    }
    param_str = '&'.join(f"{k}={v}" for k, v in params.items())
    return f"{base_url}?{param_str}"


# Setup instructions when module is run directly
if __name__ == "__main__":
    print("""
TradeStation Historical Data Collector Setup
=============================================

Step 1: Register Your Application
----------------------------------
1. Go to https://developer.tradestation.com
2. Log in with your TradeStation credentials
3. Create a new application
4. Note your Client ID and Client Secret
5. Set redirect URI to: http://localhost

Step 2: Get Initial Authorization Code
---------------------------------------
Run Python and execute:

    from tradestation_collector import generate_auth_url
    url = generate_auth_url('YOUR_CLIENT_ID')
    print(url)

Open that URL in browser, log in, and authorize.
You'll be redirected to localhost with a 'code' parameter.
Copy that code.

Step 3: Exchange Code for Tokens
---------------------------------
Use this code to get refresh token:

    import requests
    import base64

    client_id = 'YOUR_CLIENT_ID'
    client_secret = 'YOUR_CLIENT_SECRET'
    auth_code = 'CODE_FROM_REDIRECT'

    credentials = f"{client_id}:{client_secret}"
    auth_header = base64.b64encode(credentials.encode()).decode()

    response = requests.post(
        'https://signin.tradestation.com/oauth/token',
        headers={
            'Authorization': f'Basic {auth_header}',
            'Content-Type': 'application/x-www-form-urlencoded'
        },
        data={
            'grant_type': 'authorization_code',
            'code': auth_code,
            'redirect_uri': 'http://localhost'
        }
    )

    tokens = response.json()
    print("Refresh Token:", tokens.get('refresh_token'))

Step 4: Set Environment Variables
----------------------------------
Add to your .env file:

    TRADESTATION_CLIENT_ID=your_client_id
    TRADESTATION_CLIENT_SECRET=your_client_secret
    TRADESTATION_REFRESH_TOKEN=your_refresh_token


Available Commodities:
""")
    for name, info in TRADESTATION_FUTURES.items():
        print(f"  {name}: {info['symbol']} - {info['description']}")
