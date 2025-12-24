"""
IBKR Historical Futures Data Puller
Pulls historical OHLCV data for all configured commodities and stores in PostgreSQL

Usage:
    python pull_ibkr_historical.py

Requirements:
    - IBKR Client Portal Gateway running and authenticated
    - PostgreSQL database running
    - Environment variables set (or use config/credentials.env)
"""

import os
import sys
from pathlib import Path
from datetime import datetime, date, timedelta
from dotenv import load_dotenv

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

# Load credentials from config folder
credentials_path = project_root / "config" / "credentials.env"
if credentials_path.exists():
    load_dotenv(credentials_path)
    print(f"Loaded credentials from {credentials_path}")
else:
    load_dotenv()  # Try default .env
    print("Using default .env file")

import requests
import psycopg2
from psycopg2.extras import execute_values
import pandas as pd

# IBKR Contract definitions
IBKR_FUTURES_CONTRACTS = {
    'corn': {'symbol': 'ZC', 'exchange': 'CBOT', 'currency': 'USD', 'multiplier': 50},
    'wheat_srw': {'symbol': 'ZW', 'exchange': 'CBOT', 'currency': 'USD', 'multiplier': 50},
    'wheat_hrw': {'symbol': 'KE', 'exchange': 'CBOT', 'currency': 'USD', 'multiplier': 50},
    'soybeans': {'symbol': 'ZS', 'exchange': 'CBOT', 'currency': 'USD', 'multiplier': 50},
    'soybean_meal': {'symbol': 'ZM', 'exchange': 'CBOT', 'currency': 'USD', 'multiplier': 100},
    'soybean_oil': {'symbol': 'ZL', 'exchange': 'CBOT', 'currency': 'USD', 'multiplier': 600},
    'oats': {'symbol': 'ZO', 'exchange': 'CBOT', 'currency': 'USD', 'multiplier': 50},
    'crude_oil': {'symbol': 'CL', 'exchange': 'NYMEX', 'currency': 'USD', 'multiplier': 1000},
    'natural_gas': {'symbol': 'NG', 'exchange': 'NYMEX', 'currency': 'USD', 'multiplier': 10000},
    'gasoline': {'symbol': 'RB', 'exchange': 'NYMEX', 'currency': 'USD', 'multiplier': 42000},
    'heating_oil': {'symbol': 'HO', 'exchange': 'NYMEX', 'currency': 'USD', 'multiplier': 42000},
    'cotton': {'symbol': 'CT', 'exchange': 'NYBOT', 'currency': 'USD', 'multiplier': 500},
    'sugar': {'symbol': 'SB', 'exchange': 'NYBOT', 'currency': 'USD', 'multiplier': 1120},
    'coffee': {'symbol': 'KC', 'exchange': 'NYBOT', 'currency': 'USD', 'multiplier': 375},
    'cocoa': {'symbol': 'CC', 'exchange': 'NYBOT', 'currency': 'USD', 'multiplier': 10},
    'live_cattle': {'symbol': 'LE', 'exchange': 'CME', 'currency': 'USD', 'multiplier': 400},
    'lean_hogs': {'symbol': 'HE', 'exchange': 'CME', 'currency': 'USD', 'multiplier': 400},
    'feeder_cattle': {'symbol': 'GF', 'exchange': 'CME', 'currency': 'USD', 'multiplier': 500},
    'ethanol': {'symbol': 'EH', 'exchange': 'CBOT', 'currency': 'USD', 'multiplier': 29000},
}

# Contract month codes
MONTH_CODES = {
    1: 'F', 2: 'G', 3: 'H', 4: 'J', 5: 'K', 6: 'M',
    7: 'N', 8: 'Q', 9: 'U', 10: 'V', 11: 'X', 12: 'Z'
}

class IBKRHistoricalPuller:
    """Pull historical futures data from IBKR and store in PostgreSQL"""

    def __init__(self):
        # IBKR settings
        self.gateway_host = os.getenv('IBKR_GATEWAY_HOST', 'localhost')
        self.gateway_port = os.getenv('IBKR_GATEWAY_PORT', '5000')
        self.account_id = os.getenv('IBKR_ACCOUNT_ID')
        self.base_url = f"https://{self.gateway_host}:{self.gateway_port}/v1/api"

        # PostgreSQL settings
        self.db_config = {
            'host': os.getenv('DB_HOST', 'localhost'),
            'port': os.getenv('DB_PORT', '5432'),
            'database': os.getenv('DB_NAME', 'rlc_commodities'),
            'user': os.getenv('DB_USER', 'postgres'),
            'password': os.getenv('DB_PASSWORD', '')
        }

        # Disable SSL warnings for self-signed cert
        import urllib3
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    def check_gateway_auth(self):
        """Check if gateway is running and authenticated"""
        try:
            response = requests.get(
                f"{self.base_url}/iserver/auth/status",
                verify=False,
                timeout=5
            )
            if response.status_code == 200:
                data = response.json()
                return data.get('authenticated', False)
            return False
        except Exception as e:
            print(f"Gateway connection error: {e}")
            return False

    def search_contract(self, symbol: str) -> list:
        """Search for contract IDs by symbol"""
        try:
            response = requests.get(
                f"{self.base_url}/iserver/secdef/search",
                params={'symbol': symbol, 'secType': 'FUT'},
                verify=False,
                timeout=10
            )
            if response.status_code == 200:
                return response.json()
            return []
        except Exception as e:
            print(f"Error searching {symbol}: {e}")
            return []

    def get_contract_details(self, conid: int) -> dict:
        """Get details for a specific contract"""
        try:
            response = requests.get(
                f"{self.base_url}/iserver/contract/{conid}/info",
                verify=False,
                timeout=10
            )
            if response.status_code == 200:
                return response.json()
            return {}
        except Exception as e:
            print(f"Error getting contract details: {e}")
            return {}

    def get_historical_data(self, conid: int, period: str = "5y", bar: str = "1d") -> list:
        """
        Get historical OHLCV data for a contract

        Args:
            conid: Contract ID
            period: Time period (1d, 1w, 1m, 3m, 6m, 1y, 2y, 3y, 5y, 10y)
            bar: Bar size (1min, 5min, 15min, 1h, 4h, 1d, 1w, 1m)
        """
        try:
            response = requests.get(
                f"{self.base_url}/iserver/marketdata/history",
                params={
                    'conid': conid,
                    'period': period,
                    'bar': bar,
                    'outsideRth': 'false'
                },
                verify=False,
                timeout=30
            )
            if response.status_code == 200:
                data = response.json()
                return data.get('data', [])
            else:
                print(f"Error response: {response.status_code} - {response.text[:200]}")
            return []
        except Exception as e:
            print(f"Error getting historical data: {e}")
            return []

    def get_db_connection(self):
        """Get PostgreSQL connection"""
        return psycopg2.connect(**self.db_config)

    def create_tables(self):
        """Create futures_prices table if it doesn't exist"""
        create_sql = """
        CREATE TABLE IF NOT EXISTS futures_prices (
            id SERIAL PRIMARY KEY,
            commodity_code VARCHAR(20) NOT NULL,
            contract_symbol VARCHAR(20) NOT NULL,
            exchange VARCHAR(20),
            trade_date DATE NOT NULL,
            open DECIMAL(12,4),
            high DECIMAL(12,4),
            low DECIMAL(12,4),
            close DECIMAL(12,4),
            settle DECIMAL(12,4),
            volume BIGINT,
            open_interest BIGINT,
            source VARCHAR(50),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(commodity_code, contract_symbol, trade_date)
        );

        CREATE INDEX IF NOT EXISTS idx_futures_commodity_date ON futures_prices(commodity_code, trade_date);
        CREATE INDEX IF NOT EXISTS idx_futures_contract ON futures_prices(contract_symbol);
        """

        with self.get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(create_sql)
            conn.commit()
        print("Database tables ready")

    def store_prices(self, records: list):
        """Store price records in PostgreSQL"""
        if not records:
            return 0

        insert_sql = """
        INSERT INTO futures_prices
            (commodity_code, contract_symbol, exchange, trade_date, open, high, low, close, volume, source)
        VALUES %s
        ON CONFLICT (commodity_code, contract_symbol, trade_date)
        DO UPDATE SET
            open = EXCLUDED.open,
            high = EXCLUDED.high,
            low = EXCLUDED.low,
            close = EXCLUDED.close,
            volume = EXCLUDED.volume,
            source = EXCLUDED.source
        """

        values = [
            (r['commodity_code'], r['contract_symbol'], r['exchange'],
             r['trade_date'], r['open'], r['high'], r['low'], r['close'],
             r['volume'], r['source'])
            for r in records
        ]

        with self.get_db_connection() as conn:
            with conn.cursor() as cur:
                execute_values(cur, insert_sql, values)
            conn.commit()

        return len(values)

    def pull_commodity_history(self, commodity: str, contract_info: dict, period: str = "5y") -> int:
        """Pull historical data for a single commodity"""
        symbol = contract_info['symbol']
        exchange = contract_info['exchange']

        print(f"\n  Searching for {commodity} ({symbol})...")
        contracts = self.search_contract(symbol)

        if not contracts:
            print(f"    No contracts found for {symbol}")
            return 0

        total_records = 0

        # Process each contract found
        for contract in contracts[:10]:  # Limit to first 10 contracts
            conid = contract.get('conid')
            contract_name = contract.get('description', symbol)

            if not conid:
                continue

            print(f"    Fetching {contract_name} (conid: {conid})...")

            bars = self.get_historical_data(conid, period=period)

            if not bars:
                print(f"      No data returned")
                continue

            # Parse bars into records
            records = []
            for bar in bars:
                try:
                    # IBKR returns timestamp in milliseconds
                    ts = bar.get('t', 0)
                    if ts > 0:
                        trade_date = datetime.fromtimestamp(ts / 1000).date()
                    else:
                        continue

                    records.append({
                        'commodity_code': commodity,
                        'contract_symbol': contract_name[:20],  # Truncate if needed
                        'exchange': exchange,
                        'trade_date': trade_date,
                        'open': bar.get('o'),
                        'high': bar.get('h'),
                        'low': bar.get('l'),
                        'close': bar.get('c'),
                        'volume': bar.get('v'),
                        'source': 'IBKR'
                    })
                except Exception as e:
                    print(f"      Error parsing bar: {e}")
                    continue

            if records:
                stored = self.store_prices(records)
                print(f"      Stored {stored} records")
                total_records += stored

        return total_records

    def pull_all_history(self, period: str = "5y", commodities: list = None):
        """Pull historical data for all commodities"""

        # Check gateway
        print("Checking IBKR Gateway connection...")
        if not self.check_gateway_auth():
            print("\nERROR: IBKR Gateway not authenticated!")
            print("Please:")
            print("  1. Make sure the gateway is running (bin\\run.bat)")
            print("  2. Log in at https://localhost:5000")
            print("  3. Run this script again")
            return

        print("Gateway authenticated!")

        # Prepare database
        print("\nPreparing database...")
        self.create_tables()

        # Select commodities
        if commodities is None:
            commodities = list(IBKR_FUTURES_CONTRACTS.keys())

        print(f"\nPulling {period} of historical data for {len(commodities)} commodities...")
        print("=" * 60)

        total = 0
        for commodity in commodities:
            contract_info = IBKR_FUTURES_CONTRACTS.get(commodity)
            if not contract_info:
                print(f"\nUnknown commodity: {commodity}")
                continue

            count = self.pull_commodity_history(commodity, contract_info, period)
            total += count

        print("\n" + "=" * 60)
        print(f"COMPLETE: Stored {total} total price records in PostgreSQL")
        print("=" * 60)


def main():
    """Main entry point"""
    print("=" * 60)
    print("IBKR Historical Futures Data Puller")
    print("=" * 60)

    # Check for account ID
    account_id = os.getenv('IBKR_ACCOUNT_ID')
    if not account_id:
        print("\nERROR: IBKR_ACCOUNT_ID not set!")
        print("Set it with: $env:IBKR_ACCOUNT_ID='U16397321'")
        return

    print(f"\nAccount ID: {account_id}")

    puller = IBKRHistoricalPuller()

    # Pull 5 years of history for key grains/energy
    key_commodities = [
        'corn', 'wheat_srw', 'wheat_hrw',
        'soybeans', 'soybean_meal', 'soybean_oil',
        'crude_oil', 'natural_gas', 'ethanol'
    ]

    puller.pull_all_history(period="5y", commodities=key_commodities)


if __name__ == "__main__":
    main()
