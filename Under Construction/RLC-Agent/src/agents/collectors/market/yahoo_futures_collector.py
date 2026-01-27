#!/usr/bin/env python3
"""
Yahoo Finance Futures Collector
================================
Collects futures prices from Yahoo Finance (free, delayed data).

Supports:
- Grain futures (corn, wheat, soybeans)
- Oilseed products (soybean oil, soybean meal)
- Energy (crude oil, natural gas)

Usage:
    python yahoo_futures_collector.py fetch
    python yahoo_futures_collector.py fetch --save-db

Requirements:
    pip install yfinance
"""

import os
import sys
import logging
from datetime import datetime, date, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, field

# Setup paths
PROJECT_ROOT = Path(__file__).parent.parent.parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

try:
    import yfinance as yf
    YFINANCE_AVAILABLE = True
except ImportError:
    YFINANCE_AVAILABLE = False
    print("Warning: yfinance not installed. Run: pip install yfinance")

try:
    import pandas as pd
    PANDAS_AVAILABLE = True
except ImportError:
    PANDAS_AVAILABLE = False

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('YahooFuturesCollector')


# Yahoo Finance futures symbols
# Format: {symbol}=F for front month
FUTURES_SYMBOLS = {
    # Grains
    'ZC': {'yahoo': 'ZC=F', 'name': 'Corn', 'exchange': 'CBOT', 'unit': 'cents/bushel'},
    'ZW': {'yahoo': 'ZW=F', 'name': 'Chicago Wheat', 'exchange': 'CBOT', 'unit': 'cents/bushel'},
    'KE': {'yahoo': 'KE=F', 'name': 'KC Wheat', 'exchange': 'CBOT', 'unit': 'cents/bushel'},
    'ZS': {'yahoo': 'ZS=F', 'name': 'Soybeans', 'exchange': 'CBOT', 'unit': 'cents/bushel'},
    'ZM': {'yahoo': 'ZM=F', 'name': 'Soybean Meal', 'exchange': 'CBOT', 'unit': '$/ton'},
    'ZL': {'yahoo': 'ZL=F', 'name': 'Soybean Oil', 'exchange': 'CBOT', 'unit': 'cents/lb'},
    'ZO': {'yahoo': 'ZO=F', 'name': 'Oats', 'exchange': 'CBOT', 'unit': 'cents/bushel'},

    # Energy
    'CL': {'yahoo': 'CL=F', 'name': 'WTI Crude Oil', 'exchange': 'NYMEX', 'unit': '$/barrel'},
    'NG': {'yahoo': 'NG=F', 'name': 'Natural Gas', 'exchange': 'NYMEX', 'unit': '$/MMBtu'},
    'RB': {'yahoo': 'RB=F', 'name': 'RBOB Gasoline', 'exchange': 'NYMEX', 'unit': '$/gallon'},
    'HO': {'yahoo': 'HO=F', 'name': 'Heating Oil', 'exchange': 'NYMEX', 'unit': '$/gallon'},

    # Livestock
    'LE': {'yahoo': 'LE=F', 'name': 'Live Cattle', 'exchange': 'CME', 'unit': 'cents/lb'},
    'HE': {'yahoo': 'HE=F', 'name': 'Lean Hogs', 'exchange': 'CME', 'unit': 'cents/lb'},
    'GF': {'yahoo': 'GF=F', 'name': 'Feeder Cattle', 'exchange': 'CME', 'unit': 'cents/lb'},
}

# Ag-focused subset for HB reports
AG_SYMBOLS = ['ZC', 'ZW', 'KE', 'ZS', 'ZM', 'ZL']
ENERGY_SYMBOLS = ['CL', 'NG', 'RB', 'HO']


@dataclass
class FuturesQuote:
    """Single futures quote"""
    symbol: str
    name: str
    exchange: str
    price: float
    change: float
    change_pct: float
    open: float
    high: float
    low: float
    volume: int
    timestamp: datetime
    unit: str


def fetch_futures_prices(symbols: List[str] = None) -> List[FuturesQuote]:
    """
    Fetch current futures prices from Yahoo Finance.

    Args:
        symbols: List of contract codes (e.g., ['ZC', 'ZS']).
                 Defaults to all ag symbols.

    Returns:
        List of FuturesQuote objects
    """
    if not YFINANCE_AVAILABLE:
        logger.error("yfinance not available")
        return []

    symbols = symbols or AG_SYMBOLS
    quotes = []

    for symbol in symbols:
        if symbol not in FUTURES_SYMBOLS:
            logger.warning(f"Unknown symbol: {symbol}")
            continue

        info = FUTURES_SYMBOLS[symbol]
        yahoo_symbol = info['yahoo']

        try:
            ticker = yf.Ticker(yahoo_symbol)
            data = ticker.info

            if not data or 'regularMarketPrice' not in data:
                # Try getting recent history instead
                hist = ticker.history(period='1d')
                if hist.empty:
                    logger.warning(f"No data for {symbol}")
                    continue

                last_row = hist.iloc[-1]
                quote = FuturesQuote(
                    symbol=symbol,
                    name=info['name'],
                    exchange=info['exchange'],
                    price=float(last_row['Close']),
                    change=float(last_row['Close'] - last_row['Open']),
                    change_pct=float((last_row['Close'] - last_row['Open']) / last_row['Open'] * 100) if last_row['Open'] > 0 else 0,
                    open=float(last_row['Open']),
                    high=float(last_row['High']),
                    low=float(last_row['Low']),
                    volume=int(last_row['Volume']) if 'Volume' in last_row else 0,
                    timestamp=datetime.now(),
                    unit=info['unit']
                )
            else:
                quote = FuturesQuote(
                    symbol=symbol,
                    name=info['name'],
                    exchange=info['exchange'],
                    price=float(data.get('regularMarketPrice', 0)),
                    change=float(data.get('regularMarketChange', 0)),
                    change_pct=float(data.get('regularMarketChangePercent', 0)),
                    open=float(data.get('regularMarketOpen', 0)),
                    high=float(data.get('regularMarketDayHigh', 0)),
                    low=float(data.get('regularMarketDayLow', 0)),
                    volume=int(data.get('regularMarketVolume', 0) or 0),
                    timestamp=datetime.now(),
                    unit=info['unit']
                )

            quotes.append(quote)
            logger.info(f"{symbol} ({info['name']}): ${quote.price:.2f} ({quote.change:+.2f})")

        except Exception as e:
            logger.error(f"Error fetching {symbol}: {e}")

    return quotes


def fetch_historical_prices(
    symbols: List[str] = None,
    days: int = 30
) -> Optional[pd.DataFrame]:
    """
    Fetch historical futures prices.

    Args:
        symbols: List of contract codes
        days: Number of days of history

    Returns:
        DataFrame with historical prices
    """
    if not YFINANCE_AVAILABLE or not PANDAS_AVAILABLE:
        return None

    symbols = symbols or AG_SYMBOLS
    yahoo_symbols = [FUTURES_SYMBOLS[s]['yahoo'] for s in symbols if s in FUTURES_SYMBOLS]

    end_date = date.today()
    start_date = end_date - timedelta(days=days)

    try:
        data = yf.download(
            yahoo_symbols,
            start=start_date,
            end=end_date,
            progress=False
        )

        if data.empty:
            logger.warning("No historical data returned")
            return None

        return data

    except Exception as e:
        logger.error(f"Error fetching historical data: {e}")
        return None


def save_to_database(quotes: List[FuturesQuote], conn=None):
    """
    Save futures quotes to PostgreSQL database.

    Args:
        quotes: List of FuturesQuote objects
        conn: Database connection (creates new if None)
    """
    from dotenv import load_dotenv
    import psycopg2

    load_dotenv(PROJECT_ROOT / ".env")

    if conn is None:
        conn = psycopg2.connect(
            host=os.getenv('DB_HOST', 'localhost'),
            port=os.getenv('DB_PORT', '5432'),
            database=os.getenv('DB_NAME', 'rlc_commodities'),
            user=os.getenv('DB_USER', 'postgres'),
            password=os.getenv('DB_PASSWORD', '')
        )

    cur = conn.cursor()

    # Use existing table structure:
    # id, trade_date, symbol, contract_month, contract_date, open_price,
    # high_price, low_price, settlement, volume, open_interest, exchange, source, collected_at

    # Insert quotes (using existing schema)
    for quote in quotes:
        cur.execute("""
            INSERT INTO silver.futures_price
            (trade_date, symbol, contract_month, open_price, high_price, low_price,
             settlement, volume, exchange, source, collected_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
            ON CONFLICT (symbol, trade_date, contract_month)
            DO UPDATE SET
                open_price = EXCLUDED.open_price,
                high_price = EXCLUDED.high_price,
                low_price = EXCLUDED.low_price,
                settlement = EXCLUDED.settlement,
                volume = EXCLUDED.volume,
                collected_at = NOW()
        """, (
            quote.timestamp.date(),
            quote.symbol,
            'FRONT',  # Front month indicator
            quote.open,
            quote.high,
            quote.low,
            quote.price,  # Settlement = closing price
            quote.volume,
            quote.exchange,
            'yahoo_finance'
        ))

    conn.commit()
    logger.info(f"Saved {len(quotes)} quotes to database")

    return True


def main():
    """CLI interface"""
    import argparse

    parser = argparse.ArgumentParser(description='Yahoo Finance Futures Collector')
    parser.add_argument('command', choices=['fetch', 'history', 'test'],
                        help='Command to run')
    parser.add_argument('--symbols', nargs='+', default=None,
                        help='Symbols to fetch (default: all ag)')
    parser.add_argument('--days', type=int, default=30,
                        help='Days of history for history command')
    parser.add_argument('--save-db', action='store_true',
                        help='Save results to database')
    parser.add_argument('--output', '-o', help='Output file (CSV)')

    args = parser.parse_args()

    if args.command == 'test':
        print("Testing Yahoo Finance connection...")
        if not YFINANCE_AVAILABLE:
            print("FAIL: yfinance not installed")
            return

        # Test with corn
        quotes = fetch_futures_prices(['ZC'])
        if quotes:
            print(f"SUCCESS: Corn at ${quotes[0].price:.2f}")
        else:
            print("FAIL: Could not fetch data")
        return

    if args.command == 'fetch':
        print("Fetching current futures prices...")
        symbols = args.symbols or AG_SYMBOLS + ENERGY_SYMBOLS
        quotes = fetch_futures_prices(symbols)

        if not quotes:
            print("No data retrieved")
            return

        print(f"\n{'Symbol':<8} {'Name':<20} {'Price':>12} {'Change':>10} {'Volume':>12}")
        print("-" * 70)
        for q in quotes:
            print(f"{q.symbol:<8} {q.name:<20} {q.price:>12.2f} {q.change:>+10.2f} {q.volume:>12,}")

        if args.save_db:
            save_to_database(quotes)
            print(f"\nSaved {len(quotes)} quotes to database")

        if args.output:
            if PANDAS_AVAILABLE:
                df = pd.DataFrame([vars(q) for q in quotes])
                df.to_csv(args.output, index=False)
                print(f"Saved to {args.output}")

        return

    if args.command == 'history':
        print(f"Fetching {args.days} days of history...")
        symbols = args.symbols or AG_SYMBOLS
        df = fetch_historical_prices(symbols, args.days)

        if df is not None:
            print(df.tail(10))

            if args.output:
                df.to_csv(args.output)
                print(f"Saved to {args.output}")
        else:
            print("No historical data retrieved")


if __name__ == '__main__':
    main()
