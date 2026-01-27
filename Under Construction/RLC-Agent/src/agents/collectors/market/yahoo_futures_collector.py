#!/usr/bin/env python3
"""
Yahoo Finance Futures Collector
================================
Collects futures prices from Yahoo Finance (free, delayed data).

Supports:
- Grain futures (corn, wheat, soybeans)
- Oilseed products (soybean oil, soybean meal)
- Energy (crude oil, natural gas)

Session-Based Collection:
-------------------------
Captures discrete trading session data:
- Overnight (Globex): 7 PM - 7:45 AM CT (next day)
- US Session (RTH): 8:30 AM - 1:20 PM CT
- Daily Settlement: After 3 PM CT

Usage:
    python yahoo_futures_collector.py fetch
    python yahoo_futures_collector.py fetch --save-db
    python yahoo_futures_collector.py session --type overnight
    python yahoo_futures_collector.py session --type us
    python yahoo_futures_collector.py session --type settlement

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
from enum import Enum

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


class SessionType(Enum):
    """Trading session types for discrete capture"""
    OVERNIGHT = "overnight"     # Globex session (7 PM - 7:45 AM CT)
    US_SESSION = "us"          # Regular trading hours (8:30 AM - 1:20 PM CT)
    SETTLEMENT = "settlement"   # End of day settlement


@dataclass
class SessionQuote:
    """Session-specific futures quote"""
    symbol: str
    trade_date: date
    contract_month: str
    session_type: SessionType
    open: float
    high: float
    low: float
    close: float
    volume: int
    exchange: str
    collected_at: datetime


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


# =============================================================================
# SESSION-BASED COLLECTION FUNCTIONS
# =============================================================================

def fetch_session_data(
    symbols: List[str] = None,
    session_type: SessionType = SessionType.SETTLEMENT
) -> List[SessionQuote]:
    """
    Fetch futures data for a specific trading session.

    Since Yahoo Finance provides delayed aggregate data, we capture
    the current OHLC at specific times to approximate session data:
    - Overnight: Capture at ~8:45 AM ET (7:45 AM CT)
    - US Session: Capture at ~2:30 PM ET (1:30 PM CT)
    - Settlement: Capture at ~6:00 PM ET (5:00 PM CT)

    Args:
        symbols: List of contract codes
        session_type: Which session to capture

    Returns:
        List of SessionQuote objects
    """
    if not YFINANCE_AVAILABLE:
        logger.error("yfinance not available")
        return []

    symbols = symbols or AG_SYMBOLS + ENERGY_SYMBOLS
    quotes = []

    for symbol in symbols:
        if symbol not in FUTURES_SYMBOLS:
            logger.warning(f"Unknown symbol: {symbol}")
            continue

        info = FUTURES_SYMBOLS[symbol]
        yahoo_symbol = info['yahoo']

        try:
            ticker = yf.Ticker(yahoo_symbol)

            # Get 1-day history for OHLC data
            hist = ticker.history(period='1d')

            if hist.empty:
                # Try ticker.info as fallback
                data = ticker.info
                if not data or 'regularMarketPrice' not in data:
                    logger.warning(f"No data for {symbol}")
                    continue

                quote = SessionQuote(
                    symbol=symbol,
                    trade_date=date.today(),
                    contract_month='FRONT',
                    session_type=session_type,
                    open=float(data.get('regularMarketOpen', 0)),
                    high=float(data.get('regularMarketDayHigh', 0)),
                    low=float(data.get('regularMarketDayLow', 0)),
                    close=float(data.get('regularMarketPrice', 0)),
                    volume=int(data.get('regularMarketVolume', 0) or 0),
                    exchange=info['exchange'],
                    collected_at=datetime.now()
                )
            else:
                last_row = hist.iloc[-1]
                quote = SessionQuote(
                    symbol=symbol,
                    trade_date=date.today(),
                    contract_month='FRONT',
                    session_type=session_type,
                    open=float(last_row['Open']),
                    high=float(last_row['High']),
                    low=float(last_row['Low']),
                    close=float(last_row['Close']),
                    volume=int(last_row['Volume']) if 'Volume' in last_row else 0,
                    exchange=info['exchange'],
                    collected_at=datetime.now()
                )

            quotes.append(quote)
            logger.info(f"{session_type.value.upper()} {symbol}: "
                       f"O={quote.open:.2f} H={quote.high:.2f} "
                       f"L={quote.low:.2f} C={quote.close:.2f}")

        except Exception as e:
            logger.error(f"Error fetching {symbol}: {e}")

    return quotes


def save_session_to_bronze(
    quotes: List[SessionQuote],
    session_type: SessionType,
    conn=None
) -> int:
    """
    Save session quotes to appropriate bronze table.

    Args:
        quotes: List of SessionQuote objects
        session_type: Type of session (determines target table)
        conn: Database connection

    Returns:
        Number of records saved
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
    saved_count = 0

    for quote in quotes:
        try:
            if session_type == SessionType.OVERNIGHT:
                cur.execute("""
                    INSERT INTO bronze.futures_overnight_session
                    (trade_date, symbol, contract_month,
                     overnight_open, overnight_high, overnight_low, overnight_close,
                     overnight_volume, exchange, source, collected_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (trade_date, symbol, contract_month)
                    DO UPDATE SET
                        overnight_open = EXCLUDED.overnight_open,
                        overnight_high = EXCLUDED.overnight_high,
                        overnight_low = EXCLUDED.overnight_low,
                        overnight_close = EXCLUDED.overnight_close,
                        overnight_volume = EXCLUDED.overnight_volume,
                        collected_at = EXCLUDED.collected_at
                """, (
                    quote.trade_date,
                    quote.symbol,
                    quote.contract_month,
                    quote.open,
                    quote.high,
                    quote.low,
                    quote.close,
                    quote.volume,
                    quote.exchange,
                    'yahoo_finance',
                    quote.collected_at
                ))

            elif session_type == SessionType.US_SESSION:
                cur.execute("""
                    INSERT INTO bronze.futures_us_session
                    (trade_date, symbol, contract_month,
                     us_open, us_high, us_low, us_close,
                     us_volume, exchange, source, collected_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (trade_date, symbol, contract_month)
                    DO UPDATE SET
                        us_open = EXCLUDED.us_open,
                        us_high = EXCLUDED.us_high,
                        us_low = EXCLUDED.us_low,
                        us_close = EXCLUDED.us_close,
                        us_volume = EXCLUDED.us_volume,
                        collected_at = EXCLUDED.collected_at
                """, (
                    quote.trade_date,
                    quote.symbol,
                    quote.contract_month,
                    quote.open,
                    quote.high,
                    quote.low,
                    quote.close,
                    quote.volume,
                    quote.exchange,
                    'yahoo_finance',
                    quote.collected_at
                ))

            elif session_type == SessionType.SETTLEMENT:
                # Get prior settlement for change calculation
                cur.execute("""
                    SELECT settlement FROM bronze.futures_daily_settlement
                    WHERE symbol = %s AND contract_month = %s
                    AND trade_date < %s
                    ORDER BY trade_date DESC LIMIT 1
                """, (quote.symbol, quote.contract_month, quote.trade_date))
                prior_row = cur.fetchone()
                prior_settlement = prior_row[0] if prior_row else None

                change = None
                change_pct = None
                if prior_settlement and prior_settlement > 0:
                    change = quote.close - float(prior_settlement)
                    change_pct = (change / float(prior_settlement)) * 100

                cur.execute("""
                    INSERT INTO bronze.futures_daily_settlement
                    (trade_date, symbol, contract_month,
                     daily_open, daily_high, daily_low, settlement,
                     prior_settlement, change, change_pct,
                     total_volume, exchange, source, collected_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (trade_date, symbol, contract_month)
                    DO UPDATE SET
                        daily_open = EXCLUDED.daily_open,
                        daily_high = EXCLUDED.daily_high,
                        daily_low = EXCLUDED.daily_low,
                        settlement = EXCLUDED.settlement,
                        prior_settlement = EXCLUDED.prior_settlement,
                        change = EXCLUDED.change,
                        change_pct = EXCLUDED.change_pct,
                        total_volume = EXCLUDED.total_volume,
                        collected_at = EXCLUDED.collected_at
                """, (
                    quote.trade_date,
                    quote.symbol,
                    quote.contract_month,
                    quote.open,
                    quote.high,
                    quote.low,
                    quote.close,
                    prior_settlement,
                    change,
                    change_pct,
                    quote.volume,
                    quote.exchange,
                    'yahoo_finance',
                    quote.collected_at
                ))

            saved_count += 1

        except Exception as e:
            logger.error(f"Error saving {quote.symbol}: {e}")
            conn.rollback()
            continue

    conn.commit()
    logger.info(f"Saved {saved_count} {session_type.value} session quotes to bronze layer")

    return saved_count


def collect_all_sessions(symbols: List[str] = None, save_db: bool = True):
    """
    Collect data for all session types and optionally save to database.
    Typically called multiple times per day at session boundaries.

    Args:
        symbols: List of symbols to collect
        save_db: Whether to save to database

    Returns:
        Dict with session results
    """
    results = {}

    for session_type in SessionType:
        quotes = fetch_session_data(symbols, session_type)
        results[session_type.value] = {
            'count': len(quotes),
            'quotes': quotes
        }

        if save_db and quotes:
            save_session_to_bronze(quotes, session_type)

    return results


def main():
    """CLI interface"""
    import argparse

    parser = argparse.ArgumentParser(description='Yahoo Finance Futures Collector')
    parser.add_argument('command', choices=['fetch', 'history', 'test', 'session'],
                        help='Command to run')
    parser.add_argument('--symbols', nargs='+', default=None,
                        help='Symbols to fetch (default: all ag + energy)')
    parser.add_argument('--days', type=int, default=30,
                        help='Days of history for history command')
    parser.add_argument('--save-db', action='store_true',
                        help='Save results to database')
    parser.add_argument('--output', '-o', help='Output file (CSV)')
    parser.add_argument('--type', '-t',
                        choices=['overnight', 'us', 'settlement', 'all'],
                        default='settlement',
                        help='Session type for session command')

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

    if args.command == 'session':
        symbols = args.symbols or AG_SYMBOLS + ENERGY_SYMBOLS

        if args.type == 'all':
            print("Collecting all session types...")
            results = collect_all_sessions(symbols, save_db=args.save_db)
            for session, data in results.items():
                print(f"  {session}: {data['count']} quotes")
        else:
            session_map = {
                'overnight': SessionType.OVERNIGHT,
                'us': SessionType.US_SESSION,
                'settlement': SessionType.SETTLEMENT
            }
            session_type = session_map[args.type]

            print(f"Fetching {args.type.upper()} session data...")
            quotes = fetch_session_data(symbols, session_type)

            if not quotes:
                print("No data retrieved")
                return

            print(f"\n{'Symbol':<8} {'Open':>10} {'High':>10} {'Low':>10} {'Close':>10} {'Volume':>12}")
            print("-" * 70)
            for q in quotes:
                print(f"{q.symbol:<8} {q.open:>10.2f} {q.high:>10.2f} "
                      f"{q.low:>10.2f} {q.close:>10.2f} {q.volume:>12,}")

            if args.save_db:
                count = save_session_to_bronze(quotes, session_type)
                print(f"\nSaved {count} quotes to bronze.futures_{args.type}_session")

            if args.output and PANDAS_AVAILABLE:
                df = pd.DataFrame([{
                    'symbol': q.symbol,
                    'trade_date': q.trade_date,
                    'session': q.session_type.value,
                    'open': q.open,
                    'high': q.high,
                    'low': q.low,
                    'close': q.close,
                    'volume': q.volume
                } for q in quotes])
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
