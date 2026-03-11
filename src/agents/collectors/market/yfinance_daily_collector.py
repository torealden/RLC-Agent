"""
yfinance Daily Futures Collector

Fetches daily OHLCV data for commodity futures from Yahoo Finance.
Zero-auth, free, ~20 years of history. Serves as the daily workhorse
and fallback to the IBKR session-based collector.

Front month (=F): continuous contract with full history.
Deferred months: next 6 delivery months per commodity (~2 years of
history available for active listings; expired contracts return 404).

Symbols: 10 ag/energy futures (no palm oil — Bursa Malaysia only).
Tables:  bronze.futures_daily_settlement → silver.futures_price

Usage:
    python -m src.dispatcher run yfinance_futures
    python -m src.dispatcher backfill --collectors yfinance_futures
"""

import logging
from dataclasses import dataclass, field
from datetime import date, timedelta
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import yfinance as yf

from .base_collector import (
    BaseCollector,
    CollectorConfig,
    CollectorResult,
    DataFrequency,
    AuthType,
)

logger = logging.getLogger(__name__)

# ── Month code mapping ─────────────────────────────────────────────────
MONTH_CODES = {
    'F': 1, 'G': 2, 'H': 3, 'J': 4, 'K': 5, 'M': 6,
    'N': 7, 'Q': 8, 'U': 9, 'V': 10, 'X': 11, 'Z': 12,
}

# ── Symbol map ──────────────────────────────────────────────────────────
# 'months' = delivery month letters; 'yf_suffix' = yfinance exchange suffix
YF_FUTURES: Dict[str, Dict[str, Any]] = {
    # Grains — CBOT
    "ZC": {"yf": "ZC=F", "exchange": "CBOT", "name": "Corn",
           "months": "HKNUZ", "yf_suffix": ".CBT"},
    "ZS": {"yf": "ZS=F", "exchange": "CBOT", "name": "Soybeans",
           "months": "FHKNQUX", "yf_suffix": ".CBT"},
    "ZW": {"yf": "ZW=F", "exchange": "CBOT", "name": "Wheat SRW",
           "months": "HKNUZ", "yf_suffix": ".CBT"},
    "KE": {"yf": "KE=F", "exchange": "CBOT", "name": "Wheat HRW",
           "months": "HKNUZ", "yf_suffix": ".CBT"},
    "ZM": {"yf": "ZM=F", "exchange": "CBOT", "name": "Soybean Meal",
           "months": "FHKNQUVZ", "yf_suffix": ".CBT"},
    "ZL": {"yf": "ZL=F", "exchange": "CBOT", "name": "Soybean Oil",
           "months": "FHKNQUVZ", "yf_suffix": ".CBT"},
    # Energy — NYMEX
    "CL": {"yf": "CL=F", "exchange": "NYMEX", "name": "Crude Oil WTI",
           "months": "FGHJKMNQUVXZ", "yf_suffix": ".NYM"},
    "HO": {"yf": "HO=F", "exchange": "NYMEX", "name": "Heating Oil",
           "months": "FGHJKMNQUVXZ", "yf_suffix": ".NYM"},
    "RB": {"yf": "RB=F", "exchange": "NYMEX", "name": "RBOB Gasoline",
           "months": "FGHJKMNQUVXZ", "yf_suffix": ".NYM"},
    "NG": {"yf": "NG=F", "exchange": "NYMEX", "name": "Natural Gas",
           "months": "FGHJKMNQUVXZ", "yf_suffix": ".NYM"},
}

# Reverse lookup: yf ticker → (symbol, contract_month)
# Front-month tickers map to contract_month='FRONT'
_TICKER_LOOKUP: Dict[str, Tuple[str, str]] = {
    v["yf"]: (k, "FRONT") for k, v in YF_FUTURES.items()
}


@dataclass
class YFinanceConfig(CollectorConfig):
    source_name: str = "Yahoo Finance Futures"
    source_url: str = "https://finance.yahoo.com"
    auth_type: AuthType = AuthType.NONE
    frequency: DataFrequency = DataFrequency.DAILY
    cache_enabled: bool = False          # Always fetch fresh; data is free
    rate_limit_per_minute: int = 120


class YFinanceDailyCollector(BaseCollector):
    """Daily OHLCV futures collector via yfinance."""

    def __init__(self, config: YFinanceConfig = None):
        config = config or YFinanceConfig()
        super().__init__(config)

    # ── BaseCollector abstract methods ──────────────────────────────────

    def get_table_name(self) -> str:
        return "futures_daily_settlement"

    def parse_response(self, response_data: Any) -> Any:
        return response_data

    def fetch_data(
        self,
        start_date: date = None,
        end_date: date = None,
        **kwargs,
    ) -> CollectorResult:
        """
        Download daily OHLCV for all symbols via yf.download().

        Pulls front-month continuous contracts (=F) plus the next 6
        deferred delivery months per commodity.

        Writes to bronze + silver in the same pass, then returns
        a CollectorResult for the dispatcher.
        """
        end_date = end_date or date.today()
        if start_date is None:
            start_date = end_date - timedelta(days=5)

        # Front-month tickers (existing)
        front_tickers = [v["yf"] for v in YF_FUTURES.values()]

        # Deferred contract tickers
        deferred = self._build_deferred_tickers(ref_date=end_date)
        deferred_tickers = [t[0] for t in deferred]

        # Register deferred tickers in the lookup
        for yf_ticker, symbol, contract_month in deferred:
            _TICKER_LOOKUP[yf_ticker] = (symbol, contract_month)

        tickers = front_tickers + deferred_tickers

        self.logger.info(
            f"Fetching {len(front_tickers)} front + "
            f"{len(deferred_tickers)} deferred = "
            f"{len(tickers)} tickers: {start_date} → {end_date}"
        )

        try:
            df = yf.download(
                tickers,
                start=str(start_date),
                end=str(end_date + timedelta(days=1)),  # yf end is exclusive
                auto_adjust=True,
                progress=False,
                threads=True,
            )
        except Exception as e:
            return CollectorResult(
                success=False,
                source=self.config.source_name,
                error_message=f"yf.download failed: {e}",
            )

        if df is None or df.empty:
            return CollectorResult(
                success=True,
                source=self.config.source_name,
                records_fetched=0,
                warnings=["No data returned (market closed / weekend?)"],
            )

        # ── Reshape multi-level columns into flat records ───────────
        records = self._reshape(df, tickers)

        if not records:
            return CollectorResult(
                success=True,
                source=self.config.source_name,
                records_fetched=0,
                warnings=["DataFrame returned but no valid rows after reshape"],
            )

        # ── Persist ─────────────────────────────────────────────────
        rows_saved = self._save(records)

        return CollectorResult(
            success=True,
            source=self.config.source_name,
            records_fetched=rows_saved,
            period_start=str(start_date),
            period_end=str(end_date),
        )

    # ── Internal helpers ────────────────────────────────────────────────

    @staticmethod
    def _build_deferred_tickers(
        ref_date: date = None, max_contracts: int = 6
    ) -> List[Tuple[str, str, str]]:
        """
        Generate yfinance tickers for the next `max_contracts` delivery
        months per commodity, starting from `ref_date`.

        Returns list of (yf_ticker, symbol, contract_month) tuples.
        contract_month is stored as '{month_code}{YY}', e.g. 'K26'.
        """
        ref_date = ref_date or date.today()
        cur_year = ref_date.year
        cur_month = ref_date.month
        result: List[Tuple[str, str, str]] = []

        for symbol, meta in YF_FUTURES.items():
            delivery_months = meta["months"]
            suffix = meta["yf_suffix"]
            # Build ordered list of (calendar_month, code) for this commodity
            month_list = [
                (MONTH_CODES[code], code) for code in delivery_months
            ]

            collected = 0
            # Walk up to 3 years forward to find enough contracts
            for year_offset in range(4):
                year = cur_year + year_offset
                for cal_month, code in month_list:
                    # Skip months that have already passed
                    if year == cur_year and cal_month <= cur_month:
                        continue
                    yy = year % 100
                    yf_ticker = f"{symbol}{code}{yy:02d}{suffix}"
                    contract_month = f"{code}{yy:02d}"
                    result.append((yf_ticker, symbol, contract_month))
                    collected += 1
                    if collected >= max_contracts:
                        break
                if collected >= max_contracts:
                    break

        return result

    def _reshape(
        self, df: pd.DataFrame, tickers: List[str]
    ) -> List[Dict[str, Any]]:
        """Turn the multi-ticker DataFrame into a flat list of row dicts."""
        records: List[Dict[str, Any]] = []

        # yf.download returns multi-level columns when >1 ticker:
        #   level 0 = Price (Close, High, Low, Open, Volume)
        #   level 1 = Ticker
        # For a single ticker it returns flat columns.
        multi = isinstance(df.columns, pd.MultiIndex)

        for ticker in tickers:
            lookup = _TICKER_LOOKUP.get(ticker)
            if lookup is None:
                self.logger.warning(f"Unknown ticker {ticker}, skipping")
                continue
            symbol, contract_month = lookup
            meta = YF_FUTURES[symbol]

            try:
                if multi:
                    sub = df.xs(ticker, level="Ticker", axis=1)
                else:
                    sub = df  # Only one ticker
            except KeyError:
                self.logger.debug(f"No data for {ticker}")
                continue

            for trade_date, row in sub.iterrows():
                close = row.get("Close")
                if pd.isna(close):
                    continue

                trade_dt = (
                    trade_date.date()
                    if hasattr(trade_date, "date")
                    else trade_date
                )

                records.append(
                    {
                        "trade_date": trade_dt,
                        "symbol": symbol,
                        "contract_month": contract_month,
                        "exchange": meta["exchange"],
                        "open": self._clean(row.get("Open")),
                        "high": self._clean(row.get("High")),
                        "low": self._clean(row.get("Low")),
                        "close": self._clean(close),
                        "volume": self._clean_int(row.get("Volume")),
                    }
                )

        return records

    @staticmethod
    def _clean(val) -> Optional[float]:
        if val is None or pd.isna(val):
            return None
        return round(float(val), 6)

    @staticmethod
    def _clean_int(val) -> Optional[int]:
        if val is None or pd.isna(val):
            return None
        return int(val)

    def _save(self, records: List[Dict[str, Any]]) -> int:
        """Upsert into bronze + silver in a single transaction."""
        from src.services.database.db_config import get_connection

        bronze_sql = """
            INSERT INTO bronze.futures_daily_settlement
                (trade_date, symbol, contract_month,
                 daily_open, daily_high, daily_low, settlement,
                 total_volume, exchange, source, collected_at)
            VALUES
                (%(trade_date)s, %(symbol)s, %(contract_month)s,
                 %(open)s, %(high)s, %(low)s, %(close)s,
                 %(volume)s, %(exchange)s, 'yfinance', NOW())
            ON CONFLICT (trade_date, symbol, contract_month)
            DO UPDATE SET
                daily_open   = EXCLUDED.daily_open,
                daily_high   = EXCLUDED.daily_high,
                daily_low    = EXCLUDED.daily_low,
                settlement   = EXCLUDED.settlement,
                total_volume = EXCLUDED.total_volume,
                source       = EXCLUDED.source,
                collected_at = EXCLUDED.collected_at
        """

        silver_sql = """
            INSERT INTO silver.futures_price
                (trade_date, symbol, contract_month,
                 open_price, high_price, low_price, settlement,
                 volume, exchange, source, collected_at)
            VALUES
                (%(trade_date)s, %(symbol)s, %(contract_month)s,
                 %(open)s, %(high)s, %(low)s, %(close)s,
                 %(volume)s, %(exchange)s, 'yfinance', NOW())
            ON CONFLICT (trade_date, symbol, contract_month)
            DO UPDATE SET
                open_price  = EXCLUDED.open_price,
                high_price  = EXCLUDED.high_price,
                low_price   = EXCLUDED.low_price,
                settlement  = EXCLUDED.settlement,
                volume      = EXCLUDED.volume,
                source      = EXCLUDED.source,
                collected_at = EXCLUDED.collected_at
        """

        count = 0
        with get_connection() as conn:
            with conn.cursor() as cur:
                for rec in records:
                    cur.execute(bronze_sql, rec)
                    cur.execute(silver_sql, rec)
                    count += 1
            conn.commit()

        self.logger.info(f"Saved {count} rows to bronze + silver")
        return count
