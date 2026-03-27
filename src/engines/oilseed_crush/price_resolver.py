"""
Price Resolver — resolves oilseed product prices from various database sources.

Supports multiple source formats:
    futures:SYMBOL         - Monthly avg settlement from silver.futures_price
    ams:COMMODITY          - USDA AMS cash price from silver.cash_price
    ratio:SYMBOL:FACTOR    - Factor × futures settlement
    differential:SYMBOL:ADJ - Futures + adjustment
    fixed:VALUE            - Hardcoded fallback

Future: elevator bid scraping for minor oilseeds near crushing facilities.
"""

import logging
from datetime import date, timedelta
from typing import Optional, Tuple

logger = logging.getLogger(__name__)


class PriceResolver:
    """Resolves prices from database sources using configurable source specs."""

    def __init__(self, conn):
        self.conn = conn
        self._cache = {}

    def resolve(self, source_spec: str, period: date) -> Tuple[Optional[float], str]:
        """
        Resolve a price for a given period.

        Returns:
            (price, description) tuple. Price is None if unavailable.
        """
        if not source_spec:
            return None, "no source configured"

        cache_key = (source_spec, period.isoformat())
        if cache_key in self._cache:
            return self._cache[cache_key]

        result = self._resolve_impl(source_spec, period)
        self._cache[cache_key] = result
        return result

    def _resolve_impl(self, spec: str, period: date) -> Tuple[Optional[float], str]:
        parts = spec.split(":")
        source_type = parts[0].lower()

        if source_type == "futures":
            return self._resolve_futures(parts[1], period)
        elif source_type == "ams":
            return self._resolve_ams(parts[1], period)
        elif source_type == "ratio":
            return self._resolve_ratio(parts[1], float(parts[2]), period)
        elif source_type == "differential":
            return self._resolve_differential(parts[1], float(parts[2]), period)
        elif source_type == "fixed":
            val = float(parts[1])
            return val, f"fixed:{val}"
        else:
            logger.warning(f"Unknown price source type: {source_type}")
            return None, f"unknown source: {spec}"

    def _resolve_futures(self, symbol: str, period: date) -> Tuple[Optional[float], str]:
        """Monthly average settlement price from silver.futures_price."""
        cur = self.conn.cursor()

        # silver.futures_price: symbol, trade_date, settlement
        cur.execute("""
            SELECT AVG(settlement) as avg_price, COUNT(*) as n
            FROM silver.futures_price
            WHERE symbol = %s
              AND trade_date >= %s
              AND trade_date < %s + INTERVAL '1 month'
        """, (symbol, period, period))

        row = cur.fetchone()
        if row and row['avg_price'] is not None and row['n'] > 0:
            price = float(row['avg_price'])
            return price, f"{symbol} avg ({row['n']} days) = {price:.4f}"

        # Try prior month as fallback
        prior = (period.replace(day=1) - timedelta(days=1)).replace(day=1)
        cur.execute("""
            SELECT AVG(settlement) as avg_price, COUNT(*) as n
            FROM silver.futures_price
            WHERE symbol = %s
              AND trade_date >= %s
              AND trade_date < %s + INTERVAL '1 month'
        """, (symbol, prior, prior))

        row = cur.fetchone()
        if row and row['avg_price'] is not None and row['n'] > 0:
            price = float(row['avg_price'])
            return price, f"{symbol} prior month avg = {price:.4f}"

        return None, f"{symbol} no data for {period}"

    def _resolve_ams(self, commodity: str, period: date) -> Tuple[Optional[float], str]:
        """USDA AMS cash price from silver.cash_price."""
        cur = self.conn.cursor()

        cur.execute("""
            SELECT AVG(price_cash) as avg_price, COUNT(*) as n
            FROM silver.cash_price
            WHERE commodity ILIKE %s
              AND report_date >= %s
              AND report_date < %s + INTERVAL '1 month'
        """, (f"%{commodity}%", period, period))

        row = cur.fetchone()
        if row and row['avg_price'] is not None and row['n'] > 0:
            price = float(row['avg_price'])
            return price, f"AMS {commodity} avg ({row['n']} obs) = {price:.4f}"

        # Try prior month
        prior = (period.replace(day=1) - timedelta(days=1)).replace(day=1)
        cur.execute("""
            SELECT AVG(price_cash) as avg_price, COUNT(*) as n
            FROM silver.cash_price
            WHERE commodity ILIKE %s
              AND report_date >= %s
              AND report_date < %s + INTERVAL '1 month'
        """, (f"%{commodity}%", prior, prior))

        row = cur.fetchone()
        if row and row['avg_price'] is not None and row['n'] > 0:
            price = float(row['avg_price'])
            return price, f"AMS {commodity} prior month = {price:.4f}"

        return None, f"AMS {commodity} no data for {period}"

    def _resolve_ratio(self, base_symbol: str, factor: float,
                       period: date) -> Tuple[Optional[float], str]:
        """Price = factor × base futures settlement."""
        base_price, base_desc = self._resolve_futures(base_symbol, period)
        if base_price is None:
            return None, f"ratio:{base_symbol}:{factor} - base unavailable"

        price = base_price * factor
        return price, f"{factor:.2f} × {base_desc} = {price:.4f}"

    def _resolve_differential(self, base_symbol: str, adjustment: float,
                              period: date) -> Tuple[Optional[float], str]:
        """Price = base futures + adjustment."""
        base_price, base_desc = self._resolve_futures(base_symbol, period)
        if base_price is None:
            return None, f"diff:{base_symbol}:{adjustment} - base unavailable"

        price = base_price + adjustment
        return price, f"{base_desc} + {adjustment:.2f} = {price:.4f}"

    def clear_cache(self):
        self._cache.clear()
