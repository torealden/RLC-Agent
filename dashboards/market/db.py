"""
Database access for the RLC Market Dashboard.

Wraps the canonical src.services.database.db_config helper (single source of
env handling) and adds Streamlit caching. All page-level query functions
live here so caching policy is in one place:
  - engine: st.cache_resource
  - prices: ttl=300
  - comparison / realized: ttl=900
  - metadata / introspection: ttl=3600

Data facts this module depends on (verified live 2026-08-01):
  - silver.futures_price.contract_date is NULL on every row — never use it.
  - The yfinance collector writes a synthetic contract_month='FRONT' row per
    (symbol, trade_date) carrying front-month settle + OHLC. All symbols
    except FCPO have it. Real contract months match ^[FGHJKMNQUVXZ][0-9]{2}$.
"""
import os
import sys
from pathlib import Path

import pandas as pd
import streamlit as st

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.services.database.db_config import get_connection_string  # noqa: E402

def _sort_key(col: str = 'contract_month') -> str:
    """SQL expr ordering contract codes chronologically: F=Jan ... Z=Dec,
    e.g. X26 -> 202611. NULL for non-standard codes (incl. 'FRONT')."""
    return (f"(CASE WHEN {col} ~ '^[FGHJKMNQUVXZ][0-9]{{2}}$' "
            f"THEN (2000 + RIGHT({col}, 2)::int) * 100 "
            f"+ STRPOS('FGHJKMNQUVXZ', LEFT({col}, 1)) END)")


SORT_KEY = _sort_key()


@st.cache_resource
def get_engine_cached():
    from sqlalchemy import create_engine
    url = get_connection_string()
    sslmode = os.environ.get('RLC_PG_SSLMODE', 'require')
    if url.startswith('postgresql') and 'sslmode' not in url:
        url = f'{url}?sslmode={sslmode}'
    return create_engine(url, pool_pre_ping=True)


def query_df(sql: str, params: dict | None = None) -> pd.DataFrame:
    return pd.read_sql(sql, get_engine_cached(), params=params)


# ── Prices ──────────────────────────────────────────────────────────────────

@st.cache_data(ttl=300, show_spinner=False)
def get_price_strip() -> pd.DataFrame:
    """Latest front-month settle per symbol + previous settle for the day
    change + gold-layer validation flag. Uses the collector's FRONT rows;
    symbols without FRONT rows (FCPO) fall back to the nearest unexpired
    contract at their own latest date (no day-change for those — they are
    stale and rendered dimmed anyway)."""
    return query_df(f"""
        WITH latest AS (
            SELECT symbol, MAX(trade_date) AS td
            FROM silver.futures_price
            GROUP BY symbol
        ),
        front AS (
            SELECT sp.symbol, sp.trade_date, sp.settlement
            FROM silver.futures_price sp
            JOIN latest l ON l.symbol = sp.symbol AND l.td = sp.trade_date
            WHERE sp.contract_month = 'FRONT'
        ),
        fallback AS (
            SELECT sp.symbol, sp.trade_date, sp.contract_month, sp.settlement,
                   ROW_NUMBER() OVER (PARTITION BY sp.symbol
                                      ORDER BY {_sort_key('sp.contract_month')}) AS rn
            FROM silver.futures_price sp
            JOIN latest l ON l.symbol = sp.symbol AND l.td = sp.trade_date
            WHERE sp.symbol NOT IN (SELECT symbol FROM front)
              AND {_sort_key('sp.contract_month')}
                  >= EXTRACT(YEAR FROM sp.trade_date)::int * 100
                     + EXTRACT(MONTH FROM sp.trade_date)::int
        ),
        best AS (
            SELECT symbol, trade_date, NULL::text AS contract_month, settlement
            FROM front
            UNION ALL
            SELECT symbol, trade_date, contract_month, settlement
            FROM fallback WHERE rn = 1
        ),
        label AS (
            -- which real contract the FRONT settle matches (display only)
            SELECT b.symbol, sp.contract_month,
                   ROW_NUMBER() OVER (PARTITION BY b.symbol
                                      ORDER BY {_sort_key('sp.contract_month')}) AS rn
            FROM best b
            JOIN silver.futures_price sp
              ON sp.symbol = b.symbol AND sp.trade_date = b.trade_date
             AND sp.contract_month <> 'FRONT' AND sp.settlement = b.settlement
            WHERE b.contract_month IS NULL
        ),
        prev AS (
            -- previous settle of the SAME contract, so the day change is
            -- never contaminated by a front-month roll (DC jumped 12 pct
            -- on 2026-07-31 purely from N26->U26 roll)
            SELECT sp.symbol, sp.settlement, sp.trade_date,
                   ROW_NUMBER() OVER (PARTITION BY sp.symbol
                                      ORDER BY sp.trade_date DESC) AS rn
            FROM silver.futures_price sp
            JOIN best b ON b.symbol = sp.symbol
            LEFT JOIN label lb ON lb.symbol = sp.symbol AND lb.rn = 1
            WHERE sp.contract_month = COALESCE(b.contract_month,
                                               lb.contract_month)
              AND sp.trade_date < b.trade_date
        )
        SELECT b.symbol, b.trade_date,
               COALESCE(b.contract_month, lb.contract_month, 'front')
                   AS contract_month,
               b.settlement, p.settlement AS prev_settle,
               p.trade_date AS prev_trade_date, g.overall_validation
        FROM best b
        LEFT JOIN label lb ON lb.symbol = b.symbol AND lb.rn = 1
        LEFT JOIN prev p ON p.symbol = b.symbol AND p.rn = 1
        LEFT JOIN gold.futures_daily_validated g
               ON g.symbol = b.symbol AND g.trade_date = b.trade_date
              AND g.contract_month = COALESCE(b.contract_month, 'FRONT')
    """)


@st.cache_data(ttl=300, show_spinner=False)
def get_front_month_series(days: int = 400) -> pd.DataFrame:
    """Front-month continuous settle series (FRONT rows) for all symbols."""
    return query_df("""
        SELECT symbol, trade_date, settlement
        FROM silver.futures_price
        WHERE contract_month = 'FRONT'
          AND trade_date >= CURRENT_DATE - (%(days)s * INTERVAL '1 day')
        ORDER BY symbol, trade_date
    """, {'days': days})


@st.cache_data(ttl=300, show_spinner=False)
def get_front_month_ohlc(symbol: str, days: int) -> pd.DataFrame:
    """Front-month OHLC for one symbol (FRONT rows carry full OHLC)."""
    return query_df("""
        SELECT trade_date, open_price, high_price, low_price, settlement
        FROM silver.futures_price
        WHERE symbol = %(sym)s AND contract_month = 'FRONT'
          AND trade_date >= CURRENT_DATE - (%(days)s * INTERVAL '1 day')
        ORDER BY trade_date
    """, {'sym': symbol, 'days': days})


# ── Projection comparison (gold.projection_comparison_long, migration 165) ──

@st.cache_data(ttl=3600, show_spinner=False)
def get_comparison_commodities() -> pd.DataFrame:
    """Commodity/country pairs that have at least one projection source
    (not realized-only), so the picker leads with comparable series."""
    return query_df("""
        SELECT commodity, country_code,
               COUNT(DISTINCT source_type) AS n_sources, COUNT(*) AS n_rows
        FROM gold.projection_comparison_long
        GROUP BY commodity, country_code
        ORDER BY (COUNT(DISTINCT source_type) FILTER
                      (WHERE source_type <> 'realized')) DESC,
                 COUNT(*) DESC
    """)


@st.cache_data(ttl=900, show_spinner=False)
def get_comparison_metrics(commodity: str, country: str) -> pd.DataFrame:
    return query_df("""
        SELECT metric, source_type, n_rows, n_vintages, latest_vintage,
               my_min, my_max
        FROM gold.projection_comparison_coverage
        WHERE commodity = %(c)s AND country_code = %(cc)s
    """, {'c': commodity, 'cc': country})


@st.cache_data(ttl=900, show_spinner=False)
def get_comparison_data(commodity: str, country: str, metric: str) -> pd.DataFrame:
    return query_df("""
        SELECT source_type, source_detail, vintage_date, is_latest,
               vintage_rank, marketing_year, value_native, unit_native,
               confidence_low, confidence_high, month_count, value_1000mt
        FROM gold.projection_comparison_long
        WHERE commodity = %(c)s AND country_code = %(cc)s AND metric = %(m)s
          AND marketing_year IS NOT NULL
        ORDER BY source_type, marketing_year, vintage_date
    """, {'c': commodity, 'cc': country, 'm': metric})


@st.cache_data(ttl=300, show_spinner=False)
def get_forward_curves(symbol: str) -> pd.DataFrame:
    """Forward curve (real contract months, chronological) at the latest
    trade date plus ghost curves ~1 week and ~4 weeks earlier."""
    return query_df(f"""
        WITH d AS (
            SELECT MAX(trade_date) AS d0 FROM silver.futures_price
            WHERE symbol = %(sym)s
        ),
        picks AS (
            SELECT d.d0,
                   (SELECT MAX(trade_date) FROM silver.futures_price
                    WHERE symbol = %(sym)s AND trade_date <= d.d0 - 7)  AS d1,
                   (SELECT MAX(trade_date) FROM silver.futures_price
                    WHERE symbol = %(sym)s AND trade_date <= d.d0 - 28) AS d2
            FROM d
        )
        SELECT fp.trade_date, fp.contract_month, fp.settlement,
               {SORT_KEY} AS contract_sort
        FROM silver.futures_price fp, picks
        WHERE fp.symbol = %(sym)s
          AND fp.trade_date IN (picks.d0, picks.d1, picks.d2)
          AND {SORT_KEY} IS NOT NULL
          AND {SORT_KEY} >= EXTRACT(YEAR FROM picks.d0)::int * 100
                            + EXTRACT(MONTH FROM picks.d0)::int
        ORDER BY contract_sort
    """, {'sym': symbol})
