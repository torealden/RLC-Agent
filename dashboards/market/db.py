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
  - Full contract curves exist only since 2026-03-03 (DC/ZR: 2026-07-13);
    earlier history is the FRONT continuous series alone.

ROLL CONVENTION (Tore, 2026-08-01): roll on the FIRST BUSINESS DAY OF THE
CONTRACT MONTH — i.e. the front month is the nearest listed contract whose
delivery month is strictly after the current calendar month. Rationale:
after first notice day the front is a couple of cash players trading local
supply/demand and basis, so its price discovers something different from a
futures market. The yfinance FRONT series does NOT follow this rule (on
2026-07-31 it had ZS at X26 while this rule says Q26, and rode expiring DC
into its settlement period), so FRONT is used only as the pre-2026-03
historical splice, labeled 'continuous' in the roll column.
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

# RLC front month: nearest listed contract with delivery month STRICTLY
# after the trade date's calendar month (== rolled on the first business
# day of the contract month).
_RLC_FRONT_FILTER = (f"contract_month <> 'FRONT' AND {SORT_KEY} > "
                     "EXTRACT(YEAR FROM trade_date)::int * 100 "
                     "+ EXTRACT(MONTH FROM trade_date)::int")


@st.cache_data(ttl=300, show_spinner=False)
def get_price_strip() -> pd.DataFrame:
    """Latest RLC-front settle per symbol + previous settle of the SAME
    contract for the day change (a continuous-series diff picks up roll
    jumps — DC 'moved' 12 pct on 2026-07-31 purely from a roll)."""
    return query_df(f"""
        WITH latest AS (
            SELECT symbol, MAX(trade_date) AS td
            FROM silver.futures_price
            WHERE contract_month <> 'FRONT'
            GROUP BY symbol
        ),
        ranked AS (
            SELECT sp.symbol, sp.trade_date, sp.contract_month, sp.settlement,
                   ROW_NUMBER() OVER (PARTITION BY sp.symbol
                                      ORDER BY {_sort_key('sp.contract_month')}) AS rn
            FROM silver.futures_price sp
            JOIN latest l ON l.symbol = sp.symbol AND l.td = sp.trade_date
            WHERE sp.contract_month <> 'FRONT'
              AND {_sort_key('sp.contract_month')}
                  > EXTRACT(YEAR FROM sp.trade_date)::int * 100
                    + EXTRACT(MONTH FROM sp.trade_date)::int
        ),
        best AS (SELECT * FROM ranked WHERE rn = 1),
        prev AS (
            SELECT sp.symbol, sp.settlement, sp.trade_date,
                   ROW_NUMBER() OVER (PARTITION BY sp.symbol
                                      ORDER BY sp.trade_date DESC) AS rn
            FROM silver.futures_price sp
            JOIN best b ON b.symbol = sp.symbol
                       AND b.contract_month = sp.contract_month
            WHERE sp.trade_date < b.trade_date
        )
        SELECT b.symbol, b.trade_date, b.contract_month, b.settlement,
               p.settlement AS prev_settle, p.trade_date AS prev_trade_date
        FROM best b
        LEFT JOIN prev p ON p.symbol = b.symbol AND p.rn = 1
    """)


@st.cache_data(ttl=300, show_spinner=False)
def get_front_month_series(days: int = 400) -> pd.DataFrame:
    """Front-month settle series for all symbols: RLC roll where contract
    curves exist (since 2026-03-03; DC/ZR 2026-07-13), spliced with the
    yfinance FRONT continuous before that. roll column says which."""
    return query_df(f"""
        WITH rlc AS (
            SELECT symbol, trade_date, contract_month, settlement,
                   ROW_NUMBER() OVER (PARTITION BY symbol, trade_date
                                      ORDER BY {SORT_KEY}) AS rn
            FROM silver.futures_price
            WHERE {_RLC_FRONT_FILTER}
              AND trade_date >= CURRENT_DATE - (%(days)s * INTERVAL '1 day')
        ),
        rlc1 AS (SELECT * FROM rlc WHERE rn = 1)
        SELECT symbol, trade_date, settlement, 'rlc' AS roll FROM rlc1
        UNION ALL
        SELECT f.symbol, f.trade_date, f.settlement, 'continuous'
        FROM silver.futures_price f
        WHERE f.contract_month = 'FRONT'
          AND f.trade_date >= CURRENT_DATE - (%(days)s * INTERVAL '1 day')
          AND NOT EXISTS (SELECT 1 FROM rlc1 r
                          WHERE r.symbol = f.symbol
                            AND r.trade_date = f.trade_date)
        ORDER BY symbol, trade_date
    """, {'days': days})


@st.cache_data(ttl=300, show_spinner=False)
def get_front_month_ohlc(symbol: str, days: int) -> pd.DataFrame:
    """Front-month OHLC for one symbol: RLC roll where curves exist,
    FRONT continuous splice before that (roll column says which)."""
    return query_df(f"""
        WITH rlc AS (
            SELECT trade_date, open_price, high_price, low_price, settlement,
                   contract_month,
                   ROW_NUMBER() OVER (PARTITION BY trade_date
                                      ORDER BY {SORT_KEY}) AS rn
            FROM silver.futures_price
            WHERE symbol = %(sym)s AND {_RLC_FRONT_FILTER}
              AND trade_date >= CURRENT_DATE - (%(days)s * INTERVAL '1 day')
        ),
        rlc1 AS (SELECT * FROM rlc WHERE rn = 1)
        SELECT trade_date, open_price, high_price, low_price, settlement,
               contract_month, 'rlc' AS roll
        FROM rlc1
        UNION ALL
        SELECT f.trade_date, f.open_price, f.high_price, f.low_price,
               f.settlement, f.contract_month, 'continuous'
        FROM silver.futures_price f
        WHERE f.symbol = %(sym)s AND f.contract_month = 'FRONT'
          AND f.trade_date >= CURRENT_DATE - (%(days)s * INTERVAL '1 day')
          AND NOT EXISTS (SELECT 1 FROM rlc1 r
                          WHERE r.trade_date = f.trade_date)
        ORDER BY trade_date
    """, {'sym': symbol, 'days': days})


# ── Series explorer (registry-driven; identifiers come from the checked-in
#    dashboards/data/series_registry.py, never from user input) ──────────────

_ID_LIKE = {'id', 'year', 'month', 'calendar_year', 'marketing_year',
            'crop_year', 'week', 'ingest_run_id'}


@st.cache_data(ttl=3600, show_spinner=False)
def get_numeric_columns(schema: str, table: str) -> list:
    df = query_df("""
        SELECT column_name FROM information_schema.columns
        WHERE table_schema = %(s)s AND table_name = %(t)s
          AND data_type IN ('integer', 'bigint', 'numeric', 'real',
                            'double precision', 'smallint')
        ORDER BY ordinal_position
    """, {'s': schema, 't': table})
    return [c for c in df['column_name']
            if c not in _ID_LIKE and not c.endswith('_id')]


@st.cache_data(ttl=3600, show_spinner=False)
def get_distinct_values(schema: str, table: str, col: str) -> list:
    df = query_df(
        f'SELECT DISTINCT "{col}" AS v FROM {schema}.{table} '
        f'WHERE "{col}" IS NOT NULL ORDER BY 1 LIMIT 500')
    return df['v'].tolist()


@st.cache_data(ttl=900, show_spinner=False)
def get_series(schema: str, table: str, date_expr: str, value_col: str,
               commodity_col: str | None, commodity) -> pd.DataFrame:
    """Generic (obs_date, value) pull. date_expr is one of the vetted
    expressions built in explorer.py from registry metadata. Rows sharing a
    date are averaged (mixed sub-series, e.g. states or contracts)."""
    where = f'WHERE "{value_col}" IS NOT NULL'
    params = {}
    if commodity_col and commodity is not None:
        where += f' AND "{commodity_col}" = %(commodity)s'
        params['commodity'] = commodity
    return query_df(f"""
        SELECT {date_expr} AS obs, AVG("{value_col}") AS value,
               COUNT(*) AS n_rows
        FROM {schema}.{table}
        {where}
        GROUP BY 1 ORDER BY 1
    """, params)


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
          -- RLC roll convention: curve starts at the front month, i.e. the
          -- nearest contract with delivery month strictly after today's
          AND {SORT_KEY} > EXTRACT(YEAR FROM picks.d0)::int * 100
                           + EXTRACT(MONTH FROM picks.d0)::int
        ORDER BY contract_sort
    """, {'sym': symbol})
