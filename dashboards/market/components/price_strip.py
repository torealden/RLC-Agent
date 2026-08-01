"""
Always-on futures price strip. Rendered at the top of every page.

Honesty affordances:
  - badge "settle, as of <date>" (this is prior-session settle, never live)
  - amber badge when the freshest settle is more than 4 days old
  - stale symbols (e.g. FCPO) render dimmed with a "stale Nd" chip
"""
from datetime import date

import pandas as pd
import streamlit as st

from dashboards.market import db, theme

STALE_SYMBOL_DAYS = 5     # symbol lags the strip date -> dim + chip
STALE_STRIP_DAYS = 4      # strip badge turns amber (weekend-aware)


def _tile_html(sym: str, row, strip_date) -> str:
    meta = theme.SYMBOLS[sym]
    settle = theme.fmt_price(sym, row['settlement'])
    chg_html = ''
    if pd.notna(row.get('prev_settle')) and pd.notna(row.get('settlement')):
        chg = float(row['settlement']) - float(row['prev_settle'])
        pct = chg / float(row['prev_settle']) * 100 if float(row['prev_settle']) else 0.0
        color = theme.COLORS['positive'] if chg >= 0 else theme.COLORS['negative']
        sign = '+' if chg >= 0 else ''
        chg_html = (f"<span style='color:{color};font-size:0.78rem'>"
                    f"{sign}{chg:,.2f} ({sign}{pct:.1f}%)</span>")

    lag = (strip_date - row['trade_date']).days
    stale = lag > STALE_SYMBOL_DAYS
    opacity = '0.45' if stale else '1.0'
    stale_chip = (f"<span style='font-size:0.68rem;color:{theme.COLORS['neutral']}'>"
                  f" stale {lag}d</span>") if stale else ''
    # NOTE: gold.futures_daily_validated.overall_validation is NEEDS_REVIEW
    # on every row (validation pipeline never ran), so a per-tile validation
    # marker would be pure noise. Revisit if that pipeline goes live.
    return (
        f"<div style='opacity:{opacity};line-height:1.25'>"
        f"<span style='font-size:0.72rem;color:{theme.COLORS['neutral']}'>"
        f"{meta['name']} · {row['contract_month']}{stale_chip}</span><br>"
        f"<span style='font-size:1.05rem;font-weight:700'>{settle}</span>"
        f"<span style='font-size:0.65rem;color:{theme.COLORS['neutral']}'> "
        f"{meta['unit']}</span><br>{chg_html}"
        f"</div>"
    )


def render_price_strip() -> None:
    try:
        strip = db.get_price_strip()
    except Exception as e:
        st.warning(f'Price strip unavailable — database error: {e}')
        return
    if strip.empty:
        st.warning('Price strip unavailable — no rows in silver.futures_price.')
        return

    strip = strip.set_index('symbol')
    strip['trade_date'] = pd.to_datetime(strip['trade_date']).dt.date
    strip_date = strip['trade_date'].max()
    lag = (date.today() - strip_date).days

    badge_color = theme.COLORS['neutral'] if lag <= STALE_STRIP_DAYS else '#b8860b'
    badge_note = '' if lag <= STALE_STRIP_DAYS else f' — {lag} days old, check yfinance_futures collector'
    left, right = st.columns([5, 2])
    with right:
        st.markdown(
            f"<div style='text-align:right;font-size:0.75rem;color:{badge_color}'>"
            f"settle, as of {strip_date:%Y-%m-%d}{badge_note}</div>",
            unsafe_allow_html=True)

    for group in ('ag', 'energy'):
        syms = [s for s, m in theme.SYMBOLS.items()
                if m['group'] == group and s in strip.index]
        if not syms:
            continue
        cols = st.columns(9)  # widest group (ag) sets the grid for both rows
        for i, sym in enumerate(syms):
            with cols[i]:
                st.markdown(_tile_html(sym, strip.loc[sym], strip_date),
                            unsafe_allow_html=True)
    st.markdown('---')
