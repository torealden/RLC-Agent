"""
Markets page: front-month performance table, candlestick, forward curves.
"""
import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from dashboards.market import db, theme


def _pct_change_table() -> None:
    st.subheader('Front-Month Performance')
    df = db.get_front_month_series(days=400)
    if df.empty:
        st.info('No futures data available.')
        return
    df['settlement'] = pd.to_numeric(df['settlement'], errors='coerce')
    df['trade_date'] = pd.to_datetime(df['trade_date'])
    df = df.dropna(subset=['settlement'])

    # 1D change comes from the strip query (same-contract, roll-safe);
    # longer horizons use the continuous front-month series.
    strip = db.get_price_strip().set_index('symbol')

    rows = []
    for sym in theme.SYMBOLS:
        s = (df[df['symbol'] == sym]
             .sort_values('trade_date')
             .set_index('trade_date')['settlement'])
        if s.empty:
            continue
        last = s.iloc[-1]

        def pct_back(n_trading_days):
            if len(s) <= n_trading_days:
                return None
            base = s.iloc[-1 - n_trading_days]
            return (last / base - 1) * 100 if base else None

        one_day = None
        if sym in strip.index:
            r = strip.loc[sym]
            if pd.notna(r['prev_settle']) and float(r['prev_settle']):
                one_day = (float(r['settlement']) / float(r['prev_settle'])
                           - 1) * 100

        ytd_base_series = s[s.index.year == s.index[-1].year]
        ytd = ((last / ytd_base_series.iloc[0] - 1) * 100
               if len(ytd_base_series) > 1 else None)
        rows.append({
            'Market': f"{theme.symbol_name(sym)} ({sym})",
            'Last': theme.fmt_price(sym, last),
            'Unit': theme.SYMBOLS[sym]['unit'],
            '1D %': one_day, '5D %': pct_back(5),
            '30D %': pct_back(21), 'YTD %': ytd,
            'As of': s.index[-1].date().isoformat(),
        })

    table = pd.DataFrame(rows).set_index('Market')
    pct_cols = ['1D %', '5D %', '30D %', 'YTD %']

    def color_pct(v):
        if pd.isna(v):
            return ''
        return f"color: {theme.COLORS['positive'] if v >= 0 else theme.COLORS['negative']}"

    styled = (table.style
              .map(color_pct, subset=pct_cols)
              .format({c: lambda v: '—' if pd.isna(v) else f'{v:+.1f}%'
                       for c in pct_cols}))
    st.dataframe(styled, width='stretch')
    st.caption('1D is same-contract; 5D/30D/YTD use the continuous '
               'front-month series (contract rolls can add small jumps).')


def _candlestick() -> None:
    st.subheader('Daily Chart')
    c1, c2 = st.columns([2, 3])
    with c1:
        sym = st.selectbox('Market', list(theme.SYMBOLS),
                           format_func=theme.symbol_name, key='candle_sym')
    with c2:
        days = st.radio('Window', [60, 120, 250], horizontal=True,
                        format_func=lambda d: f'{d} days', key='candle_days')

    ohlc = db.get_front_month_ohlc(sym, days)
    if ohlc.empty:
        st.info(f'No front-month OHLC data for {theme.symbol_name(sym)} '
                '(FCPO has no FRONT series).')
        return
    for col in ('open_price', 'high_price', 'low_price', 'settlement'):
        ohlc[col] = pd.to_numeric(ohlc[col], errors='coerce')
    ohlc = ohlc.dropna(subset=['settlement'])
    has_ohlc = ohlc[['open_price', 'high_price', 'low_price']].notna().all(axis=1)

    fig = go.Figure()
    candles = ohlc[has_ohlc]
    if not candles.empty:
        fig.add_trace(go.Candlestick(
            x=candles['trade_date'], open=candles['open_price'],
            high=candles['high_price'], low=candles['low_price'],
            close=candles['settlement'],
            increasing_line_color=theme.COLORS['positive'],
            increasing_fillcolor=theme.COLORS['positive'],
            decreasing_line_color=theme.COLORS['negative'],
            decreasing_fillcolor=theme.COLORS['negative'],
            name=theme.symbol_name(sym)))
    else:
        fig.add_trace(go.Scatter(x=ohlc['trade_date'], y=ohlc['settlement'],
                                 mode='lines', name='Settle',
                                 line=dict(color=theme.COLORS['secondary'], width=2)))
    if len(ohlc) >= 10:
        ma = ohlc['settlement'].rolling(10).mean()
        fig.add_trace(go.Scatter(x=ohlc['trade_date'], y=ma, name='10-day MA',
                                 line=dict(color=theme.COLORS['gold'], width=1.5,
                                           dash='dot'), opacity=0.8))
    last = ohlc.iloc[-1]
    fig.add_annotation(x=last['trade_date'], y=float(last['settlement']),
                       text=f"{theme.fmt_price(sym, last['settlement'])}",
                       showarrow=True, arrowhead=0, ax=45, ay=-25,
                       font=dict(size=12))
    fig.update_layout(**theme.chart_layout(
        title=(f"{theme.symbol_name(sym)} — front month, {days} days "
               f"({theme.SYMBOLS[sym]['unit']})"),
        height=480, xaxis_rangeslider_visible=False, hovermode='x'))
    st.plotly_chart(fig, width='stretch')


def _forward_curves() -> None:
    st.subheader('Forward Curve')
    sym = st.selectbox('Market', list(theme.SYMBOLS),
                       format_func=theme.symbol_name, key='curve_sym')
    df = db.get_forward_curves(sym)
    if df.empty:
        st.info(f'No curve data for {theme.symbol_name(sym)}.')
        return
    df['settlement'] = pd.to_numeric(df['settlement'], errors='coerce')
    df = df.dropna(subset=['settlement'])
    df['trade_date'] = pd.to_datetime(df['trade_date']).dt.date

    # Category order for the x axis: contract months chronologically.
    order = (df.drop_duplicates('contract_month')
               .sort_values('contract_sort')['contract_month'].tolist())
    dates = sorted(df['trade_date'].unique(), reverse=True)
    styles = [
        dict(width=2.5, dash='solid'),
        dict(width=1.5, dash='dash'),
        dict(width=1.5, dash='dot'),
    ]
    opacities = [1.0, 0.55, 0.35]

    fig = go.Figure()
    for i, d in enumerate(dates[:3]):
        curve = df[df['trade_date'] == d].sort_values('contract_sort')
        label = f'{d:%b %d}' if i else f'{d:%b %d} (latest)'
        fig.add_trace(go.Scatter(
            x=curve['contract_month'], y=curve['settlement'],
            mode='lines+markers' if i == 0 else 'lines',
            name=label, opacity=opacities[i],
            line=dict(color=theme.COLORS['secondary'], **styles[i]),
            marker=dict(size=8)))
        if i == 0 and len(curve) >= 1:
            for idx in {0, len(curve) - 1}:
                r = curve.iloc[idx]
                fig.add_annotation(x=r['contract_month'], y=float(r['settlement']),
                                   text=theme.fmt_price(sym, r['settlement']),
                                   showarrow=False, yshift=14, font=dict(size=11))
    fig.update_layout(**theme.chart_layout(
        title=(f"{theme.symbol_name(sym)} forward curve "
               f"({theme.SYMBOLS[sym]['unit']}) — latest vs 1w / 4w ago"),
        height=380, hovermode='x',
        xaxis=dict(categoryorder='array', categoryarray=order,
                   gridcolor='rgba(128,128,128,0.10)')))
    st.plotly_chart(fig, width='stretch')


def render() -> None:
    _pct_change_table()
    st.markdown('---')
    _candlestick()
    st.markdown('---')
    _forward_curves()
