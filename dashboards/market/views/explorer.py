"""
Series Explorer: registry-driven interactive charting of any tracked
bronze/silver series. The catalog is dashboards/data/series_registry.py —
the same registry the data-inventory dashboard uses, so a series showing
up there shows up here.

Chart modes: Line, YoY %, Seasonal (year-over-year overlay). Rows sharing
a date are averaged and labeled as such (many tables carry sub-series like
states or contracts).
"""
from datetime import date

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from dashboards.data import series_registry as reg
from dashboards.market import db, theme

MAX_POINTS = 50_000


def _catalog():
    """[(schema, table, meta)] keyed by category."""
    out = {}
    for schema, registry in (('bronze', reg.BRONZE_REGISTRY),
                             ('silver', reg.SILVER_REGISTRY)):
        for table, meta in registry.items():
            out.setdefault(meta.get('category', 'other'), []).append(
                (schema, table, meta))
    return out


def _date_expr(meta) -> str | None:
    """Vetted SQL date expression from registry metadata (registry values
    are checked-in code, not user input)."""
    dt = meta.get('date_type')
    if dt == 'date':
        return f'"{meta["date_col"]}"::date'
    if dt == 'year':
        return f'make_date("{meta["date_col"]}"::int, 1, 1)'
    if dt == 'year_month':
        return (f'make_date("{meta["year_col"]}"::int, '
                f'"{meta["month_col"]}"::int, 1)')
    if dt == 'crop_year':
        return f'"{meta["date_col"]}"'   # text label, categorical axis
    return None


def _freshness_chip(meta, last_obs) -> None:
    freq = meta.get('expected_frequency', 'on_demand')
    limit = reg.FRESHNESS_THRESHOLDS.get(freq)
    if last_obs is None or limit is None or not hasattr(last_obs, 'year'):
        st.caption(f'Latest: {last_obs} · update cadence: {freq}')
        return
    age = (date.today() - last_obs).days
    ok = age <= limit
    color = theme.COLORS['positive'] if ok else theme.COLORS['negative']
    word = 'Fresh' if ok else f'STALE ({age}d, expected {freq})'
    st.markdown(f"<span style='color:{color}'>●</span> {word} · "
                f"latest observation {last_obs}", unsafe_allow_html=True)


def render() -> None:
    catalog = _catalog()
    cat_order = sorted(catalog,
                       key=lambda c: reg.CATEGORIES.get(c, (c, 99))[1])

    c1, c2, c3 = st.columns([2, 3, 2])
    with c1:
        category = st.selectbox(
            'Category', cat_order,
            format_func=lambda c: reg.CATEGORIES.get(c, (c.title(),))[0],
            key='ex_cat')
    tables = sorted(catalog[category], key=lambda t: t[2]['display_name'])
    with c2:
        schema, table, meta = st.selectbox(
            'Series', tables,
            format_func=lambda t: f"{t[2]['display_name']}  [{t[0]}]",
            key='ex_table')
    value_cols = db.get_numeric_columns(schema, table)
    if not value_cols:
        st.info('No numeric columns found for this table.')
        return
    with c3:
        value_col = st.selectbox('Measure', value_cols, key='ex_value')

    commodity_col = meta.get('commodity_col')
    commodity = None
    if commodity_col:
        options = db.get_distinct_values(schema, table, commodity_col)
        if options:
            commodity = st.selectbox(commodity_col.replace('_', ' ').title(),
                                     options, key='ex_commodity')

    date_expr = _date_expr(meta)
    if date_expr is None:
        st.info('This table has no temporal column — showing a preview.')
        st.dataframe(db.query_df(
            f'SELECT * FROM {schema}.{table} LIMIT 200'), width='stretch')
        return

    df = db.get_series(schema, table, date_expr, value_col,
                       commodity_col, commodity)
    if df.empty:
        st.info('No rows for this selection.')
        return

    is_crop_year = meta.get('date_type') == 'crop_year'
    if not is_crop_year:
        df['obs'] = pd.to_datetime(df['obs'])
        if len(df) > MAX_POINTS:
            df = (df.set_index('obs').resample('MS')['value'].mean()
                  .dropna().reset_index())
            st.caption(f'{len(df):,} points after monthly aggregation '
                       f'(raw series exceeded {MAX_POINTS:,}).')
    df['value'] = pd.to_numeric(df['value'], errors='coerce')
    df = df.dropna(subset=['value'])

    if df.get('n_rows') is not None and (df['n_rows'] > 1).any():
        st.caption('Multiple rows per date (e.g. states/contracts) are '
                   'averaged — pick a narrower filter for a single series.')

    last_obs = df['obs'].iloc[-1]
    _freshness_chip(meta, last_obs.date() if hasattr(last_obs, 'date')
                    else last_obs)

    freq = meta.get('expected_frequency', 'on_demand')
    modes = ['Line', 'YoY %']
    if not is_crop_year and freq in ('daily', 'weekly', 'monthly'):
        modes.append('Seasonal')
    mode = st.radio('Chart', modes, horizontal=True, key='ex_mode')

    title = f"{meta['display_name']} — {value_col}" + (
        f' · {commodity}' if commodity else '')

    fig = go.Figure()
    if mode == 'Line':
        fig.add_trace(go.Scatter(x=df['obs'], y=df['value'], mode='lines',
                                 line=dict(color=theme.COLORS['secondary'],
                                           width=2), name=value_col))
        last = df.iloc[-1]
        fig.add_annotation(x=last['obs'], y=float(last['value']),
                           text=f"{float(last['value']):,.1f}",
                           showarrow=True, arrowhead=0, ax=40, ay=-25,
                           font=dict(size=11))
    elif mode == 'YoY %':
        if is_crop_year:
            yoy = df.set_index('obs')['value'].pct_change() * 100
        else:
            monthly = df.set_index('obs')['value'].resample('MS').mean()
            periods = {'daily': 12, 'weekly': 12, 'monthly': 12,
                       'quarterly': 4, 'annual': 1}.get(freq, 1)
            yoy = (monthly.pct_change(periods) * 100
                   if freq in ('daily', 'weekly', 'monthly')
                   else df.set_index('obs')['value'].pct_change(periods) * 100)
        yoy = yoy.dropna()
        colors = [theme.COLORS['positive'] if v >= 0
                  else theme.COLORS['negative'] for v in yoy]
        fig.add_trace(go.Bar(x=yoy.index, y=yoy.values,
                             marker_color=colors, name='YoY %'))
        fig.add_hline(y=0, line_color='rgba(128,128,128,0.4)')
        title += ' · year-over-year %'
    else:  # Seasonal
        df = df.assign(yr=df['obs'].dt.year)
        years = sorted(df['yr'].unique())[-6:]
        for i, yr in enumerate(years):
            sub = df[df['yr'] == yr]
            x = (sub['obs'].dt.dayofyear if freq in ('daily', 'weekly')
                 else sub['obs'].dt.month)
            current = yr == years[-1]
            fig.add_trace(go.Scatter(
                x=x, y=sub['value'], name=str(yr), mode='lines',
                line=dict(color=theme.COMMODITY_COLORS[i % 10],
                          width=3 if current else 1.5),
                opacity=1.0 if current else 0.6))
        fig.update_xaxes(title='day of year' if freq in ('daily', 'weekly')
                         else 'month',
                         dtick=1 if freq == 'monthly' else None)
        title += ' · seasonal overlay (last 6 years)'

    fig.update_layout(**theme.chart_layout(title=title, height=460))
    st.plotly_chart(fig, width='stretch')

    st.download_button(
        'Download CSV',
        df.to_csv(index=False).encode(),
        file_name=f'{table}_{value_col}.csv', mime='text/csv',
        key='ex_download')
