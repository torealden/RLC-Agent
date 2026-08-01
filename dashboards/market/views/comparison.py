"""
Projection Comparison page: Tore's estimates vs LLM forecasts vs USDA WASDE
vintages, against realized actuals, per (commodity, country, metric).

Reads gold.projection_comparison_long / _coverage (migration 165).

Honesty rules baked in:
  - a selected source with zero rows shows a grayed chip + info box, never a
    silently missing line
  - mixed units are never plotted on one axis: sources convert to 1000 MT or
    are pulled from the chart with an explicit banner (still in the table)
  - the WASDE revision path is labeled as 2026-only vintage depth
"""
import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from dashboards.market import db, theme

SOURCES = ['user', 'llm', 'usda']   # realized always shown when present


def _pickers():
    pairs = db.get_comparison_commodities()
    if pairs.empty:
        st.error('gold.projection_comparison_long returned no rows — has '
                 'migration 165 been applied?')
        return None

    c1, c2, c3, c4 = st.columns([2, 1, 2, 2])
    with c1:
        commodity = st.selectbox('Commodity',
                                 pairs['commodity'].unique().tolist(),
                                 key='cmp_commodity')
    countries = (pairs[pairs['commodity'] == commodity]['country_code']
                 .tolist())
    with c2:
        default_ix = countries.index('US') if 'US' in countries else 0
        country = st.selectbox('Country', countries, index=default_ix,
                               key='cmp_country')
    cov = db.get_comparison_metrics(commodity, country)
    metrics = sorted(cov['metric'].unique().tolist())
    with c3:
        preferred = [m for m in ('ending_stocks', 'production', 'crush')
                     if m in metrics]
        metric = st.selectbox('Metric', metrics,
                              index=metrics.index(preferred[0]) if preferred else 0,
                              key='cmp_metric')
    with c4:
        chosen = st.multiselect(
            'Projection sources',
            SOURCES, default=SOURCES,
            format_func=lambda s: theme.SOURCE_LABELS[s],
            key='cmp_sources')
    return commodity, country, metric, chosen, cov


def _coverage_chips(cov: pd.DataFrame, metric: str, chosen: list) -> None:
    cols = st.columns(4)
    for i, src in enumerate(['realized'] + SOURCES):
        row = cov[(cov['metric'] == metric) & (cov['source_type'] == src)]
        with cols[i]:
            label = theme.SOURCE_LABELS[src]
            if row.empty:
                st.markdown(
                    f"<span style='color:{theme.COLORS['neutral']}'>"
                    f"○ <b>{label}</b>: no data</span>",
                    unsafe_allow_html=True)
            else:
                r = row.iloc[0]
                color = theme.SOURCE_COLORS[src]
                st.markdown(
                    f"<span style='color:{color}'>●</span> <b>{label}</b>: "
                    f"{int(r['n_vintages'])} vintage(s) · "
                    f"MY {theme.my_label(int(r['my_min']))}–"
                    f"{theme.my_label(int(r['my_max']))} · "
                    f"latest {r['latest_vintage']}",
                    unsafe_allow_html=True)
    missing = [theme.SOURCE_LABELS[s] for s in chosen
               if cov[(cov['metric'] == metric)
                      & (cov['source_type'] == s)].empty]
    if missing:
        st.info(f"No data recorded for: {', '.join(missing)}. "
                'Your estimates load via scripts/harvest_user_estimates.py; '
                'LLM forecasts land in core.forecasts when forecast '
                'generation runs. The chart shows what exists.')


def _resolve_units(df: pd.DataFrame):
    """Decide the display basis. Returns (df with 'display_value',
    unit_label, list of excluded-source messages)."""
    units = df.groupby('source_type')['unit_native'].agg(
        lambda s: s.dropna().unique().tolist())
    flat = sorted({u for lst in units for u in lst})
    if len(flat) <= 1:
        df = df.assign(display_value=df['value_native'])
        return df, (flat[0] if flat else ''), []

    # mixed native units -> standardize to 1000 MT where possible
    excluded = []
    conv_ok = df['value_1000mt'].notna()
    for src in df['source_type'].unique():
        sub = df[df['source_type'] == src]
        if sub['value_1000mt'].isna().all():
            u = ', '.join(sub['unit_native'].dropna().unique())
            excluded.append(
                f"{theme.SOURCE_LABELS.get(src, src)} ({u} — no safe "
                'conversion to 1000 MT)')
    df = df[conv_ok].assign(display_value=df.loc[conv_ok, 'value_1000mt'])
    return df, '1000 MT', excluded


def _overlay_chart(df: pd.DataFrame, metric: str, chosen: list,
                   show_vintages: bool, unit_label: str) -> None:
    fig = go.Figure()
    my_all = sorted(df['marketing_year'].unique())

    def x_of(sub):
        return [theme.my_label(int(m)) for m in sub['marketing_year']]

    # realized: heavy neutral reference line
    rl = df[df['source_type'] == 'realized'].sort_values('marketing_year')
    if not rl.empty:
        partial = rl['month_count'].fillna(12) < 12
        fig.add_trace(go.Scatter(
            x=x_of(rl), y=rl['display_value'], name=theme.SOURCE_LABELS['realized'],
            mode='lines+markers',
            line=dict(color=theme.SOURCE_COLORS['realized'], width=3),
            marker=dict(size=8,
                        symbol=['circle-open' if p else 'circle'
                                for p in partial]),
            customdata=rl['month_count'].fillna(12).astype(int),
            hovertemplate='%{x}: %{y:,.0f} (%{customdata} months)<extra>Realized</extra>'))

    # user: current vintage solid diamonds, older vintages faded points
    if 'user' in chosen:
        u = df[df['source_type'] == 'user']
        cur = u[u['is_latest']].sort_values('marketing_year')
        old = u[~u['is_latest']]
        if not cur.empty:
            fig.add_trace(go.Scatter(
                x=x_of(cur), y=cur['display_value'],
                name=theme.SOURCE_LABELS['user'], mode='lines+markers',
                line=dict(color=theme.SOURCE_COLORS['user'], width=2),
                marker=dict(size=10, symbol='diamond'),
                customdata=cur['vintage_date'],
                hovertemplate='%{x}: %{y:,.0f} (vintage %{customdata})<extra>Mine</extra>'))
        if not old.empty:
            fig.add_trace(go.Scatter(
                x=x_of(old), y=old['display_value'], name='Mine (older vintages)',
                mode='markers', opacity=0.35,
                marker=dict(size=7, symbol='diamond',
                            color=theme.SOURCE_COLORS['user']),
                customdata=old['vintage_date'],
                hovertemplate='%{x}: %{y:,.0f} (vintage %{customdata})<extra>Mine, older</extra>'))

    # llm: line + confidence band when present
    if 'llm' in chosen:
        l = df[df['source_type'] == 'llm'].sort_values('marketing_year')
        if not l.empty:
            if l['confidence_low'].notna().any():
                fig.add_trace(go.Scatter(
                    x=x_of(l) + x_of(l)[::-1],
                    y=list(l['confidence_high']) + list(l['confidence_low'])[::-1],
                    fill='toself', fillcolor='rgba(46,117,182,0.15)',
                    line=dict(width=0), showlegend=False, hoverinfo='skip'))
            fig.add_trace(go.Scatter(
                x=x_of(l), y=l['display_value'], name=theme.SOURCE_LABELS['llm'],
                mode='lines+markers',
                line=dict(color=theme.SOURCE_COLORS['llm'], width=2, dash='dash'),
                marker=dict(size=8, symbol='square')))

    # usda: latest per MY; optional per-release revision spaghetti
    if 'usda' in chosen:
        w = df[df['source_type'] == 'usda']
        latest = w[w['is_latest']].sort_values('marketing_year')
        if not latest.empty:
            fig.add_trace(go.Scatter(
                x=x_of(latest), y=latest['display_value'],
                name=theme.SOURCE_LABELS['usda'], mode='lines+markers',
                line=dict(color=theme.SOURCE_COLORS['usda'], width=2),
                marker=dict(size=8, symbol='triangle-up'),
                customdata=latest['source_detail'],
                hovertemplate='%{x}: %{y:,.0f} (%{customdata})<extra>USDA</extra>'))
        if show_vintages:
            for vint, grp in w[~w['is_latest']].groupby('source_detail'):
                grp = grp.sort_values('marketing_year')
                fig.add_trace(go.Scatter(
                    x=x_of(grp), y=grp['display_value'], name=str(vint),
                    mode='lines', opacity=0.30, showlegend=False,
                    line=dict(color=theme.SOURCE_COLORS['usda'], width=1),
                    hovertemplate='%{x}: %{y:,.0f}<extra>' + str(vint) + '</extra>'))

    # annotate the latest value of each primary trace
    for src, sub in [(s, df[(df['source_type'] == s)
                            & df['is_latest']]) for s in
                     ['realized'] + [c for c in chosen if c in
                                     df['source_type'].unique()]]:
        sub = sub.dropna(subset=['display_value'])
        if sub.empty:
            continue
        r = sub.loc[sub['marketing_year'].idxmax()]
        fig.add_annotation(
            x=theme.my_label(int(r['marketing_year'])), y=float(r['display_value']),
            text=f"{float(r['display_value']):,.0f}", showarrow=False,
            yshift=14, font=dict(size=11, color=theme.SOURCE_COLORS[src]))

    fig.update_layout(**theme.chart_layout(
        title=f'{metric} by marketing year ({unit_label})',
        height=480,
        xaxis=dict(categoryorder='array',
                   categoryarray=[theme.my_label(int(m)) for m in my_all],
                   gridcolor='rgba(128,128,128,0.10)')))
    st.plotly_chart(fig, width='stretch')


def _divergence_table(df: pd.DataFrame, chosen: list, unit_label: str) -> None:
    st.subheader('Divergence vs realized')
    latest = df[df['is_latest']]
    pivot = latest.pivot_table(index='marketing_year', columns='source_type',
                               values='display_value', aggfunc='first')
    if pivot.empty:
        st.info('Nothing to tabulate for this selection.')
        return
    cols = [c for c in ['realized', 'user', 'llm', 'usda']
            if c in pivot.columns and (c == 'realized' or c in chosen)]
    pivot = pivot[cols]
    if 'realized' in pivot.columns:
        for src in [c for c in cols if c != 'realized']:
            pivot[f'{src} Δ%'] = ((pivot[src] / pivot['realized'] - 1) * 100)
    pivot.index = [theme.my_label(int(m)) for m in pivot.index]
    pivot = pivot.rename(columns=theme.SOURCE_LABELS)

    delta_cols = [c for c in pivot.columns if 'Δ%' in str(c)]

    def color_delta(v):
        if pd.isna(v):
            return ''
        return f"color: {theme.COLORS['positive'] if v >= 0 else theme.COLORS['negative']}"

    styled = (pivot.sort_index(ascending=False).style
              .map(color_delta, subset=delta_cols)
              .format('{:,.0f}', subset=[c for c in pivot.columns
                                         if c not in delta_cols], na_rep='—')
              .format('{:+.1f}%', subset=delta_cols, na_rep='—'))
    st.dataframe(styled, width='stretch')
    st.caption(f'Values in {unit_label}. Δ% is each source\'s current '
               'vintage vs realized for that marketing year.')


def render() -> None:
    picked = _pickers()
    if picked is None:
        return
    commodity, country, metric, chosen, cov = picked

    _coverage_chips(cov, metric, chosen)

    df = db.get_comparison_data(commodity, country, metric)
    if df.empty:
        st.warning('No rows at all for this series — nothing to chart.')
        return
    df = df[(df['source_type'] == 'realized') | df['source_type'].isin(chosen)]
    df['value_native'] = pd.to_numeric(df['value_native'], errors='coerce')
    df['value_1000mt'] = pd.to_numeric(df['value_1000mt'], errors='coerce')

    my_min = int(df['marketing_year'].min())
    my_max = int(df['marketing_year'].max())
    if my_max > my_min:
        lo, hi = st.slider('Marketing year range', my_min, my_max,
                           (max(my_min, my_max - 12), my_max),
                           format='%d', key='cmp_my_range')
        df = df[df['marketing_year'].between(lo, hi)]

    df, unit_label, excluded = _resolve_units(df)
    if excluded:
        st.warning('Units differ across sources; showing 1000 MT. Dropped '
                   'from this page (no safe conversion — view natively in '
                   'the Series Explorer): ' + '; '.join(excluded))

    has_vintage_path = (df['source_type'].eq('usda')
                        & ~df['is_latest']).any()
    show_vintages = False
    if 'usda' in chosen and has_vintage_path:
        show_vintages = st.checkbox('Show WASDE revision path', value=False,
                                    key='cmp_vintages')
        if show_vintages:
            st.caption('WASDE vintage depth is 2026-only (releases Jan–Jul '
                       '2026); earlier marketing years show the FINAL value '
                       'only.')

    _overlay_chart(df, metric, chosen, show_vintages, unit_label)
    _divergence_table(df, chosen, unit_label)
