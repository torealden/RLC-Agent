"""
Global Oilseeds Supply & Demand Dashboard Page
===============================================
Visualizes global soybean and oilseed balance sheets from USDA FAS PSD data.

Called from app.py as a page function.
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots


# Country display names and colors
COUNTRY_NAMES = {
    # Aggregates
    'WD': 'World', 'E4': 'EU-27', 'E2': 'EU-15', 'E3': 'EU-25',
    # Americas
    'US': 'United States', 'BR': 'Brazil', 'AR': 'Argentina',
    'CA': 'Canada', 'MX': 'Mexico', 'CO': 'Colombia', 'UY': 'Uruguay',
    'PA': 'Paraguay', 'BL': 'Bolivia', 'BO': 'Bolivia', 'VE': 'Venezuela',
    'PE': 'Peru', 'EC': 'Ecuador', 'CL': 'Chile', 'GY': 'Guyana',
    'GT': 'Guatemala', 'DR': 'Dominican Republic', 'HO': 'Honduras',
    'NU': 'Nicaragua', 'CS': 'Costa Rica', 'PM': 'Panama', 'HA': 'Haiti',
    'CU': 'Cuba', 'JM': 'Jamaica',
    # Europe
    'TU': 'Turkey', 'UK': 'United Kingdom', 'FR': 'France', 'GM': 'Germany',
    'IT': 'Italy', 'SP': 'Spain', 'PL': 'Poland', 'RO': 'Romania',
    'BU': 'Bulgaria', 'HU': 'Hungary', 'GR': 'Greece', 'CZ': 'Czech Republic',
    'AU': 'Austria', 'BE': 'Belgium', 'NL': 'Netherlands', 'PO': 'Portugal',
    'SW': 'Sweden', 'DA': 'Denmark', 'FI': 'Finland', 'EI': 'Ireland',
    'HR': 'Croatia', 'SR': 'Serbia', 'BK': 'Bosnia', 'AL': 'Albania',
    'EN': 'Estonia', 'LG': 'Latvia', 'LH': 'Lithuania', 'SZ': 'Switzerland',
    'NO': 'Norway', 'IC': 'Iceland',
    # FSU
    'RS': 'Russia', 'UP': 'Ukraine', 'BO': 'Belarus', 'KZ': 'Kazakhstan',
    'UZ': 'Uzbekistan', 'GG': 'Georgia', 'AJ': 'Azerbaijan', 'AM': 'Armenia',
    'KG': 'Kyrgyzstan', 'TI': 'Tajikistan', 'TX': 'Turkmenistan', 'MD': 'Moldova',
    # East Asia
    'CH': 'China', 'CN': 'China', 'JA': 'Japan', 'KS': 'South Korea',
    'KN': 'North Korea', 'TW': 'Taiwan', 'HK': 'Hong Kong', 'MG': 'Mongolia',
    # Southeast Asia
    'ID': 'Indonesia', 'MY': 'Malaysia', 'TH': 'Thailand', 'VM': 'Vietnam',
    'BM': 'Burma (Myanmar)', 'RP': 'Philippines', 'CB': 'Cambodia', 'LA': 'Laos',
    # South Asia
    'IN': 'India', 'PK': 'Pakistan', 'BG': 'Bangladesh', 'CE': 'Sri Lanka',
    'NP': 'Nepal', 'BT': 'Bhutan',
    # Oceania
    'AS': 'Australia', 'NZ': 'New Zealand', 'FJ': 'Fiji',
    # Middle East
    'IR': 'Iran', 'IZ': 'Iraq', 'SA': 'Saudi Arabia', 'IS': 'Israel',
    'JO': 'Jordan', 'LE': 'Lebanon', 'SY': 'Syria', 'KU': 'Kuwait',
    'AE': 'UAE', 'YM': 'Yemen', 'BA': 'Bahrain', 'QA': 'Qatar',
    # North Africa
    'EG': 'Egypt', 'MO': 'Morocco', 'AG': 'Algeria', 'TS': 'Tunisia',
    'LY': 'Libya',
    # Sub-Saharan Africa
    'SF': 'South Africa', 'ZA': 'South Africa', 'NI': 'Nigeria', 'NG': 'Nigeria',
    'ET': 'Ethiopia', 'KE': 'Kenya', 'GH': 'Ghana', 'TZ': 'Tanzania',
    'UG': 'Uganda', 'MZ': 'Mozambique', 'ZI': 'Zimbabwe', 'SU': 'Sudan',
    'AO': 'Angola', 'SN': 'Senegal', 'ML': 'Mali', 'BF': 'Burkina Faso',
    'IV': 'Ivory Coast', 'CM': 'Cameroon', 'CG': 'Congo (Kinshasa)',
    'CF': 'Congo (Brazzaville)', 'RW': 'Rwanda', 'RB': 'Rwanda',
    'DM': 'Benin', 'GN': 'Guinea', 'ER': 'Eritrea', 'GA': 'Gambia',
    'DJ': 'Djibouti', 'CT': 'Central African Republic', 'BD': 'Burundi',
    'CD': 'Chad', 'GB': 'Gabon', 'GI': 'Guinea-Bissau', 'GC': 'Grenada',
}

COUNTRY_COLORS = {
    'United States': '#1f4e79', 'Brazil': '#548235', 'Argentina': '#2e75b6',
    'China': '#c00000', 'Paraguay': '#7030a0', 'India': '#f4b942',
    'Russia': '#8B4513', 'Canada': '#c55a11', 'EU-27': '#4472c4',
    'Ukraine': '#ffc000', 'Australia': '#70ad47', 'World': '#333333',
    'Indonesia': '#e06c2e', 'Malaysia': '#2e75b6',
}


def _get_latest_by_country(df):
    """Keep only the latest report_date per country/marketing_year."""
    if 'report_date' in df.columns:
        return df.sort_values('report_date').groupby(
            ['country_code', 'marketing_year'], as_index=False
        ).last()
    return df


def page_global_oilseeds(query_fn):
    """Main page function for Global Oilseeds S&D."""

    st.title("Global Oilseeds Supply & Demand")
    st.caption("Source: USDA FAS Production, Supply & Distribution (PSD) | Units: 1,000 MT")

    # ── Commodity selector ─────────────────────────────────────────
    commodity_options = {
        'Soybeans': 'soybeans',
        'Corn': 'corn',
        'Wheat': 'wheat',
        'Rapeseed/Canola': 'rapeseed',
        'Palm Oil': 'palm_oil',
        'Cotton': 'cotton',
    }

    col_sel1, col_sel2 = st.columns([2, 2])
    with col_sel1:
        selected_name = st.selectbox("Commodity", list(commodity_options.keys()), index=0)
        commodity = commodity_options[selected_name]
    with col_sel2:
        my_range = st.slider("Marketing Years", 2020, 2025, (2021, 2025))

    # ── Pull data ──────────────────────────────────────────────────
    df_raw = query_fn("""
        SELECT country_code, country, marketing_year, report_date,
               area_harvested, production, imports, total_supply,
               domestic_consumption, exports, ending_stocks, total_distribution,
               crush
        FROM bronze.fas_psd
        WHERE commodity = %(commodity)s
          AND marketing_year BETWEEN %(my_start)s AND %(my_end)s
        ORDER BY marketing_year, country_code, report_date
    """, params={'commodity': commodity, 'my_start': my_range[0], 'my_end': my_range[1]})

    if df_raw.empty:
        st.warning(f"No PSD data found for {selected_name}.")
        return

    # Use latest report per country/MY
    df = _get_latest_by_country(df_raw)

    # Clean country names — prefer the mapping, then the longer of country/code
    df['country_name'] = df['country_code'].map(COUNTRY_NAMES)
    # Fill gaps: use 'country' column if it's a real name (not just the code)
    mask = df['country_name'].isna()
    df.loc[mask, 'country_name'] = df.loc[mask, 'country'].where(
        df.loc[mask, 'country'].str.len() > 3, df.loc[mask, 'country_code']
    )

    # Deduplicate: some countries appear with code AND full name as separate rows
    # Keep only the row with the longest country string per country_code/MY
    df['_name_len'] = df['country'].str.len()
    df = df.sort_values('_name_len', ascending=False).drop_duplicates(
        subset=['country_code', 'marketing_year'], keep='first'
    ).drop(columns=['_name_len'])

    # Calculate stocks-to-use
    df['stu_pct'] = df.apply(
        lambda r: round(r['ending_stocks'] / r['total_distribution'] * 100, 1)
        if r['total_distribution'] and r['total_distribution'] > 0 else None, axis=1
    )

    # ── KEY METRICS (latest MY) ────────────────────────────────────
    latest_my = df['marketing_year'].max()
    world = df[(df['country_code'] == 'WD') & (df['marketing_year'] == latest_my)]

    if not world.empty:
        w = world.iloc[0]
        st.subheader(f"MY {latest_my}/{str(latest_my + 1)[-2:]} World {selected_name}")
        m1, m2, m3, m4, m5 = st.columns(5)
        m1.metric("Production", f"{w['production']:,.0f}")
        m2.metric("Total Supply", f"{w['total_supply']:,.0f}")
        m3.metric("Consumption", f"{w['domestic_consumption']:,.0f}")
        m4.metric("Exports", f"{w['exports']:,.0f}")
        m5.metric("Ending Stocks", f"{w['ending_stocks']:,.0f}")

    st.divider()

    # ── PRODUCTION RANKINGS ────────────────────────────────────────
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Production by Country")

        prod = df[
            (df['marketing_year'] == latest_my) &
            (df['country_code'] != 'WD') &
            (df['production'].notna()) &
            (df['production'] > 0)
        ].nlargest(10, 'production').copy()

        if not prod.empty:
            fig_prod = px.bar(
                prod.sort_values('production', ascending=True),
                x='production', y='country_name',
                orientation='h',
                color='country_name',
                color_discrete_map=COUNTRY_COLORS,
                labels={'production': '1,000 MT', 'country_name': ''},
                title=f"Top Producers — MY {latest_my}/{str(latest_my+1)[-2:]}",
            )
            fig_prod.update_layout(showlegend=False, height=400, margin=dict(l=0, r=20))
            st.plotly_chart(fig_prod, use_container_width=True)

    with col2:
        st.subheader("Stocks-to-Use Ratio")

        stu = df[
            (df['marketing_year'] == latest_my) &
            (df['country_code'] != 'WD') &
            (df['stu_pct'].notna()) &
            (df['production'].notna()) &
            (df['production'] > 1000)  # Only major producers
        ].sort_values('stu_pct', ascending=True).copy()

        if not stu.empty:
            fig_stu = px.bar(
                stu,
                x='stu_pct', y='country_name',
                orientation='h',
                color='stu_pct',
                color_continuous_scale=['#c00000', '#f4b942', '#548235'],
                labels={'stu_pct': 'Stocks/Use %', 'country_name': ''},
                title=f"Stocks-to-Use — MY {latest_my}/{str(latest_my+1)[-2:]}",
            )
            fig_stu.update_layout(
                showlegend=False, height=400, margin=dict(l=0, r=20),
                coloraxis_showscale=False,
            )
            st.plotly_chart(fig_stu, use_container_width=True)

    # ── PRODUCTION TREND ───────────────────────────────────────────
    st.subheader("Production Trends — Major Producers")

    major_countries = ['US', 'BR', 'AR', 'CH', 'IN', 'CA', 'RS', 'E4', 'UP', 'PA']
    if commodity == 'palm_oil':
        major_countries = ['ID', 'MY', 'TH', 'CO', 'NG']
    elif commodity == 'rapeseed':
        major_countries = ['E4', 'CA', 'AS', 'CH', 'IN', 'UP', 'RS']

    trend = df[
        (df['country_code'].isin(major_countries)) &
        (df['production'].notna()) &
        (df['production'] > 0)
    ].copy()

    if not trend.empty:
        fig_trend = px.line(
            trend.sort_values('marketing_year'),
            x='marketing_year', y='production',
            color='country_name',
            color_discrete_map=COUNTRY_COLORS,
            markers=True,
            labels={'production': '1,000 MT', 'marketing_year': 'Marketing Year',
                    'country_name': ''},
            title=f"{selected_name} Production by Country",
        )
        fig_trend.update_layout(height=450, legend=dict(orientation='h', y=-0.15))
        fig_trend.update_xaxes(dtick=1)
        st.plotly_chart(fig_trend, use_container_width=True)

    # ── TRADE FLOWS ────────────────────────────────────────────────
    st.subheader("Trade Flows — Exports vs Imports")

    col3, col4 = st.columns(2)

    with col3:
        exporters = df[
            (df['marketing_year'] == latest_my) &
            (df['country_code'] != 'WD') &
            (df['exports'].notna()) &
            (df['exports'] > 500)
        ].nlargest(8, 'exports').copy()

        if not exporters.empty:
            fig_exp = px.bar(
                exporters.sort_values('exports', ascending=True),
                x='exports', y='country_name',
                orientation='h',
                color='country_name',
                color_discrete_map=COUNTRY_COLORS,
                labels={'exports': '1,000 MT', 'country_name': ''},
                title=f"Top Exporters — MY {latest_my}/{str(latest_my+1)[-2:]}",
            )
            fig_exp.update_layout(showlegend=False, height=350, margin=dict(l=0))
            st.plotly_chart(fig_exp, use_container_width=True)

    with col4:
        importers = df[
            (df['marketing_year'] == latest_my) &
            (df['country_code'] != 'WD') &
            (df['imports'].notna()) &
            (df['imports'] > 500)
        ].nlargest(8, 'imports').copy()

        if not importers.empty:
            fig_imp = px.bar(
                importers.sort_values('imports', ascending=True),
                x='imports', y='country_name',
                orientation='h',
                color='country_name',
                color_discrete_map=COUNTRY_COLORS,
                labels={'imports': '1,000 MT', 'country_name': ''},
                title=f"Top Importers — MY {latest_my}/{str(latest_my+1)[-2:]}",
            )
            fig_imp.update_layout(showlegend=False, height=350, margin=dict(l=0))
            st.plotly_chart(fig_imp, use_container_width=True)

    # ── WORLD BALANCE SHEET TABLE ──────────────────────────────────
    st.subheader(f"World {selected_name} Balance Sheet")

    world_bs = df[df['country_code'] == 'WD'].sort_values('marketing_year', ascending=False)
    if not world_bs.empty:
        display_cols = ['marketing_year', 'production', 'imports', 'total_supply',
                        'domestic_consumption', 'exports', 'ending_stocks', 'stu_pct']
        if 'crush' in world_bs.columns and world_bs['crush'].notna().any():
            display_cols.insert(4, 'crush')

        bs_display = world_bs[display_cols].copy()
        bs_display.columns = [c.replace('_', ' ').title() for c in bs_display.columns]
        bs_display = bs_display.rename(columns={'Stu Pct': 'STU %', 'Marketing Year': 'MY'})

        st.dataframe(bs_display, width="stretch", hide_index=True)

    # ── COUNTRY COMPARISON TABLE ───────────────────────────────────
    st.subheader(f"Country Comparison — MY {latest_my}/{str(latest_my+1)[-2:]}")

    country_table = df[
        (df['marketing_year'] == latest_my) &
        (df['country_code'] != 'WD') &
        (df['production'].notna()) &
        (df['production'] > 100)
    ].sort_values('production', ascending=False).copy()

    if not country_table.empty:
        display_cols = ['country_name', 'production', 'imports', 'domestic_consumption',
                        'exports', 'ending_stocks', 'stu_pct']
        if 'crush' in country_table.columns and country_table['crush'].notna().any():
            display_cols.insert(3, 'crush')

        ct_display = country_table[display_cols].copy()
        ct_display.columns = [c.replace('_', ' ').title() for c in ct_display.columns]
        ct_display = ct_display.rename(columns={
            'Country Name': 'Country', 'Stu Pct': 'STU %'
        })

        st.dataframe(ct_display, width="stretch", hide_index=True)

    # ── CRUSH MARGINS (if oilseed) ─────────────────────────────────
    if commodity in ('soybeans', 'rapeseed', 'palm_oil'):
        st.divider()
        st.subheader("US Crush Margins")

        try:
            margins = query_fn("""
                SELECT period, oilseed_code, crush_margin, margin_pct,
                       oil_price_cents_lb, meal_price_per_ton, seed_price_per_unit,
                       gross_processing_value
                FROM silver.oilseed_crush_margin
                WHERE oilseed_code = %(code)s
                ORDER BY period DESC
                LIMIT 24
            """, params={'code': commodity})
        except Exception:
            margins = pd.DataFrame()

        if not margins.empty:
            fig_margin = go.Figure()
            fig_margin.add_trace(go.Bar(
                x=margins['period'], y=margins['crush_margin'],
                marker_color=margins['crush_margin'].apply(
                    lambda v: '#548235' if v > 0 else '#c00000'
                ),
                name='Crush Margin',
            ))
            fig_margin.update_layout(
                title=f"{selected_name} Crush Margin ($/unit)",
                height=350,
                yaxis_title="$/unit",
                xaxis_title="",
            )
            st.plotly_chart(fig_margin, use_container_width=True)
        else:
            st.info("No crush margin data yet. Run: `python -m src.engines.oilseed_crush.engine --range 2025-01 2026-03`")
