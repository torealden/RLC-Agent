"""
RLC Commodities Intelligence Dashboard
=======================================
Multi-page Streamlit dashboard powered by the RLC agricultural commodities database.

Launch:  streamlit run src/dashboard/app.py
"""
import os
import streamlit as st
import pandas as pd
import plotly.express as px

from dotenv import load_dotenv
load_dotenv()
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import psycopg2
from datetime import datetime, timedelta
from functools import lru_cache

# ── Page config ──────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="RLC Commodities Intelligence",
    page_icon="🌾",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Database ─────────────────────────────────────────────────────────────────
@st.cache_resource
def get_connection():
    return psycopg2.connect(
        host=os.environ.get('RLC_PG_HOST', os.environ.get('DB_HOST', 'localhost')),
        port=os.environ.get('RLC_PG_PORT', os.environ.get('DB_PORT', 5432)),
        dbname=os.environ.get('RLC_PG_DATABASE', os.environ.get('DB_NAME', 'rlc_commodities')),
        user=os.environ.get('RLC_PG_USER', os.environ.get('DB_USER', 'postgres')),
        password=os.environ.get('RLC_PG_PASSWORD', os.environ.get('DB_PASSWORD', '')),
        sslmode=os.environ.get('RLC_PG_SSLMODE', 'prefer'),
    )

def query(sql, params=None):
    """Run a SQL query and return a DataFrame."""
    conn = get_connection()
    try:
        df = pd.read_sql(sql, conn, params=params)
        return df
    except Exception as e:
        # Reconnect on failure
        st.cache_resource.clear()
        conn = get_connection()
        return pd.read_sql(sql, conn, params=params)


# ── Color palette ────────────────────────────────────────────────────────────
COLORS = {
    'primary': '#1f4e79',
    'secondary': '#2e75b6',
    'accent': '#c55a11',
    'positive': '#548235',
    'negative': '#c00000',
    'neutral': '#7f7f7f',
    'corn': '#f4b942',
    'soybeans': '#548235',
    'wheat': '#c55a11',
    'soybean_oil': '#2e75b6',
    'soybean_meal': '#8faadc',
    'palm_oil': '#e06c2e',
}

COMMODITY_COLORS = ['#1f4e79', '#2e75b6', '#548235', '#c55a11', '#f4b942',
                    '#7030a0', '#c00000', '#8faadc', '#e06c2e', '#a9d18e']

# ── Sidebar navigation ──────────────────────────────────────────────────────
st.sidebar.title("RLC Commodities")
st.sidebar.caption("Intelligence Dashboard")

page = st.sidebar.radio(
    "Navigate",
    ["Overview", "Global Oilseeds S&D", "US Balance Sheets", "CFTC Positioning",
     "Export Analysis", "Crush & Processing",
     "MPOB Palm Oil", "Weather"],
    index=0,
)


# ═══════════════════════════════════════════════════════════════════════════════
# PAGE: OVERVIEW
# ═══════════════════════════════════════════════════════════════════════════════
def page_overview():
    st.title("Commodities Intelligence Overview")

    # Key metrics row
    col1, col2, col3, col4 = st.columns(4)

    # Latest CFTC sentiment
    cftc = query("""
        SELECT commodity,
               report_date,
               mm_net,
               mm_net_change
        FROM gold.cftc_sentiment
        ORDER BY commodity
    """)
    if not cftc.empty:
        with col1:
            st.metric("CFTC Report Date",
                       cftc['report_date'].iloc[0].strftime('%b %d, %Y') if hasattr(cftc['report_date'].iloc[0], 'strftime') else str(cftc['report_date'].iloc[0]))

    # Data freshness
    freshness = query("""
        SELECT collector_name, last_collected, last_status,
               hours_since_collection, is_overdue
        FROM core.data_freshness
        ORDER BY hours_since_collection
    """)
    if not freshness.empty:
        overdue_count = freshness['is_overdue'].sum()
        ok_count = (freshness['last_status'] == 'success').sum()
        failed_count = (freshness['last_status'] == 'failed').sum()
        with col2:
            st.metric("Active Collectors", f"{ok_count}", f"{overdue_count} overdue" if overdue_count else "All current")
        with col3:
            st.metric("Failed Collectors", f"{failed_count}",
                       delta_color="inverse")
        with col4:
            try:
                total_rows = query("SELECT SUM(last_row_count)::bigint as total_rows FROM core.data_freshness")
                if not total_rows.empty:
                    val = pd.to_numeric(total_rows['total_rows'].iloc[0], errors='coerce')
                    if pd.notna(val):
                        st.metric("Total Rows Tracked", f"{int(val):,}")
            except Exception:
                pass

    st.divider()

    # Two-column layout: CFTC Sentiment + Data Freshness
    left, right = st.columns([3, 2])

    with left:
        st.subheader("Managed Money Net Positioning")
        if not cftc.empty:
            cftc_chart = cftc.copy()
            cftc_chart['color'] = cftc_chart['mm_net'].apply(
                lambda x: COLORS['positive'] if x > 0 else COLORS['negative']
            )
            fig = go.Figure()
            fig.add_trace(go.Bar(
                x=cftc_chart['commodity'],
                y=cftc_chart['mm_net'],
                marker_color=cftc_chart['color'],
                text=cftc_chart['mm_net'].apply(lambda x: f"{x:,.0f}"),
                textposition='outside',
            ))
            fig.update_layout(
                height=400,
                margin=dict(t=20, b=40, l=60, r=20),
                yaxis_title="Net Contracts",
                xaxis_title="",
                showlegend=False,
            )
            st.plotly_chart(fig, use_container_width=True)

    with right:
        st.subheader("Data Freshness")
        if not freshness.empty:
            display_df = freshness[['collector_name', 'last_status',
                                    'hours_since_collection', 'is_overdue']].copy()
            display_df['hours_since_collection'] = display_df['hours_since_collection'].apply(
                lambda x: f"{float(x):.1f}h" if x else "-"
            )
            display_df.columns = ['Collector', 'Status', 'Age', 'Overdue']
            st.dataframe(display_df, width="stretch", height=400, hide_index=True)

    st.divider()

    # US Soybean S&D snapshot
    st.subheader("US Soybean Balance Sheet (Recent Marketing Years)")
    soy_bs = query("""
        SELECT * FROM gold.fas_us_soybeans_balance_sheet
        ORDER BY marketing_year DESC LIMIT 6
    """)
    if not soy_bs.empty:
        st.dataframe(soy_bs, width="stretch", hide_index=True)


# ═══════════════════════════════════════════════════════════════════════════════
# PAGE: US BALANCE SHEETS
# ═══════════════════════════════════════════════════════════════════════════════
def page_balance_sheets():
    st.title("US Balance Sheets")

    commodity = st.selectbox(
        "Commodity",
        ["Soybeans", "Corn", "Wheat", "Soybean Oil", "Soybean Meal"],
    )

    view_map = {
        "Soybeans": "gold.fas_us_soybeans_balance_sheet",
        "Corn": "gold.fas_us_corn_balance_sheet",
        "Wheat": "gold.fas_us_wheat_balance_sheet",
        "Soybean Oil": "gold.us_soybean_oil_balance_sheet",
        "Soybean Meal": "gold.us_soybean_meal_balance_sheet",
    }

    bs = query(f"SELECT * FROM {view_map[commodity]} ORDER BY marketing_year DESC")

    if bs.empty:
        st.warning(f"No balance sheet data for {commodity}")
        return

    # Convert numeric columns from string if needed
    for col in bs.columns:
        if col != 'marketing_year' and col not in ('unit', 'source', 'report_date'):
            bs[col] = pd.to_numeric(bs[col], errors='ignore')

    st.dataframe(bs, width="stretch", hide_index=True)

    # Find ending stocks column (varies by view)
    es_col = None
    for candidate in ['ending_stocks', 'ending_stocks_mil_lbs', 'ending_stocks_thou_st']:
        if candidate in bs.columns:
            es_col = candidate
            break

    if es_col and 'marketing_year' in bs.columns:
        st.subheader("Ending Stocks Trend")
        chart_df = bs.sort_values('marketing_year')

        fig = make_subplots(specs=[[{"secondary_y": True}]])

        fig.add_trace(
            go.Bar(
                x=chart_df['marketing_year'].astype(str),
                y=pd.to_numeric(chart_df[es_col], errors='coerce'),
                name="Ending Stocks",
                marker_color=COLORS['primary'],
            ),
            secondary_y=False,
        )

        # Calculate stocks-to-use if possible
        use_col = None
        for col in ['total_distribution', 'total_use', 'domestic_consumption',
                     'domestic_use_thou_st']:
            if col in chart_df.columns:
                use_col = col
                break

        export_col = None
        for col in ['exports', 'exports_mil_lbs', 'exports_thou_st']:
            if col in chart_df.columns:
                export_col = col
                break

        if use_col and export_col:
            es = pd.to_numeric(chart_df[es_col], errors='coerce')
            total_use = pd.to_numeric(chart_df[use_col], errors='coerce')
            if use_col == 'domestic_consumption':
                total_use = total_use + pd.to_numeric(chart_df[export_col], errors='coerce')
            stu = (es / total_use * 100).round(1)
            fig.add_trace(
                go.Scatter(
                    x=chart_df['marketing_year'].astype(str),
                    y=stu,
                    name="Stocks/Use %",
                    line=dict(color=COLORS['accent'], width=3),
                    mode='lines+markers',
                ),
                secondary_y=True,
            )
            fig.update_yaxes(title_text="Stocks/Use %", secondary_y=True)

        fig.update_layout(
            height=450,
            margin=dict(t=30, b=40),
            yaxis_title="1000 MT",
            legend=dict(orientation="h", yanchor="bottom", y=1.02),
        )
        fig.update_yaxes(title_text="1000 MT", secondary_y=False)
        st.plotly_chart(fig, use_container_width=True)

    # Production & exports comparison — find the right columns
    prod_col = next((c for c in ['production', 'production_mil_lbs', 'production_thou_st'] if c in bs.columns), None)
    exp_col = next((c for c in ['exports', 'exports_mil_lbs', 'exports_thou_st'] if c in bs.columns), None)
    if prod_col and exp_col:
        st.subheader("Production vs Exports")
        chart_df = bs.sort_values('marketing_year')
        fig = go.Figure()
        fig.add_trace(go.Bar(
            x=chart_df['marketing_year'].astype(str),
            y=pd.to_numeric(chart_df[prod_col], errors='coerce'),
            name="Production",
            marker_color=COLORS['positive'],
        ))
        fig.add_trace(go.Bar(
            x=chart_df['marketing_year'].astype(str),
            y=pd.to_numeric(chart_df[exp_col], errors='coerce'),
            name="Exports",
            marker_color=COLORS['secondary'],
        ))
        fig.update_layout(
            barmode='group', height=400,
            margin=dict(t=30, b=40),
            yaxis_title="1000 MT",
            legend=dict(orientation="h", yanchor="bottom", y=1.02),
        )
        st.plotly_chart(fig, use_container_width=True)


# ═══════════════════════════════════════════════════════════════════════════════
# PAGE: CFTC POSITIONING
# ═══════════════════════════════════════════════════════════════════════════════
def page_cftc():
    st.title("CFTC Managed Money Positioning")

    commodity = st.selectbox(
        "Commodity",
        ["corn", "soybeans", "soybean_oil", "soybean_meal",
         "wheat_srw", "wheat_hrw", "wheat_hrs",
         "crude_oil", "natural_gas", "cotton", "sugar",
         "live_cattle", "lean_hogs"],
    )

    years_back = st.slider("Years of History", 1, 20, 5)
    cutoff = datetime.now() - timedelta(days=years_back * 365)

    df = query("""
        SELECT report_date, mm_long, mm_short,
               mm_net,
               prod_long, prod_short,
               prod_net
        FROM bronze.cftc_cot
        WHERE commodity = %s AND report_date >= %s
        ORDER BY report_date
    """, (commodity, cutoff))

    if df.empty:
        st.warning(f"No CFTC data for {commodity}")
        return

    # Ensure numeric types
    for col in ['mm_long', 'mm_short', 'mm_net', 'prod_long', 'prod_short', 'prod_net']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')

    # Net position chart
    st.subheader(f"{commodity.replace('_', ' ').title()} — Managed Money Net Position")
    fig = go.Figure()

    # Color the bars by sign
    colors = [COLORS['positive'] if v >= 0 else COLORS['negative'] for v in df['mm_net']]

    fig.add_trace(go.Bar(
        x=df['report_date'],
        y=df['mm_net'],
        marker_color=colors,
        name="MM Net",
    ))

    # Add moving average
    df['ma_13wk'] = df['mm_net'].rolling(13).mean()
    fig.add_trace(go.Scatter(
        x=df['report_date'],
        y=df['ma_13wk'],
        name="13-Week MA",
        line=dict(color=COLORS['accent'], width=2),
    ))

    fig.update_layout(
        height=450,
        margin=dict(t=20, b=40, l=60, r=20),
        yaxis_title="Net Contracts",
        legend=dict(orientation="h", yanchor="bottom", y=1.02),
        xaxis=dict(rangeslider=dict(visible=True), type="date"),
    )
    st.plotly_chart(fig, use_container_width=True)

    # Long/Short breakdown
    st.subheader("Long vs Short Positions")
    fig2 = go.Figure()
    fig2.add_trace(go.Scatter(
        x=df['report_date'], y=df['mm_long'],
        name="MM Long", fill='tozeroy',
        line=dict(color=COLORS['positive']),
    ))
    fig2.add_trace(go.Scatter(
        x=df['report_date'], y=-df['mm_short'],
        name="MM Short (inverted)", fill='tozeroy',
        line=dict(color=COLORS['negative']),
    ))
    fig2.update_layout(
        height=350,
        margin=dict(t=20, b=40),
        yaxis_title="Contracts",
        legend=dict(orientation="h", yanchor="bottom", y=1.02),
    )
    st.plotly_chart(fig2, use_container_width=True)

    # Statistics
    st.subheader("Position Statistics")
    col1, col2, col3, col4 = st.columns(4)
    latest_net = df['mm_net'].iloc[-1]
    pctile = (df['mm_net'] <= latest_net).mean() * 100

    col1.metric("Current Net", f"{latest_net:,.0f}")
    col2.metric("Percentile", f"{pctile:.0f}th")
    col3.metric("Period High", f"{df['mm_net'].max():,.0f}")
    col4.metric("Period Low", f"{df['mm_net'].min():,.0f}")


# ═══════════════════════════════════════════════════════════════════════════════
# PAGE: EXPORT ANALYSIS
# ═══════════════════════════════════════════════════════════════════════════════
def page_exports():
    st.title("US Export Analysis")

    tab1, tab2 = st.tabs(["Export Sales by Month", "Top Destinations"])

    with tab1:
        commodity = st.selectbox(
            "Commodity",
            ["Soybeans", "Corn", "Wheat", "Soybean Meal", "Soybean Oil"],
            key="export_commodity",
        )

        commodity_lower = commodity.lower().replace(" ", "_")

        export_name = {"soybeans": "soybeans", "corn": "corn", "wheat": "wheat",
                       "soybean_meal": "soybean_meal", "soybean_oil": "soybean_oil"
                       }.get(commodity_lower, commodity_lower)

        # Monthly export volumes from FAS export sales
        monthly = query("""
            SELECT
                date_trunc('month', week_ending)::date as month,
                EXTRACT(YEAR FROM week_ending) as year,
                EXTRACT(MONTH FROM week_ending) as mo,
                SUM(weekly_exports) as total_exports
            FROM bronze.fas_export_sales
            WHERE commodity = %s
            GROUP BY 1, 2, 3
            ORDER BY 1
        """, (export_name,))

        if monthly.empty:
            st.info("No export sales data available for this commodity. Trying FAS PSD...")
            # Fallback to PSD exports
            psd_exports = query("""
                SELECT marketing_year as year, exports
                FROM bronze.fas_psd
                WHERE commodity = %s AND country_code = 'US'
                ORDER BY marketing_year
            """, (commodity_lower.replace("_", " ") if "_" not in commodity_lower else commodity_lower,))

            if not psd_exports.empty:
                fig = px.bar(psd_exports, x='year', y='exports',
                             title=f"US {commodity} Annual Exports (1000 MT)",
                             color_discrete_sequence=[COLORS['primary']])
                fig.update_layout(height=450)
                st.plotly_chart(fig, use_container_width=True)
        else:
            st.subheader(f"US {commodity} Monthly Export Volumes")
            # Heatmap by year/month
            pivot = monthly.pivot_table(
                index='year', columns='mo', values='total_exports', aggfunc='sum'
            )
            pivot.columns = ['Jan','Feb','Mar','Apr','May','Jun',
                            'Jul','Aug','Sep','Oct','Nov','Dec'][:len(pivot.columns)]

            fig = px.imshow(
                pivot.iloc[-10:],  # Last 10 years
                labels=dict(x="Month", y="Year", color="MT"),
                color_continuous_scale="YlOrRd",
                aspect="auto",
            )
            fig.update_layout(height=450, margin=dict(t=30))
            st.plotly_chart(fig, use_container_width=True)

            # Bar chart by year
            yearly = monthly.groupby('year')['total_exports'].sum().reset_index()
            yearly['year'] = pd.to_numeric(yearly['year'], errors='coerce')
            yearly = yearly.dropna(subset=['year'])
            yearly = yearly[yearly['year'] >= yearly['year'].max() - 10]
            fig2 = px.bar(yearly, x='year', y='total_exports',
                         title=f"Annual {commodity} Export Sales Volume",
                         color_discrete_sequence=[COLORS['primary']])
            fig2.update_layout(height=400, yaxis_title="MT")
            st.plotly_chart(fig2, use_container_width=True)

    with tab2:
        commodity_export_map = {
            "soybeans": "soybeans", "corn": "corn", "wheat": "wheat",
            "soybean_meal": "soybean_meal", "soybean_oil": "soybean_oil",
        }
        export_name = commodity_export_map.get(commodity_lower, commodity_lower)
        st.subheader("Top Export Destinations (Current Marketing Year)")
        destinations = query("""
            SELECT country, SUM(weekly_exports) as total_exports,
                   SUM(outstanding_sales) as outstanding
            FROM bronze.fas_export_sales
            WHERE week_ending >= NOW() - INTERVAL '1 year'
              AND commodity = %s
            GROUP BY country
            ORDER BY total_exports DESC
            LIMIT 15
        """, (export_name,))

        if not destinations.empty:
            fig = px.bar(
                destinations, x='total_exports', y='country',
                orientation='h',
                color_discrete_sequence=[COLORS['primary']],
            )
            fig.update_layout(
                height=500, margin=dict(t=20, l=120),
                yaxis=dict(autorange="reversed"),
                xaxis_title="MT",
            )
            st.plotly_chart(fig, use_container_width=True)


# ═══════════════════════════════════════════════════════════════════════════════
# PAGE: CRUSH & PROCESSING
# ═══════════════════════════════════════════════════════════════════════════════
def page_crush():
    st.title("US Crush & Processing")

    commodity = st.selectbox(
        "Commodity",
        ["soybeans", "canola", "cottonseed", "corn", "sunflower"],
        format_func=lambda x: x.title(),
        key="crush_commodity",
    )

    # Get data from the gold matrix
    df = query("""
        SELECT year, month, header_pattern, display_value
        FROM gold.fats_oils_crush_matrix
        WHERE commodity = %s AND display_value IS NOT NULL
        ORDER BY year, month
    """, (commodity,))

    if df.empty:
        st.warning(f"No crush data for {commodity}")
        return

    # Available attributes
    attributes = sorted(df['header_pattern'].unique())

    # Primary attribute selection
    default_attrs = []
    for a in attributes:
        if any(kw in a.lower() for kw in ['crush', 'crude oil production', 'seeds crushed']):
            default_attrs.append(a)
    if not default_attrs:
        default_attrs = [attributes[0]]

    selected = st.multiselect(
        "Attributes", attributes, default=default_attrs[:3]
    )

    if not selected:
        return

    filtered = df[df['header_pattern'].isin(selected)].copy()
    filtered['year'] = pd.to_numeric(filtered['year'], errors='coerce')
    filtered['month'] = pd.to_numeric(filtered['month'], errors='coerce')
    filtered = filtered.dropna(subset=['year', 'month'])
    filtered['date'] = pd.to_datetime(
        filtered['year'].astype(int).astype(str) + '-' + filtered['month'].astype(int).astype(str).str.zfill(2) + '-01'
    )

    # Time series chart
    st.subheader(f"{commodity.title()} — Monthly Data")
    fig = px.line(
        filtered, x='date', y='display_value',
        color='header_pattern',
        color_discrete_sequence=COMMODITY_COLORS,
    )
    fig.update_layout(
        height=450,
        margin=dict(t=20, b=40),
        yaxis_title="Value",
        xaxis_title="",
        legend=dict(orientation="h", yanchor="bottom", y=1.02, title=""),
    )
    st.plotly_chart(fig, use_container_width=True)

    # YoY comparison for primary attribute
    if len(selected) >= 1:
        attr = selected[0]
        attr_df = df[df['header_pattern'] == attr].copy()
        attr_df['year'] = pd.to_numeric(attr_df['year'], errors='coerce')
        attr_df['month'] = pd.to_numeric(attr_df['month'], errors='coerce')
        st.subheader(f"{attr} — Year-over-Year")

        pivot = attr_df.pivot_table(index='month', columns='year', values='display_value')
        # Last 5 years
        recent_years = sorted(pivot.columns)[-5:]
        pivot = pivot[recent_years]
        month_labels = ['Jan','Feb','Mar','Apr','May','Jun',
                       'Jul','Aug','Sep','Oct','Nov','Dec']

        fig2 = go.Figure()
        for yr in recent_years:
            if yr in pivot.columns:
                fig2.add_trace(go.Scatter(
                    x=month_labels[:len(pivot[yr])],
                    y=pivot[yr],
                    name=str(int(yr)),
                    mode='lines+markers',
                ))
        fig2.update_layout(
            height=400,
            margin=dict(t=20, b=40),
            yaxis_title="Value",
            legend=dict(orientation="h", yanchor="bottom", y=1.02),
        )
        st.plotly_chart(fig2, use_container_width=True)

    # Data table
    with st.expander("Raw Data"):
        wide = filtered.pivot_table(
            index=['year', 'month'], columns='header_pattern', values='display_value'
        ).reset_index()
        st.dataframe(wide, width="stretch", hide_index=True)


# ═══════════════════════════════════════════════════════════════════════════════
# PAGE: MPOB PALM OIL
# ═══════════════════════════════════════════════════════════════════════════════
def page_mpob():
    st.title("Malaysia Palm Oil Industry (MPOB)")

    df = query("""
        SELECT data_year, category, indicator, region, value, unit
        FROM gold.mpob_industry_summary
        ORDER BY data_year, category, indicator
    """)

    if df.empty:
        # Fallback to bronze
        df = query("""
            SELECT data_year, category, indicator, region, value, unit
            FROM bronze.mpob_industry_overview
            ORDER BY data_year, category, indicator
        """)

    if df.empty:
        st.warning("No MPOB data available")
        return

    tab1, tab2, tab3, tab4 = st.tabs([
        "Production & Stocks", "Exports & Revenue", "Prices", "Yield & OER"
    ])

    with tab1:
        # CPO Production
        prod = df[(df['category'] == 'cpo_production') & (df['indicator'] == 'MALAYSIA')].sort_values('data_year')
        stocks = df[(df['category'] == 'closing_stocks') & (df['indicator'] == 'TOTAL PALM OIL')].sort_values('data_year')

        if not prod.empty:
            fig = make_subplots(specs=[[{"secondary_y": True}]])
            fig.add_trace(go.Bar(
                x=prod['data_year'], y=prod['value'] / 1e6,
                name="CPO Production (M MT)",
                marker_color=COLORS['positive'],
            ), secondary_y=False)

            if not stocks.empty:
                fig.add_trace(go.Scatter(
                    x=stocks['data_year'], y=stocks['value'] / 1e6,
                    name="Closing Stocks (M MT)",
                    line=dict(color=COLORS['accent'], width=3),
                    mode='lines+markers',
                ), secondary_y=True)

            fig.update_layout(
                title="Malaysia CPO Production & Closing Stocks",
                height=450, margin=dict(t=50, b=40),
                legend=dict(orientation="h", yanchor="bottom", y=1.02),
            )
            fig.update_yaxes(title_text="Million MT", secondary_y=False)
            fig.update_yaxes(title_text="Million MT", secondary_y=True)
            st.plotly_chart(fig, use_container_width=True)

        # Regional production breakdown
        regional = df[(df['category'] == 'cpo_production') & (df['indicator'] != 'MALAYSIA')].copy()
        if not regional.empty:
            fig2 = px.bar(
                regional.sort_values('data_year'),
                x='data_year', y=regional['value'] / 1e6,
                color='indicator',
                title="CPO Production by Region",
                color_discrete_sequence=COMMODITY_COLORS,
            )
            fig2.update_layout(
                height=400, barmode='stack',
                yaxis_title="Million MT",
                legend_title="Region",
            )
            st.plotly_chart(fig2, use_container_width=True)

    with tab2:
        # Exports
        exports = df[df['category'] == 'exports_volume'].sort_values(['data_year', 'indicator'])
        if not exports.empty:
            # Total palm oil exports
            po_exports = exports[exports['indicator'] == 'PALM OIL'].sort_values('data_year')
            fig = px.bar(
                po_exports, x='data_year', y=po_exports['value'] / 1e6,
                title="Palm Oil Exports",
                color_discrete_sequence=[COLORS['primary']],
            )
            fig.update_layout(height=400, yaxis_title="Million MT")
            st.plotly_chart(fig, use_container_width=True)

        # Revenue
        revenue = df[df['category'] == 'exports_revenue'].sort_values(['data_year', 'indicator'])
        if not revenue.empty:
            total_rev = revenue.groupby('data_year')['value'].sum().reset_index()
            fig2 = px.bar(
                total_rev, x='data_year', y=total_rev['value'] / 1e3,
                title="Total Export Revenue",
                color_discrete_sequence=[COLORS['accent']],
            )
            fig2.update_layout(height=400, yaxis_title="RM Billion")
            st.plotly_chart(fig2, use_container_width=True)

    with tab3:
        prices = df[df['category'] == 'prices'].sort_values(['data_year', 'indicator'])
        if not prices.empty:
            key_prices = prices[prices['indicator'].isin([
                'CPO (LOCAL DELIVERED)', 'RBD PALM OIL (FOB)',
                'RBD PALM OLEIN (FOB)', 'PALM KERNEL (EX-MILL)', 'PFAD (FOB)',
            ])]
            if not key_prices.empty:
                fig = px.line(
                    key_prices, x='data_year', y='value',
                    color='indicator',
                    title="Key Palm Oil Prices (RM/MT)",
                    color_discrete_sequence=COMMODITY_COLORS,
                    markers=True,
                )
                fig.update_layout(
                    height=450,
                    yaxis_title="RM / Tonne",
                    legend=dict(orientation="h", yanchor="bottom", y=1.02, title=""),
                )
                st.plotly_chart(fig, use_container_width=True)

    with tab4:
        # FFB Yield
        ffb = df[(df['category'] == 'ffb_yield')].sort_values(['data_year', 'indicator'])
        if not ffb.empty:
            fig = px.line(
                ffb, x='data_year', y='value',
                color='indicator',
                title="FFB Yield by Region (MT/Hectare)",
                color_discrete_sequence=COMMODITY_COLORS,
                markers=True,
            )
            fig.update_layout(height=400, yaxis_title="MT / Hectare")
            st.plotly_chart(fig, use_container_width=True)

        # OER
        oer = df[df['category'] == 'oer'].sort_values(['data_year', 'indicator'])
        if not oer.empty:
            fig2 = px.line(
                oer, x='data_year', y='value',
                color='indicator',
                title="Oil Extraction Rate by Region (%)",
                color_discrete_sequence=COMMODITY_COLORS,
                markers=True,
            )
            fig2.update_layout(height=400, yaxis_title="%")
            st.plotly_chart(fig2, use_container_width=True)


# ═══════════════════════════════════════════════════════════════════════════════
# PAGE: WEATHER
# ═══════════════════════════════════════════════════════════════════════════════
def page_weather():
    st.title("Agricultural Weather")

    # Recent weather by location (use Celsius, extend range to find data)
    weather = query("""
        SELECT location_id,
               AVG(temp_avg_c) as avg_temp_c,
               SUM(precipitation_mm) as total_precip_mm,
               MAX(observation_date) as latest_date,
               COUNT(*) as observations
        FROM silver.weather_observation
        WHERE observation_date > CURRENT_DATE - INTERVAL '30 days'
          AND temp_avg_c IS NOT NULL
        GROUP BY location_id
        HAVING COUNT(*) > 3
        ORDER BY location_id
    """)

    if weather.empty:
        st.info("No recent weather observations available")
        return

    # Extract location name from location_id
    weather['location'] = weather['location_id'].apply(
        lambda x: x.replace('_', ' ').title() if x else x
    )
    # Convert C to F for display
    weather['avg_temp_f'] = weather['avg_temp_c'] * 9/5 + 32
    weather['total_precip_in'] = weather['total_precip_mm'] / 25.4

    latest = weather['latest_date'].max()
    st.subheader(f"Weather Summary (data through {latest})")

    col1, col2 = st.columns(2)

    with col1:
        fig = px.bar(
            weather.sort_values('avg_temp_f', ascending=True),
            x='avg_temp_f', y='location', orientation='h',
            title="Average Temperature (°F)",
            color='avg_temp_f',
            color_continuous_scale='RdYlBu_r',
        )
        fig.update_layout(height=max(400, len(weather) * 25), margin=dict(l=120))
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        fig2 = px.bar(
            weather.sort_values('total_precip_in', ascending=True),
            x='total_precip_in', y='location', orientation='h',
            title="Total Precipitation (inches)",
            color='total_precip_in',
            color_continuous_scale='Blues',
        )
        fig2.update_layout(height=max(400, len(weather) * 25), margin=dict(l=120))
        st.plotly_chart(fig2, use_container_width=True)

    # Drought monitor
    st.subheader("Drought Conditions")
    drought = query("""
        SELECT * FROM bronze.drought_conditions
        WHERE map_date = (SELECT MAX(map_date) FROM bronze.drought_conditions)
        ORDER BY state
    """)
    if not drought.empty:
        st.dataframe(drought, width="stretch", hide_index=True)


# ═══════════════════════════════════════════════════════════════════════════════
# ROUTING
# ═══════════════════════════════════════════════════════════════════════════════
from global_oilseeds import page_global_oilseeds

pages = {
    "Overview": page_overview,
    "Global Oilseeds S&D": lambda: page_global_oilseeds(query),
    "US Balance Sheets": page_balance_sheets,
    "CFTC Positioning": page_cftc,
    "Export Analysis": page_exports,
    "Crush & Processing": page_crush,
    "MPOB Palm Oil": page_mpob,
    "Weather": page_weather,
}

pages[page]()

# Footer
st.sidebar.divider()
st.sidebar.caption(f"Data as of {datetime.now().strftime('%B %d, %Y')}")
st.sidebar.caption("RLC Commodities Intelligence")
