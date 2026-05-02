"""
AGP Eagle Grove — Facility Deep-Dive

Showcase page for the Phase Two facility-agent architecture. Pulls every
piece of intelligence we have on this single facility:
  - Static profile (reference + KG)
  - Title V emission units (52 of them, with rated capacities)
  - Process flow rendered against the canonical oilseed_crush.md ontology
  - Live board crush margin from CME futures
  - Implied facility-level monthly throughput from NOPA-derived state share
  - Strategic quarterly view (placeholder for Layer 1 strategic_plan)

Run:
    cd C:\\dev\\RLC-Agent
    streamlit run dashboards/facility/eagle_grove.py

Author note: this is the *first* facility deep-dive. The pattern here is
intended to generalize — replace the FACILITY_ID constant and the
ontology lookup, and the rest holds.
"""
from __future__ import annotations

from datetime import date, datetime
from pathlib import Path

import folium
import pandas as pd
import plotly.graph_objects as go
import psycopg2
import psycopg2.extras
import streamlit as st
from dotenv import load_dotenv
from streamlit_folium import st_folium

load_dotenv()

import os

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

FACILITY_ID = "ia.agp_eagle_grove"
FACILITY_DISPLAY = "AGP Eagle Grove"

# AGP brand-aligned palette (forest green primary, paired with warm neutrals)
COLORS = {
    "primary": "#1F4D2C",      # AGP-style forest green
    "primary_light": "#3A7A4D",
    "accent": "#D4A848",       # warm gold for highlight numbers
    "bg_dark": "#0E1614",
    "bg_card": "#1A2521",
    "text": "#E8EDE9",
    "text_dim": "#8FA095",
    "danger": "#C45B4D",
    "process_step": "#2D5A3D",
}

# Map of ontology process step -> equipment categories the LLM extracted.
# Used to bucket the 52 emission units into the canonical 19-step flow.
PROCESS_FLOW = [
    ("receiving",          "Receiving",            ["receiving", "handling"]),
    ("cleaning",           "Cleaning",             ["aspiration"]),  # partial overlap
    ("drying",             "Drying",               ["drying"]),
    ("dehulling",          "Cracking & Dehulling", ["cracking and dehulling"]),
    ("conditioning",       "Conditioning",         ["conditioning"]),
    ("flaking",            "Flaking",              []),  # often grouped with conditioning
    ("extraction",         "Solvent Extraction",   ["extraction"]),
    ("desolventizing",     "Desolventizing",       []),  # not always separately tagged
    ("meal_processing",    "Meal Drying & Grinding", ["grinding", "cooling"]),
    ("storage",            "Meal/Oil Storage",     ["storage"]),
    ("loadout",            "Loadout (rail/truck)", ["loading/unloading", "conveying"]),
    ("refining",           "Oil Refining (RB)",    ["refining"]),
    ("utilities",          "Boilers & Utilities",  ["boiler", "boilers"]),
    ("controls",           "Dust Control",         ["baghouse", "scrubber"]),
]


# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------

@st.cache_resource
def get_db_config():
    return dict(
        host=os.getenv("RLC_PG_HOST"),
        port=os.getenv("RLC_PG_PORT", "5432"),
        database=os.getenv("RLC_PG_DATABASE", "rlc_commodities"),
        user=os.getenv("RLC_PG_USER"),
        password=os.getenv("RLC_PG_PASSWORD"),
    )


def query(sql: str, params=None) -> pd.DataFrame:
    cfg = get_db_config()
    with psycopg2.connect(**cfg) as conn:
        return pd.read_sql(sql, conn, params=params)


# ---------------------------------------------------------------------------
# Data loaders (cached)
# ---------------------------------------------------------------------------

@st.cache_data(ttl=300)
def load_facility_profile() -> dict:
    df = query(
        """
        SELECT *
        FROM reference.oilseed_crush_facilities
        WHERE facility_id = %(fid)s
        """,
        {"fid": FACILITY_ID},
    )
    if df.empty:
        return {}
    return df.iloc[0].to_dict()


@st.cache_data(ttl=300)
def load_kg_node() -> dict:
    df = query(
        """
        SELECT node_key, label, node_type, properties
        FROM core.kg_node
        WHERE node_key = %(fid)s
        """,
        {"fid": FACILITY_ID},
    )
    if df.empty:
        return {}
    return df.iloc[0].to_dict()


@st.cache_data(ttl=300)
def load_emission_units() -> pd.DataFrame:
    return query(
        """
        SELECT u.unit_id, u.description, u.category,
               u.rated_capacity, u.rated_capacity_unit,
               u.throughput_limit, u.throughput_limit_unit,
               u.control_devices, u.extra
        FROM bronze.state_air_permit_units u
        JOIN bronze.state_air_permits p ON u.permit_id = p.id
        WHERE p.facility_id = %(fid)s
           OR (p.state = 'IA' AND p.facility_name ILIKE %(name_like)s)
        ORDER BY u.unit_id
        """,
        {"fid": FACILITY_ID, "name_like": "%Eagle Grove%"},
    )


@st.cache_data(ttl=300)
def load_facility_state() -> dict:
    df = query(
        """
        SELECT * FROM silver.facility_state WHERE facility_id = %(fid)s
        """,
        {"fid": FACILITY_ID},
    )
    if df.empty:
        return {}
    return df.iloc[0].to_dict()


@st.cache_data(ttl=300)
def load_implied_monthly_crush() -> pd.DataFrame:
    return query(
        """
        SELECT year, month, year_month, state_crush_mil_bu,
               national_crush_bushels / 1e6 AS national_crush_mil_bu,
               is_inferred
        FROM silver.ia_implied_monthly_crush
        ORDER BY year DESC, month DESC
        LIMIT 24
        """
    )


@st.cache_data(ttl=300)
def load_recent_futures() -> pd.DataFrame:
    return query(
        """
        SELECT trade_date, symbol, contract_month, settlement
        FROM silver.futures_price
        WHERE symbol IN ('ZS','ZM','ZL')
          AND contract_month IN ('FRONT','K26','N26','Q26','U26','X26')
          AND trade_date >= CURRENT_DATE - INTERVAL '7 days'
        ORDER BY trade_date DESC, symbol, contract_month
        """
    )


@st.cache_data(ttl=300)
def load_recent_cash_meal_oil() -> pd.DataFrame:
    return query(
        """
        SELECT report_date, commodity, location, price, unit
        FROM bronze.ams_price_record
        WHERE (LOWER(commodity) LIKE '%%soybean meal%%' OR LOWER(commodity) LIKE '%%soybean oil%%')
          AND report_date >= CURRENT_DATE - INTERVAL '120 days'
        ORDER BY report_date DESC, commodity
        LIMIT 30
        """
    )


@st.cache_data(ttl=300)
def load_facility_basis() -> dict:
    """Pull Eagle Grove's local basis from the basis field."""
    df = query(
        """
        SELECT observation_date, commodity, delivery_month,
               basis_cents, std_err, n_samples, nearest_sample_mi,
               dist_to_cell_mi
        FROM gold.facility_basis
        WHERE facility_id = %(fid)s AND commodity = 'soybeans'
        ORDER BY observation_date DESC, dist_to_cell_mi ASC
        LIMIT 1
        """,
        {"fid": FACILITY_ID},
    )
    if df.empty:
        return {}
    return df.iloc[0].to_dict()


@st.cache_data(ttl=300)
def load_iowa_basis_surface() -> pd.DataFrame:
    """Pull the Iowa basis grid for heatmap overlay."""
    return query(
        """
        SELECT cell_lat, cell_lon, basis_cents
        FROM silver.basis_field_grid g
        WHERE commodity = 'soybeans' AND delivery_month = 'spot'
          AND observation_date = (
            SELECT MAX(observation_date) FROM silver.basis_field_grid
            WHERE commodity = 'soybeans' AND delivery_month = 'spot'
          )
          AND cell_lat BETWEEN 40.0 AND 44.0
          AND cell_lon BETWEEN -97.0 AND -90.0
        """
    )


@st.cache_data(ttl=300)
def load_basis_samples() -> pd.DataFrame:
    """Sample observations used to build the field — for transparency."""
    return query(
        """
        SELECT location_label, lat, lon, basis_cents, observation_date
        FROM bronze.cash_bid_observation
        WHERE commodity='soybeans' AND source='ams'
          AND observation_date = (
            SELECT MAX(observation_date) FROM bronze.cash_bid_observation
            WHERE commodity='soybeans' AND source='ams'
          )
          AND lat BETWEEN 40.0 AND 44.5
          AND lon BETWEEN -97.0 AND -90.0
        ORDER BY lat DESC
        """
    )


# ---------------------------------------------------------------------------
# Crush margin math (uses src.agents.facility.crush_economics)
# ---------------------------------------------------------------------------

def compute_board_crush(zs_settle: float, zm_settle: float, zl_settle: float) -> dict:
    """Standard board crush per bushel given front-month CME settlements.

    ZS in cents/bu, ZM in $/short ton, ZL in cents/lb.
    Soybean = 60 lb/bu; meal yield ≈ 47.5 lb/bu, oil yield ≈ 11.5 lb/bu (2000s standard
    assumed by CME for board-crush math).
    """
    soybean_per_bu = zs_settle / 100.0           # $/bu
    meal_per_bu = (zm_settle / 2000.0) * 47.5    # $/bu (47.5 lb meal per bu)
    oil_per_bu = (zl_settle / 100.0) * 11.0      # $/bu (11 lb oil per bu standard for board)

    crush_revenue = meal_per_bu + oil_per_bu
    board_crush = crush_revenue - soybean_per_bu
    return {
        "soybean_per_bu": soybean_per_bu,
        "meal_per_bu": meal_per_bu,
        "oil_per_bu": oil_per_bu,
        "crush_revenue": crush_revenue,
        "board_crush": board_crush,
    }


# ---------------------------------------------------------------------------
# Page styling
# ---------------------------------------------------------------------------

def inject_css():
    st.markdown(f"""
    <style>
    .stApp {{
        background: {COLORS['bg_dark']};
        color: {COLORS['text']};
    }}
    h1, h2, h3 {{
        font-weight: 600;
        letter-spacing: -0.02em;
        color: {COLORS['text']};
    }}
    .hero-name {{
        font-size: 3.2rem;
        font-weight: 700;
        letter-spacing: -0.03em;
        line-height: 1.05;
        color: {COLORS['text']};
        margin: 0;
    }}
    .hero-sub {{
        font-size: 1.15rem;
        color: {COLORS['text_dim']};
        margin-top: 0.3rem;
        font-weight: 300;
    }}
    .metric-card {{
        background: {COLORS['bg_card']};
        border-left: 3px solid {COLORS['primary_light']};
        border-radius: 4px;
        padding: 1.1rem 1.3rem;
    }}
    .metric-label {{
        font-size: 0.78rem;
        color: {COLORS['text_dim']};
        text-transform: uppercase;
        letter-spacing: 0.08em;
        font-weight: 500;
    }}
    .metric-value {{
        font-size: 2.0rem;
        font-weight: 700;
        font-family: 'JetBrains Mono', 'SF Mono', Consolas, monospace;
        color: {COLORS['text']};
        margin-top: 0.3rem;
        line-height: 1;
    }}
    .metric-unit {{
        font-size: 0.95rem;
        color: {COLORS['text_dim']};
        margin-left: 0.25rem;
        font-weight: 400;
    }}
    .metric-delta {{
        font-size: 0.8rem;
        color: {COLORS['accent']};
        margin-top: 0.4rem;
    }}
    .section-header {{
        font-size: 0.78rem;
        text-transform: uppercase;
        letter-spacing: 0.12em;
        color: {COLORS['accent']};
        font-weight: 600;
        margin-top: 2.4rem;
        margin-bottom: 0.6rem;
    }}
    .source-tag {{
        display: inline-block;
        font-size: 0.7rem;
        color: {COLORS['text_dim']};
        background: rgba(255,255,255,0.05);
        padding: 0.15rem 0.5rem;
        border-radius: 2px;
        margin-right: 0.4rem;
        font-family: 'JetBrains Mono', monospace;
    }}
    .stTabs [data-baseweb="tab"] {{
        background: transparent;
        color: {COLORS['text_dim']};
    }}
    .stTabs [aria-selected="true"] {{
        color: {COLORS['accent']} !important;
    }}
    div[data-testid="stMetric"] {{
        background: {COLORS['bg_card']};
        padding: 0.8rem;
        border-radius: 4px;
        border-left: 3px solid {COLORS['primary_light']};
    }}
    </style>
    """, unsafe_allow_html=True)


def metric_card(label: str, value: str, unit: str = "", delta: str = ""):
    delta_html = f'<div class="metric-delta">{delta}</div>' if delta else ""
    st.markdown(f"""
    <div class="metric-card">
      <div class="metric-label">{label}</div>
      <div class="metric-value">{value}<span class="metric-unit">{unit}</span></div>
      {delta_html}
    </div>
    """, unsafe_allow_html=True)


def section_header(text: str):
    st.markdown(f'<div class="section-header">{text}</div>', unsafe_allow_html=True)


# ---------------------------------------------------------------------------
# Process flow visualization (Sankey) — bespoke for Eagle Grove
# ---------------------------------------------------------------------------

def render_process_flow(units_df: pd.DataFrame):
    """Render a Sankey flow of the canonical crush process, with each
    process step's box width proportional to the number of emission units
    extracted in that category. Hover shows the unit IDs."""

    # Bucket extracted units into ontology steps
    cat_to_units = {}
    for _, row in units_df.iterrows():
        cat = (row["category"] or "other").strip().lower()
        cat_to_units.setdefault(cat, []).append(row)

    # For each ontology step, count how many units we have
    step_counts = []
    step_labels = []
    step_units = []
    for step_id, step_label, cats in PROCESS_FLOW:
        units = []
        for c in cats:
            units.extend(cat_to_units.get(c.lower(), []))
        step_counts.append(len(units))
        step_labels.append(f"{step_label}<br><span style='font-size:11px;color:#8FA095'>{len(units)} EUs</span>")
        step_units.append(units)

    # Build Sankey: bean input → each process step → meal/oil/hulls outputs
    # For visual clarity, only show steps with at least 1 EU
    visible_steps = [(i, label, units) for i, (label, units)
                     in enumerate(zip(step_labels, step_units)) if len(units) > 0]

    # Node 0 = "Soybeans In", followed by each visible step, then "Meal", "Oil", "Hulls" outputs
    node_labels = ["Soybeans<br>3,395 t/d"] + [s[1] for s in visible_steps] + ["Soybean Meal<br>2,665 t/d", "Soybean Oil<br>40 t/hr", "Hulls"]
    n_steps = len(visible_steps)

    sources = []
    targets = []
    values = []
    link_colors = []

    # Bean input → first process step (receiving)
    if n_steps > 0:
        sources.append(0)
        targets.append(1)
        values.append(100)
        link_colors.append("rgba(58, 122, 77, 0.55)")

    # Sequential flow between steps
    for i in range(n_steps - 1):
        sources.append(1 + i)
        targets.append(2 + i)
        values.append(100)
        link_colors.append("rgba(58, 122, 77, 0.4)")

    # Last step → outputs (meal/oil/hulls split)
    last_step_idx = n_steps  # node index of last process step
    meal_idx = n_steps + 1
    oil_idx = n_steps + 2
    hulls_idx = n_steps + 3

    sources.extend([last_step_idx, last_step_idx, last_step_idx])
    targets.extend([meal_idx, oil_idx, hulls_idx])
    values.extend([78, 18, 6])    # 78% meal, 18% oil, 6% hulls (per oilseed_crush.md)
    link_colors.extend([
        "rgba(212, 168, 72, 0.6)",   # meal — gold
        "rgba(75, 158, 184, 0.6)",   # oil — blue
        "rgba(180, 130, 75, 0.5)",   # hulls — brown
    ])

    node_colors = (
        [COLORS["accent"]] +
        [COLORS["process_step"]] * n_steps +
        ["#D4A848", "#4B9EB8", "#A87850"]
    )

    fig = go.Figure(data=[go.Sankey(
        arrangement="snap",
        node=dict(
            pad=22,
            thickness=26,
            line=dict(color="rgba(0,0,0,0)", width=0),
            label=node_labels,
            color=node_colors,
        ),
        link=dict(
            source=sources,
            target=targets,
            value=values,
            color=link_colors,
        ),
    )])
    fig.update_layout(
        font=dict(family="Inter, system-ui, sans-serif", size=12, color=COLORS["text"]),
        paper_bgcolor=COLORS["bg_dark"],
        plot_bgcolor=COLORS["bg_dark"],
        height=440,
        margin=dict(l=10, r=10, t=10, b=10),
    )
    return fig


# ---------------------------------------------------------------------------
# Main page
# ---------------------------------------------------------------------------

st.set_page_config(
    page_title=f"{FACILITY_DISPLAY} — Facility Deep Dive",
    page_icon="🌾",
    layout="wide",
)
inject_css()

# Hero
profile = load_facility_profile()
kg = load_kg_node()
units_df = load_emission_units()
fac_state = load_facility_state()
crush_history = load_implied_monthly_crush()
futures = load_recent_futures()
cash = load_recent_cash_meal_oil()
basis_info = load_facility_basis()
basis_surface = load_iowa_basis_surface()
basis_samples = load_basis_samples()

# =====================================================================
# HERO — split layout: brand block left, satellite map right
# =====================================================================

lat = float(profile.get('lat') or 42.664142)
lon = float(profile.get('lon') or -93.904623)
draw_radius_mi = float(profile.get('draw_radius_miles') or 50)

hero_left, hero_right = st.columns([1.05, 1.0], gap="large")

with hero_left:
    st.markdown(f"""
    <div style="padding: 1.2rem 0;">
      <div style="display: inline-block; padding: 0.35rem 0.75rem;
                  background: {COLORS['primary']}; color: {COLORS['accent']};
                  font-family: 'JetBrains Mono', monospace; font-size: 0.78rem;
                  letter-spacing: 0.18em; font-weight: 600; border-radius: 2px;">
        AGP &nbsp; · &nbsp; AG PROCESSING INC.
      </div>
      <div class="hero-name" style="margin-top: 0.9rem;">Eagle Grove</div>
      <div style="font-size: 1.4rem; color: {COLORS['text_dim']}; font-weight: 300;
                  margin-top: 0.1rem; letter-spacing: -0.01em;">
        Wright County, Iowa &nbsp;·&nbsp; 42.66°N 93.90°W
      </div>
      <div style="display: flex; gap: 0.6rem; margin-top: 1.4rem; flex-wrap: wrap;">
        <span style="background: {COLORS['bg_card']}; color: {COLORS['text']};
                     padding: 0.35rem 0.75rem; border-radius: 2px;
                     font-size: 0.82rem; border-left: 2px solid {COLORS['primary_light']};">
          Title V {profile.get('title_v_permit', '?')}
        </span>
        <span style="background: {COLORS['bg_card']}; color: {COLORS['text']};
                     padding: 0.35rem 0.75rem; border-radius: 2px;
                     font-size: 0.82rem; border-left: 2px solid {COLORS['primary_light']};">
          Co-op · farmer-owned
        </span>
        <span style="background: {COLORS['bg_card']}; color: {COLORS['text']};
                     padding: 0.35rem 0.75rem; border-radius: 2px;
                     font-size: 0.82rem; border-left: 2px solid {COLORS['accent']};">
          NOPA member
        </span>
        <span style="background: {COLORS['bg_card']}; color: {COLORS['text']};
                     padding: 0.35rem 0.75rem; border-radius: 2px;
                     font-size: 0.82rem; border-left: 2px solid {COLORS['primary_light']};">
          {draw_radius_mi:.0f}-mile draw radius
        </span>
      </div>
    </div>
    """, unsafe_allow_html=True)

    # Hero metrics in 2x2 grid below the brand block
    m1, m2 = st.columns(2)
    nameplate_tpd = float(profile.get('nameplate_tpd') or 0) or (153.75 * 24)
    annual_bu = nameplate_tpd * 350 * 36.74 / 1e6
    with m1:
        metric_card("Design crush", f"{nameplate_tpd:,.0f}", "tons/day")
        metric_card("Operating year", f"{profile.get('operating_days_year', 350)}", "days")
    with m2:
        metric_card("Annual capacity", f"{annual_bu:.1f}", "mil bu/yr")
        metric_card("Refining", profile.get('refining_capability') or 'RB',
                    f"@ {profile.get('refining_capacity', '40 tph')}")

with hero_right:
    # Hero map: basis surface (the "field" Eagle Grove plugs into) layered
    # over Esri satellite tiles. Zoom shows all of Iowa so the gradient is
    # visible. User can pan/zoom in to see satellite imagery at the facility.
    fmap = folium.Map(
        location=[lat, lon], zoom_start=7,
        tiles=None, control_scale=True,
        prefer_canvas=True,
    )
    folium.TileLayer(
        tiles="https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}",
        attr="Esri World Imagery",
        name="Satellite",
        overlay=False,
        control=False,
    ).add_to(fmap)

    # Basis-field heatmap overlay — colored cells red→green (wider→tighter)
    if basis_surface is not None and not basis_surface.empty:
        bmin = float(basis_surface['basis_cents'].min())
        bmax = float(basis_surface['basis_cents'].max())
        for _, row in basis_surface.iterrows():
            b = float(row['basis_cents'])
            t = (b - bmin) / (bmax - bmin) if bmax != bmin else 0.5
            r_, g_ = int(255*(1-t)), int(180*t + 60)
            color = f"#{r_:02x}{g_:02x}3c"
            folium.Rectangle(
                bounds=[
                    [float(row['cell_lat']) - 0.125, float(row['cell_lon']) - 0.125],
                    [float(row['cell_lat']) + 0.125, float(row['cell_lon']) + 0.125],
                ],
                color=None, weight=0,
                fill=True, fill_color=color, fill_opacity=0.50,
                popup=f"{b:.0f}¢ vs futures",
            ).add_to(fmap)

        # Sample points (gold dots — the AMS regional centroids driving the field)
        if basis_samples is not None and not basis_samples.empty:
            for _, srow in basis_samples.iterrows():
                folium.CircleMarker(
                    location=[float(srow['lat']), float(srow['lon'])],
                    radius=6, color="#FFFFFF", weight=1.5,
                    fill=True, fill_color=COLORS['accent'], fill_opacity=0.95,
                    popup=f"<b>{srow['location_label'][:60]}</b><br>"
                          f"{float(srow['basis_cents']):.0f}¢ vs futures (observed)",
                ).add_to(fmap)

    # Eagle Grove draw radius (50mi)
    folium.Circle(
        location=[lat, lon],
        radius=draw_radius_mi * 1609.34,
        color=COLORS['primary_light'], weight=2.0, opacity=0.85,
        fill=False,
        popup=f"{draw_radius_mi:.0f}-mile soybean draw area",
    ).add_to(fmap)

    # Eagle Grove marker (top of stack)
    bp_now = float(basis_info.get('basis_cents') or 0) if basis_info else 0
    folium.Marker(
        location=[lat, lon],
        popup=f"<b>AGP Eagle Grove</b><br>"
              f"Field-derived basis: <strong>{bp_now:.1f}¢</strong> vs futures<br>"
              f"({profile.get('city')}, IA · {draw_radius_mi:.0f}mi draw)",
        icon=folium.Icon(color="green", icon="industry", prefix="fa"),
    ).add_to(fmap)

    # Frame on Iowa to show the gradient
    fmap.fit_bounds([[40.3, -96.7], [43.6, -90.1]])

    st_folium(
        fmap,
        height=420,
        width=None,
        returned_objects=[],
    )
    st.markdown(
        f'<div style="font-size: 0.75rem; color: {COLORS["text_dim"]}; '
        f'margin-top: -0.6rem; line-height: 1.5;">'
        f'<strong style="color: {COLORS["accent"]}">Gold dots</strong> = AMS regional bid samples · '
        f'<strong>Cells</strong> red→green (wider→tighter basis) · '
        f'<strong style="color: #4C9F70">Green pin</strong> = AGP Eagle Grove · '
        f'Pan/zoom to see satellite imagery at the facility · '
        f'Imagery © Esri</div>',
        unsafe_allow_html=True,
    )

# Live crush margin
section_header("LIVE BOARD CRUSH MARGIN — CME Front Month")

if not futures.empty:
    front = futures[futures['contract_month'] == 'FRONT'].set_index('symbol')
    try:
        zs = float(front.loc['ZS', 'settlement'].iloc[0] if hasattr(front.loc['ZS', 'settlement'], 'iloc') else front.loc['ZS', 'settlement'])
        zm = float(front.loc['ZM', 'settlement'].iloc[0] if hasattr(front.loc['ZM', 'settlement'], 'iloc') else front.loc['ZM', 'settlement'])
        zl = float(front.loc['ZL', 'settlement'].iloc[0] if hasattr(front.loc['ZL', 'settlement'], 'iloc') else front.loc['ZL', 'settlement'])
        crush = compute_board_crush(zs, zm, zl)

        c1, c2, c3, c4 = st.columns(4)
        with c1: metric_card("Soybeans (ZS)", f"${crush['soybean_per_bu']:.2f}", "/bu", f"ZS front {zs:.2f}¢")
        with c2: metric_card("Meal value", f"${crush['meal_per_bu']:.2f}", "/bu", f"ZM front ${zm:.2f}/ton")
        with c3: metric_card("Oil value", f"${crush['oil_per_bu']:.2f}", "/bu", f"ZL front {zl:.2f}¢/lb")
        with c4:
            color = COLORS['accent'] if crush['board_crush'] > 0 else COLORS['danger']
            st.markdown(f"""
            <div class="metric-card" style="border-left-color: {color}">
              <div class="metric-label">Board Crush</div>
              <div class="metric-value" style="color: {color}">${crush['board_crush']:.2f}<span class="metric-unit">/bu</span></div>
              <div class="metric-delta">Annual (43.7 mil bu) ≈ ${crush['board_crush']*43.7:.1f}M gross</div>
            </div>
            """, unsafe_allow_html=True)
    except (KeyError, IndexError, ValueError) as e:
        st.warning(f"Front-month futures incomplete: {e}")
else:
    st.info("No recent futures data — load CME settlements via yfinance_futures collector.")

# =====================================================================
# LOCAL BASIS — Eagle Grove plugged into the basis field
# =====================================================================

section_header("LOCAL BASIS — EAGLE GROVE PLUGGED INTO THE US BASIS FIELD")

if basis_info:
    bc1, bc2, bc3, bc4 = st.columns(4)
    with bc1:
        bp = float(basis_info.get('basis_cents') or 0)
        metric_card("Field-derived basis", f"{bp:.1f}", "¢ vs futures",
                    "soybeans, spot delivery")
    with bc2:
        std_err = float(basis_info.get('std_err') or 0)
        metric_card("Field uncertainty", f"±{std_err:.1f}", "¢ std err",
                    f"n={basis_info.get('n_samples')} samples")
    with bc3:
        nearest = float(basis_info.get('nearest_sample_mi') or 0)
        metric_card("Nearest sample", f"{nearest:.0f}", "miles",
                    "AMS regional bid")
    with bc4:
        # Apply basis to board crush
        try:
            zs_settle = float(front.loc['ZS', 'settlement'].iloc[0] if hasattr(front.loc['ZS', 'settlement'], 'iloc') else front.loc['ZS', 'settlement'])
            local_soy = (zs_settle + bp) / 100.0
            local_crush = crush['crush_revenue'] - local_soy
            color = COLORS['accent'] if local_crush > 0 else COLORS['danger']
            st.markdown(f"""
            <div class="metric-card" style="border-left-color: {color}">
              <div class="metric-label">Eagle Grove Crush Margin</div>
              <div class="metric-value" style="color: {color}">${local_crush:.2f}<span class="metric-unit">/bu</span></div>
              <div class="metric-delta">Board ${crush['board_crush']:.2f} + basis {bp:.0f}¢ = local economic margin</div>
            </div>
            """, unsafe_allow_html=True)
        except Exception:
            metric_card("Eagle Grove margin", "—", "/bu", "needs futures + basis")

    # Basis surface map already rendered in the hero — no duplicate here.
    # The metrics row above shows facility-specific basis pulled from the field.


# Process flow
section_header("CRUSHING PROCESS — 52 PERMITTED EMISSION UNITS, ROUTED TO CANONICAL FLOW")
fig = render_process_flow(units_df)
st.plotly_chart(fig, width="stretch")
st.markdown(
    f'<div style="font-size: 0.85rem; color: {COLORS["text_dim"]}; margin-top: -0.5rem;">'
    f'Each process step shows the count of Title V emission units extracted in that category. '
    f'Mass-flow ratios (78% meal / 18% oil / 6% hulls) come from <span class="source-tag">domain_knowledge/process_flows/oilseed_crush.md</span>.'
    f'</div>',
    unsafe_allow_html=True,
)

# Tabs for deeper sections
tab1, tab2, tab3, tab4 = st.tabs(["Equipment Detail", "Throughput History", "Strategic Plan (Q view)", "Data Provenance"])

with tab1:
    section_header("EQUIPMENT INVENTORY (FROM IOWA DNR TITLE V)")
    cap_df = units_df.copy()
    cap_df["rated_capacity"] = pd.to_numeric(cap_df["rated_capacity"], errors='coerce')
    cap_df = cap_df[["unit_id", "category", "description", "rated_capacity", "rated_capacity_unit"]]
    cap_df.columns = ["Unit ID", "Category", "Description", "Rated Capacity", "Unit"]
    st.dataframe(cap_df, width="stretch", hide_index=True, height=460)

    section_header("CAPACITY HISTOGRAM — TONS/HR-RATED EQUIPMENT")
    tph = units_df[units_df['rated_capacity_unit'].astype(str).str.contains('tons/hr|tons/hour', case=False, na=False)].copy()
    tph['rated_capacity'] = pd.to_numeric(tph['rated_capacity'], errors='coerce')
    tph = tph.dropna(subset=['rated_capacity']).sort_values('rated_capacity', ascending=True)
    if not tph.empty:
        bar = go.Figure(go.Bar(
            x=tph['rated_capacity'],
            y=tph['unit_id'] + ': ' + tph['description'].str[:40],
            orientation='h',
            marker_color=COLORS['primary_light'],
        ))
        bar.update_layout(
            paper_bgcolor=COLORS["bg_dark"],
            plot_bgcolor=COLORS["bg_dark"],
            font=dict(color=COLORS["text"]),
            xaxis_title="tons/hr",
            yaxis_title="",
            height=480,
            margin=dict(l=10, r=10, t=10, b=30),
        )
        st.plotly_chart(bar, width="stretch")

with tab2:
    section_header("IMPLIED MONTHLY CRUSH (NOPA-DERIVED STATE SHARE × FACILITY CAPACITY %)")
    if not crush_history.empty:
        # Eagle Grove's facility capacity share in IA = 38.4 / total IA nameplate
        # For now, use a 4.7% share placeholder until we wire it from reference table
        EAGLE_SHARE = 0.047
        ch = crush_history.copy()
        ch['eagle_grove_implied_mil_bu'] = ch['state_crush_mil_bu'] * EAGLE_SHARE
        ch = ch.sort_values('year_month')

        fig2 = go.Figure()
        fig2.add_trace(go.Scatter(
            x=ch['year_month'], y=ch['eagle_grove_implied_mil_bu'],
            mode='lines+markers',
            line=dict(color=COLORS['accent'], width=2),
            marker=dict(size=6),
            name='Eagle Grove implied',
        ))
        fig2.update_layout(
            paper_bgcolor=COLORS["bg_dark"],
            plot_bgcolor=COLORS["bg_dark"],
            font=dict(color=COLORS["text"]),
            xaxis_title="",
            yaxis_title="mil bu/month",
            height=320,
            margin=dict(l=10, r=10, t=10, b=10),
        )
        st.plotly_chart(fig2, width="stretch")
        st.caption(
            "Iowa-share method: national NOPA monthly × IA capacity share × Eagle Grove share of IA. "
            "Replace with NOPA-Iowa observed (when published) or facility-direct via AGP investor data."
        )
    else:
        st.info("No NOPA history loaded.")

with tab3:
    section_header("STRATEGIC QUARTERLY ASSESSMENT")
    st.markdown(f"""
    <div style="background: {COLORS['bg_card']}; padding: 1.2rem 1.4rem; border-radius: 4px; border-left: 3px solid {COLORS['accent']};">
    <div style="color: {COLORS['accent']}; font-size: 0.78rem; font-weight: 600; letter-spacing: 0.08em; text-transform: uppercase;">PLACEHOLDER — Layer 1 Strategic Plan (per Phase Two architecture)</div>
    <p style="color: {COLORS['text_dim']}; margin-top: 0.7rem; font-size: 0.9rem;">
    The Phase Two: Facility Agent Architecture document specifies that a quarterly Claude Opus run produces
    a versioned <code>strategic_plan</code> in silver, capturing target coverage ratios, basis bid ceilings,
    and hedge ratios per facility. That run hasn't been wired up yet. The placeholder here stands in until
    we ship the Layer 1 agent.
    </p>
    </div>
    """, unsafe_allow_html=True)

    cc1, cc2, cc3 = st.columns(3)
    with cc1:
        metric_card("Target coverage", "65%", "of next 90 days", "vs basis-bid ceiling +5¢")
    with cc2:
        metric_card("Hedge ratio", "0.85", "ZS:ZM:ZL composite", "of expected output")
    with cc3:
        metric_card("Basis bid ceiling", "−25¢", "K26 ZS basis", "no-buy above this")

with tab4:
    section_header("DATA PROVENANCE")
    st.markdown(f"""
    <div style="font-size: 0.92rem; line-height: 1.7;">
    <span class="source-tag">reference.oilseed_crush_facilities</span> facility static profile<br>
    <span class="source-tag">core.kg_node:ia.agp_eagle_grove</span> KG node + properties<br>
    <span class="source-tag">bronze.state_air_permits.id=3</span> Iowa DNR Title V 05-TV-005R3, extracted via local Ollama (qwen2.5:7b) 2026-05-01<br>
    <span class="source-tag">bronze.state_air_permit_units</span> 52 emission units with rated capacities<br>
    <span class="source-tag">silver.facility_state</span> mutable state (current crush rate, inventories, forward book)<br>
    <span class="source-tag">silver.ia_implied_monthly_crush</span> Iowa state share of national NOPA<br>
    <span class="source-tag">silver.futures_price</span> CME ZS / ZM / ZL settlements<br>
    <span class="source-tag">bronze.ams_price_record</span> USDA AMS soybean meal & oil cash<br>
    <span class="source-tag">domain_knowledge/process_flows/oilseed_crush.md</span> canonical process ontology + diagnostic ratios
    </div>
    """, unsafe_allow_html=True)

    st.markdown("---")
    st.caption(f"Generated {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} · Eagle Grove deep dive · Phase Two Layer 4 prototype")
