"""
US Basis Field Viewer — standalone, facility-agnostic.

The basis field as its own object: zoom in on any region, switch commodity,
delivery month, date. Optional facility overlay. Optional sample-point
visibility. Built for both internal analysis and prospect demos.

Run:
    cd C:\\dev\\RLC-Agent
    streamlit run dashboards/basis_field/app.py --server.port 8521

Architecture (per project_basis_field.md three-layer model):
- This page renders Layer 1 only — the geographic-economic field.
- Layer 2 (facility identity premium) and Layer 3 (competitive uplift) are
  facility-specific and not shown here.
"""
from __future__ import annotations

from datetime import date, datetime, timedelta

import folium
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
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

COLORS = {
    "primary": "#1F4D2C",
    "primary_light": "#3A7A4D",
    "accent": "#D4A848",
    "bg_dark": "#0E1614",
    "bg_card": "#1A2521",
    "text": "#E8EDE9",
    "text_dim": "#8FA095",
    "danger": "#C45B4D",
}

# Commodity selector — only commodities with enough sample density to interpolate
COMMODITY_OPTIONS = {
    "soybeans":     {"label": "Soybeans (input)",       "ready": True,  "min_samples": 30},
    "corn":         {"label": "Corn (input)",           "ready": True,  "min_samples": 30},
    "wheat":        {"label": "Wheat (input)",          "ready": True,  "min_samples": 20},
    "sorghum":      {"label": "Sorghum (input, sparse)", "ready": True, "min_samples": 10},
    "soybean_oil":  {"label": "Soybean Oil (output) — pending more sources", "ready": False, "min_samples": 8},
    "soybean_meal": {"label": "Soybean Meal (output) — pending more sources", "ready": False, "min_samples": 8},
}

UNCERTAINTY_THRESHOLD_CENTS = 6.0   # cells with std_err above this get hatched
HATCH_LINES_PER_CELL = 4            # number of diagonal hatch strokes per uncertain cell

# Coloring modes for the cell heatmap
COLOR_MODES = {
    "Basis (¢ vs futures)":  "basis",
    "Uncertainty (std err)": "uncertainty",
    "Sample density":        "density",
}


# Bounding box presets
REGION_PRESETS = {
    "Continental US (Corn Belt + Plains + Mid-South)": (35.0, 49.0, -104.0, -82.0, 4),
    "Iowa close-up":                                   (40.3, 43.6, -96.7, -90.1, 7),
    "Illinois":                                        (37.0, 42.5, -91.5, -87.5, 7),
    "Missouri":                                        (36.0, 40.6, -95.8, -89.1, 7),
    "Nebraska":                                        (40.0, 43.0, -104.0, -95.3, 7),
    "Indiana / Ohio":                                  (37.7, 41.8, -88.1, -80.5, 7),
    "South Plains (KS, OK, TX)":                       (33.0, 40.0, -103.5, -94.6, 6),
    "Mid-South (TN, AR, MS)":                          (32.0, 36.5, -94.6, -88.2, 6),
}


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


@st.cache_data(ttl=300)
def latest_field_date(commodity: str, delivery: str) -> date | None:
    df = query(
        """
        SELECT MAX(observation_date) AS d
        FROM silver.basis_field_grid
        WHERE commodity = %(c)s AND delivery_month = %(d)s
        """,
        {"c": commodity, "d": delivery},
    )
    if df.empty or pd.isna(df.iloc[0]["d"]):
        return None
    return df.iloc[0]["d"]


@st.cache_data(ttl=300)
def load_grid(commodity: str, delivery: str, observation_date: date,
              lat_min: float, lat_max: float, lon_min: float, lon_max: float) -> pd.DataFrame:
    return query(
        """
        SELECT cell_lat, cell_lon, basis_cents, std_err, n_samples, nearest_sample_mi
        FROM silver.basis_field_grid
        WHERE commodity = %(c)s AND delivery_month = %(d)s
          AND observation_date = %(dt)s
          AND cell_lat BETWEEN %(lat_min)s AND %(lat_max)s
          AND cell_lon BETWEEN %(lon_min)s AND %(lon_max)s
        ORDER BY cell_lat DESC, cell_lon ASC
        """,
        {"c": commodity, "d": delivery, "dt": observation_date,
         "lat_min": lat_min, "lat_max": lat_max,
         "lon_min": lon_min, "lon_max": lon_max},
    )


@st.cache_data(ttl=300)
def load_samples(commodity: str, observation_date: date,
                 lat_min: float, lat_max: float, lon_min: float, lon_max: float) -> pd.DataFrame:
    return query(
        """
        SELECT location_label, lat, lon, basis_cents, cash_price, observation_date
        FROM bronze.cash_bid_observation
        WHERE commodity = %(c)s
          AND observation_date = (
            SELECT MAX(observation_date) FROM bronze.cash_bid_observation
            WHERE commodity = %(c)s AND observation_date <= %(dt)s
          )
          AND lat BETWEEN %(lat_min)s AND %(lat_max)s
          AND lon BETWEEN %(lon_min)s AND %(lon_max)s
        """,
        {"c": commodity, "dt": observation_date,
         "lat_min": lat_min, "lat_max": lat_max,
         "lon_min": lon_min, "lon_max": lon_max},
    )


@st.cache_data(ttl=300)
def load_facilities(lat_min: float, lat_max: float, lon_min: float, lon_max: float) -> pd.DataFrame:
    return query(
        """
        SELECT facility_id, name, operator, city, state, lat, lon,
               nameplate_tpd, primary_oilseed, status
        FROM reference.oilseed_crush_facilities
        WHERE lat IS NOT NULL AND lon IS NOT NULL
          AND lat BETWEEN %(lat_min)s AND %(lat_max)s
          AND lon BETWEEN %(lon_min)s AND %(lon_max)s
        """,
        {"lat_min": lat_min, "lat_max": lat_max,
         "lon_min": lon_min, "lon_max": lon_max},
    )


# ---------------------------------------------------------------------------
# Styling
# ---------------------------------------------------------------------------

def inject_css():
    st.markdown(f"""
    <style>
    .stApp {{ background: {COLORS['bg_dark']}; color: {COLORS['text']}; }}
    h1, h2, h3 {{ font-weight: 600; letter-spacing: -0.02em; color: {COLORS['text']}; }}
    .hero-name {{ font-size: 2.6rem; font-weight: 700; letter-spacing: -0.03em; line-height: 1.05; margin: 0; }}
    .hero-sub {{ font-size: 1.05rem; color: {COLORS['text_dim']}; margin-top: 0.3rem; font-weight: 300; }}
    .metric-card {{ background: {COLORS['bg_card']}; border-left: 3px solid {COLORS['primary_light']};
                    border-radius: 4px; padding: 1.0rem 1.2rem; }}
    .metric-label {{ font-size: 0.78rem; color: {COLORS['text_dim']};
                     text-transform: uppercase; letter-spacing: 0.08em; font-weight: 500; }}
    .metric-value {{ font-size: 1.8rem; font-weight: 700;
                     font-family: 'JetBrains Mono', 'SF Mono', Consolas, monospace;
                     color: {COLORS['text']}; margin-top: 0.3rem; line-height: 1; }}
    .metric-unit {{ font-size: 0.85rem; color: {COLORS['text_dim']}; margin-left: 0.25rem; font-weight: 400; }}
    .legend-row {{ font-size: 0.85rem; color: {COLORS['text_dim']}; line-height: 1.6; }}
    [data-testid="stSidebar"] {{ background: {COLORS['bg_card']}; }}
    [data-testid="stSidebar"] label, [data-testid="stSidebar"] p {{ color: {COLORS['text']}; }}
    div[data-testid="stMetric"] {{ background: {COLORS['bg_card']}; padding: 0.7rem;
                                    border-radius: 4px; border-left: 3px solid {COLORS['primary_light']}; }}
    </style>
    """, unsafe_allow_html=True)


def metric_card(label, value, unit=""):
    st.markdown(f"""
    <div class="metric-card">
      <div class="metric-label">{label}</div>
      <div class="metric-value">{value}<span class="metric-unit">{unit}</span></div>
    </div>
    """, unsafe_allow_html=True)


# ---------------------------------------------------------------------------
# Page
# ---------------------------------------------------------------------------

st.set_page_config(
    page_title="US Basis Field — Multi-Commodity Viewer",
    page_icon="🌾",
    layout="wide",
)
inject_css()

# Sidebar controls
st.sidebar.markdown(f"<h3 style='color:{COLORS['accent']};margin-top:0;'>Field Controls</h3>",
                    unsafe_allow_html=True)

commodity_label = st.sidebar.selectbox(
    "Commodity",
    [c for c, m in COMMODITY_OPTIONS.items() if m["ready"]],
    format_func=lambda c: COMMODITY_OPTIONS[c]["label"],
)
delivery = st.sidebar.selectbox("Delivery month", ["spot", "K26", "N26", "Q26", "U26", "X26"])

region_label = st.sidebar.selectbox("Region", list(REGION_PRESETS.keys()))
lat_min, lat_max, lon_min, lon_max, default_zoom = REGION_PRESETS[region_label]

# Date selector
default_date = latest_field_date(commodity_label, delivery) or date.today()
selected_date = st.sidebar.date_input(
    "Field date",
    value=default_date,
    min_value=date.today() - timedelta(days=180),
    max_value=date.today(),
)

# Coloring mode
color_mode_label = st.sidebar.selectbox(
    "Color cells by",
    list(COLOR_MODES.keys()),
    index=0,
)
color_mode = COLOR_MODES[color_mode_label]

# Overlay toggles
st.sidebar.markdown("---")
st.sidebar.markdown("**Overlays**")
show_samples = st.sidebar.checkbox("Show AMS sample points", value=True)
show_facilities = st.sidebar.checkbox("Show oilseed crusher facilities", value=False)
show_uncertainty_hatch = st.sidebar.checkbox(
    f"Hatch high-uncertainty cells (std_err > {UNCERTAINTY_THRESHOLD_CENTS:.0f}¢)",
    value=True,
    help="Diagonal stripes mark cells where the field is making a less-confident prediction.",
)
show_basis_contours = st.sidebar.checkbox(
    "Basis contour lines (every 5¢)",
    value=False,
    help="Iso-basis lines so you can read gradients at a glance — like a topo map.",
)

# Sidebar — note about not-yet-ready commodities
st.sidebar.markdown("---")
st.sidebar.markdown("**Pending commodities**")
for c, m in COMMODITY_OPTIONS.items():
    if not m["ready"]:
        st.sidebar.markdown(
            f'<div style="font-size:0.78rem;color:{COLORS["text_dim"]};margin:0.2rem 0;">'
            f'• {m["label"]}</div>',
            unsafe_allow_html=True,
        )

# Hero
st.markdown(f"""
<div style="margin-top: 0.3rem; margin-bottom: 1.2rem;">
  <div style="display: inline-block; padding: 0.32rem 0.7rem; background: {COLORS['primary']};
              color: {COLORS['accent']}; font-family: 'JetBrains Mono', monospace;
              font-size: 0.74rem; letter-spacing: 0.18em; font-weight: 600; border-radius: 2px;">
    US BASIS FIELD &nbsp;·&nbsp; LAYER 1 (GEOGRAPHIC-ECONOMIC)
  </div>
  <div class="hero-name" style="margin-top: 0.7rem;">
    {COMMODITY_OPTIONS[commodity_label]["label"]} · {delivery}
  </div>
  <div class="hero-sub">
    {region_label} &nbsp;·&nbsp; {selected_date}  &nbsp;·&nbsp;
    Interpolated from USDA AMS regional bid samples · IDW v1
  </div>
</div>
""", unsafe_allow_html=True)

# Load data
grid = load_grid(commodity_label, delivery, selected_date,
                 lat_min, lat_max, lon_min, lon_max)
samples = load_samples(commodity_label, selected_date,
                       lat_min, lat_max, lon_min, lon_max) if show_samples else pd.DataFrame()
facilities = load_facilities(lat_min, lat_max, lon_min, lon_max) if show_facilities else pd.DataFrame()

# Top metrics row
col1, col2, col3, col4 = st.columns(4)
with col1:
    metric_card("Grid cells", f"{len(grid):,}", "interpolated")
with col2:
    metric_card("Sample points", f"{len(samples) if not samples.empty else 0}", "in view")
with col3:
    if not grid.empty:
        bmin = float(grid['basis_cents'].min())
        bmax = float(grid['basis_cents'].max())
        metric_card("Basis range", f"{bmin:.0f} to {bmax:.0f}", "¢")
    else:
        metric_card("Basis range", "—", "")
with col4:
    if not grid.empty:
        avg_se = float(grid['std_err'].dropna().mean()) if grid['std_err'].notna().any() else 0
        metric_card("Avg uncertainty", f"±{avg_se:.1f}", "¢ std err")
    else:
        metric_card("Avg uncertainty", "—", "")

# Map
center_lat = (lat_min + lat_max) / 2
center_lon = (lon_min + lon_max) / 2

fmap = folium.Map(
    location=[center_lat, center_lon],
    zoom_start=default_zoom,
    tiles=None, control_scale=True, prefer_canvas=True,
)
folium.TileLayer(
    tiles="https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}",
    attr="Esri", name="Satellite", overlay=False, control=False,
).add_to(fmap)

# Field cells — color by selected mode (basis / uncertainty / density)
def color_for_value(t: float, mode: str) -> str:
    """Map normalized value [0..1] to a hex color per the selected mode."""
    t = max(0.0, min(1.0, t))
    if mode == "basis":
        # Red (wide basis) → green (tight basis)
        r_, g_ = int(255 * (1 - t)), int(180 * t + 60)
        return f"#{r_:02x}{g_:02x}3c"
    elif mode == "uncertainty":
        # White/cream (low std_err = confident) → dark red (high std_err = guessing)
        r_ = 240
        g_ = int(220 * (1 - t) + 30)
        b_ = int(180 * (1 - t) + 30)
        return f"#{r_:02x}{g_:02x}{b_:02x}"
    elif mode == "density":
        # White (few samples) → dark blue (many samples)
        r_ = int(220 * (1 - t) + 30)
        g_ = int(220 * (1 - t) + 80)
        b_ = int(180 * (1 - 0.4*t) + 60)
        return f"#{r_:02x}{g_:02x}{b_:02x}"
    return "#888888"


def add_hatch(fmap, lat_c: float, lon_c: float, half_size: float = 0.125,
              n_lines: int = HATCH_LINES_PER_CELL, color: str = "#1A1A1A",
              opacity: float = 0.55, weight: float = 1.2):
    """Draw n parallel diagonal lines (NE-SW direction) inside a cell.
    Lines run from the lower-left edge to the upper-right edge, offset
    perpendicular to the diagonal so they appear as evenly-spaced hatching."""
    # Diagonal lines from the cell's bottom-left to top-right, offset along the
    # perpendicular axis (NW-SE direction).
    for i in range(n_lines):
        # Offset normalized to [-0.85 .. +0.85] of the cell half-size, in the
        # NW-SE direction (lat decreases as lon decreases for SW corner)
        frac = (i + 0.5) / n_lines  # 0..1
        offset = (frac - 0.5) * 1.7 * half_size  # spans most of cell
        # Line endpoints (clip diagonals at cell edges where they would overflow)
        # Simple approach: a NE-SW diagonal segment offset perpendicular
        p1 = (lat_c - half_size + offset, lon_c - half_size - offset)
        p2 = (lat_c + half_size + offset, lon_c + half_size - offset)
        # Clip into the cell bounds
        def clip_pt(lat, lon):
            return (
                max(lat_c - half_size, min(lat_c + half_size, lat)),
                max(lon_c - half_size, min(lon_c + half_size, lon)),
            )
        folium.PolyLine(
            locations=[clip_pt(*p1), clip_pt(*p2)],
            color=color, weight=weight, opacity=opacity,
        ).add_to(fmap)


if not grid.empty:
    # Compute the value range for the selected color mode
    if color_mode == "basis":
        vals = grid['basis_cents'].astype(float)
        vmin, vmax = float(vals.min()), float(vals.max())
        legend_low, legend_high = "wider (red)", "tighter (green)"
    elif color_mode == "uncertainty":
        vals = grid['std_err'].fillna(0).astype(float)
        vmin, vmax = float(vals.min()), float(vals.max())
        legend_low, legend_high = "confident (cream)", "guessing (red)"
    else:  # density
        vals = grid['n_samples'].fillna(0).astype(float)
        vmin, vmax = float(vals.min()), float(vals.max())
        legend_low, legend_high = "few (light)", "many (blue)"

    for _, row in grid.iterrows():
        if color_mode == "basis":
            v = float(row['basis_cents'])
        elif color_mode == "uncertainty":
            v = float(row['std_err'] or 0)
        else:
            v = float(row['n_samples'] or 0)
        t = (v - vmin) / (vmax - vmin) if vmax != vmin else 0.5
        color = color_for_value(t, color_mode)

        cell_lat = float(row['cell_lat'])
        cell_lon = float(row['cell_lon'])
        std_err_v = float(row['std_err'] or 0)

        folium.Rectangle(
            bounds=[
                [cell_lat - 0.125, cell_lon - 0.125],
                [cell_lat + 0.125, cell_lon + 0.125],
            ],
            color=None, weight=0,
            fill=True, fill_color=color, fill_opacity=0.55,
            popup=(f"<b>{float(row['basis_cents']):.0f}¢</b> vs futures<br>"
                   f"±{std_err_v:.1f}¢ std err<br>"
                   f"n={row['n_samples']} samples · nearest {row['nearest_sample_mi'] or 0:.0f}mi"),
        ).add_to(fmap)

        # Hatch high-uncertainty cells
        if show_uncertainty_hatch and std_err_v > UNCERTAINTY_THRESHOLD_CENTS:
            add_hatch(fmap, cell_lat, cell_lon)

# Basis contour lines — only on basis-color mode (other modes don't pair)
if show_basis_contours and not grid.empty and color_mode == "basis":
    # Reshape grid into 2D array
    g = grid.copy()
    g['cell_lat'] = g['cell_lat'].astype(float).round(4)
    g['cell_lon'] = g['cell_lon'].astype(float).round(4)
    g['basis_cents'] = g['basis_cents'].astype(float)
    pivot = g.pivot_table(
        index='cell_lat', columns='cell_lon', values='basis_cents', aggfunc='mean'
    )
    if not pivot.empty and pivot.shape[0] > 2 and pivot.shape[1] > 2:
        lats_arr = pivot.index.to_numpy()
        lons_arr = pivot.columns.to_numpy()
        Z = pivot.to_numpy()
        # Levels every 5¢
        z_min = float(np.nanmin(Z))
        z_max = float(np.nanmax(Z))
        levels = np.arange(np.floor(z_min/5)*5, np.ceil(z_max/5)*5 + 1, 5)
        # Compute contours headlessly
        fig_h, ax_h = plt.subplots()
        cs = ax_h.contour(lons_arr, lats_arr, Z, levels=levels)
        # Extract contour line segments and render them via folium
        for level_idx, level_val in enumerate(cs.levels):
            try:
                segs = cs.allsegs[level_idx]
            except (AttributeError, IndexError):
                segs = []
            for seg in segs:
                if len(seg) < 2:
                    continue
                # seg is (N, 2) array of (lon, lat) — flip to (lat, lon)
                pts = [(float(p[1]), float(p[0])) for p in seg]
                folium.PolyLine(
                    locations=pts,
                    color="#FFFFFF",
                    weight=1.0,
                    opacity=0.55,
                    popup=f"{int(level_val)}¢ basis iso-line",
                ).add_to(fmap)
        plt.close(fig_h)

# Sample dots
if not samples.empty:
    for _, row in samples.iterrows():
        b = float(row['basis_cents']) if pd.notna(row['basis_cents']) else 0
        folium.CircleMarker(
            location=[float(row['lat']), float(row['lon'])],
            radius=7, color="#FFFFFF", weight=1.5,
            fill=True, fill_color=COLORS['accent'], fill_opacity=0.95,
            popup=f"<b>{row['location_label'][:60]}</b><br>"
                  f"<b>{b:.0f}¢</b> vs futures (observed)<br>"
                  f"date {row['observation_date']}",
        ).add_to(fmap)

# Facility overlay
if not facilities.empty:
    for _, row in facilities.iterrows():
        # Pull the field's basis at this facility for the popup
        nearest = grid.iloc[((grid['cell_lat'] - float(row['lat']))**2 +
                             (grid['cell_lon'] - float(row['lon']))**2).idxmin()] if not grid.empty else None
        local_basis_str = (f"<br>Field basis: <b>{float(nearest['basis_cents']):.0f}¢</b>"
                           if nearest is not None else "")
        folium.Marker(
            location=[float(row['lat']), float(row['lon'])],
            popup=f"<b>{row['name']}</b><br>{row['city']}, {row['state']}<br>"
                  f"Operator: {row['operator']}<br>"
                  f"Capacity: {row['nameplate_tpd'] or '?'} tpd"
                  f"{local_basis_str}",
            icon=folium.Icon(color="green", icon="industry", prefix="fa"),
        ).add_to(fmap)

fmap.fit_bounds([[lat_min, lon_min], [lat_max, lon_max]])

st_folium(fmap, height=620, width=None, returned_objects=[])

# Legend / explanation — adapts to the active color mode
hatch_text = (
    f' <strong>Diagonal hatching</strong> = std_err > {UNCERTAINTY_THRESHOLD_CENTS:.0f}¢ '
    f'(field is making a less-confident prediction).'
    if show_uncertainty_hatch else ''
)
contour_text = (
    ' <strong style="color:#FFFFFF;">White lines</strong> = basis iso-contours every 5¢ (read gradient direction + steepness).'
    if (show_basis_contours and color_mode == 'basis') else ''
)

st.markdown(f"""
<div class="legend-row" style="margin-top: 0.5rem;">
  <strong style="color: {COLORS['accent']}">Gold dots</strong> = AMS regional bid samples (the data driving the field).
  <strong>Cells</strong> colored by <em>{color_mode_label.lower()}</em> ({legend_low} → {legend_high}).{hatch_text}{contour_text}
  <strong>Click any cell</strong> for that location's basis + std_err + sample count.
  <strong>Click any gold dot</strong> for that AMS region's actual observed bid.
  Pan/zoom to see satellite imagery underneath.
</div>
""", unsafe_allow_html=True)

# Field summary table
with st.expander("Sample points contributing to this view", expanded=False):
    if not samples.empty:
        s = samples.sort_values('basis_cents').copy()
        s['basis_cents'] = s['basis_cents'].astype(float).round(1)
        s['cash_price'] = pd.to_numeric(s['cash_price'], errors='coerce').round(2)
        st.dataframe(
            s[['location_label', 'lat', 'lon', 'basis_cents', 'cash_price', 'observation_date']],
            width="stretch", hide_index=True,
        )
    else:
        st.info("No sample points in current view (try a wider region or different date).")

# Methodology notes
with st.expander("How the field is built (methodology)", expanded=False):
    st.markdown(f"""
    **Layer 1 — Geographic-economic basis field** (this view):

    1. **Sample sources**: USDA AMS Daily Grain Bid reports across {len([c for c in COMMODITY_OPTIONS if COMMODITY_OPTIONS[c]['ready']])} commodities and 17+ states. Each AMS regional aggregate (e.g. "North Central Iowa Country Elevators") is treated as one spatial sample at the centroid of that region.
    2. **Interpolation**: Inverse-distance weighting (power=2), max-distance cutoff 250 mi, requires ≥3 samples within range to predict. Resolution 0.25° (~17 mi at IA latitude).
    3. **Uncertainty**: Std-err per cell from weighted variance of contributing samples.
    4. **Limitations**: Regional aggregates are coarser than single-elevator bids. For higher resolution we need facility-level scrapes (AGP daily bids, etc.) — pending.
    5. **Stationarity**: Field assumes today's spatial gradient is the relevant signal; large weather/policy shocks can invalidate near-term predictions.

    **Layer 2 (facility identity premium) and Layer 3 (competitive uplift)** are NOT shown here. Those are facility-specific deltas applied on top of the field — see facility-level dashboards.

    **Data freshness**: AMS reports update daily on business days. Field is recomputed when the daily backfill completes.
    """)

st.markdown("---")
st.caption(f"Generated {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} · "
           f"`silver.basis_field_grid` · IDW v1 · "
           f"Imagery © Esri World Imagery")
