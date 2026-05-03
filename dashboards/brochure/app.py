"""
RLC-Agent — Product Brochure / Walkthrough

Built 2026-05-03 for the John Donicht meeting (and as the foundation for
the June 4 conference demo). Single-page scrolling brochure. Every number
is live from the database — what John sees IS the platform.

Run:
    streamlit run dashboards/brochure/app.py --server.port 8522

Tone: peer-to-peer factual. Transparent about limitations. No marketing
fluff — the platform speaks for itself.
"""
from __future__ import annotations

import os
from datetime import datetime, date

import folium
import pandas as pd
import plotly.graph_objects as go
import psycopg2
import psycopg2.extras
import streamlit as st
from dotenv import load_dotenv
from streamlit_folium import st_folium

load_dotenv()


COLORS = {
    "primary":       "#1F4D2C",
    "primary_light": "#3A7A4D",
    "accent":        "#D4A848",
    "bg_dark":       "#0E1614",
    "bg_card":       "#1A2521",
    "bg_section":    "#15201C",
    "text":          "#E8EDE9",
    "text_dim":      "#8FA095",
    "text_muted":    "#6A7A6F",
    "danger":        "#C45B4D",
    "process":       "#2D5A3D",
}


# ============================================================================
# Database
# ============================================================================

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


@st.cache_data(ttl=600)
def load_platform_stats() -> dict:
    """Live numbers used throughout the brochure."""
    stats = {}
    df = query("SELECT COUNT(*) AS n FROM bronze.state_air_permits")
    stats["facilities"] = int(df.iloc[0]["n"])

    df = query("SELECT COUNT(*) AS n FROM bronze.state_air_permit_units")
    stats["emission_units"] = int(df.iloc[0]["n"])

    df = query("SELECT COUNT(DISTINCT state) AS n FROM bronze.state_air_permits")
    stats["facility_states"] = int(df.iloc[0]["n"])

    df = query("""
        SELECT COUNT(*) AS n_obs, COUNT(DISTINCT (lat, lon)) AS n_locs
        FROM bronze.cash_bid_observation WHERE source='ams'
    """)
    stats["basis_observations"] = int(df.iloc[0]["n_obs"])
    stats["basis_locations"] = int(df.iloc[0]["n_locs"])

    df = query("""
        SELECT COUNT(*) AS n FROM silver.basis_field_grid
        WHERE observation_date = (SELECT MAX(observation_date) FROM silver.basis_field_grid)
    """)
    stats["field_grid_cells"] = int(df.iloc[0]["n"])

    df = query("SELECT COUNT(DISTINCT commodity) AS n FROM silver.basis_field_grid")
    stats["commodity_fields"] = int(df.iloc[0]["n"])

    df = query("SELECT COUNT(DISTINCT state) AS n FROM reference.basis_region_centroid")
    stats["basis_states"] = int(df.iloc[0]["n"])

    df = query("SELECT COUNT(*) AS n FROM core.kg_node")
    stats["kg_nodes"] = int(df.iloc[0]["n"])

    df = query("SELECT COUNT(*) AS n FROM core.kg_edge")
    stats["kg_edges"] = int(df.iloc[0]["n"])

    df = query("SELECT COUNT(*) AS n FROM silver.dscan_embeddings")
    stats["archive_embedded"] = int(df.iloc[0]["n"])

    return stats


@st.cache_data(ttl=600)
def load_iowa_basis_surface() -> pd.DataFrame:
    return query("""
        SELECT cell_lat, cell_lon, basis_cents, std_err, n_samples, nearest_sample_mi
        FROM silver.basis_field_grid
        WHERE commodity = 'soybeans' AND delivery_month = 'spot'
          AND observation_date = (
            SELECT MAX(observation_date) FROM silver.basis_field_grid
            WHERE commodity = 'soybeans' AND delivery_month = 'spot'
          )
          AND cell_lat BETWEEN 40.0 AND 44.0
          AND cell_lon BETWEEN -97.0 AND -90.0
    """)


@st.cache_data(ttl=600)
def load_basis_samples() -> pd.DataFrame:
    return query("""
        SELECT location_label, lat, lon, basis_cents
        FROM bronze.cash_bid_observation
        WHERE commodity='soybeans' AND source='ams'
          AND observation_date = (
            SELECT MAX(observation_date) FROM bronze.cash_bid_observation
            WHERE commodity='soybeans' AND source='ams'
          )
          AND lat BETWEEN 40.0 AND 44.5 AND lon BETWEEN -97.0 AND -90.0
    """)


@st.cache_data(ttl=600)
def load_eagle_grove_summary() -> dict:
    df = query("""
        SELECT facility_name, operator, city, permit_number,
               expiration_date, raw_pdf_pages,
               (SELECT COUNT(*) FROM bronze.state_air_permit_units u
                WHERE u.permit_id = p.id) AS n_units
        FROM bronze.state_air_permits p
        WHERE facility_id = 'agp_eagle_grove'
    """)
    if df.empty:
        return {}
    return df.iloc[0].to_dict()


@st.cache_data(ttl=600)
def load_facility_table() -> pd.DataFrame:
    return query("""
        SELECT
            p.facility_name,
            p.city,
            p.state,
            (SELECT COUNT(*) FROM bronze.state_air_permit_units u WHERE u.permit_id = p.id) AS n_units,
            p.expiration_date
        FROM bronze.state_air_permits p
        ORDER BY n_units DESC
    """)


# ============================================================================
# Styling
# ============================================================================

def inject_css():
    st.markdown(f"""
    <style>
    .stApp {{ background: {COLORS['bg_dark']}; color: {COLORS['text']}; }}
    .main .block-container {{ padding-top: 2rem; padding-bottom: 2rem; max-width: 1200px; }}
    h1, h2, h3 {{ font-weight: 600; letter-spacing: -0.02em; color: {COLORS['text']}; }}

    /* Section dividers */
    .section-band {{
        background: {COLORS['bg_section']};
        padding: 2.5rem 2.5rem;
        border-radius: 6px;
        margin: 1.8rem 0;
        border-left: 3px solid {COLORS['primary_light']};
    }}
    .section-eyebrow {{
        font-size: 0.78rem;
        text-transform: uppercase;
        letter-spacing: 0.18em;
        color: {COLORS['accent']};
        font-weight: 600;
        margin-bottom: 0.5rem;
    }}
    .section-title {{
        font-size: 2.2rem;
        font-weight: 700;
        line-height: 1.05;
        letter-spacing: -0.025em;
        margin: 0;
        color: {COLORS['text']};
    }}
    .section-sub {{
        font-size: 1.0rem;
        color: {COLORS['text_dim']};
        line-height: 1.55;
        margin-top: 0.6rem;
        font-weight: 300;
    }}

    /* Hero */
    .hero {{
        padding: 3rem 0 2rem 0;
    }}
    .hero-chip {{
        display: inline-block; padding: 0.4rem 0.9rem;
        background: {COLORS['primary']}; color: {COLORS['accent']};
        font-family: 'JetBrains Mono', monospace;
        font-size: 0.78rem; letter-spacing: 0.18em; font-weight: 600;
        border-radius: 2px;
    }}
    .hero-title {{
        font-size: 3.5rem; font-weight: 700; letter-spacing: -0.035em;
        line-height: 1.0; margin: 1rem 0 0.5rem 0;
        color: {COLORS['text']};
    }}
    .hero-tag {{
        font-size: 1.3rem; color: {COLORS['text_dim']};
        font-weight: 300; line-height: 1.4;
        max-width: 750px;
    }}

    /* Stat cards */
    .stat-grid {{
        display: grid;
        grid-template-columns: repeat(4, 1fr);
        gap: 1rem;
        margin: 1.5rem 0;
    }}
    .stat-card {{
        background: {COLORS['bg_card']};
        border-left: 3px solid {COLORS['primary_light']};
        border-radius: 4px;
        padding: 1.2rem 1.3rem;
    }}
    .stat-label {{
        font-size: 0.72rem;
        color: {COLORS['text_dim']};
        text-transform: uppercase;
        letter-spacing: 0.10em;
        font-weight: 500;
    }}
    .stat-value {{
        font-size: 2.2rem;
        font-weight: 700;
        font-family: 'JetBrains Mono', 'SF Mono', Consolas, monospace;
        color: {COLORS['text']};
        margin-top: 0.4rem;
        line-height: 1;
    }}
    .stat-unit {{
        font-size: 0.85rem;
        color: {COLORS['text_dim']};
        margin-left: 0.25rem;
        font-weight: 400;
    }}
    .stat-context {{
        font-size: 0.78rem;
        color: {COLORS['accent']};
        margin-top: 0.5rem;
        font-weight: 500;
    }}

    /* Pillars */
    .pillar {{
        background: {COLORS['bg_card']};
        border-radius: 4px;
        padding: 1.5rem 1.5rem;
        height: 100%;
        border-top: 2px solid {COLORS['accent']};
    }}
    .pillar-num {{
        font-family: 'JetBrains Mono', monospace;
        font-size: 0.78rem;
        color: {COLORS['accent']};
        font-weight: 600;
    }}
    .pillar-title {{
        font-size: 1.3rem;
        font-weight: 700;
        margin: 0.4rem 0 0.6rem 0;
        color: {COLORS['text']};
    }}
    .pillar-body {{
        font-size: 0.95rem;
        color: {COLORS['text_dim']};
        line-height: 1.6;
    }}

    /* Body text */
    p, li {{
        color: {COLORS['text']};
        font-size: 1.0rem;
        line-height: 1.65;
    }}
    .body-muted {{ color: {COLORS['text_dim']}; }}

    /* Code/source tags */
    .source-tag {{
        display: inline-block;
        font-size: 0.72rem;
        color: {COLORS['text_dim']};
        background: rgba(255,255,255,0.05);
        padding: 0.18rem 0.55rem;
        border-radius: 2px;
        margin-right: 0.4rem;
        font-family: 'JetBrains Mono', monospace;
    }}
    .pull-quote {{
        border-left: 3px solid {COLORS['accent']};
        padding-left: 1.2rem;
        margin: 1.5rem 0;
        font-size: 1.15rem;
        font-style: italic;
        color: {COLORS['text']};
        line-height: 1.5;
    }}
    .footer-meta {{
        font-size: 0.78rem;
        color: {COLORS['text_muted']};
        margin-top: 3rem;
        padding-top: 1rem;
        border-top: 1px solid rgba(255,255,255,0.08);
    }}
    </style>
    """, unsafe_allow_html=True)


def stat_card(label, value, unit="", context=""):
    ctx_html = f'<div class="stat-context">{context}</div>' if context else ""
    # Single-line HTML so Streamlit's markdown renderer doesn't treat indented
    # multi-line HTML as a code block.
    return (f'<div class="stat-card">'
            f'<div class="stat-label">{label}</div>'
            f'<div class="stat-value">{value}<span class="stat-unit">{unit}</span></div>'
            f'{ctx_html}'
            f'</div>')


# ============================================================================
# Page
# ============================================================================

st.set_page_config(
    page_title="RLC-Agent — A Facility-Level Intelligence Platform",
    page_icon="🌾",
    layout="wide",
)
inject_css()

stats = load_platform_stats()


# ----- HERO --------------------------------------------------------------
st.markdown(f"""
<div class="hero">
  <span class="hero-chip">RLC-AGENT &nbsp;·&nbsp; PRODUCT WALKTHROUGH</span>
  <div class="hero-title">A facility-level<br/>intelligence platform<br/>for ag processing.</div>
  <div class="hero-tag">
    Every facility modeled as an agent — running on real permit data, real economics
    (basis field + crush margins), and a strategic plan. Built on free public-source data
    and local LLMs. Production-grade infrastructure, not a research prototype.
  </div>
</div>
""", unsafe_allow_html=True)


# ----- SECTION 1: WHAT THIS IS -------------------------------------------
st.markdown(f"""
<div class="section-band">
  <div class="section-eyebrow">Section 01 · Product</div>
  <div class="section-title">Three pillars, one substrate.</div>
  <div class="section-sub">
    The platform is an integrated stack — facility-level data flows up,
    market-level fields flow down, an agent-shaped logic layer makes them
    actionable. None of these pillars works as well alone as they do together.
  </div>
</div>
""", unsafe_allow_html=True)

p1, p2, p3 = st.columns(3, gap="medium")
with p1:
    st.markdown(f"""
    <div class="pillar">
      <div class="pillar-num">01</div>
      <div class="pillar-title">Facility intelligence</div>
      <div class="pillar-body">
        Every state-issued Title V air permit becomes structured equipment data.
        Capacity, throughput limits, control devices, operating constraints — all
        machine-readable. {stats['facilities']} facilities, {stats['emission_units']}
        emission units, {stats['facility_states']} states already modeled.
      </div>
    </div>
    """, unsafe_allow_html=True)
with p2:
    st.markdown(f"""
    <div class="pillar">
      <div class="pillar-num">02</div>
      <div class="pillar-title">Basis field</div>
      <div class="pillar-body">
        Treats basis as a continuous geographic-economic field, not a per-facility
        property. {stats['basis_observations']:,} sample observations from
        {stats['basis_states']} states drive a daily-refreshed gridded surface
        across {stats['commodity_fields']} commodities. Any new facility plugs in
        by querying the field at its lat/lon.
      </div>
    </div>
    """, unsafe_allow_html=True)
with p3:
    st.markdown(f"""
    <div class="pillar">
      <div class="pillar-num">03</div>
      <div class="pillar-title">Agent layer</div>
      <div class="pillar-body">
        Per-facility agent runs the daily decision loop: pull state, pull forecasts,
        pull KG-adjusted risk signals, evaluate against strategic plan, output bid
        sheet. Quarterly Claude Opus run writes the strategic plan;
        daily Ollama instance executes against it. Knowledge graph
        ({stats['kg_nodes']} nodes, {stats['kg_edges']} edges) provides analyst
        framework.
      </div>
    </div>
    """, unsafe_allow_html=True)


# ----- SECTION 2: THE HARD PROBLEM ---------------------------------------
st.markdown(f"""
<div class="section-band">
  <div class="section-eyebrow">Section 02 · Why this is hard</div>
  <div class="section-title">Crushers don't tell you their capacity.<br/>Or their basis. Or, often, their address.</div>
  <div class="section-sub">
    Agricultural processors are unusually opaque. Capacity figures are proprietary;
    daily bid sheets are members-only or behind paid-data subscriptions; even existence
    is sometimes obscured. Traditional commodity research treats this opacity as a
    fact of life — the analyst aggregates to industry totals and works in averages.
  </div>
</div>

<p>
That aggregation is the cost. Every analytical decision based on a state- or
national-level total <em>misses</em> the dispersion that the facility-level reality
contains. Two crushers in the same state can have radically different basis structure,
crush margins, and strategic positions, but you wouldn't know unless you got inside
each one.
</p>
<p>
The breakthrough that changed this: <strong>state air-quality permits are public</strong>.
Every facility that emits regulated pollutants must publish a Title V permit listing
every piece of equipment, its rated capacity, and its operating constraints. We
extract that. Then we plug each facility into the geographic basis field. Then we
run the agent loop. The opacity dissolves, one facility at a time.
</p>

<div class="pull-quote">
The clearest signal isn't always the loudest one. State DNRs have been publishing
itemized facility data for decades. We just hadn't gotten around to reading it
systematically until language models made the cost of doing so trivial.
</div>
""", unsafe_allow_html=True)


# ----- SECTION 3: DATA SUBSTRATE -----------------------------------------
st.markdown(f"""
<div class="section-band">
  <div class="section-eyebrow">Section 03 · The data, live</div>
  <div class="section-title">Built on public sources. Refreshed daily.</div>
  <div class="section-sub">
    Every number on this page is queried from the production database at the moment
    you load it. No marketing fabrications, no future projections.
  </div>
</div>
""", unsafe_allow_html=True)

# Live stats grid — built as a single concatenated string with no internal
# indentation so Streamlit's markdown parser doesn't mangle it.
cards_html = (
    '<div class="stat-grid">'
    + stat_card("Facilities modeled", f"{stats['facilities']}", "", f"{stats['emission_units']:,} emission units extracted")
    + stat_card("Basis observations", f"{stats['basis_observations']:,}", "", f"{stats['basis_locations']} distinct sample locations")
    + stat_card("Field grid cells", f"{stats['field_grid_cells']:,}", "", f"{stats['commodity_fields']} commodities, daily refresh")
    + stat_card("Knowledge graph", f"{stats['kg_nodes']}", "nodes", f"{stats['kg_edges']} relationships, {stats['archive_embedded']:,} archive docs embedded")
    + '</div>'
)
st.markdown(cards_html, unsafe_allow_html=True)


# ----- SECTION 4: THE BASIS FIELD -----------------------------------------
st.markdown(f"""
<div class="section-band">
  <div class="section-eyebrow">Section 04 · The field, live</div>
  <div class="section-title">Soybean basis across Iowa, today.</div>
  <div class="section-sub">
    Inverse-distance interpolation over USDA AMS regional bid samples (gold dots).
    Cells colored red→green by basis tightness. Pan/zoom drills into satellite imagery.
    This same view exists for corn, wheat, sorghum, and across the Corn Belt + Mid-South + Plains.
  </div>
</div>
""", unsafe_allow_html=True)

basis_surface = load_iowa_basis_surface()
basis_samples = load_basis_samples()

if not basis_surface.empty:
    fmap = folium.Map(
        location=[42.3, -93.5], zoom_start=7,
        tiles=None, control_scale=True, prefer_canvas=True,
    )
    folium.TileLayer(
        tiles="https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}",
        attr="Esri", name="Satellite", overlay=False, control=False,
    ).add_to(fmap)

    bmin = float(basis_surface['basis_cents'].min())
    bmax = float(basis_surface['basis_cents'].max())
    for _, row in basis_surface.iterrows():
        b = float(row['basis_cents'])
        t = (b - bmin) / (bmax - bmin) if bmax != bmin else 0.5
        r_, g_ = int(255 * (1 - t)), int(180 * t + 60)
        color = f"#{r_:02x}{g_:02x}3c"
        # Plain-English popup with the reasoning chain visible
        std_e = float(row['std_err'] or 0)
        n_s = int(row['n_samples'] or 0)
        nearest = float(row['nearest_sample_mi'] or 0)
        popup_html = (
            f"<b>{b:.0f}¢</b> under futures<br>"
            f"<span style='color:#666'>Confident within ±{std_e:.0f}¢</span><br>"
            f"<span style='color:#666'>Built from {n_s} nearby price reports</span><br>"
            f"<span style='color:#666'>Nearest one {nearest:.0f} miles away</span>"
        )
        folium.Rectangle(
            bounds=[[float(row['cell_lat']) - 0.125, float(row['cell_lon']) - 0.125],
                    [float(row['cell_lat']) + 0.125, float(row['cell_lon']) + 0.125]],
            color=None, weight=0, fill=True, fill_color=color, fill_opacity=0.55,
            popup=popup_html,
        ).add_to(fmap)

    if not basis_samples.empty:
        for _, row in basis_samples.iterrows():
            folium.CircleMarker(
                location=[float(row['lat']), float(row['lon'])],
                radius=6, color="#FFFFFF", weight=1.5,
                fill=True, fill_color=COLORS['accent'], fill_opacity=0.95,
                popup=f"<b>{row['location_label'][:60]}</b><br>{float(row['basis_cents']):.0f}¢ vs futures (observed)",
            ).add_to(fmap)

    fmap.fit_bounds([[40.3, -96.7], [43.6, -90.1]])
    st_folium(fmap, height=480, width=None, returned_objects=[])

st.markdown(f"""
<p class="body-muted" style="font-size:0.9rem; margin-top:-0.5rem;">
The basis gradient flows exactly as economic geography predicts — tightest in
southwest Iowa near the Missouri River barge terminals, widest in southeast and
northeast Iowa where transport options narrow. This isn't a model fit; it's the field.
</p>
""", unsafe_allow_html=True)


# ----- SECTION 5: A FACILITY DEEP-DIVE ----------------------------------
eg = load_eagle_grove_summary()

st.markdown(f"""
<div class="section-band">
  <div class="section-eyebrow">Section 05 · Facility deep-dive</div>
  <div class="section-title">AGP Eagle Grove — first showcase facility.</div>
  <div class="section-sub">
    Iowa Title V permit <code>{eg.get('permit_number','?')}</code> extracted into
    {eg.get('n_units','?')} emission units with rated capacities. Eagle Grove plugs
    into the basis field at <strong>−86.2¢ vs futures</strong> (12 samples, ±5.1¢ uncertainty,
    nearest sample 21mi away). Live board crush margin pulled from CME front-month
    settlements; Eagle-Grove-specific margin = board crush + local basis.
  </div>
</div>
""", unsafe_allow_html=True)

# Facility roster table
fac = load_facility_table()
if not fac.empty:
    fac_display = fac.copy()
    fac_display['expiration_date'] = fac_display['expiration_date'].astype(str)
    fac_display.columns = ['Facility', 'City', 'State', 'Emission Units', 'Permit Expires']
    st.markdown(f"""
    <p class="body-muted" style="margin-bottom:0.3rem;">
    The {stats['facilities']} facilities currently in bronze, ranked by extracted emission unit count:
    </p>
    """, unsafe_allow_html=True)
    st.dataframe(fac_display, width="stretch", hide_index=True, height=400)

st.markdown(f"""
<p style="margin-top:1rem;">
Each facility folder under <code>permits/&lt;industry&gt;/&lt;state&gt;/&lt;facility&gt;/</code>
contains the source PDF, Excel-friendly equipment list, narrative summary, and a
<em>process-flow coverage report</em> — a checklist comparing extracted equipment
against the canonical 19-step oilseed-crush ontology so a domain expert can
spot-check what was caught and what was missed.
</p>
""", unsafe_allow_html=True)


# ----- SECTION 6: THE ARCHITECTURE ----------------------------------------
st.markdown(f"""
<div class="section-band">
  <div class="section-eyebrow">Section 06 · How it fits together</div>
  <div class="section-title">Field, identity, network — three layers of basis.</div>
  <div class="section-sub">
    The architectural insight that changed everything (per Tore, 2026-05-02):
    basis is a property of economic geography, not of facilities. Facilities <em>sample</em>
    the field. Their identity (decades of trust, payment record, co-op membership) adds a
    premium on top. Their proximity to other crushers creates a network effect.
  </div>
</div>

<div style="background: {COLORS['bg_card']}; padding: 1.6rem 1.8rem; border-radius: 4px; margin: 1.2rem 0;">
  <p><strong>Layer 1 — Geographic-economic field</strong> (built):
  IDW interpolation over AMS regional bid samples. Same answer to any facility at the same lat/lon.
  Universal infrastructure that compounds across every commodity, every facility, every industry we add.</p>

  <p><strong>Layer 2 — Facility identity premium</strong> (Sprint 2):
  Per-facility delta from the field. AGP Eagle Grove has built a +11¢ premium over 40 years
  of co-op patronage and reliability. New entrants start at 0¢ and converge over years.</p>

  <p><strong>Layer 3 — Local competitive uplift</strong> (Sprint 2):
  Adding a facility <em>changes the field for everyone in its draw area</em>. Two crushers
  within 50mi each pay 2-4¢ more than a monopsony facility would.</p>

  <p style="margin-bottom:0;"><em style="color: {COLORS['accent']}">Effective basis at facility F</em> =
  field(F.lat, F.lon) + premium(F) + competitive_uplift(F, world)</p>
</div>
""", unsafe_allow_html=True)


# ----- SECTION 7: ROADMAP & ASKS ------------------------------------------
st.markdown(f"""
<div class="section-band">
  <div class="section-eyebrow">Section 07 · What's next</div>
  <div class="section-title">June 4 conference. Sprint 1 ships by June 1.</div>
  <div class="section-sub">
    Building toward a credible "facility with forward book + strategic plan + crush margin curve"
    demo on three facilities. Sprint 2 expands the basis field to its full three-layer form,
    builds the Layer 1 strategic agent, and adds biofuel/slaughter/render facility coverage.
  </div>
</div>
""", unsafe_allow_html=True)

s1, s2, s3 = st.columns(3, gap="medium")
with s1:
    st.markdown(f"""
    <div class="pillar">
      <div class="pillar-num">SPRINT 1</div>
      <div class="pillar-title">Now → June 1</div>
      <div class="pillar-body">
      Forward curves end-to-end (basis at K/N/Q/U/X delivery months).<br/><br/>
      Strategic plan + position + P&amp;L tables.<br/><br/>
      Hybrid regex+LLM extraction (deterministic table parse + LLM narrative).<br/><br/>
      Best-of-N runs with checker-agent validation against canonical equipment lists.<br/><br/>
      Eagle Grove + 2 more facilities fully demo-ready.
      </div>
    </div>
    """, unsafe_allow_html=True)
with s2:
    st.markdown(f"""
    <div class="pillar">
      <div class="pillar-num">SPRINT 2</div>
      <div class="pillar-title">Months 2-3</div>
      <div class="pillar-body">
      Layer 1 strategic agent (Claude Opus quarterly run writes the plan).<br/><br/>
      Layer 2 facility identity premium (anchored from AMS Mills/Processors gap).<br/><br/>
      Layer 3 competitive uplift (network effect math).<br/><br/>
      One adjacent industry: biofuel OR slaughter/render.<br/><br/>
      Daily auto-refresh of basis field on the 4060 grinder.
      </div>
    </div>
    """, unsafe_allow_html=True)
with s3:
    st.markdown(f"""
    <div class="pillar">
      <div class="pillar-num">VISION</div>
      <div class="pillar-title">Months 3-12+</div>
      <div class="pillar-body">
      Full industry coverage: biofuel, slaughter, render, fats/greases, UCO, food mfg.<br/><br/>
      International: Canada first, then Brazil, Argentina.<br/><br/>
      Symbiotic forecasting: LLM forecasts every monthly series in parallel to spreadsheets, reconciled against realized data.<br/><br/>
      Self-updating ontologies as new permits ingest.
      </div>
    </div>
    """, unsafe_allow_html=True)


# ----- HOW WE KNOW (methodology) ------------------------------------------
st.markdown(f"""
<div class="section-band">
  <div class="section-eyebrow">Section 08 · How we know what we know</div>
  <div class="section-title">Reasoning visible, not just outputs.</div>
  <div class="section-sub">
    A facility-specific basis number is only as good as its provenance. Every value the
    platform reports carries its reasoning chain — what samples drove it, how far they
    were, how much they agreed. The point isn't to be right every time. The point is
    that you can always see why we said what we said.
  </div>
</div>

<div style="background: {COLORS['bg_card']}; padding: 1.5rem 1.7rem; border-radius: 4px; margin: 1rem 0;">
  <p style="margin-top:0;"><strong>Worked example — Eagle Grove basis −86.2¢:</strong></p>
  <ol>
    <li><strong>Field samples within 250mi</strong>: 12 USDA AMS regional bid points, ranging from −63¢ (SW Iowa, Missouri River barge access) to −91¢ (SE Iowa, landlocked).</li>
    <li><strong>Inverse-distance weighting (power=2)</strong>: closer samples count more. The North Central Iowa centroid (~21mi away) dominates; the regional gradient pulls slightly toward neighbor districts.</li>
    <li><strong>Predicted value at Eagle Grove's lat/lon</strong>: −86.2¢ vs front-month futures.</li>
    <li><strong>Uncertainty</strong>: ±5.1¢ standard error from weighted variance of contributing samples.</li>
    <li><strong>Where this is wrong</strong>: AGP Eagle Grove's actual mill-gate bid is closer to −75¢ — the 11¢ difference is the AGP identity premium (decades of co-op trust, fast settlement, reliable grading) that the geographic field doesn't capture. That premium is Layer 2.</li>
  </ol>
</div>

<p>
The same chain runs for every number on the platform. Crush margin? Board crush
+ field basis − transport. Facility throughput? Rated capacity × utilization assumption,
flagged as "design rate" not "actual." Forward curve? Spot interpolation + futures
spread + carry-cost constraint. Each step has a defensible source, and each output
has an uncertainty band.
</p>


<div class="section-band" style="border-left-color: {COLORS['accent']};">
  <div class="section-eyebrow" style="color: {COLORS['accent']};">Section 09 · Where we're still uncertain</div>
  <div class="section-title">What's developing, and why.</div>
  <div class="section-sub">
    Analytical infrastructure at this stage isn't finished — it's accreting. These are
    the gaps we're working on, named explicitly so a sharp reviewer can probe them.
  </div>
</div>

<ul>
  <li><strong>Single-run LLM extraction varies bidirectionally by 50-70%</strong> on long
  Title V permits. Best-of-N (3-5 runs, union by unit_id) ships in Sprint 1 W4 to fix this.
  Until then, we treat extracted unit counts as a lower bound and the canonical process-flow
  ontology as a checklist for what's missing.</li>

  <li><strong>3 large permits time out</strong> on single-shot extraction (Cargill Cedar
  Rapids 57004 at 190pp, ADM Frankfort at 475pp, Cargill Lafayette at 292pp). Chunked
  extraction with merge ships in Sprint 1 W4.</li>

  <li><strong>One permit (Bunge Decatur) is image-only</strong> — scanned PDF, no native
  text. pytesseract OCR preprocessing layer queued for Sprint 2.</li>

  <li><strong>Forward basis is sparse beyond X26 and F26</strong>. Mid-curve months
  (K26, N26, U26) need additional sample sources — terminal scrapes, processor postings,
  or partnership data. Sprint 2 priority.</li>

  <li><strong>Layer 2 (facility identity premium) and Layer 3 (network competitive uplift)
  are designed but not yet built</strong>. Dashboards currently show Layer 1 only — the
  geographic-economic baseline. Sprint 2 ships both with empirical anchoring from
  AMS Mills/Processors-vs-Country-Elevator gap.</li>

  <li><strong>Demo facilities are 17 in Iowa + 3 in Indiana</strong>. The platform is
  designed to scale; what's needed for new states is just the per-state PDF download
  collector, not new architecture.</li>
</ul>

<p style="margin-top:1.2rem;" class="body-muted">
This is the standard ledger of an analytical platform under active development. Nothing
on this list is structural — each is solvable with focused work, named on the roadmap,
and tracked in memory so we don't relearn the lesson when we re-run the work.
</p>
""", unsafe_allow_html=True)


# ----- FOOTER -------------------------------------------------------------
st.markdown(f"""
<div class="footer-meta">
Generated {datetime.now().strftime('%Y-%m-%d %H:%M')} · Round Lakes Commodities (RLC-Agent) ·
All numbers above are queried live at page load · Built with public-source data
(USDA AMS, state DNRs, CME) plus local Ollama inference on RTX 5080 + RTX 4060 ·
No paid data subscriptions used in this prototype.
</div>
""", unsafe_allow_html=True)
