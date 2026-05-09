"""
Facility Intelligence Console (FIC).

Operator-facing control surface for the project. One tile per facility,
click into entity detail (overview, sentiment, news, permits, relationships,
raw record), or add a new facility via the form.

Run:
    cd C:\\dev\\RLC-Agent
    streamlit run dashboards/facility/index.py

Routes via query param:
  - (none)        -> grid of all facilities, filterable
  - ?facility=X   -> facility detail page (X = facility_id)
  - ?new=1        -> new-facility form

Layer 1 of the FIC vision (per chat 2026-05-09):
  L1. Add/edit facilities via form  (this file)
  L2. Edit relationships on detail page  (this file, Edges tab)
  L3. On-boarding hook propagates new facility into pipelines  (TBD)
  L4. Due-diligence agent generates reports from facility data  (TBD)
"""

from __future__ import annotations

import os
import re
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
import psycopg2
import psycopg2.extras
import streamlit as st
from dotenv import load_dotenv
import folium
from streamlit_folium import st_folium

ROOT = Path(__file__).resolve().parent.parent.parent
load_dotenv(ROOT / ".env")

st.set_page_config(
    page_title="FIC — Facility Intelligence Console",
    page_icon="🏭",
    layout="wide",
    initial_sidebar_state="expanded",
)


# ---------------------------------------------------------------------------
# DB
# ---------------------------------------------------------------------------

@st.cache_resource
def get_conn():
    """Cached connection for the Streamlit session. We force AUTOCOMMIT so
    every SELECT closes its implicit transaction immediately. Without this,
    psycopg2's default behaviour leaves the connection in 'idle in
    transaction' state forever — which holds a lock that blocks any
    background TRUNCATE/DDL on tables we read here. (Saw exactly that:
    a 4h41m idle-in-transaction connection blocked director_appointment
    TRUNCATEs from the loader.)
    """
    conn = psycopg2.connect(
        host=os.environ.get("RLC_PG_HOST", "localhost"),
        port=os.environ.get("RLC_PG_PORT", "5432"),
        database=os.environ.get("RLC_PG_DATABASE", "rlc_commodities"),
        user=os.environ.get("RLC_PG_USER", "postgres"),
        password=os.environ.get("RLC_PG_PASSWORD", ""),
    )
    conn.autocommit = True
    return conn


def query(sql: str, params=None) -> list[dict]:
    conn = get_conn()
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(sql, params or [])
        return cur.fetchall()


def execute(sql: str, params=None) -> int:
    """Execute a write statement, return rowcount. Connection is autocommit."""
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute(sql, params or [])
        return cur.rowcount


# ---------------------------------------------------------------------------
# Data accessors
# ---------------------------------------------------------------------------

@st.cache_data(ttl=60)
def list_facilities() -> pd.DataFrame:
    rows = query("""
        SELECT facility_id, name, industry_code, operator, parent_company,
               city, county, state, lat, lon, status, data_source,
               verified_at, verified_by, notes
        FROM reference.facility_master
        UNION ALL
        SELECT facility_id, name, 'oilseed_crush' AS industry_code,
               operator, parent_company, city, county, state, lat, lon,
               status, data_source, verified_at, verified_by, notes
        FROM reference.oilseed_crush_facilities
        WHERE is_canonical = TRUE
    """)
    df = pd.DataFrame(rows)
    return df


@st.cache_data(ttl=60)
def list_facility_ids_with_sentiment() -> set:
    rows = query("""
        SELECT DISTINCT facility_id FROM gold.facility_sentiment_daily
    """)
    return {r["facility_id"] for r in rows}


@st.cache_data(ttl=60)
def list_facility_ids_with_news() -> set:
    """Facilities mentioned in classified news (any market_locality match)."""
    try:
        rows = query("""
            SELECT DISTINCT facility_id FROM gold.facility_sentiment_daily
            WHERE news_count > 0
        """)
        return {r["facility_id"] for r in rows}
    except Exception:
        return set()


@st.cache_data(ttl=60)
def list_facilities_with_permits() -> dict:
    """Return {(state, lower(name), lower(city)): n_units} since the permit
    table doesn't carry facility_id — match by name+city when rendering."""
    try:
        rows = query("""
            SELECT state, LOWER(facility_name) AS fname, LOWER(city) AS city,
                   n_units
            FROM silver.facility_air_permit_capacity
        """)
        return {(r["state"], r["fname"], r["city"]): r["n_units"] for r in rows}
    except Exception:
        return {}


def _approx_facility_has_permits(fac: dict, idx: dict) -> bool:
    """Try to match a facility to the permit index by state + lower-cased
    name/operator + city."""
    state = (fac.get("state") or "").strip()
    city = (fac.get("city") or "").lower().strip()
    op = (fac.get("operator") or "").lower().strip()
    name = (fac.get("name") or "").lower().strip()
    for (s, fname, fcity), _ in idx.items():
        if s != state:
            continue
        if fcity and city and fcity != city:
            continue
        if op and (op in fname or fname in op):
            return True
        if name and (name in fname or fname in name):
            return True
    return False


@st.cache_data(ttl=60)
def fetch_facility(facility_id: str) -> dict | None:
    """Get a single facility's combined record from either reference table."""
    rows = query("""
        SELECT facility_id, name, industry_code, operator, parent_company,
               city, county, state, country, lat, lon, status,
               data_source, verified_at, verified_by, verification_method,
               notes, sources, created_at, updated_at
        FROM reference.facility_master
        WHERE facility_id = %s
        UNION ALL
        SELECT facility_id, name, 'oilseed_crush' AS industry_code,
               operator, parent_company, city, county, state, country, lat, lon,
               status, data_source, verified_at, verified_by,
               verification_method, notes, sources, created_at, updated_at
        FROM reference.oilseed_crush_facilities
        WHERE facility_id = %s AND is_canonical = TRUE
        LIMIT 1
    """, (facility_id, facility_id))
    return rows[0] if rows else None


@st.cache_data(ttl=60)
def fetch_sentiment(facility_id: str) -> pd.DataFrame:
    rows = query("""
        SELECT as_of_date, topic_sentiments, oil_share, news_count
        FROM gold.facility_sentiment_daily
        WHERE facility_id = %s
        ORDER BY as_of_date DESC
        LIMIT 90
    """, (facility_id,))
    return pd.DataFrame(rows)


@st.cache_data(ttl=60)
def fetch_recent_news(facility_id: str, limit: int = 20) -> pd.DataFrame:
    """News articles where the classifier tagged this facility_id."""
    try:
        rows = query("""
            SELECT a.title, a.published_at, a.source_name, a.article_url,
                   c.topic_scores, c.locality, c.facility_relevance_keys,
                   c.confidence_score
            FROM bronze.news_article a
            JOIN silver.news_classified c ON c.news_article_id = a.id
            WHERE c.facility_relevance_keys::text LIKE %s
               OR c.locality::text LIKE %s
            ORDER BY a.published_at DESC NULLS LAST
            LIMIT %s
        """, (f"%{facility_id}%", f"%{facility_id}%", limit))
        return pd.DataFrame(rows)
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=60)
def fetch_permits(fac: dict) -> pd.DataFrame:
    """Permit table doesn't have facility_id — fuzzy-match on
    state + name/operator + city."""
    try:
        rows = query("""
            SELECT facility_name, operator, city, county, industry,
                   permit_number, permit_type, expiration_date, n_units, units
            FROM silver.facility_air_permit_capacity
            WHERE state = %s
        """, (fac.get("state") or "",))
        if not rows:
            return pd.DataFrame()
        out = []
        op = (fac.get("operator") or "").lower().strip()
        nm = (fac.get("name") or "").lower().strip()
        cty = (fac.get("city") or "").lower().strip()
        for r in rows:
            f_name = (r.get("facility_name") or "").lower()
            f_op = (r.get("operator") or "").lower()
            f_cty = (r.get("city") or "").lower()
            # match: same city AND (operator overlaps OR name overlaps)
            if cty and f_cty and cty != f_cty:
                continue
            ok = False
            if op and (op in f_name or op in f_op or f_op in op):
                ok = True
            if not ok and nm and (nm in f_name or f_name in nm):
                ok = True
            if ok:
                out.append(r)
        return pd.DataFrame(out)
    except Exception:
        return pd.DataFrame()


# ---------------------------------------------------------------------------
# Industry styling
# ---------------------------------------------------------------------------

INDUSTRY_META = {
    "oilseed_crush":     {"icon": "🌱", "color": "#2D5A3D", "label": "Oilseed Crush"},
    "ethanol":           {"icon": "🌽", "color": "#C58A2B", "label": "Ethanol"},
    "biodiesel":         {"icon": "🛢", "color": "#5A4A2D", "label": "Biodiesel"},
    "renewable_diesel":  {"icon": "🛢", "color": "#5A4A2D", "label": "Renewable Diesel"},
    "pork_packing":      {"icon": "🐖", "color": "#8B3A3A", "label": "Pork Packing"},
    "beef_packing":      {"icon": "🐄", "color": "#5C2E2E", "label": "Beef Packing"},
    "egg_layers":        {"icon": "🥚", "color": "#A88534", "label": "Egg Layers"},
    "pig_finishing":     {"icon": "🐖", "color": "#8B3A3A", "label": "Pig Finishing"},
    "grain_handling":    {"icon": "🌾", "color": "#4A6B45", "label": "Grain Handling"},
    "rail_terminal":     {"icon": "🚂", "color": "#3D4D5C", "label": "Rail Terminal"},
    "river_terminal":    {"icon": "⚓", "color": "#2E4A5C", "label": "River Terminal"},
    "feed_mill":         {"icon": "🌾", "color": "#4A6B45", "label": "Feed Mill"},
    "other":             {"icon": "🏭", "color": "#555555", "label": "Other"},
}

STATUS_BADGE = {
    "active": ("🟢", "Active"),
    "Operating": ("🟢", "Operating"),
    "idle": ("🟡", "Idle"),
    "closed": ("⚫", "Closed"),
    "Closed": ("⚫", "Closed"),
    "announced": ("🔵", "Announced"),
    "Announced": ("🔵", "Announced"),
    "under_construction": ("🟠", "Under Construction"),
    "Under Construction": ("🟠", "Under Construction"),
    "Planned 2025": ("🔵", "Planned 2025"),
    "unknown": ("⚪", "Unknown"),
}


def industry_style(code: str) -> dict:
    return INDUSTRY_META.get(code, INDUSTRY_META["other"])


def status_badge(status: str | None) -> str:
    if not status:
        return "⚪ Unknown"
    icon, label = STATUS_BADGE.get(status, ("⚪", status))
    return f"{icon} {label}"


# ---------------------------------------------------------------------------
# Grid view
# ---------------------------------------------------------------------------

def render_grid():
    df = list_facilities()
    sentiment_set = list_facility_ids_with_sentiment()
    permits_idx = list_facilities_with_permits()
    # Compute which facilities have permit matches (fuzzy)
    permits_set = set()
    for _, r in df.iterrows():
        if _approx_facility_has_permits(r.to_dict(), permits_idx):
            permits_set.add(r["facility_id"])

    title_col, action_col = st.columns([4, 1])
    with title_col:
        st.title("FIC · Facility Intelligence Console")
        st.caption(
            f"{len(df)} facilities across {df['industry_code'].nunique()} industries. "
            f"Click a tile for detail, or add a new one."
        )
    with action_col:
        st.markdown("###")
        if st.button("➕ Add facility", use_container_width=True, type="primary"):
            st.query_params["new"] = "1"
            st.rerun()

    # --- Filter bar ---------------------------------------------------------
    c1, c2, c3, c4 = st.columns([1.5, 1.5, 1.5, 2])
    with c1:
        industries = ["(all)"] + sorted(df["industry_code"].dropna().unique())
        ind = st.selectbox("Industry", industries, index=0)
    with c2:
        states = ["(all)"] + sorted(df["state"].dropna().unique())
        st_sel = st.selectbox("State", states, index=0)
    with c3:
        statuses = ["(all)"] + sorted(df["status"].dropna().unique().tolist())
        status_sel = st.selectbox("Status", statuses, index=0)
    with c4:
        search = st.text_input("Search operator / city", "")

    f = df.copy()
    if ind != "(all)":
        f = f[f["industry_code"] == ind]
    if st_sel != "(all)":
        f = f[f["state"] == st_sel]
    if status_sel != "(all)":
        f = f[f["status"] == status_sel]
    if search:
        s = search.lower()
        f = f[
            f["operator"].fillna("").str.lower().str.contains(s)
            | f["city"].fillna("").str.lower().str.contains(s)
            | f["facility_id"].fillna("").str.lower().str.contains(s)
        ]

    f = f.sort_values(["state", "industry_code", "city"]).reset_index(drop=True)

    # --- Summary stats ------------------------------------------------------
    s1, s2, s3, s4, s5 = st.columns(5)
    s1.metric("Showing", len(f))
    s2.metric("Verified", int(f["verified_at"].notna().sum()))
    s3.metric("With sentiment", sum(1 for fid in f["facility_id"] if fid in sentiment_set))
    s4.metric("With permits", sum(1 for fid in f["facility_id"] if fid in permits_set))
    s5.metric("Industries", f["industry_code"].nunique())

    st.divider()

    # --- View toggle: Grid | Map -------------------------------------------
    view_mode = st.radio(
        "View",
        ["Grid", "Map"],
        horizontal=True,
        label_visibility="collapsed",
        key="grid_view_mode",
    )
    st.divider()

    if len(f) == 0:
        st.info("No facilities match these filters.")
        return

    if view_mode == "Map":
        render_map(f)
    else:
        # Grid: rows of 4 tiles
        cols_per_row = 4
        for row_start in range(0, len(f), cols_per_row):
            cols = st.columns(cols_per_row)
            for i in range(cols_per_row):
                idx = row_start + i
                if idx >= len(f):
                    break
                r = f.iloc[idx]
                with cols[i]:
                    render_tile(r, sentiment_set, permits_set)


def render_map(df: pd.DataFrame):
    """Folium map of facilities with lat/lon. Click a marker to open detail."""
    geo = df.dropna(subset=["lat", "lon"]).copy()
    geo["lat"] = pd.to_numeric(geo["lat"], errors="coerce")
    geo["lon"] = pd.to_numeric(geo["lon"], errors="coerce")
    geo = geo.dropna(subset=["lat", "lon"])
    geo = geo[(geo["lat"] != 0) & (geo["lon"] != 0)]

    if len(geo) == 0:
        st.info("None of the filtered facilities have coordinates on file.")
        return

    skipped = len(df) - len(geo)
    if skipped > 0:
        st.caption(
            f"Showing {len(geo)} of {len(df)} (skipped {skipped} without coordinates)."
        )

    # Map centered on the centroid of the visible set
    center_lat = float(geo["lat"].mean())
    center_lon = float(geo["lon"].mean())
    m = folium.Map(
        location=[center_lat, center_lon],
        zoom_start=6,
        tiles="cartodbpositron",
    )

    for _, r in geo.iterrows():
        sty = industry_style(r["industry_code"])
        operator = r.get("operator") or "(no operator)"
        city = r.get("city") or "(no city)"
        state = r.get("state") or ""
        popup_html = (
            f"<div style='font-family:sans-serif;'>"
            f"<b>{operator}</b><br>"
            f"{city}, {state}<br>"
            f"<i>{sty['icon']} {sty['label']}</i><br>"
            f"<code style='font-size:0.8em;'>{r['facility_id']}</code>"
            f"</div>"
        )
        # The tooltip text is what we read back to navigate on click —
        # use the facility_id verbatim so the lookup is unambiguous.
        folium.CircleMarker(
            location=[float(r["lat"]), float(r["lon"])],
            radius=7,
            color=sty["color"],
            fill=True,
            fill_color=sty["color"],
            fill_opacity=0.85,
            weight=2,
            popup=folium.Popup(popup_html, max_width=260),
            tooltip=str(r["facility_id"]),
        ).add_to(m)

    # Render the map; capture click events
    out = st_folium(
        m,
        height=600,
        use_container_width=True,
        returned_objects=["last_object_clicked_tooltip"],
        key="facility_map",
    )

    clicked = (out or {}).get("last_object_clicked_tooltip")
    if clicked:
        # Confirm-and-navigate UI so a stray click on a marker doesn't immediately
        # warp us out of the map view.
        st.success(f"Selected: `{clicked}`")
        if st.button(f"Open {clicked} →", type="primary"):
            st.query_params["facility"] = clicked
            st.rerun()

    # Inline industry legend
    with st.expander("Industry legend"):
        legend_cols = st.columns(4)
        items = list(INDUSTRY_META.items())
        for i, (code, meta) in enumerate(items):
            with legend_cols[i % 4]:
                st.markdown(
                    f"<span style='color:{meta['color']};font-size:1.2em;'>●</span> "
                    f"{meta['icon']} {meta['label']}",
                    unsafe_allow_html=True,
                )


def render_tile(r: pd.Series, sentiment_set: set, permits_set: set):
    sty = industry_style(r["industry_code"])
    fid = r["facility_id"]

    has_sentiment = fid in sentiment_set
    has_permits = fid in permits_set
    is_verified = pd.notna(r["verified_at"])

    op = r.get("operator") or "(no operator)"
    city = r.get("city") or "(no city)"
    state = r.get("state") or ""

    # Tile body via markdown
    tile_html = f"""
<div style="
    background: {sty['color']}22;
    border-left: 4px solid {sty['color']};
    border-radius: 4px;
    padding: 0.6rem 0.8rem 0.4rem 0.8rem;
    margin-bottom: 0.5rem;
    min-height: 130px;
">
  <div style="font-size: 0.85rem; color: #888;">
    {sty['icon']} {sty['label']} · {state}
  </div>
  <div style="font-size: 1.05rem; font-weight: 600; line-height: 1.2; margin-top: 2px;">
    {op}
  </div>
  <div style="font-size: 0.9rem; color: #aaa; margin-top: 1px;">
    {city}
  </div>
  <div style="font-size: 0.8rem; margin-top: 6px;">
    {status_badge(r.get('status'))}
    {' · ✓ verified' if is_verified else ' · unverified'}
  </div>
  <div style="font-size: 0.75rem; color: #888; margin-top: 4px;">
    {'📊 sentiment' if has_sentiment else '·'}
    {' 📋 permits' if has_permits else ''}
  </div>
</div>
"""
    st.markdown(tile_html, unsafe_allow_html=True)
    if st.button("Open →", key=f"open_{fid}", use_container_width=True):
        st.query_params["facility"] = fid
        st.rerun()


# ---------------------------------------------------------------------------
# New-facility form (FIC Layer 1)
# ---------------------------------------------------------------------------

US_STATES = [
    "IA", "IL", "IN", "MN", "NE", "MO", "OH", "SD", "ND", "KS", "WI",
    "AR", "AL", "MS", "TN", "KY", "TX", "OK", "MI", "PA", "NY", "GA",
    "NC", "SC", "MD", "DE", "FL", "VA", "CA", "OR", "WA", "ID", "MT",
    "WY", "CO", "UT", "NM", "AZ", "NV", "ME", "VT", "NH", "MA", "RI",
    "CT", "NJ", "WV", "AK", "HI",
]
INDUSTRY_CODES = list(INDUSTRY_META.keys())
STATUSES = ["active", "idle", "closed", "announced", "under_construction", "unknown"]


def slugify(s: str) -> str:
    import re
    s = (s or "").lower().strip()
    s = re.sub(r"[^a-z0-9]+", "_", s).strip("_")
    return s[:60]


def render_new_facility():
    """Form to add a new facility to reference.facility_master."""
    if st.button("← Back to grid"):
        del st.query_params["new"]
        st.rerun()

    st.title("➕ Add a New Facility")
    st.caption(
        "Adds a row to `reference.facility_master`. The row will be tagged "
        "`data_source='user_form'` for provenance. Fields can be edited later."
    )

    with st.form("new_facility_form", clear_on_submit=False):
        c1, c2 = st.columns(2)
        with c1:
            operator = st.text_input("Operator (company name)*", "")
            parent = st.text_input("Parent company", "")
            city = st.text_input("City*", "")
            county = st.text_input("County", "")
            state = st.selectbox("State*", US_STATES, index=0)
            country = st.text_input("Country (ISO-2)", "US")
        with c2:
            industry_code = st.selectbox(
                "Industry*", INDUSTRY_CODES,
                format_func=lambda c: f"{INDUSTRY_META[c]['icon']} {INDUSTRY_META[c]['label']}",
            )
            status = st.selectbox("Status*", STATUSES, index=0)
            lat = st.number_input("Latitude", value=0.0, format="%.6f", step=0.001)
            lon = st.number_input("Longitude", value=0.0, format="%.6f", step=0.001)
            name = st.text_input("Facility name (display)", "",
                                 help="Defaults to '<Operator> — <City>' if blank")

        sources = st.text_input("Sources cited (URLs, docs)", "")
        notes = st.text_area("Notes", "", height=80,
                             help="Anything operationally relevant — capacity, history, etc.")

        # facility_id auto-generated from state + operator + city
        suggested_id = ""
        if state and operator and city:
            suggested_id = f"{state.lower()}.{slugify(operator)}_{slugify(city)}"
        facility_id = st.text_input(
            "facility_id (auto-suggested; edit if needed)",
            suggested_id,
            help="Stable canonical key. Convention: <state>.<operator>_<city>",
        )

        submitted = st.form_submit_button("Add facility", type="primary")

    if not submitted:
        return

    # Validation
    errs = []
    if not operator: errs.append("Operator is required.")
    if not city: errs.append("City is required.")
    if not industry_code: errs.append("Industry is required.")
    if not facility_id: errs.append("facility_id is required (use the auto-suggestion).")
    if errs:
        for e in errs: st.error(e)
        return

    if not name:
        name = f"{operator} — {city}"

    lat_v = lat if abs(lat) > 0.0001 else None
    lon_v = lon if abs(lon) > 0.0001 else None

    # Insert
    try:
        execute(
            """
            INSERT INTO reference.facility_master (
                facility_id, name, industry_code, operator, parent_company,
                city, county, state, country, lat, lon, status,
                data_source, sources, notes,
                verified_at, verified_by, verification_method,
                created_at, updated_at
            )
            VALUES (
                %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s,
                'user_form', %s, %s,
                NOW(), 'fic_user', 'manual_entry',
                NOW(), NOW()
            )
            """,
            (facility_id, name, industry_code, operator, (parent or None),
             city, (county or None), state, country, lat_v, lon_v, status,
             (sources or None), (notes or None)),
        )
        # Bust caches
        list_facilities.clear()
        st.success(f"Created `{facility_id}`. Running on-boarding hook…")

        # FIC Layer 3 — on-boarding hook
        try:
            sys_path = str(ROOT)
            import sys as _sys
            if sys_path not in _sys.path:
                _sys.path.insert(0, sys_path)
            from scripts.onboard_facility import onboard

            with st.spinner("Computing geographic edges, seeding news queries, "
                            "checking SEC ticker…"):
                ob = onboard(facility_id)

            st.markdown("### On-boarding result")
            c1, c2 = st.columns(2)
            with c1:
                de = ob.get("distance_edges", {})
                st.markdown(
                    f"**Distance edges:** {de.get('status')}  ·  "
                    f"{de.get('edges_added', 0)} added  "
                    f"(radius {de.get('radius_miles', '?')} mi, "
                    f"{de.get('peers_scanned', 0)} peers)"
                )
                if de.get("details"):
                    st.code("\n".join(de["details"][:8]), language="text")

                pe = ob.get("parent_edges", {})
                st.markdown(
                    f"**Parent-company edges:** {pe.get('status')}  ·  "
                    f"{pe.get('edges_added', 0)} added"
                )
                if pe.get("siblings"):
                    st.code("\n".join(pe["siblings"][:8]), language="text")
            with c2:
                ns = ob.get("news_source", {})
                if ns.get("added"):
                    st.markdown(f"**News source:** ✓ added  \n"
                                f"`{ns['source_name']}`  \n"
                                f"query: `{ns['query']}`")
                else:
                    st.markdown(f"**News source:** {ns.get('status')}  "
                                f"({ns.get('reason', '')})")

                sl = ob.get("sentiment_loop", {})
                st.markdown(f"**Sentiment loop:** {sl.get('status')}")
                st.caption(sl.get("note", ""))

                pt = ob.get("public_ticker", {})
                st.markdown(f"**Public ticker:** {pt.get('status')}"
                            + (f" — `{pt['ticker']}`" if pt.get("ticker") else ""))
                st.caption(pt.get("note", ""))

            st.markdown("---")
            if st.button("Open the new facility →", type="primary",
                         key=f"open_new_{facility_id}"):
                st.query_params["facility"] = facility_id
                if "new" in st.query_params: del st.query_params["new"]
                st.rerun()
            return  # stay on this page so the user can read the on-boarding result
        except Exception as ob_err:
            st.warning(
                f"Facility was created, but on-boarding hook failed: {ob_err}\n\n"
                f"You can run it later: "
                f"`python -m scripts.onboard_facility --facility-id {facility_id}`"
            )
            st.query_params["facility"] = facility_id
            if "new" in st.query_params: del st.query_params["new"]
            st.rerun()
    except psycopg2.IntegrityError as e:
        st.error(f"Insert failed (likely duplicate facility_id): {e}")
        get_conn().rollback()
    except Exception as e:
        st.error(f"Insert failed: {e}")
        try: get_conn().rollback()
        except Exception: pass


# ---------------------------------------------------------------------------
# Relationships (FIC Layer 1 — edit edges)
# ---------------------------------------------------------------------------

EDGE_TYPES = [
    # Strong / structural
    "parent_company",      # same operator group / corporate parent
    "ownership",           # equity stake / JV partner
    "draw_region",         # geographic catchment overlap
    "industry",             # same industry classification
    "supply_chain",        # buyer / seller relationship
    "logistics",           # rail / barge / truck route shared
    "competitive",         # direct rivalry for same customers
    # Weak / informational (Slice 1: 2026-05-09 — exec & board ties)
    "executive_move",      # person moved from operator A to operator B
    "shared_director",     # board member sits on both companies' boards
    "shared_advisor",      # consultant / banker / law firm shared
    "supplier_relationship",  # documented supplier link (less direct than supply_chain)
    "other",
]


@st.cache_data(ttl=30)
def fetch_edges_for(facility_id: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    out = query("""
        SELECT id, market_id, source_facility_id, target_facility_id,
               edge_type, weight, notes, is_active, updated_at
        FROM reference.facility_edge_weights
        WHERE source_facility_id = %s AND is_active = TRUE
        ORDER BY edge_type, weight DESC
    """, (facility_id,))
    inc = query("""
        SELECT id, market_id, source_facility_id, target_facility_id,
               edge_type, weight, notes, is_active, updated_at
        FROM reference.facility_edge_weights
        WHERE target_facility_id = %s AND is_active = TRUE
        ORDER BY edge_type, weight DESC
    """, (facility_id,))
    return pd.DataFrame(out), pd.DataFrame(inc)


@st.cache_data(ttl=30)
def fetch_exec_moves_for_operator(operator: str | None) -> pd.DataFrame:
    if not operator:
        return pd.DataFrame()
    rows = query("""
        SELECT id, person_name, role, from_operator, to_operator,
               event_date, announced_date, source_type, source_url,
               confidence, notes
        FROM silver.executive_move
        WHERE LOWER(from_operator) = LOWER(%s)
           OR LOWER(to_operator) = LOWER(%s)
        ORDER BY COALESCE(event_date, announced_date, extracted_at::date) DESC
    """, (operator, operator))
    return pd.DataFrame(rows)


@st.cache_data(ttl=30)
def fetch_shared_directors_for_operator(operator: str | None) -> pd.DataFrame:
    if not operator:
        return pd.DataFrame()
    rows = query("""
        SELECT operator_a_display, operator_b_display, person_name_a,
               role_a, role_b, year_a, year_b, both_active
        FROM gold.cross_company_director_links
        WHERE LOWER(operator_a_display) = LOWER(%s)
           OR LOWER(operator_b_display) = LOWER(%s)
        ORDER BY both_active DESC, GREATEST(COALESCE(year_a, 0), COALESCE(year_b, 0)) DESC
    """, (operator, operator))
    return pd.DataFrame(rows)


def render_relationships_tab(facility_id: str, all_facilities: pd.DataFrame):
    out_df, in_df = fetch_edges_for(facility_id)
    fac_row = all_facilities[all_facilities["facility_id"] == facility_id]
    operator = fac_row.iloc[0]["operator"] if len(fac_row) else None

    c1, c2 = st.columns(2)
    with c1:
        st.markdown(f"### Outgoing edges ({len(out_df)})")
        if len(out_df) == 0:
            st.caption("No outgoing relationships defined.")
        else:
            for _, r in out_df.iterrows():
                cols = st.columns([3, 1, 1])
                cols[0].markdown(
                    f"**{r['edge_type']}** → `{r['target_facility_id']}`  \n"
                    f"weight: {r['weight']:.2f}"
                    + (f"  ·  {r['notes']}" if r["notes"] else "")
                )
                cols[1].caption(str(r["updated_at"])[:10])
                if cols[2].button("✕", key=f"delout_{r['id']}", help="Soft-delete this edge"):
                    execute(
                        "UPDATE reference.facility_edge_weights "
                        "SET is_active = FALSE, updated_at = NOW() WHERE id = %s",
                        (r["id"],),
                    )
                    fetch_edges_for.clear()
                    st.rerun()
    with c2:
        st.markdown(f"### Incoming edges ({len(in_df)})")
        if len(in_df) == 0:
            st.caption("No incoming relationships defined.")
        else:
            for _, r in in_df.iterrows():
                st.markdown(
                    f"`{r['source_facility_id']}` → **{r['edge_type']}**  \n"
                    f"weight: {r['weight']:.2f}"
                    + (f"  ·  {r['notes']}" if r["notes"] else "")
                )

    st.divider()
    st.markdown("### Add edge")

    # Build a human-friendly searchable label for each facility:
    # "AGP — Eagle Grove (IA) · ia.agp_eagle_grove"
    others = all_facilities[all_facilities["facility_id"] != facility_id].copy()
    others["__label"] = (
        others["operator"].fillna("(no operator)").astype(str)
        + " — "
        + others["city"].fillna("(no city)").astype(str)
        + " ("
        + others["state"].fillna("?").astype(str)
        + ") · "
        + others["facility_id"]
    )
    others = others.sort_values("__label").reset_index(drop=True)
    label_to_id = dict(zip(others["__label"], others["facility_id"]))

    with st.form(f"add_edge_{facility_id}"):
        c1, c2, c3 = st.columns([2, 1.5, 1])
        with c1:
            target_label = st.selectbox(
                "Target facility (type to search)",
                options=list(label_to_id.keys()),
                help="Streamlit selectboxes are search-as-you-type — start "
                     "typing operator, city, or state.",
            )
            target = label_to_id[target_label]
        with c2:
            etype = st.selectbox("Edge type", EDGE_TYPES)
        with c3:
            weight = st.number_input("Weight", value=1.0, step=0.1, format="%.2f")
        edge_notes = st.text_input("Notes (optional)", "")
        market_id = st.text_input("market_id (optional)",
                                  value="iowa_oilseed_crush_v1",
                                  help="Tags this edge to a Market Field market scope")
        submitted = st.form_submit_button("Add edge", type="primary")

    if submitted:
        try:
            execute(
                """
                INSERT INTO reference.facility_edge_weights (
                    market_id, source_facility_id, target_facility_id,
                    edge_type, weight, notes, is_active, created_at, updated_at
                ) VALUES (%s, %s, %s, %s, %s, %s, TRUE, NOW(), NOW())
                """,
                (market_id or None, facility_id, target, etype, weight,
                 edge_notes or None),
            )
            fetch_edges_for.clear()
            st.success(f"Added edge: {facility_id} —[{etype}]→ {target}")
            st.rerun()
        except Exception as e:
            st.error(f"Insert failed: {e}")
            try: get_conn().rollback()
            except Exception: pass

    # ----- Exec moves involving this facility's operator ---------------------
    st.divider()
    st.markdown("### Personnel moves involving this operator")
    if not operator:
        st.caption("No operator set on this facility — can't search for personnel moves.")
    else:
        em = fetch_exec_moves_for_operator(operator)
        if len(em) == 0:
            st.caption(f"No executive moves recorded for `{operator}` yet. "
                       f"Add one below.")
        else:
            for _, r in em.iterrows():
                arrow = "→" if r['to_operator'] and r['to_operator'].lower() == operator.lower() \
                       else "←" if r['from_operator'] and r['from_operator'].lower() == operator.lower() \
                       else "↔"
                date_str = (str(r.get('event_date') or r.get('announced_date'))[:10]
                            if r.get('event_date') or r.get('announced_date') else "?")
                src_label = ""
                if r.get("source_url"):
                    src_label = f"  ·  [source]({r['source_url']})"
                st.markdown(
                    f"- **{r['person_name']}** ({r.get('role') or 'role unknown'}): "
                    f"`{r.get('from_operator') or '?'}` {arrow} "
                    f"`{r.get('to_operator') or '?'}`  ·  *{date_str}*  "
                    f"({r.get('source_type', '?')}){src_label}"
                )
                if r.get("notes"):
                    st.caption(r["notes"])

        # Add-exec-move form
        with st.expander("➕ Record an executive move"):
            with st.form(f"add_exec_move_{facility_id}"):
                c1, c2 = st.columns(2)
                with c1:
                    em_person = st.text_input("Person name *", "")
                    em_role = st.text_input("Role / title", "",
                                            help="e.g. CFO, VP Origination")
                    em_from = st.text_input("From operator", "",
                                            help="Prior employer (string match)")
                    em_to = st.text_input("To operator *",
                                          value=operator or "",
                                          help="New employer")
                with c2:
                    em_event_date = st.date_input("Event date (effective)",
                                                  value=None, format="YYYY-MM-DD")
                    em_announced_date = st.date_input("Announced date",
                                                      value=None, format="YYYY-MM-DD")
                    em_source_type = st.selectbox(
                        "Source type",
                        ["news_article", "press_release", "linkedin", "manual"],
                        index=0,
                    )
                    em_source_url = st.text_input("Source URL", "")
                em_notes = st.text_area("Notes", "", height=70)
                em_submit = st.form_submit_button("Add move", type="primary")

            if em_submit:
                if not em_person or not em_to:
                    st.error("Person name and 'to operator' are required.")
                else:
                    person_norm = re.sub(r"[^a-z]+", "_",
                                         em_person.lower()).strip("_")
                    try:
                        execute("""
                            INSERT INTO silver.executive_move (
                                person_name, person_normalized, role,
                                from_operator, to_operator,
                                event_date, announced_date,
                                source_type, source_url,
                                confidence, notes,
                                extracted_by
                            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s,
                                      1.0, %s, 'fic_user')
                        """, (
                            em_person, person_norm, (em_role or None),
                            (em_from or None), em_to,
                            em_event_date, em_announced_date,
                            em_source_type, (em_source_url or None),
                            (em_notes or None),
                        ))
                        fetch_exec_moves_for_operator.clear()
                        st.success(f"Recorded: {em_person} → {em_to}")
                        st.rerun()
                    except Exception as e:
                        st.error(f"Insert failed: {e}")
                        try: get_conn().rollback()
                        except Exception: pass

    # ----- Shared directors (board overlap) ----------------------------------
    if operator:
        sd = fetch_shared_directors_for_operator(operator)
        if len(sd) > 0:
            st.divider()
            st.markdown(f"### Shared board members involving {operator}")
            for _, r in sd.iterrows():
                tag = "🟢 both active" if r["both_active"] else "🟡 historical"
                other = (r["operator_b_display"]
                         if r["operator_a_display"].lower() == operator.lower()
                         else r["operator_a_display"])
                st.markdown(
                    f"- **{r['person_name_a']}**: serves on `{operator}` "
                    f"({r.get('role_a') or 'director'}) "
                    f"AND `{other}` ({r.get('role_b') or 'director'})  ·  {tag}"
                )



# ---------------------------------------------------------------------------
# Due Diligence tab (FIC Layer 4)
# ---------------------------------------------------------------------------

@st.cache_data(ttl=30)
def fetch_dd_history(facility_id: str) -> pd.DataFrame:
    rows = query("""
        SELECT id, generated_at, generated_by, model, prompt_version,
               input_tokens, output_tokens, cost_usd, elapsed_sec
        FROM silver.due_diligence_report
        WHERE facility_id = %s
        ORDER BY generated_at DESC
    """, (facility_id,))
    return pd.DataFrame(rows)


@st.cache_data(ttl=30)
def fetch_dd_report(report_id: int) -> dict | None:
    rows = query("""
        SELECT id, facility_id, generated_at, generated_by, model,
               prompt_version, report_json, report_markdown,
               input_summary, input_tokens, output_tokens, cost_usd,
               elapsed_sec
        FROM silver.due_diligence_report
        WHERE id = %s
    """, (report_id,))
    return rows[0] if rows else None


def render_due_diligence_tab(facility_id: str):
    history = fetch_dd_history(facility_id)
    n = len(history)

    top = st.columns([3, 1, 1])
    with top[0]:
        st.markdown("### Due-Diligence Reports")
        if n == 0:
            st.caption("No reports generated yet for this facility.")
        else:
            st.caption(
                f"{n} report{'s' if n != 1 else ''} on file. "
                f"Most recent: {history.iloc[0]['generated_at'].strftime('%Y-%m-%d %H:%M')}"
                f"  ·  ${float(history.iloc[0]['cost_usd'] or 0):.4f}"
            )

    with top[1]:
        st.markdown("###")
        gen_label = "Regenerate" if n > 0 else "Generate report"
        if st.button(gen_label, type="primary", use_container_width=True,
                     key=f"gen_{facility_id}"):
            with st.spinner(
                "Calling Claude…  pulls facility profile + edges + permits + "
                "sentiment + recent news + operator's SEC filings (when public) "
                "+ KG context. ~30-60 seconds."
            ):
                try:
                    import sys as _sys
                    _sys.path.insert(0, str(ROOT))
                    from scripts.due_diligence_agent import generate_report, save_report
                    result = generate_report(facility_id)
                    save_report(result, generated_by="fic_user")
                    fetch_dd_history.clear()
                    st.success(
                        f"Generated. ${result['cost_usd']:.4f}, "
                        f"{result['elapsed_sec']:.1f}s, "
                        f"{result['input_tokens']}↓ / {result['output_tokens']}↑ tokens."
                    )
                    st.rerun()
                except Exception as e:
                    st.error(f"Generation failed: {e}")

    with top[2]:
        st.markdown("###")
        if n > 0:
            if st.button("History", use_container_width=True,
                         key=f"hist_{facility_id}"):
                st.session_state[f"dd_show_history_{facility_id}"] = \
                    not st.session_state.get(f"dd_show_history_{facility_id}", False)

    if n == 0:
        st.info(
            "Click **Generate report** to produce a structured due-diligence "
            "brief. The agent pulls every piece of intelligence we have on "
            "this facility plus the operator's last 10 SEC filings (when "
            "publicly traded), and asks Claude to synthesize a banker-grade "
            "report covering exec summary, facility profile, operator overview, "
            "market position, material events, risk factors, and recommendation."
        )
        return

    # Optional history pane
    if st.session_state.get(f"dd_show_history_{facility_id}"):
        with st.expander("All reports for this facility", expanded=True):
            display = history.copy()
            display["generated_at"] = display["generated_at"].dt.strftime("%Y-%m-%d %H:%M")
            display["cost_usd"] = display["cost_usd"].apply(
                lambda x: f"${float(x):.4f}" if x is not None else "—"
            )
            st.dataframe(display, use_container_width=True, hide_index=True)

    # Show the most recent report
    latest_id = int(history.iloc[0]["id"])
    rep = fetch_dd_report(latest_id)
    if not rep:
        st.error(f"Could not load report {latest_id}")
        return

    # Metadata strip
    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Generated", rep["generated_at"].strftime("%Y-%m-%d %H:%M"))
    m2.metric("Cost", f"${float(rep['cost_usd'] or 0):.4f}")
    m3.metric("Tokens", f"{rep['input_tokens']}↓/{rep['output_tokens']}↑")
    m4.metric("Elapsed", f"{float(rep['elapsed_sec'] or 0):.1f}s")

    # Input summary (what fed the report)
    with st.expander("Input data used to generate this report", expanded=False):
        st.json(rep["input_summary"], expanded=True)

    # The report itself
    st.divider()
    st.markdown(rep["report_markdown"])


# ---------------------------------------------------------------------------
# Detail view
# ---------------------------------------------------------------------------

def render_detail(facility_id: str):
    fac = fetch_facility(facility_id)
    if not fac:
        st.error(f"Facility `{facility_id}` not found.")
        if st.button("← Back to grid"):
            del st.query_params["facility"]
            st.rerun()
        return

    sty = industry_style(fac["industry_code"])

    # Back navigation
    if st.button("← Back to grid"):
        del st.query_params["facility"]
        st.rerun()

    # --- Header -------------------------------------------------------------
    st.markdown(f"## {sty['icon']} {fac.get('name') or fac.get('operator')}")
    st.caption(
        f"`{facility_id}`  ·  {sty['label']}  ·  "
        f"{fac.get('city') or '?'}, {fac.get('state') or '?'}"
        + (f"  ·  {fac.get('county')} County" if fac.get("county") else "")
    )

    # Quick facts row
    h1, h2, h3, h4 = st.columns(4)
    h1.metric("Status", status_badge(fac.get("status")))
    h2.metric("Operator", fac.get("operator") or "—")
    h3.metric("Parent", fac.get("parent_company") or "—")
    h4.metric("Verified", "Yes" if fac.get("verified_at") else "No")

    # --- Tabs ---------------------------------------------------------------
    (tab_overview, tab_sentiment, tab_news, tab_permits, tab_edges,
     tab_dd, tab_raw) = st.tabs(
        ["Overview", "Sentiment", "News", "Permits", "Relationships",
         "Due Diligence", "Raw record"]
    )

    with tab_overview:
        render_overview(fac)
    with tab_sentiment:
        render_sentiment_tab(facility_id)
    with tab_news:
        render_news_tab(facility_id)
    with tab_permits:
        render_permits_tab(fac)
    with tab_edges:
        render_relationships_tab(facility_id, list_facilities())
    with tab_dd:
        render_due_diligence_tab(facility_id)
    with tab_raw:
        st.json(fac, expanded=False)


def render_overview(fac: dict):
    c1, c2 = st.columns([1.2, 1])
    with c1:
        st.markdown("### Location")
        if fac.get("lat") and fac.get("lon"):
            map_df = pd.DataFrame({
                "lat": [float(fac["lat"])],
                "lon": [float(fac["lon"])],
            })
            st.map(map_df, zoom=10, use_container_width=True)
            st.caption(f"{float(fac['lat']):.4f}, {float(fac['lon']):.4f}")
        else:
            st.info("No coordinates on file.")

    with c2:
        st.markdown("### Provenance")
        prov_rows = [
            ("Data source",    fac.get("data_source") or "—"),
            ("Verified at",    str(fac.get("verified_at") or "—")[:19]),
            ("Verified by",    fac.get("verified_by") or "—"),
            ("Method",         fac.get("verification_method") or "—"),
            ("Sources cited",  fac.get("sources") or "—"),
            ("Created",        str(fac.get("created_at") or "—")[:19]),
            ("Updated",        str(fac.get("updated_at") or "—")[:19]),
        ]
        for k, v in prov_rows:
            st.markdown(f"**{k}**: {v}")

    if fac.get("notes"):
        st.markdown("### Notes")
        st.text(fac["notes"])

    # --- Danger zone ---------------------------------------------------------
    st.divider()
    with st.expander("⚠️ Danger zone — delete this facility"):
        st.markdown(
            "Hard-deletes the row from `reference.facility_master` (or "
            "`reference.oilseed_crush_facilities` if it lives there). All "
            "edges in `reference.facility_edge_weights` referencing this "
            "facility are also hard-deleted. This is reversible only by "
            "writing a new migration to recreate the row."
        )
        confirm = st.text_input(
            f"To confirm, type the facility_id exactly: `{fac['facility_id']}`",
            key=f"confirm_{fac['facility_id']}",
        )
        if st.button("Delete this facility permanently",
                     type="primary", disabled=(confirm != fac["facility_id"])):
            try:
                # Edges first (FK-style cleanup, even though no FK is enforced)
                ne = execute(
                    """
                    DELETE FROM reference.facility_edge_weights
                    WHERE source_facility_id = %s OR target_facility_id = %s
                    """,
                    (fac["facility_id"], fac["facility_id"]),
                )
                # Facility row — try both tables
                nf = execute(
                    "DELETE FROM reference.facility_master WHERE facility_id = %s",
                    (fac["facility_id"],),
                )
                noc = execute(
                    "DELETE FROM reference.oilseed_crush_facilities WHERE facility_id = %s",
                    (fac["facility_id"],),
                )
                # Bust caches
                list_facilities.clear()
                fetch_edges_for.clear()
                st.success(
                    f"Deleted `{fac['facility_id']}` "
                    f"(rows: master={nf}, oilseed_crush={noc}, edges={ne}). "
                    f"Returning to grid…"
                )
                if "facility" in st.query_params: del st.query_params["facility"]
                st.rerun()
            except Exception as e:
                st.error(f"Delete failed: {e}")
                try: get_conn().rollback()
                except Exception: pass


def render_sentiment_tab(facility_id: str):
    df = fetch_sentiment(facility_id)
    if len(df) == 0:
        st.info("No sentiment data for this facility yet. (Market Field daily "
                "loop populates this for facilities with news pipeline coverage.)")
        return

    df = df.sort_values("as_of_date")
    st.line_chart(df.set_index("as_of_date")[["news_count"]],
                  height=250, use_container_width=True)
    st.caption(f"News mentions per day · last {len(df)} days of data")

    if "topic_sentiments" in df.columns and df["topic_sentiments"].notna().any():
        st.markdown("### Topic sentiment timeseries")
        # topic_sentiments is JSON. Pivot recent rows.
        recent = df.tail(30).copy()
        topics = pd.json_normalize(recent["topic_sentiments"].dropna())
        topics["as_of_date"] = recent["as_of_date"].values[: len(topics)]
        st.dataframe(topics.set_index("as_of_date"), use_container_width=True)


def render_news_tab(facility_id: str):
    df = fetch_recent_news(facility_id)
    if len(df) == 0:
        st.info("No news articles tagged to this facility yet.")
        return
    for _, r in df.iterrows():
        ts = str(r["published_at"])[:10] if pd.notna(r.get("published_at")) else ""
        title = r.get("title") or "(no title)"
        src = r.get("source_name") or ""
        url = r.get("article_url") or ""
        conf = r.get("confidence_score")
        conf_str = f" · confidence: `{conf:.2f}`" if conf is not None else ""
        if url:
            st.markdown(f"- **[{title}]({url})**  \n  *{src} · {ts}*{conf_str}")
        else:
            st.markdown(f"- **{title}**  \n  *{src} · {ts}*{conf_str}")


def render_permits_tab(fac: dict):
    df = fetch_permits(fac)
    if len(df) == 0:
        st.info(
            "No air permit data extracted for this facility yet. (Title V "
            "permit extraction pipeline target — `project_state_air_permits_llm.md`. "
            "Matching is fuzzy on state + name/operator + city — false negatives are "
            "possible if operator naming differs.)"
        )
        return
    # Top-level summary
    st.markdown(f"**{len(df)} permit record(s) matched** for this facility.")
    for _, r in df.iterrows():
        with st.expander(
            f"{r['permit_type']} {r['permit_number']} — "
            f"{r['facility_name']} ({r.get('n_units') or 0} units)"
        ):
            st.write(f"Operator: {r.get('operator')}  ·  "
                     f"City: {r.get('city')}  ·  County: {r.get('county')}")
            st.write(f"Industry: {r.get('industry')}  ·  "
                     f"Expires: {r.get('expiration_date')}")
            if r.get("units"):
                st.json(r["units"], expanded=False)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    if st.query_params.get("new"):
        render_new_facility()
        return
    facility_id = st.query_params.get("facility")
    if facility_id:
        render_detail(facility_id)
    else:
        render_grid()


if __name__ == "__main__":
    main()
