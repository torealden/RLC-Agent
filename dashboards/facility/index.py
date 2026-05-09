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
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
import psycopg2
import psycopg2.extras
import streamlit as st
from dotenv import load_dotenv

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
    return psycopg2.connect(
        host=os.environ.get("RLC_PG_HOST", "localhost"),
        port=os.environ.get("RLC_PG_PORT", "5432"),
        database=os.environ.get("RLC_PG_DATABASE", "rlc_commodities"),
        user=os.environ.get("RLC_PG_USER", "postgres"),
        password=os.environ.get("RLC_PG_PASSWORD", ""),
    )


def query(sql: str, params=None) -> list[dict]:
    conn = get_conn()
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(sql, params or [])
        return cur.fetchall()


def execute(sql: str, params=None) -> int:
    """Execute a write statement, commit, return rowcount."""
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute(sql, params or [])
        conn.commit()
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

    # --- Grid ---------------------------------------------------------------
    if len(f) == 0:
        st.info("No facilities match these filters.")
        return

    # Render in rows of 4 tiles
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
        st.success(f"Created `{facility_id}`. Opening detail page…")
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

EDGE_TYPES = ["draw_region", "parent_company", "industry", "supply_chain",
              "logistics", "competitive", "ownership", "other"]


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


def render_relationships_tab(facility_id: str, all_facilities: pd.DataFrame):
    out_df, in_df = fetch_edges_for(facility_id)

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
    other_ids = sorted(
        [fid for fid in all_facilities["facility_id"].tolist() if fid != facility_id]
    )
    with st.form(f"add_edge_{facility_id}"):
        c1, c2, c3 = st.columns([2, 1.5, 1])
        with c1:
            target = st.selectbox("Target facility", other_ids)
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
    tab_overview, tab_sentiment, tab_news, tab_permits, tab_edges, tab_raw = st.tabs(
        ["Overview", "Sentiment", "News", "Permits", "Relationships", "Raw record"]
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
