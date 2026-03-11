"""RLC-Agent Operations Dashboard — System health at a glance."""

import streamlit as st
import pandas as pd
from datetime import datetime

st.set_page_config(page_title="RLC Ops Dashboard", page_icon="📡", layout="wide")

# ---------------------------------------------------------------------------
# Import DB helpers (handle import errors gracefully)
# ---------------------------------------------------------------------------
try:
    from db import (
        get_health_inputs,
        get_data_freshness,
        get_active_alerts,
        get_recent_failures,
        get_daily_run_counts,
        get_collector_success_rates,
        is_dispatcher_running,
    )

    DB_OK = True
except Exception as e:
    DB_OK = False
    DB_ERROR = str(e)


# ---------------------------------------------------------------------------
# Schedule reference (from master_scheduler.py RELEASE_SCHEDULES)
# ---------------------------------------------------------------------------
SCHEDULES = [
    ("futures_overnight", "daily", "Weekdays 08:45 ET", 3),
    ("futures_us_session", "daily", "Weekdays 14:30 ET", 3),
    ("futures_settlement", "daily", "Weekdays 18:00 ET", 2),
    ("cme_settlements", "daily", "Weekdays 17:00 ET", 2),
    ("usda_nass", "weekly", "Monday 16:00 ET", 1),
    ("eia_petroleum", "weekly", "Wednesday 10:30 ET", 1),
    ("eia_ethanol", "weekly", "Wednesday 10:30 ET", 1),
    ("usda_fas", "weekly", "Thursday 08:30 ET", 1),
    ("drought", "weekly", "Thursday 08:30 ET", 2),
    ("canada_cgc", "weekly", "Thursday 13:30 ET", 3),
    ("cftc_cot", "weekly", "Friday 15:30 ET", 1),
    ("usda_ams_tallow", "weekly", "Friday 14:00 ET", 4),
    ("usda_ams_ddgs", "weekly", "Friday 14:00 ET", 4),
    ("usda_wasde", "monthly", "~12th 12:00 ET", 1),
    ("nopa_crush", "monthly", "~15th 12:00 ET (disabled)", 2),
    ("mpob", "monthly", "~10th 04:00 ET", 2),
    ("census_trade", "monthly", "~6th 08:30 ET", 3),
    ("epa_rfs", "monthly", "~15th 12:00 ET", 3),
    ("canada_statscan", "monthly", "~20th 08:30 ET", 3),
    ("conab_safras", "monthly", "~14th 09:00 ET", 1),
    ("conab_supply_demand", "monthly", "~14th 09:30 ET", 2),
    ("usda_nass_stocks", "quarterly", "~1st 12:00 ET", 1),
    ("canada_statscan_stocks", "quarterly", "Varies 08:30 ET", 2),
    ("usda_ers_feed_grains", "on_demand", "—", 5),
    ("usda_ers_oil_crops", "on_demand", "—", 5),
    ("usda_ers_wheat", "on_demand", "—", 5),
]

# ---------------------------------------------------------------------------
# Header
# ---------------------------------------------------------------------------
st.title("RLC-Agent Operations Dashboard")

col_time, col_refresh = st.columns([3, 1])
with col_time:
    st.caption(f"Last refreshed: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
with col_refresh:
    if st.button("Refresh Now"):
        st.rerun()

# Auto-refresh (default 5 minutes)
refresh_sec = st.sidebar.number_input("Auto-refresh (seconds)", min_value=30, value=300, step=30)

if not DB_OK:
    st.error(f"Cannot connect to database: {DB_ERROR}")
    st.stop()

# ---------------------------------------------------------------------------
# 1. Health Score Banner
# ---------------------------------------------------------------------------
st.divider()

try:
    health = get_health_inputs()
except Exception as e:
    st.warning(f"Could not compute health score: {e}")
    health = None

if health:
    score = health["score"]
    if score >= 80:
        color = "green"
    elif score >= 50:
        color = "orange"
    else:
        color = "red"

    h_cols = st.columns(5)
    h_cols[0].metric("Health Score", f"{score}/100")
    h_cols[1].metric("Sources OK", health["ok"])
    h_cols[2].metric("Overdue", health["overdue"])
    h_cols[3].metric("Failed (24h)", health["failed_24h"])
    h_cols[4].metric("Stale", health["stale"])

    # Dispatcher status
    disp = is_dispatcher_running()
    st.sidebar.markdown(f"**Dispatcher:** {'Running' if disp else 'Stopped'}")
else:
    st.info("No collection data yet — run the dispatcher or execute a manual collection.")

# ---------------------------------------------------------------------------
# 2. Data Freshness Table
# ---------------------------------------------------------------------------
st.divider()
st.subheader("Data Freshness")

try:
    df_fresh = get_data_freshness()
    if df_fresh.empty:
        st.info("No data freshness records found. Run some collectors first.")
    else:
        def freshness_status(row):
            if row.get("is_overdue"):
                return "🔴 Overdue"
            hrs = row.get("hours_since_collection")
            if hrs is None:
                return "⚪ Never"
            if hrs > 24:
                return "🟡 Stale"
            return "🟢 OK"

        df_fresh["Status"] = df_fresh.apply(freshness_status, axis=1)
        display_cols = ["collector_name", "display_name", "last_collected",
                        "hours_since_collection", "expected_frequency", "last_status", "Status"]
        show_cols = [c for c in display_cols if c in df_fresh.columns]
        df_show = df_fresh[show_cols].copy()
        if "hours_since_collection" in df_show.columns:
            df_show["hours_since_collection"] = df_show["hours_since_collection"].apply(
                lambda x: f"{x:.1f}" if pd.notna(x) else "—"
            )
        st.dataframe(df_show, width="stretch", hide_index=True)
except Exception as e:
    st.error(f"Error loading freshness data: {e}")

# ---------------------------------------------------------------------------
# 3. Active Alerts
# ---------------------------------------------------------------------------
st.divider()
st.subheader("Active Alerts")

try:
    df_alerts = get_active_alerts()
    if df_alerts.empty:
        st.success("No active alerts.")
    else:
        for _, row in df_alerts.iterrows():
            severity_icon = {1: "🔴", 2: "🟠"}.get(row.get("priority", 3), "🟡")
            with st.expander(
                f"{severity_icon} [{row.get('source', '?')}] {row.get('summary', '')}  —  "
                f"{row.get('event_time', '')}"
            ):
                st.write(f"**Type:** {row.get('event_type', '')}")
                details = row.get("details")
                if details:
                    st.json(details)
except Exception as e:
    st.error(f"Error loading alerts: {e}")

# ---------------------------------------------------------------------------
# 4. Recent Failures (last 7 days)
# ---------------------------------------------------------------------------
st.divider()
st.subheader("Recent Failures (7 days)")

try:
    df_fail = get_recent_failures()
    if df_fail.empty:
        st.success("No failures in the last 7 days.")
    else:
        for _, row in df_fail.iterrows():
            dur = row.get("duration_sec")
            dur_str = f"{dur}s" if dur and pd.notna(dur) else "—"
            with st.expander(
                f"**{row.get('collector_name', '?')}** — "
                f"{row.get('run_started_at', '')}  ({dur_str})"
            ):
                st.code(row.get("error_message", "No error message recorded."))
except Exception as e:
    st.error(f"Error loading failures: {e}")

# ---------------------------------------------------------------------------
# 5. Success Rate Trends (last 30 days)
# ---------------------------------------------------------------------------
st.divider()
st.subheader("Collection Trends (30 days)")

try:
    df_daily = get_daily_run_counts()
    if df_daily.empty:
        st.info("No collection runs in the last 30 days.")
    else:
        pivot = df_daily.pivot_table(index="run_date", columns="status", values="n", fill_value=0)
        st.bar_chart(pivot)

    df_rates = get_collector_success_rates()
    if not df_rates.empty:
        st.dataframe(df_rates, width="stretch", hide_index=True)
except Exception as e:
    st.error(f"Error loading trends: {e}")

# ---------------------------------------------------------------------------
# 6. Schedule Overview
# ---------------------------------------------------------------------------
st.divider()
st.subheader("Schedule Overview")

df_sched = pd.DataFrame(SCHEDULES, columns=["Collector", "Frequency", "Expected Release", "Priority"])
df_sched = df_sched.sort_values(["Priority", "Frequency"])

st.dataframe(df_sched, width="stretch", hide_index=True)

# ---------------------------------------------------------------------------
# Auto-refresh via st.rerun after delay
# ---------------------------------------------------------------------------
import time as _time

_time.sleep(refresh_sec)
st.rerun()
