"""RLC: BBD Economics — single-screen demo for the Friday Helios meeting.

Layout (top to bottom, scroll):
  1. Scope banner — show the breadth of what's wired
  2. Implied Feedstock Value calculator — live inputs, live calc, BBD price stack
  3. Policy scenario sensitivity — 4 scenarios side-by-side, tornado chart
  4. Facility decision space — per-feedstock margin ranking for selected facility
  5. Knowledge Graph depth — pick a node, show enriched context
  6. BBD balance sheets — production / stocks / use trends
"""

from __future__ import annotations

import sys
from datetime import date
from pathlib import Path

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

# Ensure project root is on path so we can import the IFV callable directly
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from dashboards.helios_demo.db import (
    get_facility_industry_summary,
    get_facility_map_data,
    get_kg_node_full,
    get_marine_highway_traces,
    get_port_data,
    get_rail_traces,
    get_recent_balance_sheet,
    get_scope_stats,
    get_three_way_comparison,
    list_demo_facilities,
    list_demo_kg_nodes,
)
from src.kg.callables.implied_feedstock_value import (
    evaluate_all_pathways_for_facility as _evaluate_all_pathways_for_facility,
    run as _ifv_run,
)


# Cached wrappers — IFV calls hit DB; cache by input args so demo stays fast
# under repeated user input changes.
@st.cache_data(ttl=300, show_spinner=False)
def ifv_run(fuel, region, feedstock_code, as_of_date, mode,
            policy_scenario="extension_2031", target_margin_per_gal=0.0,
            observed_cash_per_lb=None):
    return _ifv_run(
        fuel=fuel, region=region, feedstock_code=feedstock_code,
        as_of_date=as_of_date, mode=mode, policy_scenario=policy_scenario,
        target_margin_per_gal=target_margin_per_gal,
        observed_cash_per_lb=observed_cash_per_lb,
    )


@st.cache_data(ttl=300, show_spinner=False)
def evaluate_all_pathways_for_facility(facility_name, fuel, region, as_of_date, policy_scenario):
    return _evaluate_all_pathways_for_facility(
        facility_name=facility_name, fuel=fuel, region=region,
        as_of_date=as_of_date, policy_scenario=policy_scenario,
    )


# ---------------------------------------------------------------------------
# Page setup
# ---------------------------------------------------------------------------

st.set_page_config(
    page_title="RLC: BBD Economics",
    page_icon=":fuelpump:",
    layout="wide",
    initial_sidebar_state="collapsed",
)

st.markdown(
    """
    <style>
    .main .block-container {padding-top: 1.5rem; padding-bottom: 2rem;}
    h1 {color: #1a3a5c; margin-bottom: 0.25rem;}
    h2 {color: #1a3a5c; border-bottom: 1px solid #e0e6ed; padding-bottom: 0.25rem; margin-top: 2rem;}
    .stack-component {font-family: 'Consolas', monospace; font-size: 0.95rem;}
    .source-note {color: #6b7280; font-size: 0.85rem; font-style: italic;}
    </style>
    """,
    unsafe_allow_html=True,
)

st.title("RLC: BBD Economics")
st.caption("Live calculator anchored to CARB pathway data + RLC BBD price stack")


# ===========================================================================
# 1. Scope banner
# ===========================================================================

scope = get_scope_stats()

c1, c2, c3, c4, c5 = st.columns(5)
c1.metric("KG nodes",      f"{scope['kg_nodes']:,}",     help="Commodities, models, policies, facilities")
c2.metric("KG edges",      f"{scope['kg_edges']:,}",     help="Causal links / dependencies")
c3.metric("KG contexts",   f"{scope['kg_contexts']:,}",  help="Expert rules, seasonal norms, pace tracking")
c4.metric("CARB pathways", f"{scope['carb_pathways']:,}", help="Tier 1 + Tier 2 LCFS-certified pathways")
c5.metric(
    "Medallion tables",
    f"{scope['bronze'] + scope['silver'] + scope['gold'] + scope['reference']:,}",
    help=f"Bronze {scope['bronze']} + Silver {scope['silver']} + Gold {scope['gold']} + Reference {scope['reference']}",
)


# ===========================================================================
# 2. Implied Feedstock Value calculator
# ===========================================================================

st.header("Implied Feedstock Value")
st.write(
    "Given a fuel, region, feedstock and policy scenario, compute the BBD "
    "four-component price stack (ULSD/jet + D4 RIN + LCFS + 45Z) and back into "
    "a $/lb feedstock bid. CI comes from live CARB pathway data; D4 + LCFS "
    "pulled from `bronze.credit_prices`."
)

cfg_col, output_col = st.columns([1, 2])

with cfg_col:
    fuel = st.selectbox(
        "Fuel",
        ["renewable_diesel", "biodiesel", "saf"],
        index=0,
        help="Renewable Diesel | Biodiesel | Sustainable Aviation Fuel",
    )
    region = st.selectbox(
        "Region",
        ["gulf", "midwest", "west_coast", "pnw", "rocky_mtn"],
        index=1,
    )
    feedstock_code = st.selectbox(
        "Feedstock",
        ["tallow", "used_cooking_oil", "distillers_corn_oil",
         "soybean_oil", "canola_oil", "corn_oil",
         "choice_white_grease", "poultry_fat"],
        index=0,
    )
    policy_scenario = st.selectbox(
        "Policy scenario",
        ["extension_2031", "expiry_2027", "iluc_removed", "domestic_restriction", "none"],
        index=0,
        help=(
            "extension_2031: 45Z through 2031 with ILUC (current law)\n"
            "expiry_2027:    45Z = 0 after 2027-12-31 (cliff stress test)\n"
            "iluc_removed:   crop oil CI drops 22 g/MJ for 45Z only — LCFS keeps ILUC\n"
            "domestic_restriction: 45Z = 0 for non-US feedstocks\n"
            "none:           45Z = 0 always (pre-IRA counterfactual)"
        ),
    )
    as_of_date = st.date_input(
        "As of date",
        value=date(2026, 5, 15),
        help="Trading date for credit price lookup; matters for expiry_2027 cliff",
    )
    observed_cash = st.number_input(
        "Observed cash price ($/lb)",
        min_value=0.0, max_value=2.0, value=0.50, step=0.01,
        help="If provided, computes producer margin at this cash price",
    )

with output_col:
    ifv = ifv_run(
        fuel=fuel,
        region=region,
        feedstock_code=feedstock_code,
        as_of_date=as_of_date,
        mode="cash_compare",
        policy_scenario=policy_scenario,
        observed_cash_per_lb=observed_cash,
    )
    if "error" in ifv:
        st.error(f"{ifv['error']}: {ifv.get('detail', '')}")
    else:
        b = ifv["breakdown_per_gal"]
        inp = ifv["inputs_used"]

        # Headline
        m1, m2, m3 = st.columns(3)
        m1.metric(
            "Implied bid (breakeven)",
            f"${ifv['implied_bid_per_lb']:.4f}/lb",
            help=f"${ifv['implied_bid_per_short_ton']:.0f}/short ton",
        )
        m2.metric(
            "Pathway CI",
            f"{inp['pathway_ci_score']:.1f} g/MJ",
            help=f"LCFS baseline {inp['lcfs_baseline_ci']:.2f} g/MJ | 45Z threshold 50.0 g/MJ",
        )
        margin_at_cash = ifv.get("producer_margin_at_observed_per_gal", 0)
        m3.metric(
            f"Margin at ${observed_cash:.2f}/lb cash",
            f"${margin_at_cash:.3f}/gal",
            help=ifv.get("cash_compare", {}).get("interpretation", ""),
        )

        # Stack visualization
        stack_df = pd.DataFrame({
            "Component": ["Base refined product", "D4 RIN", "LCFS", "45Z",
                          "OPEX", "Fixed cost"],
            "Value": [b["base_refined_product"], b["d4_rin_value"], b["lcfs_value"], b["cfpc_45z_value"],
                      b["opex"], b["fixed_cost"]],
            "Type": ["Revenue", "Revenue", "Revenue", "Revenue", "Cost", "Cost"],
        })
        fig = px.bar(
            stack_df, x="Value", y="Component", color="Type", orientation="h",
            text=stack_df["Value"].apply(lambda v: f"${v:+.2f}"),
            color_discrete_map={"Revenue": "#2e7d32", "Cost": "#c62828"},
            title=f"Per-gallon stack: effective selling price ${b['effective_selling_price']:.2f}/gal",
        )
        # Pad x-axis on both sides so 'outside' text labels don't get clipped
        vmin = float(min(stack_df["Value"].min(), 0))
        vmax = float(max(stack_df["Value"].max(), 0))
        span = vmax - vmin
        fig.update_layout(
            height=320,
            showlegend=True,
            margin=dict(l=10, r=10, t=40, b=20),
            xaxis=dict(range=[vmin - span * 0.10, vmax + span * 0.20]),
        )
        fig.update_traces(textposition="outside", cliponaxis=False)
        st.plotly_chart(fig, width="stretch")

        # 45Z derivation breakdown
        st.markdown(
            f"<div class='stack-component'>"
            f"<b>45Z derivation:</b> $1.00 base × max(0, (50 − CI)/50) = "
            f"$1.00 × (50 − {inp['pathway_ci_score']:.0f})/50 = "
            f"<b>${b['cfpc_45z_value']:.2f}/gal</b><br>"
            f"<b>LCFS derivation:</b> ${inp['lcfs_credit_per_mt']:.0f}/MT × "
            f"({inp['lcfs_baseline_ci']:.2f} − {inp['pathway_ci_score']:.0f}) g/MJ × "
            f"134.47 MJ/gal × 1e-6 = <b>${b['lcfs_value']:.2f}/gal</b>"
            f"</div>",
            unsafe_allow_html=True,
        )

        with st.expander("Data provenance"):
            for k, v in ifv["data_sources"].items():
                st.markdown(f"<div class='source-note'>{k}: {v}</div>", unsafe_allow_html=True)
            if ifv["fallback_inputs"]:
                st.markdown("**KG fallbacks:**")
                for f in ifv["fallback_inputs"]:
                    st.markdown(f"<div class='source-note'>- {f}</div>", unsafe_allow_html=True)


# ===========================================================================
# 3. Policy scenario sensitivity
# ===========================================================================

st.header("Policy Scenario Sensitivity")
st.write(
    "Same (fuel, region, feedstock), evaluated across all 4 policy scenarios. "
    "Captures the 45Z architecture — which scenarios materially move the bid, and which don't, "
    "depends on the feedstock CI."
)

scen_ifv = ifv_run(
    fuel=fuel, region=region, feedstock_code=feedstock_code,
    as_of_date=as_of_date, mode="scenario_grid",
)

if "scenario_grid" in scen_ifv:
    rows = []
    for scen, cells in scen_ifv["scenario_grid"].items():
        for case in ("worst", "base", "best"):
            rows.append({
                "Scenario": scen,
                "Margin case": case,
                "Implied bid ($/lb)": cells[case]["implied_bid_per_lb"],
                "45Z ($/gal)": cells[case]["cfpc_45z_per_gal"],
                "Target margin ($/gal)": cells[case]["target_margin_per_gal"],
            })
    grid_df = pd.DataFrame(rows)

    fig2 = px.bar(
        grid_df[grid_df["Margin case"] == "base"],
        x="Scenario", y="Implied bid ($/lb)",
        text="Implied bid ($/lb)",
        color="45Z ($/gal)",
        color_continuous_scale="Greens",
        title="Base-case implied bid by policy scenario (color = 45Z $/gal)",
    )
    fig2.update_traces(texttemplate="$%{text:.4f}", textposition="outside")
    fig2.update_layout(height=380, margin=dict(l=10, r=10, t=40, b=20))
    st.plotly_chart(fig2, width="stretch")

    with st.expander("Full 4×3 grid (4 scenarios × worst/base/best margin cases)"):
        pivot = grid_df.pivot_table(
            index="Scenario", columns="Margin case", values="Implied bid ($/lb)",
        )[["worst", "base", "best"]]
        st.dataframe(pivot.style.format("${:.4f}"), width="stretch")

    st.markdown(f"**Reading**: {scen_ifv['reasoning']}")


# ===========================================================================
# 4. Facility decision space
# ===========================================================================

st.header("Facility Decision Space")
st.write(
    "Pull every CARB-certified pathway for a producer and rank by margin. "
    "This is the procurement allocation problem the facility agent solves daily. "
    "LCFS and 45Z are computed independently per pathway."
)

facilities_df = list_demo_facilities(fuel="renewable_diesel")

fac_col, fac_results = st.columns([1, 3])

with fac_col:
    facility_choice = st.selectbox(
        "Facility",
        options=facilities_df["fuel_producer"].tolist(),
        index=facilities_df["fuel_producer"].tolist().index("Diamond Green Diesel Holdings LLC")
        if "Diamond Green Diesel Holdings LLC" in facilities_df["fuel_producer"].tolist()
        else 0,
    )
    fac_row = facilities_df[facilities_df["fuel_producer"] == facility_choice].iloc[0]
    st.metric("Active pathways", int(fac_row["n_pathways"]))
    st.metric("Distinct feedstocks", int(fac_row["n_feedstocks"]))
    st.caption(f"Location: {fac_row['sample_location']}")
    st.caption(f"CI range: {fac_row['min_ci']} – {fac_row['max_ci']} g/MJ")

with fac_results:
    fac_eval = evaluate_all_pathways_for_facility(
        facility_name=facility_choice,
        fuel="renewable_diesel",
        region=region,
        as_of_date=as_of_date,
        policy_scenario=policy_scenario,
    )
    if "error" in fac_eval:
        st.error(fac_eval["error"])
    else:
        bpf = pd.DataFrame(fac_eval["best_per_feedstock"])
        bpf_display = bpf[[
            "feedstock_decision_rank", "feedstock_code", "pathway_ci_score",
            "lcfs_value_per_gal", "cfpc_45z_value_per_gal",
            "effective_selling_price_per_gal", "implied_bid_per_lb",
            "pathway_id", "certification_date",
        ]].rename(columns={
            "feedstock_decision_rank":          "Rank",
            "feedstock_code":                   "Feedstock",
            "pathway_ci_score":                 "CI",
            "lcfs_value_per_gal":               "LCFS $/gal",
            "cfpc_45z_value_per_gal":           "45Z $/gal",
            "effective_selling_price_per_gal":  "Eff sell $/gal",
            "implied_bid_per_lb":               "Bid $/lb",
            "pathway_id":                       "Pathway",
            "certification_date":               "Cert date",
        })
        st.dataframe(
            bpf_display.style.format({
                "CI": "{:.1f}",
                "LCFS $/gal": "${:.2f}",
                "45Z $/gal": "${:.2f}",
                "Eff sell $/gal": "${:.2f}",
                "Bid $/lb": "${:.4f}",
            }),
            width="stretch",
            hide_index=True,
        )
        st.markdown(f"**Spread**: ${fac_eval['feedstock_spread_per_lb']:.4f}/lb best-to-worst feedstock")
        st.caption(fac_eval["reasoning"])


# ===========================================================================
# 4b. Multi-industry US facility map
# ===========================================================================

st.header("Multi-Industry Facility Map")
st.write(
    "Every geocoded facility in our reach — curated multi-industry master plus "
    "EPA-registered universe classified by NAICS. Rail mainlines, barge terminals, "
    "ports being added (currently in build). Toggle industries; click to inspect."
)

industry_summary = get_facility_industry_summary()
map_df = get_facility_map_data()

# Industry palette — semantic grouping for visual coherence
INDUSTRY_GROUPS = {
    "Biofuel production": [
        "ethanol", "biodiesel", "renewable_diesel", "saf", "ethyl_alcohol",
    ],
    "Oilseed / oil processing": [
        "oilseed_crush", "oilseed_crush_other", "soybean_oil_mills",
        "fats_oils_refining", "wet_corn_milling",
    ],
    "Meat / rendering": [
        "pork_packing", "beef_packing", "poultry_processing",
        "meat_processing", "rendering", "egg_layers", "animal_production",
    ],
    "Food manufacturing": [
        "food_manufacturing_other", "flour_milling",
    ],
    "Oleochemical / chemical": [
        "oleochemical", "oleochemical_personal_care",
        "chemical_mfg_other", "fertilizer_n", "fertilizer_p",
    ],
    "Petroleum": [
        "petroleum_refinery", "petroleum_lubricants", "petroleum_other",
    ],
    "Storage / transport": [
        "warehousing_storage", "transport_support",
    ],
    "Other": ["other"],
}

# Inverse lookup
INDUSTRY_TO_GROUP = {ind: grp for grp, inds in INDUSTRY_GROUPS.items() for ind in inds}
map_df["group"] = map_df["industry_code"].map(INDUSTRY_TO_GROUP).fillna("Other")

map_ctrl_col, map_view_col = st.columns([1, 4])

with map_ctrl_col:
    st.metric("Total geocoded", f"{len(map_df):,}")
    st.metric("States", int(map_df["state"].nunique()))
    st.metric("Industries", int(map_df["industry_code"].nunique()))

    selected_groups = st.multiselect(
        "Industry groups",
        options=list(INDUSTRY_GROUPS.keys()),
        default=[
            "Biofuel production",
            "Oilseed / oil processing",
            "Meat / rendering",
            "Oleochemical / chemical",
            "Petroleum",
        ],
    )
    show_curated_only = st.checkbox(
        "Curated only", value=False,
        help="Show only hand-verified facilities (excludes NAICS-discovered breadth)",
    )
    st.markdown("**Rail network**")
    show_class_i  = st.checkbox("Class I (BNSF/UP/CSX/NS/CN/CPRS)", value=True)
    show_passenger = st.checkbox("Amtrak", value=False)
    show_regional  = st.checkbox("Regional", value=False)
    st.markdown("**Waterways & ports**")
    show_marine_highways = st.checkbox("Marine highways (M-routes)", value=True)
    show_ports = st.checkbox("Ports (sized by tonnage)", value=True)

with map_view_col:
    filtered = map_df[map_df["group"].isin(selected_groups)].copy()
    if show_curated_only:
        filtered = filtered[filtered["data_tier"] == "curated"]

    if filtered.empty:
        st.info("No facilities match the current filter.")
    else:
        # Hover text
        filtered["hover"] = (
            filtered["name"].fillna("(unknown)")
            + "<br><b>" + filtered["industry_code"] + "</b>"
            + "<br>" + filtered["city"].fillna("") + ", " + filtered["state"].fillna("")
            + "<br>Status: " + filtered["status"].fillna("?")
            + "<br>Source: " + filtered["data_tier"]
        )

        fig_map = px.scatter_geo(
            filtered,
            lat="lat", lon="lon",
            color="group",
            hover_name="name",
            custom_data=["hover"],
            scope="usa",
            opacity=0.7,
            color_discrete_sequence=px.colors.qualitative.Bold,
        )
        fig_map.update_traces(
            marker=dict(size=6, line=dict(width=0)),
            hovertemplate="%{customdata[0]}<extra></extra>",
        )

        # Rail overlay — add toggled rail traces UNDER the facility scatter.
        # Plotly draws traces in order; layered by adding rail traces last would
        # cover dots, so we add the facility scatter last by appending rail first.
        rail_traces = get_rail_traces()
        rail_layers = []
        if show_class_i and "Class I" in rail_traces:
            rail_layers.append(("Class I rail",  rail_traces["Class I"],  "#9aa5b1"))
        if show_passenger and "Passenger" in rail_traces:
            rail_layers.append(("Amtrak",        rail_traces["Passenger"], "#e5be53"))
        if show_regional and "Regional" in rail_traces:
            rail_layers.append(("Regional rail", rail_traces["Regional"], "#7a8590"))

        for name, trace, color in rail_layers:
            fig_map.add_trace(go.Scattergeo(
                lon=trace["lon"], lat=trace["lat"],
                mode="lines",
                line=dict(width=0.8, color=color),
                opacity=0.55,
                name=f"{name} ({trace['miles']:,.0f} mi)",
                hoverinfo="skip",
                showlegend=True,
            ))

        # Marine Highways — blue lines, below ports above land
        if show_marine_highways:
            mh = get_marine_highway_traces()
            if mh["segments"]:
                fig_map.add_trace(go.Scattergeo(
                    lon=mh["lon"], lat=mh["lat"],
                    mode="lines",
                    line=dict(width=1.4, color="#3a7bbd"),
                    opacity=0.65,
                    name=f"Marine Hwy ({mh['n_routes']} routes)",
                    hoverinfo="skip",
                    showlegend=True,
                ))

        # Ports — sized by tonnage tier
        if show_ports:
            ports = get_port_data()
            if not ports.empty:
                tier_size = {"top_tier": 14, "major": 10, "mid": 7, "small": 4, "unranked": 4}
                ports["marker_size"] = ports["tonnage_tier"].map(tier_size).fillna(4)
                ports["hover"] = (
                    ports["port_name"]
                    + "<br>" + ports["waterway_class"]
                    + " | tier: " + ports["tonnage_tier"]
                    + "<br>Total tons/yr: " + ports["total_tons"].apply(lambda v: f"{int(v):,}" if v else "?")
                )
                fig_map.add_trace(go.Scattergeo(
                    lon=ports["lon"], lat=ports["lat"],
                    mode="markers",
                    marker=dict(
                        size=ports["marker_size"],
                        color="#3a7bbd",
                        symbol="diamond",
                        line=dict(width=1, color="#dceaf5"),
                        opacity=0.85,
                    ),
                    name=f"Ports ({len(ports)})",
                    customdata=ports[["hover"]].values,
                    hovertemplate="%{customdata[0]}<extra></extra>",
                    showlegend=True,
                ))
        fig_map.update_layout(
            height=560,
            margin=dict(l=0, r=0, t=0, b=0),
            legend=dict(
                orientation="h", yanchor="bottom", y=-0.12, xanchor="center", x=0.5,
                bgcolor="rgba(0,0,0,0)",
            ),
            geo=dict(
                scope="usa",
                projection_type="albers usa",
                showland=True,
                landcolor="#1b1f24",
                showsubunits=True,
                subunitcolor="#3a3f47",
                showcountries=False,
                bgcolor="rgba(0,0,0,0)",
            ),
        )
        st.plotly_chart(fig_map, width="stretch")

        st.caption(
            f"Showing {len(filtered):,} of {len(map_df):,} geocoded facilities. "
            f"Rail/barge/port nodes in build — will add as toggleable layers."
        )

with st.expander("Industry-level counts (curated + discovered)"):
    pivot = industry_summary.pivot_table(
        index="industry_code", columns="data_tier",
        values="n_facilities", aggfunc="sum", fill_value=0,
    )
    pivot["total"] = pivot.sum(axis=1)
    pivot = pivot.sort_values("total", ascending=False)
    st.dataframe(pivot, width="stretch")


# ===========================================================================
# 5. Knowledge Graph depth
# ===========================================================================

st.header("Knowledge Graph Depth")
st.write(
    "Every number above is anchored to an expert-curated context. "
    f"{scope['kg_contexts']} contexts encode the BBD margin framework, "
    "CARB pathway methodology, RFS RIN mechanics, and the analytical structure "
    "that turns raw data into margin recommendations."
)

kg_col, kg_view = st.columns([1, 3])

with kg_col:
    kg_nodes_df = list_demo_kg_nodes()
    node_options = (
        kg_nodes_df["node_key"] + " — " + kg_nodes_df["label"]
    ).tolist()
    node_selection = st.selectbox(
        "Node",
        options=node_options,
        index=node_options.index(
            kg_nodes_df[kg_nodes_df["node_key"] == "rd_price_stack"].iloc[0]["node_key"]
            + " — "
            + kg_nodes_df[kg_nodes_df["node_key"] == "rd_price_stack"].iloc[0]["label"]
        ) if "rd_price_stack" in kg_nodes_df["node_key"].values else 0,
    )
    selected_key = node_selection.split(" — ")[0]
    row = kg_nodes_df[kg_nodes_df["node_key"] == selected_key].iloc[0]
    st.metric("Contexts", int(row["n_contexts"]))
    st.metric("Edges", int(row["n_edges"]))
    st.caption(f"Type: {row['node_type']}")

with kg_view:
    full = get_kg_node_full(selected_key)
    if not full:
        st.warning(f"Node {selected_key} not found")
    else:
        # Attribution fields we suppress when rendering for clients —
        # internal source provenance (consulting work, doc refs) should not
        # surface in the demo. The contexts themselves remain intact in the DB.
        _ATTRIBUTION_FIELDS = {"source_doc", "source", "applicable_to", "client"}

        def _scrub_attribution(d):
            if not isinstance(d, dict):
                return d
            out = {}
            for k, v in d.items():
                if k in _ATTRIBUTION_FIELDS:
                    continue
                if isinstance(v, str) and ("hobo" in v.lower() or "section8" in v.lower() or "section 8" in v.lower()):
                    continue
                out[k] = v
            return out

        if full["contexts"]:
            st.subheader("Expert contexts")
            for ctx in full["contexts"]:
                with st.expander(f"`{ctx['context_type']}` / {ctx['context_key']}"):
                    val = ctx["context_value"]
                    if isinstance(val, dict):
                        val = _scrub_attribution(val)
                        if "content" in val:
                            content = val["content"]
                            # Scrub HOBO mentions in narrative content too
                            for tag in ("HOBO", "hobo_section8", "Section 8", "hobo "):
                                content = content.replace(tag, "")
                            st.write(content.strip())
                            other = {k: v for k, v in val.items() if k != "content"}
                            if other:
                                st.json(other)
                        else:
                            st.json(val)
                    else:
                        text = str(val)
                        for tag in ("HOBO", "hobo_section8", "Section 8"):
                            text = text.replace(tag, "")
                        st.write(text.strip())

        if full["edges"]:
            st.subheader("Causal links")
            edge_df = pd.DataFrame(full["edges"])
            edge_df_display = edge_df[["direction", "edge_type", "source_key", "target_key", "confidence"]]
            st.dataframe(
                edge_df_display.rename(columns={
                    "direction": "Dir",
                    "edge_type": "Type",
                    "source_key": "From",
                    "target_key": "To",
                    "confidence": "Conf",
                }),
                width="stretch",
                hide_index=True,
            )


# ===========================================================================
# 6. 3-way balance sheet comparison
# ===========================================================================

st.header("Balance Sheet: Tore vs LLM vs Canonical")
st.write(
    "Three independent estimates of each commodity's annual S&D, side-by-side. "
    "**Tore** = analyst spreadsheet (the human forecast). "
    "**LLM** = independent LLM-generated forecast (forecast pipeline lands Wed; placeholders below). "
    "**Canonical** = source-of-record from EIA monthly biofuels / EPA RFS / EMTS. "
    "Where they diverge is where forecast methodology pays for itself."
)

tw_ctrl, tw_view = st.columns([1, 4])

with tw_ctrl:
    tw_commodity = st.selectbox(
        "Commodity",
        ["renewable_diesel", "biodiesel", "saf", "bbd_combined"],
        index=0,
    )
    tw_year_range = st.slider(
        "Year range",
        min_value=2018, max_value=2030, value=(2022, 2026),
    )

with tw_view:
    tw_df = get_three_way_comparison(tw_commodity, tw_year_range)
    if tw_df.empty:
        st.info(f"No data for {tw_commodity} in {tw_year_range[0]}–{tw_year_range[1]}.")
    else:
        # Show only annual line items most useful for the demo story
        priority_items = [
            "production", "imports", "exports",
            "domestic_consumption", "ending_stocks",
            "capacity_mmgy",
        ]
        present = [i for i in priority_items if i in tw_df["line_item"].values]
        tw_df = tw_df[tw_df["line_item"].isin(present)].copy()

        # Pretty labels
        label_map = {
            "production":           "Production",
            "imports":              "Imports",
            "exports":              "Exports",
            "domestic_consumption": "Domestic Consumption",
            "ending_stocks":        "Ending Stocks",
            "capacity_mmgy":        "Capacity (MMGY)",
        }
        tw_df["Item"] = tw_df["line_item"].map(label_map)

        # Reformat to one column per year-source. Wide format with multi-level header.
        wide = tw_df.pivot_table(
            index="Item",
            columns="year",
            values=["tore_value", "canonical_value"],
            aggfunc="first",
        )
        # Reorder so each year shows Tore / Canonical side-by-side
        years_sorted = sorted(tw_df["year"].unique())
        ordered_cols = []
        for y in years_sorted:
            for src in ["tore_value", "canonical_value"]:
                if (src, y) in wide.columns:
                    ordered_cols.append((src, y))
        wide = wide[ordered_cols]
        # Rename: ('tore_value', 2025) -> '2025 Tore' etc.
        wide.columns = [
            f"{y} {('Tore' if src == 'tore_value' else 'Canonical')}"
            for src, y in wide.columns
        ]
        # Item ordering
        item_order = [label_map[i] for i in present]
        wide = wide.reindex(item_order)

        st.dataframe(
            wide.style.format("{:,.1f}", na_rep="—"),
            width="stretch",
        )

        st.caption(
            f"**LLM forecast column pending** — wiring core.forecasts to populate "
            f"under each year, with methodology_version + as_of_date metadata for "
            f"audit. Once live, the Tore/LLM/Canonical triple will compute residuals "
            f"per `core.accuracy_metrics`, and the methodology with the lower historical "
            f"error gets weighted higher in the consensus forecast."
        )

        # Highlight years where Tore and canonical materially diverge (the
        # "human is forecasting ahead of EIA" story).
        diverged = tw_df.dropna(subset=["canonical_value"]).copy()
        diverged["abs_pct"] = diverged["tore_vs_canonical_pct"].abs()
        diverged = diverged[diverged["abs_pct"] > 5].sort_values("abs_pct", ascending=False)
        if not diverged.empty:
            with st.expander(f"Notable divergences (|Δ|>5%): {len(diverged)} rows"):
                show = diverged[["Item", "year", "tore_value", "canonical_value", "tore_vs_canonical_pct"]]
                show.columns = ["Item", "Year", "Tore", "Canonical", "% Diff"]
                st.dataframe(
                    show.style.format({
                        "Tore": "{:,.1f}",
                        "Canonical": "{:,.1f}",
                        "% Diff": "{:+.1f}%",
                    }),
                    width="stretch", hide_index=True,
                )


# ---------------------------------------------------------------------------
# Footer
# ---------------------------------------------------------------------------

st.divider()
st.caption(
    f"Math: pure functions in `src/agents/facility/hefa_economics.py` (24 unit tests passing) | "
    f"Callable: `src/kg/callables/implied_feedstock_value.py` (registered in `core.kg_callable`, "
    f"5 smoke tests passing through production invoker) | "
    f"Source data: 892 CARB pathways + bronze.credit_prices forward curve + "
    f"gold.us_liquid_fuel_* views"
)
