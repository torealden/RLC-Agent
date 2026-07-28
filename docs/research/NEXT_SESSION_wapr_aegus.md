# Next-session prompt — WAPR yield-skill study, Aegus hookup

Paste the block below into a fresh Claude Code session (`/clear` first). It kicks off execution of the
study designed in `docs/research/wapr_yield_skill_study_v1.md`.

---

```
Execute the WAPR vs traditional-weather yield-skill study in Aegus. Read
docs/research/wapr_yield_skill_study_v1.md FIRST (the full design), then the memories
reference_aegus_mcp_integration and reference_wapr_helios_index. Start with SOYBEANS.

Aegus is connected via MCP (OAuth). Project id: pid_wng5oup58gj7r0mdl8unmy9w (soybean-yield-
from-weather paper). Gotchas that already cost time — honor them:
- The model stage references data sources by NAME, not the upload UUID. If a run fails on
  source id, use model_replace_data_source (from=UUID -> to=name).
- Real PDFs for the literature stage must be drag-dropped in the Aegus WEB UI (base64 attach
  from an agent is un-emittable above tens of KB). Don't try to attach PDFs via MCP.
- There is NO list-projects tool and the Explore stage is not on MCP.

Build order:
1. FIRST build the analysis panel in RLC's Postgres (one table: commodity x region x
   marketing_year x as-of-month), joining:
     - target: detrended yield anomaly = bronze.nass_state_yields.yield_per_acre minus the
       fitted trend in silver.yield_trend  (US state). Country arm: gold.psd_wasde_vintages
       final yield minus its own trend.
     - WAPR (set A): bronze.helios_climate_risk, actuals ONLY (is_forecasted=false), factors
       too_hot/too_cold/too_wet/too_dry_wapr + wapr + wapr_hist_avg, accumulated by phase.
       Country/daily; country codes are lowercase 2-letter ISO (us, br).
     - traditional (set B): silver.yield_features (state x weekly: gdd_cum, precip_cum_mm,
       stress_days_heat/drought, excess_moisture_days, tmax/tmin, ndvi_anomaly, condition_index,
       growth_stage) + silver.weather_observation (soil_moisture_0_7cm, evapotranspiration_mm,
       humidity->VPD). Crop-area-weight state->national to match WAPR's support.
     - USDA baseline (set C): gold.psd_wasde_vintages, yield by vintage/report_date per MY.
   Then upload the panel as an Aegus source.
2. Model per information set (feols primary + GBT check), leave-one-season-out AND
   leave-one-region-out CV. Skill vs BOTH baselines: trend-only and USDA month-M projection.
3. Run the H3 structural-vs-informational ABLATION LADDER via model_sweep (rungs S0..S4 -> S_A;
   report structural = S4-S0, informational residual = S_A-S4).
4. Tables: A/B/C/union skill table; phase x factor skill heatmap (H2); decomposition table (H3).
5. Draft sections: Question; Data & Alignment; Overall skill (H1); Stage-conditional (H2);
   Structural decomposition (H3); Limitations.

RESOLVE THESE AT START (verify, do not assume):
- Deep soil moisture is NOT in the DB (only 0-7cm). Tore wants shallow AND deep -> source
  ERA5-Land layers (0-7/7-28/28-100/100-289cm) or proceed shallow-only and flag it.
- Confirm silver.yield_trend covers corn + wheat classes at the needed states (soy is covered).
- Confirm the join keys between yield_features (state/week) and WAPR (country/day) after
  national aggregation, and the WASDE vintage report_date cadence for the T2 monthly panel.

Lead with the honest constraint in the draft, don't bury it: WAPR is ~5 seasons, country-level
only -> N-bound, exploratory. Power comes from pooling across states/countries + the in-season
monthly (T2) panel. Every coefficient is provisional. Sequence: soybeans -> corn -> wheat/durum
(WAPR wheat is NOT spring/winter split -> resolution mismatch, note it).
```

---

*Saved 2026-07-28 alongside the study design. If the design doc changes, refresh this prompt.*
