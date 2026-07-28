# Does a Composite Climate-Risk Index Add Skill Over Traditional Weather in Explaining Crop-Yield Anomalies?
## A factor- and phase-resolved test of Helios WAPR — study design v1

*Draft 2026-07-28. Grounded in the live RLC database + the characterized `bronze.helios_climate_risk`.
Written to be executed in Aegus next session (data → model → tables → draft). Data claims below are
verified against `information_schema` except where marked **[confirm]**.*

---

## 1. Question and contribution

Helios sells **WAPR** (Weighted Average Percent Risk) as a single climate-risk signal, built from four
dimensions — *too hot, too cold, too wet, too dry* — phenology-weighted and benchmarked to local
climatology. RLC's traditional edge is a weather→yield read from raw observations (rainfall, temperature,
soil moisture, stress days). The paper asks, **factor by factor and stage by stage:**

1. **(H1 — overall)** Does WAPR, resolved into its risk factors, explain **departures of realized yield
   from trend** better than a traditional weather feature set — overall, out of sample?
2. **(H2 — conditional)** Does one win **in specific growth stages for specific factors** (e.g.
   `too_dry` during reproductive/grain-fill vs. raw precip in that window)?
3. **(H3 — structural vs. informational)** Where a gap exists, **is it structural or informational?**
   WAPR bakes in four structural transforms a raw-weather model usually lacks — phenology weighting,
   nonlinear/threshold risk mapping, climatology-anomaly normalization, and spatial aggregation. Give
   the traditional model those same transforms one at a time and measure how much of the gap each
   closes. **The residual after equalizing structure is WAPR's genuine informational content** — "the
   difference without the structural differences."

Contribution: a clean, reproducible decomposition of *why* a commercial climate-risk index does or
doesn't beat first-principles weather, rather than a black-box horse race.

---

## 2. Target variable — yield departure from trend

The truth we predict is the **detrended yield anomaly**, not the level:

```
anomaly[commodity, region, my] = realized_yield − trend_yield(my)
```

- **US, state resolution:** `bronze.nass_state_yields.yield_per_acre` (3,674 rows: commodity × state ×
  year) minus the pre-fitted trend in `silver.yield_trend` (282 rows: intercept/slope/slope_quadratic
  per commodity × state, with `trend_type`, `r_squared`). Anomaly is a subtraction — no refitting.
- **Country resolution (global):** `gold.psd_wasde_vintages.yield` (final active-MY vintage) minus a
  trend fit over its own history; PSD covers every WAPR country.

Two target *framings*, both using this anomaly:
- **T1 — end-of-season:** final anomaly vs. full-season predictors. N = region-seasons.
- **T2 — in-season, as-of month M:** the *same* final anomaly, predicted from information available at
  each USDA report month M. This is where H2 (stage conditioning) and the "beat-USDA" test live, and it
  multiplies N by the number of in-season months.

---

## 3. The three competing information sets

All aligned to the **same as-of date and same spatial support** before any comparison — otherwise we'd
be testing feature engineering or resolution, not information.

**A — WAPR, resolved by factor** (`bronze.helios_climate_risk`, actuals only, `is_forecasted=false`):
`too_hot_wapr`, `too_cold_wapr`, `too_wet_wapr`, `too_dry_wapr`, and the `wapr` composite; each
accumulated within phase windows using the table's `phase` field; `wapr_hist_avg` gives the
already-normalized current-vs-normal read. Country/daily, 2021-07 → present.

**B — traditional weather** (`silver.yield_features`, 31,320 rows, state × weekly, already engineered):
`gdd_cum`, `precip_cum_mm`, `stress_days_heat`, `stress_days_drought`, `excess_moisture_days`,
`tmax_weekly_avg`, `tmin_weekly_avg`, `ndvi_anomaly`, `condition_index`, plus `growth_stage`/progress
fields for phase alignment. Raw supplements from `silver.weather_observation`: `soil_moisture_0_7cm`
(shallow), `soil_temp_0_7cm_c`, `evapotranspiration_mm`, humidity → VPD.
  - **Gap to fill:** *deep* soil moisture is **not** in the DB (only 0–7 cm). Tore wants shallow **and**
    deep. Deep-layer soil moisture needs a reanalysis pull (ERA5-Land has 0–7/7–28/28–100/100–289 cm).
    Treat deep soil moisture as an **added input to source**, not an existing column. **[confirm]**

**C — USDA's own monthly projection** (`gold.psd_wasde_vintages`, 54,166 rows): yield by `vintage` /
`report_date` per marketing year. This is not a rival feature set so much as **the baseline to beat** —
at month M, the WASDE yield-vs-trend is what the market already prices in. The sharp version of H1
becomes *incremental skill*: does A (or B) add explained variance **beyond** USDA's month-M projection?
This sidesteps the circularity that USDA already ingests weather.

---

## 4. Panel construction and the resolution problem

**Unit of analysis:** `commodity × region × marketing_year × as-of-month`.

**The central confound:** WAPR is **national only**; the traditional panel is **US-state weekly**. If
traditional wins, part of the edge could be pure spatial resolution, not better signal. Handling:
- **Primary head-to-head at matched support:** crop-area-weight the state features up to **national**
  (US) so A and B share the same geography. National crop weights from
  `bronze.nass_state_yields.area_harvested`.
- **Resolution arm (feeds H3):** keep the state-level traditional model as a separate arm; the
  state−national skill difference *is* the "spatial resolution" structural component.

**Temporal alignment:** accumulate A and B to each as-of month M (season-to-date and phase-window
aggregates); WASDE vintage at M for C. WAPR actuals only.

**Window:** the WAPR history binds everything to **~5 seasons (2021–2025/26)**. Power comes from
**pooling** — across US states (H2 within-US) and across PSD countries (global panel). See §8 limits;
this is the study's binding constraint and must be stated up front.

---

## 5. Estimation and skill metrics

- **Same model class on every information set** — a regularized linear model (elastic net) as the
  interpretable primary, plus gradient-boosted trees as a nonlinear check. Fairness requires identical
  CV folds and tuning budget across A, B, C, and their unions.
- **Out-of-sample by construction:** leave-one-season-out **and** leave-one-region-out CV (the two ways
  the model must generalize). Report both — LOSO tests temporal transfer, LORO spatial transfer.
- **Metrics:** OOS R² and RMSE of the anomaly, expressed as **skill scores** against two baselines:
  (i) trend-only (anomaly = 0), (ii) USDA month-M projection (set C). A signal earns its keep only if it
  beats *both*.
- **Inference under small N:** block bootstrap by season/region for CIs; treat single-season effects as
  descriptive, not significant.

---

## 6. H2 — growth-stage × factor analysis

Map both sides to a common phenology axis (`phase` in A; `growth_stage`/progress in B) collapsed to:
*Planting/Emergence · Vegetative · Reproductive · Grain-Fill · Maturity/Harvest*. For each
**phase × factor** cell, compute the marginal OOS skill of the WAPR factor vs. its traditional analog:

| WAPR factor | traditional analog | prior expectation |
|---|---|---|
| `too_dry` | precip deficit, `stress_days_drought`, soil-moisture anomaly | matters most Reproductive→Grain-Fill |
| `too_hot` | `stress_days_heat`, tmax, VPD | Reproductive (pollination) |
| `too_wet` | `excess_moisture_days`, precip surplus | Planting + Harvest |
| `too_cold` | frost/tmin | Emergence + Maturity |

Deliverable: a **phase × factor skill heatmap** showing where each signal wins. This is the paper's most
useful practical output — it says *when to trust WAPR and when to trust the gauge*.

---

## 7. H3 — the structural-vs-informational decomposition (the core)

An **ablation ladder**: start from raw traditional weather and add, one at a time, each structural
transform WAPR already embeds; record how much of the A−B gap each rung closes.

```
Rung 0  raw traditional weather (level features)                → skill S0
Rung 1  + phenology weighting (phase-window aggregation)        → S1
Rung 2  + nonlinear/threshold risk transform (soft-max stress)  → S2
Rung 3  + climatology-anomaly normalization (vs local normal)   → S3
Rung 4  + national crop-weighted aggregation (match WAPR support)→ S4
        WAPR skill                                              → S_A
```

- **Structural component** of the gap = `S4 − S0` (what re-engineering B WAPR-style buys you).
- **Informational residual** = `S_A − S4` (WAPR's genuine private signal, after B has every structural
  advantage). This is the answer to *"what the difference would be without the structural differences."*
- **Sustainability (structural persistence):** re-run the ladder per season and per commodity; a
  *structural* difference is stable in sign and magnitude across resamples, an *incidental* one is not.
  Report rolling estimates + bootstrap CIs on `S_A − S4`.

If the residual `S_A − S4 ≈ 0`, WAPR is a convenience wrapper over weather RLC can reproduce; if it's
positive and stable, Helios carries information beyond first-principles weather (candidate sources:
proprietary satellite soil-moisture, sub-daily extremes, a better climatology). Either finding is
publishable and directly informs the Pepsi build vs. buy decision.

---

## 8. Commodities, sequence, and scope limits

**Order:** soybeans first (best-provisioned: state yields, trend, `yield_features`, WAPR `soya_beans`
11 countries) → corn (`corn_commodity_tracked`, 12 countries) → wheat + durum.

**Wheat caveat (hard):** WAPR carries `wheat` and `durum_wheat` but **does not split spring/winter**.
RLC yield data resolves HRW/SRW/HRS/durum. So "various wheat varieties" is a **one-to-many mismatch on
the WAPR side** — a variety-resolved test can only compare RLC's variety yields against a single common-
wheat WAPR series (durum separate). State-of-cultivation weighting partly recovers this; note it as a
resolution limit, don't paper over it.

**The binding limitation — say it first, not last:** WAPR is ~5 seasons of country-level daily data.
An annual national yield-anomaly regression is N≈5 per country — powerless alone. The design survives
only by (a) pooling across states and countries, and (b) using the T2 in-season monthly panel for
frequency. **This is an exploratory / proof-of-concept study; treat every coefficient as provisional and
lead with the sample constraint.** Other limits: USDA-projection endogeneity (handled via incremental-
skill framing, §3C), deep-soil-moisture sourcing (§3B), and PSD country yields being USDA estimates, not
ground truth.

---

## 9. Aegus execution plan (next session)

1. **Data** — build the panel in RLC's DB first (a single `commodity × region × my × month` table
   joining anomaly, the three information sets, phase), then upload as an Aegus source. **Reference the
   source by NAME in the model stage, not the upload UUID** (the gotcha that cost the last run —
   `model_replace_data_source` from=UUID→name if it bites).
2. **Model** — one `feols`/GBT per information set + `model_sweep` to run the §7 ablation ladder as a
   grid; leave-one-out CV folds.
3. **Tables** — model-bound skill table (A vs B vs C vs unions), the §6 phase×factor heatmap, the §7
   decomposition table (S0…S4, S_A).
4. **Draft** — sections: Question · Data & Alignment · Overall skill (H1) · Stage-conditional (H2) ·
   Structural decomposition (H3) · Limitations. Flag the single-support and 5-season caveats in the
   Methodology section honestly, as the last thin-slice draft did.
5. Real PDFs for the literature stage must be **drag-dropped in the Aegus web UI** — base64 attach from
   an agent is un-emittable above tens of KB.

---

*Open data tasks to resolve at session start (verify, don't assume): deep soil-moisture source
(ERA5-Land); whether `silver.yield_trend` covers corn + wheat classes at needed states; exact join keys
between `yield_features` (state/week) and WAPR (country/day) after national aggregation; and the WASDE
vintage `report_date` cadence in `psd_wasde_vintages` for the T2 monthly panel.*
