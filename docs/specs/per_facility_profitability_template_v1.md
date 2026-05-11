# Per-Facility Profitability Template v1 — Design Spec

**Built from**: `D:\Switch Over\Biomass-Based Diesel\Plant Model Project\` (4 workbooks, Nov 2022 vintage). The template is a structural distillation — it generalizes the Braya per-facility analysis so we can clone-and-populate one workbook per facility in `reference.oilseed_crush_facilities`.

**Why it matters**: every other piece of the project so far has been a regional or aggregate model. This is the asset that makes RLC's analysis sellable to stock analysts: one professionally-laid-out workbook per real plant, with auditable inputs and outputs, refreshed monthly.

---

## 1. Source workbook architecture (what we're distilling)

The Long-Term Plant Profitability model has **regional** profit-per-gallon outputs computed from these inputs:

| Sheet | Role |
|---|---|
| `Fuel Values` | RD selling price + per-feedstock revenue per gallon ($/gal) |
| `Feedstock Forecast` | Feedstock prices in $/lb (monthly) + cost-per-gallon by region |
| `Freight Forecast` | Origin→facility and facility→destination freight ($/lb) |
| `LCFS Credit Values` | LCFS $/gal contribution by feedstock + average CI scores |
| `Other Credit Values` | RIN/45Z contributions |
| `Feedstock Conversions` | lb-per-gallon yield (RD vs BD differ — soy 7.5 BD vs 7.5 RD; tallow 9.38 RD vs 7.75 BD) |
| `[Region] Profits by Commodity` | Output: profit-per-gallon by (region, feedstock, month) |

The Braya workbook adds the per-facility wrinkle:
- Facility location → freight from origin (Central Illinois) and to destination (West Coast)
- Facility-specific feedstock mix
- Revenue per Gallon broken down by (RD price + LCFS + D4 RIN + BTC + Cap&Trade)

---

## 2. Per-Facility Template — Sheet design (10 tabs)

### Tab 1: `Identity`
Facility metadata pulled from `reference.oilseed_crush_facilities`.

| Field | Source |
|---|---|
| facility_id, name, operator, parent_company | reference table |
| city, county, state, zip, lat, lon | reference + geocode |
| Title V / EPA FRS / EIA Plant ID | reference table |
| Status, year_built, last_expansion | reference table |
| Capacity bpd / MMbu/yr | reference table |
| Refining capability + capacity | reference table |
| Co-located biofuel? MGY? | reference table |
| Process type, draw_radius | reference table |
| Last updated | timestamp at populate |

### Tab 2: `Inputs - Static`
Per-facility constants (some are KG defaults, some facility-specific overrides):

| Field | Default | Override basis |
|---|---|---|
| Feedstock yields (lb/gal by RD vs BD) | KG `Feedstock Conversions` table | Plant tech specs |
| Average CI scores (g CO2/MJ) by feedstock | CARB pathway DB / KG `ci_value_framework` | LCFS pathway certification |
| Variable OPEX ($/gal) | KG `hefa_opex_structure` (HOBO base) | Plant-specific actuals when known |
| Fixed OPEX + depreciation per gal | KG `crushing_plant_opex_structure` defaults | Plant capex/lifetime |
| Equivalence values (D4 RIN per gal) | RD=1.7, BD=1.5, SAF=1.7 | n/a |
| Region (Gulf/Midwest/WCB/etc.) | derived from state | n/a |
| Annual operating days | 350 | maintenance schedule |

### Tab 3: `Inputs - Time Series`
Monthly time series, 2020-01 through current + forecast horizon. **This is the live-data tab**.

Columns:
- Period (1st of month)
- Feedstock cost ($/lb) — one column per feedstock
- Fuel selling price ($/gal) — RD, BD, SAF, ULSD
- D4 RIN price ($/RIN)
- LCFS credit price ($/MT CO2)
- 45Z value per gallon (CI-dependent — calculated)
- Freight in ($/lb feedstock to facility)
- Freight out ($/gal fuel to destination)

**Sources** (auto-populated via the IFV kg_callable resolver):
- silver.cash_price (when available)
- gold.bbd_sd_watch (feedstock supply context)
- core.forecasts_historical (forward curves)
- silver.rfs_volume_projections (mandate trajectory)
- core.kg_context for missing values (HOBO calibration)

### Tab 4: `Revenue Build`
Per-feedstock per-month revenue stack ($/gal):

```
Total Revenue/gal = Base Fuel Price + LCFS Value + D4 RIN Value + 45Z Value + Other Credits
```

Where:
- LCFS Value = LCFS_$/MT × pathway_CI_advantage_g/MJ × conversion factor
- D4 RIN Value = D4_RIN_$/gal × equivalence_ratio (1.7 for RD)
- 45Z = lookup based on CI score and policy_scenario

### Tab 5: `Cost Build`
Per-feedstock per-month cost stack ($/gal):

```
Total Cost/gal = Feedstock cost + Freight In + Freight Out + Variable OPEX + Fixed OPEX + Depreciation
where Feedstock cost = feedstock_$/lb × yield_lb_per_gal
```

### Tab 6: `Profit by Feedstock`
Output: $/gal margin by (feedstock, month). Revenue − Cost. **This mirrors the source workbook's "[Region] Profits by Commodity" tab.**

Color-coded heatmap when populated (green > 0.50/gal, yellow 0–0.50, red < 0).

### Tab 7: `Operating Model`
Monthly operating model:
- Throughput (gallons/month) = capacity × utilization × operating_days_in_month
- Feedstock mix split (e.g., 60% SBO + 30% UCO + 10% tallow)
- Weighted avg margin per gallon = Σ(mix_share × margin_per_gal)
- Monthly EBITDA = throughput × weighted_margin
- Annual roll-up

### Tab 8: `Returns Summary`
- Annual EBITDA (10-year forecast)
- Cumulative cash flow
- IRR (unlevered + levered if debt assumed)
- NPV at 8/10/12% discount
- Breakeven feedstock cost ($/lb at zero margin)
- Payback period

### Tab 9: `Sensitivity`
Tornado chart inputs — single-variable sensitivity:
- D4 RIN ±20% → ΔIRR
- LCFS credit ±20% → ΔIRR
- 45Z scenario (extension_2031 / expiry_2027 / iluc_removed / domestic_restriction)
- Feedstock cost ±10% → ΔIRR
- Throughput ±10%

### Tab 10: `Notes & Provenance`
- Source workbook (template) version
- Populated_at timestamp + Git commit hash
- Data sources used (DB tables/views) with their refresh timestamps
- Manual override log (any cell where user edited the auto-populated value)
- Pending issues / verification flags

---

## 3. Clone-and-populate procedure

```
scripts/build_per_facility_workbook.py --facility_id ia.cargill_iowa_falls
  ├─ Load template from models/templates/per_facility_profitability_v1.xlsx
  ├─ Load facility row from reference.oilseed_crush_facilities
  ├─ Populate Identity tab
  ├─ Populate Inputs-Static tab (KG defaults + facility overrides)
  ├─ Populate Inputs-Time Series tab from DB views (feedstock prices, RIN, LCFS, etc.)
  ├─ Recalc Revenue/Cost/Profit tabs (formula-driven, just opens-saves)
  ├─ Save to models/per_facility/{facility_id}.xlsx
  └─ UPDATE reference.oilseed_crush_facilities SET crush_model_xlsx_path = ...
```

Idempotent. Re-run any time inputs change. Manual overrides preserved via a special `Notes & Provenance` log.

---

## 4. Implementation phases

| Phase | What | Status |
|---|---|---|
| **2A** | Build template skeleton (10 tabs, structure, named ranges) | this session |
| **2B** | Populate one example facility (`ia.cargill_iowa_falls`) end-to-end | this session |
| **2C** | Wire IFV kg_callable to read facility XLSX as source for cost defaults | next session |
| **2D** | Generate all 20 IA facility workbooks; spot-check | next session |
| **2E** | National rollout (98 ops facilities) once IA validated | post-validation |

---

## 5. Open design questions (TBD with user review)

1. **Workbook format**: xlsx (formula-driven) vs xlsm (with VBA refresh button)? v1 = xlsx; the populator is the refresh mechanism.
2. **Forecast horizon**: 5-year vs 10-year vs 30-year (matching the KG `oilseed_crushing_plant_model` template). v1 = 10 years.
3. **Multi-feedstock plants**: the current Braya structure shows margin by single-feedstock-channel. For plants that blend (most BBD plants do), we add a `Feedstock Mix` column on Tab 7 to weight the margins. v1 = supports both.
4. **Refining vs raw oil output for crushers**: oilseed crushers produce CRUDE oil typically; some refine. Tab 4 needs separate price columns for crude vs RBD. v1 = both columns, blank where N/A.
5. **Distribution rights / sellable IP**: should the template include a watermark / footer that identifies the facility-specific version + RLC as analyst? Reasonable for the stock-analyst sales channel.

---

*v1 design Apr 2026. Iterate after Tore review of one populated example.*
