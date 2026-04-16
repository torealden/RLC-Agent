# KG Gap-Fill Strategy

*63 nodes in `core.kg_node` still have zero contexts AND zero edges. This doc classifies each by what it needs, assigns a priority, and gives a concrete next action.*

---

## 1. Classification scheme

Every orphan gets one tag that dictates how it should be filled:

| Tag | Meaning | Tool |
|---|---|---|
| `AUTO_COMPUTE` | Can be derived from data already in our DB (seasonal norms, percentiles, coverage summaries, pace vs. USDA). | Extend `src/knowledge_graph/seasonal_calculator.py` and `pace_calculator.py`; seeder script. |
| `DOC_PENDING` | Needs external source material we can process — expected to be addressed by the Google Drive ingest once `.gdoc` files are accessible. | Wait on doc auth; then extract. |
| `ECONOMETRIC` | Needs a quantitative model — calibrated from historical data, fitted coefficients. Will become a `kg_callable`. | Write a model, seed contexts from model output. |
| `LIGHT_TOUCH` | Can be filled from a single short prose context written by hand (e.g., calendar definition, policy description). | Manual context insert. |
| `DELETE` | Placeholder that was created without content and doesn't earn its place — remove rather than fill. | `UPDATE kg_node ... RETIRED` or DELETE. |
| `LOW_VALUE` | Exists, useful to keep, but not on the forecasting critical path — defer indefinitely. | Leave empty for now. |

---

## 2. Priority tiers

Priority is set by whether the node is on the critical path for producing any of the monthly forecasts the LLM is supposed to generate against spreadsheets.

- **P0** — blocks a forecast or an active callable
- **P1** — directly informs reasoning about a monthly series we forecast
- **P2** — useful context but not load-bearing
- **P3** — nice to have; stub for future work

---

## 3. Orphan inventory — classification + actions

### data_series (17 orphan)

| node_key | Tag | Priority | Action |
|---|---|---|---|
| `usda.grain_stocks` | LIGHT_TOUCH | **P0** | Hand-write cadence (quarterly Mar/Jun/Sep/Jan, 12:00 ET) + interpretive rule (Sep stocks - Jun stocks + Jul-Aug usage = residual). This is the quarterly_residual_model input — must be filled. |
| `usda.grain_stocks.feed_residual` | ECONOMETRIC | **P0** | Residual demand = `total_supply − (crush + ethanol + FSI + exports + ending_stocks)`. Same quarterly_residual_model. |
| `usda.flash_sales` | LIGHT_TOUCH | **P1** | Daily 9:00 AM ET release; 100k+ MT threshold for flash reporting. |
| `epa.emts` | AUTO_COMPUTE | **P1** | Have `bronze.epa_rfs_rin_transaction`. Seed coverage + monthly RIN generation stats. |
| `eia.biodiesel_feedstock` | LIGHT_TOUCH | **P1** | Monthly EIA Form 819 report, ~30-day lag; tracks feedstock input to BBD production. Drives EIA guardrail in allocation engine. |
| `eia.rd_feedstock` | LIGHT_TOUCH | **P1** | Same release as biodiesel_feedstock; distinct consumer — track separately. |
| `brazil.imea` | AUTO_COMPUTE | **P1** | We have `bronze.imea_mato_grosso`. Compute planting/harvest pace norm. |
| `brazil.stu` | ECONOMETRIC | P2 | Brazil soybean stocks-to-use derived from `silver.conab_balance_sheet`. Seasonal norms. |
| `nass.fats_oils.canola_crush` | AUTO_COMPUTE | **P1** | Canola crush data in `bronze.usda_nass`. Compute monthly series + YoY. |
| `nass.fats_oils.canola_oil_stocks` | AUTO_COMPUTE | **P1** | Same NASS Fats & Oils report. |
| `us.canola_oil.domestic_use` | LIGHT_TOUCH | **P1** | Derived from balance sheet identity. |
| `us.canola_oil.ending_stocks` | AUTO_COMPUTE | **P1** | Balance sheet stocks line; can compute seasonal norm. |
| `stats_can.prospective_plantings` | LIGHT_TOUCH | P2 | Annual canola planting intentions (late April); cadence + interpretive rule. |
| `yg_relative_price_corn` | ECONOMETRIC | P2 | YG/corn ratio as feedstock pull indicator. Compute historical mean/band. |
| `yg_relative_price_sbo` | ECONOMETRIC | P2 | YG/SBO ratio — when YG trades at discount to SBO, animal fats win BBD allocation. |
| `sre.compliance_deficit` | DOC_PENDING | P2 | EPA RFS Small Refinery Exemption exposure — needs policy doc. |
| `baltic_dry_index` | LIGHT_TOUCH | P3 | Macro shipping indicator. Single context describing series + typical role. |

### seasonal_event (12 orphan)

These are all high-value and deserve short hand-written contexts. Essentially calendar anchors the LLM needs.

| node_key | Tag | Priority | Action |
|---|---|---|---|
| `corn_pollination_window` | LIGHT_TOUCH | **P0** | July; already referenced by `weather_adjusted_yield` callable. Must fill — rule: "peak pollination third week of July Corn Belt; extreme heat/drought during this window causes the biggest yield hits". |
| `soybean_pod_fill_aug` | LIGHT_TOUCH | **P0** | Aug/early Sep; soy yield critical window. Same structure. |
| `usda_june30_reports` / `usda_june30_stocks` | LIGHT_TOUCH | **P0** | Most volatile trading day of year. Rule + historical volatility. |
| `brazil_safrinha_window` | LIGHT_TOUCH | **P1** | Second-crop corn planting Jan-Mar; late finish implies lower yield. |
| `canola_planting_na` / `canola_growing_na` / `canola_harvest_na` | LIGHT_TOUCH | **P1** | Canada canola calendar. |
| `wheat_winter_kill` | LIGHT_TOUCH | P2 | Dec-Feb; freeze + no snow cover. |
| `summer_uco_peak` | LIGHT_TOUCH | P2 | UCO collection seasonal pattern. |
| `winter_fat_demand` | LIGHT_TOUCH | P2 | Feed ration fat inclusion rises in winter. |
| `la_nina_cycle` | LIGHT_TOUCH | P2 | ENSO cycle; Brazil production correlation. Same structural context as `el_nino_2015_16` already has. |

### commodity (4 orphan)

| node_key | Tag | Priority | Action |
|---|---|---|---|
| `tallow_feedstock` | MERGE | **P0** | Likely alias of `bleachable_fancy_tallow` (which has 3 contexts). Consolidate; either delete tallow_feedstock or add an alias context. |
| `d6_rin` | LIGHT_TOUCH | **P1** | Conventional ethanol RIN, $/gal price series. Short rule: D6 trades at discount to D4 since 2019; spread reflects blending economics. |
| `heating_oil` | LIGHT_TOUCH | P2 | NYH ULSD proxy; RD price anchor. |
| `camelina_oil` | DOC_PENDING | P3 | Novel feedstock; placeholder context until relevant doc arrives. |

### metric (4 orphan)

| node_key | Tag | Priority | Action |
|---|---|---|---|
| `palm_oil_oer` | LIGHT_TOUCH | **P1** | Created during MPOB ingest but never got its own contexts — I'll close this with a 1-paragraph definition. |
| `hefa_capex_benchmark` | DOC_PENDING | P2 | HOBO docs have this data; should have been captured. Secondary pass on HOBO. |
| `global_saf_offtake` | DOC_PENDING | P3 | Depends on external tracker (IATA, IEA). |
| `best_worst_margins` | DOC_PENDING | P3 | HOBO-specific; secondary pass on HOBO section 8. |

### policy (5 orphan)

| node_key | Tag | Priority | Action |
|---|---|---|---|
| `rfs2` | LIGHT_TOUCH | **P0** | Core policy — one paragraph on RVO mandate mechanics. CRITICAL gap given this is foundational. |
| `argentina_export_tax` | LIGHT_TOUCH | **P1** | Differential export tax (DET) disincentivizes crush; change in DET shifts Argentine crush volume. |
| `phase1_trade_deal` | LIGHT_TOUCH | P2 | US-China 2020 agreement; annual purchase commitments. |
| `state_rd_incentives` | DOC_PENDING | P2 | Covered by HOBO section 2 but not fully extracted. |
| `saf_grand_challenge` | LIGHT_TOUCH | P2 | US SAF target = 3bn gal by 2030, 35bn gal by 2050. |

### region (6 orphan)

| node_key | Tag | Priority | Action |
|---|---|---|---|
| `us.corn_belt` | LIGHT_TOUCH | **P0** | Definition + states + production share; used throughout callables. |
| `argentina.pampas` | LIGHT_TOUCH | **P1** | Regional bounding + commodity mix. |
| `brazil.south` | LIGHT_TOUCH | **P1** | PR/RS; alternate to Mato Grosso for soy + wheat. |
| `brazil.goias` | LIGHT_TOUCH | P2 | Second-tier soy; link to `goiania_go` weather location. |
| `us.pnw` | LIGHT_TOUCH | P2 | Wheat export corridor (white wheat, spring wheat). |
| `black_sea` | LIGHT_TOUCH | **P1** | Russia + Ukraine + Romania corridor; global wheat/corn swing producer. |

### report (3 orphan)

| node_key | Tag | Priority | Action |
|---|---|---|---|
| `nopa.monthly` | LIGHT_TOUCH | **P0** | Monthly NOPA crush release; monthly soybean crush = core silver series. Foundational. |
| `usda.grain_stocks_report` | LIGHT_TOUCH | **P0** | Feeds `usda.grain_stocks` data_series; quarterly Mar/Jun/Sep/Jan. |
| `eia.monthly_biofuels` | LIGHT_TOUCH | **P1** | EIA Form 819 capacity + feedstock. |

### analytical_model (2 orphan)

| node_key | Tag | Priority | Action |
|---|---|---|---|
| `outlook_forum_adjustment_model` | ECONOMETRIC | P2 | Ag Outlook Forum → May WASDE adjustment pattern. Model from historical data. |
| `derecho_impact_model` | DOC_PENDING | P3 | Extreme weather event framework — `special_situations/2020_derecho.md` exists; ingest that. |

### market_participant (7 orphan)

All LIGHT_TOUCH, P2:
- `argentine_crushers`, `brazil.crushers`, `china_crushers`, `uco_collectors`, `copa`, `food_manufacturers`, `commercial_hedgers`. Each gets a one-paragraph description of role + typical volumes + incentive structure.

### price_level (2 orphan)

| node_key | Tag | Priority | Action |
|---|---|---|---|
| `canola_oil_la_1dollar` | LIGHT_TOUCH | **P1** | $1.00/lb LA floor as historical support level. |
| `canada_canola_700_tonne` | LIGHT_TOUCH | **P1** | CAD $700/tonne support level. Both are price-architecture anchor points. |

### technology (1 orphan)

`power_to_liquid` — DOC_PENDING, P3.

---

## 4. Execution plan

**Phase A — P0 fills (must complete before any monthly-series forecast)**

Targets (11 nodes):
`usda.grain_stocks`, `usda.grain_stocks.feed_residual`, `corn_pollination_window`, `soybean_pod_fill_aug`, `usda_june30_reports`, `usda_june30_stocks`, `tallow_feedstock` (merge), `rfs2`, `us.corn_belt`, `nopa.monthly`, `usda.grain_stocks_report`.

Action: one seeder script like `kg_seed_orphan_commodities.py` containing all 11 LIGHT_TOUCH contexts. ~1-2 hours work.

**Phase B — P1 data_series auto-computation**

Hook `seasonal_calculator.py` to emit seasonal_norm contexts for every data_series node that has coverage in silver/bronze. Expected coverage: `brazil.imea`, `nass.fats_oils.canola_crush`, `nass.fats_oils.canola_oil_stocks`, `us.canola_oil.ending_stocks`, `epa.emts`.

**Phase C — Doc ingestion batch**

When Google Drive docs are accessible: extract HOBO-leftovers (`hefa_capex_benchmark`, `best_worst_margins`, `state_rd_incentives`) and any new sources (FCL canola crush margin, Braya SBO refining, CPPIB draft). Target: close out DOC_PENDING nodes.

**Phase D — Econometric models**

For each ECONOMETRIC-tagged node, build a fitted model, create a `kg_callable` row, and seed contexts from model output. Priority queue:
1. `yg_relative_price_sbo` (critical for BBD allocation)
2. `usda.grain_stocks.feed_residual` (quarterly_residual_model)
3. `outlook_forum_adjustment_model`

**Phase E — Delete/merge**

- Merge `tallow_feedstock` into `bleachable_fancy_tallow` (same commodity; retire one).
- Decide on `camelina_oil` — keep as stub if we track the market, delete otherwise.

---

## 5. Metrics to track

- **Orphan count** (currently 63; target <20 post-Phase A, <10 post-Phase D).
- **Contexts with provenance** (currently 72/156 = 46%; target 100% post-Phase D — every new context must ship with a provenance row).
- **Callables per P0 node** (currently 1; target ≥1 for every P0 data_series, ideally ≥2 per commodity).
- **Forecast-book coverage** (see task #13): every monthly series the LLM is supposed to forecast must have a KG path that ends in a callable.

---

*Next: execute Phase A in a single seeder pass.*
