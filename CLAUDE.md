# RLC-Agent Project Context

## Overview
RLC-Agent is an agricultural commodity data collection, analysis, and reporting system focused on US and global grain/oilseed markets, biofuels, and energy markets relevant to agriculture. The system uses a **medallion data architecture** (Bronze/Silver/Gold) with PostgreSQL as the primary database.

**Project Location:** `C:\Users\torem\RLC Dropbox\RLC Team Folder\RLC-Agent`

---

## Database Architecture

**Connection:** PostgreSQL database `rlc_commodities` on `localhost:5432`, user: `postgres`

### Bronze Layer (31 Tables) - Raw Ingested Data

| Table | Description | Key Fields |
|-------|-------------|------------|
| `bronze.fas_psd` | USDA FAS global S&D balance sheets | commodity, country_code, marketing_year, production, exports, ending_stocks |
| `bronze.fas_export_sales` | Weekly US export sales by country | commodity, country, week_ending, weekly_exports, outstanding_sales |
| `bronze.usda_nass` | NASS crop progress/condition/production | commodity, state, week_ending, value |
| `bronze.nass_crop_progress` | Weekly crop planting, emergence, maturity | commodity, state, week_ending, progress_pct |
| `bronze.nass_crop_condition` | Weekly crop condition ratings | commodity, state, week_ending, excellent, good, fair, poor, very_poor |
| `bronze.conab_production` | Brazil all crops by state (7,255 records) | crop_year, state, commodity, area_planted, production, yield |
| `bronze.conab_supply_demand` | Brazil S&D balance | crop_year, commodity, item, value |
| `bronze.conab_prices` | Brazil commodity prices | date, commodity, state, price |
| `bronze.census_trade` | US import/export trade with HS codes (1,536 records) | hs_code, country, flow, quantity, value |
| `bronze.cftc_cot` | Commitments of Traders data | commodity, report_date, commercial_long, commercial_short, mm_long, mm_short |
| `bronze.eia_raw_ingestion` | EIA energy data (ethanol, petroleum, etc.) | series_id, period, value |
| `bronze.eia_ethanol` | EIA ethanol production/stocks | series_id, period, value |
| `bronze.eia_petroleum` | EIA petroleum data | series_id, period, value |
| `bronze.epa_rfs_rin_generation` | RFS RIN generation data | year, month, rin_type, gallons |
| `bronze.epa_rfs_rin_transaction` | RFS RIN transaction data | transaction_date, rin_type, quantity |
| `bronze.weather_raw` | Raw weather observations | station_id, observation_time, temp, precip |
| `bronze.weather_observations` | Hourly weather data | location, observation_time, temperature, precipitation |
| `bronze.weather_emails` | Meteorologist commentary extracts | email_date, subject, extracted_text |
| `bronze.usda_ams_cash_prices` | AMS cash grain prices | commodity, location, date, price |
| `bronze.usda_ers_data` | ERS oilcrops and wheat data | series_name, period, value |
| `bronze.cme_settlements` | CME futures settlement prices | contract, settle_date, settle_price |
| `bronze.ndvi_data` | Satellite vegetation index data | region, date, ndvi_value |
| `bronze.wheat_tenders` | International wheat tender data | country, tender_date, quantity, price |

### Silver Layer (26 Tables) - Cleaned & Standardized Data

| Table | Description | Key Fields |
|-------|-------------|------------|
| `silver.monthly_realized` | Monthly S&D actuals from NASS (400+ records) | commodity, marketing_year, month, attribute, realized_value, source |
| `silver.user_sd_estimate` | User-provided S&D projections | commodity, country, marketing_year, attribute, estimate_value |
| `silver.crop_progress` | Standardized progress with YoY comparisons | commodity, state, week_ending, progress_pct, yoy_change |
| `silver.crop_condition` | Condition ratings with G/E calculations | commodity, state, week_ending, good_excellent_pct |
| `silver.nass_latest_progress` | Most recent crop progress | commodity, state, progress_pct |
| `silver.nass_crop_condition_ge` | Good/Excellent condition percentage | commodity, state, week_ending, ge_pct |
| `silver.conab_production` | Standardized Brazil production with YoY | crop_year, state, commodity, production, yoy_change |
| `silver.conab_balance_sheet` | Brazil S&D with stocks-to-use ratios | crop_year, commodity, stocks_to_use |
| `silver.census_trade_monthly` | Monthly trade flows by partner | commodity, country, month, imports, exports |
| `silver.cftc_position_history` | Net positioning calculations | commodity, report_date, mm_net, comm_net |
| `silver.ethanol_weekly` | Weekly ethanol with moving averages | week_ending, production, stocks, ma_4wk |
| `silver.weather_observation` | Cleaned hourly weather (152,792 records) | station_id, state, observation_time, temp_f, precip_in |
| `silver.cash_price` | Daily cash prices standardized | commodity, location, price_date, cash_price |
| `silver.futures_price` | Daily futures settlements | contract, settle_date, settle_price |

### Gold Layer (53 Views) - Analytics-Ready

#### US Balance Sheet Views
| View | Description |
|------|-------------|
| `gold.fas_us_corn_balance_sheet` | US Corn S&D balance sheet (formatted) |
| `gold.fas_us_soybeans_balance_sheet` | US Soybeans S&D balance sheet |
| `gold.fas_us_wheat_balance_sheet` | US Wheat S&D balance sheet |
| `gold.us_soybean_balance_sheet` | Historical US soybean S&D (from ERS) |
| `gold.us_soybean_meal_balance_sheet` | US soybean meal S&D |
| `gold.us_soybean_oil_balance_sheet` | US soybean oil S&D |
| `gold.us_wheat_balance_sheet` | Historical US wheat S&D |

#### Brazil Production Views
| View | Description |
|------|-------------|
| `gold.brazil_soybean_production` | Brazil soy by state (1,750 records) |
| `gold.brazil_corn_production` | Brazil corn (1st and 2nd crop) by state |
| `gold.brazil_national_production` | Brazil national totals |
| `gold.brazil_production_by_state` | All Brazil crops by state |
| `gold.brazil_balance_sheet` | Brazil S&D balance sheet |
| `gold.brazil_crop_summary` | Latest Brazil crop estimates |

#### Crop Conditions & Progress Views
| View | Description |
|------|-------------|
| `gold.corn_condition_latest` | Current corn condition vs 5-year avg |
| `gold.soybean_condition_latest` | Current soybean condition vs 5-year avg |
| `gold.wheat_condition_latest` | Current wheat condition vs 5-year avg |
| `gold.nass_condition_yoy` | Condition ratings year-over-year |

#### CFTC Positioning Views
| View | Description |
|------|-------------|
| `gold.cftc_sentiment` | Current managed money positioning summary |
| `gold.cftc_mm_extremes` | Historical position extremes |
| `gold.cftc_corn_positioning` | Corn managed money positions |
| `gold.cftc_soybean_positioning` | Soybean managed money positions |
| `gold.cftc_wheat_positioning` | Wheat managed money positions |

#### Energy & Biofuels Views
| View | Description |
|------|-------------|
| `gold.eia_ethanol_weekly` | Weekly ethanol production summary |
| `gold.eia_petroleum_weekly` | Weekly petroleum data |
| `gold.eia_prices_daily` | Daily energy prices |
| `gold.ethanol_production_summary` | Ethanol production with trends |
| `gold.rin_monthly_trend` | RIN generation trends |
| `gold.rin_generation_summary` | RIN generation by type |
| `gold.d6_ethanol_trend` | D6 conventional ethanol RINs |
| `gold.d4_bbd_trend` | D4 biomass-based diesel RINs |

#### Weather Views
| View | Description |
|------|-------------|
| `gold.weather_latest` | Most recent readings by location |
| `gold.weather_summary` | Aggregated weather data |
| `gold.weather_regional_summary` | Regional weather averages |

#### Price & Trade Views
| View | Description |
|------|-------------|
| `gold.futures_daily_validated` | Validated daily futures prices |

---

## Monthly Realized Data (`silver.monthly_realized`)

Tracks monthly S&D component actuals from NASS processing reports for comparison against annual balance sheet estimates.

### Available Attributes by Source

**NASS_FATS_OILS** (Fats & Oils Report):
- `oil_production_crude` - Crude oil production
- `oil_production_refined` - Refined oil production
- `oil_stocks` - Oil ending stocks
- Commodities: soybeans, canola, corn, cottonseed, sunflower

**NASS_GRAIN_CRUSH** (Grain Crushings Report):
- `crush` - Grain crushed/processed (corn for ethanol, alcohol)
- Commodities: corn, sorghum

**NASS_FLOUR_MILL** (Flour Milling Report - Quarterly):
- `flour_production` - Flour produced by wheat class
- Commodities: wheat (HRW, SRW, HRS, durum)

**NASS_PEANUT** (Peanut Processing Report):
- `peanuts_milled` - Peanuts milled for food use
- `peanuts_crushed` - Peanuts crushed for oil
- `peanuts_stocks` - Peanut ending stocks
- `peanuts_usage` - Peanut consumption

---

## Key Data Source Reference

### USDA FAS PSD Data Structure

The `bronze.fas_psd` table contains global supply/demand balance sheets:

| Column | Description |
|--------|-------------|
| commodity | corn, soybeans, wheat, barley, rice, sorghum, palm_oil, cotton |
| country_code | US, BR, AR, CH (China), E4 (EU), RS (Russia), UP (Ukraine), etc. |
| marketing_year | Marketing year (e.g., 2024 = 2024/25 MY) |
| production | Production in 1000 MT |
| beginning_stocks | Opening inventory |
| imports | Total imports |
| total_supply | Beginning + Production + Imports |
| domestic_consumption | Total domestic use |
| feed_dom_consumption | Feed & residual use |
| fsi_consumption | Food, Seed, Industrial use |
| crush | Crush (oilseeds only) |
| exports | Total exports |
| ending_stocks | Closing inventory |

**Key Balance Sheet Relationships**:
- `total_supply = beginning_stocks + production + imports`
- `total_distribution = domestic_consumption + exports`
- `ending_stocks = total_supply - total_distribution`
- `stocks_to_use_ratio = ending_stocks / total_distribution * 100`

### Country Codes Reference

| Code | Country | Role |
|------|---------|------|
| US | United States | Major producer/exporter |
| BR | Brazil | #1 soy exporter, major corn |
| AR | Argentina | Major soy/corn exporter |
| CH/CN | China | #1 soybean importer |
| E4/EU | European Union | Major wheat, rapeseed |
| RS/RU | Russia | #1 wheat exporter |
| UP/UA | Ukraine | Major corn/wheat exporter |
| AS/AU | Australia | Major wheat exporter |
| CA | Canada | Major wheat, rapeseed |
| IN | India | Major wheat, soybean meal |
| ID | Indonesia | #1 palm oil producer |
| MY | Malaysia | #2 palm oil producer |

### Marketing Year Reference

| Commodity | Marketing Year Start | Example |
|-----------|---------------------|---------|
| Corn | September 1 | MY 2024 = Sep 2024 - Aug 2025 |
| Soybeans | September 1 | MY 2024 = Sep 2024 - Aug 2025 |
| Wheat | June 1 | MY 2024 = Jun 2024 - May 2025 |
| Cotton | August 1 | MY 2024 = Aug 2024 - Jul 2025 |
| Rice | August 1 | MY 2024 = Aug 2024 - Jul 2025 |
| Brazil Soybeans | February 1 | MY 2024 = Feb 2024 - Jan 2025 |
| Argentina Soybeans | March 1 | MY 2024 = Mar 2024 - Feb 2025 |

### Unit Conversions

**Volume:**
- 1 bushel corn = 25.4 kg = 56 lbs
- 1 bushel soybeans = 27.2 kg = 60 lbs
- 1 bushel wheat = 27.2 kg = 60 lbs
- 1 MT = 36.74 bushels corn = 36.74 bushels soybeans

**Weight:**
- 1 MT (metric ton) = 1,000 kg = 2,204.6 lbs
- 1 short ton = 907 kg = 2,000 lbs

**US to Metric Conversion:**
- Corn: mil bu × 0.0254 = MMT
- Soybeans: mil bu × 0.0272 = MMT

---

## Regional References

**US Corn Belt:** IA, IL, NE, MN, IN, OH, SD, WI, MO, KS
**US Soybean Belt:** IL, IA, MN, IN, NE, OH, MO, SD, ND, AR
**US HRW Wheat:** KS, OK, TX, CO, NE
**US SRW Wheat:** OH, IN, IL, MO, AR, KY, TN
**US Spring Wheat:** ND, MT, MN, SD
**Brazil Soybean:** Mato Grosso (MT), Parana (PR), Rio Grande do Sul (RS), Goias (GO), Mato Grosso do Sul (MS)

---

## Domain Knowledge Resources

**Location:** `domain_knowledge/`

### Data Dictionaries (`data_dictionaries/`)
| File | Description |
|------|-------------|
| `usda_fas_psd_api_reference.json` | PSD commodity/country codes |
| `usda_psd_marketing_year_bible.md` | Complete marketing year reference |
| `eia_series_id_ref_biof_and_petrol.md` | 150+ EIA series IDs for ethanol, biodiesel, petroleum |
| `eia_series_reference.json` | EIA series in JSON format |
| `epa_rfs_rin_reference.json` | EPA RFS RIN codes and references |
| `hs_codes_reference.json` | HS trade codes for agricultural commodities |
| `us_census_trade_reference.json` | Census trade data reference |
| `ag_econ_models_reference.json` | Agricultural economics models |
| `weather_forecast_data_reference.md` | Weather forecast data sources |

### Crop Maps (`crop_maps/`)
Production maps for 38 countries (201 maps total):
- `crop_maps/us/` - US county-level production maps (corn, soybeans, wheat varieties, cotton, etc.)
- `crop_maps/brazil/` - Brazil state-level maps
- `crop_map_inventory.json` - Maps all crop maps to PSD commodity codes

### Crop Calendars (`crop_calendars/`)
Monthly GIF files showing global crop planting/harvest timing for all 12 months.

### Balance Sheets (`balance_sheets/`)
S&D templates organized by commodity category:
- `biofuels/` - Ethanol, biodiesel, renewable diesel
- `fats_greases/` - Animal fats, UCO, tallow
- `feed_grains/` - Corn, sorghum, barley
- `food_grains/` - Wheat, rice
- `oilseeds/` - Soybeans, canola, sunflower

### Sample Reports (`sample_reports/`)
Professional market commentary examples including:
- Higby Barrett weekly reports
- Historical market event case studies (drought years, trade disruptions)
- Industry outlook reports

### Special Situations (`special_situations/`)
Historical market event documentation:
- `2012_us_drought.md` - Major US drought impact
- `2018_china_trade_war.md` - Trade war effects
- `2020_china_demand_surge.md` - Chinese buying surge
- `2020_derecho.md` - Midwest storm damage
- `2022_ukraine_war.md` - Black Sea disruption

---

## LLM Database Access

### MCP Server Tools
The MCP server at `src/mcp/commodities_db_server.py` provides direct database access.

**Available Tools:**
| Tool | Function |
|------|----------|
| `get_balance_sheet` | S&D balance sheet for commodity/country |
| `get_production_ranking` | Global production rankings |
| `get_stocks_to_use` | Stocks-to-use ratio analysis |
| `analyze_supply_demand` | Comprehensive S&D with YoY changes |
| `get_brazil_production` | Brazil state-level production |
| `query_database` | Custom SQL queries |

### CLI Query Tool
```bash
# Get commodity summary
python src/tools/db_query.py --analysis commodity_coverage

# Get US corn balance sheet
python src/tools/db_query.py --analysis us_corn_balance

# Custom SQL query
python src/tools/db_query.py "SELECT * FROM bronze.fas_psd WHERE commodity='corn' LIMIT 5"
```

---

## Quick Query Examples

### US Corn Balance Sheet
```sql
SELECT * FROM gold.fas_us_corn_balance_sheet ORDER BY marketing_year DESC LIMIT 3;
```

### Global Soybean Production Comparison
```sql
SELECT country, country_code, marketing_year, production, exports, ending_stocks
FROM bronze.fas_psd
WHERE commodity = 'soybeans' AND marketing_year >= 2024
ORDER BY production DESC;
```

### Brazil Soybean Production by State
```sql
SELECT * FROM gold.brazil_soybean_production
WHERE crop_year = '2024/25'
ORDER BY production DESC LIMIT 10;
```

### CFTC Positioning
```sql
SELECT * FROM gold.cftc_sentiment;
```

### US Corn Stocks-to-Use Trend
```sql
SELECT
    marketing_year,
    ending_stocks,
    domestic_consumption + exports as total_use,
    ROUND(ending_stocks / (domestic_consumption + exports) * 100, 1) as stocks_use_pct
FROM bronze.fas_psd
WHERE commodity = 'corn' AND country_code = 'US'
ORDER BY marketing_year DESC;
```

### Monthly Soybean Oil Production
```sql
SELECT calendar_year, month, realized_value / 1e9 as billion_lbs
FROM silver.monthly_realized
WHERE commodity = 'soybeans'
  AND attribute = 'oil_production_crude'
  AND source = 'NASS_FATS_OILS'
ORDER BY calendar_year DESC, month DESC
LIMIT 12;
```

### Corn for Ethanol by Month
```sql
SELECT calendar_year, month, realized_value
FROM silver.monthly_realized
WHERE commodity = 'corn' AND attribute = 'crush' AND source = 'NASS_GRAIN_CRUSH'
ORDER BY calendar_year DESC, month DESC;
```

### Weather Summary
```sql
SELECT state, AVG(temp_f) as avg_temp, SUM(precip_in) as total_precip
FROM silver.weather_observation
WHERE observation_time > NOW() - INTERVAL '7 days'
GROUP BY state ORDER BY state;
```

---

## Data Collectors Status

### Complete (Data Flowing)
| Collector | Source | Frequency |
|-----------|--------|-----------|
| usda_nass_collector | USDA NASS | Weekly |
| nass_processing | Fats/Oils, Grain Crush, Flour, Peanut | Monthly |
| census_trade_collector | Census Bureau | Monthly |
| cftc_cot_collector | CFTC | Weekly (Friday) |
| weather_collector_agent | OpenWeather/Open-Meteo | Hourly |
| weather_email_agent | Gmail | Daily |
| usda_fas_psd | USDA FAS PSD | Monthly |

### Partial (Needs Integration)
| Collector | Issue |
|-----------|-------|
| eia_ethanol | Needs save_to_bronze standardization |
| eia_petroleum | Needs save_to_bronze |
| conab | Schema ready, needs save_to_bronze |
| cme_settlements | Needs testing |
| usda_ams | Needs standardization |
| usda_ers | Needs save_to_bronze |
| fas_export_sales | In development |

---

## EIA Series ID Quick Reference

### Ethanol (Weekly)
- `PET.W_EPOOXE_YOP_NUS_MBBLD.W` - US Production (kbd)
- `PET.W_EPOOXE_SAE_NUS_MBBL.W` - US Stocks (kb)

### Gasoline (Weekly)
- `PET.WGTSTUS1.W` - US Total Stocks
- `PET.WGFUPUS2.W` - US Product Supplied
- `PET.EMM_EPM0_PTE_NUS_DPG.W` - US Retail Price

### Diesel (Weekly)
- `PET.WDISTUS1.W` - US Total Stocks
- `PET.EMD_EPD0_PTE_NUS_DPG.W` - US Retail Price

### Natural Gas
- `NG.RNGWHHD.D` - Henry Hub Spot Price (daily)
- `NG.NW2_EPG0_SWO_R48_BCF.W` - Working Gas Storage (weekly)

### Propane (Weekly)
- `PET.W_EPLLPZ_SAE_NUS_MBBL.W` - US Total Stocks
- `PET.W_EPLLPA_PRS_R20_DPG.W` - Midwest Residential Price

**Full reference:** `domain_knowledge/data_dictionaries/eia_series_id_ref_biof_and_petrol.md`

---

## Data Update Schedule

| Data Source | Update Frequency | Timing |
|-------------|------------------|--------|
| USDA FAS PSD | Monthly | WASDE day 12:00 PM ET |
| USDA NASS Progress | Weekly | Monday |
| USDA NASS Processing | Monthly | ~10th of month |
| CFTC COT | Weekly | Friday release |
| EIA Petroleum Weekly | Weekly | Wednesday |
| CONAB | Monthly | Variable |
| Weather | Hourly | Continuous |
| Export Sales | Weekly | Thursday 8:30 AM ET |

---

## Key Paths

| Resource | Location |
|----------|----------|
| Database Schemas | `database/schemas/` |
| Data Collectors | `src/collectors/` |
| Agents | `src/agents/`, `src/scheduler/agents/` |
| Configuration | `config/` |
| Domain Knowledge | `domain_knowledge/` |
| Weather Graphics | `data/weather_graphics/` |
| Generated Graphics | `data/generated_graphics/` |
| Documentation | `docs/DATA_SOURCES_PUNCHLIST.md` |

---

## Graphics & Visualization

### Weather Graphics (`data/weather_graphics/`)
Weather images extracted from meteorologist emails, organized by date.

### Generated Graphics (`data/generated_graphics/`)
Custom visualizations from database data:
- `charts/` - Time series, bar charts, YoY comparisons
- `maps/` - Overlay maps, regional analysis
- `tables/` - Balance sheets, comparison tables

### Graphics Generator Capabilities (`src/agents/graphics_generator_agent.py`)
- Time series charts (prices, production, stocks)
- Bar charts (comparisons, rankings)
- Year-over-year comparison charts
- CFTC positioning charts
- Ethanol production dashboards
- Crop condition charts
- Balance sheet tables

---

## Session Protocol

**On every session start, follow this protocol:**

1. **Check briefing**: Call `get_briefing()` MCP tool to see unread system events (new data arrivals, failures, overdue alerts)
2. **Check data freshness**: Call `get_data_freshness()` to see which data sources are current vs stale/overdue
3. **Summarize** what's new and what needs attention for the user
4. **When analyzing data**: Query the Knowledge Graph for analyst context (see below)
5. **After processing events**: Call `acknowledge_events([event_ids])` to mark them as read

---

## CNS (Central Nervous System) Tools

The MCP server provides 7 tools for system awareness and analyst intelligence:

### Status & Notification Tools

| Tool | Purpose | When to Use |
|------|---------|-------------|
| `get_briefing` | Unacknowledged system events (LLM inbox) | Session start — see what happened since last session |
| `get_data_freshness` | Data staleness for each collector | Session start — check if any data is overdue |
| `acknowledge_events` | Mark events as read | After summarizing events for the user |
| `get_collection_history` | Run history for a specific collector | When debugging failures or checking reliability |

### Knowledge Graph Tools

| Tool | Purpose | When to Use |
|------|---------|-------------|
| `search_knowledge_graph` | Search KG nodes by type, label, or key | Finding relevant analyst context for a topic |
| `get_kg_context` | Full enriched context for a node (contexts + edges + summary) | **Primary tool** — when analyzing any commodity or data series |
| `get_kg_relationships` | Relationships from/to a node | Exploring causal links, cross-market effects |

### Knowledge Graph Usage

The Knowledge Graph contains **analyst-level frameworks** extracted from professional commodity reports. It encodes:
- **Expert rules**: "When managed money net long exceeds 90th percentile, liquidation risk increases"
- **Causal chains**: "Late safrinha planting → reduced Brazil corn yield → tighter global S&D"
- **Risk thresholds**: Soybean oil price architecture floors/ceilings based on biofuel economics
- **Seasonal patterns**: Growing season calendar, WASDE report methodology shifts by month
- **Cross-market links**: Soy oil vs palm oil substitution, wheat-corn feed price linkage

**When analyzing a commodity**, always call `get_kg_context(node_key)` first. The KG provides the analytical framework that turns raw numbers into meaningful insight.

### Key Node Keys Reference

| Category | node_key | What It Provides |
|----------|----------|------------------|
| **Commodities** | | |
| Corn | `corn` | Cross-market relationships, positioning rules |
| Soybeans | `soybeans` | Chinese demand tracking, pod fill development, Brazil S&D |
| Soybean Oil | `soybean_oil` | Price architecture (RD capacity, biofuel breakevens) |
| Soybean Meal | `soybean_meal` | Crush economics, Argentine competition |
| Ethanol | `ethanol` | RFS mandates, corn grind demand, SRE policy impacts |
| Wheat (SRW/HRW/HRS) | `wheat_srw`, `wheat_hrw`, `wheat_hrs` | Feed price linkage to corn, winter kill risk |
| Canola Oil | `canola_oil` | CI advantage for biofuels, Canada supply dynamics |
| Renewable Diesel | `renewable_diesel` | Feedstock competition, capacity expansion |
| **Data Series** | | |
| CFTC COT | `cftc.cot` | Positioning analytics, managed money sentiment |
| Crop Conditions | `usda.crop_condition_rating` | G/E → yield prediction model, seasonal decline norms |
| Crop Development | `usda.crop_progress.development` | Silking/blooming/pod stages, frost vulnerability |
| WASDE Patterns | `usda.wasde.revision_pattern` | Historical yield revision direction by month |
| Export Sales | `usda.export_sales` | Pace vs USDA projection, Chinese buying patterns |
| EIA Ethanol | `eia.ethanol` | Production/stocks weekly trends |
| Brazil CONAB | `brazil.conab` | Crop framework: IMEA planting, STU, crush competition |
| NOPA Crush | `nopa.crush` | Monthly crush analytics |
| FSA Acreage | `fsa.acreage` | Acreage revision prediction from certified data |
| **Analytical Models** | | |
| Crop Condition → Yield | `crop_condition_yield_model` | G/E change predicts USDA yield change (signature methodology) |
| Planting Pace → Acreage | `planting_pace_acreage_model` | Early finish → corn up, soy down (counterintuitive) |
| Quarterly Residual | `quarterly_residual_model` | Stocks estimation from export inspections |
| Acreage Rules | `acreage_rules_of_thumb` | Farmers prefer corn, late corn kills soy |
| **Seasonal Events** | | |
| Peak Weather | `peak_weather_sensitivity` | Jul corn pollination, Aug-Sep soy pod fill |
| August WASDE Pivot | `august_wasde_pivot` | Methodology shift from trendline to surveys |
| June 30 Reports | `usda_june30_acreage` | Most volatile trading day framework |
| **Policy** | | |
| RFS Mandates | `rfs2` | Renewable fuel standard mechanics |
| SCOTUS SRE | `scotus_sre_ruling_2021` | Small refinery exemption policy crisis |
| RVO | `rvo` | Annual renewable volume obligations |
| **Biofuel Markets** | | |
| BBD Balance Sheet | `bbd_balance_sheet_model` | Production vs mandate, capacity utilization, oversupply |
| BBD Margins | `bbd_margin_model` | Revenue stack (fuel + RIN + LCFS), feedstock cost by mix |
| Feedstock Supply Chain | `feedstock_supply_chain_model` | UCO/tallow/DCO global supply, import trends |
| RIN Oversupply | `rin_oversupply_model` | Nesting mechanics, D4/D6 spread, mandate math |
| SAF | `sustainable_aviation_fuel` | Emerging demand competitor for feedstocks |
| UCO | `used_cooking_oil` | Lowest CI feedstock, China sourcing, traceability |
| LCFS Credits | `lcfs_credit_framework` | CI-based credit value by feedstock |
| Crusher Feasibility | `crusher_feasibility_model` | Consulting framework for crush facility analysis |
| **Pre-Season Models** | | |
| Prospective Plantings | `prospective_plantings_framework` | March 31 surprise probability, range analysis |
| Insurance Price Ratio | `insurance_price_ratio_model` | Feb RP ratio predicts acreage allocation |
| Balance Sheet Construction | `balance_sheet_construction` | Independent S&D methodology |
| Outlook Forum Pipeline | `outlook_forum_adjustment_model` | AOF → May WASDE adjustment framework |

### Example KG Workflow

When a user asks about **corn positioning**:
1. Call `get_kg_context('cftc.cot')` → returns expert rules on positioning extremes, seasonal patterns
2. Call `get_kg_context('corn')` → returns cross-market links, demand drivers
3. Call `get_kg_relationships('corn')` → shows what CAUSES corn price movement
4. Query `gold.cftc_corn_positioning` for current data
5. Frame the analysis using KG context: "Net long at X contracts is in the Yth percentile for [month]. Historical pattern suggests..."

---

## Current Knowledge Graph Stats

- **160 nodes** (commodities, data series, models, seasonal events, policies, regions, market participants)
- **90 edges** (causal links, competition, seasonal patterns, predictions)
- **70 contexts** (expert rules, risk thresholds, seasonal norms, computed percentiles, pace tracking)
- **75 sources** (HB Weekly Text reports, Fastmarkets quarterlies, consulting engagements)
- **8 extraction batches** covering the complete annual analytical cycle + biofuel demand infrastructure
- **15 computed contexts**: 9 seasonal norms (CFTC monthly percentiles × 6 + crop condition weekly × 3) + 6 pace tracking (soy crush vs USDA × 4 MYs + corn grind YoY × 2 MYs)

### Computed Context Types

| Type | Source | What It Provides |
|------|--------|------------------|
| `seasonal_norm` / `cftc_mm_net_monthly` | seasonal_calculator | Monthly p10/p25/p50/p75/p90 of managed money net positioning |
| `seasonal_norm` / `crop_condition_ge_weekly` | seasonal_calculator | Weekly p10-p90 of Good/Excellent crop ratings |
| `pace_tracking` / `soy_crush_pace_myNNNN` | pace_calculator | Soy crush cumulative pace vs USDA annual projection (%) |
| `pace_tracking` / `corn_grind_pace_myNNNN` | pace_calculator | Corn grind YoY pace comparison |

### Delta Summaries in Event Log

When collectors complete, the `event_log.details` JSONB now includes intelligent delta summaries:
- **CFTC COT**: week-over-week positioning changes, 1-year percentile ranking, extreme position flags
- **Crop Conditions**: G/E weekly change, year-over-year comparison
- **NASS Processing**: month-over-month crush changes with percentage
- **EIA Ethanol**: production/stocks week-over-week changes (when data available)
