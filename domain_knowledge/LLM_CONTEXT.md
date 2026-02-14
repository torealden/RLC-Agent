# RLC-Agent Comprehensive Context Guide

**Project Location:** `C:\Users\torem\RLC Dropbox\RLC Team Folder\RLC-Agent`

This document provides complete context for all resources, data sources, and capabilities available for agricultural commodity analysis and report generation.

---

## Project Overview

RLC-Agent is an agricultural commodity data collection, analysis, and reporting system focused on:
- US and global grain/oilseed markets (corn, soybeans, wheat)
- Biofuels (ethanol, biodiesel, renewable diesel, SAF)
- Energy markets relevant to agriculture
- Weather impacts on crop production
- Market positioning and trade flows

---

## Database Architecture

**Connection:** PostgreSQL database using medallion architecture

### Bronze Layer (Raw Data)
Raw data as collected from sources, minimal transformation.

| Table | Description | Key Fields |
|-------|-------------|------------|
| `bronze.usda_nass` | USDA NASS crop progress, condition, production | commodity, state, week_ending, value |
| `bronze.census_trade` | US trade data with 10-digit HS codes | hs_code, country, flow, quantity, value |
| `bronze.cftc_cot` | CFTC Commitments of Traders | commodity, report_date, commercial_long, commercial_short |
| `bronze.eia_ethanol` | EIA ethanol production/stocks | series_id, period, value |
| `bronze.eia_petroleum` | EIA petroleum data | series_id, period, value |
| `bronze.weather_observations` | Hourly weather data | location, observation_time, temperature, precipitation |
| `bronze.weather_emails` | Extracted meteorologist commentary | email_date, subject, extracted_text |
| `bronze.usda_ams_cash_prices` | AMS cash grain prices | commodity, location, date, price |
| `bronze.usda_ers_data` | ERS oilcrops and wheat data | series_name, period, value |
| `bronze.conab_production` | Brazilian CONAB crop production | crop_year, state, commodity, area_planted, production |
| `bronze.conab_supply_demand` | Brazilian S&D balance | crop_year, commodity, item, value |
| `bronze.conab_prices` | Brazilian commodity prices | date, commodity, state, price |

### Silver Layer (Cleaned/Standardized)
Cleaned, validated, and standardized data with derived fields.

| Table | Description |
|-------|-------------|
| `silver.crop_progress` | Standardized crop progress with YoY comparisons |
| `silver.crop_condition` | Condition ratings with good/excellent calculations |
| `silver.ethanol_weekly` | Weekly ethanol data with moving averages |
| `silver.cot_positioning` | Net positioning calculations |
| `silver.conab_production` | Standardized Brazilian production with YoY |
| `silver.conab_balance_sheet` | S&D with stocks-to-use ratios |

### Gold Layer (Analytics Views)
Pre-computed analytics views for reporting.

| View | Description |
|------|-------------|
| `gold.corn_condition_latest` | Current corn condition vs 5-year average |
| `gold.soybean_condition_latest` | Current soybean condition vs 5-year average |
| `gold.wheat_condition_latest` | Current wheat condition vs 5-year average |
| `gold.ethanol_production_summary` | Weekly ethanol production with trends |
| `gold.cftc_corn_positioning` | Managed money corn positions |
| `gold.cftc_soybean_positioning` | Managed money soybean positions |
| `gold.cftc_wheat_positioning` | Managed money wheat positions |
| `gold.brazil_soybean_production` | Brazilian soybean by state |
| `gold.brazil_corn_production` | Brazilian corn (1st and 2nd crop) by state |
| `gold.brazil_crop_summary` | Latest Brazilian crop estimates |
| `gold.brazil_balance_sheet` | Brazilian S&D with ratios |

---

## Domain Knowledge Resources

**Location:** `domain_knowledge/`

### Crop Maps
**Location:** `domain_knowledge/crop_maps/`

#### US Crop Maps (`crop_maps/us/`)
| File | Commodity | Use For |
|------|-----------|---------|
| `US - Corn - 2023.png` | Corn | County-level production density, drought impact analysis |
| `US - Soybean - 2023.png` | Soybeans | County-level production, regional analysis |
| `US - Wheat - 2023.png` | Wheat (all) | Combined wheat production geography |
| `US - Winter Wheat - 2023.png` | Winter Wheat | HRW/SRW region identification |
| `US - Spring Wheat - 2023.png` | Spring Wheat | Northern Plains analysis |
| `US - Cotton - 2023.png` | Cotton | Cotton belt geography |
| `US - Sorghum - 2023.png` | Sorghum | Great Plains analysis |
| `US - Rice - 2023.png` | Rice | Delta and California |
| `US - Barley - 2023.png` | Barley | Northern tier production |
| `US - Canola - 2023.png` | Canola | Northern Plains canola |
| `US - Sunflower - 2023.png` | Sunflower | Dakotas region |
| `US - Peanut - 2023.png` | Peanuts | Southeast peanut belt |
| `US - Sugarbeets - 2023.png` | Sugarbeets | Red River Valley |
| `US - Sugarcane - 2023.png` | Sugarcane | Florida, Louisiana |

#### Brazil Crop Maps (`crop_maps/brazil/`)
| File | Commodity | Use For |
|------|-----------|---------|
| `Brazil - Soybean - 2023.png` | Soybeans | State-level production analysis |

### Data Dictionaries
**Location:** `domain_knowledge/data_dictionaries/`

| File | Description |
|------|-------------|
| `eia_series_id_ref_biof_and_petrol.md` | Comprehensive EIA series ID reference for ethanol, biodiesel, renewable diesel, gasoline, diesel, natural gas, propane, jet fuel (150+ series) |

### Crop Calendars
**Location:** `domain_knowledge/crop_calendars/`

Monthly GIF files showing global crop planting/harvest timing for January through December.

### Balance Sheets
**Location:** `domain_knowledge/balance_sheets/`

Organized by commodity category:
- `biofuels/` - Ethanol, biodiesel, renewable diesel S&D
- `fats_greases/` - Animal fats, UCO, tallow
- `feed_grains/` - Corn, sorghum, barley
- `food_grains/` - Wheat, rice
- `oilseeds/` - Soybeans, canola, sunflower
- `macro/` - Cross-commodity economic data

### Sample Reports
**Location:** `domain_knowledge/sample_reports/`

| Subfolder | Contents |
|-----------|----------|
| `data/` | USDA and industry data reports by commodity (cross_commodity, energy, feed_grains, food_grains, livestock, oilseeds_fats_greases) |
| `historical/` | Market event case studies (drought_years, market_events, trade_disruptions) |
| `industry_reports/` | Third-party reports (outlook_reports, special_reports, trusted_sources) |
| Root | Higby Barrett weekly reports (examples of professional market commentary) |

### Sample Presentations
**Location:** `domain_knowledge/sample_presentations/`

Conference presentations including Feed Grains, Oilseeds, Biomass Based Diesel Outlook, and webinar materials.

### Special Situations
**Location:** `domain_knowledge/special_situations/`

Historical market event documentation:
- `2012_us_drought.md` - Major US drought impact analysis
- `2018_china_trade_war.md` - Trade war effects on grain markets
- `2020_china_demand_surge.md` - Chinese buying surge
- `2020_derecho.md` - Midwest storm damage
- `2022_ukraine_war.md` - Black Sea disruption

### Templates
**Location:** `domain_knowledge/templates/`

- `marketing_year_template.md` - Standard marketing year analysis template

### Operator Guides
**Location:** `domain_knowledge/operator_guides/`

System operation documentation for different functional roles.

### Additional Reference Files
| File | Description |
|------|-------------|
| `data_sources.xlsx` | Master list of data sources |
| `system_inventory.md` | System component inventory |
| `Iowa Extension - Cost of Storing Grain.pdf` | Grain storage cost reference |
| `Price Reporting Handbook - AMS.pdf` | USDA AMS price reporting methodology |
| `Projections for Brazilian Production - English.xlsx` | CONAB projection templates |
| `uco_facilities.csv`, `uco_locations.txt` | Used cooking oil facility data |

### Key Regional References

**US Corn Belt:** IA, IL, NE, MN, IN, OH, SD, WI, MO, KS
**US Soybean Belt:** IL, IA, MN, IN, NE, OH, MO, SD, ND, AR
**US HRW Wheat:** KS, OK, TX, CO, NE
**US SRW Wheat:** OH, IN, IL, MO, AR, KY, TN
**US Spring Wheat:** ND, MT, MN, SD
**Brazil Soybean:** Mato Grosso (MT), Parana (PR), Rio Grande do Sul (RS), Goias (GO), Mato Grosso do Sul (MS)

---

## Graphics and Visualizations

### Weather Graphics (Dynamic)
**Location:** `data/weather_graphics/`

Weather graphics extracted from meteorologist emails, organized by date:
```
data/weather_graphics/
├── 2026-01-28/
│   ├── weather_map_001.png
│   ├── precipitation_forecast.png
│   └── temperature_outlook.png
```

### Generated Graphics
**Location:** `data/generated_graphics/`

Custom visualizations generated from database data:
```
data/generated_graphics/
├── charts/          # Time series, bar charts, YoY comparisons
├── maps/            # Overlay maps, regional analysis
└── tables/          # Balance sheets, comparison tables
```

### Graphics Generator Capabilities
The `graphics_generator_agent.py` can create:
- Time series charts (prices, production, stocks)
- Bar charts (comparisons, rankings)
- Year-over-year comparison charts
- CFTC positioning charts
- Ethanol production dashboards
- Crop condition charts
- Balance sheet tables

---

## Data Collectors

### Complete (Data Flowing)
| Collector | Source | Frequency | Data |
|-----------|--------|-----------|------|
| `usda_nass_collector.py` | USDA NASS | Weekly | Crop progress, condition, production |
| `census_trade_collector.py` | Census Bureau | Monthly | Trade with HS codes |
| `cftc_cot_collector.py` | CFTC | Weekly | Positioning data |
| `weather_collector_agent.py` | OpenWeather/Open-Meteo | Hourly | Weather observations |
| `weather_email_agent.py` | Gmail | Daily | Meteorologist commentary + graphics |

### Partial (Needs Integration)
| Collector | Source | Issue |
|-----------|--------|-------|
| `eia_ethanol_collector.py` | EIA | Working, needs save_to_bronze standardization |
| `eia_petroleum_collector.py` | EIA | Working, needs save_to_bronze |
| `conab_collector.py` | CONAB Brazil | Schema ready, needs save_to_bronze |
| `cme_settlements_collector.py` | CME | Needs testing |
| `usda_ams_collector.py` | USDA AMS | Working, needs standardization |
| `usda_ers_collector.py` | USDA ERS | Working, needs save_to_bronze |

### Blocked
| Collector | Source | Issue |
|-----------|--------|-------|
| `usda_fas_collector.py` | USDA FAS | API returning 500 errors |

### Not Started (Priority)
- USDA WASDE - Monthly supply/demand estimates
- Canada CGC - Canadian grain commission
- MPOB - Malaysian palm oil
- US Drought Monitor - Weekly drought conditions

---

## Configuration Files

| File | Purpose |
|------|---------|
| `config/eia_series_config.json` | EIA series IDs organized by commodity/frequency with collection profiles |
| `config/usda_commodities.json` | USDA commodity mappings |
| `.env` | Database credentials, API keys |
| `.mcp.json` | MCP server configuration |

### EIA Collection Profiles
The `eia_series_config.json` includes pre-defined collection profiles:
- `biofuels_weekly` - Ethanol and biodiesel weekly data
- `biofuels_monthly` - Monthly biodiesel/renewable diesel
- `petroleum_weekly` - Gasoline, diesel, propane weekly
- `agricultural_energy` - All energy data relevant to agriculture

---

## Report Generation Guidelines

### When Discussing Weather Impacts
1. Reference appropriate crop map from `domain_knowledge/crop_maps/`
2. Check `data/weather_graphics/` for latest weather visualizations
3. Query `bronze.weather_observations` for actual conditions
4. Include meteorologist commentary from `bronze.weather_emails`

### When Analyzing Crop Conditions
1. Query `gold.{commodity}_condition_latest` for current vs 5-year average
2. Reference crop map for regional context
3. Include state-level breakdown for affected regions

### When Analyzing Markets
1. Query `gold.cftc_{commodity}_positioning` for managed money positions
2. Include net position and week-over-week change
3. Relate positioning to fundamental outlook

### When Analyzing Brazilian Production
1. Query `gold.brazil_{commodity}_production` by state
2. Reference `Brazil - Soybean - 2023.png` for geography
3. Include YoY comparisons from silver layer

### When Analyzing Biofuels
1. Query `gold.ethanol_production_summary` for weekly production
2. Reference EIA series from `eia_series_config.json` for additional data
3. Include stocks and implied demand calculations

---

## Useful SQL Queries

### Latest Crop Conditions
```sql
SELECT * FROM gold.corn_condition_latest;
SELECT * FROM gold.soybean_condition_latest;
```

### CFTC Positioning
```sql
SELECT * FROM gold.cftc_corn_positioning
ORDER BY report_date DESC LIMIT 10;
```

### Ethanol Production
```sql
SELECT * FROM gold.ethanol_production_summary
WHERE period >= CURRENT_DATE - INTERVAL '52 weeks'
ORDER BY period;
```

### Brazilian Production
```sql
SELECT * FROM gold.brazil_crop_summary;
SELECT * FROM gold.brazil_soybean_production
WHERE crop_year = '2024/25';
```

### Weather Data
```sql
SELECT * FROM bronze.weather_emails
ORDER BY email_date DESC LIMIT 5;
```

---

## Directory Structure

```
RLC-Agent/
├── config/                 # Configuration files
│   ├── eia_series_config.json
│   └── ...
├── data/                   # Collected and generated data
│   ├── weather_graphics/   # Extracted weather images
│   └── generated_graphics/ # Custom visualizations
├── database/               # Database schemas
│   └── schemas/            # SQL schema files
├── docs/                   # Documentation
│   └── DATA_SOURCES_PUNCHLIST.md
├── domain_knowledge/       # Reference materials
│   ├── crop_maps/          # USDA production maps
│   ├── data_dictionaries/  # Data reference docs
│   ├── GRAPHICS_REFERENCE.md
│   └── LLM_CONTEXT.md      # This file
├── src/                    # Source code
│   ├── agents/             # Agent implementations
│   ├── collectors/         # Data collectors
│   └── scheduler/          # Scheduled tasks
├── .env                    # Environment variables
└── requirements.txt        # Python dependencies
```

---

## Notes

- **SAF Data:** EIA does not have dedicated series IDs for Sustainable Aviation Fuel; it's aggregated in "Other Biofuels"
- **FAS API:** Currently returning 500 errors; monitor for resolution
- **Weather Images:** Extracted daily from meteorologist emails when available
- **Crop Maps:** Source is USDA NASS; shows harvested acreage by county/state
