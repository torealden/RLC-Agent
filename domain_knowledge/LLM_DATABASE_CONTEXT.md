# RLC Commodities Database - LLM Analysis Context

## Overview

This document provides an LLM with the context needed to query and analyze the RLC Commodities database. The database contains comprehensive agricultural commodity supply/demand data, prices, weather, trade flows, and market positioning data.

**Database**: `rlc_commodities` on `localhost:5432` (PostgreSQL)
**Architecture**: Medallion (Bronze → Silver → Gold)

---

## Quick Start Queries

### Get Current US Corn Balance Sheet
```sql
SELECT * FROM gold.fas_us_corn_balance_sheet ORDER BY marketing_year DESC LIMIT 3;
```

### Get Current US Soybean Balance Sheet
```sql
SELECT * FROM gold.fas_us_soybeans_balance_sheet ORDER BY marketing_year DESC LIMIT 3;
```

### Get Global Corn Production by Country
```sql
SELECT country, country_code, marketing_year, production, exports, ending_stocks
FROM bronze.fas_psd
WHERE commodity = 'corn' AND marketing_year >= 2024
ORDER BY production DESC;
```

### Get Brazil Soybean Production by State
```sql
SELECT * FROM gold.brazil_soybean_production WHERE crop_year = '2024/25' ORDER BY production DESC LIMIT 10;
```

### Get Latest Weather Conditions
```sql
SELECT * FROM gold.weather_latest ORDER BY state, location;
```

### Get CFTC Positioning
```sql
SELECT * FROM gold.cftc_sentiment;
```

---

## Data Domains

### 1. Supply & Demand Balance Sheets

#### USDA FAS PSD Data (`bronze.fas_psd`)
Global production, supply, distribution data from USDA Foreign Agricultural Service.

| Column | Description |
|--------|-------------|
| commodity | corn, soybeans, wheat, barley, rice, sorghum, palm_oil, cotton, etc. |
| country_code | US, BR, AR, CH (China), E4 (EU), RS (Russia), UP (Ukraine), etc. |
| marketing_year | Marketing year (e.g., 2024 = 2024/25 MY) |
| production | Production in 1000 MT |
| beginning_stocks | Beginning stocks in 1000 MT |
| imports | Imports in 1000 MT |
| total_supply | Beginning + Production + Imports |
| domestic_consumption | Total domestic use |
| feed_dom_consumption | Feed & residual use |
| fsi_consumption | Food, Seed, Industrial use |
| crush | Crush (oilseeds only) |
| exports | Exports in 1000 MT |
| ending_stocks | Ending stocks in 1000 MT |
| unit | Always "1000 MT" |

**Key Relationships**:
- `total_supply = beginning_stocks + production + imports`
- `total_distribution = domestic_consumption + exports`
- `ending_stocks = total_supply - total_distribution`
- `stocks_to_use_ratio = ending_stocks / total_distribution * 100`

**Example**: US Corn S&D Analysis
```sql
SELECT
    marketing_year,
    production,
    total_supply,
    domestic_consumption,
    exports,
    ending_stocks,
    ROUND(ending_stocks / (domestic_consumption + exports) * 100, 1) as stocks_use_pct
FROM bronze.fas_psd
WHERE commodity = 'corn' AND country_code = 'US'
ORDER BY marketing_year DESC;
```

#### User Estimates (`silver.user_sd_estimate`)
User-provided S&D projections for comparison against official USDA data.

#### Monthly Realized Data (`silver.monthly_realized`)
Tracks monthly S&D component actuals from NASS processing reports. Use this to compare realized progress vs annual balance sheet estimates.

| Column | Description |
|--------|-------------|
| commodity | soybeans, corn, wheat, sorghum, canola, cottonseed, peanuts, etc. |
| country | Always 'US' for NASS data |
| marketing_year | Marketing year the data belongs to |
| month | Calendar month (1-12) |
| calendar_year | Calendar year |
| attribute | What's being measured (see below) |
| realized_value | The actual value |
| unit | LB, BU, CWT depending on source |
| source | NASS_FATS_OILS, NASS_GRAIN_CRUSH, NASS_FLOUR_MILL, NASS_PEANUT |

**Key Attributes**:
- `oil_production_crude` - Crude oil production (Fats & Oils)
- `oil_production_refined` - Refined oil production (Fats & Oils)
- `oil_stocks` - Oil ending stocks (Fats & Oils)
- `crush` - Grain crushed/processed (Grain Crushings)
- `flour_production` - Flour produced (Flour Milling - quarterly)
- `peanuts_milled` - Peanuts milled for food use (Peanut Processing)
- `peanuts_crushed` - Peanuts crushed for oil (Peanut Processing)
- `peanuts_stocks` - Peanut ending stocks (Peanut Processing)
- `peanuts_usage` - Peanut usage/consumption (Peanut Processing)

**Example**: Track Soybean Oil Production vs USDA Estimate
```sql
-- Monthly soybean oil production (in LB)
SELECT calendar_year, month, realized_value / 1e9 as billion_lbs
FROM silver.monthly_realized
WHERE commodity = 'soybeans'
  AND attribute = 'oil_production_crude'
  AND source = 'NASS_FATS_OILS'
ORDER BY calendar_year DESC, month DESC
LIMIT 12;

-- YTD corn for ethanol
SELECT
    marketing_year,
    SUM(realized_value) as ytd_bushels,
    COUNT(*) as months_reported
FROM silver.monthly_realized
WHERE commodity = 'corn'
  AND attribute = 'crush'
  AND source = 'NASS_GRAIN_CRUSH'
GROUP BY marketing_year
ORDER BY marketing_year DESC;

-- Monthly peanut milling
SELECT calendar_year, month, realized_value / 1e6 as million_lbs
FROM silver.monthly_realized
WHERE commodity = 'peanuts'
  AND attribute = 'peanuts_milled'
  AND source = 'NASS_PEANUT'
ORDER BY calendar_year DESC, month DESC
LIMIT 12;
```

#### Gold Views for US Balance Sheets
- `gold.fas_us_corn_balance_sheet` - US corn formatted balance sheet
- `gold.fas_us_soybeans_balance_sheet` - US soybeans formatted balance sheet
- `gold.fas_us_wheat_balance_sheet` - US wheat formatted balance sheet
- `gold.us_soybean_balance_sheet` - Historical US soybean S&D (from ERS)
- `gold.us_soybean_meal_balance_sheet` - US soybean meal S&D
- `gold.us_soybean_oil_balance_sheet` - US soybean oil S&D
- `gold.us_wheat_balance_sheet` - Historical US wheat S&D

---

### 2. Brazil Production (CONAB)

#### Brazil Production by State (`bronze.conab_production`, `gold.brazil_*_production`)
Detailed Brazilian crop production data from CONAB (Companhia Nacional de Abastecimento).

**Key Tables**:
- `gold.brazil_soybean_production` - Soybean production by state
- `gold.brazil_corn_production` - Corn production by state
- `gold.brazil_national_production` - National totals
- `gold.brazil_production_by_state` - All crops by state
- `gold.brazil_balance_sheet` - Brazil S&D balance sheet

**Example**: Brazil Soybean Area vs Production Trend
```sql
SELECT crop_year, state, area_planted_ha, production_tonnes, yield_kg_ha
FROM gold.brazil_soybean_production
WHERE state = 'MATO GROSSO'
ORDER BY crop_year DESC LIMIT 5;
```

---

### 3. US Crop Progress & Conditions (NASS)

#### Crop Progress (`bronze.nass_crop_progress`, `silver.nass_latest_progress`)
Weekly crop planting, emergence, maturity stages.

#### Crop Conditions (`bronze.nass_crop_condition`, `silver.nass_crop_condition_ge`)
Weekly crop condition ratings (Good/Excellent percentage).

**Example**: Current Corn Conditions vs Last Year
```sql
SELECT * FROM gold.nass_condition_yoy WHERE commodity = 'CORN' ORDER BY week_ending DESC LIMIT 5;
```

---

### 4. Trade Data

#### Export Sales (`bronze.fas_export_sales`)
Weekly US export sales and shipments by country.

| Column | Description |
|--------|-------------|
| commodity | corn, soybeans, wheat, etc. |
| country | Destination country |
| marketing_year | Marketing year |
| week_ending | Week ending date |
| weekly_exports | Exports for the week (MT) |
| accumulated_exports | YTD accumulated exports |
| outstanding_sales | Unshipped sales commitments |
| net_sales | New sales minus cancellations |

#### Census Trade (`bronze.census_trade`, `silver.census_trade_monthly`)
Monthly trade flows by commodity and partner country.

**Example**: Top Soybean Export Destinations
```sql
SELECT country, accumulated_exports, outstanding_sales
FROM bronze.fas_export_sales
WHERE commodity = 'soybeans'
  AND week_ending = (SELECT MAX(week_ending) FROM bronze.fas_export_sales WHERE commodity = 'soybeans')
ORDER BY accumulated_exports DESC LIMIT 10;
```

---

### 5. CFTC Positioning (Commitment of Traders)

#### Position Data (`bronze.cftc_cot`, `silver.cftc_position_history`)
Managed money and commercial positions in commodity futures.

**Gold Views**:
- `gold.cftc_sentiment` - Current positioning summary
- `gold.cftc_mm_extremes` - Historical position extremes

| Column | Description |
|--------|-------------|
| commodity | Corn, Soybeans, Wheat, etc. |
| mm_net_long | Managed Money net position |
| comm_net_long | Commercial net position |
| oi_total | Total open interest |

**Example**: Current Managed Money Positioning
```sql
SELECT commodity, mm_net_long,
       CASE WHEN mm_net_long > 0 THEN 'NET LONG' ELSE 'NET SHORT' END as position
FROM gold.cftc_sentiment;
```

---

### 6. Energy & Biofuels

#### EIA Data (`bronze.eia_raw_ingestion`)
Energy Information Administration petroleum and biofuels data.

**Gold Views**:
- `gold.eia_ethanol_weekly` - Weekly ethanol production
- `gold.eia_petroleum_weekly` - Weekly petroleum data
- `gold.eia_prices_daily` - Daily energy prices

#### EPA RFS/RIN Data (`bronze.epa_rfs_*`)
Renewable Fuel Standard compliance and RIN generation data.

**Gold Views**:
- `gold.rin_monthly_trend` - RIN trends over time
- `gold.rin_generation_summary` - RIN generation by type
- `gold.d6_ethanol_trend` - D6 conventional ethanol RINs
- `gold.d4_bbd_trend` - D4 biomass-based diesel RINs

---

### 7. Weather

#### Weather Observations (`bronze.weather_raw`, `silver.weather_observation`)
Hourly weather data from key agricultural regions.

**Gold Views**:
- `gold.weather_latest` - Most recent readings by location
- `gold.weather_summary` - Aggregated weather data
- `gold.weather_regional_summary` - Regional averages

| Column | Description |
|--------|-------------|
| station_id | Weather station ID |
| state | US state |
| temp_f | Temperature (Fahrenheit) |
| precip_in | Precipitation (inches) |
| humidity_pct | Relative humidity |

---

### 8. Prices

#### Cash Prices (`silver.cash_price`)
Daily cash prices for grains, oilseeds, and livestock.

#### Futures Prices (`silver.futures_price`, `gold.futures_daily_validated`)
Daily futures settlement prices.

**Example**: Latest Corn Basis
```sql
SELECT location, cash_price, futures_ref, cash_price - futures_ref as basis
FROM silver.cash_price
WHERE commodity = 'CORN' AND price_date = (SELECT MAX(price_date) FROM silver.cash_price WHERE commodity = 'CORN')
ORDER BY basis;
```

---

## Marketing Year Reference

Different commodities use different marketing year start months:

| Commodity | Marketing Year Start | Example |
|-----------|---------------------|---------|
| Corn | September 1 | MY 2024 = Sep 2024 - Aug 2025 |
| Soybeans | September 1 | MY 2024 = Sep 2024 - Aug 2025 |
| Wheat | June 1 | MY 2024 = Jun 2024 - May 2025 |
| Cotton | August 1 | MY 2024 = Aug 2024 - Jul 2025 |
| Rice | August 1 | MY 2024 = Aug 2024 - Jul 2025 |

---

## Unit Conversions

### Volume
- 1 bushel corn = 25.4 kg = 56 lbs
- 1 bushel soybeans = 27.2 kg = 60 lbs
- 1 bushel wheat = 27.2 kg = 60 lbs
- 1 MT = 36.74 bushels corn = 36.74 bushels soybeans

### Weight
- 1 MT (metric ton) = 1,000 kg = 2,204.6 lbs
- 1 short ton = 907 kg = 2,000 lbs

### US Balance Sheet Units
- Production, stocks: Million bushels (mil bu)
- Global data: 1000 MT (thousand metric tons)

**To convert US bushels to metric for global comparison**:
- Corn: mil bu × 0.0254 = MMT
- Soybeans: mil bu × 0.0272 = MMT

---

## Country Codes Reference

| Code | Country | Role |
|------|---------|------|
| US | United States | Major producer/exporter |
| BR | Brazil | #1 soy exporter, major corn |
| AR | Argentina | Major soy/corn exporter |
| CH/CN | China | #1 importer of soybeans |
| E4/EU | European Union | Major wheat, rapeseed |
| RS/RU | Russia | #1 wheat exporter |
| UP/UA | Ukraine | Major corn/wheat exporter |
| AS/AU | Australia | Major wheat exporter |
| CA | Canada | Major wheat, rapeseed |
| IN | India | Major wheat, soybean meal |
| ID | Indonesia | #1 palm oil producer |
| MY | Malaysia | #2 palm oil producer |

---

## Analytical Queries

### Compare Global Soybean Production
```sql
SELECT
    country,
    marketing_year,
    production,
    ROUND(production / SUM(production) OVER (PARTITION BY marketing_year) * 100, 1) as pct_of_total
FROM bronze.fas_psd
WHERE commodity = 'soybeans' AND marketing_year = 2025 AND production > 1000
ORDER BY production DESC;
```

### Calculate US Corn Stocks-to-Use Trend
```sql
SELECT
    marketing_year,
    ending_stocks,
    domestic_consumption + exports as total_use,
    ROUND(ending_stocks / (domestic_consumption + exports) * 100, 1) as stocks_use_pct
FROM bronze.fas_psd
WHERE commodity = 'corn' AND country_code = 'US'
ORDER BY marketing_year;
```

### Track Brazil vs US Soybean Production
```sql
SELECT
    f1.marketing_year,
    f1.production as us_prod,
    f2.production as brazil_prod,
    ROUND((f2.production - f1.production) / f1.production * 100, 1) as brazil_vs_us_pct
FROM bronze.fas_psd f1
JOIN bronze.fas_psd f2 ON f1.marketing_year = f2.marketing_year
WHERE f1.commodity = 'soybeans' AND f1.country_code = 'US'
  AND f2.commodity = 'soybeans' AND f2.country_code = 'BR'
ORDER BY f1.marketing_year;
```

### Get Weather Impact Summary
```sql
SELECT state,
       AVG(temp_f) as avg_temp,
       SUM(precip_in) as total_precip,
       COUNT(*) as readings
FROM silver.weather_observation
WHERE observation_time > NOW() - INTERVAL '7 days'
GROUP BY state
ORDER BY state;
```

---

## How to Query This Database

### Python (recommended)
```python
import psycopg2
import pandas as pd

conn = psycopg2.connect(
    host='localhost',
    port='5432',
    dbname='rlc_commodities',
    user='postgres',
    password='your_password'
)

df = pd.read_sql("SELECT * FROM gold.fas_us_corn_balance_sheet", conn)
```

### Direct SQL
Use any PostgreSQL client (psql, DBeaver, pgAdmin) to connect to:
- Host: localhost
- Port: 5432
- Database: rlc_commodities
- User: postgres

---

## Data Freshness

| Data Source | Update Frequency | Last Updated |
|-------------|------------------|--------------|
| USDA FAS PSD | Monthly (WASDE) | Check `collected_at` |
| CONAB | Monthly | Check bronze.conab_* |
| NASS Progress | Weekly (Mon) | Check `report_date` |
| CFTC COT | Weekly (Fri) | Check `report_date` |
| Weather | Hourly | Check `observation_time` |
| Export Sales | Weekly (Thu) | Check `week_ending` |

---

## Available Commodities in PSD

Currently loaded in `bronze.fas_psd`:
- **Grains**: corn, wheat, barley, rice, sorghum
- **Oilseeds**: soybeans
- **Oilseed Products**: soybean_meal, soybean_oil, palm_oil
- **Fiber**: cotton

To check current coverage:
```sql
SELECT commodity, COUNT(DISTINCT country_code) as countries,
       MIN(marketing_year) as min_year, MAX(marketing_year) as max_year
FROM bronze.fas_psd
GROUP BY commodity
ORDER BY commodity;
```
