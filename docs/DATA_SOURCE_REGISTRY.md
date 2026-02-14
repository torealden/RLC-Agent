# Agricultural Data Source Registry

**Version:** 1.0
**Last Updated:** December 10, 2025
**Purpose:** Master registry of all agricultural commodity data sources for the HB Weekly Report automation system

---

## Table of Contents
1. [Source Classification](#source-classification)
2. [US Government Sources (Free)](#us-government-sources-free)
3. [International Government Sources](#international-government-sources)
4. [Exchange & Market Data](#exchange--market-data)
5. [Industry Organizations](#industry-organizations)
6. [Weather & Climate Data](#weather--climate-data)
7. [Third-Party Aggregators](#third-party-aggregators)
8. [Credentials Summary](#credentials-summary)
9. [Implementation Priority](#implementation-priority)

---

## Source Classification

### Authentication Levels
- **FREE**: No authentication required, public API or data download
- **API_KEY**: Requires free API key registration
- **PAID**: Requires paid subscription
- **SCRAPE**: No API available, requires web scraping

### Data Frequency
- **DAILY**: Updated every business day
- **WEEKLY**: Updated once per week
- **MONTHLY**: Updated once per month
- **QUARTERLY**: Updated quarterly
- **ANNUAL**: Updated annually
- **REALTIME**: Streaming or near-real-time updates

---

## US Government Sources (Free)

### 1. USDA FAS OpenDataWeb
**Category:** Export Sales, WASDE, PSD
**Authentication:** FREE
**Frequency:** WEEKLY / MONTHLY
**Base URL:** `https://apps.fas.usda.gov/OpenData/api/`

| Endpoint | Description | Update Schedule |
|----------|-------------|-----------------|
| `/esr/exports` | Export Sales Report | Thursday 8:30 AM ET |
| `/psd/commodity` | Production, Supply, Distribution | Monthly (WASDE day) |
| `/gats/gats_data` | Global Agricultural Trade System | Varies |

**Implementation Notes:**
- RESTful API with JSON responses
- No API key required (throttling may apply)
- Bulk download available for historical data
- Python SDK: None official, use `requests`

**Example Request:**
```python
import requests
url = "https://apps.fas.usda.gov/OpenData/api/esr/exports/commodityCode/0440000"
response = requests.get(url)
data = response.json()
```

---

### 2. USDA NASS Quick Stats
**Category:** Crop Progress, Acreage, Production
**Authentication:** API_KEY (Free)
**Frequency:** WEEKLY / ANNUAL
**Base URL:** `https://quickstats.nass.usda.gov/api/`

| Endpoint | Description | Update Schedule |
|----------|-------------|-----------------|
| `/api_GET` | Query all NASS data | Monday 4:00 PM ET (crop progress) |

**API Key Registration:** https://quickstats.nass.usda.gov/api

**Data Available:**
- Crop Progress & Condition (weekly during season)
- Planted/Harvested Acreage
- Production estimates
- County-level data

**Implementation Notes:**
- Rate limit: ~50,000 queries/day per API key
- Returns CSV or JSON
- Python package: `nass-api`

---

### 3. USDA FGIS Export Inspections
**Category:** Export Inspections
**Authentication:** FREE
**Frequency:** WEEKLY
**Data URL:** `https://www.ams.usda.gov/mnreports/`

**Reports:**
- GX_GR110.TXT - Weekly export inspections

**Implementation Notes:**
- Fixed-width text files, need parsing
- Updates Monday 11:00 AM ET
- No API, direct file download

---

### 4. CFTC Commitments of Traders
**Category:** Positioning Data
**Authentication:** FREE
**Frequency:** WEEKLY
**Base URL:** `https://www.cftc.gov/MarketReports/CommitmentsofTraders/`

| Report | Description | Update Schedule |
|--------|-------------|-----------------|
| Disaggregated | By trader type (MM, Swap, Prod) | Friday 3:30 PM ET (as of Tuesday) |
| Legacy | Traditional classification | Friday 3:30 PM ET |
| TFF | Traders in Financial Futures | Friday 3:30 PM ET |

**API Endpoint:** `https://publicreporting.cftc.gov/resource/jun7-fc8e.json` (Socrata API)

**Python Package:** `cot_reports`
```bash
pip install cot_reports
```

**Implementation Notes:**
- Bulk historical data available as ZIP files
- Socrata API supports filtering
- Data as of Tuesday, released Friday

---

### 5. EIA Petroleum & Ethanol Data
**Category:** Ethanol Production, Stocks, Blending
**Authentication:** API_KEY (Free)
**Frequency:** WEEKLY
**Base URL:** `https://api.eia.gov/v2/`

| Series | Description | Update Schedule |
|--------|-------------|-----------------|
| `PET.W_EPOOXE_YOP_NUS_MBBLD.W` | Ethanol Production | Wednesday 10:30 AM ET |
| `PET.WCESTUS1.W` | Ethanol Stocks | Wednesday 10:30 AM ET |
| `PET.W_EPOOXE_IM0_NUS-Z00_MBBLD.W` | Ethanol Imports | Wednesday 10:30 AM ET |

**API Key Registration:** https://www.eia.gov/opendata/register.php

**Python Package:** `eia-python`
```bash
pip install eia-python
```

**Implementation Notes:**
- Well-documented RESTful API
- Rate limits generous for registered users
- Historical data back to 2010+

---

### 6. NOAA/NWS Weather Data
**Category:** Weather Forecasts, Climate Data
**Authentication:** FREE
**Frequency:** DAILY
**Base URL:** `https://api.weather.gov/`

**Implementation Notes:**
- No API key required
- Rate limiting applies
- Point forecasts and gridded data available

---

### 7. US Drought Monitor
**Category:** Drought Conditions
**Authentication:** FREE
**Frequency:** WEEKLY
**Data URL:** `https://droughtmonitor.unl.edu/`

**API:** GeoJSON/Shapefile downloads available
**Update Schedule:** Thursday (data as of Tuesday)

**Implementation Notes:**
- Downloadable GeoJSON for mapping
- Historical archive available
- State/county level data

---

## International Government Sources

### 8. MPOB - Malaysian Palm Oil Board
**Category:** Palm Oil Production, Stocks, Exports
**Authentication:** SCRAPE
**Frequency:** MONTHLY
**Base URL:** `http://bepi.mpob.gov.my/`

| Report | Description | Update Schedule |
|--------|-------------|-----------------|
| Overview | Production, Exports, Stocks | ~10th of following month |
| Palm Oil Prices | Daily CPO prices | Daily |

**Implementation Notes:**
- No official API
- Data in HTML tables, requires scraping
- Some data available via Barchart/cmdty (paid)
- OpenDataDSL has MPOB connector (third-party)

**Scraping Approach:**
```python
from bs4 import BeautifulSoup
import requests
url = "http://bepi.mpob.gov.my/index.php/en/statistics/production.html"
# Parse HTML tables
```

---

### 9. CONAB - Brazil National Supply Company
**Category:** Brazil Crop Estimates, Stocks
**Authentication:** FREE
**Frequency:** MONTHLY
**Base URL:** `https://www.conab.gov.br/info-agro/safras`

**Reports:**
- Grain Crop Survey (Safras)
- Monthly bulletins
- Regional breakdowns

**Implementation Notes:**
- Data in PDF reports (need parsing)
- Some Excel downloads available
- Portuguese language

---

### 10. ABIOVE - Brazilian Vegetable Oil Industry Association
**Category:** Brazil Soy Crush, Exports
**Authentication:** FREE
**Frequency:** MONTHLY
**Base URL:** `https://abiove.org.br/en/statistics/`

**Data Available:**
- Soybean crush volumes
- Soymeal/oil production
- Export statistics

**Implementation Notes:**
- Data downloadable as Excel files
- Some PDF reports
- English available

---

### 11. BCBA/Rosario - Argentina Grain Exchanges
**Category:** Argentina Crop Estimates, Basis
**Authentication:** FREE
**Frequency:** WEEKLY
**URLs:**
- Buenos Aires: `https://www.bolsadecereales.com/`
- Rosario: `https://www.bcr.com.ar/`

**Implementation Notes:**
- Weekly crop progress reports
- Harvest pace tracking
- Some data requires scraping
- Spanish language

---

### 12. Eurostat / EC AGRI
**Category:** EU Production, Trade, Stocks
**Authentication:** API_KEY (Free)
**Frequency:** MONTHLY / ANNUAL
**Base URL:** `https://agridata.ec.europa.eu/extensions/DataPortal/API_Documentation.html`

**Endpoints:**
- Machine-to-machine API available
- Eurostat SDMX API for detailed data

**Implementation Notes:**
- Registration required for API access
- Bulk data downloads available
- Well-structured metadata

---

### 13. IGC - International Grains Council
**Category:** Global Supply/Demand Balances
**Authentication:** PAID
**Frequency:** MONTHLY
**URL:** `https://www.igc.int/`

**Data Available:**
- Global grain supply/demand
- Trade forecasts
- GMR (Grain Market Report)

**Implementation Notes:**
- Subscription required for full data
- Some summary data free
- Excel format exports

---

### 14. FAO-AMIS (Agricultural Market Information System)
**Category:** Global Food Security Monitoring
**Authentication:** FREE
**Frequency:** MONTHLY
**Base URL:** `https://www.amis-outlook.org/`

**Data Available:**
- G20 supply/demand balances
- Policy monitor
- Market outlook

**Implementation Notes:**
- Data visualization portal
- Some bulk downloads
- Good for global context

---

### 15. Statistics Canada
**Category:** Canada Crop Estimates, Exports
**Authentication:** FREE
**Frequency:** VARIES
**Base URL:** `https://www150.statcan.gc.ca/`

**Implementation Notes:**
- Good API documentation
- Canola, wheat, barley data
- CANSIM tables

---

### 16. SAGPYA/MAGYP - Argentina Ministry
**Category:** Argentina Official Production Data
**Authentication:** FREE
**Frequency:** MONTHLY
**URL:** `https://www.argentina.gob.ar/agricultura`

---

### 17. AGROSTAT - Ukraine Agriculture Statistics
**Category:** Ukraine Grain Production, Exports
**Authentication:** FREE
**Frequency:** VARIES
**URL:** `https://agro.me.gov.ua/`

---

### 18. ABARES - Australia
**Category:** Australia Crop Estimates
**Authentication:** FREE
**Frequency:** QUARTERLY
**URL:** `https://www.agriculture.gov.au/abares`

---

## Exchange & Market Data

### 19. CME Group Market Data
**Category:** Futures Prices, Settlement, Open Interest
**Authentication:** PAID (DataMine) / FREE (Delayed)
**Frequency:** DAILY / REALTIME

**Free Options:**
- CME Daily Bulletin (delayed, PDF)
- Settlement prices via website

**Paid Options:**
- DataMine API: Historical and real-time
- Market Data Platform: Streaming

**Third-Party Options:**
- **Databento**: `https://databento.com/` (pay-per-use)
- **Polygon.io**: Futures data
- **Alpha Vantage**: Limited futures

**Implementation Notes:**
- Free settlement prices available via web scraping
- barchart.com has delayed data
- TradingView has free charts

---

### 20. ICE Futures
**Category:** Soft Commodities, Energy
**Authentication:** PAID
**Frequency:** DAILY
**URL:** `https://www.theice.com/market-data`

**Relevant Markets:**
- Cotton, Coffee, Cocoa, Sugar
- Canola (via ICE Canada)

---

### 21. Dalian Commodity Exchange (DCE)
**Category:** China Soy, Corn, Palm Oil Futures
**Authentication:** FREE (delayed)
**Frequency:** DAILY
**URL:** `http://www.dce.com.cn/`

---

### 22. Bursa Malaysia Derivatives
**Category:** CPO Futures (FCPO)
**Authentication:** FREE (delayed)
**Frequency:** DAILY
**URL:** `https://www.bursamalaysia.com/`

---

## Industry Organizations

### 23. NOPA - National Oilseed Processors Association
**Category:** US Soybean Crush
**Authentication:** FREE
**Frequency:** MONTHLY
**URL:** `https://www.nopa.org/`

**Data Available:**
- Monthly crush statistics
- Soybean oil stocks
- Regional breakdown (some)

**Release Schedule:** ~15th of month (for prior month)

**Implementation Notes:**
- Data often reported in news (Reuters, AgriCensus)
- Historical data may require scraping
- Accessible via ag news APIs

---

### 24. RFA - Renewable Fuels Association
**Category:** US Ethanol Industry
**Authentication:** FREE
**Frequency:** VARIES
**URL:** `https://ethanolrfa.org/`

**Data Available:**
- Ethanol production capacity
- Plant locations
- Industry statistics

---

### 25. USGC - US Grains Council
**Category:** US Grain Export Programs
**Authentication:** FREE
**Frequency:** VARIES
**URL:** `https://grains.org/`

---

### 26. Nebraska Ethanol Board
**Category:** Regional Ethanol Data
**Authentication:** FREE
**URL:** `http://ethanol.nebraska.gov/`

---

## Weather & Climate Data

### 27. NOAA Climate Prediction Center
**Category:** Long-Range Forecasts
**Authentication:** FREE
**Frequency:** WEEKLY/MONTHLY
**URL:** `https://www.cpc.ncep.noaa.gov/`

**Data Available:**
- 6-10 day, 8-14 day outlooks
- Monthly/seasonal forecasts
- El Nino/La Nina monitoring

---

### 28. NASA POWER
**Category:** Agricultural Weather Data
**Authentication:** FREE
**Frequency:** DAILY
**Base URL:** `https://power.larc.nasa.gov/`

**Implementation Notes:**
- Global coverage
- Gridded data
- API available

---

### 29. Climate Hazards Group (CHIRPS)
**Category:** Rainfall Estimates
**Authentication:** FREE
**Frequency:** DAILY
**URL:** `https://www.chc.ucsb.edu/data/chirps`

---

### 30. World Weather Inc
**Category:** Agricultural Forecasts
**Authentication:** PAID
**URL:** `https://www.worldweatherinc.com/`

---

## Third-Party Aggregators

### 31. Barchart
**Category:** Comprehensive Commodity Data
**Authentication:** PAID (cmdty API)
**URL:** `https://www.barchart.com/cmdty`

**Includes:**
- USDA data
- Cash prices (basis)
- Futures
- International sources

---

### 32. DTN/Progressive Farmer
**Category:** Cash Prices, Basis
**Authentication:** PAID
**URL:** `https://www.dtn.com/`

---

### 33. AgriCensus
**Category:** Physical Market Prices, News
**Authentication:** PAID
**URL:** `https://www.agricensus.com/`

---

### 34. Refinitiv Eikon
**Category:** Comprehensive Market Data
**Authentication:** PAID
**URL:** `https://www.refinitiv.com/`

---

### 35. S&P Global Platts
**Category:** Physical Commodity Prices
**Authentication:** PAID
**URL:** `https://www.spglobal.com/platts/`

---

### 36. Gro Intelligence
**Category:** Agricultural Data Platform
**Authentication:** PAID
**URL:** `https://www.gro-intelligence.com/`

---

## Credentials Summary

### Free - No Credentials Required
| Source | Status |
|--------|--------|
| USDA FAS OpenDataWeb | Ready to implement |
| CFTC COT | Ready to implement |
| USDA FGIS Inspections | Ready to implement |
| NOAA Weather | Ready to implement |
| Drought Monitor | Ready to implement |
| CONAB Brazil | Ready to implement |
| ABIOVE Brazil | Ready to implement |
| FAO-AMIS | Ready to implement |

### Free - API Key Required
| Source | Registration URL | Status |
|--------|------------------|--------|
| USDA NASS Quick Stats | https://quickstats.nass.usda.gov/api | **NEED KEY** |
| EIA Petroleum | https://www.eia.gov/opendata/register.php | **NEED KEY** |
| Eurostat/EC AGRI | https://agridata.ec.europa.eu/ | **NEED KEY** |

### Paid Subscriptions Required
| Source | Estimated Cost | Priority |
|--------|---------------|----------|
| CME DataMine | $$$ | High (can use free delayed) |
| IGC | $$ | Medium |
| Barchart cmdty | $$$ | Low (can use free sources) |
| DTN | $$$ | Low |

### Web Scraping Required
| Source | Difficulty | Priority |
|--------|------------|----------|
| MPOB Malaysia | Medium | High |
| BCBA Argentina | Medium | Medium |
| CME Settlements (free) | Easy | High |
| NOPA (from news) | Medium | High |

---

## Implementation Priority

### Phase 1 - Immediate (Free, No Auth)
1. **CFTC COT** - Python package available (`cot_reports`)
2. **USDA FAS OpenDataWeb** - Export sales, PSD data
3. **USDA FGIS Inspections** - Text file parsing
4. **Drought Monitor** - GeoJSON downloads
5. **FAO-AMIS** - Global context

### Phase 2 - API Keys (Free Registration)
1. **USDA NASS** - Crop progress (critical for HB report)
2. **EIA** - Ethanol data (critical for corn demand)
3. **Eurostat** - EU market data

### Phase 3 - Web Scraping
1. **MPOB Malaysia** - Palm oil fundamentals
2. **CME Settlements** - Daily prices
3. **CONAB/ABIOVE Brazil** - South America supply
4. **NOPA** - US crush data

### Phase 4 - Paid Sources (Optional)
1. **CME DataMine** - If real-time needed
2. **Barchart** - If consolidated view needed
3. **IGC** - For global balances

---

## Collector Status Tracker

**Last Audit: December 19, 2025**

### US Government Sources

| Source | Collector File | Status | API Key | Notes |
|--------|---------------|--------|---------|-------|
| USDA NASS Quick Stats | `usda_nass_collector.py` | ✅ COMPLETE | 🔑 Free | Crop progress, stocks, production |
| USDA FAS Export Sales | `usda_fas_collector.py` | ✅ COMPLETE | None | Weekly export commitments |
| USDA FAS PSD | `usda_fas_collector.py` | ✅ COMPLETE | None | Global S&D balances |
| USDA AMS Market News | `usda_ams_collector.py` | ✅ COMPLETE | None | Tallow, DDGS, corn oil prices |
| USDA ERS Feed Grains | `usda_ers_collector.py` | ✅ COMPLETE | None | Historical corn S&D |
| USDA ERS Oil Crops | - | ❌ MISSING | None | **GAP: Needs collector** |
| USDA ERS Wheat | - | ❌ MISSING | None | **GAP: Needs collector** |
| USDA WASDE | - | ❌ MISSING | None | **GAP: Key monthly S&D** |
| USDA FGIS Inspections | `export_inspections_agent/` | ✅ COMPLETE | None | Full agent with database |
| EIA Ethanol | `eia_ethanol_collector.py` | ✅ COMPLETE | 🔑 Free | Production, stocks, blending |
| EIA Petroleum | `eia_petroleum_collector.py` | ✅ COMPLETE | 🔑 Free | Energy prices |
| EPA RFS/EMTS | `epa_rfs_collector.py` | ⚠️ PARTIAL | None | Excel parsing fragile |
| CFTC COT | `cftc_cot_collector.py` | ✅ COMPLETE | None | Managed money positions |
| US Drought Monitor | `drought_collector.py` | ✅ COMPLETE | None | Weekly drought conditions |
| Census Trade | `census_trade_collector.py` | ✅ COMPLETE | Optional | Import/export by HS code |

### International Sources - South America

| Source | Collector File | Status | API Key | Notes |
|--------|---------------|--------|---------|-------|
| Brazil CONAB | `conab_collector.py` | ✅ COMPLETE | None | Official crop estimates |
| Brazil ABIOVE | `abiove_collector.py` | ✅ COMPLETE | None | Soy crush data |
| Brazil IBGE SIDRA | `ibge_sidra_collector.py` | ✅ COMPLETE | None | Production statistics |
| Brazil IMEA | `imea_collector.py` | ✅ COMPLETE | None | Mato Grosso regional |
| Brazil ANEC | `south_america_trade_data/` | ✅ COMPLETE | None | Port lineup data |
| Brazil Comex Stat | `south_america_trade_data/` | ✅ COMPLETE | None | Trade by HS code |
| Argentina MAGyP | `magyp_collector.py` | ✅ COMPLETE | None | Ministry crop data |
| Argentina INDEC | `south_america_trade_data/` | ✅ COMPLETE | None | Trade statistics |
| Argentina Rosario | - | ❌ MISSING | None | **GAP: Exchange prices** |
| Paraguay | `south_america_trade_data/` | ✅ COMPLETE | None | Regional trade |
| Uruguay | `south_america_trade_data/` | ✅ COMPLETE | None | Regional trade |

### International Sources - Canada

| Source | Collector File | Status | API Key | Notes |
|--------|---------------|--------|---------|-------|
| Canadian Grain Commission | `canada_cgc_collector.py` | ✅ COMPLETE | None | Visible supply, deliveries |
| Statistics Canada | `canada_statscan_collector.py` | ✅ COMPLETE | None | Official stats via CANSIM |

### International Sources - Other

| Source | Collector File | Status | API Key | Notes |
|--------|---------------|--------|---------|-------|
| MPOB Malaysia | `mpob_collector.py` | ⚠️ PARTIAL | None | Scraping fragile |
| Indonesia GAPKI | - | ❌ MISSING | None | **GAP: Indonesia palm** |
| FAOSTAT | `faostat_collector.py` | ✅ COMPLETE | None | Global production/trade |
| Eurostat | - | ❌ MISSING | None | **GAP: EU data** |
| EU MARS Bulletin | - | ❌ MISSING | None | **GAP: EU crop conditions** |
| Australia ABARES | - | ❌ MISSING | None | **GAP: Australia S&D** |
| Ukraine AgMin | - | ❌ MISSING | None | **GAP: Ukraine exports** |
| Russia SovEcon | - | ❌ MISSING | 💰 | **GAP: Russia data** |
| China GACC | - | ❌ MISSING | 💰 | **GAP: China imports** |

### Exchange/Price Data

| Source | Collector File | Status | API Key | Notes |
|--------|---------------|--------|---------|-------|
| CME Settlements | `cme_settlements_collector.py` | ✅ COMPLETE | None | Free delayed data |
| Interactive Brokers | `ibkr_collector.py` | ✅ COMPLETE | Account | Historical OHLC |
| TradeStation | `tradestation_collector.py` | ✅ COMPLETE | Account | OAuth required |
| Unified Futures | `futures_data_collector.py` | ✅ COMPLETE | Either | Auto fallback |

### Industry/Trade Associations

| Source | Collector File | Status | API Key | Notes |
|--------|---------------|--------|---------|-------|
| **NOPA (US Soy Crush)** | - | ❌ MISSING | None | **CRITICAL GAP** |
| UN Comtrade | - | ❌ MISSING | 🔑 Free | **GAP: Global trade** |

---

## Gap Analysis by Commodity

### 🌽 CORN Balance Sheet Coverage

| Data Point | US | Brazil | Argentina | EU | Ukraine | China |
|------------|:--:|:------:|:---------:|:--:|:-------:|:-----:|
| Production | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ |
| Stocks | ✅ | ⚠️ | ⚠️ | ❌ | ❌ | ❌ |
| Exports | ✅ | ✅ | ✅ | ❌ | ❌ | N/A |
| Ethanol Use | ✅ | ⚠️ | N/A | ❌ | N/A | N/A |
| Crop Progress | ✅ | ✅ | ❌ | ❌ | ❌ | ❌ |

**Priority Corn Gaps:** WASDE, Ukraine, EU Eurostat

### 🌾 WHEAT Balance Sheet Coverage

| Data Point | US | Canada | EU | Russia | Ukraine | Australia | Argentina |
|------------|:--:|:------:|:--:|:------:|:-------:|:---------:|:---------:|
| Production | ✅ | ✅ | ❌ | ❌ | ❌ | ❌ | ✅ |
| By Class | ✅ | ✅ | ❌ | ❌ | ❌ | ❌ | ❌ |
| Stocks | ✅ | ✅ | ❌ | ❌ | ❌ | ❌ | ⚠️ |
| Exports | ✅ | ✅ | ❌ | ❌ | ❌ | ❌ | ✅ |

**Priority Wheat Gaps:** ERS Wheat Yearbook, Russia, EU, Australia, Ukraine

### 🫘 SOYBEANS Balance Sheet Coverage

| Data Point | US | Brazil | Argentina | China |
|------------|:--:|:------:|:---------:|:-----:|
| Production | ✅ | ✅ | ✅ | ❌ |
| Crush | ❌ | ✅ | ❌ | ❌ |
| Exports | ✅ | ✅ | ✅ | N/A |
| Imports | ✅ | N/A | N/A | ❌ |

**Priority Soybean Gaps:** NOPA (US Crush), China GACC imports, Argentina crush

---

## Immediate Action Items

### 1. Get Free API Keys (30 minutes)
- [ ] USDA NASS: https://quickstats.nass.usda.gov/api
- [ ] EIA: https://www.eia.gov/opendata/register.php
- [ ] Add to `.env` file

### 2. Build Critical Missing Collectors
- [ ] **NOPA US Crush** - Monthly soybean crush (Priority 1)
- [ ] **USDA WASDE** - Monthly S&D report (Priority 1)
- [ ] **USDA ERS Wheat/Oil Crops** - Extend existing ERS collector (Priority 2)

### 3. Test Existing Collectors
- [ ] Run USDA FAS collector (no key needed)
- [ ] Run CFTC COT collector (no key needed)
- [ ] Verify database storage working

---

## Notes

### Data Quality Considerations
- Government sources (USDA, EIA) are authoritative
- Industry sources (NOPA) may have reporting lags
- Third-party aggregators add convenience but cost
- Web-scraped data needs validation

### Legal/Terms of Service
- Always check terms of service before scraping
- Some sites explicitly prohibit scraping
- Government data is generally public domain
- Exchange data often has redistribution restrictions

### Update Scheduling
- USDA reports have set release schedules
- Plan data collection around report timing
- Build in retry logic for API failures
- Consider timezone differences for international sources
