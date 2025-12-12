# South American Commodity Data Sources

## Overview

This document catalogs all identified data sources for South American agricultural commodities, including API access information, registration requirements, and implementation status.

### Target Countries
- **Primary**: Brazil, Argentina, Colombia, Paraguay, Uruguay
- **Secondary**: Bolivia, Chile, Peru (for completeness)

### Target Commodities
- **Grains**: Corn, Wheat (HRW/HRS/SRW/Durum), Sorghum, Barley, Oats, Rice
- **Oilseeds**: Soybeans, Soybean Meal, Soybean Oil, Sunflower, Canola
- **Industrial**: Cotton, Sugar
- **Biofuels**: Ethanol, Biodiesel

---

## BRAZIL DATA SOURCES

### 1. CONAB (Companhia Nacional de Abastecimento)
**National Supply Company - Crop Estimates & Supply/Demand**

| Field | Details |
|-------|---------|
| **URL** | https://www.conab.gov.br |
| **Data Portal** | https://portaldeinformacoes.conab.gov.br/download-arquivos.html |
| **API Type** | File downloads (CSV/Excel) |
| **Auth Required** | No |
| **Update Frequency** | Monthly (grain/fiber surveys) |
| **Data Lag** | Real-time during harvest season |

**Available Data:**
- Grain & fiber harvest estimates (safras)
- Supply & demand balances (Oferta e Demanda)
- Agricultural prices (weekly/monthly by state)
- Sugarcane & coffee assessments (quarterly)
- Historical production data

**Key URLs:**
- Safras (Crop Estimates): https://www.conab.gov.br/info-agro/safras
- Supply/Demand: https://www.conab.gov.br/info-agro/oferta-e-demanda-agropecuaria
- Download Portal: https://portaldeinformacoes.conab.gov.br/download-arquivos.html

**Contact:** geasa@conab.gov.br, geote@conab.gov.br

---

### 2. ABIOVE (Brazilian Vegetable Oil Industry Association)
**Soybean Complex Statistics**

| Field | Details |
|-------|---------|
| **URL** | https://abiove.org.br/en/ |
| **Statistics Portal** | https://abiove.org.br/statistics/ |
| **API Type** | Web downloads (Excel/PDF) |
| **Auth Required** | No |
| **Update Frequency** | Monthly |
| **Data Lag** | 1 month |

**Available Data:**
- Monthly soybean crush volumes
- Soybean oil & meal production
- Processing capacity by state/region
- Export volumes (soybean complex)
- Supply/demand balance forecasts

**Implementation Notes:**
- Data in Excel/PDF format, requires scraping
- Monthly S&D balance is key data point
- Source uses Comex Stat for trade data

---

### 3. IBGE (Instituto Brasileiro de Geografia e Estatistica)
**Brazilian Institute of Geography and Statistics**

| Field | Details |
|-------|---------|
| **URL** | https://www.ibge.gov.br |
| **API Base** | https://servicodados.ibge.gov.br/api/v1/ |
| **SIDRA API** | https://sidra.ibge.gov.br/ |
| **Auth Required** | No |
| **Update Frequency** | Varies by survey |
| **Data Lag** | Monthly to Annual |

**Available Data:**
- PAM (Municipal Agricultural Production) - Annual
- LSPA (Systematic Survey of Agricultural Production) - Monthly
- Agricultural Census data
- Geographic data for Brazil

**API Endpoints:**
- `/datasets` - List available datasets
- `/geography` - Geographic information
- SIDRA tables for agricultural production

---

### 4. EMBRAPA AgroAPI
**Brazilian Agricultural Research Corporation**

| Field | Details |
|-------|---------|
| **URL** | https://www.embrapa.br/agroapi |
| **API Type** | REST API |
| **Auth Required** | Yes (free registration) |
| **Registration** | https://www.embrapa.br/agroapi |

**Available APIs:**
- **ClimAPI**: Climate forecasting for agriculture
- **SATVeg API**: NDVI/EVI vegetation indices (2000-present)
- **Agrofit API**: Registered agricultural products database
- **Bioinsumos API**: Bioinputs information
- **AgroTermos API**: Agricultural terminology

---

### 5. CEPEA (Center for Advanced Studies on Applied Economics)
**Agricultural Prices - ESALQ/USP**

| Field | Details |
|-------|---------|
| **URL** | https://www.cepea.esalq.usp.br |
| **API Type** | No direct API |
| **Data Access** | Via commercial providers (Barchart, LSEG, CEIC) |
| **Update Frequency** | Daily |
| **Data Lag** | Real-time |

**Available Data:**
- Daily cash prices for grains, livestock, sugar, coffee
- FOB and premium prices
- ESALQ reference prices (key benchmarks)
- Weekly and monthly aggregates

**Commercial Access:**
- Barchart Commodities: https://www.barchart.com/cmdty/data/fundamental/explore/CEPEA
- LSEG Data: https://www.lseg.com/en/data-analytics/financial-data/commodities-data/agricultural-data/esalq

---

### 6. ANEC (National Association of Grain Exporters)
**Brazil Export Lineups & Shipping Forecasts**

| Field | Details |
|-------|---------|
| **URL** | https://www.anec.com.br |
| **API Type** | PDF reports / press releases |
| **Auth Required** | No |
| **Update Frequency** | Weekly |
| **Data Lag** | Real-time |

**Available Data:**
- Weekly export forecasts (soybeans, corn, meal)
- Port lineup data
- Monthly export totals
- Shipping schedules

**Implementation Notes:**
- PDF parsing required
- Data widely republished by news agencies
- USDA AMS provides alternative Brazil shipping data

---

### 7. B3 (Brasil Bolsa Balcao)
**Brazilian Exchange - Commodity Futures**

| Field | Details |
|-------|---------|
| **URL** | https://www.b3.com.br |
| **API Type** | ICE Connect / Commercial |
| **Auth Required** | Yes (paid subscription) |
| **Update Frequency** | Real-time |

**Available Data:**
- Agricultural futures (soybeans, corn, coffee, sugar, cattle)
- Settlement prices
- Open interest
- Historical data

**Access Options:**
- ICE Connect Desktop
- ICE XL (Excel integration)
- ICE APIs (enterprise)

---

## ARGENTINA DATA SOURCES

### 1. MAGyP (Ministry of Agriculture, Livestock and Fisheries)
**Government Production Statistics**

| Field | Details |
|-------|---------|
| **URL** | https://www.magyp.gob.ar |
| **Open Data** | https://www.magyp.gob.ar/datosabiertos/ |
| **Dataset Portal** | https://datos.magyp.gob.ar/dataset |
| **API Type** | Open Data Portal |
| **Auth Required** | No |
| **Update Frequency** | Monthly |

**Available Data:**
- Stored grain inventories (monthly)
- Production estimates
- Price information
- Agricultural census data

---

### 2. Bolsa de Cereales de Buenos Aires (BCBA)
**Buenos Aires Grain Exchange**

| Field | Details |
|-------|---------|
| **URL** | https://www.bolsadecereales.com |
| **Reports** | https://www.bolsadecereales.com/estimaciones-informes |
| **API Type** | Web reports / No public API |
| **Auth Required** | No (for reports) |
| **Update Frequency** | Weekly/Monthly |

**Available Data:**
- Weekly crop progress reports
- Production estimates (soybeans, corn, wheat, sunflower, barley)
- Planted area estimates
- Harvest progress

---

### 3. Bolsa de Comercio de Rosario (BCR)
**Rosario Board of Trade**

| Field | Details |
|-------|---------|
| **URL** | https://www.bcr.com.ar |
| **API Type** | No public API |
| **Auth Required** | N/A |
| **Update Frequency** | Weekly/Monthly |

**Available Data:**
- Grain production estimates
- Reference prices (most important physical grain market in Argentina)
- Trade statistics
- Monthly grain reports

**Context:**
- 80%+ of Argentina's vegetable oil processing capacity in Rosario region
- 90%+ of soybean exports through Rosario/San Lorenzo ports
- Reference prices for national and international markets

---

### 4. MATBA ROFEX
**Argentine Futures Exchange**

| Field | Details |
|-------|---------|
| **URL** | https://www.matbarofex.com.ar |
| **API Type** | ICE Data Services / OpenDataDSL |
| **Auth Required** | Yes (paid) |
| **Update Frequency** | Real-time / Daily |

**Available Data:**
- Futures prices (soybeans, corn, wheat, sunflower, sorghum, barley)
- Soybean oil futures
- Options data
- Settlement values

**Products Traded:**
- Futures: 100 MT contract size
- Options: American style
- ICA MATba (Argentine Commodities Index)

**Access via ICE:**
- ICE Developer Portal: https://developer.ice.com/fixed-income-data-services/catalog/matba-rofex

---

### 5. INDEC (National Statistics Institute)
**Trade Data - Already Implemented**

| Field | Details |
|-------|---------|
| **URL** | https://www.indec.gob.ar |
| **API Type** | CSV/XLS downloads |
| **Auth Required** | No |
| **Status** | ✅ Implemented |

---

## COLOMBIA DATA SOURCES

### 1. DANE (National Statistics Department)
**National Agricultural Survey**

| Field | Details |
|-------|---------|
| **URL** | https://www.dane.gov.co |
| **ENA Survey** | https://www.dane.gov.co/index.php/en/statistics-by-topic-1/agricultural-sector/national-agricultural-survey-ena |
| **Socrata API** | https://datos.gov.co |
| **Auth Required** | Optional (higher rate limits with token) |
| **Update Frequency** | Annual survey |
| **Status** | ✅ Trade data implemented |

**Available Data:**
- Agricultural production by crop
- Planted/harvested area
- Livestock statistics
- Agricultural census

---

## URUGUAY DATA SOURCES

### 1. MGAP DIEA (Agricultural Statistics Directorate)
**Ministry of Livestock, Agriculture and Fishing**

| Field | Details |
|-------|---------|
| **URL** | https://www.gub.uy/ministerio-ganaderia-agricultura-pesca/tematica/diea |
| **Downloads** | https://descargas.mgap.gub.uy/DIEA/Anuarios/ |
| **API Type** | PDF/Excel downloads |
| **Auth Required** | No |
| **Update Frequency** | Annual yearbook, periodic surveys |
| **Status** | ✅ Trade data implemented |

**Available Data:**
- Agricultural Statistics Yearbook
- Soybean/corn planted area estimates
- Production surveys
- Land lease data

---

## PARAGUAY DATA SOURCES

### 1. CAPECO (Chamber of Grain Exporters)
**Industry Production Data**

| Field | Details |
|-------|---------|
| **URL** | http://www.capeco.org.py |
| **API Type** | Reports / No API |
| **Auth Required** | No |

**Available Data:**
- Crop area and production estimates
- Export statistics
- Industry reports

### 2. SENAVE (Plant Health Service)
**National Plant and Seed Health Service**

| Field | Details |
|-------|---------|
| **URL** | https://www.senave.gov.py |
| **API Type** | No public API |

**Available Data:**
- Seed quality data
- Phytosanitary certificates
- Biotech approvals

---

## GLOBAL/SUPPLEMENTARY DATA SOURCES

### 1. UN Comtrade
**International Trade Statistics**

| Field | Details |
|-------|---------|
| **URL** | https://comtrade.un.org |
| **API** | https://comtrade.un.org/data/doc/api/ |
| **Auth Required** | Optional (higher limits with token) |
| **Free Limits** | 500 records/call without key, 100,000 with key |
| **Registration** | https://uncomtrade.org/docs/premium-subscribers/ |

**Access Tiers:**
- Without token: Unlimited calls, 500 records/call
- With token: 500 calls/day, 100,000 records/call
- Premium: Bulk and async downloads

---

### 2. FAOSTAT
**FAO Food & Agriculture Statistics**

| Field | Details |
|-------|---------|
| **URL** | https://www.fao.org/faostat/en/ |
| **API** | REST API via bulk downloads |
| **Python Package** | `pip install faostat` |
| **R Package** | FAOSTAT on CRAN |
| **Auth Required** | No |
| **Update Frequency** | Annual (historical) |

**Available Data:**
- Production quantities (1961-present)
- Trade flows
- Food balances
- Prices
- Land use
- Emissions

**Data Codes:**
- Production: https://www.fao.org/faostat/en/#data/QCL
- Trade: https://www.fao.org/faostat/en/#data/TCL

---

### 3. IGC (International Grains Council)
**Global Grain Market Analysis**

| Field | Details |
|-------|---------|
| **URL** | https://www.igc.int |
| **API Type** | Subscription-based |
| **Auth Required** | Yes (paid) |

**Available Data:**
- World Grain Statistics (Excel, annual)
- Grain Shipments report
- Grains and Oilseeds Index (GOI) - publicly available
- Supply/demand projections

---

### 4. Nasdaq Data Link (Quandl)
**Commodity & Financial Data**

| Field | Details |
|-------|---------|
| **URL** | https://data.nasdaq.com |
| **API Docs** | https://docs.data.nasdaq.com |
| **Registration** | https://data.nasdaq.com/sign-up |
| **Auth Required** | Yes (free tier available) |

**Available Data:**
- Free commodity datasets from various sources
- Premium agricultural data
- Historical futures prices

---

## WHEAT TENDER MARKET MONITORING

### Key Importing Countries & Agencies

| Country | Agency | Tender Frequency |
|---------|--------|------------------|
| **Egypt** | GASC → Mostakbal Misr | Every 10-12 days (Jun-Feb) |
| **Algeria** | OAIC | Regular tenders |
| **Tunisia** | State Grains Agency | Periodic |
| **Morocco** | ONICL | Periodic |
| **Saudi Arabia** | SAGO | Regular |
| **Iraq** | Grain Board of Iraq | Regular |

### Monitoring Sources

1. **News Services:**
   - Reuters (primary source)
   - Bloomberg
   - AgroChart: https://www.agrochart.com/en/news/
   - Agricensus: https://www.agricensus.com
   - Commodity3: https://www.commodity3.com

2. **Aggregation Services:**
   - APK-Inform: https://www.apk-inform.com
   - CMNavigator: https://cmnavigator.com

3. **Approach for Automation:**
   - Web scraping of news feeds
   - RSS feed monitoring
   - Email alert subscriptions
   - API integration with news services (paid)

---

## IMPLEMENTATION STATUS

| Source | Region | Status | Priority |
|--------|--------|--------|----------|
| CONAB | Brazil | 🔨 Building | High |
| ABIOVE | Brazil | 🔨 Building | High |
| IBGE SIDRA | Brazil | 🔨 Building | Medium |
| CEPEA | Brazil | ⏳ Planned | Medium |
| ANEC | Brazil | ⏳ Planned | Medium |
| MAGyP | Argentina | 🔨 Building | High |
| BCBA | Argentina | 🔨 Building | High |
| BCR | Argentina | ⏳ Planned | Medium |
| Comex Stat (Brazil) | Brazil | ✅ Done | - |
| INDEC | Argentina | ✅ Done | - |
| DANE Socrata | Colombia | ✅ Done | - |
| Uruguay DNA | Uruguay | ✅ Done | - |
| Paraguay WITS | Paraguay | ✅ Done | - |
| FAOSTAT | Global | 🔨 Building | Medium |
| UN Comtrade | Global | 🔨 Building | Medium |

---

## API REGISTRATION LINKS

| Service | Registration URL |
|---------|-----------------|
| UN Comtrade | https://comtrade.un.org/data/ |
| EMBRAPA AgroAPI | https://www.embrapa.br/agroapi |
| Nasdaq Data Link | https://data.nasdaq.com/sign-up |
| DANE Socrata | https://www.datos.gov.co/signup |

---

*Document generated: December 2025*
*Last updated: December 2025*
