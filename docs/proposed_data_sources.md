# Proposed Additional Data Sources for RLC

**Generated:** January 19, 2026

This document outlines potential data sources to complement your existing commodity data infrastructure. Sources are organized by category and priority.

---

## Currently Documented Sources (For Reference)

| Source | Category | Status |
|--------|----------|--------|
| USDA WASDE | USDA-OCE | Configured |
| Export Sales | USDA-FAS | Configured |
| Export Inspections | USDA-AMS | Configured |
| Grain Stocks | USDA-NASS | Configured |
| Prospective Plantings | USDA-NASS | Configured |
| Acreage | USDA-NASS | Configured |
| Crop Progress | USDA-NASS | Configured |
| Cattle on Feed | USDA-NASS | Configured |
| Feed Grains Outlook | USDA-ERS | Configured |
| Oil Crops Outlook | USDA-ERS | Configured |
| EIA Petroleum Status | EIA | Configured |
| EIA Monthly Biodiesel | EIA | Configured |
| Census Trade | Census | Configured |
| CONAB Brazil | CONAB | Configured |
| CME Settlements | CME | Configured |
| NOPA Crush | NOPA | Configured |

---

## PROPOSED NEW SOURCES

### 1. USDA Additional APIs (High Priority)

#### USDA-FAS Production, Supply, Distribution (PSD) Database
- **URL:** https://apps.fas.usda.gov/opendatawebv2/
- **Data:** Global supply/demand for all major commodities by country
- **Access:** Free API - JSON responses
- **Value:** Country-level production, imports, exports, stocks for grains/oilseeds worldwide
- **Update:** Updated with each WASDE release

#### USDA NASS Quick Stats API
- **URL:** https://www.nass.usda.gov/developer/index.php
- **Data:** County-level yields, acreage, production for corn, soybeans, wheat
- **Access:** Free API with key
- **Value:** Granular US production data for crop modeling
- **Update:** Varies by survey

#### USDA AMS Grain Transportation (AgTransport)
- **URL:** https://agtransport.usda.gov/
- **Data:** Barge rates, rail rates, truck rates, export pace
- **Access:** Free - CSV/API available
- **Value:** Logistics/basis analysis for grain movement
- **Update:** Weekly

---

### 2. Ethanol & Biofuels (High Priority)

#### EIA Weekly Ethanol Production
- **URL:** https://www.eia.gov/dnav/pet/pet_pnp_wprode_s1_w.htm
- **Data:** Weekly ethanol production, stocks, imports
- **Access:** Free - downloadable data
- **Value:** Critical for corn demand modeling
- **Update:** Weekly (Wednesday)

#### Renewable Fuels Association (RFA) Supply/Demand
- **URL:** https://ethanolrfa.org/markets-and-statistics/weekly-and-monthly-ethanol-supply-and-demand
- **Data:** Weekly and monthly ethanol S&D with detailed breakdowns
- **Access:** Free
- **Value:** Industry perspective on ethanol markets
- **Update:** Weekly

#### EPA RIN Prices (D4/D6)
- **URL:** https://www.epa.gov/fuels-registration-reporting-and-compliance-help/rin-trades-and-price-information
- **Data:** RIN credit prices for biodiesel (D4), ethanol (D6)
- **Access:** Free via EPA EMTS
- **Value:** Essential for biofuel economics/margins
- **Update:** Weekly

---

### 3. International Sources (High Priority)

#### ABIOVE - Brazil Soybean Industry
- **URL:** https://abiove.org.br/statistics/
- **Data:** Brazilian soybean crush, meal/oil production, exports
- **Access:** Free - monthly reports (PDF/Excel)
- **Value:** Brazil is #1 soybean producer; crush data critical
- **Update:** Monthly (around 12th)

#### Bolsa de Comercio de Rosario (BCR) - Argentina
- **URL:** https://www.bcr.com.ar/
- **Data:** Argentine corn/soy production estimates, harvest progress
- **Access:** Free reports (may need translation)
- **Value:** Argentina is #3 corn, #3 soybean exporter
- **Update:** Weekly during harvest, monthly otherwise

#### Canadian Grain Commission (CGC)
- **URL:** https://www.grainscanada.gc.ca/en/grain-research/statistics/
- **Data:** Canadian grain/canola exports, handling, quality
- **Access:** Free - CSV files via Open Government Portal
- **Value:** Canada is top canola/wheat exporter
- **Update:** Weekly

#### Eurostat - EU Crop Statistics
- **URL:** https://ec.europa.eu/eurostat/statistics-explained/index.php?title=Agricultural_production_-_crops
- **Data:** EU cereals and oilseeds production, area, yields
- **Access:** Free API available
- **Value:** EU is major wheat, barley, rapeseed producer
- **Update:** Monthly/Quarterly

#### Ukraine Grain Exports
- **URL:** https://www.ams.usda.gov/services/transportation-analysis/ukraine
- **Data:** Ukraine grain transportation, export volumes, shipping corridors
- **Access:** Free (USDA AMS)
- **Value:** Major wheat/corn exporter; geopolitical sensitivity
- **Update:** Monthly reports

---

### 4. Market Data & Positioning (Medium-High Priority)

#### CFTC Commitments of Traders (COT)
- **URL:** https://publicreporting.cftc.gov/stories/s/r4w3-av2u
- **Data:** Futures positioning by trader category (managed money, commercials, etc.)
- **Access:** Free API via CFTC Public Reporting
- **Value:** Sentiment indicator for grains, oilseeds, livestock
- **Update:** Weekly (Friday, as of Tuesday)

#### ICE Futures US Agriculture
- **URL:** https://www.ice.com/agriculture
- **Data:** Sugar, cotton, coffee, cocoa, canola futures
- **Access:** Subscription required for real-time; delayed free
- **Value:** Expands coverage to soft commodities
- **Update:** Daily settlements available

---

### 5. Freight & Logistics (Medium Priority)

#### Baltic Dry Index (BDI)
- **URL:** https://www.balticexchange.com/ (subscription) or https://tradingeconomics.com/commodity/baltic (free delayed)
- **Data:** Dry bulk shipping rates (Capesize, Panamax, Supramax)
- **Access:** Subscription for real-time; free delayed available
- **Value:** Global grain shipping cost indicator
- **Update:** Daily

#### USDA AMS Grain Transportation Report (GTR)
- **URL:** https://www.ams.usda.gov/services/transportation-analysis/gtr-datasets
- **Data:** Barge, rail, truck, ocean freight rates
- **Access:** Free
- **Value:** Comprehensive US ag transportation costs
- **Update:** Weekly

---

### 6. Sustainable Aviation Fuel (SAF) (Medium Priority)

#### EIA Other Biofuels / SAF
- **URL:** https://www.eia.gov/todayinenergy/detail.php?id=65204
- **Data:** SAF production capacity, projections
- **Access:** Free
- **Value:** Growing market affecting soybean oil demand
- **Update:** Monthly (in STEO)

#### EPA EMTS - Renewable Jet Fuel (RJF)
- **URL:** https://www.epa.gov/fuels-registration-reporting-and-compliance-help/public-data-renewable-fuel-standard
- **Data:** SAF volumes registered under RFS
- **Access:** Free
- **Value:** Tracks SAF production/consumption
- **Update:** Monthly

---

### 7. Weather (Medium Priority - Complement to Meteorologist Emails)

#### NOAA Climate Prediction Center
- **URL:** https://www.cpc.ncep.noaa.gov/
- **Data:** 6-10 day, 8-14 day outlooks; drought monitor
- **Access:** Free
- **Value:** Official US weather forecasts for crop areas
- **Update:** Daily/Weekly

#### Open-Meteo API
- **URL:** https://open-meteo.com/
- **Data:** Historical and forecast weather data globally
- **Access:** Free API (no key required for basic use)
- **Value:** Programmatic weather data for any location
- **Update:** Hourly/Daily

---

## Implementation Priority Matrix

| Priority | Source | Effort | Value |
|----------|--------|--------|-------|
| **P0** | USDA PSD API | Low | High - Global S&D |
| **P0** | EIA Weekly Ethanol | Low | High - Corn demand |
| **P0** | EPA RIN Prices | Low | High - Biofuel margins |
| **P0** | CFTC COT Reports | Low | High - Positioning |
| **P1** | ABIOVE Brazil | Medium | High - Brazil crush |
| **P1** | USDA AgTransport | Low | Medium - Freight |
| **P1** | Canadian CGC | Low | Medium - Canola/Wheat |
| **P1** | NASS Quick Stats | Medium | Medium - County data |
| **P2** | Argentina BCR | Medium | Medium - SA production |
| **P2** | Eurostat | Medium | Medium - EU data |
| **P2** | Baltic Dry Index | Medium | Medium - Ocean freight |
| **P2** | SAF/EPA EMTS | Low | Medium - Emerging |
| **P3** | ICE Softs | High | Lower priority |
| **P3** | Ukraine exports | Low | Situational |

---

## Recommended First Actions

1. **Add USDA PSD API** - Single API gives global supply/demand for all commodities
2. **Add EIA Ethanol Weekly** - Direct CSV download, critical for corn demand
3. **Add EPA RIN Prices** - Free, essential for biofuel economics
4. **Add CFTC COT** - Python library available (`cot_reports`), weekly positioning data
5. **Add ABIOVE monthly report scraper** - Brazil soy crush is market-moving

---

## Notes

- Most US government sources (USDA, EIA, EPA, CFTC, Census) have free APIs or downloadable data
- International sources (ABIOVE, BCR, CGC) often require scraping PDFs or Excel files
- Commercial data (OPIS, Baltic Exchange, ICE real-time) requires subscriptions
- Weather data can complement meteorologist emails with programmatic access

---

*This list is not exhaustive but covers the most valuable additions based on your current commodity focus (grains, oilseeds, biofuels, livestock).*
