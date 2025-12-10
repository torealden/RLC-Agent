# North America Agricultural Data Source Registry

**Version:** 2.0
**Last Updated:** December 10, 2025
**Purpose:** Comprehensive registry of data sources for trade-flow-based fundamental analysis of agricultural commodities in North America

---

## Table of Contents

1. [Trade Flow Data Sources](#1-trade-flow-data-sources)
2. [Feed Grains](#2-feed-grains)
3. [Food Grains (Wheat)](#3-food-grains-wheat)
4. [Oilseeds & Products](#4-oilseeds--products)
5. [Tropical Oils (Palm, Coconut, Lauric)](#5-tropical-oils)
6. [Biofuels](#6-biofuels)
7. [Biofuel Feedstocks](#7-biofuel-feedstocks)
8. [Energy](#8-energy)
9. [Co-Products & By-Products](#9-co-products--by-products)
10. [Input Costs](#10-input-costs)
11. [Futures & Cash Prices](#11-futures--cash-prices)
12. [Weather & Crop Conditions](#12-weather--crop-conditions)

---

## 1. Trade Flow Data Sources

### Global Trade Aggregators

| Source | Coverage | Auth | URL | Notes |
|--------|----------|------|-----|-------|
| **UN Comtrade** | 200+ countries, HS codes | API Key (Free) | https://comtrade.un.org/data/doc/api/ | Best global coverage, Python/R packages available |
| **USDA GATS** | Agricultural trade, all countries | API Key (Free) | https://apps.fas.usda.gov/gats/ | USDA commodity groupings |
| **USDA FAS OpenDataWeb** | Export sales, PSD | FREE | https://apps.fas.usda.gov/opendatawebV2/ | Weekly export sales by destination |

### US Trade Data

| Source | Coverage | Auth | URL | Notes |
|--------|----------|------|-----|-------|
| **US Census Bureau** | All US imports/exports by HS | API Key (Free) | https://www.census.gov/data/developers/data-sets/international-trade.html | Monthly, 10-digit HTS codes |
| **USDA FATUS** | Agricultural trade aggregations | FREE | https://www.ers.usda.gov/data-products/fatus/ | 4,000+ HTS codes aggregated |
| **USITC DataWeb** | Tariff and trade data | FREE | https://dataweb.usitc.gov/ | Tariff schedules included |

### Canada Trade Data

| Source | Coverage | Auth | URL | Notes |
|--------|----------|------|-----|-------|
| **Statistics Canada** | Canadian imports/exports | FREE | https://www150.statcan.gc.ca/t1/tbl1/en/tv.action?pid=3210000801 | Grain exports by destination |
| **Canadian Grain Commission** | Weekly grain movement | FREE | https://www.grainscanada.gc.ca/en/grain-research/statistics/grain-statistics-weekly/ | CSV downloads |
| **Open Government Portal** | Trade datasets | FREE | https://open.canada.ca/data/ | Multiple ag datasets |

### Mexico Trade Data

| Source | Coverage | Auth | URL | Notes |
|--------|----------|------|-----|-------|
| **INEGI** | Mexican trade statistics | FREE | https://www.inegi.org.mx/ | Spanish, some English |
| **SIAP** | Ag production/trade | FREE | https://www.gob.mx/siap | Mexican ag ministry |

---

## 2. Feed Grains

**Commodities:** Corn, Sorghum, Barley, Oats

### Supply & Demand

| Source | Data Type | Frequency | Auth | URL |
|--------|-----------|-----------|------|-----|
| **USDA ERS Feed Grains Database** | S&D, prices, stocks | Monthly | FREE | https://www.ers.usda.gov/data-products/feed-grains-database/ |
| **USDA WASDE** | Supply/demand projections | Monthly | FREE | https://www.usda.gov/oce/commodity/wasde |
| **USDA NASS** | Production, acreage, stocks | Varies | API Key | https://quickstats.nass.usda.gov/api |
| **USDA FAS PSD** | Global S&D by country | Monthly | FREE | https://apps.fas.usda.gov/psdonline/ |

### Trade Flows

| Source | Data Type | Frequency | Auth | URL |
|--------|-----------|-----------|------|-----|
| **USDA FAS Export Sales** | Weekly export sales by destination | Weekly (Thu) | FREE | https://apps.fas.usda.gov/OpenData/api/esr/exports |
| **FGIS Export Inspections** | Weekly inspections by commodity | Weekly (Mon) | FREE | https://www.ams.usda.gov/mnreports/gx_gr110.txt |
| **US Census** | Monthly imports/exports | Monthly | API Key | Census API |

### Positioning

| Source | Data Type | Frequency | Auth | URL |
|--------|-----------|-----------|------|-----|
| **CFTC COT** | Managed Money, Commercial positions | Weekly (Fri) | FREE | https://publicreporting.cftc.gov/ |

### Marketing Year Reference
- **Corn/Sorghum:** September 1 - August 31
- **Barley/Oats:** June 1 - May 31

---

## 3. Food Grains (Wheat)

**Commodities:** Hard Red Winter (HRW), Hard Red Spring (HRS), Soft Red Winter (SRW), White Wheat, Durum

### Supply & Demand

| Source | Data Type | Frequency | Auth | URL |
|--------|-----------|-----------|------|-----|
| **USDA ERS Wheat Data** | S&D by class, prices | Monthly | FREE | https://www.ers.usda.gov/data-products/wheat-data/ |
| **USDA WASDE** | All wheat S&D | Monthly | FREE | WASDE report |
| **USDA NASS** | Acreage, production by class | Varies | API Key | NASS Quick Stats |
| **Kansas Wheat Commission** | HRW quality data | Annual | FREE | https://kswheat.com/ |

### Canadian Wheat

| Source | Data Type | Frequency | Auth | URL |
|--------|-----------|-----------|------|-----|
| **Canadian Grain Commission** | Exports by grade/destination | Weekly | FREE | https://www.grainscanada.gc.ca/ |
| **Statistics Canada** | Production, stocks | Monthly | FREE | StatsCan tables |
| **Canadian Wheat Board** | Quality reports | Annual | FREE | https://canadianwheat.ca/ |

### Trade Flows

| Source | Data Type | Frequency | Auth | URL |
|--------|-----------|-----------|------|-----|
| **USDA FAS Export Sales** | By class and destination | Weekly | FREE | FAS API |
| **US Wheat Associates** | FOB prices, export premiums | Daily | FREE | https://uswheat.org/ |

### Marketing Year: June 1 - May 31

---

## 4. Oilseeds & Products

### Soybeans & Products

**Commodities:** Soybeans, Soybean Meal, Soybean Oil

| Source | Data Type | Frequency | Auth | URL |
|--------|-----------|-----------|------|-----|
| **USDA ERS Oil Crops Yearbook** | S&D, prices, trade | Monthly | FREE | https://www.ers.usda.gov/data-products/oil-crops-yearbook/ |
| **NOPA Crush Report** | US soybean crush, oil stocks | Monthly (~15th) | PAID ($1,200/yr) | https://www.nopa.org/ |
| **USDA FAS PSD** | Global S&D | Monthly | FREE | PSD Online |
| **Census Fats & Oils** | Domestic use, stocks | Monthly | FREE | Census Bureau |

### Canola/Rapeseed

| Source | Data Type | Frequency | Auth | URL |
|--------|-----------|-----------|------|-----|
| **Canola Council of Canada** | Production, crush, exports | Monthly | FREE | https://www.canolacouncil.org/markets-stats/ |
| **Statistics Canada Crush** | Oilseed crushing statistics | Annual | FREE | https://www150.statcan.gc.ca/ |
| **USDA FAS PSD** | Global canola/rapeseed | Monthly | FREE | PSD Online |
| **ICE Futures Canada** | Canola futures | Daily | PAID | ICE |

### Sunflower

| Source | Data Type | Frequency | Auth | URL |
|--------|-----------|-----------|------|-----|
| **National Sunflower Association** | US production, crush | Annual | FREE | https://www.sunflowernsa.com/stats/ |
| **USDA ERS Oil Crops** | Sunflower S&D | Monthly | FREE | ERS Oil Crops |
| **USDA FAS** | Global sunflower | Monthly | FREE | FAS PSD |

### Marketing Years
- **Soybeans:** September 1 - August 31
- **Canola:** August 1 - July 31
- **Sunflower:** September 1 - August 31

---

## 5. Tropical Oils

**Commodities:** Palm Oil, Palm Kernel Oil, Coconut Oil, Lauric Oils

| Source | Data Type | Frequency | Auth | URL |
|--------|-----------|-----------|------|-----|
| **MPOB (Malaysia)** | Production, stocks, exports | Monthly (~10th) | FREE (scrape) | http://bepi.mpob.gov.my/ |
| **GAPKI (Indonesia)** | Indonesian palm oil | Monthly | FREE | https://gapki.id/ |
| **USDA FAS PSD** | Global palm, coconut | Monthly | FREE | PSD Online |
| **Asian Coconut Community** | Coconut oil market review | Monthly | FREE | https://coconutcommunity.org/ |
| **Oil World** | All vegetable oils | Weekly | PAID | https://www.oilworld.biz/ |

### US Import Sources
- Palm Oil: Malaysia, Indonesia
- Coconut Oil: Philippines, Indonesia, India
- Palm Kernel Oil: Malaysia, Indonesia

---

## 6. Biofuels

### Ethanol

| Source | Data Type | Frequency | Auth | URL |
|--------|-----------|-----------|------|-----|
| **EIA Ethanol** | Production, stocks, imports | Weekly (Wed) | API Key | https://api.eia.gov/v2/ |
| **EPA RFS/EMTS** | RIN generation, D6 codes | Monthly | FREE | https://www.epa.gov/fuels-registration-reporting-and-compliance-help/public-data-renewable-fuel-standard |
| **RFA (Renewable Fuels Assoc)** | Industry statistics | Monthly | FREE | https://ethanolrfa.org/ |
| **USDA AMS Ethanol Report** | Plant prices, DDGS | Weekly | FREE | https://www.ams.usda.gov/mnreports/ams_3616.pdf |

### Biodiesel

| Source | Data Type | Frequency | Auth | URL |
|--------|-----------|-----------|------|-----|
| **EIA Biodiesel** | Production, capacity, feedstocks | Monthly | API Key | https://www.eia.gov/biofuels/biodiesel/production/ |
| **EPA RFS/EMTS** | D4 RINs (biomass-based diesel) | Monthly | FREE | EPA RFS Data |
| **Clean Fuels Alliance** | Industry data (formerly NBB) | Varies | FREE | https://cleanfuels.org/ |

### Renewable Diesel

| Source | Data Type | Frequency | Auth | URL |
|--------|-----------|-----------|------|-----|
| **EIA Renewable Diesel** | Production capacity by plant | Monthly | API Key | https://www.eia.gov/biofuels/renewable/capacity/ |
| **EPA RFS** | D4 RINs | Monthly | FREE | EPA |
| **CARB LCFS** | California credits | Quarterly | FREE | https://ww2.arb.ca.gov/our-work/programs/low-carbon-fuel-standard |

### Sustainable Aviation Fuel (SAF)

| Source | Data Type | Frequency | Auth | URL |
|--------|-----------|-----------|------|-----|
| **EPA RFS** | SAF production (limited) | Annual | FREE | EPA |
| **DOE AFDC** | SAF overview/statistics | Varies | FREE | https://afdc.energy.gov/fuels/sustainable-aviation-fuel |
| **IATA** | Global SAF production | Annual | FREE | https://www.iata.org/ |
| **ICAO CORSIA** | Eligible fuels registry | Ongoing | FREE | https://www.icao.int/environmental-protection/CORSIA/ |

### RIN D-Codes Reference
- **D3:** Cellulosic biofuel
- **D4:** Biomass-based diesel (biodiesel, renewable diesel)
- **D5:** Advanced biofuel
- **D6:** Renewable fuel (corn ethanol)
- **D7:** Cellulosic diesel

---

## 7. Biofuel Feedstocks

### Animal Fats & Rendered Products

| Source | Data Type | Frequency | Auth | URL |
|--------|-----------|-----------|------|-----|
| **USDA AMS Tallow/Protein Report** | Tallow, grease, lard prices | Weekly | FREE | https://www.ams.usda.gov/mnreports/nw_ls442.txt |
| **USDA ERS** | Import statistics | Monthly | FREE | ERS Charts of Note |
| **The Jacobsen** | BFT, CWG, UCO, DCO, poultry fat | Daily | PAID | https://thejacobsen.com/ |
| **Fastmarkets** | Animal fats & proteins | Daily | PAID | https://www.fastmarkets.com/ |

### Feedstock Categories

| Feedstock | Common Abbreviation | Primary Use |
|-----------|---------------------|-------------|
| Bleachable Fancy Tallow | BFT | Biodiesel, RD |
| Choice White Grease | CWG | Biodiesel, RD |
| Yellow Grease | YG | Biodiesel, RD |
| Used Cooking Oil | UCO | Biodiesel, RD, SAF |
| Distillers Corn Oil | DCO | Biodiesel, RD |
| Poultry Fat | PF | Biodiesel, RD |
| Lard | - | Biodiesel |
| Technical Tallow | Tech | Biodiesel |
| Edible Tallow | - | Food, Biodiesel |

### Other Feedstocks

| Feedstock | Source | Notes |
|-----------|--------|-------|
| Camelina Oil | USDA NASS | Emerging SAF feedstock |
| Canola Oil | See Oilseeds | Major RD feedstock |
| Soybean Oil | See Oilseeds | Major biodiesel feedstock |
| Palm Oil | See Tropical Oils | Not RFS-qualified for US |

**Note:** No public data exists for UCO - USDA ERS estimates based on assumptions.

---

## 8. Energy

### Crude Oil & Refined Products

| Source | Data Type | Frequency | Auth | URL |
|--------|-----------|-----------|------|-----|
| **EIA Petroleum** | Crude, gasoline, diesel, jet, natgas | Weekly/Monthly | API Key | https://api.eia.gov/v2/ |
| **EIA Weekly Petroleum Status** | Stocks, production, imports | Weekly (Wed) | FREE | https://www.eia.gov/petroleum/supply/weekly/ |
| **API Weekly Statistical Bulletin** | Industry estimates | Weekly (Tue) | PAID | Via Refinitiv/ICE |
| **EIA Short-Term Energy Outlook** | Price forecasts | Monthly | FREE | https://www.eia.gov/outlooks/steo/ |

### Key EIA Series IDs

| Product | Series ID | Frequency |
|---------|-----------|-----------|
| WTI Crude | PET.RWTC.W | Weekly |
| Brent Crude | PET.RBRTE.W | Weekly |
| ULSD (Diesel) | PET.EER_EPD2D_PF4_RGC_DPG.W | Weekly |
| RBOB Gasoline | PET.EER_EPMRR_PF4_RGC_DPG.W | Weekly |
| Jet Fuel | PET.EER_EPJK_PF4_RGC_DPG.W | Weekly |
| Natural Gas (Henry Hub) | NG.RNGWHHD.W | Weekly |

### Refining Co-Products

| Product | Data Source | Notes |
|---------|-------------|-------|
| ULSD (Ultra-Low Sulfur Diesel) | EIA | Primary road diesel |
| Heating Oil | EIA | Northeast seasonal demand |
| Jet Fuel/Kerosene | EIA | Aviation demand |
| Residual Fuel Oil | EIA | Marine bunker fuel |
| Propane/LPG | EIA | Crop drying demand |
| Asphalt | EIA | Construction |
| Petroleum Coke | EIA | Industrial |
| Naphtha | EIA | Petrochemical feedstock |

---

## 9. Co-Products & By-Products

### Ethanol Co-Products

| Product | Source | Frequency | URL |
|---------|--------|-----------|-----|
| **DDGS (Dried Distillers Grains)** | USDA AMS | Weekly | https://www.ams.usda.gov/mnreports/ams_3618.pdf |
| **WDGS (Wet Distillers Grains)** | USDA AMS | Weekly | Same report |
| **MWDG (Modified Wet)** | USDA AMS | Weekly | Same report |
| **Distillers Corn Oil (DCO)** | USDA AMS | Weekly | Ethanol report |
| **CO2 (Food Grade)** | No public data | - | Industry estimates |

### Biodiesel Co-Products

| Product | Source | Notes |
|---------|--------|-------|
| **Crude Glycerin** | No standard source | ~10% of biodiesel output |
| **Refined Glycerin** | ICIS (paid) | Pharmaceutical/food grade |

### Soy Processing Co-Products

| Product | Source | URL |
|---------|--------|-----|
| **Soybean Hulls** | USDA AMS | Feed ingredient |
| **Soy Lecithin** | No public data | Food additive |

### Corn Processing Co-Products (Wet Milling)

| Product | Notes |
|---------|-------|
| Corn Gluten Feed | ~21% protein |
| Corn Gluten Meal | ~60% protein |
| Corn Germ | Oil extraction |
| Corn Steep Liquor | Fermentation nutrient |
| Corn Starch | Industrial/food |
| HFCS | High fructose corn syrup |

---

## 10. Input Costs

### Fertilizer

| Source | Data Type | Frequency | Auth | URL |
|--------|-----------|-----------|------|-----|
| **USDA ERS Fertilizer** | Prices, consumption | Annual | FREE | https://www.ers.usda.gov/data-products/fertilizer-use-and-price/ |
| **USDA Ag Transport** | Regional fertilizer prices | Monthly | FREE | https://agtransport.usda.gov/ |
| **Green Markets** | Weekly price index | Weekly | PAID | https://fertilizerpricing.com/ |
| **DTN Fertilizer** | Regional prices | Daily | PAID | DTN |

### Key Fertilizers

| Product | Formula | Primary Crop Use |
|---------|---------|------------------|
| Anhydrous Ammonia | NH3 | Corn (N) |
| Urea | CO(NH2)2 | Corn, wheat (N) |
| UAN (28%, 32%) | N solution | All crops (N) |
| DAP | (NH4)2HPO4 | All crops (N, P) |
| MAP | NH4H2PO4 | All crops (N, P) |
| Potash (MOP) | KCl | All crops (K) |

### Other Inputs

| Input | Source | Notes |
|-------|--------|-------|
| Diesel (farm) | EIA | Farm machinery fuel |
| Propane | EIA | Grain drying |
| Seed costs | USDA ERS | Annual estimates |
| Pesticides/Herbicides | USDA NASS | Survey data |
| Land rent | USDA NASS | Cash rent surveys |

---

## 11. Futures & Cash Prices

### CME Group (CBOT/NYMEX)

| Source | Data Type | Auth | URL |
|--------|-----------|------|-----|
| **CME DataMine** | Historical settlements | PAID | https://www.cmegroup.com/market-data/datamine-api.html |
| **CME Daily Bulletin** | Daily settlements (free) | FREE | https://www.cmegroup.com/ftp/bulletin/ |
| **TradingView** | 10-min delayed | FREE | https://www.tradingview.com/cme/ |
| **Databento** | Historical/streaming | Pay-per-use | https://databento.com/ |
| **Barchart** | Historical data | PAID | https://www.barchart.com/ |

### Key Agricultural Futures Contracts

| Contract | Exchange | Symbol | Contract Size |
|----------|----------|--------|---------------|
| Corn | CBOT | ZC | 5,000 bu |
| Soybeans | CBOT | ZS | 5,000 bu |
| Soybean Meal | CBOT | ZM | 100 short tons |
| Soybean Oil | CBOT | ZL | 60,000 lbs |
| Wheat (SRW) | CBOT | ZW | 5,000 bu |
| Wheat (HRW) | KCBT | KE | 5,000 bu |
| Wheat (HRS) | MGEX | MWE | 5,000 bu |
| Canola | ICE Canada | RS | 20 MT |
| Ethanol | CBOT | EH | 29,000 gal |

### Energy Futures

| Contract | Exchange | Symbol | Contract Size |
|----------|----------|--------|---------------|
| WTI Crude | NYMEX | CL | 1,000 bbl |
| RBOB Gasoline | NYMEX | RB | 42,000 gal |
| ULSD (Heating Oil) | NYMEX | HO | 42,000 gal |
| Natural Gas | NYMEX | NG | 10,000 MMBtu |

### Cash Prices

| Source | Coverage | Auth | URL |
|--------|----------|------|-----|
| **USDA AMS** | Grain, livestock | FREE | https://www.ams.usda.gov/market-news |
| **DTN** | Cash bids, basis | PAID | https://www.dtn.com/ |
| **Barchart cmdty** | Cash prices | PAID | https://www.barchart.com/cmdty |

---

## 12. Weather & Crop Conditions

### Weather

| Source | Data Type | Auth | URL |
|--------|-----------|------|-----|
| **NOAA/NWS** | Forecasts, observations | FREE | https://api.weather.gov/ |
| **US Drought Monitor** | Drought conditions | FREE | https://droughtmonitor.unl.edu/ |
| **NOAA CPC** | 6-14 day, monthly outlooks | FREE | https://www.cpc.ncep.noaa.gov/ |
| **NASA POWER** | Ag weather, global | FREE | https://power.larc.nasa.gov/ |

### Crop Conditions

| Source | Data Type | Frequency | Auth | URL |
|--------|-----------|-----------|------|-----|
| **USDA NASS Crop Progress** | Condition ratings | Weekly (Mon) | API Key | https://quickstats.nass.usda.gov/ |
| **USDA NASS Prospective Plantings** | Acreage intentions | March | FREE | NASS reports |
| **USDA NASS Acreage** | Planted acres | June | FREE | NASS reports |

---

## Appendix A: API Keys Required

| Source | Registration URL | Env Variable |
|--------|------------------|--------------|
| USDA NASS | https://quickstats.nass.usda.gov/api | `NASS_API_KEY` |
| EIA | https://www.eia.gov/opendata/register.php | `EIA_API_KEY` |
| UN Comtrade | https://comtradeplus.un.org/ | `COMTRADE_API_KEY` |
| US Census | https://api.census.gov/data/key_signup.html | `CENSUS_API_KEY` |
| USDA FAS | https://api.data.gov/signup/ | `FAS_API_KEY` |

---

## Appendix B: Data Update Schedule

| Report | Release Day | Release Time (ET) |
|--------|-------------|-------------------|
| WASDE | ~12th of month | 12:00 PM |
| Crop Progress | Monday | 4:00 PM |
| Export Sales | Thursday | 8:30 AM |
| Export Inspections | Monday | 11:00 AM |
| CFTC COT | Friday | 3:30 PM |
| EIA Petroleum Weekly | Wednesday | 10:30 AM |
| NOPA Crush | ~15th of month | Varies |
| MPOB | ~10th of month | Varies |
| EPA RFS RINs | Monthly | Varies |

---

## Appendix C: HS Codes Reference

### Grains (Chapter 10)
- 1001: Wheat
- 1002: Rye
- 1003: Barley
- 1004: Oats
- 1005: Corn (maize)
- 1007: Sorghum

### Oilseeds (Chapter 12)
- 1201: Soybeans
- 1205: Rapeseed/canola
- 1206: Sunflower seeds

### Fats & Oils (Chapter 15)
- 1507: Soybean oil
- 1511: Palm oil
- 1513: Coconut/palm kernel oil
- 1514: Rapeseed/canola oil
- 1515: Sunflower oil

### Oilseed Meals (Chapter 23)
- 2304: Soybean meal
- 2306: Rapeseed meal

---

## Appendix D: Collector Implementation Status

| Data Source | Collector Status | Priority |
|-------------|------------------|----------|
| CFTC COT | ✅ Implemented | High |
| USDA FAS Export Sales | ✅ Implemented | High |
| EIA Ethanol | ✅ Implemented | High |
| Drought Monitor | ✅ Implemented | High |
| MPOB | ✅ Implemented | Medium |
| USDA NASS | 🔲 Planned | High |
| EIA Petroleum | 🔲 Planned | High |
| EPA RFS | 🔲 Planned | High |
| UN Comtrade | 🔲 Planned | Medium |
| US Census Trade | 🔲 Planned | Medium |
| Canadian Grain Commission | 🔲 Planned | Medium |
| Statistics Canada | 🔲 Planned | Medium |
| USDA AMS Tallow | 🔲 Planned | Medium |
| USDA AMS DDGS | 🔲 Planned | Medium |
| CME Settlements | 🔲 Planned | High |
| USDA ERS Feed Grains | 🔲 Planned | High |
| USDA ERS Oil Crops | 🔲 Planned | High |
| USDA ERS Wheat | 🔲 Planned | High |
| USDA ERS Fertilizer | 🔲 Planned | Medium |
