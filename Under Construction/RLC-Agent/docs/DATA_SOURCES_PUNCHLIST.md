# RLC Data Sources Punchlist
**Generated: 2026-01-28**

## Status Legend
- **COMPLETE**: Collector working, database schema exists, save_to_bronze implemented, data flowing
- **PARTIAL**: Collector exists but missing database integration or schema
- **BLOCKED**: Collector exists but external issue (API down, credentials needed)
- **NOT STARTED**: No collector exists yet
- **LOW PRIORITY**: Nice to have, not critical for launch

---

## US GOVERNMENT DATA

### USDA Sources

| Source | Collector | Schema | save_to_bronze | Data in DB | Status | Notes |
|--------|-----------|--------|----------------|------------|--------|-------|
| USDA NASS | usda_nass_collector.py | Yes | Yes | 580 records | **COMPLETE** | Crop progress, condition, acreage, production, stocks |
| USDA FAS Export Sales | usda_fas_collector.py | No | No | 0 | **BLOCKED** | API returning 500 errors - ticket submitted |
| USDA AMS Cash Prices | usda_ams_collector.py | Yes | No | 22,849 | **PARTIAL** | Working but needs save_to_bronze standardization |
| USDA ERS Oilcrops | usda_ers_collector.py | Yes | No | 25,441 | **PARTIAL** | Data exists, needs save_to_bronze |
| USDA ERS Wheat | usda_ers_collector.py | Yes | No | 53,373 | **PARTIAL** | Data exists, needs save_to_bronze |
| USDA WASDE | None | No | No | 0 | **NOT STARTED** | Need to build WASDE collector |
| USDA FGIS Export Inspections | None | No | No | 0 | **NOT STARTED** | Weekly export inspection data |

### Other US Government

| Source | Collector | Schema | save_to_bronze | Data in DB | Status | Notes |
|--------|-----------|--------|----------------|------------|--------|-------|
| EIA Ethanol | eia_ethanol_collector.py | Yes | No | 612 | **PARTIAL** | Working, has gold views, needs save_to_bronze |
| EIA Petroleum | eia_petroleum_collector.py | Yes | No | (shared) | **PARTIAL** | Working, shares EIA schema |
| Census Trade | census_trade_collector.py | Yes | Yes | 1,461 | **COMPLETE** | 10-digit HS codes with volume |
| CFTC COT | cftc_cot_collector.py | Yes | Yes | 306 | **COMPLETE** | Weekly positioning data |
| EPA RFS | epa_rfs_collector.py | No | No | 0 | **NOT STARTED** | Renewable fuel standard data |
| US Drought Monitor | drought_collector.py | No | No | 0 | **NOT STARTED** | Weekly drought conditions |

---

## MARKET DATA

| Source | Collector | Schema | save_to_bronze | Data in DB | Status | Notes |
|--------|-----------|--------|----------------|------------|--------|-------|
| CME Settlements | cme_settlements_collector.py | Yes | No | 10 | **PARTIAL** | Needs testing and save_to_bronze |
| Yahoo Futures | yahoo_futures_collector.py | Yes | No | 10 | **PARTIAL** | Basic data flowing |
| IBKR Real-time | ibkr_collector.py | No | No | 0 | **NOT STARTED** | Requires IBKR TWS running |
| TradeStation | tradestation_collector.py | No | No | 0 | **LOW PRIORITY** | Alternative to IBKR |
| Barchart | None | No | No | 0 | **NOT STARTED** | Historical futures data |

---

## SOUTH AMERICA

| Source | Collector | Schema | save_to_bronze | Data in DB | Status | Notes |
|--------|-----------|--------|----------------|------------|--------|-------|
| CONAB Brazil | conab_collector.py | No | No | 0 | **PARTIAL** | Collector exists, needs schema + integration |
| ABIOVE Brazil Crush | abiove_collector.py | No | No | 0 | **NOT STARTED** | Brazilian crush/export data |
| IMEA Mato Grosso | imea_collector.py | No | No | 0 | **NOT STARTED** | MT state production costs |
| IBGE SIDRA | ibge_sidra_collector.py | No | No | 0 | **NOT STARTED** | Brazilian national statistics |
| Argentina MAGyP | magyp_collector.py | No | No | 0 | **NOT STARTED** | Argentine production data |
| Argentina BCR Rosario | None | No | No | 0 | **NOT STARTED** | Rosario Board of Trade |
| Paraguay | None | No | No | 0 | **LOW PRIORITY** | Paraguayan ag data |
| Uruguay | None | No | No | 0 | **LOW PRIORITY** | Uruguayan ag data |

---

## CANADA

| Source | Collector | Schema | save_to_bronze | Data in DB | Status | Notes |
|--------|-----------|--------|----------------|------------|--------|-------|
| Canada CGC | canada_cgc_collector.py | No | No | 0 | **NOT STARTED** | Grain Commission weekly data |
| Statistics Canada | canada_statscan_collector.py | No | No | 0 | **NOT STARTED** | Canadian production/stocks |
| ICE Canola | None | No | No | 0 | **NOT STARTED** | Winnipeg canola futures |

---

## ASIA / OCEANIA

| Source | Collector | Schema | save_to_bronze | Data in DB | Status | Notes |
|--------|-----------|--------|----------------|------------|--------|-------|
| MPOB Malaysia | mpob_collector.py | No | No | 0 | **NOT STARTED** | Palm oil production/stocks |
| China Customs | None | No | No | 0 | **NOT STARTED** | Chinese import data |
| CNGOIC China | None | No | No | 0 | **NOT STARTED** | Chinese grain/oilseed data |
| ABARE Australia | None | No | No | 0 | **LOW PRIORITY** | Australian production |

---

## EUROPE

| Source | Collector | Schema | save_to_bronze | Data in DB | Status | Notes |
|--------|-----------|--------|----------------|------------|--------|-------|
| EU MARS Crop | None | No | No | 0 | **NOT STARTED** | EU crop monitoring |
| Eurostat | None | No | No | 0 | **NOT STARTED** | EU trade/production stats |
| APK-Inform Ukraine | None | No | No | 0 | **NOT STARTED** | Ukrainian grain data |
| SovEcon Russia | None | No | No | 0 | **LOW PRIORITY** | Russian grain estimates |

---

## GLOBAL / OTHER

| Source | Collector | Schema | save_to_bronze | Data in DB | Status | Notes |
|--------|-----------|--------|----------------|------------|--------|-------|
| FAOSTAT | faostat_collector.py | No | No | 0 | **NOT STARTED** | FAO global production data |
| IGC | None | No | No | 0 | **NOT STARTED** | Int'l Grains Council |
| AMIS | None | No | No | 0 | **LOW PRIORITY** | Ag Market Info System |
| World Bank Pink Sheet | None | No | No | 0 | **LOW PRIORITY** | Commodity price indices |

---

## WEATHER

| Source | Collector | Schema | save_to_bronze | Data in DB | Status | Notes |
|--------|-----------|--------|----------------|------------|--------|-------|
| OpenWeather | weather_collector_agent.py | Yes | Yes | 152,763 | **COMPLETE** | Hourly observations |
| Open-Meteo | weather_collector_agent.py | Yes | Yes | (shared) | **COMPLETE** | Backup weather source |
| Weather Email | weather_email_agent.py | Yes | Yes | 45 | **COMPLETE** | Email weather extraction |
| NOAA CPC | None | No | No | 0 | **NOT STARTED** | 6-10 and 8-14 day outlooks |

---

## TENDERS / TRADE FLOW

| Source | Collector | Schema | save_to_bronze | Data in DB | Status | Notes |
|--------|-----------|--------|----------------|------------|--------|-------|
| Wheat Tenders | wheat_tender_collector.py | No | No | 0 | **NOT STARTED** | GASC, Algeria, etc. |
| Vessel Lineups | None | No | No | 0 | **NOT STARTED** | Port loading lineups |

---

## PRIORITY RANKING FOR COMPLETION

### Tier 1 - Critical for Launch (Complete These First)
1. ~~USDA NASS~~ **DONE**
2. ~~Census Trade~~ **DONE**
3. ~~CFTC COT~~ **DONE**
4. ~~EIA Ethanol~~ **WORKING** (needs save_to_bronze)
5. USDA FAS Export Sales **BLOCKED** - waiting on API fix
6. CONAB Brazil - collector exists, needs schema

### Tier 2 - Important (Complete Soon After Launch)
7. USDA WASDE - need new collector
8. CME/Yahoo Futures - needs save_to_bronze
9. Canada CGC - collector exists
10. MPOB Palm Oil - collector exists
11. US Drought Monitor - collector exists

### Tier 3 - Nice to Have
12. Argentina MAGyP
13. ABIOVE Brazil
14. EPA RFS
15. NOAA CPC Outlooks
16. FAOSTAT

### Tier 4 - Future
17. China data sources
18. European sources
19. Vessel lineups
20. Real-time IBKR integration

---

## SUMMARY STATS

| Status | Count |
|--------|-------|
| COMPLETE | 5 |
| PARTIAL | 6 |
| BLOCKED | 1 |
| NOT STARTED | 25+ |
| LOW PRIORITY | 6 |

**Next Actions:**
1. Add save_to_bronze to EIA collectors
2. Create CONAB database schema and integration
3. Monitor FAS API status
4. Build WASDE collector
