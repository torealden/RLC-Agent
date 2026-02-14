# USDA FAS OpenData API Reference

**Base URL:** `https://api.fas.usda.gov/api`
**Authentication:** API Key in header (`X-API-KEY`)
**API Key:** Set `FAS_API_KEY` environment variable
**Registration:** https://api.fas.usda.gov

**Status as of 2026-01-29:** API WORKING - Use X-API-KEY header (not API_KEY)

---

## ESR Data API (Export Sales Report)

Weekly export sales data by commodity, country, and marketing year.

### Reference Endpoints

| Endpoint | Description |
|----------|-------------|
| `GET /api/esr/regions` | Region codes and names |
| `GET /api/esr/countries` | Countries mapped to region codes |
| `GET /api/esr/commodities` | Commodity information listings |
| `GET /api/esr/unitsOfMeasure` | Units of measure data |
| `GET /api/esr/datareleasedates` | Commodity-level data release dates |

### Data Endpoints

| Endpoint | Parameters | Description |
|----------|------------|-------------|
| `GET /api/esr/exports/commodityCode/{commodityCode}/allCountries/marketYear/{marketYear}` | commodityCode (int), marketYear (int) | Export records to all countries |
| `GET /api/esr/exports/commodityCode/{commodityCode}/countryCode/{countryCode}/marketYear/{marketYear}` | commodityCode (int), countryCode (int), marketYear (int) | Exports for specific country |

### Commodity Codes (ESR)

**Note: ESR and PSD use DIFFERENT commodity codes!**

| Commodity | ESR Code | Description |
|-----------|----------|-------------|
| Corn | 401 | Corn |
| Soybeans | 801 | Soybeans |
| All Wheat | 107 | All Wheat |
| Wheat HRW | 101 | Hard Red Winter |
| Wheat SRW | 102 | Soft Red Winter |
| Wheat HRS | 103 | Hard Red Spring |
| Wheat White | 104 | White Wheat |
| Soybean Meal | 901 | Soybean Cake & Meal |
| Soybean Oil | 902 | Soybean Oil |
| Sorghum | 701 | Sorghum |
| Cotton | 1404 | All Upland Cotton |
| Rice | 1505 | All Rice |

### Example Request
```
GET https://api.fas.usda.gov/api/esr/exports/commodityCode/401/allCountries/marketYear/2025
Headers:
  X-API-KEY: your-api-key
  Accept: application/json
```

---

## PSD Data API (Production, Supply & Distribution)

Supply/demand balance data by commodity, country, and marketing year.

### Reference Endpoints

| Endpoint | Description |
|----------|-------------|
| `GET /api/psd/regions` | PSD regional classifications |
| `GET /api/psd/countries` | Countries with regional associations |
| `GET /api/psd/commodities` | Commodity codes and descriptions |
| `GET /api/psd/unitsOfMeasure` | Units of measure identifiers |
| `GET /api/psd/commodityAttributes` | Production/Supply/Distribution attribute IDs |

### Data Endpoints

| Endpoint | Parameters | Description |
|----------|------------|-------------|
| `GET /api/psd/commodity/{commodityCode}/dataReleaseDates` | commodityCode (string) | Forecast release dates |
| `GET /api/psd/commodity/{commodityCode}/country/all/year/{marketYear}` | commodityCode (string), marketYear (int) | All countries by year |
| `GET /api/psd/commodity/{commodityCode}/country/{countryCode}/year/{marketYear}` | commodityCode (string), countryCode (string), marketYear (int) | Country-specific data |
| `GET /api/psd/commodity/{commodityCode}/world/year/{marketYear}` | commodityCode (string), marketYear (int) | World aggregate |

### Example Request
```
GET https://apps.fas.usda.gov/OpenData/api/psd/commodity/0440000/country/US/year/2025
Headers:
  API_KEY: your-api-key
  Accept: application/json
```

---

## GATS Data API (Global Agricultural Trade System)

Census Bureau and UN ComTrade data.

### Reference Endpoints

| Endpoint | Description |
|----------|-------------|
| `GET /api/gats/regions` | Region codes and names |
| `GET /api/gats/countries` | Country listings with regions |
| `GET /api/gats/commodities` | HS10-level commodity classifications |
| `GET /api/gats/HS6Commodities` | HS6-level commodity classifications |
| `GET /api/gats/unitsOfMeasure` | Units of measure |
| `GET /api/gats/customsDistricts` | US Customs Districts |

### Release Date Endpoints

| Endpoint | Description |
|----------|-------------|
| `GET /api/gats/census/data/exports/dataReleaseDates` | Census export release dates |
| `GET /api/gats/census/data/imports/dataReleaseDates` | Census import release dates |
| `GET /api/gats/UNTrade/data/exports/dataReleaseDates` | UN Trade export release dates |
| `GET /api/gats/UNTrade/data/imports/dataReleaseDates` | UN Trade import release dates |

### Census Data Endpoints

| Endpoint | Parameters | Description |
|----------|------------|-------------|
| `GET /api/gats/censusImports/partnerCode/{partnerCode}/year/{year}/month/{month}` | partnerCode, year, month | US imports by partner |
| `GET /api/gats/censusExports/partnerCode/{partnerCode}/year/{year}/month/{month}` | partnerCode, year, month | US exports by partner |
| `GET /api/gats/censusReExports/partnerCode/{partnerCode}/year/{year}/month/{month}` | partnerCode, year, month | US re-exports |

### Customs District Endpoints

| Endpoint | Parameters | Description |
|----------|------------|-------------|
| `GET /api/gats/customsDistrictExports/partnerCode/{partnerCode}/year/{year}/month/{month}` | partnerCode, year, month | Exports by district |
| `GET /api/gats/customsDistrictImports/partnerCode/{partnerCode}/year/{year}/month/{month}` | partnerCode, year, month | Imports by district |
| `GET /api/gats/customsDistrictReExports/partnerCode/{partnerCode}/year/{year}/month/{month}` | partnerCode, year, month | Re-exports by district |

### UN ComTrade Endpoints

| Endpoint | Parameters | Description |
|----------|------------|-------------|
| `GET /api/gats/UNTradeImports/reporterCode/{reporterCode}/year/{year}` | reporterCode, year | UN ComTrade imports |
| `GET /api/gats/UNTradeExports/reporterCode/{reporterCode}/year/{year}` | reporterCode, year | UN ComTrade exports |
| `GET /api/gats/UNTradeReExports/reporterCode/{reporterCode}/year/{year}` | reporterCode, year | UN ComTrade re-exports |

---

## Response Fields

### ESR Export Sales Fields
- `weekEndingDate` - Week ending date
- `marketYear` - Marketing year
- `countryDescription` - Destination country
- `countryCode` - Country code
- `regionDescription` - Region name
- `weeklyExports` - Weekly export volume
- `accumulatedExports` - Cumulative exports
- `outstandingSales` - Outstanding commitments
- `grossNewSales` - New sales
- `netSales` - Net sales
- `prevMarketYearAccumulatedExports` - Prior year comparison

### PSD Supply/Demand Fields
- `marketYear` - Marketing year
- `countryDescription` - Country name
- `beginningStocks` - Beginning stocks
- `production` - Production
- `imports` - Imports
- `totalSupply` - Total supply
- `domesticConsumption` - Domestic use
- `exports` - Exports
- `totalUse` - Total use
- `endingStocks` - Ending stocks
- `areaHarvested` - Harvested area
- `yieldPerHectare` - Yield

---

## Troubleshooting

### HTTP 500 Errors
Server-side issue - check https://apps.fas.usda.gov/opendatawebv2/ for status

### HTTP 403 Errors
API key missing or invalid - ensure `API_KEY` header is set

### HTTP 401 Errors
API key expired - re-register at https://apps.fas.usda.gov/opendatawebv2/
