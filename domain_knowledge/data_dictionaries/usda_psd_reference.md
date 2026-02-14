# USDA FAS PSD API: Complete Technical Reference

The USDA Foreign Agricultural Service Production, Supply, and Distribution (PSD) API provides programmatic access to the world's most authoritative database of global agricultural supply and demand forecasts. **Two API systems exist**: the modern FAS OpenData API at `api.fas.usda.gov` (recommended) and the legacy PSDOnlineDataServices at `apps.fas.usda.gov`. The API requires registration for an API key, offers historical data back to **1960**, covers **100+ countries** and **all major agricultural commodities**, and updates monthly on WASDE release days at **12:00 PM ET**. For commodity market analysis, the PSD API delivers the same official USDA forecasts that move global grain, oilseed, cotton, and livestock markets.

---

## API architecture and base URLs

The FAS maintains two distinct API infrastructures for accessing PSD data, with the modern system being the recommended approach for new development.

### Primary API (Recommended)
| Component | Value |
|-----------|-------|
| **Base URL** | `https://api.fas.usda.gov` |
| **Swagger Specification** | `https://api.fas.usda.gov/assets/swagger/swagger.json` |
| **Interactive Documentation** | `https://apps.fas.usda.gov/opendatawebV2/#/home` |
| **Swagger UI** | `https://apps.fas.usda.gov/opendata/swagger/ui/index` |

### Legacy API
| Component | Value |
|-----------|-------|
| **Base URL** | `https://apps.fas.usda.gov/PSDOnlineDataServices` |
| **Swagger UI** | `https://apps.fas.usda.gov/PSDOnlineDataServices/swagger/ui/index` |
| **Note** | Less documented; may return empty results without precise filtering |

### Alternative URL Format
An additional endpoint format exists at `https://apps.fas.usda.gov/OpenData/api/psd/` which mirrors the primary API structure.

---

## Complete endpoint reference

The PSD API provides **reference data endpoints** for code lookups and **data retrieval endpoints** for commodity forecasts.

### Reference data endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/psd/regions` | GET | Returns all region codes and names |
| `/api/psd/countries` | GET | Returns countries with corresponding region codes |
| `/api/psd/commodities` | GET | Returns all commodities with 7-digit codes |
| `/api/psd/unitsOfMeasure` | GET | Returns unit of measure codes and descriptions |
| `/api/psd/commodityAttributes` | GET | Returns all attribute codes (production, supply, distribution) |

### Data retrieval endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/psd/commodity/{commodityCode}/country/all/year/{marketYear}` | GET | Forecast data for all countries |
| `/api/psd/commodity/{commodityCode}/country/{countryCode}/year/{marketYear}` | GET | Forecast data for specific country |
| `/api/psd/commodity/{commodityCode}/world/year/{marketYear}` | GET | World-aggregated data |
| `/api/psd/commodity/{commodityCode}/dataReleaseDates` | GET | Last release dates for commodity |

### Legacy endpoint
| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/CommodityData/GetAllData` | GET | Bulk data retrieval (requires filters) |

---

## Authentication and rate limits

### API key registration

All API requests **require an API key** obtained through api.data.gov. Registration is free and immediate.

**Registration URL**: https://api.data.gov/signup/

After providing your name and email, you receive a **40-character alphanumeric key** instantly.

### Authentication methods

**Method 1: HTTP Header (Recommended)**
```
X-Api-Key: YOUR_API_KEY_HERE
```

**Method 2: Custom Header (Legacy)**
```
API_KEY: YOUR_API_KEY_HERE
```

**Method 3: Query Parameter**
```
?api_key=YOUR_API_KEY_HERE
```

### Rate limits

| Key Type | Hourly Limit | Daily Limit |
|----------|--------------|-------------|
| **Registered Key** | 1,000 requests | Unlimited |
| **DEMO_KEY** | 30 requests | 50 requests |

Rate limit information appears in response headers:
```
X-RateLimit-Limit: 1000
X-RateLimit-Remaining: 998
```

Exceeding limits returns **HTTP 429** and blocks requests for one hour. Higher limits may be requested by contacting the FAS API team.

---

## Commodity codes and symbology

PSD uses **7-digit numeric codes** to identify commodities. The first digits indicate commodity group, with remaining digits providing specificity.

### Major grain commodity codes

| Commodity | Code |
|-----------|------|
| Corn | 0440000 |
| Wheat (aggregate) | 0410000 |
| Rice | varies by type |
| Barley | varies |
| Sorghum | varies |
| Oats | varies |

### Oilseed and product codes

| Commodity | Seed Code | Meal Code | Oil Code |
|-----------|-----------|-----------|----------|
| Soybeans | 2222000* | 2304/230250 | 1507 |
| Rapeseed/Canola | 1205 | 230640-49 | 1514 |
| Sunflowerseed | 1206 | 230630 | 151211/19 |
| Cottonseed | 120720-29 | 230610 | 151221/29 |
| Peanuts | 120210-42 | 2305 | 1508 |
| Palm (oil only) | — | — | 1511 |
| Palm Kernel | 120710 | 230660 | 151321/29 |
| Copra | 1203 | 230650 | 151311/19 |

*Verify current soybean code via `/api/psd/commodities` endpoint*

### Discovering commodity codes programmatically

Always retrieve current codes from the commodities endpoint:
```bash
curl -H 'X-Api-Key: YOUR_KEY' 'https://api.fas.usda.gov/api/psd/commodities'
```

Response format:
```json
[
  {"commodityCode": "0440000", "commodityName": "Corn"},
  {"commodityCode": "0410000", "commodityName": "Wheat"}
]
```

---

## Attribute codes for supply and demand data

PSD attributes represent the supply-use balance sheet variables that analysts track. Query the `/api/psd/commodityAttributes` endpoint for the complete current list.

### Supply-side attributes

| Attribute | Description |
|-----------|-------------|
| Area Harvested | Land area harvested (1,000 hectares) |
| Beginning Stocks | Opening inventory at marketing year start |
| Production | Total output |
| Imports | Total imports |
| Yield | Production per unit area |
| Animal Inventory | Livestock head count |
| Slaughter | Animals processed |
| Total Supply | Beginning stocks + Production + Imports |

### Demand-side attributes

| Attribute | Description |
|-----------|-------------|
| Crush | Amount processed (oilseeds) |
| Domestic Consumption | Total domestic use |
| Food Use Dom. | Human food consumption |
| Feed | Animal feed consumption |
| Seed | Planting seed use |
| Industrial Use | Industrial applications (ethanol, biodiesel) |
| Other Use | Miscellaneous uses |
| Waste | Losses |
| Exports | Total exports |
| Ending Stocks | Closing inventory |

---

## Country and region codes

PSD uses **4-digit FAS-specific codes** (not ISO) for countries, plus **2-letter codes** in some API responses.

### Major producing and consuming countries

| Country | 4-Digit Code | 2-Letter |
|---------|--------------|----------|
| United States | — | US |
| Brazil | 3510 | BR |
| Argentina | 3570 | AR |
| China | 5700 | CN |
| India | 5330 | IN |
| European Union | varies | EU |
| Russia | 4621 | RU |
| Ukraine | 4623 | UA |
| Canada | 1220 | CA |
| Australia | 6020 | AU |
| Japan | 5880 | JP |
| Mexico | 2010 | MX |
| Indonesia | 5600 | ID |
| Thailand | 5490 | TH |
| Vietnam | 5520 | VN |

### Regional groupings

| Region | Countries Included |
|--------|-------------------|
| European Union (EU-27) | Austria (4330), Belgium-Luxembourg (4230), Bulgaria (4870), Cyprus (4910), Czech Republic (4351), Denmark (4090), Estonia (4470), Finland (4050), France (4270), Germany (4280), Greece (4840), Hungary (4370), Ireland (4190), Italy (4750), Latvia (4490), Lithuania (4510), Malta (4730), Netherlands (4210), Poland (4550), Portugal (4710), Romania (4850), Slovakia (4359), Slovenia (4792), Spain (4700), Sweden (4010) |
| Former Soviet Union | Russia (4621), Ukraine (4623), Kazakhstan (4634), Belarus (4622), Uzbekistan (4644), others |
| Other Europe | United Kingdom (4120), Switzerland (4410), Norway (4030), Turkey (4890) |

### Retrieving country codes
```bash
curl -H 'X-Api-Key: YOUR_KEY' 'https://api.fas.usda.gov/api/psd/countries'
```

Response includes region associations:
```json
[
  {"countryCode": "BR", "countryName": "Brazil", "regionCode": "SA"}
]
```

---

## Marketing year conventions and date formats

Marketing years vary by commodity and country, creating complexity for cross-commodity analysis.

### Marketing year format
- **API parameter**: 4-digit year (e.g., `2024`)
- **Display format**: Split year notation `2024/25`
- The first year indicates when the marketing year **begins**

### Marketing year start months by commodity

| Commodity/Region | Marketing Year Period |
|------------------|----------------------|
| U.S. Corn, Soybeans | September/August |
| U.S. Wheat | June/May |
| U.S. Rice | August/July |
| Brazil Soybeans | February/January |
| Argentina Soybeans | March/February |
| EU Wheat | July/June |
| EU Rapeseed | July/June |
| Northern Hemisphere (default) | October/September |
| Southern Hemisphere (varies) | Second year may begin MY |

### Critical note on stocks data
PSD stocks data aggregates differing local marketing years and **should not be interpreted as world stock levels at a fixed point in time**.

---

## Unit of measure codes

| Code | Description | Typical Use |
|------|-------------|-------------|
| MT | Metric Tons | Smaller quantities |
| 1000 MT | Thousand Metric Tons | Most production/trade data |
| MMT | Million Metric Tons | World totals, summaries |
| HA | Hectares | Area (smaller regions) |
| 1000 HA | Thousand Hectares | Area harvested |
| Bales | 480-pound bales | Cotton |
| Head | Animal count | Livestock inventory |
| Pieces | Individual items | Hides |
| Dozen | 12 units | Eggs |

Retrieve unit codes via:
```bash
curl -H 'X-Api-Key: YOUR_KEY' 'https://api.fas.usda.gov/api/psd/unitsOfMeasure'
```

---

## Complete API query examples

### Example 1: Get all corn data for Brazil, 2024
```bash
curl -H 'X-Api-Key: YOUR_KEY' \
  'https://api.fas.usda.gov/api/psd/commodity/0440000/country/BR/year/2024'
```

### Example 2: Get world soybean data, 2024
```bash
curl -H 'X-Api-Key: YOUR_KEY' \
  'https://api.fas.usda.gov/api/psd/commodity/2222000/world/year/2024'
```

### Example 3: Get corn data for all countries, 2024
```bash
curl -H 'X-Api-Key: YOUR_KEY' \
  'https://api.fas.usda.gov/api/psd/commodity/0440000/country/all/year/2024'
```

### Python implementation

```python
import requests
import pandas as pd

API_KEY = 'your_api_key_here'
BASE_URL = 'https://api.fas.usda.gov'

def get_psd_data(endpoint):
    """Fetch PSD data from FAS API"""
    headers = {
        'Accept': 'application/json',
        'X-Api-Key': API_KEY
    }
    response = requests.get(f'{BASE_URL}{endpoint}', headers=headers)
    if response.status_code == 200:
        return pd.DataFrame(response.json())
    else:
        raise Exception(f"API Error: {response.status_code} - {response.text}")

# Get reference data
commodities = get_psd_data('/api/psd/commodities')
countries = get_psd_data('/api/psd/countries')
attributes = get_psd_data('/api/psd/commodityAttributes')

# Get corn data for Brazil 2024
corn_brazil = get_psd_data('/api/psd/commodity/0440000/country/BR/year/2024')

# Get world wheat data
wheat_world = get_psd_data('/api/psd/commodity/0410000/world/year/2024')
```

### Response structure

Data endpoint responses include:
- `commodityCode` - 7-digit commodity identifier
- `countryCode` - Country identifier
- `marketYear` - Marketing year
- `attributeId` - Maps to attribute names from commodityAttributes
- `value` - Numeric value
- `unitId` - Maps to unit names from unitsOfMeasure

---

## Data coverage and update schedule

### Historical availability
PSD contains data for most commodities **since 1960**, making it invaluable for long-term trend analysis and modeling.

### Geographic coverage
- **100+ individual countries** covered through Interagency Commodity Estimates Committees
- **Regional aggregates** (World, EU, Former Soviet Union, etc.)
- Focus on **key producing and consuming countries**

### Commodity categories

| Category | Commodities |
|----------|-------------|
| Grains | Wheat, corn, rice, barley, sorghum, oats, rye, millet |
| Oilseeds | Soybeans, rapeseed/canola, sunflowerseed, peanuts, cottonseed, palm kernel, copra |
| Oilseed products | Soybean meal/oil, rapeseed meal/oil, sunflower meal/oil, palm oil, palm kernel oil |
| Fiber | Cotton |
| Sugar | Sugar (cane and beet) |
| Livestock | Beef and veal, pork, poultry (chicken, turkey), lamb/mutton |
| Dairy | Milk, butter, cheese, dry milk products |
| Other | Eggs, hides |

### Update frequency

| Data Type | Update Frequency |
|-----------|------------------|
| WASDE commodities | Monthly (WASDE release day) |
| Horticultural products | Twice yearly |
| Export Sales Reports | Weekly (Thursdays, 8:30 AM ET) |

### WASDE release schedule (2026)
January 12, February 10, March 10, April 9, May 12, June 11, July 10, August 12, September 11, October 9, November 10, December 10

**PSD database updates at 12:00 PM ET** on WASDE release days. FAS commodity circulars follow at **12:15 PM ET**.

---

## Relationship between PSD and WASDE

The PSD database is the **underlying data source** for WASDE (World Agricultural Supply and Demand Estimates) summaries.

### Data flow
1. FAS attachés at **93 overseas offices** provide field intelligence
2. Interagency committee reviews all sources during **lock-up** (analysts enter secured facility at 2:00 AM on release day)
3. NASS domestic estimates incorporated
4. WAOB chairs final review
5. WASDE released at 12:00 PM ET
6. PSD database updated simultaneously
7. FAS circulars released at 12:15 PM ET

### Coverage differences

| Aspect | WASDE | PSD |
|--------|-------|-----|
| Detail level | Summary/aggregate | Country-by-country detail |
| Commodities | Core agricultural commodities | Broader coverage |
| U.S. livestock/dairy | Full coverage | More limited international |
| Format | PDF report | Database/API |

---

## Related USDA data systems

### GATS (Global Agricultural Trade System)
**URL**: https://apps.fas.usda.gov/gats

| Aspect | GATS | PSD |
|--------|------|-----|
| Focus | Actual recorded trade | Supply-use balance estimates |
| Data source | U.S. Census Bureau | Interagency estimates |
| Updates | Monthly (~5th of month) | Monthly (WASDE day) |
| History | Back to 1967 | Back to 1960 |
| Use case | Trade flow analysis | S&D balance sheets |

GATS provides **actual transaction data** while PSD provides **estimated trade flows** within supply-use balances.

### GAIN (Global Agricultural Information Network)
**URL**: https://gain.fas.usda.gov/

GAIN reports contain **preliminary attaché estimates** that feed into official PSD numbers. GAIN is **not official USDA data**—it represents field-level intelligence before interagency review. The database contains reports since 1995/96, with **2,000+ reports added annually** from 98 offices covering 177 markets.

### NASS (National Agricultural Statistics Service)
**URL**: https://www.nass.usda.gov/Quick_Stats/

NASS provides **U.S. domestic production statistics** used in PSD. Key reports include Crop Progress, Grain Stocks, Prospective Plantings, and Planted Acreage. NASS focuses exclusively on U.S. statistics while PSD covers international.

### ERS (Economic Research Service)
**URL**: https://www.ers.usda.gov/

ERS analysts participate in PSD interagency committees and use PSD as the **primary source for historical supply/demand data** in their Agricultural Baseline Database. ERS produces 10-year baseline projections incorporating PSD data.

---

## Official documentation and resources

### API documentation
- **FAS OpenData Portal**: https://apps.fas.usda.gov/opendatawebV2/#/home
- **Swagger UI**: https://apps.fas.usda.gov/opendata/swagger/ui/index
- **Legacy Swagger**: https://apps.fas.usda.gov/PSDOnlineDataServices/swagger/ui/index
- **API Key Registration**: https://api.data.gov/signup/

### PSD online interface
- **Main Portal**: https://apps.fas.usda.gov/psdonline/
- **FAS Databases Page**: https://www.fas.usda.gov/data/databases-applications

### Commodity circulars (PDF)
- **Oilseeds**: https://apps.fas.usda.gov/psdonline/circulars/oilseeds.pdf
- **Grains**: https://apps.fas.usda.gov/psdonline/circulars/grain.pdf
- **Cotton**: https://apps.fas.usda.gov/psdonline/circulars/cotton.pdf

### Data catalogs
- **Data.gov**: https://catalog.data.gov/dataset/production-supply-and-distribution-database
- **Ag Data Commons**: https://data.nal.usda.gov/dataset/usda-foreign-agricultural-service-production-supply-and-distribution-database

### Release calendars
- **FAS Scheduled Reports**: https://www.fas.usda.gov/data/scheduled-reports
- **WASDE Schedule**: https://www.usda.gov/oce/commodity-markets/wasde-report

---

## Conclusion

The FAS PSD API provides comprehensive programmatic access to official USDA global commodity supply and demand forecasts—the same data that drives WASDE reports and influences agricultural commodity markets worldwide. Key implementation considerations include: (1) use the modern `api.fas.usda.gov` endpoint with the `X-Api-Key` header for best compatibility; (2) always retrieve current commodity, country, and attribute codes from reference endpoints before querying data; (3) account for marketing year variations across commodities and countries; (4) plan for the **1,000 requests/hour rate limit** when building production systems; and (5) align data retrieval with the WASDE release schedule to capture fresh forecasts. The combination of PSD for supply-use balances, GATS for actual trade data, and GAIN for field intelligence provides a complete picture of global agricultural markets for sophisticated commodity analysis.