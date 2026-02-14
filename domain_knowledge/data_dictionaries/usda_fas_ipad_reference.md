# USDA FAS Crop Explorer NDVI Data System: Complete Technical Reference

The Crop Explorer platform at **ipad.fas.usda.gov** provides global vegetation monitoring through satellite-derived NDVI data, but does **not expose a traditional REST API**. Programmatic access requires using the GADAS ArcGIS REST services at **geo.fas.usda.gov** or the FAS Open Data API for commodity data integration. This documentation provides the complete technical specifications needed to build an automated data collection system for commodity market analysis.

---

## Platform architecture and access points

The USDA Foreign Agricultural Service operates Crop Explorer through the International Production Assessment Division (IPAD), providing near-real-time vegetation monitoring across **130+ countries** using primarily MODIS satellite data at **250-meter resolution** with **8-day composite** updates.

### Primary URLs and entry points

| Resource | URL | Purpose |
|----------|-----|---------|
| **Crop Explorer Main** | `https://ipad.fas.usda.gov/cropexplorer/` | Primary web interface |
| **Country Summary** | `https://ipad.fas.usda.gov/countrysummary/Default.aspx?id={CountryCode}` | Country-specific data |
| **Image View** | `https://ipad.fas.usda.gov/cropexplorer/imageview.aspx?regionid={REGION}&product={PRODUCT}` | NDVI imagery |
| **Subregion Charts** | `https://ipad.fas.usda.gov/Cropexplorer/subregion_chart.aspx` | Time-series visualization |
| **MODIS GeoTIFF Downloads** | `https://ipad.fas.usda.gov/cropexplorer/modis_ndvi/modis_geotiff_new.aspx` | Direct imagery downloads |
| **Data Sources Documentation** | `https://ipad.fas.usda.gov/cropexplorer/datasources.aspx` | Methodology and sources |
| **GADAS (GIS Platform)** | `https://geo.fas.usda.gov/GADAS/index.html` | ArcGIS-based data access |
| **FAS Open Data** | `https://apps.fas.usda.gov/OpenData/api/` | Commodity data API |

The GeoTIFF download URL structure follows this pattern:
```
https://ipad.fas.usda.gov/cropexplorer/modis_ndvi/modis_geotiff_new.aspx?modis_tile={TILE}&doy={DAY_OF_YEAR}&year={YEAR}&mdate={DATE}
```

---

## API endpoints and data access methods

### GADAS ArcGIS REST Services (Primary NDVI access)

The GADAS platform provides the most direct programmatic access to vegetation index data through Esri ArcGIS REST API standards.

**Base URL:** `https://geo.fas.usda.gov/arcgis2/rest/services`

| Service Folder | Endpoint | Data Type |
|----------------|----------|-----------|
| `/G_VegetationIndex` | `VIIRS_NDVI_Anomaly_Global_8day/ImageServer` | VIIRS NDVI anomalies |
| `/G_VegetationIndex` | `MODIS_PASG_MONTH/ImageServer` | Percent Average Seasonal Greenness |
| `/G_Soil_Moisture` | Various ImageServers | Soil moisture products |
| `/G_Climatology` | Various ImageServers | Climate data layers |
| `/G_SMAP` | Various ImageServers | SMAP soil moisture |
| `/G_CropMask_IGBP` | Various ImageServers | Crop classification masks |
| `/G_Tools` | `Run_Stored_Procedure/GPServer/Tool` | Analytics processing |

**GP Tool for NDVI Analytics:**
```http
POST https://geo.fas.usda.gov/arcgis2/rest/services/G_Tools/Run_Stored_Procedure/GPServer/Tool/submitJob
Content-Type: application/x-www-form-urlencoded

features={"source":"modis_terra","reportdayofyearstart":60,"reportdayofyearend":273,"reportyear":"2025","frequency":"8-day","attribute":"ndvi","ids":"04020394","adminunit":""}&stored_procedure=sp_geo_map_analytics&environment=dev&f=json
```

**GP Tool Parameters:**

| Parameter | Type | Valid Values |
|-----------|------|--------------|
| `source` | string | `modis_terra`, `modis_aqua`, `viirs` |
| `reportdayofyearstart` | integer | 1-365 |
| `reportdayofyearend` | integer | 1-365 |
| `reportyear` | string | Year(s), comma-separated (e.g., "2024,2025") |
| `frequency` | string | `8-day`, `16-day`, `monthly` |
| `attribute` | string | `ndvi`, `evi`, `anomaly` |
| `ids` | string | Administrative unit IDs |
| `adminunit` | string | Administrative unit name |

### FAS Open Data API (Commodity integration)

While not providing direct NDVI data, the FAS Open Data API supplies the commodity production and trade data that integrates with Crop Explorer assessments.

**Base URL:** `https://apps.fas.usda.gov/OpenData/api/`

**Swagger Documentation:** `https://apps.fas.usda.gov/opendata/swagger/ui/index`

**Authentication:** API key required via header `{"API_KEY": "your-api-key"}`

**Key Endpoints:**

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/esr/regions` | GET | Export regions list |
| `/esr/countries` | GET | Country codes and names |
| `/esr/commodities` | GET | Commodity code list |
| `/esr/exports/commodityCode/{code}/allCountries/marketYear/{year}` | GET | Export sales data |
| `/psd/commodity/{code}/country/all/year/{year}` | GET | Production/supply/distribution |
| `/psd/commodities` | GET | PSD commodity codes |
| `/psd/commodityAttributes` | GET | Attribute definitions |

**Example PSD Request:**
```python
import requests
import pandas as pd

url = 'https://apps.fas.usda.gov/OpenData/api/psd/commodity/0440000/world/year/2025'
headers = {'Accept': 'application/json', 'API_KEY': 'your_key_here'}
response = requests.get(url, headers=headers)
df = pd.DataFrame(response.json())
```

### Response formats supported

| System | Formats Available |
|--------|-------------------|
| **GADAS ArcGIS** | JSON (`?f=json`), GeoJSON (`?f=geojson`), PBF, KML/KMZ, HTML |
| **FAS Open Data** | JSON (primary) |
| **Direct Downloads** | GeoTIFF, PNG, JPEG |

### Authentication and rate limits

| System | Authentication | Rate Limits |
|--------|----------------|-------------|
| **Crop Explorer Web** | Public access, no auth | N/A |
| **GADAS ArcGIS** | Token available at `/arcgis2/tokens/`, many services public | MaxRecordCount: 1000 per query |
| **FAS Open Data API** | API key required from api.data.gov | ~1,000 requests/hour |

---

## Data series and symbology specifications

### Available NDVI products

| Data Series | Description | Calculation | Update |
|-------------|-------------|-------------|--------|
| **NDVI (Current)** | Raw vegetation index | (NIR - RED) / (NIR + RED) | 8-day |
| **NDVI Departure from Average** | Current minus historical mean | Current - Historical Average | 8-day |
| **NDVI Departure from Previous Year** | Year-over-year comparison | Current - Same Period Last Year | 8-day |
| **NDVI Departure from Previous Period** | Sequential change | Current - Previous 8-day | 8-day |
| **PASG** | Percent of Average Seasonal Greenness | (Current Accumulated / Historical Accumulated) × 100% | Seasonal |

### NDVI technical specifications

| Parameter | Specification |
|-----------|---------------|
| **Data Type** | 16-bit signed integer (INT16) |
| **Scale Factor** | 0.0001 |
| **Valid Range (Raw)** | -2000 to 10000 |
| **Valid Range (Scaled)** | -0.2 to 1.0 |
| **Fill Value** | -3000 |
| **Conversion Formula** | Actual_NDVI = Raw_Value × 0.0001 |

### NDVI value interpretation

| NDVI Range | Surface Type | Crop Condition |
|------------|--------------|----------------|
| -1.0 to 0 | Water, clouds, snow | N/A |
| 0 to 0.1 | Bare soil, rock, sand | Fallow/pre-planting |
| 0.1 to 0.3 | Sparse vegetation | Early emergence |
| 0.3 to 0.5 | Moderate vegetation | Active growth |
| 0.5 to 0.7 | Dense vegetation | Peak vegetative stage |
| 0.7 to 0.9 | Very dense vegetation | Maximum canopy |
| 0.9 to 1.0 | Extremely dense | Rare in agriculture |

### Anomaly calculation methodology

**Vegetation Condition Index (VCI):**
```
VCI = [(NDVI_current - NDVI_min) / (NDVI_max - NDVI_min)] × 100%
```

**Mean-referenced VCI (MVCI):**
```
MVCI = [(NDVI_current - NDVI_mean) / NDVI_mean] × 100
```

**Ratio to Median (RMNDVI):**
```
RMNDVI = (NDVI_current / NDVI_median) × 100
```

---

## Satellite data sources and specifications

### Primary sensors

| Sensor | Platform | Resolution | Period | Composite |
|--------|----------|------------|--------|-----------|
| **MODIS** | Terra/Aqua | 250 m | 2000-present | 8-day |
| **VIIRS** | NOAA-20/Suomi NPP | 375 m | 2012-present | 8-day |
| **SPOT-VEG** | SPOT-4/5 | 1 km | 1998-2014 | 10-day (dekadal) |
| **AVHRR** | NOAA series | 4-8 km | 1981-present | 10-day |

### MODIS band configuration

| Band | Wavelength (nm) | Purpose |
|------|-----------------|---------|
| Band 1 (Red) | 620-670 | NDVI calculation |
| Band 2 (NIR) | 841-876 | NDVI calculation |
| Band 3 (Blue) | 459-479 | Atmospheric correction |
| Band 7 (Mid-IR) | 2105-2155 | Quality assessment |

### MODIS product identifiers

| Product ID | Description | Resolution | Temporal |
|------------|-------------|------------|----------|
| MOD13Q1 | Terra VI 16-Day L3 | 250 m | 16-day |
| MYD13Q1 | Aqua VI 16-Day L3 | 250 m | 16-day |
| MOD13A1 | Terra VI 16-Day L3 | 500 m | 16-day |
| MOD13A2 | Terra VI 16-Day L3 | 1 km | 16-day |
| MOD13C1 | Terra VI CMG | ~5.6 km | 16-day |
| MOD13C2 | Terra VI Monthly CMG | ~5.6 km | Monthly |

---

## Geographic coverage and region codes

### Global coverage

Crop Explorer monitors **130+ countries** across all major agricultural regions. Coverage includes dedicated crop-specific regions for major commodities.

### Region ID codes (URL parameter)

| Region ID | Coverage |
|-----------|----------|
| `world` | Global view |
| `us` | United States |
| `europe` | European Union |
| `umb` | Ukraine/Moldova/Belarus |
| `central_america2` | Central America |
| `metu` | Middle East/Turkey |

### USDA FAS country codes (4-digit system)

| Region | Code Range | Examples |
|--------|------------|----------|
| **Western Hemisphere** | 1xxx-3xxx | Canada (1220), Mexico (2010), Brazil (3510) |
| **European Union** | 4xxx | Germany (4280), France (4270), Poland (4361) |
| **Former Soviet Union** | 46xx | Russia (4621), Ukraine (4623), Kazakhstan (4634) |
| **Asia** | 5xxx | China (5700), India (5330), Japan (5880) |
| **Oceania** | 6xxx | Australia (6020), New Zealand (6160) |
| **Africa** | 7xxx | Egypt (7290), Nigeria (7530), South Africa (7910) |
| **Unknown** | 9990 | Unspecified destination |

### Commodity codes (PSD system)

| Commodity | Code |
|-----------|------|
| Corn | 0440000 |
| Wheat | 0410000 |
| Soybeans | 2222000 |
| Rice | 0422110 |
| Cotton | 2631000 |

### US agricultural belts

| Belt | Primary States |
|------|----------------|
| Corn Belt | Iowa, Illinois, Indiana, Ohio, Nebraska, Minnesota |
| Winter Wheat Belt | Kansas, Oklahoma, Texas, Nebraska, Colorado |
| Spring Wheat Belt | Montana, North Dakota, South Dakota, Minnesota |
| Cotton Belt | Texas, Georgia, Mississippi, Arkansas, Arizona |
| Soybean Belt | Iowa, Illinois, Indiana, Minnesota, Missouri |

---

## Temporal coverage and baseline periods

### Historical data availability

| Source | Start Date | End Date | Resolution |
|--------|------------|----------|------------|
| **AVHRR GAC** | July 1981 | June 2006 | 8 km |
| **SPOT-VEG** | January 1999 | May 2008 | 1 km |
| **MODIS Operational** | February 2002 | Present | 250 m |

### Baseline periods for anomaly calculations

| Sensor | Baseline Period | Type |
|--------|-----------------|------|
| **AVHRR GAC** | July 1981 - June 2006 | Long-term average |
| **SPOT-VEG** | Jan 1999 - Dec 2002 + Jun 2006 - May 2008 | Short-term average |
| **MODIS** | February 2002 - Present | Rolling average |
| **NASA GLAM Anomaly** | 2000-2011 | Fixed reference |

### Data latency

| Processing Level | Latency |
|------------------|---------|
| Near Real-Time (LANCE) | Within 3 hours |
| NRT Vegetation Products | Within 24 hours |
| Standard Processing | 2-5 days |
| 8-day Composite Available | +8 days from start of period |

### Marketing year conventions

| Commodity | Marketing Year |
|-----------|----------------|
| Wheat, Barley, Oats | June 1 - May 31 |
| Corn, Sorghum, Soybeans | September 1 - August 31 |
| Cotton, Rice | August 1 - July 31 |
| Soybean Oil/Meal | October 1 - September 30 |

---

## Companion data series

Crop Explorer provides integrated access to complementary agricultural monitoring data following similar geographic and temporal structures.

| Data Type | Source | Resolution | Update |
|-----------|--------|------------|--------|
| **Precipitation** | CHIRPS | 8 km | Daily/cumulative |
| **Temperature** | AFWA/stations | 25-51 km | Daily |
| **Soil Moisture** | NASA SMAP | ~9 km | Every 3 days |
| **Evapotranspiration** | Satellite-derived | Variable | Periodic |
| **Drought Index (SPI)** | Calculated | Regional | 5-day |
| **Snow Cover** | MODIS | 500 m | Daily |

### SMAP soil moisture (NASA-USDA partnership)

Available on Google Earth Engine:
- Collection: `NASA_USDA/HSL/SMAP_soil_moisture`
- Variables: Surface soil moisture, subsurface soil moisture, soil moisture profile
- Documentation: `https://earth.gsfc.nasa.gov/hydro/data/nasa-usda-global-soil-moisture-data`

---

## Integration with USDA systems

### System relationships

```
[MODIS/VIIRS Satellites] → [IPAD/Crop Explorer Analysis]
                                      ↓
[GAIN Reports] ←→ [PSD Database] ←→ [WASDE]
                          ↓
              [WAP Monthly Circular]
```

### Connected USDA FAS databases

| System | URL | Data Sync |
|--------|-----|-----------|
| **PSD Online** | `https://apps.fas.usda.gov/psdonline/` | WASDE release day (12:00 ET) |
| **WASDE** | `https://www.fas.usda.gov/data/wasde` | Monthly |
| **GAIN** | `https://gain.fas.usda.gov/` | Continuous |
| **WAP Circular** | `https://apps.fas.usda.gov/psdonline/circulars/production.pdf` | Monthly |
| **GADAS** | `https://geo.fas.usda.gov/GADAS/` | Real-time |

---

## Implementation best practices

### Building NDVI time series

1. **Define region of interest** using USDA region codes or administrative boundaries
2. **Select appropriate baseline** for anomaly comparison (MODIS 2002-present recommended)
3. **Account for compositing** - 8-day periods overlap at boundaries
4. **Apply crop masks** to isolate specific commodity signals
5. **Correlate with phenology** - NDVI interpretation varies by growth stage

### Crop-specific NDVI-yield correlations

| Crop | Peak NDVI R² | Accumulated NDVI R² | Notes |
|------|--------------|---------------------|-------|
| **Corn** | 0.88 | 0.93 | Excellent predictor |
| **Soybeans** | 0.62 | 0.73 | Good under specific conditions |
| **Spring Wheat** | 0.40 | 0.60 | Moderate utility |
| **Winter Wheat** | Variable | 0.50-0.85 | Region-dependent |
| **Cotton** | Poor | Poor | Not recommended |

### Recommended workflow

```python
# Example: Automated NDVI data collection
import requests
import pandas as pd
from datetime import datetime, timedelta

# 1. Get commodity codes
commodities_url = 'https://apps.fas.usda.gov/OpenData/api/psd/commodities'
headers = {'API_KEY': 'your_key'}

# 2. Query GADAS for NDVI analytics
gadas_url = 'https://geo.fas.usda.gov/arcgis2/rest/services/G_Tools/Run_Stored_Procedure/GPServer/Tool/submitJob'
params = {
    'features': '{"source":"modis_terra","reportyear":"2025","frequency":"8-day","attribute":"ndvi"}',
    'f': 'json'
}

# 3. Correlate with PSD production data
psd_url = 'https://apps.fas.usda.gov/OpenData/api/psd/commodity/0440000/world/year/2025'
```

---

## Machine-readable JSON specification

```json
{
  "system": {
    "name": "USDA FAS Crop Explorer NDVI Data System",
    "organization": "USDA Foreign Agricultural Service - International Production Assessment Division",
    "version": "2026-01",
    "documentation_date": "2026-01-30"
  },
  "endpoints": {
    "crop_explorer": {
      "base_url": "https://ipad.fas.usda.gov/cropexplorer/",
      "type": "web_application",
      "authentication": "none",
      "endpoints": {
        "main": "/Default.aspx",
        "image_view": "/imageview.aspx",
        "subregion_chart": "/subregion_chart.aspx",
        "geotiff_download": "/modis_ndvi/modis_geotiff_new.aspx",
        "data_sources": "/datasources.aspx"
      },
      "url_parameters": {
        "image_view": {
          "regionid": {"type": "string", "values": ["world", "us", "europe", "umb", "central_america2", "metu"]},
          "product": {"type": "string", "values": ["modis_ndvi_8day"]}
        },
        "geotiff_download": {
          "modis_tile": {"type": "string", "format": "x##y##", "example": "x24y04"},
          "doy": {"type": "integer", "range": [1, 365]},
          "year": {"type": "integer"},
          "mdate": {"type": "string", "format": "MM/DD/YYYY"}
        }
      }
    },
    "gadas_arcgis": {
      "base_url": "https://geo.fas.usda.gov/arcgis2/rest/services",
      "type": "arcgis_rest",
      "authentication": {
        "type": "optional_token",
        "token_url": "https://geo.fas.usda.gov/arcgis2/tokens/"
      },
      "rate_limits": {"max_record_count": 1000},
      "response_formats": ["json", "pjson", "geojson", "pbf", "kml", "html"],
      "services": {
        "vegetation_index": {
          "path": "/G_VegetationIndex",
          "layers": [
            "VIIRS_NDVI_Anomaly_Global_8day/ImageServer",
            "MODIS_PASG_MONTH/ImageServer"
          ]
        },
        "soil_moisture": {"path": "/G_Soil_Moisture"},
        "climatology": {"path": "/G_Climatology"},
        "smap": {"path": "/G_SMAP"},
        "crop_mask": {"path": "/G_CropMask_IGBP"},
        "analytics_tool": {
          "path": "/G_Tools/Run_Stored_Procedure/GPServer/Tool",
          "method": "POST",
          "parameters": {
            "source": {"type": "string", "values": ["modis_terra", "modis_aqua", "viirs"]},
            "reportdayofyearstart": {"type": "integer", "range": [1, 365]},
            "reportdayofyearend": {"type": "integer", "range": [1, 365]},
            "reportyear": {"type": "string", "format": "YYYY or YYYY,YYYY"},
            "frequency": {"type": "string", "values": ["8-day", "16-day", "monthly"]},
            "attribute": {"type": "string", "values": ["ndvi", "evi", "anomaly"]},
            "ids": {"type": "string", "description": "Administrative unit IDs"},
            "adminunit": {"type": "string"}
          }
        }
      }
    },
    "fas_opendata": {
      "base_url": "https://apps.fas.usda.gov/OpenData/api",
      "type": "rest_api",
      "swagger_url": "https://apps.fas.usda.gov/opendata/swagger/ui/index",
      "authentication": {
        "type": "api_key",
        "header_name": "API_KEY",
        "registration_url": "https://api.data.gov"
      },
      "rate_limits": {"requests_per_hour": 1000},
      "response_format": "json",
      "endpoints": {
        "esr_regions": {"path": "/esr/regions", "method": "GET"},
        "esr_countries": {"path": "/esr/countries", "method": "GET"},
        "esr_commodities": {"path": "/esr/commodities", "method": "GET"},
        "esr_exports": {
          "path": "/esr/exports/commodityCode/{commodityCode}/allCountries/marketYear/{marketYear}",
          "method": "GET",
          "parameters": {
            "commodityCode": {"type": "string"},
            "marketYear": {"type": "integer"}
          }
        },
        "psd_commodity": {
          "path": "/psd/commodity/{commodityCode}/country/{countryCode}/year/{marketYear}",
          "method": "GET"
        },
        "psd_world": {
          "path": "/psd/commodity/{commodityCode}/world/year/{marketYear}",
          "method": "GET"
        },
        "psd_commodities": {"path": "/psd/commodities", "method": "GET"},
        "psd_attributes": {"path": "/psd/commodityAttributes", "method": "GET"}
      }
    },
    "psd_services": {
      "base_url": "https://apps.fas.usda.gov/PSDOnlineDataServices/api",
      "swagger_url": "https://apps.fas.usda.gov/PSDOnlineDataServices/swagger/ui/index",
      "authentication": {"type": "api_key", "header_name": "API_KEY"},
      "endpoints": {
        "all_data": {"path": "/CommodityData/GetAllData", "method": "GET"}
      }
    }
  },
  "data_specifications": {
    "ndvi": {
      "data_type": "INT16",
      "scale_factor": 0.0001,
      "valid_range": {"raw": [-2000, 10000], "scaled": [-0.2, 1.0]},
      "fill_value": -3000,
      "units": "dimensionless",
      "formula": "NDVI = (NIR - RED) / (NIR + RED)"
    },
    "interpretation": {
      "water_clouds_snow": {"range": [-1.0, 0]},
      "bare_soil": {"range": [0, 0.1]},
      "sparse_vegetation": {"range": [0.1, 0.3]},
      "moderate_vegetation": {"range": [0.3, 0.5]},
      "dense_vegetation": {"range": [0.5, 0.7]},
      "very_dense_vegetation": {"range": [0.7, 0.9]}
    }
  },
  "satellite_sources": {
    "modis": {
      "platforms": ["Terra", "Aqua"],
      "spatial_resolution_m": 250,
      "temporal_resolution": "8-day composite",
      "spectral_bands": {
        "red": {"wavelength_nm": [620, 670]},
        "nir": {"wavelength_nm": [841, 876]}
      },
      "operational_since": "2000-02-01",
      "collection": "6.1"
    },
    "viirs": {
      "platforms": ["Suomi NPP", "NOAA-20"],
      "spatial_resolution_m": 375,
      "temporal_resolution": "8-day composite"
    },
    "avhrr": {
      "spatial_resolution_km": 8,
      "archive_period": {"start": "1981-07-01", "end": "2006-06-30"}
    }
  },
  "temporal_specifications": {
    "update_frequency": "8-day",
    "data_latency": {
      "near_real_time": "3 hours",
      "nrt_vegetation": "24 hours",
      "standard_processing": "2-5 days"
    },
    "historical_availability": {
      "modis": {"start": "2002-02-01"},
      "avhrr": {"start": "1981-07-01", "end": "2006-06-30"},
      "spot_veg": {"start": "1999-01-01", "end": "2008-05-31"}
    },
    "baseline_periods": {
      "avhrr_long_term": {"start": "1981-07-01", "end": "2006-06-30"},
      "modis_rolling": {"start": "2002-02-01", "end": "present"},
      "glam_reference": {"start": "2000-01-01", "end": "2011-12-31"}
    }
  },
  "geographic_coverage": {
    "countries_monitored": 130,
    "regional_codes": {
      "type": "usda_fas_custom",
      "format": "4-digit",
      "ranges": {
        "western_hemisphere": "1xxx-3xxx",
        "european_union": "4xxx",
        "former_soviet_union": "46xx",
        "asia": "5xxx",
        "oceania": "6xxx",
        "africa": "7xxx"
      }
    },
    "boundary_systems": ["FAO_GAUL", "USDA_Custom", "FIPS"]
  },
  "commodity_codes": {
    "corn": "0440000",
    "wheat": "0410000",
    "soybeans": "2222000",
    "rice": "0422110",
    "cotton": "2631000",
    "barley": "0430000",
    "sorghum": "0459100"
  },
  "marketing_years": {
    "wheat_barley_oats": {"start_month": 6, "start_day": 1},
    "corn_sorghum_soybeans": {"start_month": 9, "start_day": 1},
    "cotton_rice": {"start_month": 8, "start_day": 1},
    "soybean_products": {"start_month": 10, "start_day": 1}
  },
  "companion_data": {
    "precipitation": {"source": "CHIRPS", "resolution_km": 8},
    "temperature": {"source": "AFWA", "resolution_km": "25-51"},
    "soil_moisture": {"source": "NASA_SMAP", "resolution_km": 9, "gee_collection": "NASA_USDA/HSL/SMAP_soil_moisture"},
    "drought_index": {"type": "SPI", "timestep_days": 5}
  },
  "related_systems": {
    "psd_online": {
      "url": "https://apps.fas.usda.gov/psdonline/",
      "description": "Production Supply Distribution database",
      "sync": "WASDE release day 12:00 ET"
    },
    "wasde": {
      "url": "https://www.fas.usda.gov/data/wasde",
      "description": "World Agricultural Supply and Demand Estimates",
      "frequency": "monthly"
    },
    "gain": {
      "url": "https://gain.fas.usda.gov/",
      "description": "Global Agricultural Information Network"
    },
    "wap_circular": {
      "url": "https://apps.fas.usda.gov/psdonline/circulars/production.pdf",
      "description": "World Agricultural Production monthly report"
    },
    "gadas": {
      "url": "https://geo.fas.usda.gov/GADAS/",
      "description": "Global Agricultural and Disaster Assessment System"
    },
    "glam": {
      "url": "https://glam1.gsfc.nasa.gov/",
      "description": "NASA Global Agricultural Monitoring"
    }
  },
  "contacts": {
    "crop_explorer": "ronald.frantz@fas.usda.gov",
    "gadas": ["lisa.colson@usda.gov", "curt.reynolds@usda.gov"],
    "gadas_support": "GMA.GADAS@usda.gov"
  },
  "data_terms": {
    "license": "Creative Commons Attribution",
    "public_domain": true,
    "attribution_required": true
  }
}
```

---

## Conclusion

The USDA FAS Crop Explorer NDVI system provides comprehensive global vegetation monitoring but requires a multi-endpoint integration strategy for automated data collection. **GADAS ArcGIS REST services** offer the most direct programmatic access to NDVI imagery and analytics, while the **FAS Open Data API** provides essential commodity context through PSD and export data. The system delivers **250-meter resolution MODIS data** with **8-day update cycles** covering over **130 countries**, with historical records extending to 1981 through legacy AVHRR data. For commodity market analysis, the strongest NDVI-yield correlations are found for corn (R² up to 0.93), making this system particularly valuable for grain market forecasting when integrated with PSD production estimates and WASDE forecasts.