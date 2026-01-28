# RLC Graphics and Visual Reference Guide

This document provides locations and descriptions of visual assets available for report generation and analysis.

---

## Static Reference Maps

### US Crop Production Maps
**Location:** `domain_knowledge/crop_maps/us/`

| File | Commodity | Description |
|------|-----------|-------------|
| `US - Corn - 2023.png` | Corn | County-level corn production density map |
| `US - Soybean - 2023.png` | Soybeans | County-level soybean production density map |
| `US - Wheat - 2023.png` | Wheat (all) | Combined wheat production map |
| `US - Winter Wheat - 2023.png` | Winter Wheat | HRW/SRW production regions |
| `US - Spring Wheat - 2023.png` | Spring Wheat | Northern Plains spring wheat |
| `US Winter Wheat Area.png` | Winter Wheat | Planted area visualization |
| `US Soybean Area.png` | Soybeans | Planted area visualization |
| `US - Cotton - 2023.png` | Cotton | Cotton belt production |
| `US - Sorghum - 2023.png` | Sorghum | Great Plains sorghum |
| `US - Rice - 2023.png` | Rice | Delta and California rice |
| `US - Barley - 2023.png` | Barley | Northern tier barley |
| `US - Canola - 2023.png` | Canola | Northern Plains canola |
| `US - Oats - 2023.png` | Oats | Oat production areas |
| `US - Sunflower - 2023.png` | Sunflower | Dakotas sunflower |
| `US - Peanut - 2023.png` | Peanuts | Southeast peanut belt |
| `US - Sugarbeets - 2023.png` | Sugarbeets | Red River Valley, etc. |
| `US - Sugarcane - 2023.png` | Sugarcane | Florida, Louisiana |

### South America Crop Maps
**Location:** `domain_knowledge/crop_maps/brazil/`

| File | Commodity | Description |
|------|-----------|-------------|
| `Brazil - Soybean - 2023.png` | Soybeans | Brazilian state-level production |

---

## Dynamic Graphics (Weather Email Extracts)
**Location:** `data/weather_graphics/`

When the weather email agent processes meteorologist emails, it extracts embedded graphics and saves them here organized by date:

```
data/weather_graphics/
├── 2026-01-28/
│   ├── weather_map_001.png
│   ├── precipitation_forecast.png
│   └── temperature_outlook.png
├── 2026-01-27/
│   └── ...
```

**Usage:** Reference the most recent date folder for current weather graphics.

---

## Generated Graphics
**Location:** `data/generated_graphics/`

The graphics generator agent creates custom visualizations from database data:

```
data/generated_graphics/
├── charts/
│   ├── ethanol_production_weekly.png
│   ├── cftc_positioning.png
│   └── corn_condition_yoy.png
├── maps/
│   └── drought_overlay.png
└── tables/
    └── balance_sheet_comparison.png
```

---

## How to Use Graphics in Reports

### Referencing Crop Maps
When discussing regional production or weather impacts, reference the appropriate crop map:

> "The drought conditions are concentrated in the western Corn Belt (see US - Corn - 2023.png for production geography), affecting approximately 25% of total US corn acreage."

### Referencing Weather Graphics
For current weather analysis, check the latest date folder in `data/weather_graphics/`:

> "Today's precipitation forecast (see weather_graphics/2026-01-28/precipitation_forecast.png) shows significant rainfall expected across the Delta region."

### Requesting Generated Graphics
When a visualization would enhance the report, request it from the graphics generator:

**Available chart types:**
- Time series (prices, production, stocks)
- Bar charts (comparisons, rankings)
- YoY comparison charts
- CFTC positioning charts
- Balance sheet tables
- Regional comparison maps

**Example request format:**
```
Generate chart:
- Type: time_series
- Data: EIA ethanol production (weekly, last 52 weeks)
- Include: 5-year average overlay
- Title: "US Ethanol Production vs 5-Year Average"
```

---

## Key Regions for Weather/Production Analysis

### US Corn Belt
Primary states: IA, IL, NE, MN, IN, OH, SD, WI, MO, KS
- Reference: `US - Corn - 2023.png`

### US Soybean Belt
Primary states: IL, IA, MN, IN, NE, OH, MO, SD, ND, AR
- Reference: `US - Soybean - 2023.png`

### US Winter Wheat Belt
HRW: KS, OK, TX, CO, NE
SRW: OH, IN, IL, MO, AR, KY, TN
- Reference: `US - Winter Wheat - 2023.png`

### US Spring Wheat Belt
Primary states: ND, MT, MN, SD
- Reference: `US - Spring Wheat - 2023.png`

### Brazil Soybean
Key states: Mato Grosso (MT), Parana (PR), Rio Grande do Sul (RS), Goias (GO), Mato Grosso do Sul (MS)
- Reference: `Brazil - Soybean - 2023.png`

---

## Notes

- All crop maps are from USDA NASS and show harvested acreage by county/state
- Weather graphics are automatically extracted from meteorologist emails daily
- Generated graphics are created on-demand and cached for reuse
- When referencing maps in reports, provide context about which regions are most relevant to the analysis
