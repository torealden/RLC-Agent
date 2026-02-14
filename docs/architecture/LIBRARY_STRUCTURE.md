# RLC Reference Library Structure

This folder serves as a knowledge base for LLM-assisted commodity analysis.
The goal is to provide context, reference materials, and current data that
enables better, faster, and more informed analysis.

---

## Directory Structure

```
report_samples/
│
├── data/                              # CURRENT REPORT SAMPLES
│   │                                  # Replace with latest each release
│   │
│   ├── corn/
│   │   ├── feed_grains_outlook.pdf
│   │   ├── crop_progress.pdf
│   │   └── ...
│   │
│   ├── soybeans/
│   │   ├── oil_crops_outlook.pdf
│   │   ├── nopa_crush.pdf
│   │   └── ...
│   │
│   ├── wheat/
│   │   ├── wheat_outlook.pdf
│   │   └── ...
│   │
│   ├── cattle/
│   │   ├── cattle_on_feed.pdf
│   │   ├── cattle_on_feed.zip         # CSV data
│   │   └── ...
│   │
│   ├── energy/
│   │   ├── eia_petroleum_status.pdf
│   │   ├── eia_biodiesel_monthly.xlsx
│   │   └── ...
│   │
│   └── cross_commodity/               # Multi-commodity reports
│       ├── wasde.pdf
│       ├── grain_stocks.pdf
│       ├── prospective_plantings.pdf
│       ├── acreage.pdf
│       ├── export_sales.pdf
│       ├── export_inspections.xlsx
│       └── ...
│
├── reference/                         # STATIC REFERENCE MATERIALS
│   │                                  # Update annually or as needed
│   │
│   ├── crop_maps/                     # Where crops are grown
│   │   ├── us/
│   │   │   ├── corn_production_map.pdf
│   │   │   ├── soybean_production_map.pdf
│   │   │   ├── wheat_production_map.pdf
│   │   │   ├── cattle_feedlots_map.pdf
│   │   │   └── state_rankings.xlsx
│   │   ├── brazil/
│   │   │   ├── soybean_regions.pdf
│   │   │   ├── corn_safrinha_regions.pdf
│   │   │   └── ...
│   │   ├── argentina/
│   │   └── global/
│   │
│   ├── crop_calendars/                # Planting/harvest timing
│   │   ├── us_crop_calendar.pdf
│   │   ├── brazil_crop_calendar.pdf
│   │   ├── argentina_crop_calendar.pdf
│   │   ├── northern_hemisphere.pdf
│   │   └── southern_hemisphere.pdf
│   │
│   ├── market_specs/                  # Contract specifications
│   │   ├── cme_corn_specs.pdf
│   │   ├── cme_soybean_specs.pdf
│   │   ├── cme_wheat_specs.pdf
│   │   ├── cme_cattle_specs.pdf
│   │   ├── ice_canola_specs.pdf
│   │   └── delivery_points.pdf
│   │
│   ├── glossaries/                    # Terminology
│   │   ├── usda_glossary.pdf
│   │   ├── futures_terminology.pdf
│   │   └── commodity_abbreviations.md
│   │
│   ├── data_dictionaries/             # What each data field means
│   │   ├── wasde_field_definitions.pdf
│   │   ├── export_sales_codes.pdf
│   │   └── ...
│   │
│   └── methodology/                   # How data is collected/calculated
│       ├── usda_survey_methodology.pdf
│       ├── crop_condition_ratings_explained.pdf
│       └── ...
│
├── industry_reports/                  # EXTERNAL ANALYSIS
│   │                                  # Competitor/partner insights
│   │
│   ├── outlook_reports/               # Annual/quarterly outlooks
│   │   ├── usda_baseline_projections.pdf
│   │   ├── fao_food_outlook.pdf
│   │   ├── igc_grain_market_report.pdf
│   │   └── ...
│   │
│   ├── special_reports/               # One-off analysis
│   │   └── ...
│   │
│   └── trusted_sources/               # Regular publications you value
│       ├── source_name/
│       └── ...
│
├── historical/                        # KEY HISTORICAL CONTEXT
│   │                                  # Major events, unusual years
│   │
│   ├── drought_years/
│   │   ├── 2012_us_drought.pdf
│   │   └── ...
│   │
│   ├── trade_disruptions/
│   │   ├── 2018_china_tariffs.pdf
│   │   └── ...
│   │
│   └── market_events/
│       └── ...
│
├── rlc_strat_docs/                    # YOUR STRATEGY DOCUMENTS
│   └── (existing content)
│
└── schemas/                           # DATABASE SCHEMAS
    ├── cattle_on_feed_bronze.sql      # (or link to rlc_scheduler/schemas/)
    └── ...
```

---

## Content Recommendations

### High Value - Get First
| Content | Source | Why It Matters |
|---------|--------|----------------|
| US Crop Production Maps | USDA NASS | Know which states matter for each crop |
| Crop Calendars (US, Brazil, Argentina) | USDA FAS / Attaché Reports | Seasonal timing is everything |
| WASDE Methodology | USDA OCE | Understand how numbers are derived |
| CME Contract Specs | CME Group | Delivery months, sizes, grades |
| State Production Rankings | USDA NASS | Top 10 states per commodity |

### Medium Value - Add Over Time
| Content | Source | Why It Matters |
|---------|--------|----------------|
| USDA Baseline Projections | USDA OCE | 10-year forward outlook |
| FAO Food Outlook | FAO | Global perspective |
| Historical Drought Analysis | Various | Pattern recognition |
| Export Inspection Points | USDA AMS | Geography of trade |
| Feedlot Locations | USDA NASS | Cattle geography |

### Nice to Have
| Content | Source | Why It Matters |
|---------|--------|----------------|
| Competitor Reports | Various | Alternative viewpoints |
| Academic Papers | Universities | Deep methodology |
| Port Capacity Data | Various | Logistics constraints |

---

## Naming Conventions

**Current Reports (data/):**
```
{report_code}_{MMYY}.{ext}
Examples:
  wasde_0126.pdf
  cofd_0126.pdf
  cofd_0126.zip
```

**Reference Materials (reference/):**
```
{topic}_{region}_{version}.{ext}
Examples:
  corn_production_map_us_2024.pdf
  crop_calendar_brazil_2024.pdf
```

---

## Update Cadence

| Folder | Update Frequency |
|--------|------------------|
| data/ | Each report release (replace previous) |
| reference/crop_maps | Annually |
| reference/crop_calendars | Annually |
| reference/market_specs | When contracts change |
| industry_reports | As published |
| historical | As needed |

---

## How the LLM Uses This

1. **Report Analysis**: When analyzing a new WASDE, reference the methodology doc
2. **Geographic Context**: When discussing drought, reference crop maps
3. **Timing Context**: When discussing planting progress, reference crop calendars
4. **Cross-Validation**: Compare your analysis to industry reports
5. **Historical Patterns**: Reference past events for context

---

## Quick Links for Gathering Materials

### USDA Crop Maps
- https://www.nass.usda.gov/Charts_and_Maps/Crops_County/

### USDA Crop Calendars
- https://ipad.fas.usda.gov/ogamaps/cropcalendar.aspx

### CME Contract Specs
- https://www.cmegroup.com/markets/agriculture.html

### USDA Methodology
- https://www.nass.usda.gov/Education_and_Outreach/

