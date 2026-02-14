# Power BI Dashboard Design Specifications

## Overview

This document provides detailed visualization specifications for RLC commodity dashboards. Each dashboard is designed to be **visually striking** while delivering actionable insights through carefully chosen KPIs.

**Design Philosophy:**
- Clean, professional aesthetic with strategic use of color
- Hero metrics at the top for immediate impact
- Progressive detail as you scroll down
- Consistent color language across all dashboards
- Dark theme option for dramatic visual impact

---

## Color Palette

### Primary Colors
| Use | Color | Hex |
|-----|-------|-----|
| Bullish/Positive | Green | `#2E8B57` (Sea Green) |
| Bearish/Negative | Red | `#DC143C` (Crimson) |
| Neutral/Current Year | Gold | `#DAA520` |
| Prior Year | Steel Blue | `#4682B4` |
| 5-Year Average | Gray | `#708090` |
| Background (Dark) | Charcoal | `#1E1E1E` |
| Background (Light) | Off-White | `#F5F5F5` |

### Commodity Accent Colors
| Commodity | Primary | Secondary |
|-----------|---------|-----------|
| Soybeans | `#90EE90` (Light Green) | `#228B22` (Forest) |
| Corn | `#FFD700` (Gold) | `#FFA500` (Orange) |
| Wheat | `#DEB887` (Burlywood) | `#8B4513` (Saddle Brown) |
| Soybean Oil | `#87CEEB` (Sky Blue) | `#4169E1` (Royal Blue) |
| Soybean Meal | `#DDA0DD` (Plum) | `#8B008B` (Dark Magenta) |

---

## Dashboard Type 1: Commodity Dashboard

One dashboard per commodity (Soybeans, Corn, Wheat, Soybean Oil, Soybean Meal)

### Page Layout (1920x1080)

```
┌─────────────────────────────────────────────────────────────────────────┐
│  [LOGO]  SOYBEANS DASHBOARD                    [Date] [Refresh Button] │
├─────────────────────────────────────────────────────────────────────────┤
│                          HERO KPI CARDS (Row 1)                         │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐  │
│  │ GLOBAL   │  │ US       │  │ BRAZIL   │  │ STOCKS/  │  │ PRICE    │  │
│  │ PROD     │  │ PROD     │  │ PROD     │  │ USE %    │  │ ($/bu)   │  │
│  │ 410 MMT  │  │ 121 MMT  │  │ 172 MMT  │  │ 28.5%    │  │ $10.25   │  │
│  │ ▲ +2.1%  │  │ ▼ -3.2%  │  │ ▲ +8.4%  │  │ ▼ -1.2pp │  │ ▲ +$0.35 │  │
│  └──────────┘  └──────────┘  └──────────┘  └──────────┘  └──────────┘  │
├─────────────────────────────────────────────────────────────────────────┤
│  PRODUCTION COMPARISON (Row 2)          │  GLOBAL BALANCE (Row 2)      │
│  ┌─────────────────────────────────┐    │  ┌─────────────────────────┐ │
│  │  [Stacked Area Chart]           │    │  │  [Waterfall Chart]      │ │
│  │  US vs Brazil vs Argentina      │    │  │  Supply → Use → Stocks  │ │
│  │  Production over 10 years       │    │  │                         │ │
│  │  with forecast shading          │    │  │                         │ │
│  └─────────────────────────────────┘    │  └─────────────────────────┘ │
├─────────────────────────────────────────────────────────────────────────┤
│  TRADE FLOWS (Row 3)                    │  POSITIONING (Row 3)         │
│  ┌─────────────────────────────────┐    │  ┌─────────────────────────┐ │
│  │  [Sankey or Flow Map]           │    │  │  [Gauge + Line Combo]   │ │
│  │  Export flows: US→China,        │    │  │  CFTC MM Net Position   │ │
│  │  Brazil→China, Arg→EU           │    │  │  with 1-yr percentile   │ │
│  └─────────────────────────────────┘    │  └─────────────────────────┘ │
├─────────────────────────────────────────────────────────────────────────┤
│  S&D BALANCE TABLE (Row 4) - Full Width                                 │
│  ┌─────────────────────────────────────────────────────────────────────┐│
│  │  [Matrix/Table with Conditional Formatting]                         ││
│  │  Marketing Year | Beg Stk | Prod | Imports | Supply | Crush | ...   ││
│  │  2024/25        | 342     | 4435 | 25      | 4802   | 2410  | ...   ││
│  │  2023/24        | 264     | 4162 | 30      | 4456   | 2285  | ...   ││
│  │  (with sparklines in each cell showing trend)                       ││
│  └─────────────────────────────────────────────────────────────────────┘│
└─────────────────────────────────────────────────────────────────────────┘
```

### Specific Visualizations

#### 1. Hero KPI Cards
**Type:** Card with conditional formatting
**Size:** 180px x 120px each
**Design:**
- Large number (48pt font, bold)
- Subtitle with units (12pt, gray)
- YoY change indicator with arrow (colored green/red)
- Subtle gradient background matching commodity color
- Micro-sparkline showing 5-year trend (optional)

**Data Sources:**
```sql
-- Global Production
SELECT SUM(production) as global_prod,
       marketing_year
FROM bronze.fas_psd
WHERE commodity = 'soybeans'
GROUP BY marketing_year;

-- Stocks/Use
SELECT ending_stocks / (domestic_consumption + exports) * 100 as stu
FROM bronze.fas_psd
WHERE commodity = 'soybeans' AND country_code = 'US';
```

#### 2. Production Comparison - Stacked Area Chart
**Type:** Area chart with transparency
**Design:**
- Gradient fills (top to bottom fade)
- US = Gold, Brazil = Green, Argentina = Blue
- Dashed line for 5-year average
- Shaded region for forecast years
- Hover shows exact values

**DAX Measure:**
```dax
Production MMT =
DIVIDE([Production 1000 MT], 1000, 0)
```

#### 3. Global Balance Waterfall
**Type:** Waterfall chart
**Design:**
- Start with Beginning Stocks (gray)
- Add Production (green gradient)
- Add Imports (light green)
- Subtract Domestic Use (red)
- Subtract Exports (orange-red)
- End with Ending Stocks (blue)

#### 4. Trade Flow Visualization
**Type:** Sankey diagram OR Chord diagram
**Design:**
- Left side: Exporters (US, Brazil, Argentina)
- Right side: Importers (China, EU, Mexico, Japan)
- Flow width = volume
- Color by exporter

**Alternative:** World map with animated flow arrows

#### 5. CFTC Positioning Gauge
**Type:** Gauge + Line chart combo
**Design:**
- Semi-circle gauge showing current position
- Color zones: Red (net short) → Yellow (neutral) → Green (net long)
- Historical line below showing 52-week trend
- Reference lines at 1-year high/low

**Data:**
```sql
SELECT commodity, mm_net, mm_percentile, sentiment
FROM gold.cftc_sentiment
WHERE commodity = 'Soybeans';
```

#### 6. S&D Balance Table
**Type:** Matrix with conditional formatting
**Design:**
- Header row: Marketing years (columns)
- Row categories: S&D line items
- Cell formatting:
  - Green highlight for stocks building
  - Red highlight for stocks drawing
  - Bold for current year
  - Italic for forecasts
- Mini sparklines in rightmost column showing 10-year trend
- Stocks/Use row with data bars

---

## Dashboard Type 2: Country Dashboard

One dashboard per major country (US, Brazil, Argentina, China)

### US Dashboard Layout

```
┌─────────────────────────────────────────────────────────────────────────┐
│  [FLAG]  UNITED STATES AGRICULTURAL DASHBOARD            [Date Range]  │
├─────────────────────────────────────────────────────────────────────────┤
│  COMMODITY SNAPSHOT CARDS                                               │
│  ┌─────────┐ ┌─────────┐ ┌─────────┐ ┌─────────┐ ┌─────────┐           │
│  │ CORN    │ │SOYBEANS │ │ WHEAT   │ │ SOY OIL │ │SOY MEAL │           │
│  │ Prod    │ │ Prod    │ │ Prod    │ │ Prod    │ │ Prod    │           │
│  │ S/U%    │ │ S/U%    │ │ S/U%    │ │ S/U%    │ │ S/U%    │           │
│  └─────────┘ └─────────┘ └─────────┘ └─────────┘ └─────────┘           │
├────────────────────────────────┬────────────────────────────────────────┤
│  PRODUCTION MAP (Left)         │  CROP CONDITIONS (Right)               │
│  ┌────────────────────────┐    │  ┌──────────────────────────────────┐ │
│  │  [US State Choropleth] │    │  │  [Line Chart]                    │ │
│  │  Production by state   │    │  │  Good/Excellent % by week        │ │
│  │  Color intensity =     │    │  │  Current vs Prior Year vs 5yr   │ │
│  │  production volume     │    │  │                                  │ │
│  └────────────────────────┘    │  └──────────────────────────────────┘ │
├────────────────────────────────┴────────────────────────────────────────┤
│  EXPORT PROGRESS                                                        │
│  ┌─────────────────────────────────────────────────────────────────────┐│
│  │  [Bullet Chart or Progress Bar]                                     ││
│  │  Corn:     ████████████░░░░░░░░  62% of USDA est (1.85B bu)        ││
│  │  Soybeans: ██████████████░░░░░░  78% of USDA est (1.82B bu)        ││
│  │  Wheat:    ████████░░░░░░░░░░░░  45% of USDA est (850M bu)         ││
│  └─────────────────────────────────────────────────────────────────────┘│
├────────────────────────────────┬────────────────────────────────────────┤
│  MONTHLY CRUSH TRACKING        │  WEATHER SUMMARY                       │
│  ┌────────────────────────┐    │  ┌──────────────────────────────────┐ │
│  │  [Bar + Line Combo]    │    │  │  [Regional Heatmap]              │ │
│  │  Monthly crush bars    │    │  │  Temp departure by Corn Belt    │ │
│  │  vs USDA pace line     │    │  │  state, precip anomaly overlay  │ │
│  └────────────────────────┘    │  └──────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────────────┘
```

### Brazil Dashboard Layout

```
┌─────────────────────────────────────────────────────────────────────────┐
│  [FLAG]  BRAZIL AGRICULTURAL DASHBOARD                   [Date Range]  │
├─────────────────────────────────────────────────────────────────────────┤
│  SOY COMPLEX HERO METRICS                                               │
│  ┌───────────┐ ┌───────────┐ ┌───────────┐ ┌───────────┐               │
│  │ SOYBEAN   │ │ SOY MEAL  │ │ SOY OIL   │ │ CRUSH     │               │
│  │ PROD      │ │ PROD      │ │ PROD      │ │ MARGIN    │               │
│  │ 172 MMT   │ │ 55 MMT    │ │ 14.5 MMT  │ │ $42/MT    │               │
│  └───────────┘ └───────────┘ └───────────┘ └───────────┘               │
├────────────────────────────────┬────────────────────────────────────────┤
│  PRODUCTION BY STATE (Map)     │  EXPORT DESTINATIONS (Donut)          │
│  ┌────────────────────────┐    │  ┌──────────────────────────────────┐ │
│  │  [Brazil State Map]    │    │  │  [Donut Chart]                   │ │
│  │  MT, PR, RS, GO, MS    │    │  │  China 70%, EU 12%, Other 18%   │ │
│  │  sized by production   │    │  │                                  │ │
│  └────────────────────────┘    │  └──────────────────────────────────┘ │
├────────────────────────────────┴────────────────────────────────────────┤
│  CONAB S&D BALANCE SHEET                                                │
│  ┌─────────────────────────────────────────────────────────────────────┐│
│  │  [Matrix - styled like Excel screenshot]                            ││
│  │  Soybean S&D | Meal S&D | Oil S&D                                   ││
│  │  Tabbed or stacked                                                  ││
│  └─────────────────────────────────────────────────────────────────────┘│
├────────────────────────────────┬────────────────────────────────────────┤
│  PLANTING/HARVEST PROGRESS     │  HISTORICAL YIELD TREND                │
│  ┌────────────────────────┐    │  ┌──────────────────────────────────┐ │
│  │  [Area Progress Chart] │    │  │  [Line with Confidence Band]    │ │
│  │  % complete vs 5yr avg │    │  │  Yield MT/ha with trend line    │ │
│  └────────────────────────┘    │  └──────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────────────┘
```

### China Dashboard Layout

```
┌─────────────────────────────────────────────────────────────────────────┐
│  [FLAG]  CHINA IMPORT & DEMAND DASHBOARD                 [Date Range]  │
├─────────────────────────────────────────────────────────────────────────┤
│  IMPORT METRICS                                                         │
│  ┌───────────┐ ┌───────────┐ ┌───────────┐ ┌───────────┐               │
│  │ SOYBEAN   │ │ SOYBEAN   │ │ CRUSH     │ │ MEAL      │               │
│  │ IMPORTS   │ │ STOCKS    │ │ VOLUME    │ │ DEMAND    │               │
│  │ 103 MMT   │ │ 15 MMT    │ │ 91 MMT    │ │ 72 MMT    │               │
│  └───────────┘ └───────────┘ └───────────┘ └───────────┘               │
├────────────────────────────────┬────────────────────────────────────────┤
│  IMPORT SOURCES (Stacked Bar)  │  MONTHLY IMPORT PACE (Line)           │
│  ┌────────────────────────┐    │  ┌──────────────────────────────────┐ │
│  │  Brazil | US | Arg     │    │  │  Monthly imports vs 5yr range   │ │
│  │  % share over time     │    │  │  Shaded confidence band         │ │
│  └────────────────────────┘    │  └──────────────────────────────────┘ │
├────────────────────────────────┴────────────────────────────────────────┤
│  CHINA SOY COMPLEX BALANCE SHEET                                        │
│  ┌─────────────────────────────────────────────────────────────────────┐│
│  │  [Matrix - matching Excel format]                                   ││
│  │  Imports | Supply | Crush | Stocks | Stocks/Use                     ││
│  └─────────────────────────────────────────────────────────────────────┘│
└─────────────────────────────────────────────────────────────────────────┘
```

---

## Visual Design Details

### Card Design Specification

```
┌────────────────────────────┐
│  ░░░░░░░░░░░░░░░░░░░░░░░░  │  ← Subtle gradient header
│                            │
│      172.5                 │  ← Main metric (48pt, bold)
│      MMT                   │  ← Unit (14pt, gray)
│                            │
│   ▲ +8.4% YoY              │  ← Change indicator (16pt, green/red)
│   ═══════════════          │  ← Mini sparkline (optional)
│   BRAZIL SOYBEANS          │  ← Label (12pt, caps)
└────────────────────────────┘
```

### Table Conditional Formatting Rules

| Condition | Format |
|-----------|--------|
| Stocks/Use < 10% | Red background, white text |
| Stocks/Use 10-15% | Orange background |
| Stocks/Use 15-20% | Yellow background |
| Stocks/Use > 20% | Green background |
| YoY Change > +5% | Green arrow, bold |
| YoY Change < -5% | Red arrow, bold |
| Forecast year | Italic text |
| Current year | Bold text, gold highlight |

### Chart Styling Guidelines

1. **Line Charts:**
   - 3px line weight for current year
   - 1.5px for comparisons
   - Dashed for averages
   - Gradient area fill below (20% opacity)

2. **Bar Charts:**
   - Rounded corners (4px radius)
   - Gradient fills (left to right)
   - Subtle shadow for depth
   - Gap ratio: 30%

3. **Maps:**
   - Use sequential color scale (light to dark)
   - Include legend with clear breaks
   - Hover tooltip with details
   - Consider 3D extrusion for impact

4. **Gauges:**
   - 180-degree arc
   - Clear color zones (red/yellow/green)
   - Current value prominently displayed
   - Target line if applicable

---

## Data Sources by Dashboard

### Commodity Dashboard Data Sources

| Visual | Gold View / Table | Key Columns |
|--------|-------------------|-------------|
| Global Production | `bronze.fas_psd` | commodity, country_code, marketing_year, production |
| US Balance Sheet | `gold.fas_us_soybeans_balance_sheet` | All columns |
| Brazil Production | `gold.brazil_soybean_production` | crop_year, state, production_mmt, production_yoy_pct |
| CFTC Positioning | `gold.cftc_sentiment` | commodity, mm_net, mm_percentile, sentiment |
| Price Data | `silver.futures_price` | commodity, price_date, settle_price |

### Country Dashboard Data Sources

| Visual | Gold View / Table | Key Columns |
|--------|-------------------|-------------|
| US Crop Conditions | `gold.nass_condition_yoy` | commodity, week_ending, current_ge_pct, prior_year_ge_pct |
| US Monthly Crush | `silver.monthly_realized` | commodity, calendar_year, month, realized_value |
| Brazil by State | `gold.brazil_soybean_production` | crop_year, state, production_mmt |
| Weather Summary | `gold.weather_regional_summary` | region, avg_temp_f, total_precip_in |

---

## Implementation Priority

### Phase 1 (Immediate)
1. **US Soybeans Dashboard** - Most complete data
2. **Brazil Soybeans Dashboard** - CONAB data ready
3. **Global Soybeans Dashboard** - FAS PSD data ready

### Phase 2 (Next)
4. US Corn Dashboard
5. US Wheat Dashboard
6. China Import Dashboard

### Phase 3 (Future)
7. Argentina Dashboard
8. Energy/Biofuels Dashboard
9. Executive Summary Dashboard

---

## Power BI Template Files Needed

1. `RLC_Commodity_Template.pbit` - Base template with theme
2. `RLC_Soybeans_Dashboard.pbix` - Complete soybeans dashboard
3. `RLC_Corn_Dashboard.pbix` - Complete corn dashboard
4. `RLC_US_Country_Dashboard.pbix` - US country view
5. `RLC_Brazil_Country_Dashboard.pbix` - Brazil country view

---

## Recommended Power BI Custom Visuals

| Visual Type | Marketplace Name | Use Case |
|-------------|-----------------|----------|
| Sankey | Sankey Chart by ChartExpo | Trade flows |
| Bullet Chart | Bullet Chart by OKViz | Progress tracking |
| Infographic | Infographic Designer | Hero cards |
| Map | Icon Map | Production by region |
| Gauge | Linear Gauge | CFTC positioning |
| Sparklines | Sparkline by OKViz | Table trends |
