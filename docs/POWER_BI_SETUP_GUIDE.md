# RLC Commodities - Power BI Visualization Setup Guide

## Overview

This guide walks through creating professional, information-dense Power BI dashboards for commodity analysis using data from the `rlc_commodities` PostgreSQL database.

---

## Part 1: Database Connection Setup

### Step 1: Connect Power BI to PostgreSQL

1. Open **Power BI Desktop**
2. **Get Data** → **Database** → **PostgreSQL database**
3. Enter connection details:
   - **Server:** `127.0.0.1`
   - **Database:** `rlc_commodities`
4. Click **OK**
5. Select **Database** authentication
   - **User name:** `postgres`
   - **Password:** `[your password]`
6. Click **Connect**

### Step 2: Load Balance Sheet Views

In the Navigator, expand `gold` schema and select:
- `us_soybeans_balance_sheet_v2`
- `us_corn_balance_sheet_v2`
- `us_soybean_meal_balance_sheet`
- `us_soybean_oil_balance_sheet`
- `world_soybeans_balance_sheet`
- `balance_sheet_universal`

Click **Load** (or **Transform Data** to preview first).

---

## Part 2: Balance Sheet Visualizations

### 2.1 Waterfall Chart - Supply & Demand Flow

**Purpose:** Show how Beginning Stocks flow through supply additions and demand deductions to Ending Stocks.

**Setup:**
1. Insert → **Waterfall Chart**
2. Create a calculated table for waterfall data:

```dax
BalanceSheetWaterfall =
VAR SelectedYear = SELECTEDVALUE(us_soybeans_balance_sheet_v2[marketing_year])
RETURN
UNION(
    ROW("Line Item", "Beginning Stocks", "Value", [beginning_stocks], "Type", "Start", "Order", 1),
    ROW("Line Item", "+ Production", "Value", [production], "Type", "Increase", "Order", 2),
    ROW("Line Item", "+ Imports", "Value", [imports], "Type", "Increase", "Order", 3),
    ROW("Line Item", "= Total Supply", "Value", [total_supply], "Type", "Subtotal", "Order", 4),
    ROW("Line Item", "- Crush", "Value", -[crush], "Type", "Decrease", "Order", 5),
    ROW("Line Item", "- Exports", "Value", -[exports], "Type", "Decrease", "Order", 6),
    ROW("Line Item", "- Seed/Residual", "Value", -([seed]+[residual]), "Type", "Decrease", "Order", 7),
    ROW("Line Item", "= Ending Stocks", "Value", [ending_stocks], "Type", "End", "Order", 8)
)
```

**Alternative - Simpler Approach:**
Use the native Waterfall with:
- **Category:** Create a column with ordered line items
- **Y-axis:** Value
- **Breakdown:** (optional) by year

**Formatting:**
- Increase color: Green (#2E7D32)
- Decrease color: Red (#C62828)
- Total color: Navy (#1A365D)

---

### 2.2 Traditional Balance Sheet Matrix

**Purpose:** Excel-style balance sheet with years as columns.

**Setup:**
1. Insert → **Matrix**
2. Configure:
   - **Rows:** Create a calculated column for line item ordering
   - **Columns:** `marketing_year`
   - **Values:** `value`

**DAX for Line Item Order:**
```dax
LineItemOrder =
SWITCH(
    TRUE(),
    [metric_type] = "beginning_stocks", 1,
    [metric_type] = "production", 2,
    [metric_type] = "imports", 3,
    [metric_type] = "total_supply", 4,
    [metric_type] = "crush", 5,
    [metric_type] = "domestic_total", 6,
    [metric_type] = "exports", 7,
    [metric_type] = "total_use", 8,
    [metric_type] = "ending_stocks", 9,
    [metric_type] = "stocks_to_use_pct", 10,
    99
)
```

**Formatting:**
- Bold subtotals (Total Supply, Total Use)
- Conditional formatting on Ending Stocks (red if low)
- Right-align numbers
- Format as millions with 1 decimal

---

### 2.3 KPI Cards with Sparklines

**Purpose:** Executive summary showing key metrics with trends.

**Setup Option A - New Card Visual:**
1. Insert → **Card (new)**
2. Add fields:
   - **Fields:** `ending_stocks`
   - **Reference labels:** Prior year value
3. Add sparkline via formatting options

**Setup Option B - Multi KPI Custom Visual:**
1. Get from AppSource: **Multi KPI**
2. Configure:
   - **Main Value:** Current year Ending Stocks
   - **Sparkline:** Historical ending stocks
   - **Variance:** vs. prior year

**Key KPIs to Display:**
| KPI | Measure | Alert Threshold |
|-----|---------|-----------------|
| Ending Stocks | Current year | < 200 MMT (red) |
| Stocks-to-Use | Ratio % | < 10% (red) |
| Production | Current year | YoY change |
| Exports | Current year | vs. 5-yr avg |

---

### 2.4 Stocks-to-Use Gauge

**Purpose:** Visual indicator of supply tightness.

**Setup:**
1. Insert → **Gauge**
2. Configure:
   - **Value:** `stocks_to_use_pct`
   - **Minimum:** 0
   - **Maximum:** 30
   - **Target:** 15 (historical average)

**Conditional Formatting:**
- 0-10%: Red (Tight)
- 10-15%: Yellow (Below Average)
- 15-20%: Green (Comfortable)
- 20%+: Blue (Burdensome)

---

### 2.5 Multi-Year Comparison Line Chart

**Purpose:** Trend analysis across marketing years.

**Setup:**
1. Insert → **Line Chart**
2. Configure:
   - **X-axis:** `marketing_year`
   - **Y-axis:** Select metric (production, exports, ending_stocks)
   - **Legend:** (optional) commodity or country for comparison

**Enhancements:**
- Add data labels on endpoints
- Add trend line (Analytics pane)
- Highlight current year with different color

---

### 2.6 Country Comparison - Small Multiples

**Purpose:** Compare balance sheets across countries.

**Setup:**
1. Insert → **Line Chart** or **Clustered Bar**
2. Configure:
   - **X-axis:** `marketing_year`
   - **Y-axis:** `production` (or other metric)
   - **Small multiples:** `country`

**Best For:**
- US vs. Brazil vs. Argentina production
- World ending stocks by major producer
- Export market share changes

---

## Part 3: Trade Flow Visualizations (After Data Import)

### 3.1 Flow Map - Trade Arrows

**Purpose:** Show export flows from origin to destination with weighted arrows.

**Prerequisites:**
- Install **Flow Map** custom visual from AppSource
- Trade data with: origin_country, destination_country, volume

**Setup:**
1. Get custom visual: **Flow Map** by Weiwei Cui
2. Configure:
   - **Origin:** Source country
   - **Destination:** Destination country
   - **Value:** Trade volume (determines arrow width)
   - **Latitude/Longitude:** Or let it geocode country names

**Formatting:**
- Style: Curved lines (great circle)
- Color: By commodity or trade value
- Tooltip: Volume, % of total exports

**Data Required (from Census/Trade Sheets):**
```
| origin | destination | commodity | volume_mt | year |
|--------|-------------|-----------|-----------|------|
| USA    | China       | Soybeans  | 25000000  | 2024 |
| USA    | Mexico      | Soybeans  | 5000000   | 2024 |
| Brazil | China       | Soybeans  | 70000000  | 2024 |
```

---

### 3.2 Destination Ranking Bar Chart

**Purpose:** Top export destinations by volume.

**Setup:**
1. Insert → **Clustered Bar Chart**
2. Configure:
   - **Y-axis:** Destination country
   - **X-axis:** Export volume
   - **Sort:** Descending by volume

**Enhancements:**
- Top 10 filter
- Data labels showing volume
- Conditional formatting by region

---

### 3.3 Trade Balance Waterfall

**Purpose:** Show Exports - Imports = Net Trade Position.

**Setup:**
- Same as Balance Sheet Waterfall
- Categories: Exports (green), Imports (red), Net Position (total)

---

## Part 4: Price Visualizations (After My Market News Integration)

### 4.1 Price Time Series with Bands

**Purpose:** Current price vs. historical range.

**Setup:**
1. Insert → **Area Chart** (for band) + **Line Chart** overlay
2. Configure:
   - **X-axis:** Date
   - **Y-axis:** Price
   - Band: 5-year min/max range
   - Line: Current year price

**DAX Measures:**
```dax
5YrHigh = CALCULATE(MAX(Prices[price]), DATESINPERIOD(...))
5YrLow = CALCULATE(MIN(Prices[price]), DATESINPERIOD(...))
5YrAvg = CALCULATE(AVERAGE(Prices[price]), DATESINPERIOD(...))
```

---

### 4.2 Crush Margin Calculator

**Purpose:** Live crush margin calculation.

**Setup:**
Card visuals showing:
- Soybean Price × 1 bu
- Meal Value (47 lbs × meal price)
- Oil Value (11 lbs × oil price)
- **Gross Processing Value**
- **Crush Margin** = GPV - Bean Cost

---

## Part 5: Dashboard Layout Templates

### Template A: Executive Summary (Single Page)

```
┌─────────────────────────────────────────────────────────────────┐
│  RLC COMMODITIES DASHBOARD          [Commodity Slicer] [Year]   │
├─────────────┬─────────────┬─────────────┬──────────────────────┤
│ PRODUCTION  │   EXPORTS   │   STOCKS    │  STOCKS/USE GAUGE    │
│   [KPI]     │    [KPI]    │    [KPI]    │      [Gauge]         │
├─────────────┴─────────────┴─────────────┴──────────────────────┤
│                                                                 │
│  [WATERFALL CHART - Supply & Demand Flow]                       │
│                                                                 │
├─────────────────────────────────┬───────────────────────────────┤
│  [LINE CHART - 5 Year Trend]    │  [BAR CHART - Top Exporters]  │
│                                 │                               │
└─────────────────────────────────┴───────────────────────────────┘
```

### Template B: Trade Focus (Single Page)

```
┌─────────────────────────────────────────────────────────────────┐
│  GLOBAL TRADE FLOWS              [Commodity] [Year] [Direction] │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  [FLOW MAP - Weighted Arrows from Origins to Destinations]      │
│                                                                 │
├─────────────────────────────────┬───────────────────────────────┤
│  [TOP 10 DESTINATIONS]          │  [MONTHLY TREND LINE]         │
│  [Horizontal Bar Chart]         │  [Line Chart]                 │
├─────────────────────────────────┴───────────────────────────────┤
│  [MATRIX - Country × Month with Volume Values]                  │
└─────────────────────────────────────────────────────────────────┘
```

### Template C: Price & Margins (Single Page)

```
┌─────────────────────────────────────────────────────────────────┐
│  PRICE ANALYSIS                  [Commodity] [Date Range]       │
├──────────────────────┬──────────────────────┬──────────────────┤
│  SOYBEANS            │  SOY MEAL            │  SOY OIL         │
│  $XX.XX              │  $XXX.XX             │  $X.XXXX         │
│  [sparkline]         │  [sparkline]         │  [sparkline]     │
├──────────────────────┴──────────────────────┴──────────────────┤
│                                                                 │
│  [AREA CHART - Price vs 5-Year Range Band]                      │
│                                                                 │
├─────────────────────────────────┬───────────────────────────────┤
│  CRUSH MARGIN                   │  [MARGIN HISTORY LINE CHART]  │
│  [Waterfall: Meal+Oil-Bean]     │                               │
└─────────────────────────────────┴───────────────────────────────┘
```

---

## Part 6: Color Palette & Formatting Standards

### RLC Color Palette

| Usage | Color | Hex Code |
|-------|-------|----------|
| Primary (headers, main series) | Navy Blue | #1A365D |
| Secondary (supporting series) | Steel Blue | #4A6FA5 |
| Accent (highlights) | Gold | #C6963C |
| Positive (gains, surplus) | Forest Green | #2E7D32 |
| Negative (losses, deficit) | Crimson | #C62828 |
| Warning | Amber | #F9A825 |
| Neutral | Slate Gray | #64748B |
| Background | Off-White | #F8FAFC |

### Number Formatting

| Data Type | Format | Example |
|-----------|--------|---------|
| Volume (large) | #,##0.0 "M" | 4,200.5 M |
| Volume (bushels) | #,##0 | 4,200,000,000 |
| Price ($/bu) | $#,##0.00 | $10.25 |
| Price (¢/lb) | #,##0.00 ¢ | 42.50 ¢ |
| Percentage | 0.0% | 8.3% |
| Ratio | 0.00 | 0.08 |

### Font Standards

- **Titles:** Segoe UI Semibold, 14pt
- **Subtitles:** Segoe UI, 12pt, Gray
- **Values:** Segoe UI, 24pt (KPIs), 10pt (tables)
- **Labels:** Segoe UI, 9pt

---

## Part 7: Custom Visuals to Install

From Microsoft AppSource (Get Visuals → From AppSource):

| Visual | Purpose | Priority |
|--------|---------|----------|
| **Flow Map** | Trade flow arrows | HIGH |
| **Zebra BI Tables** | Professional financial tables | HIGH |
| **Zebra BI Cards** | KPI cards with variance | MEDIUM |
| **Multi KPI** | Sparkline KPI cards | MEDIUM |
| **xViz Waterfall** | Advanced waterfall charts | MEDIUM |
| **Drill Down Map PRO** | Geographic exploration | MEDIUM |
| **Bullet Chart** | Actual vs target | LOW |

---

## Part 8: Next Steps Checklist

### Phase 1: Balance Sheets (CURRENT - Data Ready)
- [ ] Load gold.us_soybeans_balance_sheet_v2 in Power BI
- [ ] Create waterfall chart for S&D flow
- [ ] Build KPI cards for key metrics
- [ ] Add stocks-to-use gauge
- [ ] Build 5-year trend line chart

### Phase 2: Trade Data (NEXT - Extract from Excel)
- [ ] Identify Excel trade sheets with historical data
- [ ] Create migration script for trade data
- [ ] Load into bronze.census_trade_raw or similar
- [ ] Install Flow Map custom visual
- [ ] Build trade flow dashboard

### Phase 3: Prices (AFTER - My Market News)
- [ ] Connect to My Market News API/feeds
- [ ] Load price data to database
- [ ] Build price trend charts with bands
- [ ] Create crush margin calculator
- [ ] Build price alert dashboard

### Phase 4: Automation (FINAL)
- [ ] Census API for automated trade updates
- [ ] Scheduled refresh in Power BI Service
- [ ] Email subscriptions for weekly reports

---

## Appendix: SQL Views Available

| View Name | Description | Key Columns |
|-----------|-------------|-------------|
| gold.us_soybeans_balance_sheet_v2 | US Soybeans S&D | production, exports, ending_stocks |
| gold.us_corn_balance_sheet_v2 | US Corn S&D | production, feed, ethanol, exports |
| gold.us_soybean_meal_balance_sheet | US Meal S&D | production, domestic_use, exports |
| gold.us_soybean_oil_balance_sheet | US Oil S&D + biofuels | biodiesel_use, renewable_diesel_use |
| gold.world_soybeans_balance_sheet | All countries | country filter available |
| gold.balance_sheet_universal | Generic pivot | commodity, country filters |
| gold.balance_sheet_with_yoy | With YoY changes | includes _yoy_change columns |

---

*Guide Version: 1.0 | Last Updated: January 2026*
