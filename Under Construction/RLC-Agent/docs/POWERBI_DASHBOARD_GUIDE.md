# Power BI Commodity Dashboard Guide

## Quick Start

### Step 1: Export Your Data
```powershell
# Export to Excel (recommended for first time)
python scripts/export_cached_data.py --format xlsx --output ./exports/commodity_data.xlsx

# OR export to CSV files
python scripts/export_cached_data.py --format csv --output ./exports/
```

### Step 2: Import into Power BI
1. Open **Power BI Desktop**
2. Click **Get Data** → **Excel** (or Text/CSV)
3. Select `commodity_data.xlsx`
4. Check all sheets and click **Load**

---

## Dashboard Design: HigbyBarrett Weekly Report

### Page 1: Executive Summary

**Layout:**
```
┌─────────────────────────────────────────────────────────────────┐
│  COMMODITY MARKET DASHBOARD              📅 Week of Dec 9, 2024 │
├─────────────────────────────────────────────────────────────────┤
│  ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐            │
│  │ Ethanol  │ │ Gasoline │ │   Corn   │ │ Soybeans │            │
│  │ +2.3%    │ │  -1.1%   │ │  +0.5%   │ │  -2.1%   │            │
│  └──────────┘ └──────────┘ └──────────┘ └──────────┘            │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  [    ETHANOL PRODUCTION & STOCKS CHART (Line + Area)     ]    │
│                                                                 │
├─────────────────────────────────────────────────────────────────┤
│  ┌────────────────────────┐  ┌─────────────────────────────┐   │
│  │  PRODUCTION by Region  │  │   KEY METRICS TABLE         │   │
│  │     [Pie/Donut]        │  │   Production: XXX kbd       │   │
│  │                        │  │   Stocks: XXX MB            │   │
│  │                        │  │   Days Supply: XX.X         │   │
│  └────────────────────────┘  └─────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────┘
```

**Card Visuals (Top Row):**
- Create 4 Card visuals showing week-over-week % change
- Use conditional formatting: Green for positive, Red for negative
- Add arrows (▲ ▼) in the title

**Main Chart - Ethanol Production & Stocks:**
- Visual Type: **Line and Stacked Column Chart**
- Axis: `week_ending`
- Column Values: `production_kbd` (Ethanol Production)
- Line Values: `stocks_kb` (Ethanol Stocks)
- Format: Use dual Y-axes

---

### Page 2: Ethanol Deep Dive

**Layout:**
```
┌─────────────────────────────────────────────────────────────────┐
│  ETHANOL MARKET ANALYSIS                                        │
├─────────────────────────────────────────────────────────────────┤
│  ┌─────────────────────────────────────────────────────────┐   │
│  │                                                         │   │
│  │    PRODUCTION vs STOCKS (52-Week History)               │   │
│  │    [Combo Chart - Bars for Production, Line for Stocks] │   │
│  │                                                         │   │
│  └─────────────────────────────────────────────────────────┘   │
├─────────────────────────────────────────────────────────────────┤
│  ┌──────────────────────┐  ┌────────────────────────────────┐  │
│  │                      │  │                                │  │
│  │  IMPLIED DEMAND      │  │   YEAR-OVER-YEAR COMPARISON    │  │
│  │  [Area Chart]        │  │   [Table with sparklines]      │  │
│  │                      │  │                                │  │
│  └──────────────────────┘  └────────────────────────────────┘  │
├─────────────────────────────────────────────────────────────────┤
│  ┌──────────────────────────────────────────────────────────┐  │
│  │                                                          │  │
│  │   DAYS SUPPLY TREND  [Line Chart with reference band]   │  │
│  │                                                          │  │
│  └──────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
```

**Creating the Days Supply Chart:**
1. Create a new measure:
   ```dax
   Days Supply =
   DIVIDE(
       SUM(ethanol_data[stocks_kb]),
       SUM(ethanol_data[production_kbd]) * 7,
       0
   )
   ```
2. Add constant line at 22 days (historical average)
3. Use conditional formatting: Red when below 20, Green when above 25

---

### Page 3: Petroleum & Energy

**Layout:**
```
┌─────────────────────────────────────────────────────────────────┐
│  PETROLEUM MARKET                                               │
├─────────────────────────────────────────────────────────────────┤
│  ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐           │
│  │  Crude   │ │ Gasoline │ │ Distill. │ │ Propane  │           │
│  │ Stocks   │ │ Stocks   │ │  Stocks  │ │  Stocks  │           │
│  │ XXX MB   │ │  XXX MB  │ │  XXX MB  │ │  XXX MB  │           │
│  └──────────┘ └──────────┘ └──────────┘ └──────────┘           │
├─────────────────────────────────────────────────────────────────┤
│  ┌─────────────────────────────────────────────────────────┐   │
│  │                                                         │   │
│  │   PETROLEUM STOCKS - 5 YEAR RANGE                       │   │
│  │   [Area Chart showing current vs 5-yr avg/range]        │   │
│  │                                                         │   │
│  └─────────────────────────────────────────────────────────┘   │
├─────────────────────────────────────────────────────────────────┤
│  ┌──────────────────────┐  ┌────────────────────────────────┐  │
│  │  REFINERY            │  │   IMPORTS / EXPORTS            │  │
│  │  UTILIZATION %       │  │   [Clustered Bar Chart]        │  │
│  │  [Gauge Visual]      │  │                                │  │
│  └──────────────────────┘  └────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
```

---

### Page 4: Agriculture/Crops

**Layout:**
```
┌─────────────────────────────────────────────────────────────────┐
│  AGRICULTURAL COMMODITIES                                       │
├─────────────────────────────────────────────────────────────────┤
│  ┌─────────────────────────────────────────────────────────┐   │
│  │                                                         │   │
│  │   CROP PRODUCTION - US TOTALS BY COMMODITY              │   │
│  │   [Stacked Bar Chart by Marketing Year]                 │   │
│  │                                                         │   │
│  └─────────────────────────────────────────────────────────┘   │
├─────────────────────────────────────────────────────────────────┤
│  ┌──────────────────────────────┐ ┌─────────────────────────┐  │
│  │                              │ │                         │  │
│  │   CORN                       │ │   SOYBEANS              │  │
│  │   [Multi-row Card]           │ │   [Multi-row Card]      │  │
│  │   Production: XX.X BB        │ │   Production: XX.X BB   │  │
│  │   Exports: XX.X BB           │ │   Crush: XX.X BB        │  │
│  │   Ethanol Use: XX.X BB       │ │   Exports: XX.X BB      │  │
│  │   Ending Stocks: XX.X BB     │ │   Ending Stocks: XX.X   │  │
│  │                              │ │                         │  │
│  └──────────────────────────────┘ └─────────────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
```

---

## DAX Measures to Create

Add these measures in Power BI for enhanced analytics:

### Week-over-Week Change
```dax
WoW Change % =
VAR CurrentWeek = MAX(ethanol_data[week_ending])
VAR PriorWeek = DATEADD(MAX(ethanol_data[week_ending]), -7, DAY)
VAR CurrentValue = CALCULATE(SUM(ethanol_data[production_kbd]), ethanol_data[week_ending] = CurrentWeek)
VAR PriorValue = CALCULATE(SUM(ethanol_data[production_kbd]), ethanol_data[week_ending] = PriorWeek)
RETURN
DIVIDE(CurrentValue - PriorValue, PriorValue, 0)
```

### Year-over-Year Change
```dax
YoY Change % =
VAR CurrentWeek = MAX(ethanol_data[week_ending])
VAR PriorYear = DATEADD(MAX(ethanol_data[week_ending]), -364, DAY)
VAR CurrentValue = CALCULATE(SUM(ethanol_data[production_kbd]), ethanol_data[week_ending] = CurrentWeek)
VAR PriorValue = CALCULATE(SUM(ethanol_data[production_kbd]), ethanol_data[week_ending] = PriorYear)
RETURN
DIVIDE(CurrentValue - PriorValue, PriorValue, 0)
```

### Rolling 4-Week Average
```dax
4-Week Avg Production =
AVERAGEX(
    DATESINPERIOD(
        ethanol_data[week_ending],
        MAX(ethanol_data[week_ending]),
        -28,
        DAY
    ),
    [Production KBD]
)
```

### Days of Supply
```dax
Days Supply =
DIVIDE(
    [Total Stocks KB],
    [Daily Production KB],
    0
)
```

---

## Visual Formatting Tips

### Color Palette (HigbyBarrett Style)
```
Primary Blue:    #1E3A5F
Secondary Blue:  #3E6B9B
Accent Gold:     #D4A843
Positive Green:  #2E7D32
Negative Red:    #C62828
Neutral Gray:    #757575
Background:      #F5F5F5
```

### Apply Theme
1. Go to **View** → **Themes** → **Customize current theme**
2. Set the colors above
3. Save as "HigbyBarrett Theme"

### Card Visual Settings
- Font size: 28pt for main number
- Data label: 12pt
- Category label: 10pt, gray
- Background: White with subtle shadow
- Border: None or 1px light gray

### Chart Best Practices
1. **Always include titles** - Clear, descriptive titles
2. **Add data labels** on important points
3. **Use gridlines sparingly** - Light, subtle
4. **Consistent date formatting** - MMM DD, YYYY
5. **Include tooltips** with context
6. **Add trend lines** where helpful

---

## Creating the Weekly Report

### Automated Report Features

1. **Date Slicer** - Add a date slicer set to "Relative" → "Last 52 weeks"

2. **Report Title with Dynamic Date**
   Create a measure:
   ```dax
   Report Title =
   "Weekly Market Report - Week Ending " &
   FORMAT(MAX(ethanol_data[week_ending]), "MMMM DD, YYYY")
   ```

3. **Export to PDF**
   - File → Export → Export to PDF
   - This creates a shareable PDF report

4. **Email Subscription** (Power BI Service)
   - Publish to Power BI Service
   - Set up email subscription for weekly delivery

---

## Quick Wins for Impressive Demo

### 1. KPI Cards with Sparklines
Use the "KPI" visual with:
- Value: Current week's production
- Trend: Production over last 12 weeks
- Target: Same week last year

### 2. Decomposition Tree
Great for exploring "why" questions:
- Start with Total Production
- Break down by: Region → Product Type → Facility

### 3. Key Influencers Visual
Shows what drives high/low values:
- Analyze: Production levels
- By: Weather, prices, stocks, etc.

### 4. Smart Narrative
Auto-generates text insights:
- Insert → AI Visuals → Smart Narrative
- Automatically describes trends

### 5. Custom Tooltip Pages
Create detailed hover information:
1. Create a new page
2. Set Page Size to "Tooltip"
3. Add detailed charts
4. Set as tooltip for main visuals

---

## Sample Dashboard Screenshots

After building, your dashboard should have:

**Page 1: Executive Summary**
- 4 KPI cards at top
- Main combo chart (production + stocks)
- Pie chart for regional breakdown
- Key metrics table

**Page 2: Ethanol Detail**
- 52-week production history
- Implied demand trend
- Days supply gauge
- YoY comparison table

**Page 3: Petroleum**
- Stock levels by product
- 5-year range chart
- Refinery utilization
- Trade balance

**Page 4: Agriculture**
- Supply/Demand balance
- Commodity cards (Corn, Soybeans)
- Export trends

---

## Troubleshooting

**"No data showing in visual"**
- Check filters aren't excluding all data
- Verify date column is recognized as date type
- Check for null values in key columns

**"Relationships not working"**
- Go to Model view
- Create relationships between tables
- Use common columns (date, commodity, etc.)

**"Slow performance"**
- Import only needed columns
- Create calculated columns instead of measures where possible
- Use DirectQuery only when necessary

---

## Next Steps After Demo

1. **Publish to Power BI Service** for sharing
2. **Create mobile layout** for phone viewing
3. **Set up scheduled refresh** (requires gateway)
4. **Build additional pages** for specific commodities
5. **Add more data sources** as collectors are fixed

---

*Generated by RLC-Agent Commodity Pipeline*
