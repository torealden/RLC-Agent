# US Soybeans Dashboard - Build Guide

## Quick Start

This guide walks you through building the US Soybeans dashboard in Power BI Desktop.

**Files you'll need:**
- `RLC_Commodities_Theme.json` - Import first for consistent styling
- `US_Soybeans_Dashboard.pq` - Power Query connection scripts
- `US_Soybeans_DAX_Measures.dax` - All calculated measures
- `06_soybean_dashboard_views.sql` - Run against database first

---

## Step 0: Database Setup (One-time)

Run the SQL views file against your database:

```bash
psql -h localhost -U postgres -d rlc_commodities -f database/views/06_soybean_dashboard_views.sql
```

This creates:
- `gold.usda_comp_soybeans` - Pre-formatted USDA comparison data
- `gold.soybean_crush_pace` - Monthly crush vs target
- `gold.soybean_export_pace` - Export pace comparison
- `gold.soybean_crush_margin` - Calculated margins
- `gold.data_freshness` - Stale data tracking
- `gold.soybean_kpi_summary` - KPI metrics

---

## Step 1: Create New Report & Import Theme

1. Open Power BI Desktop
2. **File > New**
3. **View > Themes > Browse for themes**
4. Select `RLC_Commodities_Theme.json`
5. Theme is now applied

---

## Step 2: Connect to Database

1. **Home > Get Data > PostgreSQL database**
2. Server: `localhost`
3. Database: `rlc_commodities`
4. Click **OK**, then **Connect**

### Import these tables/views:

| Schema | Table/View | Purpose |
|--------|------------|---------|
| gold | usda_comp_soybeans | USDA Comp matrix |
| gold | soybean_kpi_summary | KPI cards |
| gold | soybean_crush_pace | Bullet chart |
| gold | soybean_export_pace | Export pace line chart |
| gold | data_freshness | Stale data overlay |
| bronze | fas_psd | Full balance sheet data |
| silver | monthly_realized | Monthly actuals |
| gold | cftc_sentiment | CFTC positioning |

5. Click **Load**

---

## Step 3: Create Dimension Tables

### 3.1 Balance Sheet Structure

1. **Home > Transform Data** (opens Power Query)
2. **Home > New Source > Blank Query**
3. **View > Advanced Editor**
4. Paste this code:

```
let
    Source = Table.FromRecords({
        [Category = "Planted Area", SortOrder = 1, Section = "SUPPLY"],
        [Category = "Harvested Area", SortOrder = 2, Section = "SUPPLY"],
        [Category = "Yield", SortOrder = 3, Section = "SUPPLY"],
        [Category = "Beginning Stocks", SortOrder = 4, Section = "SUPPLY"],
        [Category = "Production", SortOrder = 5, Section = "SUPPLY"],
        [Category = "Imports", SortOrder = 6, Section = "SUPPLY"],
        [Category = "Total Supply", SortOrder = 7, Section = "SUPPLY"],
        [Category = "Crush", SortOrder = 8, Section = "DEMAND"],
        [Category = "Exports", SortOrder = 9, Section = "DEMAND"],
        [Category = "Seed", SortOrder = 10, Section = "DEMAND"],
        [Category = "Residual", SortOrder = 11, Section = "DEMAND"],
        [Category = "Total Use", SortOrder = 12, Section = "DEMAND"],
        [Category = "Ending Stocks", SortOrder = 13, Section = "RESULTS"],
        [Category = "Stocks/Use", SortOrder = 14, Section = "RESULTS"],
        [Category = "Farm Price", SortOrder = 15, Section = "RESULTS"]
    })
in
    Source
```

5. **Home > Close & Apply**

### 3.2 Marketing Year Dimension

Repeat the process with this code:

```
let
    YearList = {2020..2030},
    ToTable = Table.FromList(YearList, Splitter.SplitByNothing(), {"MarketingYear"}),
    AddLabel = Table.AddColumn(ToTable, "MY_Label", each
        Text.From([MarketingYear]-1) & "/" & Text.End(Text.From([MarketingYear]), 2)
    )
in
    AddLabel
```

---

## Step 4: Create Measures

1. **Home > Enter Data** (create a dummy table called "Measures" with one blank row)
2. Select the Measures table
3. **Modeling > New Measure**
4. Copy measures from `US_Soybeans_DAX_Measures.dax` one at a time

**Start with these essential measures:**
- Current Marketing Year
- US Soy Production
- US Soy Production YoY %
- US Soy Crush
- US Soy Exports
- US Soy Ending Stocks
- Stocks to Use Ratio
- Stocks to Use Label
- Stocks to Use Color

---

## Step 5: Build Page 1 - Main Dashboard

### 5.1 Header Row

1. Insert **Text Box** at top
2. Type: "US SOYBEANS DASHBOARD"
3. Font: Segoe UI Semibold, 18pt, color `#1B4D4D`

### 5.2 KPI Cards Row (6 cards)

For each card:

1. **Insert > Visuals > Card**
2. Position: ~150px wide, 80px tall
3. Drag across the top row

**Card 1: US Production**
- Field: Create measure `US Soy Production Display`
- Title: "US PRODUCTION"
- Add second card below for YoY with `US Soy Production YoY Display`

**Card 2: Crush**
- Field: `US Soy Crush Display`
- Title: "CRUSH"

**Card 3: Exports**
- Field: `US Soy Exports Display`
- Title: "EXPORTS"

**Card 4: Ending Stocks**
- Field: `US Soy Ending Stocks Display`
- Title: "ENDING STOCKS"

**Card 5: Stocks/Use**
- Field: `Stocks to Use Display`
- Title: "S/U RATIO"
- Second line: `Stocks to Use Label`
- Format > Card > Background > fx > Use `Stocks to Use Color`

**Card 6: CFTC Position**
- Field: `CFTC Position Display`
- Title: "CFTC NET"
- Second line: `CFTC Position Label`

### 5.3 USDA Comp Matrix (Left side)

1. **Insert > Visuals > Matrix**
2. Size: 500px wide, 400px tall
3. Configuration:
   - Rows: `Balance Sheet Structure[Category]`
   - Columns: Create a hierarchy:
     - Level 1: `usda_comp_soybeans[my_label]`
     - Level 2: Column type (USDA, Change, RLC)
   - Values: `USDA Value`, `Change from Prior Report`, `RLC Estimate`

4. **Format pane:**
   - Column headers: Background `#1B4D4D`, Font white
   - Row headers: Font `#1A1A1A`
   - Values: Right-aligned
   - Grid: Horizontal lines `#E8E8E8`

5. **Conditional formatting on Change column:**
   - Select Change column > Format > Conditional formatting > Font color
   - Rules: > 0 = Green `#2E7D32`, < 0 = Red `#C62828`

### 5.4 Crush Pace Bullet Chart (Right side)

**Option A: Using built-in bullet chart (if available)**

1. **Insert > Visuals > Bullet Chart** (may need to download from AppSource)
2. Configuration:
   - Category: Month name from `soybean_crush_pace`
   - Value: `monthly_crush`
   - Target: `monthly_target`
   - Minimum: 0
   - Maximum: max monthly target × 1.2

**Option B: Using bar chart with reference line**

1. **Insert > Visuals > Clustered Bar Chart**
2. Axis: Month
3. Values: `Monthly Crush Actual`
4. Add reference line: `Monthly Crush Target`
5. Format bar color: `#168980`
6. Format reference line: `#C4A35A`, dashed

### 5.5 Price Chart (Right side)

1. **Insert > Visuals > Line Chart**
2. X-Axis: Date
3. Y-Axis: Settlement Price (from futures_price)
4. Title: "SOYBEAN FUTURES PRICE"
5. Line color: `#168980`
6. Add reference lines for 52-week high/low

### 5.6 Crop Conditions Chart

1. **Insert > Visuals > Line Chart**
2. X-Axis: Week
3. Y-Axis: Good/Excellent %
4. Legend: Year (Current, Prior, 5-Yr Avg)
5. Colors:
   - Current: `#168980` solid
   - Prior: `#5F6B6D` dashed
   - 5-Yr Avg: `#C4A35A` dotted

---

## Step 6: Build Page 2 - Market & Positioning

1. Right-click page tab > **Duplicate Page**
2. Rename to "Market & Positioning"
3. Keep header and KPI cards
4. Replace main visuals with:

### 6.1 Export Sales Pace (Line Chart)
- X-Axis: Week of MY
- Y-Axis: Accumulated Exports
- Lines: Current MY, Prior MY, 5-Year Avg

### 6.2 WASDE Revision History (Waterfall)
1. **Insert > Visuals > Waterfall Chart**
2. Category: Report Month
3. Y-Axis: Ending Stocks Change
4. Increase color: `#2E7D32`
5. Decrease color: `#C62828`
6. Total color: `#1B4D4D`

### 6.3 CFTC Positioning (Area Chart)
1. **Insert > Visuals > Area Chart**
2. X-Axis: Date
3. Y-Axis: Net Position
4. Add reference line at 0
5. Conditional color: Green when positive, Red when negative

### 6.4 Export Destinations (Donut)
1. **Insert > Visuals > Donut Chart**
2. Legend: Country
3. Values: Accumulated Exports
4. Colors: Use country-specific colors from theme

### 6.5 S/U Gauge
1. **Insert > Visuals > Gauge**
2. Value: `Stocks to Use Ratio`
3. Min: 0
4. Max: 0.25
5. Target: 5-year average S/U

### 6.6 Planting/Harvest Progress (Area)
1. **Insert > Visuals > Area Chart**
2. X-Axis: Week
3. Y-Axis: Progress %
4. Lines: Current, Prior, 5-Yr Avg

---

## Step 7: Add Stale Data Overlay

For any visual that should show stale data warning:

1. Select the visual
2. **Format > Effects > Background**
3. Click **fx** for conditional formatting
4. Based on field: `data_freshness[is_stale]`
5. If TRUE, background = `#C0C0C0` with 75% transparency

Alternative: Add a text box that shows/hides based on stale data:
1. Create measure: `Stale Data Message`
2. Add Card visual with this measure
3. Position overlay on top of chart
4. Set background to semi-transparent grey

---

## Step 8: Configure Slicers

### Marketing Year Slicer
1. **Insert > Visuals > Slicer**
2. Field: `Marketing Year[MY_Label]`
3. Format: Dropdown
4. Default: Current MY

### Date Range Slicer
1. **Insert > Visuals > Slicer**
2. Field: `Calendar[Date]`
3. Format: Between

---

## Step 9: Create Bookmarks

1. **View > Bookmarks**
2. Set up desired view, click **Add**
3. Create bookmarks:
   - "Executive Summary" - Just KPIs and matrix
   - "Demand Focus" - Crush and export charts
   - "Full Dashboard" - All visuals

---

## Step 10: Test & Publish

1. **Refresh data** to verify connections
2. Test all interactions and filters
3. **File > Publish > Power BI Service**
4. Set up scheduled refresh

---

## Troubleshooting

**"Column not found" errors:**
- Ensure SQL views were created successfully
- Check column names match exactly (case-sensitive)

**Empty visuals:**
- Check filters aren't excluding all data
- Verify data exists in source tables

**Stale data not showing:**
- Check `data_freshness` view is populated
- Verify date comparison logic in DAX

**Theme not applying:**
- Re-import theme after adding visuals
- Some custom visuals don't respect themes

---

## File Checklist

- [ ] SQL views created in database
- [ ] Theme imported
- [ ] All Power Query connections working
- [ ] Dimension tables created
- [ ] Core measures added
- [ ] Page 1 built with KPIs and matrix
- [ ] Page 2 built with charts
- [ ] Stale data overlay configured
- [ ] Slicers working
- [ ] Bookmarks created
- [ ] Published to service
