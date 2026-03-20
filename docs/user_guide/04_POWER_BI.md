# Part 4: Working with Power BI

[← Back to Table of Contents](00_COVER_AND_TOC.md) | [← Previous: Daily Operations](03_DAILY_OPERATIONS.md)

---

## 4.1 Connecting to the Database

Power BI can connect directly to the PostgreSQL database, giving you access to all Gold layer views.

### Prerequisites

1. **Power BI Desktop** installed (Windows only)
2. **PostgreSQL ODBC driver** (usually auto-installed with Power BI)
3. **Database credentials** (host, port, username, password)

### Creating a Connection

**Step 1: Open Power BI Desktop**

Launch Power BI Desktop and create a new report or open an existing one.

**Step 2: Get Data**

1. Click **Home** > **Get Data** > **More...**
2. Search for "PostgreSQL"
3. Select **PostgreSQL database**
4. Click **Connect**

**[GRAPHIC: Get Data Dialog]**
*See [Appendix E](APPENDIX_E_GRAPHICS.md#powerbi-getdata) for graphic specifications*

**Step 3: Enter Connection Details**

| Field | Value |
|-------|-------|
| Server | `your-database-host.com:5432` |
| Database | `rlc_commodities` |
| Data Connectivity mode | Import (recommended) or DirectQuery |

Click **OK**.

**Step 4: Authenticate**

1. Select **Database** authentication
2. Enter your username and password
3. Click **Connect**

💡 **Tip:** Check "Remember my credentials" to avoid re-entering each time.

**Step 5: Select Tables**

The Navigator shows all available schemas and tables:

1. Expand the **gold** schema
2. Check the tables/views you want to import:
   - ☑️ `us_corn_balance_sheet`
   - ☑️ `us_soybeans_balance_sheet`
   - ☑️ `wasde_changes`
3. Click **Load** (or **Transform Data** to preview first)

**[GRAPHIC: Navigator Dialog with Gold Views]**
*See [Appendix E](APPENDIX_E_GRAPHICS.md#powerbi-navigator) for graphic specifications*

### Import vs DirectQuery

| Mode | Pros | Cons | Best For |
|------|------|------|----------|
| **Import** | Fast queries, works offline | Data snapshot only, larger file size | Most use cases |
| **DirectQuery** | Always current data | Slower queries, needs connection | Real-time dashboards |

💡 **Recommendation:** Use Import mode with scheduled refresh for best performance.

### Connection String Reference

If needed for advanced configuration:

```
Host=your-database-host.com;Port=5432;Database=rlc_commodities;Username=your_username;Password=your_password;SSL Mode=Require
```

---

## 4.2 Available Data Tables

### Balance Sheets

These views provide supply & demand data in a wide format suitable for tables and charts.

#### `gold.us_corn_balance_sheet`

| Column | Type | Description |
|--------|------|-------------|
| marketing_year | Text | e.g., "2024/25" |
| area_planted | Decimal | Million acres |
| area_harvested | Decimal | Million acres |
| yield | Decimal | Bushels per acre |
| production | Decimal | Million bushels |
| beginning_stocks | Decimal | Million bushels |
| imports | Decimal | Million bushels |
| total_supply | Decimal | Million bushels |
| feed_residual | Decimal | Million bushels |
| fsi_use | Decimal | Million bushels (Food, Seed, Industrial) |
| ethanol | Decimal | Million bushels |
| exports | Decimal | Million bushels |
| total_use | Decimal | Million bushels |
| ending_stocks | Decimal | Million bushels |
| stocks_to_use | Decimal | Percentage |
| farm_price | Decimal | $/bushel |

Similar views exist for:
- `gold.us_soybeans_balance_sheet`
- `gold.us_wheat_balance_sheet`
- `gold.world_corn_balance_sheet`
- `gold.world_soybeans_balance_sheet`

### Trade Data

#### `gold.us_soybean_exports_by_destination`

| Column | Type | Description |
|--------|------|-------------|
| marketing_year | Text | e.g., "2024/25" |
| month | Date | Month of shipment |
| destination_country | Text | Country name |
| quantity_mt | Decimal | Metric tons |
| quantity_bu | Decimal | Bushels (converted) |
| cumulative_my_mt | Decimal | MY cumulative total |

#### `gold.marketing_year_trade_summary`

| Column | Type | Description |
|--------|------|-------------|
| commodity | Text | Corn, Soybeans, Wheat |
| marketing_year | Text | e.g., "2024/25" |
| total_exports_mt | Decimal | Total exports (MT) |
| usda_forecast_mt | Decimal | USDA projection |
| pace_pct | Decimal | Actual vs. forecast % |

### Positioning Data

#### `gold.cftc_managed_money_net`

| Column | Type | Description |
|--------|------|-------------|
| report_date | Date | Tuesday of report week |
| commodity | Text | Corn, Soybeans, Wheat, etc. |
| long_contracts | Integer | Long positions |
| short_contracts | Integer | Short positions |
| net_position | Integer | Long minus Short |
| change_week | Integer | Change from prior week |
| pct_of_oi | Decimal | Net as % of open interest |

### WASDE Analysis

#### `gold.wasde_changes`

| Column | Type | Description |
|--------|------|-------------|
| release_date | Date | WASDE release date |
| commodity | Text | Corn, Soybeans, Wheat |
| line_item | Text | e.g., "Ending Stocks" |
| region | Text | US, World, etc. |
| marketing_year | Text | e.g., "2024/25" |
| previous_value | Decimal | Last month's estimate |
| current_value | Decimal | This month's estimate |
| change | Decimal | Absolute change |
| pct_change | Decimal | Percentage change |

---

## 4.3 Building Dashboards

### Recommended Dashboard Structure

A well-organized commodity dashboard typically includes:

```
┌────────────────────────────────────────────────────────────────┐
│                     HEADER / FILTERS                            │
│  [Commodity ▼]  [Marketing Year ▼]  [Date Range ▼]  [Refresh]  │
├────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ┌──────────────────────┐  ┌──────────────────────┐            │
│  │    KEY METRICS       │  │   PRICE CHART        │            │
│  │  Stocks-to-Use: 10%  │  │   [Line chart]       │            │
│  │  Ending Stocks: 1.5B │  │                      │            │
│  │  vs. Last Month: -5% │  │                      │            │
│  └──────────────────────┘  └──────────────────────┘            │
│                                                                 │
│  ┌──────────────────────────────────────────────────┐          │
│  │              BALANCE SHEET TABLE                  │          │
│  │  [Pivoted supply/demand with YoY comparison]     │          │
│  └──────────────────────────────────────────────────┘          │
│                                                                 │
│  ┌─────────────────────┐  ┌─────────────────────────┐          │
│  │  EXPORT PACE        │  │  POSITIONING           │          │
│  │  [Progress bar]     │  │  [Bar chart]           │          │
│  └─────────────────────┘  └─────────────────────────┘          │
│                                                                 │
└────────────────────────────────────────────────────────────────┘
```

**[GRAPHIC: Dashboard Layout Template]**
*See [Appendix E](APPENDIX_E_GRAPHICS.md#dashboard-layout) for graphic specifications*

### Key Visualizations

#### 1. Stocks-to-Use Card

Display the most important metric prominently:

**Visual:** Card
**Field:** `stocks_to_use` from `us_corn_balance_sheet`
**Filter:** Current marketing year
**Format:** Percentage with 1 decimal

#### 2. Balance Sheet Table

**Visual:** Matrix
**Rows:** Line items (Production, Imports, Exports, etc.)
**Columns:** Marketing years
**Values:** Numerical values

**DAX for conditional formatting:**
```dax
Change Color =
IF([Change] > 0, "Green",
IF([Change] < 0, "Red", "Gray"))
```

#### 3. Historical Stocks-to-Use Line Chart

**Visual:** Line Chart
**X-Axis:** `marketing_year`
**Y-Axis:** `stocks_to_use`
**Secondary Y-Axis:** `farm_price` (optional)

#### 4. Export Pace Progress Bar

**Visual:** Gauge or Progress Bar
**Value:** Current cumulative exports
**Target:** USDA forecast
**Format:** Show percentage of target

**DAX Measure:**
```dax
Export Pace % =
DIVIDE(
    SUM('trade_summary'[total_exports_mt]),
    SUM('trade_summary'[usda_forecast_mt]),
    0
) * 100
```

#### 5. CFTC Positioning Bar Chart

**Visual:** Clustered Bar Chart
**X-Axis:** `report_date`
**Y-Axis:** `net_position`
**Legend:** Commodity

### Color Standards

Use consistent colors across all RLC dashboards:

| Commodity | Primary Color | Hex Code |
|-----------|---------------|----------|
| Corn | Gold | #FFD700 |
| Soybeans | Green | #228B22 |
| Wheat | Amber | #FFBF00 |
| Cotton | Light Blue | #87CEEB |

| Indicator | Color | Hex Code |
|-----------|-------|----------|
| Positive change | Green | #2E7D32 |
| Negative change | Red | #C62828 |
| Neutral | Gray | #757575 |

---

## 4.4 Template Dashboards

Pre-built Power BI files are available in the `PowerBI/` folder:

| File | Description |
|------|-------------|
| `US Balance Sheets.pbix` | Corn, soybean, wheat S&D |
| `us_soybean_trade_flows.pbix` | Export destinations and pace |
| `USDA Prices.pbix` | AMS cash prices dashboard |
| `First Sample Dashboard - RLC.pbix` | General template |
| `rlc_commodities_theme.pbix` | Theme file for consistent styling |

### Using a Template

1. Open the template `.pbix` file
2. Go to **Home** > **Transform Data** > **Data Source Settings**
3. Update the connection to your database
4. Click **Close & Apply**
5. Data will refresh with your credentials

### Applying the RLC Theme

1. Open your report in Power BI Desktop
2. Go to **View** > **Themes** > **Browse for themes**
3. Select `rlc_commodities_theme.json`
4. Theme applies to all visuals

---

## Scheduled Refresh (Power BI Service)

To keep your dashboards current when published to Power BI Service:

1. **Publish** your report to Power BI Service
2. Go to the dataset settings
3. Under **Scheduled refresh**:
   - Enable scheduled refresh
   - Set frequency (daily recommended)
   - Set time (after morning collections complete)
4. Under **Gateway connection**:
   - Configure on-premises data gateway if required
   - Or use cloud-to-cloud connection if database is cloud-hosted

💡 **Tip:** Schedule refresh after 10:00 AM ET to capture morning data releases.

---

## Best Practices

### Performance

- ✅ Use **Import** mode for most dashboards
- ✅ Import only the tables you need
- ✅ Use date filters to limit data volume
- ✅ Create aggregated views in Gold layer for large datasets
- ❌ Don't import Bronze tables (too large, wrong format)
- ❌ Don't use DirectQuery unless real-time data is essential

### Design

- ✅ Use consistent colors (see RLC theme)
- ✅ Include data freshness indicator (last refresh time)
- ✅ Provide context (YoY comparisons, historical ranges)
- ✅ Label axes and include units
- ❌ Don't overcrowd with too many visuals
- ❌ Don't use 3D charts (harder to read)

### Maintenance

- ✅ Document your data sources
- ✅ Test after database schema changes
- ✅ Archive old versions before major changes
- ✅ Share templates, not just finished reports

---

[← Previous: Daily Operations](03_DAILY_OPERATIONS.md) | [Next: Adding New Data Sources →](05_ADDING_DATA_SOURCES.md)
