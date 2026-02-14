# Power BI KPI Quick Reference

## Commodity KPIs - What to Show and How

### SOYBEANS Dashboard KPIs

| KPI | Visual Type | Data Source | Why This Visual |
|-----|-------------|-------------|-----------------|
| Global Production | Card with sparkline | `fas_psd` WHERE commodity='soybeans' | Immediate impact number |
| US vs Brazil Production | Dual-axis line chart | `fas_psd` US + BR | Shows divergence/convergence |
| Stocks/Use Ratio | Gauge (180°) | Calculated from `fas_psd` | Intuitive "full/empty" visual |
| CFTC Net Position | Gauge + area chart combo | `gold.cftc_sentiment` | Shows current + history |
| Crush Pace vs USDA | Bullet chart | `silver.monthly_realized` | Target vs actual |
| Export Destinations | Donut chart | `bronze.fas_export_sales` | Clean share breakdown |
| Price Trend | Candlestick or line | `silver.futures_price` | Trader-friendly |
| S&D Balance Sheet | Matrix table | `gold.fas_us_soybeans_balance_sheet` | Excel-familiar format |

**Hero Card Layout:**
```
┌─────────────────────────────────────────────────────────────────┐
│ GLOBAL PROD │ US PROD │ BRAZIL PROD │ S/U RATIO │ MM POSITION │
│   410 MMT   │ 121 MMT │   172 MMT   │   28.5%   │  +125K NET  │
│   ▲ +2.1%   │ ▼ -3.2% │   ▲ +8.4%   │  ▼ -1.2pp │   BULLISH   │
└─────────────────────────────────────────────────────────────────┘
```

---

### CORN Dashboard KPIs

| KPI | Visual Type | Data Source | Why This Visual |
|-----|-------------|-------------|-----------------|
| US Production | Card with trend | `fas_psd` WHERE commodity='corn', country='US' | #1 producer |
| Ethanol Grind | Bar chart (monthly) | `silver.monthly_realized` WHERE attribute='ethanol_grind' | Shows seasonal pattern |
| Feed/Residual | Stacked area | `fas_psd` feed_dom_consumption | Dominant use category |
| Export Pace | Progress bar | `bronze.fas_export_sales` accumulated | USDA target tracking |
| Crop Conditions | Line chart (weekly) | `gold.nass_condition_yoy` | Current vs prior year |
| Yield Trend | Line with confidence band | Historical yields | Shows variability |
| China Demand | Bar chart | `fas_psd` China imports | Key demand driver |

**Hero Card Layout:**
```
┌─────────────────────────────────────────────────────────────────┐
│  US PROD   │ ETHANOL  │ EXPORTS │ FEED USE │ ENDING STK │
│  15.1 BBU  │ 5.45 BBU │ 2.3 BBU │ 5.7 BBU  │  1.74 BBU  │
│  ▲ +3.5%   │ ▲ +1.2%  │ ▼ -8.0% │ = flat   │  ▼ -15.2%  │
└─────────────────────────────────────────────────────────────────┘
```

---

### WHEAT Dashboard KPIs

| KPI | Visual Type | Data Source | Why This Visual |
|-----|-------------|-------------|-----------------|
| Global Production | Card | `fas_psd` WHERE commodity='wheat' | Context setting |
| US by Class | Stacked bar | `gold.us_wheat_by_class` | HRW, SRW, HRS breakdown |
| Export Competitors | Grouped bar | Russia, EU, US, Canada, Australia | Market share |
| Flour Production | Line chart | `silver.monthly_realized` flour_production | Demand indicator |
| Black Sea Exports | Line chart | Ukraine + Russia exports | Supply disruption indicator |
| Stocks/Use Global | Gauge | Calculated | Tightness indicator |

---

### SOYBEAN OIL Dashboard KPIs

| KPI | Visual Type | Data Source | Why This Visual |
|-----|-------------|-------------|-----------------|
| US Production | Card | `silver.monthly_realized` oil_production | Monthly realized |
| Biodiesel Demand | Stacked area | Biodiesel + Renewable Diesel | Growing demand segment |
| Stocks | Area chart | `silver.monthly_realized` oil_stocks | Pipeline indicator |
| Price vs Crude | Dual-axis line | Soy oil price + WTI | Correlation analysis |
| Food vs Fuel Split | Donut | Calculated domestic use | Demand composition |

---

### SOYBEAN MEAL Dashboard KPIs

| KPI | Visual Type | Data Source | Why This Visual |
|-----|-------------|-------------|-----------------|
| Production | Card | Calculated from crush | Direct output of crush |
| Export Pace | Bullet chart | Export sales | US competitive position |
| Domestic Feed Use | Treemap | By livestock category | Feed composition |
| Argentina Competition | Line chart | Arg vs US exports | Key competitor |
| Price Spread | Area chart | Meal vs Corn price | Feed value comparison |

---

## Country KPIs

### UNITED STATES Dashboard

| KPI | Visual Type | Data Source |
|-----|-------------|-------------|
| Total Grain Production | Stacked bar | Sum of corn + soy + wheat |
| Crop Condition Ratings | Multi-line chart | `gold.nass_condition_yoy` |
| Export Progress by Commodity | Grouped bullet chart | Export sales |
| Production Map | Choropleth | State-level production |
| Planting Progress | Area chart | `silver.nass_latest_progress` |
| Weather Anomaly | Heatmap | `gold.weather_regional_summary` |

**State Production Map Color Scale:**
- Corn: Gold gradient (light → dark by bushels)
- Soybeans: Green gradient
- Wheat: Brown gradient

---

### BRAZIL Dashboard

| KPI | Visual Type | Data Source |
|-----|-------------|-------------|
| Soy Complex Summary | KPI cards row | CONAB totals |
| Production by State | Brazil map | `gold.brazil_soybean_production` |
| Safrinha vs First Crop | Stacked area | `gold.brazil_corn_production` |
| Export Pace | Line vs prior year | Shipping data |
| Harvest Progress | Progress bars | % complete |
| Real vs USD | Line chart | Currency impact |
| Port Lineup | Bar chart | Vessel counts |

**State Ranking Table:**
```
| Rank | State       | Production | % of Total | YoY Chg |
|------|-------------|------------|------------|---------|
| 1    | Mato Grosso | 45.2 MMT   | 26.3%      | +8.2%   |
| 2    | Paraná      | 22.1 MMT   | 12.9%      | -3.1%   |
| 3    | Rio Grande  | 21.8 MMT   | 12.7%      | +12.5%  |
```

---

### CHINA Dashboard

| KPI | Visual Type | Data Source |
|-----|-------------|-------------|
| Soybean Imports | Card + trend | `fas_psd` China imports |
| Import Sources | Donut (animated) | Brazil vs US vs Arg |
| Monthly Import Pace | Line with range | Historical comparison |
| Crush Margins | Line chart | Calculated |
| Meal Demand | Bar chart | Pig herd proxy |
| Stocks/Use | Gauge | Calculated |

**Import Source Shift Visual:**
Show US share declining in Oct-Feb, Brazil share rising in Mar-Sep

---

## Visual "Wow Factor" Elements

### 1. Animated KPI Cards
- Number counts up on page load
- Arrow pulses when showing change
- Sparkline draws in from left

### 2. Gradient Area Charts
```
Production over time:
     ╱‾‾‾‾‾‾╲
    ╱░░░░░░░░╲      ← Gradient fill (commodity color, 30% opacity)
   ╱░░░░░░░░░░╲
  ╱░░░░░░░░░░░░╲
 ╱░░░░░░░░░░░░░░╲
────────────────────
```

### 3. Waterfall Balance Sheet
```
Beginning   Production   Imports     Domestic    Exports    Ending
Stocks                               Use                    Stocks
  █           ████        █          ████         ██         ███
  │           │           │          │            │          │
  └───────────┴───────────┴──────────┴────────────┴──────────┘
```

### 4. Gauge with History Trail
```
                    ╭───────────────╮
        NET SHORT ←─│   ◉ +125K    │─→ NET LONG
                    ╰───────────────╯
        │░░░░░░░░░░░░░████████████████░░░░░░░░░░░│
       -200K                0                  +200K

        52-week range: ■ min  ─── current  ■ max
```

### 5. Production Map with Data Labels
```
        ┌──────────────────────────┐
        │                          │
        │    MT: 45.2 ●            │
        │           ╲              │
        │     PR: 22.1 ●           │
        │              ╲           │
        │       RS: 21.8 ●         │
        │                          │
        └──────────────────────────┘
```

---

## Dashboard Navigation

**Tab Structure:**
```
┌──────────┬──────────┬──────────┬──────────┬──────────┐
│ SOYBEANS │  CORN    │  WHEAT   │  OIL     │  MEAL    │  ← Commodity tabs
└──────────┴──────────┴──────────┴──────────┴──────────┘

┌──────────┬──────────┬──────────┬──────────┐
│    US    │  BRAZIL  │  CHINA   │  GLOBAL  │  ← Country tabs
└──────────┴──────────┴──────────┴──────────┘
```

Each tab maintains consistent layout but swaps data context.

---

## Color-Coded Conditional Formatting Rules

### Stocks/Use Ratio
| Range | Color | Meaning |
|-------|-------|---------|
| < 8% | `#DC143C` (Crimson) | Critically tight |
| 8-12% | `#FF8C00` (Dark Orange) | Tight |
| 12-18% | `#FFD700` (Gold) | Balanced |
| 18-25% | `#90EE90` (Light Green) | Comfortable |
| > 25% | `#228B22` (Forest Green) | Burdensome |

### YoY Change
| Change | Color | Arrow |
|--------|-------|-------|
| > +10% | `#228B22` | ▲▲ |
| +5% to +10% | `#90EE90` | ▲ |
| -5% to +5% | `#808080` | ─ |
| -10% to -5% | `#FFA07A` | ▼ |
| < -10% | `#DC143C` | ▼▼ |


