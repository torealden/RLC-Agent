# US Soybean Dashboard Specification

## Dashboard Overview

**Purpose:** Comprehensive view of US soybean supply & demand fundamentals, market positioning, and key metrics that drive price action.

**Target Audience:** Commodity traders, analysts, and decision-makers who need quick access to actionable soybean market intelligence.

**Data Refresh:** Daily (prices, CFTC), Weekly (export sales, crop progress), Monthly (WASDE, crush)

---

## Page Layout - Main Page (1920 x 1080)

```
┌─────────────────────────────────────────────────────────────────────────────────────────┐
│  [RLC Logo]    US SOYBEANS DASHBOARD           MY: 2024/25    [Date Slicer] [Refresh]   │
├─────────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                          │
│  ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐         │
│  │ US PROD  │ │  CRUSH   │ │ EXPORTS  │ │ END STK  │ │  S/U %   │ │ CFTC NET │         │
│  │ 4.461 BB │ │ 2.39 BB  │ │ 1.82 BB  │ │ 380 MB   │ │  8.5%    │ │ +125K    │         │
│  │  ▼ -3.2% │ │  ▲ +2.1% │ │  ▼ -8.0% │ │  ▼ -15%  │ │  TIGHT   │ │ BULLISH  │         │
│  └──────────┘ └──────────┘ └──────────┘ └──────────┘ └──────────┘ └──────────┘         │
│                                                                                          │
│  ┌─────────────────────────────────────────┐  ┌─────────────────────────────────────┐   │
│  │                                         │  │   SOYBEAN FUTURES PRICE             │   │
│  │     US S&D BALANCE SHEET (Matrix)       │  │   (Line chart with basis overlay)   │   │
│  │                                         │  │                                     │   │
│  │  Category          2023/24    2024/25   │  │   $14.50 ┐                          │   │
│  │  ─────────────────────────────────────  │  │          │    ╱‾‾╲                  │   │
│  │  Beginning Stocks    264        342     │  │   $13.00 │___╱    ╲___              │   │
│  │  Production        4,165      4,461     │  │          │            ╲___          │   │
│  │  Imports              25         30     │  │   $11.50 │                          │   │
│  │  ─────────────────────────────────────  │  │          └────────────────────      │   │
│  │  TOTAL SUPPLY      4,454      4,833     │  │   Basis: -$0.35 (under)             │   │
│  │  ─────────────────────────────────────  │  │   ── Front Month  -- Basis          │   │
│  │  Crush             2,300      2,390     │  └─────────────────────────────────────┘   │
│  │  Exports           1,695      1,825     │                                            │
│  │  Seed/Residual       150        138     │  ┌─────────────────────────────────────┐   │
│  │  ─────────────────────────────────────  │  │   CROP CONDITIONS                   │   │
│  │  TOTAL USE         4,145      4,353     │  │   (Multi-line: G/E% vs Prior Year)  │   │
│  │  ─────────────────────────────────────  │  │                                     │   │
│  │  Ending Stocks       342        380     │  │   70% ─────╱‾‾‾╲─────               │   │
│  │  Stocks/Use %        8.3%       8.7%    │  │   60% ____╱     ╲____               │   │
│  └─────────────────────────────────────────┘  │   50%                                │   │
│                                               │       May Jun Jul Aug Sep            │   │
│  ┌─────────────────────────────────────────┐  │   ── 2024  -- 2023  ·· 5yr Avg      │   │
│  │   MONTHLY CRUSH PACE (Bullet Chart)     │  └─────────────────────────────────────┘   │
│  │                                         │                                            │
│  │   YTD: 2.18 / 2.39 BB (91.2% of USDA)   │                                            │
│  │                                         │                                            │
│  │   Sep |████████████████|▼|░░| 199 MB    │                                            │
│  │   Oct |█████████████████|▼|░| 202 MB    │                                            │
│  │   Nov |██████████████████|▼| 215 MB     │                                            │
│  │   Dec |█████████████████|▼|░| 210 MB    │                                            │
│  │                                         │                                            │
│  └─────────────────────────────────────────┘                                            │
└─────────────────────────────────────────────────────────────────────────────────────────┘
```

## Page Layout - Page 2: Market & Positioning (1920 x 1080)

```
┌─────────────────────────────────────────────────────────────────────────────────────────┐
│  [RLC Logo]    US SOYBEANS - MARKET & POSITIONING      MY: 2024/25    [Slicers]         │
├─────────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                          │
│  ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐         │
│  │  PRICE   │ │  BASIS   │ │ CFTC NET │ │CROP COND │ │ HARVEST  │ │  WASDE   │         │
│  │ $12.45   │ │ -$0.35   │ │ +125K    │ │   62%    │ │   89%    │ │  CHANGE  │         │
│  │  ▼ -2.1% │ │  WEAK    │ │ BULLISH  │ │  G/E %   │ │ COMPLETE │ │  ▼ -15MB │         │
│  └──────────┘ └──────────┘ └──────────┘ └──────────┘ └──────────┘ └──────────┘         │
│                                                                                          │
│  ┌─────────────────────────────────────────┐  ┌─────────────────────────────────────┐   │
│  │   EXPORT SALES PACE                     │  │   WASDE REVISION HISTORY            │   │
│  │   (Line: Current MY vs Prior MY)        │  │   (Waterfall: How estimates changed)│   │
│  │                                         │  │                                     │   │
│  │        ╱‾‾‾╲                            │  │   Ending Stocks by WASDE Report     │   │
│  │       ╱     ╲___                        │  │                                     │   │
│  │   ___╱              ← On pace           │  │   May   █████████████ 450           │   │
│  │   ─────────────────────                 │  │   Jun   ████████████░ 425  ▼-25     │   │
│  │   Sep Oct Nov Dec Jan Feb Mar...        │  │   Jul   ███████████░░ 400  ▼-25     │   │
│  │                                         │  │   Aug   ██████████░░░ 380  ▼-20     │   │
│  │   ── 2024/25  -- 2023/24  ·· 5yr Avg   │  │   Sep   ██████████░░░ 380   ─        │   │
│  └─────────────────────────────────────────┘  └─────────────────────────────────────┘   │
│                                                                                          │
│  ┌─────────────────────────────────────────┐  ┌─────────────────────────────────────┐   │
│  │   CFTC NET POSITIONING                  │  │   EXPORT DESTINATIONS               │   │
│  │   (Area chart with zero line)           │  │   (Donut: YTD Accumulated)          │   │
│  │                                         │  │                                     │   │
│  │   +200K ─────────────────               │  │        ┌────────────────┐           │   │
│  │         ██████                          │  │        │    CHINA       │           │   │
│  │   +100K █████████                       │  │        │     58%        │           │   │
│  │         ████████████                    │  │        ├────────────────┤           │   │
│  │      0  ════════════════════            │  │        │ Mexico  12%    │           │   │
│  │   -100K        █████████                │  │        │ EU      8%     │           │   │
│  │                    ███████              │  │        │ Japan   6%     │           │   │
│  │   -200K                                 │  │        │ Other   16%    │           │   │
│  │         Jan  Apr  Jul  Oct              │  │        └────────────────┘           │   │
│  └─────────────────────────────────────────┘  └─────────────────────────────────────┘   │
│                                                                                          │
│  ┌─────────────────────────────────────────┐  ┌─────────────────────────────────────┐   │
│  │   STOCKS/USE GAUGE                      │  │   PLANTING / HARVEST PROGRESS       │   │
│  │                                         │  │   (Area chart vs 5-yr avg)          │   │
│  │          ╭─────────────╮                │  │                                     │   │
│  │   TIGHT ─│    8.5%    │─ AMPLE         │  │   100% ──────────────╱‾‾‾‾          │   │
│  │          ╰─────────────╯                │  │    75% ────────────╱                │   │
│  │   5%        10%       15%     20%       │  │    50% ──────────╱                  │   │
│  │   ▲min=5.3%    ▼max=18.2%              │  │    25% ────────╱                    │   │
│  │                                         │  │     0% ──────╱                      │   │
│  │   5-Year Range: 5.3% - 18.2%            │  │        Apr May Jun Jul Aug Sep Oct  │   │
│  └─────────────────────────────────────────┘  └─────────────────────────────────────┘   │
│                                                                                          │
└─────────────────────────────────────────────────────────────────────────────────────────┘
```

---

## Section 1: KPI Cards (Top Row)

### Card 1: US Production
| Property | Value |
|----------|-------|
| **Measure** | `[US Soy Production]` |
| **Format** | #,##0.000 "BB" |
| **Data Source** | `bronze.fas_psd` WHERE commodity='soybeans' AND country_code='US' |
| **Subtitle** | YoY % change with arrow |
| **Card Border** | `#168980` (Secondary Teal) |
| **Conditional Color** | Green if positive YoY, Red if negative |

**DAX Measure:**
```dax
US Soy Production =
VAR CurrentMY = SELECTEDVALUE('Marketing Year'[MarketingYear])
RETURN
CALCULATE(
    SUM(fas_psd[production]),
    fas_psd[commodity] = "soybeans",
    fas_psd[country_code] = "US",
    fas_psd[marketing_year] = CurrentMY
) / 1000  -- Convert to billions
```

### Card 2: Crush
| Property | Value |
|----------|-------|
| **Measure** | `[US Soy Crush]` |
| **Format** | #,##0.00 "BB" |
| **Data Source** | `silver.monthly_realized` OR `bronze.fas_psd` |
| **Subtitle** | YoY % + "NOPA Pace: X%" |

### Card 3: Exports
| Property | Value |
|----------|-------|
| **Measure** | `[US Soy Exports]` |
| **Format** | #,##0.00 "BB" |
| **Data Source** | `bronze.fas_export_sales` accumulated |
| **Subtitle** | "X% of USDA Est" progress |

### Card 4: Ending Stocks
| Property | Value |
|----------|-------|
| **Measure** | `[US Soy Ending Stocks]` |
| **Format** | #,##0 "MB" |
| **Data Source** | `bronze.fas_psd` ending_stocks |
| **Subtitle** | YoY change |

### Card 5: Stocks/Use Ratio
| Property | Value |
|----------|-------|
| **Measure** | `[Stocks to Use Ratio]` |
| **Format** | #0.0% |
| **Data Source** | Calculated: ending_stocks / total_use |
| **Conditional Background** | Color scale from red (<8%) to green (>15%) |
| **Label** | "TIGHT" / "BALANCED" / "AMPLE" |

**DAX Conditional Color:**
```dax
SU Ratio Color =
VAR SU = [Stocks to Use Ratio]
RETURN
SWITCH(
    TRUE(),
    SU < 0.08, "#C62828",      -- Critical (Red)
    SU < 0.12, "#FF9800",      -- Tight (Orange)
    SU < 0.18, "#FFC107",      -- Balanced (Gold)
    SU < 0.25, "#66BB6A",      -- Comfortable (Light Green)
    "#2E7D32"                   -- Ample (Green)
)
```

### Card 6: CFTC Net Position
| Property | Value |
|----------|-------|
| **Measure** | `[CFTC Net Position]` |
| **Format** | +#,##0K / -#,##0K |
| **Data Source** | `gold.cftc_sentiment` |
| **Label** | "BULLISH" / "NEUTRAL" / "BEARISH" |
| **Sparkline** | 8-week trend |

---

## Section 2: S&D Balance Sheet Matrix

**Visual Type:** Matrix

**Rows (Category):**
```
├─ SUPPLY
│   ├─ Beginning Stocks
│   ├─ Production
│   └─ Imports
├─ TOTAL SUPPLY (subtotal, bold)
├─ DEMAND
│   ├─ Crush
│   ├─ Exports
│   └─ Seed & Residual
├─ TOTAL USE (subtotal, bold)
├─ ENDING STOCKS (bold, colored)
└─ STOCKS/USE % (bold, colored)
```

**Columns:** Marketing Years (2020/21, 2021/22, 2022/23, 2023/24, 2024/25E)

**Data Source:** `gold.fas_us_soybeans_balance_sheet` or `bronze.fas_psd`

**Formatting:**
- Header row: `#1B4D4D` background, white text
- Subtotal rows: `#E8F5E9` background
- Ending stocks: Conditional formatting based on S/U ratio
- Numbers: Right-aligned, #,##0 format

**DAX for Matrix:**
```dax
Balance Sheet Value =
VAR Category = SELECTEDVALUE('Balance Sheet Structure'[Category])
VAR MY = SELECTEDVALUE('Marketing Year'[MarketingYear])
RETURN
SWITCH(
    Category,
    "Beginning Stocks", [Beginning Stocks],
    "Production", [Production],
    "Imports", [Imports],
    "Total Supply", [Total Supply],
    "Crush", [Crush],
    "Exports", [Exports],
    "Seed & Residual", [Seed Residual],
    "Total Use", [Total Use],
    "Ending Stocks", [Ending Stocks],
    "Stocks/Use %", [Stocks to Use Ratio],
    BLANK()
)
```

---

## Section 3: Monthly Crush Pace

**Visual Type:** Bullet Chart (Recommended)

**Purpose:** Show monthly crush progress toward USDA annual estimate - instant "on pace or not" assessment

**X-Axis:** Months (Sep through Aug)
**Y-Axis:** Million Bushels

**Data Series:**
1. **Actual Crush** (bars) - `silver.monthly_realized` WHERE attribute='crush'
2. **Expected Pace** (target line) - USDA annual / 12 months
3. **Cumulative %** (data labels) - Running total as % of annual

**Data Source:**
- `silver.monthly_realized` WHERE commodity='soybeans' AND attribute='crush'
- NOPA monthly crush data

**Colors:**
- Actual bars: `#168980` (Secondary Teal)
- Target line: `#C4A35A` (Accent Gold), dashed

**DAX Measures:**
```dax
Monthly Crush Actual =
CALCULATE(
    SUM(monthly_realized[realized_value]),
    monthly_realized[commodity] = "soybeans",
    monthly_realized[attribute] = "crush"
)

Monthly Crush Target =
[US Soy Crush Annual] / 12

Crush YTD Progress =
VAR YTD = CALCULATE(
    SUM(monthly_realized[realized_value]),
    monthly_realized[commodity] = "soybeans",
    monthly_realized[attribute] = "crush",
    FILTER(ALL('Calendar'), 'Calendar'[Date] <= MAX('Calendar'[Date]))
)
VAR Annual = [US Soy Crush Annual]
RETURN
DIVIDE(YTD, Annual, 0)
```

---

## Section 4: Export Sales Pace

**Visual Type:** Line Chart

**Purpose:** Compare current MY export pace vs prior year and 5-year average

**X-Axis:** Week of marketing year (Week 1-52)
**Y-Axis:** Accumulated Exports (Million Bushels)

**Data Series:**
1. **Current MY** - Solid line, `#168980`
2. **Prior MY** - Dashed line, `#5F6B6D`
3. **5-Year Average** - Dotted line, `#C4A35A`
4. **USDA Target** - Horizontal reference line, `#C62828` (if behind pace)

**Data Source:** `bronze.fas_export_sales`

**Annotations:**
- Show "X% ahead/behind pace" label
- Highlight China buying periods

**DAX:**
```dax
Export Pace Current MY =
CALCULATE(
    SUM(fas_export_sales[accumulated_exports]),
    fas_export_sales[commodity] = "soybeans",
    fas_export_sales[marketing_year] = [Current Marketing Year]
)

Export Pace Prior MY =
CALCULATE(
    SUM(fas_export_sales[accumulated_exports]),
    fas_export_sales[commodity] = "soybeans",
    fas_export_sales[marketing_year] = [Current Marketing Year] - 1
)
```

---

## Section 5: Stocks/Use Gauge

**Visual Type:** Gauge (180-degree)

**Purpose:** Show current S/U ratio in context of historical range

**Configuration:**
| Property | Value |
|----------|-------|
| Min | 0% |
| Max | 25% |
| Target | 5-year average S/U |
| Current | Current MY S/U ratio |

**Color Bands:**
- 0-8%: `#C62828` (Critical)
- 8-12%: `#FF9800` (Tight)
- 12-18%: `#FFC107` (Balanced)
- 18-25%: `#2E7D32` (Comfortable)

**Callout Value:** Current S/U with "TIGHT/BALANCED/AMPLE" label

**Secondary Info:**
- 5-Year Range: Min to Max
- Historical Avg: X%

---

## Section 6: CFTC Net Positioning

**Visual Type:** Area Chart with Zero Reference Line

**Purpose:** Show speculative positioning trend

**X-Axis:** Date (rolling 52 weeks)
**Y-Axis:** Net Contracts (thousands)

**Data Source:** `gold.cftc_sentiment` WHERE commodity='soybeans'

**Colors:**
- Positive area (net long): `#66BB6A` with 50% transparency
- Negative area (net short): `#EF5350` with 50% transparency
- Zero line: `#333333`

**Annotations:**
- Show current position value
- Show percentile rank vs 3-year history

---

## Section 7: Export Destinations

**Visual Type:** Donut Chart

**Purpose:** Show breakdown of export destinations (YTD accumulated)

**Data Source:** `bronze.fas_export_sales` grouped by country

**Segments:**
1. China - `#DE2910` (China red)
2. Mexico - `#006847`
3. EU - `#003399`
4. Japan - `#BC002D`
5. Other - `#9E9E9E`

**Center Label:** Total Exports volume

**Tooltip:** Show weekly/monthly change

---

## Section 8: Soybean Futures Price Chart

**Visual Type:** Line Chart with Secondary Axis

**Purpose:** Show price trend with basis overlay for market context

**X-Axis:** Date (rolling 12 months or current marketing year)
**Y-Axis Primary:** Price ($/bushel)
**Y-Axis Secondary:** Basis ($/bushel)

**Data Series:**
1. **Front Month Futures** - Solid line, `#168980`, 3px weight
2. **Cash Price** - Dotted line, `#5F6B6D` (optional, if available)
3. **Basis** - Area fill, `#C4A35A` with 30% transparency

**Data Source:**
- `silver.futures_price` (if available) OR external CME data
- Basis calculated: Cash - Futures

**Reference Lines:**
- 52-week high/low as horizontal bands
- Optional: Cost of production reference

**DAX Measures:**
```dax
Soybean Front Month Price =
CALCULATE(
    LASTNONBLANK(futures_price[settlement_price], 1),
    futures_price[commodity] = "soybeans",
    futures_price[contract_type] = "front_month"
)

Soybean Basis =
[Cash Price] - [Soybean Front Month Price]

Basis Label =
VAR BasisValue = [Soybean Basis]
RETURN
IF(
    BasisValue < 0,
    FORMAT(BasisValue, "$0.00") & " (under)",
    FORMAT(BasisValue, "+$0.00") & " (over)"
)

Price 52 Week High =
CALCULATE(
    MAX(futures_price[settlement_price]),
    futures_price[commodity] = "soybeans",
    DATESINPERIOD('Calendar'[Date], MAX('Calendar'[Date]), -52, WEEK)
)

Price 52 Week Low =
CALCULATE(
    MIN(futures_price[settlement_price]),
    futures_price[commodity] = "soybeans",
    DATESINPERIOD('Calendar'[Date], MAX('Calendar'[Date]), -52, WEEK)
)
```

**Tooltip:**
- Date
- Settlement price
- Daily change ($ and %)
- Basis value
- Percentile rank in 52-week range

---

## Section 9: Crop Conditions

**Visual Type:** Multi-Line Chart

**Purpose:** Track Good/Excellent crop condition ratings vs prior year and 5-year average

**X-Axis:** Week of growing season (May through September)
**Y-Axis:** Good/Excellent % (0-100%)

**Data Series:**
1. **Current Year** - Solid line, `#168980`, 3px weight
2. **Prior Year** - Dashed line, `#5F6B6D`, 2px weight
3. **5-Year Average** - Dotted line, `#C4A35A`, 2px weight
4. **5-Year Range** - Shaded band, `#E8E8E8` (min to max)

**Data Source:** `gold.nass_condition_yoy` or `silver.nass_crop_progress`

**DAX Measures:**
```dax
Soy Condition G/E Current =
CALCULATE(
    SUM(nass_condition[good_pct]) + SUM(nass_condition[excellent_pct]),
    nass_condition[commodity] = "soybeans",
    nass_condition[year] = YEAR(TODAY())
)

Soy Condition G/E Prior Year =
CALCULATE(
    SUM(nass_condition[good_pct]) + SUM(nass_condition[excellent_pct]),
    nass_condition[commodity] = "soybeans",
    nass_condition[year] = YEAR(TODAY()) - 1
)

Soy Condition 5Yr Avg =
CALCULATE(
    AVERAGEX(
        FILTER(
            nass_condition,
            nass_condition[commodity] = "soybeans" &&
            nass_condition[year] >= YEAR(TODAY()) - 5 &&
            nass_condition[year] < YEAR(TODAY())
        ),
        nass_condition[good_pct] + nass_condition[excellent_pct]
    )
)

Condition vs 5Yr Departure =
[Soy Condition G/E Current] - [Soy Condition 5Yr Avg]
```

**Conditional Formatting:**
- Current year line turns red if below 5-year min
- Current year line turns dark green if above 5-year max

**KPI Card (for header row):**
| Property | Value |
|----------|-------|
| **Measure** | `[Soy Condition G/E Current]` |
| **Format** | #0% "G/E" |
| **Subtitle** | vs 5-yr avg with arrow |

---

## Section 10: Planting & Harvest Progress

**Visual Type:** Area Chart with Multiple Series

**Purpose:** Track planting and harvest progress vs prior year and 5-year average

**X-Axis:** Week number or date (Apr-Jun for planting, Sep-Nov for harvest)
**Y-Axis:** Percent complete (0-100%)

**Data Series:**
1. **Current Year** - Solid area, `#168980` with 40% transparency
2. **Prior Year** - Dashed line, `#5F6B6D`
3. **5-Year Average** - Dotted line, `#C4A35A`

**Data Source:** `silver.nass_crop_progress` or `gold.nass_progress_comparison`

**Key Dates to Annotate:**
- 50% planted milestone
- 50% harvested milestone

**DAX Measures:**
```dax
Soy Planting Progress =
CALCULATE(
    MAX(nass_progress[value]),
    nass_progress[commodity] = "soybeans",
    nass_progress[activity] = "planted",
    nass_progress[year] = YEAR(TODAY())
)

Soy Harvest Progress =
CALCULATE(
    MAX(nass_progress[value]),
    nass_progress[commodity] = "soybeans",
    nass_progress[activity] = "harvested",
    nass_progress[year] = YEAR(TODAY())
)

Progress vs 5Yr Avg =
VAR Current = [Soy Harvest Progress]
VAR Avg5Yr = [Harvest Progress 5Yr Avg]
RETURN
Current - Avg5Yr
```

**KPI Cards:**
| Card | Measure | Example |
|------|---------|---------|
| Planting | `[Soy Planting Progress]` | "98% Planted" |
| Harvest | `[Soy Harvest Progress]` | "89% Harvested" |

---

## Section 11: WASDE Revision History

**Visual Type:** Waterfall Chart OR Horizontal Bar with Changes

**Purpose:** Show how USDA estimates have changed over time (particularly ending stocks)

**X-Axis:** WASDE Report Month (May through current)
**Y-Axis:** Million Bushels

**Data Displayed:**
- Starting estimate (first bar, full height)
- Each subsequent month shows the CHANGE (+ green, - red)
- Final bar shows current estimate

**Data Source:** `bronze.wasde_history` or archived WASDE reports

**Fields Tracked:**
- Production
- Crush
- Exports
- Ending Stocks (primary focus)

**Color Coding:**
- Increases: `#2E7D32` (Green)
- Decreases: `#C62828` (Red)
- Total/Current: `#1B4D4D` (Primary Teal)

**DAX Measures:**
```dax
WASDE Ending Stocks =
CALCULATE(
    SUM(wasde_history[ending_stocks]),
    wasde_history[commodity] = "soybeans",
    wasde_history[country] = "US"
)

WASDE Monthly Change =
VAR CurrentMonth = SELECTEDVALUE(wasde_history[report_month])
VAR PriorMonth = CurrentMonth - 1
VAR CurrentValue = [WASDE Ending Stocks]
VAR PriorValue =
    CALCULATE(
        [WASDE Ending Stocks],
        wasde_history[report_month] = PriorMonth
    )
RETURN
CurrentValue - PriorValue

WASDE Change Direction =
IF([WASDE Monthly Change] >= 0, "Increase", "Decrease")

WASDE Total Revision =
VAR FirstEstimate =
    CALCULATE(
        [WASDE Ending Stocks],
        FIRSTNONBLANK(wasde_history[report_month], 1)
    )
VAR CurrentEstimate = [WASDE Ending Stocks]
RETURN
CurrentEstimate - FirstEstimate
```

**Alternative Visual:** Small Multiples
Show separate mini-waterfall for:
- Production revisions
- Export revisions
- Ending Stocks revisions

**KPI Card:**
| Property | Value |
|----------|-------|
| **Measure** | `[WASDE Monthly Change]` |
| **Format** | +#,##0 / -#,##0 "MB" |
| **Label** | "WASDE Change" |
| **Conditional** | Green if positive, Red if negative |

---

## Section 12: Basis Chart (Detailed)

**Visual Type:** Combo Chart (Line + Area)

**Purpose:** Show basis trend at key delivery points

**X-Axis:** Date (rolling 6-12 months)
**Y-Axis:** Basis ($/bushel, typically negative range)

**Data Series:**
1. **Primary Location Basis** - Line, `#168980`
2. **Historical Range** - Shaded band, `#E8E8E8`

**Data Source:** External basis data or calculated from cash/futures

**Reference Lines:**
- Zero line (futures price reference)
- 3-year average basis

---

### Basis Location Hierarchy (IMPORTANT)

The dashboard should automatically use the best available basis location with fallback logic:

| Commodity | Primary Location | Fallback 1 | Fallback 2 | Fallback 3 |
|-----------|------------------|------------|------------|------------|
| **Corn** | Central Illinois | Gulf | Ohio River | PNW |
| **Soybeans** | Central Illinois | Gulf | Ohio River | PNW |
| **Soybean Meal** | Central Illinois | Gulf | Decatur | Rail |
| **Soybean Oil** | Central Illinois | Gulf | Decatur | - |
| **Wheat (SRW)** | Toledo | St. Louis | Gulf | - |
| **Wheat (HRW)** | Kansas City | Gulf | Texas | - |
| **Wheat (HRS)** | Minneapolis | PNW | - | - |
| **Cotton** | Memphis | Dallas | Southeast | - |
| **Other** | *Use any available* | - | - | - |

**Fallback Rule:** If primary location data is unavailable (NULL or stale > 3 days), automatically use the next available location in the hierarchy. Display the actual location being used in the chart subtitle.

**DAX Measures with Fallback Logic:**
```dax
// Location Priority Table (create as calculated table)
Basis Location Priority =
DATATABLE(
    "Commodity", STRING,
    "Priority", INTEGER,
    "Location", STRING,
    {
        {"soybeans", 1, "Central Illinois"},
        {"soybeans", 2, "Gulf"},
        {"soybeans", 3, "Ohio River"},
        {"soybeans", 4, "PNW"},
        {"corn", 1, "Central Illinois"},
        {"corn", 2, "Gulf"},
        {"corn", 3, "Ohio River"},
        {"corn", 4, "PNW"},
        {"soybean_meal", 1, "Central Illinois"},
        {"soybean_meal", 2, "Gulf"},
        {"soybean_meal", 3, "Decatur"},
        {"soybean_oil", 1, "Central Illinois"},
        {"soybean_oil", 2, "Gulf"},
        {"soybean_oil", 3, "Decatur"}
    }
)

// Get best available basis location
Best Available Basis Location =
VAR SelectedCommodity = SELECTEDVALUE(fas_psd[commodity], "soybeans")
VAR LocationsWithData =
    FILTER(
        'Basis Location Priority',
        'Basis Location Priority'[Commodity] = SelectedCommodity &&
        CALCULATE(
            COUNTROWS(basis_data),
            basis_data[location] = 'Basis Location Priority'[Location],
            basis_data[commodity] = SelectedCommodity,
            basis_data[date] >= TODAY() - 3  // Data within last 3 days
        ) > 0
    )
VAR BestLocation =
    MINX(LocationsWithData, 'Basis Location Priority'[Priority])
RETURN
CALCULATE(
    MAX('Basis Location Priority'[Location]),
    'Basis Location Priority'[Commodity] = SelectedCommodity,
    'Basis Location Priority'[Priority] = BestLocation
)

// Get basis value from best available location
Current Basis =
VAR BestLocation = [Best Available Basis Location]
VAR SelectedCommodity = SELECTEDVALUE(fas_psd[commodity], "soybeans")
RETURN
CALCULATE(
    LASTNONBLANK(basis_data[basis], 1),
    basis_data[location] = BestLocation,
    basis_data[commodity] = SelectedCommodity
)

// Subtitle showing which location is being used
Basis Location Label =
VAR BestLocation = [Best Available Basis Location]
VAR IsPrimary = BestLocation = "Central Illinois"  // Adjust per commodity
RETURN
IF(
    IsPrimary,
    BestLocation,
    BestLocation & " (primary unavailable)"
)

Basis Strength Label =
VAR BasisValue = [Current Basis]
VAR AvgBasis = [Basis 3Yr Avg]
RETURN
SWITCH(
    TRUE(),
    ISBLANK(BasisValue), "NO DATA",
    BasisValue > AvgBasis + 0.20, "STRONG",
    BasisValue > AvgBasis - 0.20, "NORMAL",
    "WEAK"
)

Basis Color =
VAR Label = [Basis Strength Label]
RETURN
SWITCH(
    Label,
    "STRONG", "#2E7D32",
    "NORMAL", "#FFC107",
    "WEAK", "#C62828",
    "NO DATA", "#9E9E9E"
)
```

**Chart Subtitle:** Dynamic - shows `[Basis Location Label]` so users know which location is displayed

**Tooltip:**
- Date
- Basis value
- Location (actual location being used)
- vs 3-year avg
- vs prior week
- Note if using fallback location

---

## Slicers & Filters

### Marketing Year Slicer
- **Type:** Dropdown
- **Values:** 2020/21 through 2024/25
- **Default:** Current marketing year

### Date Range Slicer
- **Type:** Between dates
- **Default:** Current marketing year start to today

### Report Month Slicer (for WASDE comparison)
- **Type:** Dropdown
- **Values:** Latest 12 WASDE reports

---

## Drillthrough Pages

### 1. Crush Detail Drillthrough
- Monthly NOPA crush by plant region
- Oil vs Meal production split
- Crush margin calculation

### 2. Export Detail Drillthrough
- Weekly export sales by destination
- Outstanding sales commitments
- Shipment pace vs sales pace

### 3. Historical Comparison Drillthrough
- 10-year S&D history
- Production/yield trends
- Price correlation analysis

---

## Bookmarks

1. **Executive Summary** - KPI cards + S&D matrix only
2. **Demand Focus** - Crush + Exports charts enlarged
3. **Supply Focus** - Production map + yield trends
4. **Full Dashboard** - All visuals visible

---

## Theme Colors Applied

| Element | Color | Hex |
|---------|-------|-----|
| Primary Accent | RLC Teal | `#1B4D4D` |
| Secondary Accent | Secondary Teal | `#168980` |
| Gold Highlight | Accent Gold | `#C4A35A` |
| Background | Light Gray | `#F5F5F5` |
| Card Background | White | `#FFFFFF` |
| Headers | Dark Teal | `#1B4D4D` |
| Positive Change | Green | `#2E7D32` |
| Negative Change | Red | `#C62828` |
| Neutral | Gray | `#5F6B6D` |

---

## Data Model Requirements

### Required Tables in Power BI:

1. **fas_psd** (Bronze)
   - Annual S&D balance sheet data
   - Filter: commodity='soybeans'

2. **fas_export_sales** (Bronze)
   - Weekly export sales
   - Filter: commodity='soybeans'

3. **monthly_realized** (Silver)
   - Monthly crush, stocks data from NOPA
   - Filter: commodity='soybeans'

4. **cftc_sentiment** (Gold)
   - Speculative positioning
   - Filter: commodity='soybeans'

5. **futures_price** (Silver) - NEW
   - Daily settlement prices
   - Filter: commodity='soybeans'

6. **nass_crop_progress** (Silver) - NEW
   - Weekly planting/harvest progress
   - Filter: commodity='soybeans'

7. **nass_condition** (Silver) - NEW
   - Weekly crop condition ratings (G/E/F/P/VP)
   - Filter: commodity='soybeans'

8. **wasde_history** (Bronze) - NEW
   - Monthly WASDE report archive
   - Tracks revisions to S&D estimates

9. **basis_data** (Silver) - NEW (Optional)
   - Cash vs futures spread by location
   - Locations: Gulf, Central IL, etc.

10. **Marketing Year** (Dimension)
    - Created via DAX calculated table
    - Links to all fact tables

11. **Calendar** (Dimension)
    - Standard date dimension
    - Marketing year flag
    - Growing season week number

12. **Balance Sheet Structure** (Dimension)
    - Row ordering for matrix
    - Category names

---

## Refresh Schedule

| Data Source | Frequency | Time | Notes |
|-------------|-----------|------|-------|
| fas_psd | Monthly | 15th, 9:00 AM | After WASDE release |
| fas_export_sales | Weekly | Friday, 9:00 AM | After FAS release |
| monthly_realized (NOPA) | Monthly | 16th, 10:00 AM | After NOPA release |
| cftc_sentiment | Weekly | Monday, 6:00 AM | After COT release |
| futures_price | Daily | 5:00 PM | After market close |
| nass_crop_progress | Weekly | Tuesday, 5:00 PM | Growing season only |
| nass_condition | Weekly | Tuesday, 5:00 PM | Growing season only |
| wasde_history | Monthly | 12th, 1:00 PM | WASDE release day |
| basis_data | Daily | 5:00 PM | If available |

---

## Next Steps for Implementation

1. **Import Theme:** Load `RLC_Commodities_Theme.json` into Power BI
2. **Create Calculated Tables:** Marketing Year, Balance Sheet Structure
3. **Import DAX Measures:** Copy from `RLC_DAX_Measures.dax`
4. **Build KPI Cards:** Start with 6 hero cards
5. **Build S&D Matrix:** Configure rows/columns per spec
6. **Add Charts:** Crush pace, export pace, CFTC positioning
7. **Configure Slicers:** Marketing year, date range
8. **Test Interactions:** Verify cross-filtering
9. **Create Bookmarks:** Executive summary, demand focus, etc.
10. **Publish & Schedule:** Set up refresh schedule

---

## Decisions Made

| Question | Decision |
|----------|----------|
| Crop progress/conditions | YES - Added Section 9 & 10 |
| Price chart | YES - Added Section 8 |
| Basis data | YES - Added Section 12 with location fallback |
| WASDE revisions | YES - Added Section 11 |
| Bullet vs bar charts | BULLET - on-target/off-target monitoring |
| Export destinations timing | YTD Accumulated |
| Two-page layout | YES - Fine for website embedding |
| Futures data source | IBKR collector (temporarily paused, will return) |
| Basis locations | Central IL primary for corn/soy/meal/oil, with automatic fallback hierarchy |
| USDA Comp Layout | 3 MYs: Current (25/26), Prior (24/25), Two Years Ago (23/24) |
| USDA Comp Columns | USDA estimate, Change from last report, RLC estimate |
| Stale Data Indicator | 75% transparent grey overlay when data not updated since last source release |
| Crush Margins | Yes - using implied yields from Fats & Oils (meal ~47.84, oil ~11.79 lbs/bu) |
| Alerts | Interested - visual dashboard alerts for thresholds |
| Website Integration | Power BI Embedded (best UX, stable, handles auth) |
| Separate Dashboards | Soybeans, Soybean Meal, Soybean Oil each get their own dashboard |

---

## Remaining Questions for Iteration

1. **WASDE archive** - Do you have historical WASDE data stored, or do we need to build that table?
2. **Mobile layout** - Should we design a mobile-optimized version for the website?
3. **Alerts/thresholds** - Want to add data-driven alerts (e.g., "Stocks below 300 MB")?
4. **Crush margins** - Should we add a crush margin calculation (CBOT crush spread)?
5. **Stale data indicator** - How should we visually indicate when data is more than X days old?
6. **Website integration** - Will this be embedded via Power BI Embedded, or iframe, or something else?
