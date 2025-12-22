# Mind-Blowing Commodity Visualizations
## Design Inspiration for RLC Reports and Website

---

## Design Philosophy: The Tufte Principles

> "Above all else, show the data." — Edward Tufte

The most impactful visualizations share these traits:

1. **High data-ink ratio** - Every pixel serves a purpose
2. **Clear hierarchy** - The eye knows where to look
3. **Immediate insight** - The "aha" happens in seconds
4. **Memorable** - Sticks in the mind

---

## Category 1: The Balance Sheet Family

### 1.1 The Sankey Flow
**Purpose**: Show how supply flows to demand

```
                    SUPPLY                          DEMAND
                 ═══════════                     ═══════════

    Beginning ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━▶ Crush
    Stocks                                              ▲
    (45 MMT)                                            │
        │                                               │
        │     ┌─────────────────────────────────────────┤
        │     │                                         │
        ▼     │                                         │
    Production━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━▶ Food/Feed
    (155 MMT)  │                                        ▲
               │                                        │
               │    ┌───────────────────────────────────┤
               │    │                                   │
               ▼    │                                   │
    Imports ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━▶ Exports
    (2 MMT)         │                                   ▲
                    │                                   │
                    │   ┌───────────────────────────────┤
                    │   │                               │
                    ▼   │                               │
                    ════════════════════════════▶ Ending Stocks
                                                    (23 MMT)
```

**Power BI Implementation**:
- Use the Sankey visual from AppSource
- Nodes = S&D categories
- Flow width = volume (MMT)
- Color code: Green = supply, Blue = demand

**Impact**: Shows the ENTIRE balance sheet in one flowing diagram

---

### 1.2 The Stacked Waterfall
**Purpose**: Show the mathematical identity

```
                    GLOBAL SOYBEAN BALANCE (MMT)

    400 ─┤
         │  ┌───┐
    350 ─┤  │BEG│  ┌─────────┐
         │  │STO│  │         │
    300 ─┤  │CKS│  │ PRODUC- │
         │  │   │  │  TION   │
    250 ─┤  │45 │  │         │  ┌───┐
         │  │   │  │  155    │  │IMP│
    200 ─┤  └───┘  │         │  │ 2 │  ════════
         │         │         │  └───┘  TOTAL
    150 ─┤         │         │         SUPPLY
         │         │         │          202
    100 ─┤         │         │
         │         └─────────┘
     50 ─┤
         │
      0 ─┴────────────────────────────────────
              Supply Side

    200 ─┤  ════════
         │  TOTAL      ┌───────┐
    150 ─┤  SUPPLY     │ CRUSH │  ┌─────┐
         │   202       │  82   │  │FOOD/│  ┌──────┐
    100 ─┤             │       │  │FEED │  │EXPORT│  ┌───┐
         │             │       │  │ 35  │  │  62  │  │END│
     50 ─┤             │       │  │     │  │      │  │23 │
         │             └───────┘  └─────┘  └──────┘  └───┘
      0 ─┴──────────────────────────────────────────────────
                         Demand Side
```

**The Insight**: Supply always equals demand. The visualization proves it.

---

### 1.3 The Butterfly Balance
**Purpose**: Compare two countries/years side by side

```
              BRAZIL                    US
         ◀── 2024/25 ──▶         ◀── 2024/25 ──▶

    ████████████████│                │████████████████
         Production │                │ Production
            169 MMT │                │ 121 MMT
                    │                │
         ███████████│                │███████████
               Crush│                │Crush
             57 MMT │                │ 64 MMT
                    │                │
    ████████████████│                │██████
            Exports │                │ Exports
            105 MMT │                │ 49 MMT
                    │                │
              ██████│                │████████████
       Ending Stocks│                │Ending Stocks
             33 MMT │                │ 12 MMT
                    │                │
    ────────────────┼────────────────┼────────────────
              S/U: 18%              S/U: 8%
              ▼ Comfortable         ▼ TIGHT
```

**The Insight**: Instantly see which market is tight vs. comfortable

---

## Category 2: Time Series Power

### 2.1 The Horizon Chart
**Purpose**: Show many time series in small space

```
    STOCKS-TO-USE RATIO BY COUNTRY (5 Years)
    ─────────────────────────────────────────────

    US      ▓▓▓░░░░░░░▓▓▓▓▓░░░░░░░░░░░░░░░░████████
            └─ High ─┘ └─ Normal ─┘ └── LOW ──────┘

    Brazil  ░░░░░░░░░░▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓
            └─── Consistently Comfortable ────────┘

    Argentina████████████▓▓▓▓▓░░░░░░░░░░░░░░░░░░░░
            └─ Recovering ─────────────────────────┘

    EU      ▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓░░░░░░░░░░░░░░░░░░░░░░
            └─ Stable ─┘ └── Tightening ──────────┘

            2020  2021  2022  2023  2024  2025

    Legend: ████ = < 10% (Tight)
            ▓▓▓▓ = 10-20% (Normal)
            ░░░░ = > 20% (Comfortable)
```

**The Insight**: Compare 4 countries × 5 years in the space of one chart

---

### 2.2 The 5-Year Envelope
**Purpose**: Context for current values

```
    US SOYBEAN ENDING STOCKS
    ─────────────────────────────────────────────

    500 ─┤                         5-Year Max
         │   ╭─────────────────────────────────╮
    400 ─┤  ╱                                   ╲
         │ ╱   ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░ ╲
    300 ─┤╱    ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░╲
         │     ░░░░░░░░ 5-Year Range ░░░░░░░░░░░░░░
    200 ─┤     ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░
         │      ╲░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░╱
    150 ─┤       ╲─────────────────────────────╱
         │        ╲                           ╱ 5-Year Min
    100 ─┤         ╲                         ╱
         │                    ●━━━━━━━━━━━━━●
     50 ─┤                    CURRENT YEAR
         │                    (Below Average)
      0 ─┴────────────────────────────────────────
            Sep Oct Nov Dec Jan Feb Mar Apr May Aug
```

**The Insight**: Immediately see if current year is high/low/normal

---

### 2.3 The Seasonal Spider
**Purpose**: Compare seasonal patterns across years

```
                        Jan
                         │
                    Dec  │  Feb
                      ╲  │  ╱
                       ╲ │ ╱
                   Nov───●───Mar
                       ╱│╲
                      ╱ │ ╲
                    Oct │  Apr
                        │
                   Sep──●───May
                        │
                       Aug Jun
                        │
                       Jul

    ─── 2024/25 (Current)
    ─ ─ 2023/24
    ··· 5-Year Average

    Area = Total Annual Volume
    Shape = Seasonal Pattern
```

**The Insight**: One shape shows the entire year's seasonality

---

## Category 3: Price Relationships

### 3.1 The Crush Margin Stack
**Purpose**: Show margin component breakdown

```
    SOYBEAN CRUSH MARGIN DECOMPOSITION
    ─────────────────────────────────────────────

    $/bu
    14 ─┤
        │    ┌─────────────────────────────────┐
    12 ─┤    │░░░░░░░ SOY OIL VALUE ░░░░░░░░░│ $4.89
        │    │░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░│
    10 ─┤    ├─────────────────────────────────┤
        │    │▓▓▓▓▓▓▓ SOY MEAL VALUE ▓▓▓▓▓▓▓│ $6.65
     8 ─┤    │▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓│
        │    │▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓│
     6 ─┤    └─────────────────────────────────┘
        │    ═══════════════════════════════════  GPV: $11.54
     4 ─┤
        │    ┌─────────────────────────────────┐
     2 ─┤    │████████ SOYBEAN COST ██████████│ $10.25
        │    │█████████████████████████████████│
     0 ─┤    └─────────────────────────────────┘
        │
    -2 ─┤    ════════════════════════════════════
        │         CRUSH MARGIN: $1.29/bu
        │
        └────────────────────────────────────────
```

**The Insight**: See exactly where the margin comes from

---

### 3.2 The Basis Heat Calendar
**Purpose**: Show basis patterns by location and time

```
    SOYBEAN BASIS (cents/bu) - GULF VS INTERIOR
    ─────────────────────────────────────────────

              Sep  Oct  Nov  Dec  Jan  Feb  Mar

    Gulf      +45  +52  +58  +62  +55  +48  +42
              ░░░  ▓▓▓  ███  ███  ▓▓▓  ░░░  ░░░

    Chicago   +12  +18  +22  +25  +20  +15  +10
              ░░░  ░░░  ▓▓▓  ▓▓▓  ░░░  ░░░  ░░░

    Interior  -15  -10  -5   +2   +5   +8   +10
              ▓▓▓  ░░░  ░░░  ░░░  ░░░  ░░░  ░░░

    Legend: ███ Strong (+40+)  ▓▓▓ Moderate  ░░░ Weak/Negative
```

**The Insight**: Spatial AND temporal patterns in one view

---

### 3.3 The Price-Stocks Scatter
**Purpose**: Reveal the non-linear relationship

```
    SOYBEAN PRICE vs STOCKS-TO-USE RATIO
    ─────────────────────────────────────────────

    Price
    ($/bu)
      │
    18├                              ●2012
      │                            (Drought)
    16├
      │                 ●2021
    14├           ●2022    ●2023
      │     ●2019
    12├  ●2018         ●2020          ●2024
      │●2017
    10├─────────────────────────────────────────
      │
     8├                               The
      │                            "Cliff Zone"
     6├                            Below 8% S/U,
      │                            prices spike
      └─────────────────────────────────────────
           5%    10%    15%    20%    25%    30%
                    Stocks-to-Use Ratio

    ═══════════════════════════════════════════
    Key Insight: The curve is EXPONENTIAL, not linear.
    Below 10% S/U = danger zone for prices.
```

**The Insight**: Explains why small stock changes cause big price moves

---

## Category 4: Global Trade

### 4.1 The Trade Flow Arc
**Purpose**: Show export flows visually

```
              GLOBAL SOYBEAN TRADE FLOWS (MMT)
    ─────────────────────────────────────────────

                         ╭──────────╮
                        EU │20 MMT│
                         ╰────△─────╯
                              │
         ╭───────────────────●───────────────────╮
         │                   │                   │
         │            ╭──────●──────╮            │
         │           SE Asia│25 MMT│            │
         │            ╰─────△──────╯            │
         │                  │                   │
    ╭────●────╮        ╭────●────╮        ╭────●────╮
    │ BRAZIL  │        │  CHINA  │        │   US    │
    │ 105 MMT │───────▶│ 100 MMT │◀───────│ 49 MMT  │
    │  Export │        │ Import  │        │ Export  │
    ╰─────────╯        ╰─────────╯        ╰─────────╯
         │                  △                   │
         │                  │                   │
         ╰──────────────────●───────────────────╯
                            │
                     ╭──────●──────╮
                    Other│15 MMT│
                     ╰────────────╯

    Arrow thickness = trade volume
```

**The Insight**: China's dominance and Brazil's rise in one image

---

### 4.2 The Market Share Shift
**Purpose**: Show competitive dynamics over time

```
    GLOBAL SOYBEAN EXPORT MARKET SHARE
    ─────────────────────────────────────────────

    100%├─────────────────────────────────────────
        │▓▓▓▓▓▓▓▓▓▓▓▓▓
        │▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓
        │▓▓▓▓▓ US ▓▓▓▓▓▓▓▓▓
     75%│▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓
        │▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓
        │▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓
        ├─────────────────────────────────────────
     50%│░░░░░░░░░░░░░░
        │░░░░░░░░░░░░░░░░░░░░░
        │░░░░░ BRAZIL ░░░░░░░░░░░░░░░
        │░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░
     25%│░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░
        │░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░
        ├─────────────────────────────────────────
        │███ Argentina/Other ████████████████████
      0%├─────────────────────────────────────────
         2000  2005  2010  2015  2020  2025

    The Story: Brazil overtook US as #1 exporter in 2013
               and the gap keeps widening.
```

---

## Category 5: Biofuel Economics

### 5.1 The Credit Stack
**Purpose**: Show biofuel margin components

```
    BIODIESEL ECONOMICS BREAKDOWN ($/gallon)
    ─────────────────────────────────────────────

    REVENUE STACK                 COST STACK
    ═════════════                 ══════════

    ┌───────────────┐
    │               │             ┌───────────────┐
    │  D4 RIN       │             │               │
    │  $1.05        │             │  FEEDSTOCK    │
    ├───────────────┤             │  (UCO/Tallow) │
    │               │             │  $3.60        │
    │  LCFS Credit  │             │  (75%)        │
    │  $0.75        │             │               │
    ├───────────────┤             ├───────────────┤
    │               │             │  Processing   │
    │  BTC          │             │  $0.95        │
    │  $1.00        │             │  (20%)        │
    ├───────────────┤             ├───────────────┤
    │               │             │  Other        │
    │  Biodiesel    │             │  $0.25        │
    │  Price        │             │  (5%)         │
    │  $3.50        │             │               │
    │               │             │               │
    └───────────────┘             └───────────────┘

    TOTAL: $6.30                  TOTAL: $4.80

    ════════════════════════════════════════════
    NET MARGIN: $1.50/gallon  ✓ Profitable
    ════════════════════════════════════════════
```

---

### 5.2 The Feedstock Arbitrage Map
**Purpose**: Show relative feedstock economics

```
    BIODIESEL FEEDSTOCK COMPARISON
    ─────────────────────────────────────────────

    Feedstock      CI Score    Price    Margin
                   (gCO2/MJ)   ($/lb)   Impact
    ─────────────────────────────────────────────

    UCO            ████░░░░░    $0.42    ████████
                   (20)         High     Best

    Tallow         █████░░░░    $0.38    ███████░
                   (25)         Med      Good

    Corn Oil       ██████░░░    $0.52    █████░░░
                   (35)         High     Moderate

    Soy Oil        ████████░    $0.45    ███░░░░░
                   (50)         Med      Lower

    Palm Oil       █████████    $0.38    █░░░░░░░
                   (60)         Low      Poor (CI)

    ─────────────────────────────────────────────
    Lower CI = Higher LCFS credit = Better margin
```

---

## Category 6: Executive Dashboard

### 6.1 The Single-Page Market Summary
**Purpose**: Everything a trader needs in one view

```
┌─────────────────────────────────────────────────────────────────┐
│                    RLC COMMODITY DASHBOARD                       │
│                      December 22, 2024                           │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐           │
│  │ SOYBEANS │ │SOY MEAL  │ │ SOY OIL  │ │  CORN    │           │
│  │  $10.25  │ │ $295.50  │ │  $0.425  │ │  $4.52   │           │
│  │  ▼ 2.1%  │ │  ▲ 1.5%  │ │  ▲ 3.2%  │ │  ▼ 0.8%  │           │
│  └──────────┘ └──────────┘ └──────────┘ └──────────┘           │
│                                                                  │
├─────────────────────────────────────────────────────────────────┤
│  CRUSH MARGIN        │  STOCKS-TO-USE    │  BRAZIL WEATHER      │
│  ═══════════════     │  ══════════════   │  ══════════════      │
│                      │                   │                      │
│  $1.29/bu            │  US: 8.2% ▼       │  Mato Grosso: DRY    │
│  ▲ 12% vs last week  │  Brazil: 22% ═    │  Parana: NORMAL      │
│                      │  Argentina: 18% ▲ │  Rio Grande: WET     │
│  [Margin Chart]      │  [S/U Heatmap]    │  [Weather Map]       │
│                      │                   │                      │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  PRICE vs 5-YEAR RANGE                      KEY EVENTS          │
│  ══════════════════════                     ══════════          │
│                                                                  │
│  Soybeans: ████████████░░░░ 75th %ile      • USDA WASDE: Jan 12 │
│  Soy Meal: ██████████████░░ 85th %ile      • CONAB: Jan 9       │
│  Soy Oil:  ████████████████ 95th %ile      • Brazil Harvest: Feb│
│  Corn:     ██████░░░░░░░░░░ 40th %ile      • Argentina Plant: ✓ │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

---

## Implementation Checklist

### Phase 1: Foundation (Week 1)
- [ ] Set up Power BI with ODBC connection
- [ ] Create date dimension table
- [ ] Build basic S&D measures
- [ ] Apply RLC color theme

### Phase 2: Core Visuals (Week 2)
- [ ] Balance Sheet Waterfall
- [ ] Stocks-to-Use Heatmap
- [ ] 5-Year Range Chart
- [ ] Price-Stocks Scatter

### Phase 3: Advanced (Week 3)
- [ ] Crush Economics Dashboard
- [ ] Biofuel Credit Stack
- [ ] Trade Flow Visualization
- [ ] Seasonal Spider Charts

### Phase 4: Polish (Week 4)
- [ ] Executive Summary Page
- [ ] Mobile-optimized views
- [ ] Automated refresh
- [ ] PDF export templates

---

## Resources

- [Barchart Commodities](https://www.barchart.com/futures/major-commodities)
- [Iowa State Charting Guide](https://www.extension.iastate.edu/agdm/crops/html/a2-20.html)
- [SpreadCharts Analytics](https://spreadcharts.com/)
- [COT Data Visualization](https://www.a1trading.com/commitment-of-traders/)
- [Edward Tufte Books](https://www.edwardtufte.com/tufte/)

---

*"The best visualization is one that makes the viewer feel smart, not one that makes the creator look smart."*
