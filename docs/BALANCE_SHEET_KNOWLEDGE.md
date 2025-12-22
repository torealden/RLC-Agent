# Commodity Balance Sheet Knowledge Base

## Overview

This document defines the fundamental relationships and accounting identities used in commodity balance sheets. It serves as a reference for the LLM to understand how supply and demand balances work for agricultural commodities.

## The Core Balance Sheet Identity

The fundamental equation that governs all commodity balance sheets:

```
Beginning Stocks + Production + Imports = Total Supply
Total Supply = Domestic Consumption + Exports + Ending Stocks
```

This can be rewritten as:

```
Ending Stocks = Beginning Stocks + Production + Imports - Domestic Consumption - Exports
```

### Key Insight
**Ending Stocks of Year N = Beginning Stocks of Year N+1**

This is the carryover principle that links marketing years together.

---

## Marketing Year Concepts

### What is a Marketing Year?

A marketing year is the 12-month period that begins at harvest. Different commodities have different marketing year start dates:

| Commodity | Marketing Year | Example |
|-----------|----------------|---------|
| Soybeans (US) | September - August | 2023/24 = Sep 2023 - Aug 2024 |
| Corn (US) | September - August | 2023/24 = Sep 2023 - Aug 2024 |
| Wheat (US) | June - May | 2023/24 = Jun 2023 - May 2024 |
| Palm Oil | October - September | 2023/24 = Oct 2023 - Sep 2024 |
| Rapeseed (EU) | July - June | 2023/24 = Jul 2023 - Jun 2024 |

### Why Marketing Years Matter

1. **Seasonality**: Harvest happens once per year; the marketing year captures the full cycle
2. **Comparability**: Comparing like-with-like across years
3. **Forward Pricing**: Futures contracts align with marketing years

---

## Balance Sheet Components

### SUPPLY SIDE

#### Beginning Stocks (Carryover)
- **Definition**: Inventory held at the start of the marketing year
- **Source**: Ending stocks from previous year
- **Accuracy**: Usually the most reliable number (it's already happened)

#### Production
- **Definition**: Total harvest during the marketing year
- **Components**: Planted Area × Yield = Production
- **Key Factors**:
  - Planted acreage (farmer decisions, economics, weather)
  - Yield (weather, technology, inputs)
  - Abandonment rate (crop failure)

#### Imports
- **Definition**: Commodity brought into the country
- **Sources**: Trade data (customs, shipping records)
- **Timing**: Can occur throughout the year

### DEMAND SIDE

#### Domestic Consumption
- **Definition**: Commodity used within the country
- **Components** (varies by commodity):
  - **Crush/Processing**: Soybeans → Oil + Meal
  - **Food Use**: Direct human consumption
  - **Feed Use**: Animal feed
  - **Industrial Use**: Biofuels, chemicals
  - **Seed Use**: Retained for planting

#### Exports
- **Definition**: Commodity shipped out of the country
- **Sources**: Trade data, export inspection reports
- **Timing**: Often seasonal based on harvest and shipping

#### Ending Stocks
- **Definition**: Inventory held at the end of the marketing year
- **Also Called**: Carryover, carryout
- **Significance**: Key indicator of market tightness

---

## The Crush Margin Concept

For oilseed commodities, the "crush" is a key processing step:

```
Soybeans → Soybean Oil + Soybean Meal + Loss
```

### Crush Yield (Extraction Rate)
- **Soybean Oil Yield**: ~18.5% (185 lbs oil per 60 lb bushel × 1000)
- **Soybean Meal Yield**: ~47.5 lbs per bushel
- **Loss/Moisture**: Remainder

### Gross Processing Margin (GPM)
```
GPM = (Oil Price × Oil Yield) + (Meal Price × Meal Yield) - Soybean Price
```

This margin drives crusher decisions on when and how much to process.

---

## Stocks-to-Use Ratio

A critical metric for understanding market tightness:

```
Stocks-to-Use = Ending Stocks ÷ Total Domestic Use
```

### Interpretation Guidelines

| Stocks-to-Use | Market Condition | Price Implication |
|---------------|------------------|-------------------|
| < 5% | Extremely Tight | Very High Prices |
| 5-10% | Tight | High Prices |
| 10-15% | Comfortable | Normal Prices |
| 15-20% | Ample | Lower Prices |
| > 20% | Burdensome | Low Prices |

---

## Data Quality Indicators

### Most Reliable (in order)
1. **Beginning Stocks**: Already determined
2. **Trade Data**: Customs/inspection records
3. **Production**: Post-harvest surveys
4. **Ending Stocks**: Residual calculation

### Least Reliable
1. **Domestic Use**: Often calculated as residual
2. **Forecast Production**: Pre-harvest estimates

### Common Adjustment Patterns
- USDA often adjusts "residual" use to balance
- Production estimates typically start high and decline
- Export forecasts are revised based on shipping pace

---

## Country-Specific Considerations

### United States
- **Data Source**: USDA WASDE, NASS
- **Frequency**: Monthly
- **Quality**: High, detailed breakdown

### Brazil
- **Data Sources**: CONAB, ABIOVE
- **Timing**: Lagged vs US
- **Quality**: Moderate, improving

### Argentina
- **Data Sources**: Rosario Exchange, Ag Ministry
- **Challenge**: Export tax policies affect data
- **Quality**: Moderate

### China
- **Data Sources**: USDA estimates, customs data
- **Challenge**: Official data often unreliable
- **Quality**: Low transparency

### European Union
- **Data Source**: EU Commission, COCERAL
- **Quality**: Good for aggregate, varies by member state

---

## Soybean Complex Specific

### Products
1. **Whole Soybeans**: Trade, planting
2. **Soybean Meal**: ~80% of crush value, animal feed
3. **Soybean Oil**: ~20% of crush value, food, biodiesel

### Key Relationships
```
Crush = Meal Production ÷ Meal Yield = Oil Production ÷ Oil Yield
```

### Global Trade Flow
- **Exporters**: US, Brazil, Argentina
- **Importers**: China, EU, SE Asia
- **Seasonal Pattern**: US ships Oct-Feb, Brazil/Argentina Mar-Aug

---

## Rapeseed/Canola Complex Specific

### Products
1. **Rapeseed/Canola Seed**
2. **Canola Oil**: Higher value per ton than soy oil
3. **Canola Meal**: Lower protein than soy meal

### Key Markets
- **Canada**: Largest canola exporter
- **EU**: Largest rapeseed producer (internal use)
- **China**: Major importer

---

## Sunflower Complex Specific

### Products
1. **Sunflower Seed**
2. **Sunflower Oil**: Premium for cooking
3. **Sunflower Meal**: Lower protein

### Key Markets
- **Ukraine**: Was #1 exporter (war impact)
- **Russia**: Major producer and exporter
- **Argentina**: Growing exporter

---

## Palm Oil Specific

### Unique Characteristics
- **Perennial Crop**: Trees produce for 25+ years
- **No Meal Product**: Just oil and kernel
- **Yield**: Highest oil yield per hectare

### Key Markets
- **Indonesia**: #1 producer
- **Malaysia**: #2 producer
- **India**: Largest importer

---

## Creating a New Commodity Balance Sheet

### Step 1: Identify Data Sources
1. Check USDA PSD database
2. Check local ministry/statistics agency
3. Check industry associations
4. Check trade publications

### Step 2: Define Marketing Year
- When does harvest occur?
- What marketing year convention does the market use?

### Step 3: Collect Historical Data
- Get at least 10 years of history
- Note any methodology changes
- Document units (MT, MMT, bushels, etc.)

### Step 4: Validate Relationships
- Does Beginning Stocks[N] = Ending Stocks[N-1]?
- Does Supply = Demand (approximately)?
- Are year-over-year changes reasonable?

### Step 5: Document Assumptions
- What is residual/calculated?
- What is directly measured?
- What are the data sources?

---

## Common Units and Conversions

| From | To | Multiply By |
|------|-----|-------------|
| Metric Tons | Bushels (Soy) | 36.74 |
| Metric Tons | Bushels (Corn) | 39.37 |
| Metric Tons | Bushels (Wheat) | 36.74 |
| Million Hectares | Million Acres | 2.471 |
| Kg/Hectare | Bu/Acre (Soy) | 0.0149 |

---

## Terminology Reference

| Term | Definition |
|------|------------|
| **Carryover** | Same as Ending Stocks |
| **Carryout** | Same as Ending Stocks |
| **Disappearance** | Total use (domestic + exports) |
| **Free Stocks** | Stocks not held by government |
| **Pipeline Stocks** | Minimum operational inventory |
| **Residual** | Calculated to balance the sheet |
| **WASDE** | World Agricultural Supply and Demand Estimates |
| **PSD** | Production, Supply, and Distribution |
