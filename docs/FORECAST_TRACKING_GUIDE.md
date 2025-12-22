# RLC Forecast Tracking System
## Complete Guide to Capturing, Measuring, and Improving Forecasts

---

## Table of Contents

1. [Overview](#overview)
2. [Theory: Forecast Accuracy Measurement](#theory-forecast-accuracy-measurement)
3. [System Architecture](#system-architecture)
4. [Getting Started](#getting-started)
5. [Recording Forecasts](#recording-forecasts)
6. [Recording Actuals](#recording-actuals)
7. [Computing Accuracy Metrics](#computing-accuracy-metrics)
8. [Understanding the Reports](#understanding-the-reports)
9. [The Feedback Loop](#the-feedback-loop)
10. [PowerBI Integration](#powerbi-integration)
11. [Website Display Options](#website-display-options)
12. [Best Practices](#best-practices)

---

## Overview

The RLC Forecast Tracking System is a comprehensive solution for:

1. **Capturing Forecasts**: Recording price and fundamental predictions with full vintage tracking
2. **Recording Actuals**: Storing reported values with revision history
3. **Measuring Accuracy**: Computing industry-standard metrics (MAPE, RMSE, directional accuracy)
4. **Analyzing Bias**: Identifying systematic over/under-forecasting
5. **Continuous Improvement**: Creating feedback loops to refine methodology
6. **Marketing**: Generating professional accuracy reports for credibility

### Why Track Forecasts?

| Purpose | Benefit |
|---------|---------|
| **Credibility** | Demonstrate track record to clients/stakeholders |
| **Improvement** | Identify patterns in errors to refine methodology |
| **Transparency** | Show professional approach to forecasting |
| **Learning** | Feed accuracy data back into LLM training |

---

## Theory: Forecast Accuracy Measurement

### The Fundamental Challenge

Forecast accuracy measurement must balance several considerations:

1. **Scale Independence**: Compare forecasts across different commodities and units
2. **Bias Detection**: Identify systematic over/under-forecasting
3. **Directional Value**: Was the direction right even if magnitude was off?
4. **Economic Value**: Did the forecast lead to profitable decisions?

### Core Metrics

#### 1. MAPE (Mean Absolute Percentage Error)

```
MAPE = (1/n) × Σ|Actual - Forecast| / Actual × 100
```

**Interpretation:**
| MAPE | Quality |
|------|---------|
| < 5% | Excellent |
| 5-10% | Good |
| 10-20% | Reasonable |
| 20-30% | Fair |
| > 30% | Poor |

**Pros:** Scale-independent, intuitive interpretation
**Cons:** Undefined when actual = 0, asymmetric (penalizes over-forecasts more)

#### 2. RMSE (Root Mean Squared Error)

```
RMSE = √[(1/n) × Σ(Actual - Forecast)²]
```

**Interpretation:** Same units as the forecast. Lower is better.

**Pros:** Emphasizes large errors (important for risk management)
**Cons:** Scale-dependent, can't compare across commodities directly

#### 3. MAE (Mean Absolute Error)

```
MAE = (1/n) × Σ|Actual - Forecast|
```

**Interpretation:** Same units as forecast. Average size of errors.

**Pros:** Robust to outliers (compared to RMSE)
**Cons:** Scale-dependent

#### 4. MPE (Mean Percentage Error) - Bias Indicator

```
MPE = (1/n) × Σ(Actual - Forecast) / Actual × 100
```

**Interpretation:**
- Positive MPE → Under-forecasting (actuals higher than forecasts)
- Negative MPE → Over-forecasting (actuals lower than forecasts)
- Near zero → Unbiased

#### 5. Directional Accuracy

```
DA = (Number of correct direction predictions) / (Total predictions) × 100
```

**Interpretation:**
| DA | Quality |
|----|---------|
| > 70% | Strong predictive power |
| 60-70% | Good predictive power |
| 50-60% | Marginal (barely better than coin flip) |
| < 50% | No predictive power |

#### 6. Theil's U Statistic

```
U = RMSE(Forecast) / RMSE(Naive Forecast)
```

Where naive forecast = previous period's actual value.

**Interpretation:**
- U < 1.0 → Better than naive (no-change) forecast
- U = 1.0 → Same as naive forecast
- U > 1.0 → Worse than naive forecast (concerning!)

### Research Insights

Based on academic research:

> "Models which can accurately forecast the sign of future returns, or predict turning points, have been found to be more profitable."
> — [Federal Reserve Research](https://www.federalreserve.gov/pubs/ifdp/2011/1025/ifdp1025.pdf)

> "The accuracy of forecasts according to traditional statistical criteria may give little guide to the potential profitability of employing those forecasts in a market trading strategy."
> — [IMF Working Paper](https://www.imf.org/external/pubs/ft/wp/2004/wp0441.pdf)

This suggests tracking both:
1. Statistical accuracy (MAPE, RMSE)
2. Directional/turning point accuracy

---

## System Architecture

### Database Schema

```
┌─────────────────┐      ┌─────────────────┐
│    FORECASTS    │      │     ACTUALS     │
├─────────────────┤      ├─────────────────┤
│ forecast_id     │      │ actual_id       │
│ forecast_date   │      │ report_date     │
│ target_date     │──┐   │ target_date     │
│ commodity       │  │   │ commodity       │
│ country         │  │   │ country         │
│ forecast_type   │  │   │ value_type      │
│ value           │  │   │ value           │
│ unit            │  │   │ unit            │
│ confidence_low  │  │   │ source          │
│ confidence_high │  │   │ revision_number │
│ marketing_year  │  └──→├─────────────────┤
│ notes           │      │                 │
│ source          │      │                 │
│ analyst         │      │                 │
└─────────────────┘      └─────────────────┘
         │                        │
         └───────────┬────────────┘
                     │
                     ▼
        ┌─────────────────────────┐
        │  FORECAST_ACTUAL_PAIRS  │
        ├─────────────────────────┤
        │ forecast_id             │
        │ actual_id               │
        │ error                   │
        │ percentage_error        │
        │ absolute_error          │
        │ absolute_pct_error      │
        │ direction_correct       │
        │ days_ahead              │
        └─────────────────────────┘
                     │
                     ▼
        ┌─────────────────────────┐
        │    ACCURACY_METRICS     │
        ├─────────────────────────┤
        │ commodity               │
        │ country                 │
        │ forecast_type           │
        │ mae, mape, rmse, mpe    │
        │ directional_accuracy    │
        │ theil_u                 │
        │ period_start/end        │
        └─────────────────────────┘
                     │
                     ▼
        ┌─────────────────────────┐
        │   FORECAST_FEEDBACK     │
        ├─────────────────────────┤
        │ issue_identified        │
        │ root_cause              │
        │ adjustment_made         │
        │ expected_improvement    │
        └─────────────────────────┘
```

### Vintage Tracking

The system tracks "forecast vintages" - the date when a forecast was made:

```
Example: Forecasting 2024/25 Brazil Soybean Production

Vintage 1: Oct 2023 → Forecast: 162 MMT (planting just started)
Vintage 2: Jan 2024 → Forecast: 158 MMT (dry weather concerns)
Vintage 3: Apr 2024 → Forecast: 153 MMT (drought confirmed)
Actual:    Sep 2024 → Reported: 154 MMT

Each vintage is evaluated separately:
- Oct forecast error: -8 MMT (-5.2%)
- Jan forecast error: -4 MMT (-2.6%)
- Apr forecast error: +1 MMT (+0.6%)
```

This allows you to analyze:
- How accuracy improves as target date approaches
- Which forecasters update more/less frequently
- Optimal forecast update frequency

---

## Getting Started

### Step 1: Initialize the Database

```bash
python deployment/forecast_tracker.py --init
```

This creates the necessary tables in your existing `rlc_commodities.db`.

### Step 2: Record Your First Forecast

```bash
python deployment/forecast_tracker.py --record-forecast
```

Follow the interactive prompts:
```
Commodity: soybeans
Country: US
Forecast type: production
Forecast value: 4380
Unit: million bushels
Target date: 2024-09-01
Marketing year: 2024/25
Notes: August WASDE estimate
```

### Step 3: Record Actuals When Available

```bash
python deployment/forecast_tracker.py --record-actual
```

### Step 4: Compute Accuracy

```bash
python deployment/forecast_tracker.py --match
python deployment/forecast_tracker.py --compute-accuracy
```

---

## Recording Forecasts

### Forecast Types

| Type | Description | Example Unit |
|------|-------------|--------------|
| `price` | Commodity prices | USD/bushel, USD/MT |
| `production` | Total production | MMT, million bushels |
| `ending_stocks` | Carryover inventory | MMT, million bushels |
| `exports` | Export volume | MMT, million bushels |
| `crush` | Processing volume | MMT, million bushels |
| `yield` | Productivity | bu/acre, MT/ha |
| `area` | Planted/harvested area | million acres, million ha |
| `consumption` | Domestic use | MMT |
| `imports` | Import volume | MMT |

### Programmatic Recording

```python
from deployment.forecast_tracker import ForecastTracker, Forecast

tracker = ForecastTracker()

forecast = Forecast(
    forecast_id=None,  # Auto-generated
    forecast_date="2024-08-12",  # When you made this forecast
    target_date="2024-09-01",    # What period you're forecasting
    commodity="soybeans",
    country="US",
    forecast_type="production",
    value=4380,
    unit="million bushels",
    marketing_year="2024/25",
    notes="Post-August WASDE, expecting late-season yield decline",
    source="RLC",
    analyst="John Doe"
)

tracker.record_forecast(forecast)
```

### Batch Recording from Spreadsheet

If you have forecasts in a spreadsheet, export as CSV and use:

```python
import pandas as pd
from deployment.forecast_tracker import ForecastTracker, Forecast

df = pd.read_csv("my_forecasts.csv")
tracker = ForecastTracker()

for _, row in df.iterrows():
    forecast = Forecast(
        forecast_id=None,
        forecast_date=row['forecast_date'],
        target_date=row['target_date'],
        commodity=row['commodity'],
        country=row['country'],
        forecast_type=row['type'],
        value=row['value'],
        unit=row['unit'],
        marketing_year=row.get('marketing_year'),
        notes=row.get('notes')
    )
    tracker.record_forecast(forecast)
```

---

## Recording Actuals

### When to Record Actuals

Record actuals when official reports are released:

| Source | Release Schedule | What to Record |
|--------|------------------|----------------|
| USDA WASDE | Monthly (around 12th) | US and major country S&D |
| USDA NASS | Weekly/Monthly | US production, stocks |
| CONAB | Monthly | Brazil production |
| Stats Canada | Monthly | Canada production |
| Customs Data | Monthly | Trade flows |

### Handling Revisions

The system tracks revision numbers:

```python
# Initial report
actual_v0 = Actual(
    actual_id=None,
    report_date="2024-09-12",
    target_date="2024-09-01",
    commodity="soybeans",
    country="US",
    value_type="production",
    value=4380,
    unit="million bushels",
    source="USDA",
    revision_number=0,  # Initial release
    notes="September WASDE"
)

# First revision (October WASDE)
actual_v1 = Actual(
    ...
    value=4420,
    revision_number=1,  # First revision
    notes="October WASDE revision"
)
```

### Which Actual to Use?

For accuracy measurement, you can choose:

1. **First Release**: Measures forecast vs. initial report
2. **Final Revision**: Measures forecast vs. "truth"
3. **Both**: Track separately

The system stores all revisions so you can analyze either approach.

---

## Computing Accuracy Metrics

### Basic Usage

```bash
# Match all forecasts to their actuals
python deployment/forecast_tracker.py --match

# Compute overall accuracy
python deployment/forecast_tracker.py --compute-accuracy

# Filter by commodity
python deployment/forecast_tracker.py --compute-accuracy --commodity soybeans

# Filter by country
python deployment/forecast_tracker.py --compute-accuracy --country US
```

### Understanding the Output

```
ACCURACY METRICS
========================================
Forecasts analyzed: 156
MAPE: 4.82%
RMSE: 142.5
Directional Accuracy: 68.2%
Mean Bias: -1.3%
Theil's U: 0.78 (better than naive)
```

**Interpretation:**
- MAPE 4.82% = Excellent accuracy
- Directional Accuracy 68.2% = Good at predicting direction
- Mean Bias -1.3% = Slight over-forecasting tendency
- Theil's U 0.78 = 22% better than just using last period's value

---

## Understanding the Reports

### Generating Reports

```bash
python deployment/forecast_tracker.py --report
```

This creates a markdown report in `data/forecast_reports/`.

### Report Sections

1. **Executive Summary**: Top-line metrics
2. **By Commodity**: Which commodities you forecast best
3. **By Type**: Which forecast types (price vs. production) are most accurate
4. **Trend**: Is your accuracy improving over time?

### Using Reports for Marketing

The reports are designed to be website-ready. Key talking points:

> "RLC maintains a MAPE of X% across our commodity forecasts, with Y% directional accuracy. Our forecasting methodology is continuously refined through systematic accuracy tracking and bias analysis."

---

## The Feedback Loop

### The Improvement Cycle

```
┌─────────────────┐
│  Make Forecast  │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Actual Occurs  │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ Compute Error   │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ Analyze Pattern │
└────────┬────────┘
         │
         ▼
┌─────────────────────────┐
│ Identify Root Cause     │
│ - Data source issues?   │
│ - Model assumptions?    │
│ - Timing differences?   │
│ - External factors?     │
└────────┬────────────────┘
         │
         ▼
┌─────────────────────────┐
│ Adjust Methodology      │
│ - Apply bias correction │
│ - Update model weights  │
│ - Add new data sources  │
│ - Refine assumptions    │
└────────┬────────────────┘
         │
         └──────────────────→ (back to Make Forecast)
```

### Analyzing Bias

```bash
python deployment/forecast_tracker.py --analyze-bias
```

Output:
```
BIAS ANALYSIS
============================================================
commodity   country  forecast_type  n_forecasts  mean_pct_error  bias_direction
soybeans    Brazil   production     24           8.5             Over-forecasting
soybeans    US       production     36           -2.1            Relatively unbiased
corn        US       yield          18           -5.3            Under-forecasting
```

### Getting Improvement Suggestions

```bash
python deployment/forecast_tracker.py --suggestions
```

Output:
```
IMPROVEMENT SUGGESTIONS
============================================================

soybeans - Brazil - production
  Issue: Over-forecasting (8.5%)
  Suggestion: Consider applying a yield discount factor or reviewing planted area assumptions

corn - US - yield
  Issue: Under-forecasting (-5.3%)
  Suggestion: May be too conservative on yields; review technology/weather assumptions
```

### Recording Methodology Adjustments

When you make a change based on feedback:

```python
tracker.record_feedback(
    commodity="soybeans",
    forecast_type="production",
    issue="Consistent 8% over-forecasting of Brazil production",
    root_cause="Overestimating planted area conversion to harvest",
    adjustment="Applied 5% discount to initial planted area estimates",
    expected_improvement="Reduce MAPE by 3-5%",
    analyst="John Doe"
)
```

This creates an audit trail of your methodology evolution.

---

## PowerBI Integration

### Exporting Data

```bash
python deployment/forecast_tracker.py --export-powerbi
```

This creates an Excel file with two sheets:
1. **Forecast_Actual_Pairs**: All matched pairs with errors
2. **Monthly_Summary**: Aggregated accuracy by month

### Recommended Visualizations

1. **Accuracy Trend Line Chart**
   - X-axis: Month
   - Y-axis: MAPE
   - Shows improvement over time

2. **Commodity Comparison Bar Chart**
   - X-axis: Commodity
   - Y-axis: MAPE
   - Highlights strengths

3. **Bias Heat Map**
   - Rows: Commodities
   - Columns: Countries
   - Color: Bias (red=over, green=under)

4. **Directional Accuracy Gauge**
   - Simple gauge showing % correct direction

5. **Forecast vs Actual Scatter Plot**
   - X-axis: Forecast value
   - Y-axis: Actual value
   - Perfect forecasts fall on 45° line

---

## Website Display Options

### Option 1: Summary Dashboard

```html
<div class="accuracy-dashboard">
  <h2>Our Track Record</h2>

  <div class="metric-card">
    <span class="metric-value">4.8%</span>
    <span class="metric-label">Average Forecast Error (MAPE)</span>
  </div>

  <div class="metric-card">
    <span class="metric-value">68%</span>
    <span class="metric-label">Directional Accuracy</span>
  </div>

  <div class="metric-card">
    <span class="metric-value">1,247</span>
    <span class="metric-label">Forecasts Tracked</span>
  </div>
</div>
```

### Option 2: Methodology Page

A dedicated page explaining:
1. How you make forecasts
2. How you measure accuracy
3. How you use feedback to improve
4. Historical accuracy data

### Option 3: Interactive Explorer

Allow users to:
- Filter by commodity/country
- See historical accuracy trends
- Download accuracy reports

---

## Best Practices

### 1. Record Forecasts Immediately

Don't wait - record forecasts as soon as you make them. This ensures:
- Accurate vintage dates
- No selection bias (only recording "good" forecasts)
- Complete history

### 2. Use Consistent Units

Always use the same units for the same commodity:
- US soybeans: million bushels
- World soybeans: million metric tons

### 3. Document Your Assumptions

Use the `notes` field to capture:
- Key assumptions
- Data sources used
- Reasoning for the forecast

### 4. Review Accuracy Monthly

Set a calendar reminder to:
1. Record new actuals
2. Match forecasts to actuals
3. Generate accuracy report
4. Analyze any concerning patterns

### 5. Act on Feedback

When you identify bias:
1. Record it in the feedback table
2. Adjust your methodology
3. Track whether accuracy improves

### 6. Be Transparent About Errors

On your website:
- Show both wins and losses
- Explain what you learned from misses
- Demonstrate continuous improvement

---

## Research Sources

- [Forecasting: Principles and Practice - Accuracy Evaluation](https://otexts.com/fpp3/accuracy.html)
- [IBM: Understanding Forecast Accuracy Metrics](https://www.relexsolutions.com/resources/measuring-forecast-accuracy/)
- [Federal Reserve: Evaluating Commodity Forecasting Performance](https://www.federalreserve.gov/pubs/ifdp/2011/1025/ifdp1025.pdf)
- [IMF: Forecasting Commodity Prices](https://www.imf.org/external/pubs/ft/wp/2004/wp0441.pdf)
- [Nature: Adaptive Bias Correction for Improved Forecasting](https://www.nature.com/articles/s41467-023-38874-y)
- [European Fiscal Board Forecast Tracker](https://commission.europa.eu/european-fiscal-board-efb/forecast-tracker_en)
- [ITR Economics Forecast Accuracy Methodology](https://itreconomics.com/forecast-accuracy/)

---

## Quick Reference

### Command Cheat Sheet

```bash
# Initialize
python deployment/forecast_tracker.py --init

# Record data
python deployment/forecast_tracker.py --record-forecast
python deployment/forecast_tracker.py --record-actual

# Analyze
python deployment/forecast_tracker.py --match
python deployment/forecast_tracker.py --compute-accuracy
python deployment/forecast_tracker.py --analyze-bias
python deployment/forecast_tracker.py --suggestions

# Report
python deployment/forecast_tracker.py --report
python deployment/forecast_tracker.py --export-powerbi

# Filter
python deployment/forecast_tracker.py --compute-accuracy --commodity soybeans
python deployment/forecast_tracker.py --analyze-bias --commodity corn
```

### Metric Interpretation Quick Guide

| Metric | Good | Excellent |
|--------|------|-----------|
| MAPE | < 10% | < 5% |
| Directional Accuracy | > 60% | > 70% |
| Theil's U | < 1.0 | < 0.8 |
| Bias (MPE) | -5% to +5% | -2% to +2% |
