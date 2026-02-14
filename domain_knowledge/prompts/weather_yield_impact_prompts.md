# Weather-Yield Impact Reasoning Prompts

## Overview
These prompts guide LLM reasoning about weather forecast impacts on crop yields. Use with data from `gold.weather_forecast_summary` and `gold.weather_7day_outlook` views.

---

## 1. Growth Stage Context

### Corn Growth Stages & Critical Periods
| Stage | Timing | Duration | Critical Weather Factor |
|-------|--------|----------|------------------------|
| Planting | Apr 15 - May 25 | 2-4 weeks | Soil temp >50°F, not too wet |
| Emergence | May 1 - Jun 10 | 1-2 weeks | Soil moisture, no frost |
| V6-V12 (Vegetative) | Jun 1 - Jul 10 | 4-6 weeks | Adequate moisture for root development |
| VT/R1 (Tasseling/Silking) | Jul 10 - Aug 5 | 2-3 weeks | **MOST CRITICAL** - Heat/drought destroys pollination |
| R2-R4 (Grain Fill) | Aug 1 - Sep 10 | 5-6 weeks | Moderate temps, consistent moisture |
| R5-R6 (Maturity) | Sep 1 - Oct 15 | 4-6 weeks | Dry conditions for harvest |

### Soybean Growth Stages & Critical Periods
| Stage | Timing | Duration | Critical Weather Factor |
|-------|--------|----------|------------------------|
| Planting | May 1 - Jun 10 | 2-4 weeks | Soil temp >50°F |
| Emergence-V3 | May 15 - Jun 25 | 3-4 weeks | Adequate moisture |
| V4-R1 (Vegetative to Bloom) | Jun 15 - Jul 20 | 4-5 weeks | Good moisture for node development |
| R2-R4 (Flowering/Pod Set) | Jul 15 - Aug 15 | 3-4 weeks | **MOST CRITICAL** - Moisture stress reduces pod count |
| R5-R6 (Seed Fill) | Aug 10 - Sep 15 | 4-5 weeks | Consistent moisture for seed weight |
| R7-R8 (Maturity) | Sep 10 - Oct 10 | 3-4 weeks | Dry for harvest |

### Wheat Growth Stages (Winter Wheat)
| Stage | Timing | Duration | Critical Weather Factor |
|-------|--------|----------|------------------------|
| Planting | Sep 15 - Nov 1 | 4-6 weeks | Soil moisture for germination |
| Fall Tillering | Oct 15 - Dec 1 | 6-8 weeks | Mild temps, good moisture |
| Dormancy | Dec - Feb | 8-12 weeks | Snow cover protects from cold |
| Greenup/Jointing | Mar 15 - Apr 30 | 4-6 weeks | Adequate moisture, no late freeze |
| Heading/Flowering | May 1 - May 25 | 2-3 weeks | **CRITICAL** - No heat/drought |
| Grain Fill | May 20 - Jun 20 | 3-4 weeks | Cool temps extend fill period |
| Harvest | Jun 10 - Jul 15 | 3-4 weeks | Dry conditions |

---

## 2. Stress Thresholds

### Temperature Thresholds
```
CORN:
- Optimal growth: 77-91°F (25-33°C)
- Heat stress begins: >86°F (30°C)
- Severe heat stress: >95°F (35°C) - pollen viability drops sharply
- Night temps >77°F (25°C) - reduces yield by increasing respiration
- Frost damage: <32°F (0°C) - kills actively growing tissue
- GDD base: 50°F (10°C), cap at 86°F (30°C)

SOYBEANS:
- Optimal growth: 77-86°F (25-30°C)
- Heat stress begins: >86°F (30°C)
- Severe stress: >95°F (35°C) - flower abortion
- Frost damage: <32°F (0°C)
- GDD base: 50°F (10°C)

WHEAT:
- Optimal grain fill: 59-77°F (15-25°C)
- Heat stress: >86°F (30°C) - accelerates maturity, reduces test weight
- Severe heat: >95°F (35°C) - kernel shriveling
- Winter kill threshold: <-4°F (-20°C) without snow cover
```

### Precipitation Thresholds
```
CORN (weekly needs):
- Vegetative: 0.75-1.0 inch/week (19-25mm)
- Tasseling/Silking: 1.5-2.0 inch/week (38-50mm) **CRITICAL**
- Grain fill: 1.0-1.5 inch/week (25-38mm)
- Drought stress: <0.5 inch/week for 2+ weeks
- Excess moisture: >3 inches/week - root damage, disease

SOYBEANS (weekly needs):
- Vegetative: 0.5-0.75 inch/week (13-19mm)
- Flowering/Pod: 1.25-1.75 inch/week (32-44mm) **CRITICAL**
- Seed fill: 1.0-1.5 inch/week (25-38mm)

WHEAT:
- Spring growth: 0.75-1.0 inch/week (19-25mm)
- Heading: 1.0-1.25 inch/week (25-32mm)
- Grain fill: 0.75-1.0 inch/week (19-25mm)
```

---

## 3. Yield Impact Estimation

### Corn Yield Impact Matrix
| Stress Event | Growth Stage | Yield Impact |
|--------------|--------------|--------------|
| 1 week drought | Vegetative | -2 to -5% |
| 1 week drought | Pollination | -5 to -15% |
| 2+ week drought | Pollination | -15 to -40% |
| 3 days >100°F | Pollination | -10 to -25% |
| 5 days >95°F | Grain fill | -5 to -10% |
| Late frost | Emergence | Replant needed |
| Excess rain | Planting | Delayed planting, -2 to -8% |
| Flooding 48hrs | Any | -5 to -20% depending on stage |

### Soybean Yield Impact Matrix
| Stress Event | Growth Stage | Yield Impact |
|--------------|--------------|--------------|
| 2 week drought | Flowering | -10 to -20% |
| 2 week drought | Pod fill | -8 to -15% |
| 3 days >100°F | Flowering | -5 to -15% |
| Early frost | R6-R7 | -5 to -15% (green stem) |
| Excess moisture | Harvest | Quality issues, -2 to -5% |

---

## 4. LLM Reasoning Templates

### Template 1: 7-Day Forecast Assessment
```
PROMPT: Analyze the 7-day weather forecast for {region} and assess crop yield implications.

CONTEXT DATA:
- Region: {region_name}
- Primary crop: {primary_commodity}
- Current growth stage: {growth_stage}
- Forecast period: {date_range}

FORECAST SUMMARY:
- Total precipitation: {total_precip_mm}mm ({precip_anomaly_pct}% of normal)
- Average temperature: {avg_temp_c}°C ({temp_anomaly_c}°C vs normal)
- Heat stress days (>30°C): {heat_stress_days}
- Frost risk days: {frost_days}
- Maximum consecutive dry days: {max_dry_spell}
- GDD accumulation: {total_gdd} ({gdd_anomaly}% of normal)

REASONING FRAMEWORK:
1. Is the crop at a critical growth stage? If yes, weight impacts 2-3x
2. Are stress thresholds being exceeded? Which ones?
3. How does this compare to normal conditions?
4. What is the probability distribution (GEFS ensemble)?
5. What is the cumulative effect with recent weather?

OUTPUT FORMAT:
- Yield outlook: [Favorable / Mixed / Concerning / High Risk]
- Estimated yield impact: [X to Y%] with [confidence level]
- Key factors: [bullet points]
- Watch items: [what could change the assessment]
```

### Template 2: Regional Comparison
```
PROMPT: Compare weather conditions across major {commodity} producing regions.

REGIONS: {list_of_regions}

FOR EACH REGION ASSESS:
1. Current moisture status vs normal
2. Temperature regime vs optimal
3. Accumulated GDD vs normal pace
4. Near-term stress risks

OUTPUT TABLE:
| Region | Moisture | Temp | GDD Pace | Stress Risk | Outlook |
|--------|----------|------|----------|-------------|---------|

SYNTHESIS:
- Which regions face highest risk?
- Which regions are tracking above trend?
- Net impact on national/global production
```

### Template 3: Stress Event Analysis
```
PROMPT: A {stress_type} event is forecast for {region}. Assess yield implications.

EVENT DETAILS:
- Type: {heat_wave / drought / frost / flooding}
- Duration: {X} days
- Severity: {metrics}
- Affected area: {coverage_pct}% of region

CURRENT CROP STATUS:
- Growth stage: {stage}
- Prior stress: {yes/no, description}
- Soil moisture reserves: {status}

ASSESSMENT CRITERIA:
1. Historical analog events and their impacts
2. Crop's current resilience/vulnerability
3. Probability of event materializing (ensemble spread)
4. Potential for recovery if stress is temporary

OUTPUT:
- Pre-event yield estimate: {X} bu/acre
- Post-event range: {Y to Z} bu/acre
- Confidence: {High/Medium/Low}
- Key uncertainty: {main factor}
```

### Template 4: Seasonal Progress Update
```
PROMPT: Provide a seasonal weather assessment for {commodity} in {country}.

MARKETING YEAR: {year}
CURRENT DATE: {date}
SEASON STAGE: {early/mid/late}

SEASON-TO-DATE SUMMARY:
- Planting conditions: {summary}
- Accumulated precipitation: {mm} ({%} of normal)
- Accumulated GDD: {value} ({%} of normal)
- Stress events to date: {list}

FORWARD-LOOKING (next 2 weeks):
- Forecast summary from GEFS ensemble
- Key dates/stages approaching
- Risk scenarios

YIELD TRAJECTORY:
- USDA current estimate: {value}
- Weather-adjusted estimate: {value}
- Direction of bias: {higher/lower/neutral}
- Confidence: {%}
```

---

## 5. Signal Interpretation Guide

### Moisture Signals
| Signal | Interpretation | Market Implication |
|--------|----------------|-------------------|
| "Drought stress developing" | Precip <70% normal + dry spell >5 days | Bullish, monitor closely |
| "Below normal precipitation" | Precip 70-85% of normal | Neutral to slightly bullish |
| "Near normal moisture" | Precip 85-115% of normal | Neutral |
| "Above normal precipitation" | Precip 115-150% of normal | Neutral to bearish (good moisture) |
| "Excessive moisture risk" | Precip >150% normal | Depends on stage - harvest concern |

### Temperature Signals
| Signal | Interpretation | Market Implication |
|--------|----------------|-------------------|
| "Severe heat stress" | >4 hrs at >35°C | Bullish if during pollination |
| "Moderate heat stress" | >8 hrs at >30°C | Slight bullish bias |
| "Frost risk" | Tmin < 0°C forecast | Bullish if crop vulnerable |
| "Above normal temperatures" | Anomaly > +3°C | GDD accelerating, depends on moisture |
| "Below normal temperatures" | Anomaly < -3°C | Slower development, extends season |

### Development Signals
| Signal | Interpretation | Market Implication |
|--------|----------------|-------------------|
| "Accelerated crop development" | GDD >110% of normal | Earlier harvest, yield neutral |
| "Delayed crop development" | GDD <90% of normal | Frost risk increases, slight bearish |
| "Normal crop development pace" | GDD 90-110% of normal | Neutral |

---

## 6. Example Outputs

### Example 1: US Corn Belt Heat Event
```
Region: US Corn Belt (IA, IL, IN)
Date: July 18, 2025
Crop Stage: VT/R1 (Pollination) - CRITICAL PERIOD

7-Day Forecast (Jul 18-25):
- High temps: 95-102°F (35-39°C) for 5 consecutive days
- Overnight lows: 75-78°F (24-26°C) - minimal relief
- Precipitation: 0.1 inches (3mm) - 15% of normal
- GEFS probability of >100°F: 65%

Assessment:
YIELD OUTLOOK: HIGH RISK

The combination of extreme heat and minimal rainfall during corn pollination
represents the most yield-damaging scenario possible. Key concerns:

1. **Pollen viability**: Temperatures >95°F for >4 hours kill pollen. With 5 days
   forecast above this threshold, pollination failure is likely in areas not irrigated.

2. **Silk desiccation**: Dry silks cannot receive pollen. Low humidity and high temps
   will accelerate silk drying.

3. **Night temperature**: Overnight lows >75°F prevent plant recovery and increase
   respiration losses.

4. **Soil moisture**: Dry conditions mean no buffer. Crops already stressed will
   see compounded damage.

Estimated yield impact: -12% to -25% for affected areas
- Conservative case (-12%): Brief relief mid-week, some scattered showers
- Base case (-18%): Forecast verifies as expected
- Severe case (-25%): Heat persists, extends into following week

National yield impact: -4% to -8% (Corn Belt = ~65% of US production)

Watch items:
- Model updates showing potential for mid-week relief
- Actual rainfall vs forecast
- Crop conditions report next Monday
```

### Example 2: Brazil Soybean Planting Season
```
Region: Brazil - Mato Grosso
Date: October 5, 2025
Season Stage: Early planting window

14-Day Outlook:
- Precipitation: 45mm expected (90% of normal)
- Soil moisture: Improving after dry winter
- Temperature: 28-32°C (normal range)
- GEFS spread: Moderate uncertainty on rain timing

Assessment:
YIELD OUTLOOK: MIXED CONDITIONS

The planting window is opening on schedule with reasonable moisture prospects:

1. **Moisture status**: First significant rains of the season arrived in late September.
   Soil profiles are recharging but not yet at optimal levels.

2. **Planting pace**: Expect normal to slightly below normal planting progress in
   first two weeks. Some producers may wait for additional rain confirmation.

3. **Risk factors**:
   - If rains pause >10 days, early-planted crops face emergence stress
   - La Niña pattern increases risk of irregular rain distribution

4. **Historical context**: Similar October patterns in 2020 and 2018 led to
   above-trend yields when November rains normalized.

Yield trajectory: On track for trend yield (3.4-3.5 MT/ha)
Bias: Neutral, with downside risk if La Niña intensifies

Next assessment trigger: November precipitation totals
```

---

## 7. Query Patterns for Data Retrieval

### Get Latest 7-Day Outlook
```sql
SELECT * FROM gold.weather_7day_outlook
WHERE region_code IN ('US_CORN_BELT', 'US_DELTA', 'BR_MATO_GROSSO')
ORDER BY region_code;
```

### Get Daily Forecast with Anomalies
```sql
SELECT
    region_code, valid_date,
    precip_mm, precip_anomaly_pct,
    tavg_c, temp_anomaly_c,
    moisture_signal, temperature_signal, development_signal
FROM gold.weather_forecast_summary
WHERE forecast_date = CURRENT_DATE
AND region_code = 'US_CORN_BELT'
AND lead_days <= 14
ORDER BY valid_date;
```

### Get Ensemble Uncertainty
```sql
SELECT
    region_code, valid_date,
    precip_p10, precip_p50, precip_p90,
    temp_p10, temp_p50, temp_p90,
    (precip_p90 - precip_p10) AS precip_uncertainty,
    (temp_p90 - temp_p10) AS temp_uncertainty
FROM silver.weather_forecast_daily
WHERE model = 'GEFS'
AND forecast_date = CURRENT_DATE
ORDER BY valid_date;
```

### Compare to Climatology
```sql
SELECT
    f.region_code,
    f.valid_date,
    f.precip_mm AS forecast_precip,
    c.precip_normal_mm AS normal_precip,
    f.tavg_c AS forecast_temp,
    (c.tmin_normal_c + c.tmax_normal_c)/2 AS normal_temp
FROM silver.weather_forecast_daily f
JOIN reference.weather_climatology c
    ON f.region_code = c.region_code
    AND EXTRACT(MONTH FROM f.valid_date) = c.month
WHERE f.forecast_date = CURRENT_DATE
AND f.model = 'GFS';
```
