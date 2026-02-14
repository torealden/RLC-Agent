# 2012 US Drought

## Event Summary
The 2012 US drought was the most severe agricultural drought since the 1950s, causing massive reductions in corn and soybean yields across the Corn Belt. It remains a critical reference point for understanding weather-driven market rallies.

## Timeline

| Date | Event |
|------|-------|
| Late May 2012 | Early season dryness begins in central Corn Belt |
| June 2012 | Drought rapidly expands, USDA cuts yield forecasts |
| July 2012 | Peak of drought; 80%+ of Corn Belt in drought |
| Aug 10, 2012 | USDA report confirms disaster - corn yield ~123 bu/acre |
| September 2012 | Early harvest shows damage worse than expected in some areas |
| October 2012 | Final yields: Corn 123.4 bu/acre, Soybeans 39.6 bu/acre |

## Weather Conditions

### Key Characteristics
- Below-normal precipitation May through August
- Above-normal temperatures during pollination (July)
- Flash drought development - rapid onset
- Widespread coverage - nearly entire Corn Belt affected

### Critical Period: July 2012
The corn pollination window (roughly July 10-25) saw:
- Temperatures above 95°F for extended periods
- Minimal rainfall during tasseling
- Rapid soil moisture depletion
- Night temperatures not dropping below 70°F (critical for kernel development)

### SQL Query for Weather Analysis
```sql
-- 2012 drought: July temperatures and precipitation
SELECT
    ws.location_id,
    ws.display_name,
    COUNT(*) as days,
    AVG(ws.temp_high_f) as avg_high_f,
    MAX(ws.temp_high_f) as max_high_f,
    SUM(ws.precipitation_in) as total_precip_in,
    SUM(CASE WHEN ws.temp_high_f > 95 THEN 1 ELSE 0 END) as days_above_95
FROM gold.weather_summary ws
WHERE ws.observation_date BETWEEN '2012-07-01' AND '2012-07-31'
AND ws.region = 'US_CORN_BELT'
GROUP BY ws.location_id, ws.display_name
ORDER BY avg_high_f DESC;
```

## Production Impact

### Corn
| Year | Yield (bu/acre) | Production (billion bu) |
|------|-----------------|------------------------|
| 2011 | 147.2 | 12.36 |
| 2012 | 123.4 | 10.78 |
| 2013 | 158.1 | 13.83 |

**Key Stats:**
- 16% yield decline from 2011
- 3rd lowest yield since 2002
- Harvested acres down due to abandonment

### Soybeans
| Year | Yield (bu/acre) | Production (billion bu) |
|------|-----------------|------------------------|
| 2011 | 41.9 | 3.09 |
| 2012 | 39.6 | 3.03 |
| 2013 | 44.0 | 3.29 |

**Key Stats:**
- 5.5% yield decline from 2011
- Pod abortion during August heat
- Variable impact by state

## Price Impact

### Corn Futures (December 2012)
| Date | Price | Change |
|------|-------|--------|
| Jan 1, 2012 | ~$6.00 | - |
| June 1, 2012 | ~$5.50 | -8% |
| July 15, 2012 | ~$7.50 | +36% from June |
| Aug 10, 2012 | ~$8.00 | Peak |
| Aug 20, 2012 | $8.49 | All-time high |

**Rally Magnitude:** ~$3.00/bushel (54%) from June low to August high

### Soybean Futures (November 2012)
| Date | Price | Change |
|------|-------|--------|
| Jan 1, 2012 | ~$12.00 | - |
| June 1, 2012 | ~$13.50 | +12% |
| July 15, 2012 | ~$16.00 | +33% from Jan |
| Sep 4, 2012 | $17.89 | All-time high |

**Rally Magnitude:** ~$5.00/bushel (38%) from June to September

## Market Behavior Patterns

### Early Warning Signs (May-June)
1. Soil moisture declining in weekly drought monitor
2. Crop condition ratings starting to slip
3. Weather forecasts showing persistent heat/dryness
4. Fund buying accelerating

### Peak Panic (July)
1. USDA monthly report cuts yields significantly
2. Crop conditions plunge to poor/very poor
3. Commercials scrambling to cover short positions
4. Demand rationing begins (high prices)

### Post-Peak (August-September)
1. Harvest begins, confirms damage
2. South American planting intentions surge (acreage response)
3. Prices begin to ease as demand rationing works
4. Export sales slow dramatically

## Lessons for Future Events

### Bullish Signals to Watch
1. **Drought development during pollination** - Most critical period
2. **Night temperatures** - Above 70°F damages kernel fill
3. **Flash drought** - Rapid onset more damaging than gradual
4. **Geographic scope** - Widespread > localized

### Bearish Offsets
1. **Demand destruction** - High prices eventually ration demand
2. **South American response** - Acreage expansion follows high prices
3. **Yield variability** - Some areas escape damage

### Comparison Years
- **1988** - Similar heat/drought, different market structure
- **2011** - Drought mostly in Southern Plains (wheat)
- **2021** - Northern Plains drought, different geographic impact

## Data Files
- Weather data: Query from gold.weather_summary
- USDA reports: WASDE archives
- Price data: CME historical data (to be added)

---
*Report generated: 2026-01-23*
*Data sources: Open-Meteo historical, USDA NASS*
