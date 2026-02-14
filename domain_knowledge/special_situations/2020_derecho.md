# 2020 Iowa Derecho

## Event Summary
On August 10, 2020, a powerful derecho (land hurricane) swept across Iowa and Illinois, causing catastrophic damage to corn and soybean crops. It was one of the most destructive weather events in Midwest agricultural history, with estimated crop damage of $7.5+ billion.

## Event Details

### Storm Characteristics
- **Date:** August 10, 2020
- **Duration:** ~14 hours from Nebraska to Ohio
- **Wind speeds:** 100-140 mph (hurricane-force)
- **Width:** 40+ miles at peak
- **Path:** Nebraska → Iowa → Illinois → Indiana

### Affected Area
- 10+ million acres of crops impacted
- Cedar Rapids, IA: 70% tree canopy destroyed
- Power outages for 500,000+ customers (some for weeks)

## Agricultural Impact

### Corn Damage
| Damage Type | Severity |
|------------|----------|
| Stalk lodging | Severe - stalks broken/flat |
| Green snap | Moderate - plants snapped at stalk |
| Root lodging | Moderate - plants leaning |
| Ear damage | Severe in lodged areas |

**Estimated Corn Loss:** 550+ million bushels
- Direct yield loss
- Harvest difficulties (laying flat)
- Quality issues

### Soybean Damage
- Less severe than corn (more flexible plants)
- Pod shatter in some areas
- Some plants stripped of leaves

### USDA Crop Condition Changes
```
Iowa Corn Condition (Good/Excellent):
Aug 9, 2020:  75%
Aug 16, 2020: 53% (immediate post-derecho)
Drop: 22 percentage points in one week
```

## Price Reaction

### Corn Futures (December 2020)
| Date | Price | Event |
|------|-------|-------|
| Aug 7, 2020 | $3.20 | Pre-storm |
| Aug 10, 2020 | $3.25 | Storm day |
| Aug 11, 2020 | $3.38 | Initial reaction |
| Aug 17, 2020 | $3.45 | After crop tours |
| Aug 24, 2020 | $3.55 | USDA cut yield |

**Initial Rally:** +$0.35/bu (11%) over two weeks

### Context: China Buying Surge
The derecho coincided with:
- Massive Chinese corn purchases beginning
- Shift in global supply/demand balance
- Prices continued rallying into 2021

## Weather Data for Event
```sql
-- Wind damage analysis query
SELECT
    ws.location_id,
    ws.display_name,
    ws.observation_date,
    ws.wind_speed_mph,
    ws.wind_gust_mph,
    ws.precipitation_in
FROM gold.weather_summary ws
WHERE ws.observation_date BETWEEN '2020-08-09' AND '2020-08-12'
AND ws.region = 'US_CORN_BELT'
ORDER BY ws.observation_date, ws.location_id;

-- Note: Surface weather data may not capture full derecho intensity
-- Actual wind speeds were much higher than typical station readings
```

## Yield Impact by County

### Hardest Hit Counties (Iowa)
| County | 2019 Yield | 2020 Yield | Change |
|--------|------------|------------|--------|
| Linn | 200 bu/acre | 145 bu/acre | -28% |
| Marshall | 195 bu/acre | 150 bu/acre | -23% |
| Story | 198 bu/acre | 155 bu/acre | -22% |

### State-Level Impact
| State | 2019 Yield | 2020 Yield | Change |
|-------|------------|------------|--------|
| Iowa | 198.0 | 178.0 | -10% |
| Illinois | 187.0 | 193.0 | +3% |

## Insurance and Financial Impact

### Crop Insurance Claims
- Estimated $6+ billion in claims
- Highest single-event crop insurance payout
- Challenges with adjustment (storm vs drought damage)

### MFP/CFAP Interaction
- Some farms received both derecho loss payments and COVID payments
- Complex federal support landscape

## Lessons for Future Events

### Derecho Recognition
1. **Radar signatures** - Bow echo patterns
2. **Speed** - Moves faster than typical storms
3. **Duration** - Sustained winds, not brief gusts

### Market Behavior
1. **Initial underreaction** - Markets didn't fully price in
2. **Crop tours confirmed** - Pro Farmer found severe damage
3. **Stacking effects** - Combined with other bullish factors

### Risk Management
1. Multiple-peril crop insurance critical
2. Stalk quality monitoring post-storm
3. Harvest timing decisions (lodged corn)

## Comparison Events
- **1998 Derecho** - Similar path, less agricultural damage
- **2011 Derecho** - Lesser impact
- **August 2009** - Straight-line winds in Kansas

---
*Report generated: 2026-01-23*
*Data sources: USDA, NWS, Pro Farmer Crop Tour*
