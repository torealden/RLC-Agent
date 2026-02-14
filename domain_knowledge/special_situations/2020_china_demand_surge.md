# 2020-2021 China Corn Demand Surge

## Event Summary
Beginning in late summer 2020, China embarked on an unprecedented corn buying spree from the United States, fundamentally shifting global corn trade dynamics. This demand surge, combined with the August 2020 derecho and La Nina weather concerns in South America, drove corn prices from ~$3.20/bu to over $7.00/bu - the largest sustained corn rally since 2010-2012.

## Timeline

| Date | Event | CZ (Dec Corn) |
|------|-------|---------------|
| Aug 10, 2020 | Iowa derecho | $3.25 |
| Aug 21, 2020 | First large China corn purchase announced | $3.40 |
| Sep 2020 | China purchases accelerate | $3.60-3.80 |
| Oct 2020 | Weekly sales to China routinely 500k+ MT | $4.00 |
| Dec 2020 | USDA sharply raises China import forecast | $4.40 |
| Jan 2021 | Continued buying, South America dryness | $5.20 |
| Apr 2021 | USDA projects record China imports (26 MMT) | $5.80 |
| May 2021 | Corn reaches $7.35 (highest since 2013) | $7.35 |
| Jul 2021 | New crop (Dec 2021) hits $6.00 | $6.00 |

## Price Performance

### Corn Futures Rally
| Contract | Aug 2020 Low | May 2021 High | Rally % |
|----------|--------------|---------------|---------|
| Dec 2020 | $3.20 | $5.60 (expired) | +75% |
| March 2021 | $3.40 | $5.85 | +72% |
| May 2021 | $3.50 | $7.35 | +110% |
| Dec 2021 | $3.60 | $6.00 | +67% |

### Soybean Spillover
- Soybeans rallied from $8.50 to $16.00+ (88% gain)
- Meal and oil also surged
- Complete oilseed complex transformation

## Why China Bought So Much Corn

### Driving Factors
1. **Hog Herd Rebuilding** - After African Swine Fever (ASF) decimated herd in 2018-2019, rebuilding required massive feed
2. **Domestic Corn Deficit** - Chinese corn production lagged consumption growth
3. **Reserve Drawdown Complete** - State corn reserves depleted after years of sales
4. **Favorable US Prices** - Trade war truce made US corn competitive again
5. **Strong Yuan** - Chinese currency strength vs. USD improved buying power

### Chinese Corn Balance Sheet (MMT)
| Item | 2019/20 | 2020/21 | 2021/22 |
|------|---------|---------|---------|
| Production | 261 | 261 | 272 |
| Imports | 7.6 | 29.5 | 20.6 |
| Feed Use | 188 | 208 | 220 |
| Total Use | 280 | 295 | 302 |
| Ending Stocks | 196 | 191 | 181 |

### Import Explosion
- **2019/20 imports:** 7.6 MMT (normal)
- **2020/21 imports:** 29.5 MMT (record, +288%)
- Previous record: 7.3 MMT (1994/95)

## US Export Data

### Weekly Sales Highlights
```
Sept 2020: Record weekly corn sales
- Week ending 9/10: 2.3 MMT (mostly China)
- Week ending 9/17: 1.8 MMT
- Week ending 9/24: 2.1 MMT
```

### Cumulative Export Sales (Marketing Year)
| Month | 2020/21 Commitments | Previous Year | % of USDA Est |
|-------|---------------------|---------------|---------------|
| Oct 2020 | 44 MMT | 23 MMT | 67% |
| Jan 2021 | 58 MMT | 33 MMT | 82% |
| Apr 2021 | 69 MMT | 40 MMT | 97% |

### Export Pace vs. History
- Fastest export sales pace on record
- USDA repeatedly raised export forecast
- Final exports: 70.0 MMT (2.75 billion bushels)

## Weather Context

### 2020 US Crop
- **Derecho:** Aug 10, 2020 reduced yields in Iowa (see 2020_derecho.md)
- **Net Result:** Still above-trend yield at 172 bu/acre
- Production: 14.1 billion bushels

### South America 2020/21
- **La Nina pattern** developed in fall 2020
- **Argentina:** Drought stress, corn yields down 10%
- **Brazil safrinha:** Delayed planting, then frost damage in Jun/Jul 2021
- Combined South America shortfall added to bullish narrative

## Market Structure Changes

### Basis Behavior
| Location | Fall 2020 | Spring 2021 | Change |
|----------|-----------|-------------|--------|
| Gulf | +$0.40 | +$0.90 | +$0.50 |
| PNW | +$0.45 | +$1.10 | +$0.65 |
| Interior (IA) | -$0.05 | +$0.35 | +$0.40 |

- Export basis surged to multi-year highs
- Rail freight and barge rates increased dramatically
- Interior basis strengthened as supplies tightened

### Farmer Selling Behavior
- Many farmers undersold 2020 crop at harvest (~$4.00)
- Second chance came in 2021 ($5.50-7.00+)
- Storage paid exceptionally well

## SQL Query for Weather Analysis
```sql
-- Compare 2020 growing season weather to historical
SELECT
    ws.location_id,
    ws.display_name,
    EXTRACT(YEAR FROM ws.observation_date) as year,
    EXTRACT(MONTH FROM ws.observation_date) as month,
    AVG(ws.temp_high_f) as avg_high,
    SUM(ws.precipitation_in) as total_precip
FROM gold.weather_summary ws
WHERE ws.observation_date BETWEEN '2020-04-01' AND '2020-10-31'
AND ws.region = 'US_CORN_BELT'
GROUP BY ws.location_id, ws.display_name,
         EXTRACT(YEAR FROM ws.observation_date),
         EXTRACT(MONTH FROM ws.observation_date)
ORDER BY ws.location_id, month;
```

## Lessons for Future Events

### Demand Shock Recognition
1. **Watch weekly export sales** - China buying showed up immediately
2. **Cumulative pace matters** - Compare to USDA projection % at each date
3. **Multiple bullish factors stack** - China + derecho + La Nina = explosive rally

### Trading Implications
1. **Demand-driven rallies can be persistent** - Unlike weather scares that resolve
2. **Basis leads the way** - Export basis signaled tightness before futures fully caught up
3. **Carry structure inverts** - Nearby premiums develop as supplies tighten

### Risk Management Lessons
1. Farmers who priced at harvest ($4.00) left significant money on table
2. Scale-up selling on rallies reduces regret
3. Unusual export pace should prompt re-evaluation of marketing plan

## Comparison Events

| Event | Duration | Price Impact | Driver |
|-------|----------|--------------|--------|
| 2020-21 China | 12+ months | +110% | Demand surge |
| 2012 Drought | 4 months | +60% | Supply shock |
| 2007-08 Ethanol | 18 months | +150% | Structural demand |
| 2010-11 Russia Ban | 9 months | +80% | Export restrictions |

**2020-21 was unique as a pure demand-driven rally in a year of adequate US supply.**

---
*Report generated: 2026-01-23*
*Data sources: USDA FAS Export Sales, WASDE, CME*
