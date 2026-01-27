# [CROP] Marketing Year [YYYY-YY] Summary

## Overview
**Marketing Year:** [Start Date] to [End Date]
**Key Theme:** [Brief description - e.g., "Drought-reduced production" or "Record exports"]

## Production Summary

### Acreage
| Metric | This Year | Previous Year | 5-Year Avg |
|--------|-----------|---------------|------------|
| Planted Acres | | | |
| Harvested Acres | | | |
| Abandonment % | | | |

### Yield
| Metric | This Year | Previous Year | Trend |
|--------|-----------|---------------|-------|
| National Yield | | | |
| vs. Trend | | | |

### Regional Yields
| Region | Yield | vs. Previous Year |
|--------|-------|-------------------|
| Corn Belt | | |
| Northern Plains | | |
| Southern Plains | | |
| Delta | | |

### Total Production
| Metric | Value | Units |
|--------|-------|-------|
| Total Production | | MMT / Billion Bu |

## Supply/Demand Balance (Million Bushels)

| Item | This Year | Previous Year | Change |
|------|-----------|---------------|--------|
| Beginning Stocks | | | |
| Production | | | |
| Imports | | | |
| **Total Supply** | | | |
| Feed/Residual | | | |
| Food/Seed/Industrial | | | |
| Exports | | | |
| **Total Use** | | | |
| **Ending Stocks** | | | |
| Stocks/Use Ratio | | | |

## Price Performance

### Futures
| Metric | Price | Date |
|--------|-------|------|
| Marketing Year High | | |
| Marketing Year Low | | |
| Harvest Low | | |
| Average Price | | |

### Basis (Cash - Futures)
| Location | Harvest | Year Avg |
|----------|---------|----------|
| Gulf | | |
| PNW | | |
| Interior | | |

## Weather Summary

### Key Events
1. [Event 1]
2. [Event 2]
3. [Event 3]

### Growing Season Summary
| Month | Conditions | Impact |
|-------|-----------|--------|
| April | | |
| May | | |
| June | | |
| July | | |
| August | | |
| September | | |

### SQL Query for Weather Data
```sql
SELECT
    ws.location_id,
    EXTRACT(MONTH FROM ws.observation_date) as month,
    AVG(ws.temp_high_f) as avg_high,
    SUM(ws.precipitation_in) as total_precip
FROM gold.weather_summary ws
WHERE ws.observation_date BETWEEN '[START_DATE]' AND '[END_DATE]'
AND ws.region = 'US_CORN_BELT'
GROUP BY ws.location_id, EXTRACT(MONTH FROM ws.observation_date)
ORDER BY ws.location_id, month;
```

## Policy/Trade Events

### Government Programs
- [List any relevant farm programs, payments, etc.]

### Trade Events
- [List any tariffs, trade agreements, etc.]

### Export Highlights
| Destination | Volume | Change YoY |
|-------------|--------|------------|
| China | | |
| Mexico | | |
| Japan | | |
| Other | | |

## Key Takeaways

1. **[Takeaway 1]**
2. **[Takeaway 2]**
3. **[Takeaway 3]**

## Lessons/Patterns for Future

- [Pattern 1 to watch]
- [Pattern 2 to watch]

---
*Report generated: [DATE]*
*Data sources: USDA WASDE, NASS, FAS, CME*
