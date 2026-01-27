# 2018-2019 US-China Trade War

## Event Summary
The US-China trade war significantly impacted agricultural commodity markets, particularly soybeans. China imposed a 25% tariff on US soybeans in July 2018, fundamentally changing global trade flows and depressing US prices relative to Brazilian origins.

## Timeline

| Date | Event |
|------|-------|
| Mar 22, 2018 | Trump announces Section 301 tariffs on $50B Chinese goods |
| Apr 2, 2018 | China announces 25% tariff on US soybeans (retaliatory) |
| Apr 4, 2018 | Soybean prices begin steep decline |
| Jul 6, 2018 | Chinese tariffs take effect |
| Aug 2018 | US launches Market Facilitation Program (MFP) payments |
| Dec 1, 2018 | G20 meeting - 90-day truce announced |
| May 2019 | Negotiations break down, tariffs increase |
| Jan 15, 2020 | Phase One deal signed |

## Price Impact

### Soybean Futures (November 2018)
| Date | Price | Notes |
|------|-------|-------|
| Apr 1, 2018 | $10.40 | Pre-tariff announcement |
| Apr 6, 2018 | $10.00 | Initial sell-off |
| Jun 1, 2018 | $9.50 | Continued decline |
| Jul 16, 2018 | $8.40 | 4-year low, tariffs in effect |
| Sep 2018 | $8.20 | Harvest lows |

**Total Decline:** ~$2.20/bu (21%) from early April to harvest lows

### Corn Futures (Affected but less severe)
- Indirect impact through soybean acreage shifts
- 2019 saw increased corn acres as farmers shifted away from soybeans

## Trade Flow Disruption

### Before Trade War (2017)
- US exported ~33 MMT soybeans to China (62% of US exports)
- Brazil exported ~53 MMT to China

### After Tariffs (2018-2019)
- US exports to China collapsed to ~8 MMT
- Brazil exports surged, premium to US widened
- US soybeans deeply discounted vs. Brazil

### Basis Impact
```
Gulf Basis (CIF NOLA vs CBOT):
2017 average: +$0.50/bu
2018 harvest: -$0.20/bu (unprecedented weakness)
```

## Government Response

### Market Facilitation Program (MFP)
| Year | Payment Rate (Soybeans) | Total Program |
|------|------------------------|---------------|
| 2018 | $1.65/bu | $8.5B |
| 2019 | $2.05/bu | $14.5B |

- Payments based on historical production
- Controversial but stabilized farm income

## Long-Term Structural Changes

### Bullish for Brazil
- Accelerated infrastructure investment
- Expanded soybean acreage
- Established as preferred China supplier

### Bearish for US Competitiveness
- Lost market share difficult to regain
- Infrastructure advantages diminished
- Increased dependence on domestic demand

## SQL Query for Analysis
```sql
-- Price comparison around tariff announcement
SELECT
    observation_date,
    -- Would need price data table
    -- Placeholder for when price database is built
FROM price_data
WHERE commodity = 'soybeans'
AND observation_date BETWEEN '2018-03-01' AND '2018-12-31';
```

## Weather Context (2018)
The 2018 growing season was generally favorable for US crops:
- Above-trend yields despite trade tensions
- Large supplies added to price pressure
- Good weather = more production = lower prices during tariff period

## Lessons for Future Events

### Trade Policy Risks
1. **Lead time matters** - Markets price in announcements before implementation
2. **Retaliatory targeting** - Agriculture often first target
3. **Recovery slow** - Market share hard to regain

### Trading Implications
1. Watch for policy announcements during sensitive periods
2. Basis can move independently of flat price
3. Government programs provide floor but create distortions

### Comparison Points
- **Previous trade disputes**: Japan (1973 embargo)
- **Similar events**: Russia grain embargo, Argentina export taxes

---
*Report generated: 2026-01-23*
*Data sources: USDA FAS, CME historical (to be added)*
