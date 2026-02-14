# 2022 Russia-Ukraine War Impact on Grain Markets

## Event Summary
Russia's invasion of Ukraine on February 24, 2022, triggered the most significant disruption to global grain markets since the 1970s Soviet grain embargoes. Ukraine ("the breadbasket of Europe") and Russia together account for ~30% of global wheat exports and ~20% of corn exports. The war immediately threatened Black Sea exports, sending grain prices to all-time highs.

## Timeline

| Date | Event | Market Impact |
|------|-------|---------------|
| Feb 24, 2022 | Russia invades Ukraine | Limit up moves across grains |
| Feb 28, 2022 | SWIFT sanctions on Russian banks | Export payment concerns |
| Mar 4, 2022 | Wheat hits $13.63/bu (all-time high) | +$5.00 in 8 days |
| Mar 8, 2022 | Corn hits $7.82/bu, Soy hits $17.59 | Multi-year highs |
| Apr 2022 | Spring planting impossible in Ukraine combat zones | 2022/23 crop concerns |
| May 2022 | Ports fully blocked, Ukraine exports collapse | Black Sea premium soars |
| Jul 22, 2022 | Black Sea Grain Initiative signed (Turkey-brokered) | Prices retreat |
| Aug 2022 | First grain ships leave Odesa | Relief rally fades |
| Jul 17, 2023 | Russia withdraws from grain deal | Prices spike briefly |
| 2024 | Alternative export routes established | "New normal" |

## Price Performance

### Futures Highs (All Contracts)
| Commodity | Pre-War (Feb 23) | War High | Change | Date of High |
|-----------|------------------|----------|--------|--------------|
| Wheat (CK2) | $8.50 | $13.63 | +60% | Mar 8, 2022 |
| Corn (CK2) | $6.70 | $8.24 | +23% | Apr 29, 2022 |
| Soybeans (SK2) | $16.50 | $17.84 | +8% | Jun 9, 2022 |
| CBOT Wheat | $8.00 | $12.94 | +62% | May 17, 2022 |

### Wheat Volatility
- Feb 24-Mar 8: +$5.00/bu in 8 trading days
- Mar-May: Multiple limit moves (up and down)
- Historic implied volatility levels (50%+)
- KCBT and MGEX wheat also hit records

## Production/Export Disruption

### Ukraine's Normal Role
| Crop | Ukraine Production | World Share | Export Share |
|------|-------------------|-------------|--------------|
| Wheat | 33 MMT | 4% | 10% |
| Corn | 42 MMT | 3% | 15% |
| Sunflower | 17 MMT | 30% | 46% |
| Barley | 10 MMT | 6% | 15% |

### Export Collapse (2022/23 vs. 2021/22)
| Period | Ukraine Wheat Exports | Change |
|--------|----------------------|--------|
| 2021/22 | 19 MMT | - |
| 2022/23 (initial forecast) | 10 MMT | -47% |
| 2022/23 (actual) | 15 MMT | -21% |

*Recovery aided by Black Sea Grain Initiative and rail/road alternatives*

### Russian Production/Exports
- Russia's own exports initially disrupted by sanctions/payments
- 2022/23 Russian wheat production: 92 MMT (record)
- Russia eventually found buyers (China, Middle East, Africa)
- Russian wheat sold at significant discounts to global prices

## Global Food Security Impact

### Import-Dependent Countries Most Affected
| Country | Ukraine/Russia Share of Wheat Imports |
|---------|--------------------------------------|
| Egypt | 80%+ |
| Turkey | 70%+ |
| Bangladesh | 60%+ |
| Lebanon | 75%+ |
| Tunisia | 65%+ |
| Somalia | 90%+ |

### Food Price Indices
- FAO Food Price Index hit all-time high March 2022
- Bread prices surged in Middle East, Africa
- Fertilizer prices also spiked (Russia major exporter)

## US Market Implications

### US Wheat
- Strong export sales initially as buyers sought alternatives
- HRW drought in 2022 compounded tightness
- US wheat exports actually increased in 2022/23

### US Corn/Soybeans
- Rally extended from already-elevated 2021 levels
- New crop 2022 pricing opportunities exceptional
- Farmers rewarded for scale-up selling

### Basis Impact
| Location | Pre-War | Spring 2022 | Change |
|----------|---------|-------------|--------|
| Gulf Wheat | +$0.50 | +$1.50 | +$1.00 |
| Gulf Corn | +$0.75 | +$1.10 | +$0.35 |
| PNW Wheat | +$0.80 | +$1.75 | +$0.95 |

## Black Sea Grain Initiative

### Key Terms (Jul 2022)
- UN and Turkey brokered agreement
- Safe corridors established for commercial ships
- Joint Coordination Centre in Istanbul
- Initial 120-day term with renewals

### Performance
- ~33 million metric tons exported first year
- Corn largest commodity (~50%)
- Wheat second (~30%)
- Prices retreated as exports resumed

### Collapse and Adaptation
- Russia withdrew July 2023
- Ukraine developed alternative routes:
  - Danube River to Romania
  - Rail to Poland/EU
  - New Black Sea corridor (with naval protection)

## Weather Context (2022 US Crop)

### US Growing Season
- **HRW Wheat:** Severe drought in KS, OK, TX - yields down 30%
- **Corn/Soy:** Generally favorable Midwest conditions
- **Corn yield:** 173.3 bu/acre (near trend)

### SQL Query for 2022 Weather
```sql
-- 2022 wheat country drought analysis
SELECT
    ws.location_id,
    ws.display_name,
    EXTRACT(MONTH FROM ws.observation_date) as month,
    AVG(ws.temp_high_f) as avg_high,
    SUM(ws.precipitation_in) as monthly_precip
FROM gold.weather_summary ws
WHERE ws.observation_date BETWEEN '2022-01-01' AND '2022-06-30'
AND ws.region IN ('US_SOUTHERN_PLAINS', 'US_NORTHERN_PLAINS')
GROUP BY ws.location_id, ws.display_name,
         EXTRACT(MONTH FROM ws.observation_date)
ORDER BY ws.location_id, month;
```

## Lessons for Future Events

### Geopolitical Risk Factors
1. **Concentration risk** - Black Sea region dominates wheat/corn exports
2. **Swift moves** - Markets repriced immediately, then corrected
3. **Alternative routes develop** - Supply chains adapt over 12-18 months
4. **Food security premium** - Import-dependent countries will pay up

### Trading Implications
1. **Initial spike often overdone** - Mar 2022 highs never revisited
2. **Volatility creates opportunities** - Option premiums explode
3. **Basis reflects physical tightness** - Watch export pace
4. **Weather still matters** - US HRW drought compounded issue

### Long-Term Structural Changes
1. Importers diversifying away from Black Sea dependence
2. Increased strategic grain reserve interest globally
3. Ukraine developing non-Black Sea export capacity
4. Russian grain sold at persistent discounts

## Comparison Events

| Event | Year | Commodity | Price Impact |
|-------|------|-----------|--------------|
| Ukraine War | 2022 | Wheat | +60% to ATH |
| Russia Export Ban | 2010 | Wheat | +80% |
| Soviet Grain Embargo | 1980 | Wheat/Corn | +30% |
| Argentina Drought | 2018 | Soybeans | +20% |

**The 2022 Ukraine crisis was the largest geopolitical supply shock to grain markets in 50 years.**

---
*Report generated: 2026-01-23*
*Data sources: USDA FAS, AMIS, FAO, CME*
