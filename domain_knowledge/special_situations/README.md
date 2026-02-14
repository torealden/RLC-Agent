# Special Situations Reports

This folder contains analysis of significant market events that impacted agricultural commodities. These reports provide context for the LLM to understand historical price movements and their causes.

## Report Structure

Each special situation report should include:
1. **Event Overview** - What happened and when
2. **Weather/Production Data** - Relevant weather conditions and yield impacts
3. **Price Impact** - How markets reacted (corn, soybeans, wheat, etc.)
4. **Market Timeline** - Key dates and price movements
5. **Lessons Learned** - Patterns to watch for in future

## Key Events Catalog

### US Drought Events
- **2012 Drought** - Most severe since 1950s, major corn/soybean impact
- **2011 Drought** - Texas/Southern Plains wheat impact
- **2021 Drought** - Northern Plains, spring wheat impact
- **1988 Drought** - Historic reference point

### Weather Events
- **2020 Derecho** - August 10, 2020 - Iowa corn destruction
- **2019 Midwest Floods** - Planting delays, prevented planting
- **2008 Midwest Floods** - June flooding, yield impact
- **2013 Cold Spring** - Late planting, compressed growing season

### Trade/Policy Events
- **2018-2019 US-China Trade War** - Tariffs on soybeans
- **2022 Ukraine War** - Wheat/corn supply disruption
- **2020 COVID-19** - Demand destruction then recovery
- **2008 Financial Crisis** - Commodity supercycle peak

### South American Events
- **2021 Brazil Drought** - Safrinha corn impact
- **2012 Argentina Drought** - Soy/corn production loss
- **2020 Argentina Drought** - Production shortfall
- **La Niña Cycles** - Pattern recognition

## Data Sources for Reports

### Weather Data (from our database)
```sql
-- Example: 2012 drought analysis
SELECT
    location_id,
    observation_date,
    temp_high_f,
    precipitation_in,
    precip_7day_total_in
FROM gold.weather_summary
WHERE observation_date BETWEEN '2012-05-01' AND '2012-09-30'
AND region = 'US_CORN_BELT'
ORDER BY location_id, observation_date;
```

### Yield Data (USDA NASS)
- Crop Production reports
- Acreage reports
- Yield forecasts

### Price Data (to be built)
- Futures prices (corn, soybeans, wheat)
- Basis levels
- Spreads

## Completed Reports

| File | Event | Key Impact |
|------|-------|------------|
| `2012_us_drought.md` | US Drought | Corn yield -40 bu/acre vs trend, $8.49 high |
| `2018_china_trade_war.md` | US-China Trade War | Soybeans -$2.20/bu (21% decline) |
| `2020_derecho.md` | Iowa Derecho | 550M bushels corn lost, $0.35 rally |
| `2020_china_demand_surge.md` | China Corn Buying | Corn +110% ($3.20 to $7.35) |
| `2022_ukraine_war.md` | Russia-Ukraine War | Wheat +60% to all-time high |

## Report Naming Convention

`YYYY_event_name.md`

Examples:
- `2012_us_drought.md`
- `2018_china_trade_war.md`
- `2020_derecho.md`
- `2020_china_demand_surge.md`
- `2022_ukraine_war.md`
