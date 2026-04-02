# NASS Fats & Oils Report Day Checklist

## Release: ~3:00 PM ET, monthly (~3rd week of month)
## Data: Prior month's crush, oil production, stocks

---

## T-1: Day Before Release

### Pre-Report Preparation
- [ ] Record current RLC estimates for the reporting month
  - Soybean crush (mil bu)
  - Soybean oil production crude (mil lbs)
  - Soybean oil extraction rate (lbs/bu)
  - Soybean oil ending stocks (mil lbs)
  - Canola crush (thou MT)
  - Cottonseed crush (thou ST)
  - Sunflower crush (thou ST)
- [ ] Record LLM/model estimates (from crush margin engine)
- [ ] Calculate NOPA-implied crush if NOPA already released
  - NOPA covers ~95% of soy crush — gross up for missing plants
- [ ] Check: Are our monthly block projections in the balance sheets current?

---

## T-0: Report Day

### Immediate (3:00 - 3:15 PM ET)
- [ ] VERIFY: NASS processing collector triggered
  - Check: `python -m src.dispatcher run nass_processing`
  - Confirm data in silver.monthly_realized
- [ ] SCAN key numbers
  - Soybean crush: above or below our estimate?
  - Oil yield: trending higher or lower?
  - SBO stocks: building or drawing?
  - Surprise in any minor oilseed?

### Compare & Update (3:15 - 4:30 PM ET)
- [ ] COMPARE actual vs estimates
  | Item | RLC Est | LLM Est | Actual | RLC Error | LLM Error |
  |------|---------|---------|--------|-----------|-----------|
  | Soy Crush | | | | | |
  | Oil Prod | | | | | |
  | Oil Yield | | | | | |
  | SBO Stocks | | | | | |
  | Canola | | | | | |
  | Cottonseed | | | | | |

- [ ] UPDATE balance sheet monthly blocks
  - Enter actual crush in the soy_balance_sheet monthly crush block
  - Enter actual oil production in soyoil_balance_sheet monthly production block
  - Enter actual stocks
  - Update extraction rate
  - Verify accumulator totals match

- [ ] RECALCULATE
  - Remaining months needed to hit USDA annual projection
  - Implied pace: is USDA's annual crush achievable?
  - If not, which way does ending stocks move?
  - Crush margin engine: does new actual change the calibration?

### Implications (4:30 - 5:30 PM ET)
- [ ] ASSESS feedstock market impact
  - Higher crush = more SBO supply = bearish SBO, bullish meal?
  - Lower crush = less SBO = bullish SBO, bearish meal?
  - Oil yield change: structural or seasonal?
  - Stock draw/build: pace of BBD demand vs production
- [ ] UPDATE crush margin engine with actual data
  ```
  python -m src.engines.oilseed_crush.engine --calibrate soybeans
  ```
- [ ] RUN feedstock allocation with updated oil supply
- [ ] UPDATE weekly report charts if publishing this week

### Publication
- [ ] DRAFT Fats & Oils commentary
  - Monthly crush vs expectations
  - Oil production and yield trend
  - Stock implications for price
  - Minor oilseed highlights
- [ ] GENERATE Fats & Oils dashboard (rpt_04 template)

### End of Day
- [ ] DOC captures the update
- [ ] Log forecast accuracy in tracker
- [ ] JOURNAL: What drove the miss (if any)? Data issue or analytical error?

---

## Crush Pace Tracker

After each report, update the cumulative pace:

| Month | Actual | Cumulative | USDA Annual | Pace % | Implied Remaining |
|-------|--------|------------|-------------|--------|-------------------|
| Sep | | | | | |
| Oct | | | | | |
| Nov | | | | | |
| Dec | | | | | |
| Jan | | | | | |
| Feb | | | | | |
| Mar | | | | | |
| Apr | | | | | |
| May | | | | | |
| Jun | | | | | |
| Jul | | | | | |
| Aug | | | | | |
