# WASDE Day Checklist

## Release: ~12:00 PM ET, monthly (8th-12th of month)
## Next: April 9, 2026

---

## T-1: Day Before WASDE

### Pre-Report Preparation
- [ ] Record current RLC projections for all key variables
  - US corn: production, exports, ending stocks
  - US soybeans: production, crush, exports, ending stocks
  - US wheat: production, exports, ending stocks
  - US soybean oil: domestic use (BBD + non-BBD), exports, stocks
  - US soybean meal: domestic use, exports, stocks
  - World: production and ending stocks for corn, soybeans, wheat
  - Brazil/Argentina: production for soybeans and corn
- [ ] Record current LLM/model projections for same variables
- [ ] Note consensus expectations (Bloomberg, Reuters surveys if available)
- [ ] Identify the 3-5 variables most likely to move the market
- [ ] Pre-position analysis: what does the market expect vs what we expect?

---

## T-0: WASDE Release Day

### Immediate (12:00 - 12:15 PM ET)
- [ ] VERIFY: WASDE data collector triggered
  - Check: `python -m src.dispatcher run usda_wasde`
  - Confirm data saved to bronze.fas_psd
- [ ] SCAN: Identify the surprises
  - What changed from last month's WASDE?
  - What differs from our projection?
  - What differs from consensus?
- [ ] PRIORITIZE: Rank changes by market impact
  - Ending stocks changes (STU implications)
  - Production revisions
  - Demand revisions (especially BBD/biofuel use)

### First Hour (12:15 - 1:00 PM ET)
- [ ] UPDATE gold views: Run all balance sheet views to refresh
  ```
  psql -f scripts/ollama/output/all_balance_sheet_views.sql
  ```
- [ ] COMPUTE: Run crush margin engine with new S&D context
  ```
  python -m src.engines.oilseed_crush.engine --period YYYY-MM
  ```
- [ ] COMPARE: Pull up the comparison
  - USDA new vs USDA old (what they changed)
  - USDA new vs RLC projection (where we were right/wrong)
  - USDA new vs LLM projection (where the model was right/wrong)
- [ ] DOCUMENT: Note the key differences and reasoning

### Analysis Phase (1:00 - 3:00 PM ET)
- [ ] UPDATE spreadsheets
  - Import new WASDE numbers into balance sheet annual section
  - Adjust monthly projections if annual totals changed
  - Recalculate STU ratios
  - Update price forecasts if S&D picture changed materially
- [ ] RUN models
  - Crush margin implications of new S&D
  - Feedstock allocation: did the veg oil S&D change enough to shift economics?
  - Export pace: is USDA's export projection achievable given weekly pace?
  - Crush pace: is USDA's crush projection tracking actual NASS data?
- [ ] RECONCILE
  - Where do RLC and LLM projections now differ from USDA?
  - Are the differences justified by data USDA doesn't have?
  - Or did we miss something USDA caught?

### Publication (3:00 - 5:00 PM ET)
- [ ] DRAFT WASDE commentary for weekly report
  - Lead with the surprise (what changed that the market didn't expect)
  - Explain the implication (what it means for feedstock markets)
  - Our view (do we agree with USDA's direction? magnitude?)
- [ ] GENERATE charts
  - Updated waterfall chart
  - Ending stocks trend (new bar highlighted)
  - Any changed global numbers
- [ ] UPDATE dashboard
  - Refresh Global Oilseeds S&D page
  - Update WASDE dashboard with new numbers

### End of Day
- [ ] DOC runs with updated data
- [ ] Log forecast accuracy
  - Which variables did RLC get closer than USDA?
  - Which did the LLM get closer?
  - Update running accuracy tracker
- [ ] JOURNAL: What did we learn? What do we do differently next month?

---

## Key Variables to Track

### US Soybeans
| Variable | Units | Our Est | USDA Prior | USDA New | Diff |
|----------|-------|---------|------------|----------|------|
| Production | mil bu | | | | |
| Crush | mil bu | | | | |
| Exports | mil bu | | | | |
| Ending Stocks | mil bu | | | | |
| STU % | % | | | | |
| Farm Price | $/bu | | | | |

### US Soybean Oil
| Variable | Units | Our Est | USDA Prior | USDA New | Diff |
|----------|-------|---------|------------|----------|------|
| Production | mil lbs | | | | |
| BBD Use | mil lbs | | | | |
| Non-BBD Dom | mil lbs | | | | |
| Exports | mil lbs | | | | |
| Ending Stocks | mil lbs | | | | |

### US Corn
| Variable | Units | Our Est | USDA Prior | USDA New | Diff |
|----------|-------|---------|------------|----------|------|
| Production | mil bu | | | | |
| Ethanol Use | mil bu | | | | |
| Exports | mil bu | | | | |
| Ending Stocks | mil bu | | | | |

### World
| Variable | Units | Our Est | USDA Prior | USDA New | Diff |
|----------|-------|---------|------------|----------|------|
| Brazil Soy Prod | 1000 MT | | | | |
| Argentina Soy Prod | 1000 MT | | | | |
| China Soy Imports | 1000 MT | | | | |
| World Soy End Stk | 1000 MT | | | | |
| World Corn End Stk | 1000 MT | | | | |
| World Wheat End Stk | 1000 MT | | | | |
