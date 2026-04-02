# NASS Grain Crushings Report Day Checklist

## Release: ~3:00 PM ET, monthly (same day as Fats & Oils)
## Data: Prior month's corn for ethanol/alcohol, sorghum crush
## Key output: Corn oil (DCO) production is derived from corn grind

---

## T-1: Day Before Release

### Pre-Report Preparation
- [ ] Record current RLC estimates
  - Corn crushed for alcohol (mil bu) — ethanol grind
  - Corn crushed for other uses
  - Sorghum crushed
  - Implied DCO production (grind × DCO yield per bushel)
- [ ] Record LLM/model estimates
- [ ] Check EIA ethanol production for the reporting month
  - Implied corn grind from EIA weekly ethanol production
  - EIA production × corn-per-gallon yield ≈ expected NASS grind
- [ ] Review ethanol margin trend for the month
  - Were margins supportive of full utilization?
  - Any plant shutdowns or maintenance reported?

---

## T-0: Report Day

### Immediate (3:00 - 3:15 PM ET)
- [ ] VERIFY: NASS processing collector triggered
  - Confirm data in silver.monthly_realized (source: NASS_GRAIN_CRUSH)
- [ ] SCAN key numbers
  - Corn for alcohol: above or below estimate?
  - YoY change: is the grind trend accelerating or slowing?
  - Sorghum: any surprises?

### Compare & Update (3:15 - 4:30 PM ET)
- [ ] COMPARE actual vs estimates
  | Item | RLC Est | LLM Est | EIA Implied | Actual | Error |
  |------|---------|---------|-------------|--------|-------|
  | Corn for Alcohol | | | | | |
  | Sorghum Crush | | | | | |
  | Implied DCO Prod | | | | | |

- [ ] UPDATE balance sheet monthly blocks
  - Enter actual corn grind in corn balance sheet
  - Calculate DCO production: grind × DCO yield (~1.6-1.8 lbs/bu)
  - Enter DCO production in DCO balance sheet
  - Update ethanol balance sheet implied production
  - Verify vs EIA weekly production totals for the month

- [ ] RECALCULATE
  - Cumulative grind pace vs USDA annual FSI projection
  - Is USDA's corn-for-ethanol number achievable?
  - If pace is running hot/cold, what does that mean for:
    - Corn ending stocks?
    - DCO availability for biofuel feedstock?
    - DDG/DDGS supply?
    - Ethanol S&D balance?

### Cross-Market Implications (4:30 - 5:30 PM ET)
- [ ] ASSESS feedstock impact
  - DCO production trend: more/less available for RD/biodiesel?
  - Update feedstock allocation model with new DCO supply estimate
  - DCO price implications: if supply growing, pressure on DCO premiums?
- [ ] ASSESS ethanol market impact
  - Grind vs production efficiency: ethanol yield per bushel trending?
  - Co-product supply: DDG/DDGS production for feed market
  - Corn oil extraction rate improvements at ethanol plants?
- [ ] UPDATE crush margin engine
  - Corn-ethanol margin with actual grind data
  - Compare to board crush implied margins

### Publication
- [ ] DRAFT Grain Crushings commentary
  - Corn grind pace and ethanol demand story
  - DCO supply implications for feedstock markets
  - Sorghum note if relevant
- [ ] UPDATE relevant charts
  - Ethanol production/stocks (EIA dashboard)
  - DCO supply in feedstock allocation
  - Corn grind pace tracker

---

## Corn Grind Pace Tracker

| Month | Actual (mil bu) | Cumulative | USDA Annual | Pace % | YoY Change |
|-------|----------------|------------|-------------|--------|------------|
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

## DCO Production Tracker

| Month | Corn Grind | DCO Yield (lbs/bu) | DCO Prod (mil lbs) | Cumulative | YoY |
|-------|-----------|-------------------|-------------------|------------|-----|
| Sep | | | | | |
| Oct | | | | | |
| ... | | | | | |

## Key Relationships to Monitor

1. **EIA weekly ethanol → NASS monthly grind**: EIA is weekly and timelier.
   Monthly EIA sum should approximate NASS grind when adjusted for yield.
   Persistent divergence = data quality issue or yield change.

2. **Corn grind → DCO supply → Feedstock allocation**: More grind = more DCO.
   DCO is the cheapest veg oil feedstock. Supply growth keeps DCO prices
   below SBO, which shifts the optimal feedstock mix.

3. **Ethanol margins → grind pace**: When margins are positive, plants run at
   capacity. When negative, plants idle. Margin is the leading indicator;
   grind is the lagging confirmation.

4. **Sorghum grind**: Small but watch for substitution when corn prices spike.
   Some ethanol plants can switch. Also relevant for sorghum export competition
   with China.
