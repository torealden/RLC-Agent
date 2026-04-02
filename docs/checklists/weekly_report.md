# Weekly Report Production Checklist

## Target: Every Friday by 5:00 PM ET
## Delivery: PDF to subscribers + post to website

---

## Monday

### Content Planning
- [ ] Review prior week's report — any follow-ups needed?
- [ ] Identify this week's lead story
  - What's the market talking about?
  - What report dropped this week that moves our markets?
  - What did DOC flag as anomalous?
- [ ] Outline section narratives (1-2 sentences each)

---

## Tuesday - Thursday

### Data Collection & Analysis
- [ ] Confirm all weekly data has arrived
  - Monday: Crop Progress, FGIS Inspections
  - Wednesday: EIA Petroleum
  - Thursday: Export Sales
  - Friday (prior): CFTC COT
- [ ] Update any balance sheet adjustments based on weekly data
- [ ] Run crush margin engine for current week
- [ ] Note any price moves > 2 standard deviations (DOC anomalies)

### Section Drafts
- [ ] Executive Summary (write last — 3-4 bullet points)
- [ ] Price Dashboard (auto-generated from database)
  - Pull latest prices, calculate WoW/MoM/YoY
  - Generate range bar chart
  - Update regional spotlight
- [ ] Feedstock Markets narrative
  - Vegetable oils: what moved, why
  - Animal fats & greases: what moved, why
  - Key spread changes
- [ ] Crush & Processing Economics
  - Board crush margin trend
  - Biofuel production margins by feedstock
  - Any capacity changes or announcements
- [ ] Biofuel Production & Policy
  - RIN generation / compliance
  - Policy developments
  - EPA / state program updates
- [ ] Trade Flows
  - Export sales highlights
  - Import trends (UCO, palm, etc.)
  - Census monthly data if released this week
- [ ] Global S&D Snapshot
  - Only what changed this week
  - WASDE commentary if WASDE week
  - CONAB/MPOB if released
- [ ] Forward Look
  - Next week's data releases
  - Seasonal patterns to watch
  - Key risk factors

---

## Friday

### Assembly & Publication
- [ ] GENERATE all charts from database
  ```
  python scripts/presentation/price_dashboard_v3.py
  python scripts/presentation/report_viz_mockups.py
  python scripts/presentation/report_dashboards.py
  ```
- [ ] ASSEMBLE PDF
  - Page 1: Executive Summary + Key Metrics
  - Page 2: Price Dashboard (range bars)
  - Page 3-4: Feedstock Markets narrative + charts
  - Page 5: Crush & Processing Economics
  - Page 6: Trade Flows
  - Page 7: Global S&D
  - Page 8: Forward Look
- [ ] REVIEW
  - Read through for errors, stale data, formatting
  - Check all charts generated from current week's data
  - Verify all prices are current (not last week's)
- [ ] PUBLISH
  - Save PDF to output/reports/weekly/
  - Upload to website (roundlakescommodities.com)
  - Send via Buttondown to subscriber list
  - Post teaser to LinkedIn
- [ ] ARCHIVE
  - Commit report to git
  - Log any forecast changes made this week
  - Note what worked and what to improve

---

## Quality Control Checklist

Before publishing, verify:
- [ ] All prices are from THIS week (not stale)
- [ ] Marketing year labels are correct (Sep-Aug for soy/corn, etc.)
- [ ] Units are consistent throughout (don't mix MT and bushels)
- [ ] No placeholder text remaining ("TBD", "XX", "lorem ipsum")
- [ ] Charts match narrative (if you say "prices rose," the chart shows up)
- [ ] Competitor check: does this add value beyond what's freely available?
- [ ] Spell check / grammar check
- [ ] Footer: correct date, correct company name, disclaimer
