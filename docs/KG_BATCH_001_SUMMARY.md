# Knowledge Graph Extraction: Batch 001 Summary

**Date:** 2026-02-14  
**Sources processed:** 18 reports (12 Canola Oil weekly, 8 RIN Forecast weekly)  
**Date range:** July 2019 — May 2022  
**Report origin:** The Jacobsen (now part of HigbyBarrett)

---

## Extraction Totals

| Category | Count |
|----------|-------|
| Nodes | 38 |
| Edges | 16 |
| Contexts | 11 |
| Sources registered | 20 |

---

## Key Analytical Framework Elements Discovered

### 1. The HOBO Spread as Master Lead Indicator
Across both the canola oil and RIN report series, the HOBO spread (soybean oil minus heating oil) emerges as the single most referenced analytical anchor. It appears in virtually every RIN report and repeatedly in canola oil reports when discussing biofuel demand. The analyst treats HOBO as the "best lead indicator" for D4 RIN direction and biodiesel production economics — which then cascade into feedstock demand including canola oil.

**Confidence: Very High (referenced in 10+ reports across 3 years)**

### 2. Canadian Canola Supply Chain: Stocks → Crush → Oil → Price
The most detailed causal chain in the canola oil reports runs from Canadian carryout levels through COPA crushing volumes to canola oil production and ultimately LA cash prices. The analyst monitors COPA weekly crush data as the earliest signal, with detailed rules for interpreting week-over-week changes and translating weekly data into monthly estimates.

**Key insight:** When Canadian canola ending stocks are tight (<1M tonnes), weekly crush volumes decline sharply in the final months of the marketing year, even if seed prices would otherwise support continued processing. This supply constraint is the primary driver of canola oil basis expansion.

**Confidence: Very High (core analytical framework, consistent 2020-2022)**

### 3. BTC/RIN Substitution Dynamic
A distinctive and nuanced framework: D4 RINs and the Blender Tax Credit are alternative margin sources for biodiesel producers. When one weakens, the other must strengthen to keep production viable. This creates a "breakaway point" where D4 RINs diverge from the broader RIN complex (particularly D6 ethanol RINs). This is identified as D4-specific territory that most market participants miss.

**Confidence: High (explicitly articulated multiple times, 2019)**

### 4. SRE Political Cycle Framework
The analyst has a clear mental model of SRE risk as a political cycle phenomenon — refinery-state vs. farm-state interests, especially during election cycles. The framework includes specific expectations about White House behavior, Congressional dynamics, and the timeline of RVO finalization as constraints on policy action.

**Confidence: High (multiple reports in 2019, likely evolved post-2020)**

### 5. Substitution Hierarchy in Vegetable Oils
The reports reveal a clear substitution hierarchy:
- **In food manufacturing:** SBO → canola oil (health preference) and canola oil → SBO (price)
- **In biodiesel:** Canola oil ↔ corn oil ↔ SBO (price-driven), with LCFS/CI score favoring canola
- **In renewable diesel (emerging 2021+):** New demand channel with different feedstock preferences

The analyst tracks the canola/SBO spread as the key switching signal, noting the long-term average of 10 cents expanded to 30+ cents during the 2021-2022 supply crisis.

**Confidence: Very High (consistent across all canola oil reports)**

### 6. Soil Moisture > Planting Pace for Canola
A clear expert rule: for canola specifically, planting delays matter less than soil moisture. Farmers can plant through mid-to-late June without yield concerns. Subsoil moisture is more critical than topsoil for long-term crop development. This is a differentiated view from how the market typically trades planting progress data.

**Confidence: High (explicitly stated in 2022, implied in earlier reports)**

---

## Framework Elements to Watch for in Future Batches

Several themes are mentioned but not yet fully developed in this initial batch:
- **Renewable diesel demand for canola oil** (mentioned as starting Q4 2021/22 — later reports will have the actual data)
- **Palm oil competition** with canola oil in biodiesel (mentioned briefly, likely more detail in other report series)
- **Indonesia export policy** impact on vegetable oil complex (not yet in these reports)
- **WASDE revision patterns** — how Jacobsen forecasts eventually converge with USDA
- **Residual/negative residual** as a signal of underreported prior-year stocks

---

## Notes on Source Quality

The canola oil ("CO") reports are highly polished, data-dense weekly analysis with explicit balance sheet forecasts, quantified market expectations, and detailed data interpretation. These are excellent KG source material.

The RIN Forecast reports from 2019 are rougher — shorter, more stream-of-consciousness market commentary. They contain strong directional framework elements (HOBO as lead indicator, BTC/RIN substitution) but less quantified detail. The later RIN reports (2019 Sept onwards) show more polish, likely reflecting editorial evolution.

The HigbyBarrett Weekly Report format appears to be the comprehensive client deliverable but was too large for the API to fetch in one call — will need to be processed in sections or through a different approach.

---

## Next Steps

1. **Process remaining canola oil reports** in the folder (appears to be ~100+ covering 2020-2022)
2. **Process remaining RIN reports** (appears to be ~100+ from 2019+)  
3. **Attempt HigbyBarrett weekly reports** — these likely contain the most comprehensive cross-commodity analysis
4. **Look for other report series** as more files sync: corn, soybeans, palm oil, renewable diesel specific
5. **Begin reinforcement scoring** — as we process more reports, existing edges get source_count bumped
