# Knowledge Graph Extraction: Batch 005 Summary

**Date:** 2026-02-14  
**Sources:** 6 HB Weekly Text reports (Jul-Sep 2020, Jul-Sep 2021, Jul-Sep 2022)  
**Focus:** Growing season analytical frameworks

## Running Totals

| Category | Batch 005 | Running Total |
|----------|-----------|---------------|
| Nodes | 9 | 93 |
| Edges | 8 | 51 |
| Contexts | 5 | 34 |
| Sources | 6 | 40 |

## Key Frameworks Extracted

### 1. Crop Condition → Yield Prediction Model (BREAKTHROUGH)
The core growing season analytical engine. Uses G/E rating CHANGE (not level) between first week of prior month and first week of report month. Key insight: seasonal decline is normal (corn avg -3.4% Aug→Sep), so only DEVIATION from seasonal average signals yield change. Model validated across 2020-2022 with specific parameters for each WASDE month. When model diverges from analyst consensus by >1 bpa, the divergence direction IS the trade.

**Specific parameters captured:**
- Corn Aug: unchanged rating → +3.9 bpa expected. Need >5% G/E drop for unchanged yield
- Corn Sep: avg seasonal decline 3.4%. 4% drop → only -0.1 bpa cut implied
- Soy Aug: 1% G/E drop threshold for yield cut (more sensitive than corn)
- Sep 2022 validation: model said -0.1, analysts said -2.9. Model was right

### 2. Growing Season Calendar (Master Timeline)
Complete month-by-month framework mapping weather events, development stages, and USDA report catalysts. From May planting through January Annual Summary. Identifies when market focus shifts from weather maps to WASDE reports, and why August is the most volatile WASDE month.

### 3. FSA Acreage → USDA Revision Framework
State-by-state FSA vs June estimate comparison methodology. States where FSA EXCEEDS June estimate predict direction of USDA revision. 10yr avg harvested/planted ratio converts to production impact. Timing change: USDA moved acreage revision from Oct to Sep starting 2021, making Sep WASDE more volatile.

### 4. Contra-Intuitive Price Reaction Framework
When market rallies on bearish data, fundamental low is in. When market sells on bullish data, fundamental high is in. Aug 2020: USDA raised yield (bearish) but corn rallied 1% → signaled bottom. Aug 2022: USDA raised soy yield, sharp sell → then recovery as forward-looking weather overrode backward-looking data. Price reaction is more informative than the data itself.

### 5. Peak Weather Sensitivity / Seasonal Transition
Corn peaks July (pollination), soybeans peak Aug-mid Sep (pod fill). After peak weather: weather premium removed, focus shifts to WASDE, RSI flatlines, harvest pressure builds. Northern Plains asymmetry: 25% soy but only 18% corn acreage → drought there is disproportionately bearish soybeans.

### 6. Soybean Development Critical Window
Pod Setting (80-95% by Aug 15-23) → Pod Fill (late Aug-mid Sep) → Dropping Leaves. Pod fill is THE critical period — rain can still save yields, drought causes pod abortion. Iowa 2020: 95% pods set before Aug 23, then drought → G/E fell 23 points → pod abortion = unrecoverable damage. 40% of corn dry matter accumulates post-dent, so corn not fully immune either.

## Cross-Linkages

- **Crop condition model** is the real-time version of Batch 004's WASDE yield revision patterns (historical). Together they cover pre-report positioning (condition model) through post-report analysis (revision patterns)
- **Peak weather** connects to Batch 003 ENSO framework (La Nina → drought patterns during critical windows)
- **FSA acreage** connects to Batch 003 acreage report biases (systematic corn above, soy below)
- **Contra-intuitive reactions** are the price discovery mechanism that connects to Batch 002's fund positioning (managed money) — when funds positioned in wrong direction and data surprises, the snap-back is amplified
- **Soybean pod fill** connects to Batch 004 Brazil crop framework (Brazil planting timing determines safrinha window, which cascades to US export competition during harvest)

## Architecture Note

Batches 001-005 now form a nearly complete analytical cycle:

1. **Structural frameworks** (Batch 001: canola/biofuel, Batch 003: crush-for-oil thesis, Batch 004: RD price architecture)
2. **Pre-season forecasting** (Batch 004: initial Dec forecast, acreage economics)
3. **Growing season monitoring** (Batch 005: crop conditions, weather sensitivity, development stages)
4. **Report analysis** (Batch 004: WASDE interpretation, Batch 005: yield prediction model)
5. **Harvest/post-harvest** (Batch 002: calendar spreads, basis, fund positioning)
6. **International** (Batch 003: ENSO, Chinese demand, Batch 004: Brazil crop, Chinese tracking)
7. **Policy** (Batch 001: RINs/SREs, Batch 004: RFS mandates, Batch 003: RVO)

Main gap remaining: **spring planting season** (Apr-Jun) — planting delays, prevent plant, and the June 30 acreage/stocks reports.
