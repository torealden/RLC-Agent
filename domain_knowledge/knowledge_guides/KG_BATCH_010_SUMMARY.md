# Knowledge Graph Extraction: Batch 002 Summary

**Date:** 2026-03-13  
**Sources processed:** 9 annual reports (MPOB Annual Overview of the Industry, 2016–2024)  
**Date range covered:** January 2016 — December 2024  
**Report origin:** Malaysian Palm Oil Board (MPOB)

---

## Extraction Totals

| Category | Count |
|----------|-------|
| Nodes | 61 |
| Edges | 23 |
| Contexts | 11 |
| Sources registered | 9 |

**Node breakdown by type:**

| Node Type | Count |
|-----------|-------|
| commodity | 13 |
| region | 5 |
| country | 13 |
| data_series | 13 |
| report | 1 |
| market_participant | 2 |
| policy | 7 |
| macro_driver | 5 |
| price_level | 2 |

---

## Key Analytical Framework Elements Discovered

### 1. Palm Oil Closing Stocks as Primary CPO Price Signal
The single most reliable causal relationship in the MPOB dataset is the inverse correlation between Malaysian palm oil year-end closing stocks and CPO price. Across all nine years, high stocks consistently precede price weakness and low stocks precede price strength — with minimal exceptions.

**Nine-year documentation:**
- 2018: Stocks 3.22M tonnes (record) → 2019 CPO: RM2,079/tonne (lowest in decade)
- 2020: Stocks 1.27M tonnes (lowest) → 2021 CPO: RM4,407/tonne (then-record)
- 2022: Stocks 2.20M tonnes (building) → 2023 CPO: RM3,809/tonne (correction)

**Thresholds established:** <1.5M tonnes = very bullish; 2.0–2.5M tonnes = neutral; >3.0M tonnes = very bearish.

**Confidence: Very High (consistent across all 9 years)**

---

### 2. India as the Most Policy-Volatile Export Market
India is consistently the largest Malaysian palm oil export market (since 2014), but year-to-year volume swings of 30–75% are common — driven almost entirely by policy changes, not underlying demand. Two critical precedent events documented:

- **Jan 2019 (MICECA activation):** 5% duty advantage for Malaysian RBD palm olein vs Indonesian → Indian imports surged **+75.4%** from 2.51M to 4.41M tonnes
- **Jan 2020 (India import restriction):** India restricted processed palm oil from Malaysia → Malaysian RBD palm olein exports to India collapsed **–99.7%** from 2.34M tonnes to 7,323 tonnes in a single year

This is the single largest policy risk variable for Malaysian palm oil export volumes.

**Confidence: Very High (directly documented in 2019 and 2020 reports)**

---

### 3. Indonesia Export Policy as Primary External Price Shock Variable
Indonesian CPO export taxes, levies, domestic market obligations (DMO), and export bans are the most immediate external driver of global CPO prices and Malaysian market share. Three documented modes:

- **High levy / DMO:** Diverts Indonesian supply to domestic B-mandate → bullish for global prices and Malaysian share (2021 example: India shifted heavily to Malaysia)
- **Export ban:** Acute supply shock → immediate global price spike (April–May 2022: contributed to CPO all-time monthly high RM6,873)
- **Liberalization:** Indonesian supply floods markets → bearish for Malaysian prices and share (2023: ban removal contributed to CPO –25.1% correction)

**Confidence: Very High (three distinct event types documented across 2021–2023)**

---

### 4. El Niño Production Impact Playbook
A consistent and quantifiable pattern documented across the full nine-year dataset:

- **El Niño impact year (6–12 months after onset):** FFB yield declines 10–15%. 2016: –13.9% to 15.91 t/ha → CPO production –13.2% to 17.32M tonnes
- **Recovery year:** Strong bounce-back. 2017: FFB yield +12.4% to 17.89 t/ha → CPO production +15.0% to 19.92M tonnes
- **Biological rest year risk:** After an exceptionally high yield year (2017), a biological rest effect contributes to the following year's decline (2018: –4.1% yield)

**Confidence: High (2016–2017 cycle fully documented; biological rest observed in 2018)**

---

### 5. CPO Price Driver Hierarchy
The MPOB annual forewords explicitly name the same factors as CPO price drivers across all nine years, establishing a consistent hierarchy:

1. **Domestic stock levels** — most frequently cited, most consistent signal
2. **Soybean oil price** — "CPO and SBO compete for a share in the global vegetable oils market" (verbatim framing, repeated in every annual overview)
3. **Indonesia export policy** — increasingly prominent from 2021 onward
4. **Brent crude oil** — "palm is used as feedstock to produce biodiesel" (consistent framing)
5. **MYR/USD exchange rate** — cited in high-price and competitive years

**Confidence: Very High (cited consistently in every annual foreword)**

---

### 6. CPKO/PK Lauric Oil Pricing Divergence from CPO
The CPKO/CPO price ratio varied dramatically over the nine-year period (1.24x–2.07x), confirming that CPKO follows the lauric oil complex (coconut oil + PKO) rather than CPO alone. The most extreme divergence occurred in 2018–2019 when CPO declined modestly but CPKO fell nearly 30% — driven by independent lauric oil supply dynamics.

**CPKO/CPO ratio reference points:** 2016: 2.07x | 2019: 1.26x | 2022: 1.24x | 2024: 1.31x

**Confidence: High (nine years of price data directly supports)**

---

### 7. COVID-19 Labour Shortage as a Distinct Production Shock Type
The 2021 CPO production decline (–5.4%) occurred despite adequate planted area and no significant weather disruption — driven entirely by plantation labour shortage from suspended foreign worker intake. This produced a distinctive diagnostic signature: FFB yield declined AND OER declined simultaneously, rather than the yield-only pattern of weather shocks. Useful for distinguishing labour vs weather vs biological causes in future production analysis.

**Confidence: High (explicitly attributed in MPOB 2021 annual foreword)**

---

### 8. Ukraine War as Sunflower Oil Substitution Template
The 2022 Ukraine war provided a documented template for how Black Sea sunflower oil supply shocks propagate into palm oil demand. The sequence — supply disruption → sunflower price spike → buyer substitution to palm oil → palm price rally → alternative supply development → normalization — played out over approximately 18–24 months (2022 peak, 2023 partial correction). Applicable as a playbook for future Black Sea disruption scenarios.

**Confidence: High (directly documented across 2022–2023 MPOB reports)**

---

### 9. Kenya as Emerging African Gateway Market
Kenya emerged from a minor buyer to the 4th–7th largest Malaysian palm oil export market by 2021–2024 (0.67–1.26M tonnes). Imports primarily CPO for local refining and re-export to landlocked African countries (Uganda, Rwanda, Congo, Burundi). This represents a structural East African distribution hub model, not direct Kenyan consumption growth.

**Confidence: High (documented in 2021, 2022, 2023, 2024 reports)**

---

## Framework Elements Consistent With Batch 001

Several frameworks from Batch 001 (canola oil / RIN) are reinforced or extended by this batch:

- **Palm oil competing with soybean oil** for global vegetable oil demand share — now documented from both sides (SBO influence on canola prices in Batch 001; SBO influence on CPO in Batch 002)
- **LCFS / CI score preference for low-CI feedstocks** mentioned in MPOB context of EU biodiesel demand sustainability requirements (EUDR)
- **Crude oil biofuel channel** as price driver — confirmed for CPO as it was for canola oil and D4 RINs

---

## Notable Data Points for Future Reference

| Metric | Value | Year |
|--------|-------|------|
| CPO all-time annual average high | RM5,087.50/tonne | 2022 |
| CPO all-time annual average low (in dataset) | RM2,079.00/tonne | 2019 |
| Palm oil stocks year-end high | 3.22M tonnes | 2018 |
| Palm oil stocks year-end low | 1.27M tonnes | 2020 |
| CPO production high | 19.86M tonnes | 2019 |
| CPO production low (El Niño) | 17.32M tonnes | 2016 |
| Export revenue record | RM137.89 billion | 2022 |
| India import surge | +75.4% to 4.41M tonnes | 2019 |
| India import collapse (policy) | –99.7% for RBD olein | 2020 |
| National FFB yield peak | 17.89 t/ha | 2017 |
| National FFB yield trough | 15.47 t/ha | 2021 |

---

## Notes on Source Quality

The MPOB Annual Overview reports are highly consistent in structure across all nine years — same table format, same KPIs reported, same foreword narrative structure. This makes cross-year extraction clean and reliable. The foreword narratives explicitly name causal factors for each year's performance, making it straightforward to attribute specific edge mechanisms to documented events rather than inferring them.

One duplicate Google Doc was identified for 2019 in Drive (two documents with "MPOB_Overview_of_Industry_2019" — only the canonical version matching the attached source document was registered in the source table).

---

## Next Steps

1. **Process MPOB monthly supply and demand reports** — these contain monthly granularity for production, stocks, exports, and imports that would populate time-series tables and enable seasonal pattern analysis
2. **Process MPOB statistics handbooks** (if available) for longer historical series pre-2016
3. **Reinforce edges with USDA PSD Malaysia data** — cross-validate MPOB production and stock figures with USDA published balance sheets (WASDE / PSD database)
4. **Connect to batch 001 nodes** — the `palm_oil`, `soybean_oil`, and `crude_oil` nodes from Batch 001 now have reinforcing MPOB-documented causal connections that can be referenced in graph queries spanning both batches
5. **Build time-series tables** — annual MPOB data extracted from these 9 reports should populate medallion silver/gold tables rather than the KG itself; the KG captures structural relationships, the time-series tables capture the actual data points
