# Knowledge Graph Extraction: Batch 009 (HOBO Study) — Comprehensive Summary

**Date:** 2026-02-14  
**Source:** HOBO Renewable Fuels Landscape and Feedstock Availability Study  
**Client:** HOBO Renewable Diesel LLC  
**Prepared by:** Tore Alden / FastMarkets  
**Sections Processed:** 7 of 9 (Sections 3 and 5 still too large for API)

---

## Extraction Totals

| Sub-Batch | Section | Nodes | Edges | Contexts |
|-----------|---------|-------|-------|----------|
| 009a | Executive Summary | 18 | 14 | 10 |
| 009b | Section 1: RD/SAF 101 | 8 | 6 | 6 |
| 009c | Section 2: Policy & Regulatory | 12 | 10 | 8 |
| 009d | Section 4: Project Overview | 4 | 4 | 3 |
| 009e | Section 6: SWOT | 4 | 6 | 6 |
| 009f | Section 7: Economic Viability | 8 | 6 | 8 |
| 009g | Section 8: Price & Margin Outlook | 10 | 8 | 10 |
| **TOTAL** | **7 sections** | **64** | **54** | **51** |

## Running Totals (Batches 001-009)

| Category | Prior (001-008) | Batch 009 | Running Total |
|----------|-----------------|-----------|---------------|
| Nodes | 122 | 64 | 186 |
| Edges | 75 | 54 | 129 |
| Contexts | 46 | 51 | 97 |
| Sources | 62 | 7 | 69 |

---

## Sections Not Yet Processed

| Section | Title (Inferred) | Status | Action Needed |
|---------|-----------------|--------|---------------|
| Section 3 | RD, SAF and Feedstock Demand & Supply Fundamentals | Too large for API | Split into sub-sections |
| Section 5 | (Likely Feedstock Strategy/Availability) | Too large for API | Split into sub-sections |

**Note:** Section 2's Google Doc file ends with the heading "Section 3 – RD, SAF and Feedstock Demand & Supply Funda..." suggesting Section 3 covers the supply/demand fundamentals. Section 5 is likely the detailed feedstock availability/strategy section referenced repeatedly as THE most critical section. These two missing sections probably contain the core quantitative feedstock data.

---

## Top 10 Critical Insights from HOBO Study

### 1. SAF is Economically Impossible Without Credits (CRITICAL)
SAF costs ~$8.00/gal to produce. Airlines pay ~$4.00. The $4.00 gap MUST come from stacked credits (RINs + LCFS + 45Z). All SAF routes remain more expensive than fossil jet through 2050 per IEA/McKinsey. Policy support is not a boost — it is existentially required.

### 2. Feedstock Cost = 70-80% of HEFA Economics
$0.05/lb feedstock change = $0.35-0.40/gal margin change. Every cent/lb saved = ~$0.08/gal margin. HOBO Midwest location provides structural cost advantage vs coastal competitors sourcing from 500-1500 miles away.

### 3. HOBO CI Advantage is Worth $40-60M+/Year
HOBO targets CI in low 20s vs industry upper 30s (driven by 75% H2 recycling from off-gases). Each 5 CI points = ~$0.15/gal = $20-30M/year. 15-point advantage = $40-60M/year in additional credit revenue vs average competitor.

### 4. Product Flexibility is the Embedded Option
SAF economics flipped in 2023 (from $0.60-1.00/gal penalty to $288/ton premium vs RD) when SAF-specific credits appeared. Plants locked into one product miss these swings. HOBO's RD/SAF swing capability is an embedded option worth tens of millions annually.

### 5. Feedstock Crunch Coming by 2027
IEA warns global demand for fats/oils jumping ~56% to 174B lbs. US already importing 10x more feedstock than 2020. China may shift from UCO exporter to competitor. 45Z domestic feedstock restriction would further tighten supply. HOBO's 250-mile catchment (6x coverage) is critical hedge.

### 6. Industry Margin Collapse is Real Risk
Neste margins fell 70% in one year (Q4 2023→Q4 2024). DGD margins dropped from $0.95 to $0.60/gal. HOBO worst case = near-zero margin in Midwest. Must be prepared for lean periods with working capital reserves and hedging.

### 7. Credit Stacking Creates Market Optimization Opportunity
Federal (RFS + 45Z) + State (IL $1.50/gal SAF, CA LCFS) + International (Canada CFR, EU ReFuelEU) credits stack. Optimal routing per gallon varies daily. A dedicated trading desk doing geographic/product/credit optimization can add millions at zero capex.

### 8. EU Will Be Global SAF Price Setter
ReFuelEU mandates with severe penalties + domestic supply shortage = EU will attract global SAF supply at premium prices. This creates export opportunity for US HEFA producers including HOBO (via NYH logistics).

### 9. 45Z Scenarios Reshape Everything
Extension to 2031 + ILUC removal + domestic feedstock restriction = extremely favorable for HOBO. Expiry at 2027 = cliff for SAF. ILUC removal alone would reduce waste-vs-crop CI differentiation, changing the entire feedstock strategy calculus.

### 10. HOBO Name = The Analytical Framework
The company literally named itself after the Heating Oil-Bean Oil spread — the same HOBO spread identified in Batch 001 (from 2019 RIN forecast reports) as the "master lead indicator" for D4 RIN direction and biodiesel production economics. Full circle.

---

## Cross-References to Prior Batches

The HOBO study connects to virtually every prior extraction:

- **Batch 001:** HOBO spread as master lead indicator, D4 RIN framework, biofuel feedstock demand
- **Batch 002:** Crush-for-oil thesis (new soy crush plants = biofuel demand signal)
- **Batch 003:** ENSO weather impact on feedstock (oilseed production risk)
- **Batch 004:** Chinese demand framework (China as UCO supplier/competitor)
- **Batch 008:** BBD balance sheets, feedstock supply chain, credit market analysis
- **Batch 009 (prior):** RFS nesting deep-dive (Bob's analysis), RD industry lifecycle model, feedstock mix shift

---

## Files Produced

1. `kg_extraction_batch_009a_exec_summary.sql` — Executive Summary extraction
2. `kg_extraction_batch_009b_section1.sql` — Section 1: RD/SAF 101
3. `kg_extraction_batch_009c_section2.sql` — Section 2: Policy & Regulatory
4. `kg_extraction_batch_009d_section4.sql` — Section 4: Project Overview
5. `kg_extraction_batch_009e_section6.sql` — Section 6: SWOT
6. `kg_extraction_batch_009f_section7.sql` — Section 7: Economic Viability
7. `kg_extraction_batch_009g_section8.sql` — Section 8: Price Projections & Margin Outlook

---

## Next Steps

1. **Split Section 3** (Demand & Supply Fundamentals) into sub-sections and re-upload
2. **Split Section 5** (likely Feedstock Strategy) into sub-sections and re-upload — this is probably the most data-rich section
3. **Process remaining Motiva project documents** for companion extraction
4. **Run SQL inserts** against PostgreSQL KG infrastructure
5. **Deduplicate** nodes/edges against existing graph from batches 001-008
