# Knowledge Graph Extraction: Batch 011 Summary (MPOB 2025 incremental)

**Date:** 2026-04-16  
**Source processed:** 1 annual report (MPOB Annual Overview of the Malaysian Oil Palm Industry, 2025)  
**Date range covered:** January 2025 - December 2025  
**Report origin:** Malaysian Palm Oil Board (MPOB)  
**Source file:** G:/My Drive/google_docs_to_add/MPOB_Overview_of_Industry_2025.docx  
**Source key registered:** docx_mpob_2025

---

## Extraction Totals

| Category | New | Reinforced | Total touches |
|----------|-----|------------|---------------|
| Nodes    | 1   | 20         | 21 |
| Edges    | 3   | 10         | 13 |
| Contexts | 4   | 9          | 13 |
| Sources  | 1   | -          | 1 |

**New nodes (1):**
- indonesia.b50_mandate_2026 (policy) - announced 2025 for 2026 implementation

**Reinforced nodes (20):**
- 13 data_series updated via ON CONFLICT: mpob.cpo_production, mpob.planted_area, mpob.ffb_yield, mpob.oer, mpob.palm_oil_stocks, mpob.palm_oil_exports, mpob.palm_oil_imports, mpob.cpo_price, mpob.pk_price, mpob.cpko_price, mpob.pfad_price, mpob.rbd_olein_price, mpob.export_revenue
- 2 price_level history extended: cpo.historical_range_2016_2024, malaysia.palm_oil_stocks.history_2016_2024
- 7 region/country/policy updated via UPDATE: india, kenya, eu, turkey, philippines, japan, indonesia.biodiesel_mandate

**New edges (3):**
- indonesia.b50_mandate_2026 --CAUSES--> mpob.cpo_price
- indonesia.b50_mandate_2026 --SUPPLIES--> indonesia.biodiesel_mandate
- kenya --CONSUMES--> mpob.palm_oil_exports

**New contexts (4):**
- stocks_price_override_2025 on mpob.cpo_price (historical_analog)
- indonesia_b50_implementation_framework on indonesia.b50_mandate_2026 (expert_rule)
- kenya_east_africa_hub_framework on kenya (expert_rule)
- imports_despite_domestic_surplus_2025 on mpob.palm_oil_imports (expert_rule)

---

## Notable 2025-Only Findings

### 1. Stocks-Price Inverse Rule First Override (Material)
2025 is the first year in the 10-year MPOB dataset where rising closing stocks and rising CPO annual-average price coincided. Closing stocks 3.05M tonnes (+78.6%, 2nd-highest on record) yet CPO annual average RM4,292.50 (+2.7%). MPOB attributes the override to three named factors: Indonesia B50 mandate (2026) anticipation, firmer global soybean oil prices, and lauric complex tightness. The override is captured as a historical_analog context on mpob.cpo_price. The original inverse edge mpob.palm_oil_stocks --CAUSES--> mpob.cpo_price is preserved as default (not downgraded) but annotated with reinforced_by_2025_override evidence and a stated override mechanism: forward policy expectations dominate the stock signal when a significant Indonesian mandate step is 6-12 months away.

### 2. CPO Production Breaks 20M Tonne Ceiling
20.28M tonnes (+4.9%) -- first time above 20M in the dataset, beating the prior 2019 peak of 19.86M. Structural production ceiling assumed in batch_010 has been revised upward. Drivers: FFB yield +6.4%, OER +0.4%, planted area +1.6%.

### 3. Planted Area Trend Reversal
5.70M ha (+1.6%) -- first expansion since the 2019 peak (5.90M ha). MPOB attributes to immature palm expansion from accelerated replanting. All three regions grew. The 2019-2024 structural area decline narrative is not abandoned but qualified: 2024 appears to have been a cyclical low rather than a structural endpoint.

### 4. Indonesia B50 Mandate - New Causal Node
Indonesian B50 biodiesel mandate (announced 2025, target implementation 2026) is added as a new policy node. Two new edges link it to CPO price (CAUSES) and to the existing indonesia.biodiesel_mandate node (SUPPLIES as successor). Estimated incremental Indonesian CPO absorption at full implementation: 1.5-2.5M tonnes vs B40.

### 5. Kenya Becomes #2 Export Market (Structural)
Kenya 1.21M tonnes (7.9% share) -- surpassed both EU and China for the first time. Confirmed as an East African refining/re-export hub (Mombasa refining, re-export to Uganda/Rwanda/Burundi/Congo). New kenya --CONSUMES--> mpob.palm_oil_exports edge and kenya_east_africa_hub_framework expert-rule context capture the structural rerank.

### 6. CPKO New 10-Year High, Sharpest Lauric Divergence
CPKO annual average RM7,329.50 (+33.9%) -- new 10-year high, beating the 2022 peak of RM6,327. CPKO/CPO ratio 1.71x -- the sharpest lauric premium in the dataset on a ratio basis (vs 1.31x in 2024). Lauric pricing framework context updated with the ratio threshold rule (above 1.7x = lauric tightness dominant).

### 7. Imports Tripled Despite Record Production (New Diagnostic)
Imports 0.76M tonnes (+3x YoY) even as domestic production reached record 20.28M. Captured in the new imports_despite_domestic_surplus_2025 context. Prior mental model (imports surge ONLY on domestic shortfall) is refined: Malaysian refining sector is now understood as both a shortage-driven AND a price/logistics-driven importer.

### 8. Biodiesel Export Surge
Biodiesel exports +38.4% volume to 353,780 tonnes with revenue +48.1% to RM1.86bn. Malaysian biofuel export ramp is a secondary but notable 2025 theme; no new node created because palm_biodiesel_my already exists.

### 9. Sabah OER Decline (Flag for 2026)
First Sabah OER decline in the dataset (-1.1% to 20.31%). Sabah is historically the highest-OER region; this is not yet warning-level but the OER context is annotated with a 2026 monitoring flag.

---

## Ambiguities / User Decisions Required

1. **Source key choice**: I used docx_mpob_2025 with source_type = local_file per instructions. A corresponding .gdoc file exists at the same folder (MPOB_Overview_of_Industry_2025.gdoc) but no Google Drive ID was supplied. If the user has access to the gdoc ID, a future re-register with gdoc_<id> source_key and source_type = gdrive_doc would align with batch_010 pattern.

2. **Node label renames not applied**: The price_level history nodes retain keys cpo.historical_range_2016_2024 and malaysia.palm_oil_stocks.history_2016_2024 which are now semantically misleading (the data extends to 2025). The labels have been updated to include 2025, but the keys are left stable to preserve referential integrity. A future housekeeping step could rename the keys if acceptable.

3. **Weight adjustments**: Three edges had weights bumped/set (ffb_yield->cpo_production to 0.98, imports->stocks to 0.80, indonesia.biodiesel_mandate->cpo to 0.90). These are modest adjustments consistent with 2025 reinforcement; user may wish to review vs the broader batch_010 weighting scheme.

4. **Indonesian B40 timing**: Batch_010 described B40 as "announced in 2025." Batch_011 treats 2025 as the year B40 is in effect (with B50 targeted for 2026). If the user has more precise monthly implementation timing, the mandate_progression jsonb on indonesia.biodiesel_mandate can be refined.

5. **Biodiesel sub-segment edge**: Malaysia biodiesel export surge (+38.4%) is noted in revenue node property only. If the user wants a dedicated edge like malaysia.biodiesel_mandate --CAUSES--> palm_biodiesel_my exports, a future micro-batch could add it.

---

## Framework Elements Consistent With Batch 010

- Strong FFB yield -> CPO production linkage reinforced (2025: yield +6.4% drove production +4.9%).
- Soybean oil competitive link reinforced (MPOB explicitly cited SBO as primary support).
- Indonesia biodiesel mandate as structural bullish floor reinforced and escalated to B50 successor.
- Kenya East African hub thesis upgraded from emerging (batch_010) to structural #2 market (batch_011).
- Lauric oil complex divergence pattern (2.07x 2016, 1.24-1.31x 2019-2024) now extended with 1.71x 2025 -- widest divergence since 2016 on a ratio basis.

---

## Execution Notes

- File to execute: domain_knowledge/knowledge_guides/kg_extraction_batch_011_mpob_2025.sql
- Prerequisite: batch_010 must have been executed (all referenced node_keys come from batch_010).
- All UPDATE statements use JSONB || (right-operand-wins) merge -- existing reinforcement notes from earlier years are preserved under their own keys.
- Review recommended before execution. Do NOT execute if batch_010 has not yet been applied to the target DB.
