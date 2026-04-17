# KG Extraction Batch 019: Quarterly Outlook Reports (Q3 2021 - Q1 2022)

## Source
Jacobsen Fats, Fuels & Feedstock Outlook Reports, 35 .docx files across 3 quarters (Q3 2021, Q4 2021, Q1 2022). Earlier quarters (2020 Q3-Q4, 2021 Q1-Q2) contained only PDFs/PPTXs -- no .docx files available.

## Extraction Statistics
- **Nodes:** 11 (5 models, 2 data_series, 1 concept, 1 policy, 1 model/arc, 1 model/threshold)
- **Edges:** 12 (CAUSES, SUPPLIES, PREDICTS, EXTENDS, CONSTRAINS, PRECEDES)
- **Contexts:** 8 (expert_rules, market_indicators)
- **Sources:** 14 registered (covering 35 individual .docx files)

## Framework Evolution (Q3 2021 -> Q2 2022)

### Biggest single-quarter changes
- **Q3 2021:** Co-processing slashed from 50% to <3%. SAF volumes introduced (3B gal by 2030). Crush expansion assumptions slowed.
- **Q4 2021:** RFS proposed mandates released (Dec). SBO refining shortage identified as story of 2021. Biodiesel cuts deepened across all feedstocks.
- **Q1 2022:** UCO/YG production model overhauled (+1.4B lbs/yr). World tallow trade model raised imports +1B lbs/yr. Record prices across all feedstocks. Bird flu risk flagged. RFS post-expiration structure became key question.

### Key analytical patterns across all quarters
1. **Non-biofuel demand as swing variable** -- every balance sheet used non-biofuel as the residual to absorb BBD demand changes
2. **Fat/grease price stickiness** -- credit prices fell while feedstock prices rose, revealing structural shortage
3. **Crush capacity ratchet** -- each quarter raised capacity targets as more announcements materialized (Q3 2021 was the low point)
4. **UCO/YG estimation uncertainty** -- model changed significantly every quarter, highest-uncertainty element of entire outlook
5. **Price forecast baseline effects** -- near-term forecast changes dominated by current price levels, not S&D fundamentals

### Links to Batch 018 (Q2 2022)
Batch 019 shows how the Q2 2022 frameworks developed. The ethanol blend wall model (Q2 2022's key innovation) was absent in Q3-Q4 2021 -- biodiesel cuts had no offsetting demand support mechanism. The LCFS credit bank intervention trigger (Q2 2022) evolved from Q3 2021's early warning that credit generation could overwhelm deficits.

## Files
- **SQL:** `kg_extraction_batch_019_quarterly_outlooks.sql`
- **Predecessor:** `kg_extraction_batch_018_q2_2022_outlook.sql`
