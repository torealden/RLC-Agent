# KG Extraction Batch 013: Suncor Consulting Project

**Extracted:** 2026-04-16  
**Source:** `C:/Users/torem/RLC Dropbox/Tore Alden/Jacobsen/Projects/Suncor/` (21 files)  
**Engagement:** Fastmarkets/Jacobsen for Suncor Energy (2018 Phase 1 + 2022 Phase 2)  
**Scope:** 20-year North American oilseed/fats/biofuel feedstock S&D projections

## Documents Processed

| # | File | Type | Date |
|---|------|------|------|
| 1 | Suncor Long-Term Forecast Assumptions Report - First Draft.docx | Qualitative assumptions | 2022-11 |
| 2 | Suncor Long-Term Forecast - Nov 22.xlsx | US BS 2023-2035 (SBO, CO, UCO, DCO, Tallow, Credits, HRD, SAF) | 2022-11 |
| 3 | Long-Term US Oilseed and Fats Balance Sheets - 2018-2040.xlsx | 7 commodity BS x 3 scenarios | 2018-05 |
| 4 | Suncor Canadian Balance Sheets.xlsx | 4 Canadian commodity BS x 3 scenarios | 2018-06 |
| 5 | Feedstock Requirement Breakout.xlsx | 13 feedstock types x 3 scenarios | 2019-09 |
| 6 | Suncor Bio and RD forecast metho.xlsx | BBD demand methodology | 2018-04 |
| 7 | LCFS Feedstock Mix.xlsx | Historical LCFS credits 2011-2017 | 2018-04 |
| 8 | Historical Canadian Feedstock Data.xlsx | Canadian RFS compliance | 2018-06 |
| 9 | Historical Price Definitions.xlsx | Price basis definitions | 2018-07 |
| 10 | Suncor Plan.xlsx | Work assignment matrix | 2018-04 |
| 11-17 | Various dated BS versions, presentations, SOW, crusher lists | Supporting files | 2018 |

## Extraction Summary

| Entity | Count | Description |
|--------|-------|-------------|
| **Sources** | 17 | All registered in `core.kg_source` |
| **New Nodes** | 13 | 7 analytical models, 1 company, 5 data series |
| **New Edges** | 14 | Cross-market competition, supply chain links, causal, extension relationships |
| **New Contexts** | 12 | Expert rules, forecast frameworks, methodology documentation |
| **Existing Nodes Linked** | 12 | tallow, canola_oil, renewable_diesel, used_cooking_oil, soybean_oil, soybeans, sustainable_aviation_fuel, canada_cfr, lcfs_credit_framework, bbd_margin_model, feedstock_supply_chain_model, bbd_balance_sheet_model |

## Key Analytical Frameworks Extracted

1. **Plant-Level Feedstock Mix Methodology** -- Bottom-up facility aggregation vs static national average; mix varies monthly and shifts substantially over forecast
2. **Tallow Import Surge Requirement** -- RD tallow demand approaches total US production by 2028-2030; 65% displacement of non-biofuel users; structural price support
3. **Biodiesel Rationalization Model** -- RD expansion drives BD capacity exit starting 2024; survivors = large integrated crush-BD facilities
4. **LCFS Credit Pricing Model** -- Credit bank + CA-bound BBD supply model; marginal shipping cost floor ($50/tonne) disrupted by potential lipid cap
5. **SAF Growth at 1233% CAGR** -- 25M gal (2023) to 4B gal (2035); competes with RD for same lipid feedstocks at shared facilities
6. **Canadian Import Dependency** -- CFR mandates force structural feedstock import dependency; SBO exports cease entirely
7. **100M Acre Soy Constraint** -- Crop rotation limits + wheat price competition; depends on Ukraine war resolution
8. **Credit Value U-Curve** -- D4 RINs drop then recover; LCFS rises from crash to $170/tonne; BTC-to-IRA is largest single revenue stack hit
9. **Feedstock Yield Variation** -- 7.45-9.38 lbs/gal range; mix shifts create material volume calculation errors if single average used
10. **Phase 1 vs Phase 2 Evolution** -- No SAF in 2018; BD/RD split inverted; IRA not yet enacted; demonstrates need for frequent forecast revision

## Links to Existing KG Nodes

Connected to: `canola_oil`, `tallow`, `renewable_diesel`, `hefa_technology`, `feedstock_supply_chain_model`, `canada_cfr`, `lcfs_credit_framework`, `bbd_margin_model`, `used_cooking_oil`, `sustainable_aviation_fuel`, `soybeans`, `soybean_oil`, `bbd_balance_sheet_model`
