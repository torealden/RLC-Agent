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
| 4 | Copy of Long-Term US ... - Slides.xlsx | Presentation version with updated projections | 2018-05 |
| 5 | US Oilseed and Fats Projections - 2018-2040.xlsx | PCAU meal demand model | 2018-05 |
| 6 | Suncor Balance Sheets.xlsx | Historical 2013-2022 (10 commodities) | 2018-04 |
| 7 | Suncor Canadian Balance Sheets.xlsx | 4 Canadian commodity BS x 3 scenarios | 2018-04 |
| 8 | Feedstock Requirement Breakout.xlsx | 13 feedstock types x 3 scenarios | 2018-04 |
| 9 | Suncor Bio and RD forecast metho.xlsx | BBD demand methodology (RFS/LCFS/CAN) | 2018-04 |
| 10 | LCFS Feedstock Mix.xlsx | Historical LCFS credits 2011-2017 | 2018-04 |
| 11 | Historical Canadian Feedstock Data.xlsx | Canadian RFS compliance 2013-2014 | 2018-04 |
| 12 | Historical Price Definitions.xlsx | Price basis definitions | 2018-07 |
| 13 | Suncor Plan.xlsx | Work assignment matrix | 2018-04 |
| 14-17 | Various dated BS versions + methodology | Supporting workbooks | 2018 |

## Extraction Summary

| Entity | Count | Description |
|--------|-------|-------------|
| **Sources** | 17 | All registered in `core.kg_source` |
| **New Nodes** | 15 | 7 analytical models, 1 company, 1 commodity, 6 data series |
| **New Edges** | 15 | Cross-market competition, consumption, causal, analysis links |
| **New Contexts** | 12 | Expert rules, forecast frameworks, methodology documentation |
| **Updated Nodes** | 6 | tallow, canola_oil, used_cooking_oil, renewable_diesel, canada_cfr, feedstock_supply_chain_model |

## Key Analytical Frameworks Extracted

1. **Plant-Level Feedstock Mix Methodology** — Bottom-up aggregation vs top-down; mix shifts over time
2. **Tallow Import Surge Requirement** — RD demand exceeds US production 3-4x by 2037; single biggest supply constraint
3. **Biodiesel Rationalization Model** — RD growth drives BD capacity exit starting 2024; IRA transition accelerates
4. **LCFS Credit Floor** — Marginal shipping cost ~$50/tonne; lipid caps could override
5. **Canadian Import Dependency** — CFR mandates force structural import dependency for tallow, UCO, and redirect canola oil
6. **SAF Growth at 1233% CAGR** — Competes with RD for feedstock at shared facilities
7. **100M Acre Soy Constraint** — Crop rotation limits expansion; wheat-to-soy switching depends on Ukraine war resolution

## Links to Existing KG Nodes

Connected to: `canola_oil`, `tallow`, `renewable_diesel`, `hefa_technology`, `feedstock_supply_chain_model`, `canada_cfr`, `lcfs_credit_framework`, `bbd_margin_model`, `used_cooking_oil`, `sustainable_aviation_fuel`, `soybeans`, `acreage_rules_of_thumb`, `bbd_balance_sheet_model`
