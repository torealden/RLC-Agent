# KG Extraction Batch 016: Jacobsen Client Projects

**Date:** 2026-04-16
**Source:** `C:/Users/torem/RLC Dropbox/Tore Alden/Jacobsen/Projects/` (all subfolders except Cresta, Suncor, FCL, CPPIB, Fastmarkets -- already processed in batches 012-014)
**SQL:** `kg_extraction_batch_016_jacobsen_clients.sql`

## Extraction Statistics

| Metric | Count |
|--------|-------|
| Nodes created/updated | 14 |
| Edges created | 10 |
| Contexts created | 10 |
| Sources registered | 23 |
| Client folders processed | 14 |

## Client Folder Classification

### Extractable Analytical Content (8 folders, 10 documents)

| Client | Key Document | Content |
|--------|-------------|---------|
| **Partners Group / PBF** | PBF Feedstock Availability Study - Final Draft | Full feedstock study for Chalmette RD plant. 650-mile radius analysis, SBO/DCO/tallow/CWG/UCO availability, demand elasticity hierarchy, barge logistics, Argentine SBO import risks, BD rationalization, hedging limitations |
| **ICF / TEI** | Oleo-X Technical Diligence (TimeRenewable_Feedstock_ICF) | FOG pretreatment plant feasibility. Price/margin forecasts 2024-2026, feedstock availability 58-60B lbs, tolling vs spot economics, HPAI disease risk, BP offtake review |
| **Refining Capacity Update** | Refining Capacity Report (May 2021) | SBO refining capacity shortage model. US crush 2.2B bu, biofuel >50% of SBO demand threshold crossing, crush expansion announcements, canola West Coast potential |
| **AGP** | Report First Draft + Deliverables + Outline | Vegetable oil refining capacity impact, crude vs refined SBO balance sheets through 2027/28, refining margin cycle analysis |
| **Marathon** | SOW (Tore mark up) | Feedstock pricing staircase model -- each 10-20 MBPD incremental demand creates new price equilibrium. Global FOG supply quantification, LCFS vs RIN dominance threshold |
| **EcoEngineers** | Canadian Biofuel Feedstocks Outline | Canada CFS impact on canola/tallow/UCO/CWG. Cross-border trade flow analysis. Advanced feedstocks (camelina) |
| **Unilever** | Liquid Oils Market Intelligence Study + UCO Brief | CPG procurement intelligence: US/EU crush margins, physical SBO/canola pricing, non-GMO premiums, UCO global market research |
| **Expert Interviews** | Biofuels Expert Interview Questions (July 2022) | Trade route shifts, CI scoring hierarchy, canola RIN pathway status, 2022 crop forecasts |

### SOW/Proposal Only -- Registered but Limited Extraction (5 folders)

| Client | Document | Notes |
|--------|----------|-------|
| **Zenith Energy** | Joliet Renewables Proposal - Final | SOW for 300-mile radius study. Feedstock list, deliverables, $100K fee. Facility node created. |
| **Warburg-Pincus** | Montana Renewables SOW | 750-mile radius study scope. Facility node created. |
| **BHP** | Scope of Work | RD pricing/historical time series. Brief SOW only. |
| **Multi-Client** | Sell Sheet (root level) | 20-year feedstock outlook framework (11 feedstocks, 3 scenarios, $35K). Model node created. |
| **Generic SOW** | Scope of Work - Feedstock Availability (root level) | Template for 300-mile radius studies |

### Legal/Contracts/Internal -- Skipped (3 items)

| Source | Type | Notes |
|--------|------|-------|
| Partners Group NDA | Legal | NDA only |
| Andrew Li / ECTP | Charts + NDA | SBO Days of Cover charts (no text), plus NDA/retainer |
| AI-ML | Internal meeting | Agenda for AI Sentiment Product Meeting |

### Empty Folders -- No .docx Files (3 folders)

McKinsey, Shell, Stepan -- no .docx files found. May contain only .xlsx, .pptx, or other formats.

## Key Nodes Created

### Facilities (4)
- `pbf_chalmette_rd` -- PBF Chalmette RD Plant (Gulf Coast, Partners Group investor)
- `oleox_pascagoula` -- Oleo-X FOG Pretreatment (Pascagoula, TEI/BP offtake)
- `zenith_joliet_terminal` -- Zenith Joliet Renewables Terminal (IL)
- `montana_renewables` -- Montana Renewables (Great Falls, Warburg-Pincus)

### Models (5)
- `feedstock_radius_analysis_model` -- Consulting methodology used across 6+ engagements
- `feedstock_demand_elasticity_model` -- Oleochemical > food CPG > RD > BD > bottling > food service
- `feedstock_pricing_staircase_model` -- Marathon: 10-20 MBPD incremental pricing
- `sbo_refining_capacity_shortage_model` -- Refining margin spike/normalization cycle
- `feedstock_hedging_limitation_model` -- Structural hedging limitations for FOGs

### Concepts (5)
- `food_vs_fuel_policy_risk` -- Risk framework: ethanol precedent, CA vs EU impact
- `canada_cfs_feedstock_impact` -- CFS cross-border trade flow analysis
- `multiclient_20yr_feedstock_outlook` -- 20-year 3-scenario framework
- `rd_price_projection_methodology` -- ULSD + spread + credits methodology
- `biofuel_trade_route_shifts_2022` -- Palm-SBO substitution, Argentine opportunities
- `liquid_oils_market_intelligence` -- CPG procurement framework
- `uco_global_market_research_framework` -- UCO production/collection drivers

## Links to Existing KG Nodes

Edges connect to: `diamond_green_diesel`, `soybean_oil`, `renewable_diesel`, `feedstock_supply_chain_model`, `canola_oil`, `tallow`, `bbd_balance_sheet_model`
