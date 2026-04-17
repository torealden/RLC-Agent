# KG Batch 012: Cresta / Braya Argentine SBO Feedstock Study

**Extracted:** 2026-04-16  
**Source:** Cresta Fund Management / Braya Renewable Fuels consulting project (Fastmarkets, May-Oct 2022)  
**Folder:** `C:/Users/torem/RLC Dropbox/Tore Alden/Jacobsen/Projects/Cresta/`

## Documents Processed: 8

| Document | Type | Key Content |
|----------|------|-------------|
| Argentine EPA Certified SBO Feedstock Availability - Final Draft - 10202022.docx | Primary study | Argentine SBO supply chain, EPA certification, crush capacity, biodiesel economics, export taxes |
| Braya - Argentine SBO Refining Study - First Draft.docx | Competitive analysis | SBO refining cost chain, RD regional margin comparison, credit price forecasting |
| Braya - Follow-Up Project - Credit Price Forecast.docx | Credit forecast | RIN/LCFS pricing framework, IRA/BTC transition analysis |
| Top 5 Crusher Profiles.docx | Reference | Renova, Cargill, Molinos Agro, Terminal 6, LDC capacity data |
| Argentine EPA Certified SBO Feedstock Study - SOW.docx | Scope | Project deliverables outline |
| Follow-Up Project Scope of Work - Sep 22.docx | Scope | Braya follow-up project scope |
| EPA-Certification Paperwork - Translated.docx | Reference | COFCO EPA certification process (Spanish -> English) |
| Misc Commentary from Initial Report.docx | Notes | RFS certification overview |

## Extraction Counts

| Entity | Count |
|--------|-------|
| **Nodes** | 16 (2 regions, 1 facility, 2 policies, 4 companies, 2 data series, 1 commodity, 1 concept, 3 models) |
| **Edges** | 15 (4 SUPPLIES/ENABLES, 3 COMPETES_WITH, 3 CAUSES, 3 EXTENDS, 1 USES, 1 RISK_FACTOR) |
| **Contexts** | 9 (all expert_rule type) |
| **Sources** | 8 documents registered |

## Key Analytical Frameworks Discovered

1. **EPA Certification Supply Tiers**: 30% enrollment = 210K MT/month, 90% potential = 630K MT/month. Supply is demand-driven, not capacity-constrained. 20-day enrollment lead time.

2. **Argentine SBO Refining Cost Chain**: Crude -> degummed (0.15-0.25 cpb) -> neutralized/RD-spec (0.25-0.40 cpb total) -> RBD (0.50-0.75 cpb total). RD feedstock does NOT require full RBD refining.

3. **Farmer Hoarding as Supply Buffer**: Ending stocks 44% of use (vs 19% avg) smooths crush volumes. 12 MMT production drop produced zero crush decline. Argentine SBO supply is crush-capacity-bounded (~42 MMT), not production-bounded.

4. **RD Revenue Stack Outbids All Alternatives**: Come-by-Chance (RD price + RINs + LCFS + BTC to CA) beat every alternative Argentine SBO end use in every month Jan 2020 - Sep 2022.

5. **IRA/BTC Feedstock Substitution Impossibility**: Shifting SBO from 50% to 20% of US BBD feedstock requires +5.5B lbs fats/greases (2021), growing to +9B lbs by 2024. Non-biofuel industries would outbid BBD for fats/greases. SBO remains irreplaceable.

6. **RINs as Margin Stabilizers**: D4 RIN pricing model = (heating oil - SBO spread) + D6 RIN + policy adjustments. RINs rise when production falls short of mandates.

## Links to Existing KG

- `soybean_oil` -- Argentine supply dynamics, demand hierarchy
- `renewable_diesel` -- EU competition with Argentine biodiesel
- `hefa_technology` -- Come-by-Chance refinery conversion
- `feedstock_supply_chain_model` -- Extended with Argentina-specific dynamics
- `crusher_feasibility_model` -- Extended with Argentine crush data
- `bbd_margin_model` -- Extended with regional comparison framework
- `rin_oversupply_model` -- Extended with IRA/BTC transition analysis
- `sunflower_oil` -- Refining capacity competition in Argentina
- `argentina_export_tax` -- Reinforced with comprehensive historical rates

## Notable Content

The COFCO EPA certification paperwork (translated from Spanish) provides detailed operational process for farmer enrollment -- satellite imagery verification against 2007 baseline, waybill segregation requirements, 20-day processing timeline. This is rare operational-level documentation of how the traceability system works in practice.

The farmer hoarding behavior framework is unique to Argentina and is the single most important insight for understanding SBO supply reliability -- production variability is NOT a reliable proxy for supply variability.
