# KG Batch 014 Summary: FCL + CPPIB Consulting Projects

**Extracted:** 2026-04-16
**Source:** Jacobsen/Fastmarkets consulting project files (2022)
**Documents processed:** 10 (7 FCL .docx, 2 FCL .xlsx, 1 CPPIB .docx)

## Source Projects

### FCL (Federated Co-operatives Limited)
- **Engagement:** Canola crush margin study for proposed Regina, SK crushing plant co-located with renewable diesel facility
- **Key deliverable:** 10-year crush margin forecast, canola meal S&D balance for Saskatchewan, protein meal disposition strategy
- **Date:** Jul-Nov 2022

### CPPIB (Canada Pension Plan Investment Board)
- **Engagement:** US renewable diesel margin analysis for potential investment
- **Key deliverable:** 3-scenario (base/high/low) RD margin projection through 2030
- **Date:** Nov 2022

## Extraction Statistics

| Category | Count |
|----------|-------|
| New nodes | 9 (FCL, CPPIB, Saskatchewan, canola_seed, canola_meal, DDGs, canola_crush_margin_model, rd_margin_model_cppib, protein_meal_displacement_model) |
| Updated nodes | 3 (canola_oil, soybean_meal, soybeans) |
| New edges | 12 |
| New contexts | 9 |
| Sources registered | 10 |

## Key Frameworks Extracted

1. **Canola crush margin methodology** -- oil yield 0.42, meal yield 0.58, oil share 72-87% (vs soy 30-40%)
2. **Crushing-for-oil paradigm shift** -- NA crushers shifting from meal-driven to oil-driven crushing due to RD demand
3. **Protein meal displacement model** -- #1 risk to new crush facilities; meal cannot be stored like oil
4. **RD margin model (3 scenarios)** -- Base $2.31/gal, High $3.10, Low $1.81 avg across 2023-2030
5. **RIN as margin insurance** -- RIN values must adjust to maintain production at mandate levels
6. **CI parity rejection** -- Feedstocks will NOT trade at CI parity due to RIN structure and SBO as marginal supply
7. **HOBO spread correlation** -- Highest correlation to D4 RIN values of any variable
8. **BTC-to-IRA transition risk** -- CI >50 = $0 credit; SBO (CI ~58) and canola oil (CI ~53-55) at risk
9. **Saskatchewan supply-crush balance** -- Capacity expanding from 4.6M to 10.3M tonnes against ~11M production

## Links to Existing KG

| New Entity | Linked To | Edge Type |
|-----------|-----------|-----------|
| canola_oil | renewable_diesel | SUPPLIES |
| canola_meal | soybean_meal | COMPETES_WITH |
| saskatchewan | canola_seed | SUPPLIES |
| soybean_oil | rd_margin_model | CAUSES (HOBO) |
| canada_cfr | canola_oil | CAUSES |
| protein_meal_displacement | crusher_feasibility_model | RISK_FACTOR |
| soybean_oil | feedstock_supply_chain_model | SUPPLIES (marginal) |
| saf | renewable_diesel | COMPETES_WITH |
| ddgs | canola_meal | SUBSTITUTES |
| ethanol | rd_margin_model | CAUSES (blend wall) |
