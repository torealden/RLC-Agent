# Process Flow Coverage

Cross-checking extracted emission units against the canonical oilseed-crush process flow (`domain_knowledge/process_flows/oilseed_crush.md`).

✅ = at least one extracted unit maps to this step.  
⚠️ = the step is missing from the extraction. Could be (a) the LLM missed it, (b) the permit doesn't itemize it (bundled into another), or (c) the facility doesn't have this capability. Spot-check the source PDF to determine which.

| Status | Step | Categories that satisfy it | Extracted EUs in this step |
|---|---|---|---|
| ✅ | Receiving (truck + rail) | receiving, handling, loading/unloading | EU-080000, EU-130000, EU-220000 |
| ⚠️ | Cleaning | cleaning | — |
| ⚠️ | Drying | drying | — |
| ⚠️ | Cracking & dehulling | cracking and dehulling, cracking, dehulling | — |
| ⚠️ | Conditioning | conditioning | — |
| ⚠️ | Flaking | flaking | — |
| ⚠️ | Expanding (extruder, optional) | expanding, expander, extrusion | — |
| ⚠️ | Solvent extraction (hexane) | extraction | — |
| ⚠️ | Desolventizer-toaster (DT) | desolventizing, desolventizer, toaster | — |
| ⚠️ | Meal drying & cooling | drying, cooling | — |
| ✅ | Meal grinding | grinding | EU-310200 |
| ✅ | Meal storage | storage | EU-160000, EU-160500, EU-420000, EU-50300, EU-50400 |
| ✅ | Meal loadout (rail/truck/barge) | loading/unloading, loadout, conveying | EU-010000, EU-020000, EU-030000, EU-040000, EU-050000, EU-060000, EU-070000, EU-080000, EU-090000, EU-100000, EU-110000, EU-120000, EU-010100, EU-020000, EU-020300, EU-020400, EU-010100, EU-010300, EU-020500, EU-040000 ... (+11 more) |
| ⚠️ | Solvent (hexane) recovery | solvent_recovery, hexane, recovery | — |
| ⚠️ | Degumming | degumming | — |
| ⚠️ | Neutralization (caustic refining) | neutralization, refining | — |
| ⚠️ | Bleaching | bleaching | — |
| ⚠️ | Deodorizing (RBD final) | deodorizing | — |
| ✅ | Refined oil storage / loadout | storage, loading/unloading | EU-080000, EU-130000, EU-160000, EU-160500, EU-420000, EU-220000, EU-50300, EU-50400 |
| ⚠️ | Boilers / steam utilities | boiler, boilers | — |
| ✅ | Dust control (baghouse / cyclone / scrubber) | baghouse, scrubber, cyclone, aspiration | EU-120100, EU-130100, EU-150100, EU-160100 |
| ⚠️ | Cooling towers | cooling | — |

**Coverage: 6/22 canonical steps**

_Note: not every facility runs every step (e.g., a crude-only crusher won't have refining steps 15-18). Use this as a checklist for spot-checking, not as a hard quality score._