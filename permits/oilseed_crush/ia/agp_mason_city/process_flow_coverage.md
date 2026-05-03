# Process Flow Coverage

Cross-checking extracted emission units against the canonical oilseed-crush process flow (`domain_knowledge/process_flows/oilseed_crush.md`).

✅ = at least one extracted unit maps to this step.  
⚠️ = the step is missing from the extraction. Could be (a) the LLM missed it, (b) the permit doesn't itemize it (bundled into another), or (c) the facility doesn't have this capability. Spot-check the source PDF to determine which.

| Status | Step | Categories that satisfy it | Extracted EUs in this step |
|---|---|---|---|
| ✅ | Receiving (truck + rail) | receiving, handling, loading/unloading | EU-103 |
| ⚠️ | Cleaning | cleaning | — |
| ✅ | Drying | drying | EU-104, EU-702 |
| ✅ | Cracking & dehulling | cracking and dehulling, cracking, dehulling | EU-205 |
| ✅ | Conditioning | conditioning | EU-201, EU-202, EU-204, EU-206, EU-500, EU-505 |
| ⚠️ | Flaking | flaking | — |
| ⚠️ | Expanding (extruder, optional) | expanding, expander, extrusion | — |
| ✅ | Solvent extraction (hexane) | extraction | EU-300, EU-301 |
| ⚠️ | Desolventizer-toaster (DT) | desolventizing, desolventizer, toaster | — |
| ✅ | Meal drying & cooling | drying, cooling | EU-104, EU-702, EU-703 |
| ⚠️ | Meal grinding | grinding | — |
| ✅ | Meal storage | storage | EU-502, EU-503, EU-704 |
| ⚠️ | Meal loadout (rail/truck/barge) | loading/unloading, loadout, conveying | — |
| ⚠️ | Solvent (hexane) recovery | solvent_recovery, hexane, recovery | — |
| ⚠️ | Degumming | degumming | — |
| ⚠️ | Neutralization (caustic refining) | neutralization, refining | — |
| ⚠️ | Bleaching | bleaching | — |
| ⚠️ | Deodorizing (RBD final) | deodorizing | — |
| ✅ | Refined oil storage / loadout | storage, loading/unloading | EU-502, EU-503, EU-704 |
| ✅ | Boilers / steam utilities | boiler, boilers | EU-601, EU-602 |
| ⚠️ | Dust control (baghouse / cyclone / scrubber) | baghouse, scrubber, cyclone, aspiration | — |
| ✅ | Cooling towers | cooling | EU-703 |

**Coverage: 10/22 canonical steps**

_Note: not every facility runs every step (e.g., a crude-only crusher won't have refining steps 15-18). Use this as a checklist for spot-checking, not as a hard quality score._