# Process Flow Coverage

Cross-checking extracted emission units against the canonical oilseed-crush process flow (`domain_knowledge/process_flows/oilseed_crush.md`).

✅ = at least one extracted unit maps to this step.  
⚠️ = the step is missing from the extraction. Could be (a) the LLM missed it, (b) the permit doesn't itemize it (bundled into another), or (c) the facility doesn't have this capability. Spot-check the source PDF to determine which.

| Status | Step | Categories that satisfy it | Extracted EUs in this step |
|---|---|---|---|
| ✅ | Receiving (truck + rail) | receiving, handling, loading/unloading | EU-11, EU-12, EU-23, EU-24 |
| ✅ | Cleaning | cleaning | EU-13 |
| ✅ | Drying | drying | EU-14, EU-18, EU-19 |
| ✅ | Cracking & dehulling | cracking and dehulling, cracking, dehulling | EU-15 |
| ⚠️ | Conditioning | conditioning | — |
| ✅ | Flaking | flaking | EU-17 |
| ⚠️ | Expanding (extruder, optional) | expanding, expander, extrusion | — |
| ⚠️ | Solvent extraction (hexane) | extraction | — |
| ⚠️ | Desolventizer-toaster (DT) | desolventizing, desolventizer, toaster | — |
| ✅ | Meal drying & cooling | drying, cooling | EU-14, EU-18, EU-19, EU-20 |
| ✅ | Meal grinding | grinding | EU-16 |
| ⚠️ | Meal storage | storage | — |
| ✅ | Meal loadout (rail/truck/barge) | loading/unloading, loadout, conveying | EU-12, EU-22, EU-23, EU-24, EU-26, EU-36 |
| ⚠️ | Solvent (hexane) recovery | solvent_recovery, hexane, recovery | — |
| ⚠️ | Degumming | degumming | — |
| ⚠️ | Neutralization (caustic refining) | neutralization, refining | — |
| ⚠️ | Bleaching | bleaching | — |
| ⚠️ | Deodorizing (RBD final) | deodorizing | — |
| ✅ | Refined oil storage / loadout | storage, loading/unloading | EU-12, EU-23, EU-24 |
| ✅ | Boilers / steam utilities | boiler, boilers | EU-5, EU-7 |
| ⚠️ | Dust control (baghouse / cyclone / scrubber) | baghouse, scrubber, cyclone, aspiration | — |
| ✅ | Cooling towers | cooling | EU-20 |

**Coverage: 11/22 canonical steps**

_Note: not every facility runs every step (e.g., a crude-only crusher won't have refining steps 15-18). Use this as a checklist for spot-checking, not as a hard quality score._