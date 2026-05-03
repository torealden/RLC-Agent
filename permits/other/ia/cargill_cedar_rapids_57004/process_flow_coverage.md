# Process Flow Coverage

Cross-checking extracted emission units against the canonical oilseed-crush process flow (`domain_knowledge/process_flows/oilseed_crush.md`).

✅ = at least one extracted unit maps to this step.  
⚠️ = the step is missing from the extraction. Could be (a) the LLM missed it, (b) the permit doesn't itemize it (bundled into another), or (c) the facility doesn't have this capability. Spot-check the source PDF to determine which.

| Status | Step | Categories that satisfy it | Extracted EUs in this step |
|---|---|---|---|
| ⚠️ | Receiving (truck + rail) | receiving, handling, loading/unloading | — |
| ⚠️ | Cleaning | cleaning | — |
| ⚠️ | Drying | drying | — |
| ⚠️ | Cracking & dehulling | cracking and dehulling, cracking, dehulling | — |
| ⚠️ | Conditioning | conditioning | — |
| ⚠️ | Flaking | flaking | — |
| ⚠️ | Expanding (extruder, optional) | expanding, expander, extrusion | — |
| ⚠️ | Solvent extraction (hexane) | extraction | — |
| ⚠️ | Desolventizer-toaster (DT) | desolventizing, desolventizer, toaster | — |
| ⚠️ | Meal drying & cooling | drying, cooling | — |
| ⚠️ | Meal grinding | grinding | — |
| ⚠️ | Meal storage | storage | — |
| ⚠️ | Meal loadout (rail/truck/barge) | loading/unloading, loadout, conveying | — |
| ⚠️ | Solvent (hexane) recovery | solvent_recovery, hexane, recovery | — |
| ⚠️ | Degumming | degumming | — |
| ⚠️ | Neutralization (caustic refining) | neutralization, refining | — |
| ⚠️ | Bleaching | bleaching | — |
| ⚠️ | Deodorizing (RBD final) | deodorizing | — |
| ⚠️ | Refined oil storage / loadout | storage, loading/unloading | — |
| ⚠️ | Boilers / steam utilities | boiler, boilers | — |
| ✅ | Dust control (baghouse / cyclone / scrubber) | baghouse, scrubber, cyclone, aspiration | EU-13A, EU-15, EU-20C, EU-27, EU-40, EU-41 ... (+9 more) |
| ⚠️ | Cooling towers | cooling | — |

**Coverage: 1/22 canonical steps**

_Note: not every facility runs every step (e.g., a crude-only crusher won't have refining steps 15-18). Use this as a checklist for spot-checking, not as a hard quality score._