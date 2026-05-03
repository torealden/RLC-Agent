# Process Flow Coverage

Cross-checking extracted emission units against the canonical oilseed-crush process flow (`domain_knowledge/process_flows/oilseed_crush.md`).

✅ = at least one extracted unit maps to this step.  
⚠️ = the step is missing from the extraction. Could be (a) the LLM missed it, (b) the permit doesn't itemize it (bundled into another), or (c) the facility doesn't have this capability. Spot-check the source PDF to determine which.

| Status | Step | Categories that satisfy it | Extracted EUs in this step |
|---|---|---|---|
| ✅ | Receiving (truck + rail) | receiving, handling, loading/unloading | EU-1, EU-3 |
| ⚠️ | Cleaning | cleaning | — |
| ✅ | Drying | drying | EU-4, EU-7, EU-8, EU-9 |
| ⚠️ | Cracking & dehulling | cracking and dehulling, cracking, dehulling | — |
| ✅ | Conditioning | conditioning | EU-10, EU-5, EU-6 |
| ⚠️ | Flaking | flaking | — |
| ⚠️ | Expanding (extruder, optional) | expanding, expander, extrusion | — |
| ✅ | Solvent extraction (hexane) | extraction | EP-14 |
| ⚠️ | Desolventizer-toaster (DT) | desolventizing, desolventizer, toaster | — |
| ✅ | Meal drying & cooling | drying, cooling | EU-4, EU-7, EU-8, EU-9 |
| ⚠️ | Meal grinding | grinding | — |
| ✅ | Meal storage | storage | EP-15A, EP-15B, EP-15C, EP-15D, EP-17A, EP-17B ... (+14 more) |
| ⚠️ | Meal loadout (rail/truck/barge) | loading/unloading, loadout, conveying | — |
| ⚠️ | Solvent (hexane) recovery | solvent_recovery, hexane, recovery | — |
| ⚠️ | Degumming | degumming | — |
| ⚠️ | Neutralization (caustic refining) | neutralization, refining | — |
| ⚠️ | Bleaching | bleaching | — |
| ⚠️ | Deodorizing (RBD final) | deodorizing | — |
| ✅ | Refined oil storage / loadout | storage, loading/unloading | EP-15A, EP-15B, EP-15C, EP-15D, EP-17A, EP-17B ... (+14 more) |
| ✅ | Boilers / steam utilities | boiler, boilers | EP-11 |
| ⚠️ | Dust control (baghouse / cyclone / scrubber) | baghouse, scrubber, cyclone, aspiration | — |
| ⚠️ | Cooling towers | cooling | — |

**Coverage: 8/22 canonical steps**

_Note: not every facility runs every step (e.g., a crude-only crusher won't have refining steps 15-18). Use this as a checklist for spot-checking, not as a hard quality score._