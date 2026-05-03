# Process Flow Coverage

Cross-checking extracted emission units against the canonical oilseed-crush process flow (`domain_knowledge/process_flows/oilseed_crush.md`).

✅ = at least one extracted unit maps to this step.  
⚠️ = the step is missing from the extraction. Could be (a) the LLM missed it, (b) the permit doesn't itemize it (bundled into another), or (c) the facility doesn't have this capability. Spot-check the source PDF to determine which.

| Status | Step | Categories that satisfy it | Extracted EUs in this step |
|---|---|---|---|
| ✅ | Receiving (truck + rail) | receiving, handling, loading/unloading | 37, 40, 42, 45, 52, 54, 55, 56, 65-66, 74, 75-76, 83, 9.1, 9.2 |
| ⚠️ | Cleaning | cleaning | — |
| ✅ | Drying | drying | 14.1-14.3 |
| ✅ | Cracking & dehulling | cracking and dehulling, cracking, dehulling | 5.1, 5.2, 6.1 |
| ⚠️ | Conditioning | conditioning | — |
| ⚠️ | Flaking | flaking | — |
| ⚠️ | Expanding (extruder, optional) | expanding, expander, extrusion | — |
| ⚠️ | Solvent extraction (hexane) | extraction | — |
| ⚠️ | Desolventizer-toaster (DT) | desolventizing, desolventizer, toaster | — |
| ✅ | Meal drying & cooling | drying, cooling | 14.1-14.3 |
| ✅ | Meal grinding | grinding | 6.2 |
| ⚠️ | Meal storage | storage | — |
| ✅ | Meal loadout (rail/truck/barge) | loading/unloading, loadout, conveying | 37, 40, 42, 45, 52, 54, 55, 56, 65-66, 74, 75-76, 83, 9.1, 9.2 |
| ⚠️ | Solvent (hexane) recovery | solvent_recovery, hexane, recovery | — |
| ⚠️ | Degumming | degumming | — |
| ⚠️ | Neutralization (caustic refining) | neutralization, refining | — |
| ⚠️ | Bleaching | bleaching | — |
| ⚠️ | Deodorizing (RBD final) | deodorizing | — |
| ✅ | Refined oil storage / loadout | storage, loading/unloading | 37, 40, 42, 45, 52, 54, 55, 56, 65-66, 74, 75-76, 83, 9.1, 9.2 |
| ✅ | Boilers / steam utilities | boiler, boilers | 1.1, 1.2, 1.3 |
| ⚠️ | Dust control (baghouse / cyclone / scrubber) | baghouse, scrubber, cyclone, aspiration | — |
| ⚠️ | Cooling towers | cooling | — |

**Coverage: 8/22 canonical steps**

_Note: not every facility runs every step (e.g., a crude-only crusher won't have refining steps 15-18). Use this as a checklist for spot-checking, not as a hard quality score._