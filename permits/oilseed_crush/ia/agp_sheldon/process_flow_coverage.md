# Process Flow Coverage

Cross-checking extracted emission units against the canonical oilseed-crush process flow (`domain_knowledge/process_flows/oilseed_crush.md`).

✅ = at least one extracted unit maps to this step.  
⚠️ = the step is missing from the extraction. Could be (a) the LLM missed it, (b) the permit doesn't itemize it (bundled into another), or (c) the facility doesn't have this capability. Spot-check the source PDF to determine which.

| Status | Step | Categories that satisfy it | Extracted EUs in this step |
|---|---|---|---|
| ✅ | Receiving (truck + rail) | receiving, handling, loading/unloading | 23, 24, 60.1 |
| ⚠️ | Cleaning | cleaning | — |
| ✅ | Drying | drying | 4, 41 |
| ✅ | Cracking & dehulling | cracking and dehulling, cracking, dehulling | 15 |
| ✅ | Conditioning | conditioning | 10, 20 |
| ⚠️ | Flaking | flaking | — |
| ⚠️ | Expanding (extruder, optional) | expanding, expander, extrusion | — |
| ⚠️ | Solvent extraction (hexane) | extraction | — |
| ⚠️ | Desolventizer-toaster (DT) | desolventizing, desolventizer, toaster | — |
| ✅ | Meal drying & cooling | drying, cooling | 4, 41 |
| ⚠️ | Meal grinding | grinding | — |
| ✅ | Meal storage | storage | 26, 27, 28, 29, 30, 32 ... (+4 more) |
| ✅ | Meal loadout (rail/truck/barge) | loading/unloading, loadout, conveying | 16, 21, 22, 23, 24, 60.1 |
| ⚠️ | Solvent (hexane) recovery | solvent_recovery, hexane, recovery | — |
| ⚠️ | Degumming | degumming | — |
| ⚠️ | Neutralization (caustic refining) | neutralization, refining | — |
| ⚠️ | Bleaching | bleaching | — |
| ⚠️ | Deodorizing (RBD final) | deodorizing | — |
| ✅ | Refined oil storage / loadout | storage, loading/unloading | 23, 24, 26, 27, 28, 29 ... (+7 more) |
| ✅ | Boilers / steam utilities | boiler, boilers | 38 |
| ✅ | Dust control (baghouse / cyclone / scrubber) | baghouse, scrubber, cyclone, aspiration | 14, 19 |
| ⚠️ | Cooling towers | cooling | — |

**Coverage: 10/22 canonical steps**

_Note: not every facility runs every step (e.g., a crude-only crusher won't have refining steps 15-18). Use this as a checklist for spot-checking, not as a hard quality score._