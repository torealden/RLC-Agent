# Process Flow Coverage

Cross-checking extracted emission units against the canonical oilseed-crush process flow (`domain_knowledge/process_flows/oilseed_crush.md`).

✅ = at least one extracted unit maps to this step.  
⚠️ = the step is missing from the extraction. Could be (a) the LLM missed it, (b) the permit doesn't itemize it (bundled into another), or (c) the facility doesn't have this capability. Spot-check the source PDF to determine which.

| Status | Step | Categories that satisfy it | Extracted EUs in this step |
|---|---|---|---|
| ✅ | Receiving (truck + rail) | receiving, handling, loading/unloading | EU-15.1, EU-2.1, EU-37.1, EU-37.3, EU-F2 |
| ✅ | Cleaning | cleaning | EU-8.1 |
| ✅ | Drying | drying | EU-29.1, EU-41.4 |
| ✅ | Cracking & dehulling | cracking and dehulling, cracking, dehulling | EU-11.1 |
| ✅ | Conditioning | conditioning | EU-30.1 |
| ⚠️ | Flaking | flaking | — |
| ⚠️ | Expanding (extruder, optional) | expanding, expander, extrusion | — |
| ✅ | Solvent extraction (hexane) | extraction | EU-32.1 |
| ⚠️ | Desolventizer-toaster (DT) | desolventizing, desolventizer, toaster | — |
| ✅ | Meal drying & cooling | drying, cooling | EU-29.1, EU-29.2, EU-38, EU-41.4 |
| ✅ | Meal grinding | grinding | EU-1.1, EU-1.2, EU-4.1, EU-47 |
| ✅ | Meal storage | storage | EU-27.2, EU-31.1, EU-35.1, EU-36, EU-37.2, EU-41.10 ... (+2 more) |
| ✅ | Meal loadout (rail/truck/barge) | loading/unloading, loadout, conveying | EU-11.5, EU-14.2, EU-1.5, EU-15.1, EU-15.2, EU-37.3 ... (+6 more) |
| ⚠️ | Solvent (hexane) recovery | solvent_recovery, hexane, recovery | — |
| ⚠️ | Degumming | degumming | — |
| ⚠️ | Neutralization (caustic refining) | neutralization, refining | — |
| ⚠️ | Bleaching | bleaching | — |
| ⚠️ | Deodorizing (RBD final) | deodorizing | — |
| ✅ | Refined oil storage / loadout | storage, loading/unloading | EU-15.1, EU-27.2, EU-31.1, EU-35.1, EU-36, EU-37.2 ... (+5 more) |
| ✅ | Boilers / steam utilities | boiler, boilers | EU-34.1, EU-34.21, EU-34.22, EU-34.23, EU-34.24, EU-34.25 ... (+5 more) |
| ✅ | Dust control (baghouse / cyclone / scrubber) | baghouse, scrubber, cyclone, aspiration | EU-10.1, EU-11.2, EU-11.3, EU-11.4, EU-1.3, EU-14.1 ... (+4 more) |
| ✅ | Cooling towers | cooling | EU-29.2, EU-38 |

**Coverage: 14/22 canonical steps**

_Note: not every facility runs every step (e.g., a crude-only crusher won't have refining steps 15-18). Use this as a checklist for spot-checking, not as a hard quality score._