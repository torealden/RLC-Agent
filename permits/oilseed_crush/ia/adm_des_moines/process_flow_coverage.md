# Process Flow Coverage

Cross-checking extracted emission units against the canonical oilseed-crush process flow (`domain_knowledge/process_flows/oilseed_crush.md`).

✅ = at least one extracted unit maps to this step.  
⚠️ = the step is missing from the extraction. Could be (a) the LLM missed it, (b) the permit doesn't itemize it (bundled into another), or (c) the facility doesn't have this capability. Spot-check the source PDF to determine which.

| Status | Step | Categories that satisfy it | Extracted EUs in this step |
|---|---|---|---|
| ✅ | Receiving (truck + rail) | receiving, handling, loading/unloading | C01, C04, C06B, MP010, U03 |
| ⚠️ | Cleaning | cleaning | — |
| ⚠️ | Drying | drying | — |
| ⚠️ | Cracking & dehulling | cracking and dehulling, cracking, dehulling | — |
| ✅ | Conditioning | conditioning | GP05A |
| ⚠️ | Flaking | flaking | — |
| ⚠️ | Expanding (extruder, optional) | expanding, expander, extrusion | — |
| ✅ | Solvent extraction (hexane) | extraction | GP09A |
| ⚠️ | Desolventizer-toaster (DT) | desolventizing, desolventizer, toaster | — |
| ⚠️ | Meal drying & cooling | drying, cooling | — |
| ⚠️ | Meal grinding | grinding | — |
| ✅ | Meal storage | storage | C02, C07, GP02, R01, R02, R03 ... (+2 more) |
| ✅ | Meal loadout (rail/truck/barge) | loading/unloading, loadout, conveying | C03, C06A, C06B, GP04A, GP07, MP010 |
| ⚠️ | Solvent (hexane) recovery | solvent_recovery, hexane, recovery | — |
| ⚠️ | Degumming | degumming | — |
| ⚠️ | Neutralization (caustic refining) | neutralization, refining | — |
| ⚠️ | Bleaching | bleaching | — |
| ⚠️ | Deodorizing (RBD final) | deodorizing | — |
| ✅ | Refined oil storage / loadout | storage, loading/unloading | C02, C06B, C07, GP02, MP010, R01 ... (+4 more) |
| ✅ | Boilers / steam utilities | boiler, boilers | C013, C05, C09, R07 |
| ✅ | Dust control (baghouse / cyclone / scrubber) | baghouse, scrubber, cyclone, aspiration | GP06A |
| ⚠️ | Cooling towers | cooling | — |

**Coverage: 8/22 canonical steps**

_Note: not every facility runs every step (e.g., a crude-only crusher won't have refining steps 15-18). Use this as a checklist for spot-checking, not as a hard quality score._