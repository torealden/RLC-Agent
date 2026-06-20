# Process Flow Coverage

Cross-checking extracted emission units against the canonical oilseed-crush process flow (`domain_knowledge/process_flows/oilseed_crush.md`).

✅ = at least one extracted unit maps to this step.  
⚠️ = the step is missing from the extraction. Could be (a) the LLM missed it, (b) the permit doesn't itemize it (bundled into another), or (c) the facility doesn't have this capability. Spot-check the source PDF to determine which.

| Status | Step | Categories that satisfy it | Extracted EUs in this step |
|---|---|---|---|
| ✅ | Receiving (truck + rail) | receiving, handling, loading/unloading | EU-111 |
| ⚠️ | Cleaning | cleaning | — |
| ✅ | Drying | drying | EU-104, EU-105, EU-107c, EU-107d |
| ⚠️ | Cracking & dehulling | cracking and dehulling, cracking, dehulling | — |
| ✅ | Conditioning | conditioning | EU-107b, EU-109a, EU-109b |
| ✅ | Flaking | flaking | EU-110 |
| ⚠️ | Expanding (extruder, optional) | expanding, expander, extrusion | — |
| ✅ | Solvent extraction (hexane) | extraction | EU-102 |
| ⚠️ | Desolventizer-toaster (DT) | desolventizing, desolventizer, toaster | — |
| ✅ | Meal drying & cooling | drying, cooling | EU-104, EU-105, EU-106, EU-107c, EU-107d, EU-107e ... (+1 more) |
| ✅ | Meal grinding | grinding | EU-108 |
| ✅ | Meal storage | storage | EU-112, EU-114, EU-115, EU-117 |
| ✅ | Meal loadout (rail/truck/barge) | loading/unloading, loadout, conveying | EU-111 |
| ⚠️ | Solvent (hexane) recovery | solvent_recovery, hexane, recovery | — |
| ⚠️ | Degumming | degumming | — |
| ⚠️ | Neutralization (caustic refining) | neutralization, refining | — |
| ⚠️ | Bleaching | bleaching | — |
| ⚠️ | Deodorizing (RBD final) | deodorizing | — |
| ✅ | Refined oil storage / loadout | storage, loading/unloading | EU-111, EU-112, EU-114, EU-115, EU-117 |
| ✅ | Boilers / steam utilities | boiler, boilers | EU-101, EU-101A |
| ⚠️ | Dust control (baghouse / cyclone / scrubber) | baghouse, scrubber, cyclone, aspiration | — |
| ✅ | Cooling towers | cooling | EU-106, EU-107e, EU-119 |

**Coverage: 12/22 canonical steps**

_Note: not every facility runs every step (e.g., a crude-only crusher won't have refining steps 15-18). Use this as a checklist for spot-checking, not as a hard quality score._