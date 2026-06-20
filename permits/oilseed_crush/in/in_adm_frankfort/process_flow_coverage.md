# Process Flow Coverage

Cross-checking extracted emission units against the canonical oilseed-crush process flow (`domain_knowledge/process_flows/oilseed_crush.md`).

✅ = at least one extracted unit maps to this step.  
⚠️ = the step is missing from the extraction. Could be (a) the LLM missed it, (b) the permit doesn't itemize it (bundled into another), or (c) the facility doesn't have this capability. Spot-check the source PDF to determine which.

| Status | Step | Categories that satisfy it | Extracted EUs in this step |
|---|---|---|---|
| ✅ | Receiving (truck + rail) | receiving, handling, loading/unloading | 5511-FEU-5101, EP12 EU34 ML-1 Truck (MPeeallle/ tH) ull / Hull 1,314,000, EP12 EU35 ML-1 RaPil e(Mlleet)a lL/ oHaudllo /u Htull 1,314,000, EP51F EU51 Fugitive Rail Receiving, EU01, EU01/02 ... (+14 more) |
| ⚠️ | Cleaning | cleaning | — |
| ✅ | Drying | drying | EP08 EU23, EP09A EU24A CE-10A Meal Dryer Deck #3, EU23, EU24, EU24A |
| ⚠️ | Cracking & dehulling | cracking and dehulling, cracking, dehulling | — |
| ✅ | Conditioning | conditioning | EP03 EU07, EP03 EU09, EP03 EU16, EP20 EU17, EP44 EU44, EU10, EU11 and EU13 ... (+1 more) |
| ✅ | Flaking | flaking | EU12 and EU14 |
| ⚠️ | Expanding (extruder, optional) | expanding, expander, extrusion | — |
| ✅ | Solvent extraction (hexane) | extraction | EU38 |
| ⚠️ | Desolventizer-toaster (DT) | desolventizing, desolventizer, toaster | — |
| ✅ | Meal drying & cooling | drying, cooling | EP08 EU23, EP09A EU24A CE-10A Meal Dryer Deck #3, EP10 EU25, EP10 EU25 CE-11 Meal Cooler Deck, EU20 and EU21, EU23 ... (+6 more) |
| ✅ | Meal grinding | grinding | EP07 EU20 & EU21 CE-08 Pellet CMoioll l&er Pellet, EP20 EU17 CE-20 & 20A Hull Grinders (2 units), EU27 and EU28, EU28 |
| ✅ | Meal storage | storage | EP01 EU05, EP11 EU29, EP13 EU36, EP13 EU36 MC-1 Meal Clay Storage 6,570, EP14 EU37 RCB Refinery Clay Storage 4,500, EP19 EU47 RC-2 Silica Clay Storage 450 ... (+18 more) |
| ✅ | Meal loadout (rail/truck/barge) | loading/unloading, loadout, conveying | EU04a, EU-04a to EU05, EU-04 to EU05, EU19, EU26, EU33 ... (+3 more) |
| ⚠️ | Solvent (hexane) recovery | solvent_recovery, hexane, recovery | — |
| ⚠️ | Degumming | degumming | — |
| ⚠️ | Neutralization (caustic refining) | neutralization, refining | — |
| ⚠️ | Bleaching | bleaching | — |
| ⚠️ | Deodorizing (RBD final) | deodorizing | — |
| ✅ | Refined oil storage / loadout | storage, loading/unloading | EP01 EU05, EP11 EU29, EP13 EU36, EP13 EU36 MC-1 Meal Clay Storage 6,570, EP14 EU37 RCB Refinery Clay Storage 4,500, EP19 EU47 RC-2 Silica Clay Storage 450 ... (+21 more) |
| ✅ | Boilers / steam utilities | boiler, boilers | EU40, EU41, EU42, EU46 |
| ⚠️ | Dust control (baghouse / cyclone / scrubber) | baghouse, scrubber, cyclone, aspiration | — |
| ✅ | Cooling towers | cooling | EP10 EU25, EP10 EU25 CE-11 Meal Cooler Deck, EU20 and EU21, EU25, EU45, EU49 ... (+1 more) |

**Coverage: 12/22 canonical steps**

_Note: not every facility runs every step (e.g., a crude-only crusher won't have refining steps 15-18). Use this as a checklist for spot-checking, not as a hard quality score._