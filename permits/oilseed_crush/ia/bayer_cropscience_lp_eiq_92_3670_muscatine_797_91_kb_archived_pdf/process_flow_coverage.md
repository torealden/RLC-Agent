# Process Flow Coverage

Cross-checking extracted emission units against the canonical oilseed-crush process flow (`domain_knowledge/process_flows/oilseed_crush.md`).

✅ = at least one extracted unit maps to this step.  
⚠️ = the step is missing from the extraction. Could be (a) the LLM missed it, (b) the permit doesn't itemize it (bundled into another), or (c) the facility doesn't have this capability. Spot-check the source PDF to determine which.

| Status | Step | Categories that satisfy it | Extracted EUs in this step |
|---|---|---|---|
| ✅ | Receiving (truck + rail) | receiving, handling, loading/unloading | EU-4-0211 Technical Reactor #1, EU-4-0231 Technical Reactor #2, EU-4-1124 Technical Reactor #3, EU-9-TK-25 Reactor |
| ⚠️ | Cleaning | cleaning | — |
| ⚠️ | Drying | drying | — |
| ⚠️ | Cracking & dehulling | cracking and dehulling, cracking, dehulling | — |
| ⚠️ | Conditioning | conditioning | — |
| ⚠️ | Flaking | flaking | — |
| ⚠️ | Expanding (extruder, optional) | expanding, expander, extrusion | — |
| ✅ | Solvent extraction (hexane) | extraction | EU-01-0201 CAC Stripper, EU-11-119-2 Acid Cracking, EU-11-S-1-1 CAC Process Vents, EU-11-S-1-2, EU-11-S-1-2 CAC Process Off-Gas, EU-13-514-2 ... (+22 more) |
| ⚠️ | Desolventizer-toaster (DT) | desolventizing, desolventizer, toaster | — |
| ✅ | Meal drying & cooling | drying, cooling | EU-1-844 GT East Cooling Tower, 4 Cells, EU-1-845 GT West Cooling Tower, 2 Cells |
| ⚠️ | Meal grinding | grinding | — |
| ✅ | Meal storage | storage | EU-09-0100 Day Tank, EU-13-0949 Technical Storage Tank 'D', EU-4-0033 Catalyst Recycle Tank Knockout Pot, EU-4-0048 Centrate Surge Tank, EU-4-0049 Waste Surge Tank, EU-4-0170 GI Slurry Tank ... (+30 more) |
| ⚠️ | Meal loadout (rail/truck/barge) | loading/unloading, loadout, conveying | — |
| ⚠️ | Solvent (hexane) recovery | solvent_recovery, hexane, recovery | — |
| ⚠️ | Degumming | degumming | — |
| ⚠️ | Neutralization (caustic refining) | neutralization, refining | — |
| ⚠️ | Bleaching | bleaching | — |
| ⚠️ | Deodorizing (RBD final) | deodorizing | — |
| ✅ | Refined oil storage / loadout | storage, loading/unloading | EU-09-0100 Day Tank, EU-13-0949 Technical Storage Tank 'D', EU-4-0033 Catalyst Recycle Tank Knockout Pot, EU-4-0048 Centrate Surge Tank, EU-4-0049 Waste Surge Tank, EU-4-0170 GI Slurry Tank ... (+30 more) |
| ✅ | Boilers / steam utilities | boiler, boilers | EU-11-106 |
| ⚠️ | Dust control (baghouse / cyclone / scrubber) | baghouse, scrubber, cyclone, aspiration | — |
| ✅ | Cooling towers | cooling | EU-1-844 GT East Cooling Tower, 4 Cells, EU-1-845 GT West Cooling Tower, 2 Cells |

**Coverage: 7/22 canonical steps**

_Note: not every facility runs every step (e.g., a crude-only crusher won't have refining steps 15-18). Use this as a checklist for spot-checking, not as a hard quality score._