# Process Flow Coverage

Cross-checking extracted emission units against the canonical oilseed-crush process flow (`domain_knowledge/process_flows/oilseed_crush.md`).

✅ = at least one extracted unit maps to this step.  
⚠️ = the step is missing from the extraction. Could be (a) the LLM missed it, (b) the permit doesn't itemize it (bundled into another), or (c) the facility doesn't have this capability. Spot-check the source PDF to determine which.

| Status | Step | Categories that satisfy it | Extracted EUs in this step |
|---|---|---|---|
| ✅ | Receiving (truck + rail) | receiving, handling, loading/unloading | EU-1, EU-10, EU-1.01, EU-1.02, EU-1.03, EU-1.04 ... (+37 more) |
| ⚠️ | Cleaning | cleaning | — |
| ✅ | Drying | drying | DC-409, DC-409, Column dryer, DC-410, DC-411, DC-412, DC-413, and DT seal screw, DC-414 ... (+9 more) |
| ✅ | Cracking & dehulling | cracking and dehulling, cracking, dehulling | Cracker Roll 1 (EU-6), Cracker Roll 2 (EU-6), Cracker Roll 3 (EU-6), Cracker Roll 4 (EU-6), Cracker Roll 5 (EU-6), EU-35 ... (+11 more) |
| ✅ | Conditioning | conditioning | 3 meal sifters, pneumatic hull conveying system, DC-410, DC-411, DC-412, DC-413, DC seal screw, SC-209, DC-414, DC-414A, DC-415, DC-416, SC-223, Meal grinder #1, Meal grinder #2, Meal grinder #3, SC-221, DC-417, BE-300, DC-418, DC-419, EU-14, EU-37, EU-39 ... (+1 more) |
| ✅ | Flaking | flaking | flaker banks #1 & 2 |
| ⚠️ | Expanding (extruder, optional) | expanding, expander, extrusion | — |
| ✅ | Solvent extraction (hexane) | extraction | EU-13, Whole Soybean Extraction Plant |
| ⚠️ | Desolventizer-toaster (DT) | desolventizing, desolventizer, toaster | — |
| ✅ | Meal drying & cooling | drying, cooling | DC-409, DC-409, Column dryer, DC-410, DC-411, DC-412, DC-413, and DT seal screw, DC-414 ... (+12 more) |
| ✅ | Meal grinding | grinding | pod grinder, soybean meal grinders |
| ✅ | Meal storage | storage | 207 & 208, 44% meal tank, 48% meal tank, 48% meal tank, 44% meal tank, 4 soybean storage tanks, 18 storage bins, 2 weed seed bins (207 & 150208), 809 A & B ... (+8 more) |
| ✅ | Meal loadout (rail/truck/barge) | loading/unloading, loadout, conveying | 213 & 214, 400, 400A, 427, 428, & 429, 447, 453 ... (+34 more) |
| ⚠️ | Solvent (hexane) recovery | solvent_recovery, hexane, recovery | — |
| ⚠️ | Degumming | degumming | — |
| ⚠️ | Neutralization (caustic refining) | neutralization, refining | — |
| ⚠️ | Bleaching | bleaching | — |
| ⚠️ | Deodorizing (RBD final) | deodorizing | — |
| ✅ | Refined oil storage / loadout | storage, loading/unloading | 207 & 208, 44% meal tank, 48% meal tank, 48% meal tank, 44% meal tank, 4 soybean storage tanks, 18 storage bins, 2 weed seed bins (207 & 150208), 809 A & B ... (+13 more) |
| ✅ | Boilers / steam utilities | boiler, boilers | 2.00, Boiler #1 (60 MMBtu/hr), Boiler #1 (60 MMBTU/HR), Boiler #2, Boiler #2 (75 MMBtu/hr), Boiler #2 (75 MMBTU/HR) ... (+2 more) |
| ✅ | Dust control (baghouse / cyclone / scrubber) | baghouse, scrubber, cyclone, aspiration | coarse cut aspiration, EU-41, EU-42, fine cut aspiration |
| ✅ | Cooling towers | cooling | dryer/cooler, EU-uuu, S-17 |

**Coverage: 14/22 canonical steps**

_Note: not every facility runs every step (e.g., a crude-only crusher won't have refining steps 15-18). Use this as a checklist for spot-checking, not as a hard quality score._