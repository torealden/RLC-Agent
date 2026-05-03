# Process Flow Coverage

Cross-checking extracted emission units against the canonical oilseed-crush process flow (`domain_knowledge/process_flows/oilseed_crush.md`).

✅ = at least one extracted unit maps to this step.  
⚠️ = the step is missing from the extraction. Could be (a) the LLM missed it, (b) the permit doesn't itemize it (bundled into another), or (c) the facility doesn't have this capability. Spot-check the source PDF to determine which.

| Status | Step | Categories that satisfy it | Extracted EUs in this step |
|---|---|---|---|
| ✅ | Receiving (truck + rail) | receiving, handling, loading/unloading | EU-001, EU-002, EU-003, EU-004, EU-005, EU-019 ... (+6 more) |
| ⚠️ | Cleaning | cleaning | — |
| ✅ | Drying | drying | EU-013, EU-014, EU-015, EU-016, EU-017, EU-018 ... (+2 more) |
| ✅ | Cracking & dehulling | cracking and dehulling, cracking, dehulling | EU-009 |
| ✅ | Conditioning | conditioning | EU-033, EU-034, EU-035, EU-052 |
| ⚠️ | Flaking | flaking | — |
| ⚠️ | Expanding (extruder, optional) | expanding, expander, extrusion | — |
| ✅ | Solvent extraction (hexane) | extraction | EU-036 |
| ⚠️ | Desolventizer-toaster (DT) | desolventizing, desolventizer, toaster | — |
| ✅ | Meal drying & cooling | drying, cooling | EU-013, EU-014, EU-015, EU-016, EU-017, EU-018 ... (+3 more) |
| ✅ | Meal grinding | grinding | EU-012, EU-041 |
| ✅ | Meal storage | storage | EU-028, EU-030, EU-031, EU-042, EU-043 |
| ✅ | Meal loadout (rail/truck/barge) | loading/unloading, loadout, conveying | EU-002, EU-004, EU-019, EU-020, EU-023, EU-024 ... (+6 more) |
| ⚠️ | Solvent (hexane) recovery | solvent_recovery, hexane, recovery | — |
| ⚠️ | Degumming | degumming | — |
| ✅ | Neutralization (caustic refining) | neutralization, refining | EU-038 |
| ⚠️ | Bleaching | bleaching | — |
| ⚠️ | Deodorizing (RBD final) | deodorizing | — |
| ✅ | Refined oil storage / loadout | storage, loading/unloading | EU-002, EU-004, EU-019, EU-020, EU-028, EU-029 ... (+7 more) |
| ✅ | Boilers / steam utilities | boiler, boilers | EU-021, EU-022, EU-039, EU-045, EU-046 |
| ✅ | Dust control (baghouse / cyclone / scrubber) | baghouse, scrubber, cyclone, aspiration | EU-006, EU-007, EU-008, EU-010, EU-011, EU-025 ... (+2 more) |
| ✅ | Cooling towers | cooling | EU-051 |

**Coverage: 14/22 canonical steps**

_Note: not every facility runs every step (e.g., a crude-only crusher won't have refining steps 15-18). Use this as a checklist for spot-checking, not as a hard quality score._