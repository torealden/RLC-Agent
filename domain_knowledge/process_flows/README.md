# Process Flow Reference Library

Per-industry canonical process flow documents that describe the typical
equipment, capacity units, control devices, and diagnostic ratios you'd
expect to find in a facility of that type.

Each document feeds:

- The **Title V permit LLM extractor** — provides the expected category
  enum and equipment inventory to validate extraction against.
- The **per-facility capacity estimator** — diagnostic ratios let us infer
  total facility size from any single observed permit value (boiler MMBtu,
  hexane cap, single rated unit, storage tonnage).
- The **facility agent simulation** (phase two) — agents need a process
  model to reason about what the facility can produce.
- The **knowledge graph** — eventually each process step becomes a `kg_node`
  with edges representing material/energy flows.

## Industries

Status: 🟢 = drafted | 🟡 = stub | ⚪ = not started

| Industry | File | Status |
|---|---|---|
| Oilseed crushing (solvent extraction) | [oilseed_crush.md](oilseed_crush.md) | 🟢 |
| Biodiesel production (transesterification) | _biodiesel.md_ | ⚪ |
| Renewable diesel (HEFA) | _renewable_diesel.md_ | ⚪ |
| Sustainable aviation fuel (SAF) | _saf.md_ | ⚪ |
| Beef slaughter | _beef_slaughter.md_ | ⚪ |
| Pork slaughter | _pork_slaughter.md_ | ⚪ |
| Poultry slaughter | _poultry_slaughter.md_ | ⚪ |
| Rendering (independent + integrated) | _rendering.md_ | ⚪ |
| Used cooking oil (UCO) collection | _uco_collection.md_ | ⚪ |
| Wheat milling | _wheat_milling.md_ | ⚪ |
| Corn dry milling (ethanol) | _ethanol_dry_mill.md_ | ⚪ |
| Corn wet milling | _ethanol_wet_mill.md_ | ⚪ |
| Sugar refining | _sugar_refining.md_ | ⚪ |
| Edible oil refining (standalone) | _oil_refining.md_ | ⚪ |
| Tallow / lard / yellow grease processing | _animal_fats_processing.md_ | ⚪ |
| Food manufacturing (multi-output) | _food_manufacturing.md_ | ⚪ |

## Document template

Each industry file follows the same structure (see `oilseed_crush.md`):

1. **Purpose** — what this doc feeds
2. **High-level process flow** (ASCII diagram)
3. **Process step detail** — function, typical equipment, capacity units,
   control devices, diagnostic ratios
4. **Plant-wide utilities & support**
5. **Capacity conventions** — diagnostic ratios for size estimation
6. **Facility size brackets**
7. **Variations from the standard template**
8. **How to use** — extraction validation, capacity estimation, scouting
9. **References / sources**
10. **Maintenance** — how to enrich over time

## Maintenance principle

These docs are **living references**. When we extract a batch of permits
and observe equipment / capacity ranges that don't match the current ratios,
update the doc. The LLM extraction pipeline is what feeds the enrichment.
