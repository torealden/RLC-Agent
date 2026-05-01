# Oilseed Crushing Facility — Process Flow Reference

> **Purpose**: Canonical reference of process steps, typical equipment, capacity
> conventions, and diagnostic ratios for an oilseed crushing facility (soybean,
> canola, sunflower, cottonseed). Used by:
> - The Title V permit LLM extractor for category validation
> - Per-facility capacity estimation when permit data is incomplete
> - Symbiotic forecasting (IFV kg_callable, crush margin models)
> - Plant Intelligence module for new-facility scouting / due diligence

This document covers **solvent-extraction** crushers (the dominant U.S. and
international design for soybeans). Variations for expeller-press,
extrusion-expeller, and hybrid plants are noted at the end.

---

## High-level process flow

```
[Truck/Rail Receiving] → [Cleaning] → [Drying] → [Dehulling/Cracking] → [Conditioning] → [Flaking] → [Expanding (optional)]
        ↓
[Solvent Extraction] → [Desolventizing-Toaster (DT)] → [Meal Drying/Cooling] → [Meal Grinding] → [Meal Storage] → [Loadout]
        ↓
[Solvent Recovery] → [Crude Oil] → [Degumming] → [Neutralization] → [Bleaching] → [Deodorizing] → [Refined Oil Storage] → [Loadout]
```

Most large U.S. soy crushers integrate refining on-site. Smaller / older
facilities may ship crude oil to a separate refinery.

---

## Process step detail

### 1. Receiving (raw seed inbound)

- **Function**: Unload trucks, rail cars, barges; weigh and probe samples.
- **Typical equipment**:
  - Truck dump pit / hopper with mechanical or pneumatic conveyor
  - Rail receiving pit with bottom-dump or hopper-bottom unloading
  - Aspiration / dust collection at every pit and conveyor transfer point
- **Typical capacity units**: tons/hour (truck) and rail cars/day
- **Common control devices**: cyclone, baghouse, aspirator
- **Diagnostic**: total receiving rate ≥ 1.5x crush rate (need surge headroom)

### 2. Cleaning

- **Function**: Remove foreign material, fines, broken seed, metal.
- **Typical equipment**:
  - Vibratory screen / scalper
  - Magnetic separator
  - Air aspiration cleaner
  - Stone destoner
- **Typical capacity units**: tons/hour
- **Diagnostic**: cleaning capacity matched to receiving (no buffer needed
  because cleaning is fast); typical 1-3% material removal as screenings.

### 3. Drying

- **Function**: Reduce moisture to ~10-11% (soy) or ~7-8% (canola) for
  optimal cracking and conditioning. Often skipped if seed arrives dry.
- **Typical equipment**: continuous flow grain dryer (gas-fired)
- **Typical capacity units**: tons/hour rated, MMBtu/hr fuel input
- **Common control devices**: cyclone (typically uncontrolled NOx)
- **Diagnostic**: dryer fuel input ≈ 1-2 MMBtu per ton seed.

### 4. Dehulling / Cracking

- **Function**: Crack whole seed into ~6-8 pieces (soy) or split (canola)
  to expose meats; remove hulls in soy "Hi-Pro" lines.
- **Typical equipment**:
  - Cracking mill (corrugated rolls)
  - Aspirator (hull removal)
  - Hull conveyor / hammer mill
  - Hull surge bin
- **Typical capacity units**: tons/hour
- **Common control devices**: baghouse, cyclone aspirator
- **Diagnostic**:
  - Soy: ~5-7% hull removal in Hi-Pro, ~0% in standard 44%.
  - Hull yield from soy ≈ 6% of clean seed weight.

### 5. Conditioning

- **Function**: Heat cracked seed to 165-175°F to make it pliable for flaking.
- **Typical equipment**:
  - Rotary conditioner (steam-heated drum)
  - Vertical stack conditioner (multi-tray)
- **Typical capacity units**: tons/hour
- **Diagnostic**: residence time ~30 min; steam consumption ~150 lb/ton seed.

### 6. Flaking

- **Function**: Roll conditioned cracks into thin (0.30-0.35 mm) flakes
  to maximize surface area for solvent extraction.
- **Typical equipment**: smooth-surface flaking rolls (often 4-6 in parallel)
- **Typical capacity units**: tons/hour per roll; total = sum of rolls
- **Diagnostic**: each modern flaker handles ~50-100 tons/hr.

### 7. Expanding (optional, "extruder")

- **Function**: Texture flakes into porous "collets" for higher extraction
  efficiency. Common in modern plants but not all.
- **Typical equipment**: low-shear extruder / expander
- **Typical capacity units**: tons/hour
- **Diagnostic**: presence of an expander = ~+5-10% extraction efficiency,
  smaller solvent extractor footprint per ton.

### 8. Solvent extraction

- **Function**: Extract crude oil from flakes/collets using hexane.
- **Typical equipment**:
  - Continuous loop / horizontal-belt extractor (Crown Iron Works,
    De Smet, or Desmet Ballestra are common OEMs)
  - Hexane storage tanks (typically 2-4 above-ground, 30,000-100,000 gal each)
  - Miscella (oil + hexane) tanks
  - Spent flake conveyor to DT
- **Typical capacity units**:
  - **tons/day** (US standard) — this is the headline plant capacity figure
  - **bushels/day** (sales reporting; 1 ton ≈ 36.74 bu soy)
- **Common control devices**:
  - Mineral oil scrubber on solvent vent
  - Condenser
  - Vapor recovery on tanks
- **Diagnostic**:
  - Hexane consumption: **0.6-1.2 gallons/ton crushed** (used as facility-wide
    cap and reporting metric — Iowa typically sets 246 tons hexane/year as
    plant-wide limit, equivalent to ~1.0 gal/ton at full design rate).
  - Crude oil yield from soy ≈ 18-19% of bean weight.
  - Spent flake (white flake) ≈ 79-80% of bean weight, going to DT.

### 9. Desolventizing-Toaster (DT)

- **Function**: Strip residual hexane from spent flakes; toast (denature)
  protein for soybean meal.
- **Typical equipment**:
  - Schumacher / Crown DT vessel (multi-tray steam-heated)
  - Vapor scrubber
- **Typical capacity units**: tons/hour
- **Common control devices**: mineral oil scrubber, condenser
- **Diagnostic**: residual hexane in finished meal < 500 ppm (regulatory).

### 10. Meal drying / cooling

- **Function**: Reduce meal moisture to ~10-12% and cool to <100°F.
- **Typical equipment**: meal dryer / cooler (often integrated with DT)
- **Typical capacity units**: tons/hour
- **Diagnostic**: drying air flow ~5,000-15,000 ACFM per ton meal/hour.

### 11. Meal grinding / sizing

- **Function**: Grind meal to spec (44% protein "standard" or 48%+ "Hi-Pro");
  ensure particle size for downstream feed mills.
- **Typical equipment**: hammer mill, roller mill, fine grinder
- **Typical capacity units**: tons/hour

### 12. Meal storage

- **Function**: Buffer meal output to truck/rail loadout.
- **Typical equipment**: silos, bins, flat warehouse
- **Typical capacity units**: tons (storage volume)
- **Diagnostic**: typical meal storage = 7-14 days of plant production
  (~10,000-50,000 tons depending on plant size).

### 13. Meal loadout

- **Function**: Load meal to trucks (typical 25 tons), rail (110 tons),
  barge (1500 tons).
- **Typical equipment**:
  - Truck loadout aspiration
  - Rail meal loadout (often 200-500 tons/hr)
  - Barge loadout (Iowa: only on Mississippi/Missouri rivers)
- **Common control devices**: baghouse, aspiration cyclone

### 14. Solvent recovery

- **Function**: Distill miscella to separate hexane (recycle) from crude oil.
- **Typical equipment**:
  - Long-tube falling-film evaporator
  - Stripper column
  - Condenser / hexane vapor recovery
- **Diagnostic**: recovered hexane returns to extractor; losses are the
  facility's hexane consumption metric.

### 15. Crude oil → degumming

- **Function**: Remove phosphatides (gums) using water or acid wash.
- **Typical equipment**: degumming reactor + centrifuge
- **Typical capacity units**: tons/hour or lbs/hr
- **Diagnostic**: gum yield ≈ 1-2% of crude oil = lecithin byproduct.

### 16. Neutralization (alkali refining)

- **Function**: Remove free fatty acids using caustic soda (NaOH).
- **Typical equipment**: caustic mix tank + centrifuge
- **Diagnostic**: yields soapstock (~1-3% of oil) sent to acidulation.

### 17. Bleaching

- **Function**: Remove color, residual gums, soap, metals using activated
  bleaching clay.
- **Typical equipment**: bleaching reactor + niagara (filter) + clay
  receiving/storage silo + spent clay handling
- **Diagnostic**: bleaching clay use ≈ 0.5-1.0% of oil weight.

### 18. Deodorizing

- **Function**: Strip volatiles (free fatty acids, peroxides, color) under
  vacuum + steam at 240-260°C; final product is RBD oil (refined,
  bleached, deodorized).
- **Typical equipment**: stripper column under deep vacuum, steam ejectors
  or vacuum pumps, deodorizer distillate condenser
- **Typical capacity units**: tons/hour
- **Common control devices**: scrubber, vapor condenser
- **Diagnostic**: this step distinguishes "RBD" / refined oil from "crude"
  output; presence of a deodorizer = full-refining capability.

### 19. Refined oil storage / loadout

- **Function**: Hold and ship refined oil.
- **Typical equipment**: refined oil storage tanks (often N2-blanketed)
- **Typical capacity units**: tons (volume)
- **Diagnostic**: refined oil tanks = 1-2 weeks production.

---

## Plant-wide utilities & support equipment

These appear in every Title V and contribute to the facility's emission
profile but aren't "process" units per se:

| Utility | Typical equipment | Capacity unit |
|---|---|---|
| Boilers / steam | Coal / NG / fuel-oil boilers (often two: package + economizer) | MMBtu/hr |
| Cooling | Cooling towers (mechanical draft) | MMBtu/hr or tons cooling |
| Compressed air | Air compressors | hp / SCFM |
| Wastewater | API separator, biological treatment | gpm |
| Storage tanks | Hexane tanks, fuel tanks, oil tanks | gallons |
| Dust control | Baghouses, cyclones, scrubbers (per process step) | ACFM |

---

## Capacity conventions (diagnostic ratios)

These are the load-bearing relationships for estimating facility size from
partial data:

| Output / Input | Typical ratio | Use |
|---|---|---|
| Soybean meal yield | 0.79-0.80 of bean weight | Meal output → bean input |
| Crude oil yield (soy) | 0.18-0.19 of bean weight | Oil output → bean input |
| Hull yield (Hi-Pro) | 0.06 of bean weight | Hull stream sizing |
| Hexane usage | 0.6-1.2 gal/ton crushed | Plant-wide hexane cap → crush rate |
| Steam consumption | 130-180 lb/ton seed | Boiler sizing → crush rate |
| Power consumption | 25-40 kWh/ton seed | Electrical service → crush rate |
| MMBtu boiler / crush rate | 4-6 MMBtu/ton seed | Boiler total → crush rate |
| Bushels ↔ tons (soy) | 36.74 bu/ton | Unit conversion |
| Bushels ↔ tons (canola) | 44.09 bu/ton | Unit conversion |
| Crude oil → refined oil | ~0.97 (refining loss) | Oil yield through full refining |
| Bleaching clay use | 0.5-1.0% of oil | Spent clay disposal sizing |

---

## Typical facility size brackets (US soy)

| Size | Crush rate | Industry archetype |
|---|---|---|
| Small | < 1,000 t/d (< 36,000 bu/d) | Older / regional, often expeller-press |
| Medium | 1,000-3,000 t/d | Standard mid-size, many AGP/Cargill regional |
| Large | 3,000-5,000 t/d | Newer continuous-loop, integrated refining |
| Mega | > 5,000 t/d | Cargill Cedar Rapids, ADM Mankato, AGP Sergeant Bluff |

---

## Variations from the standard solvent-extraction template

### Expeller-press (mechanical) crushers

- **Difference**: Use hydraulic / screw press instead of solvent extraction.
- **Implications**:
  - No hexane, no DT, no solvent recovery, no scrubbers
  - Lower oil yield (~12-14% vs 18-19%)
  - "Expeller meal" has higher residual oil (~6-8% vs ~1%)
  - Typically smaller plants (<500 tons/day)
- **Permit signature**: NO hexane storage, NO solvent extractor; presence
  of "screw press" or "expeller" instead.

### Extrusion-expeller (ExPress / Insta-Pro) plants

- **Difference**: Hot extrusion immediately before press; no solvent.
- **Implications**: Identification niche / non-GMO / organic markets.
- **Permit signature**: dry extruder + screw press + no hexane.

### Co-located refining vs standalone

- **Integrated**: ~70%+ of US soy crushers refine on-site (degumming →
  neutralization → bleaching → deodorizing all present).
- **Crude-oil-only**: smaller plants ship crude oil to remote refiners.
  Permit will lack bleaching clay and deodorizer references.

### Hi-Pro vs standard meal lines

- **Hi-Pro**: includes dehulling and hull removal stage; meal protein 48%+.
- **Standard 44%**: hulls left in meal; simpler line.
- **Permit signature**: presence of hull conveyor + hull pelleter +
  separate hull bins → Hi-Pro line.

---

## How to use this document

### For permit extraction validation

When the LLM extracts emission units from a Title V, cross-check against
this list:

- Soy crusher should have ALL of: receiving, cleaning, drying, dehulling/
  cracking, conditioning, flaking, extractor, DT, meal cooler, meal
  grinder, meal storage, meal loadout, hexane tanks, boilers.
- If the extractor is missing the desolventizer-toaster but found the
  extractor, that's an LLM extraction gap (DT is always present).
- If the extractor is missing the solvent extractor but found the boilers,
  that's likely an expeller-press plant (different process).

### For capacity estimation from partial data

Given any one of:
- Hexane plant-wide cap (e.g., 246 tons/yr)
- Total boiler MMBtu/hr
- Single rated capacity (e.g., flaker tons/hr)
- Meal storage tonnage

→ derive facility crush rate using the diagnostic ratios above.

### For Plant Intelligence / new-facility scouting

When evaluating a new or expanded facility, check:
- What process steps are PRESENT (extraction integration tier)
- What size bracket (capacity)
- What feedstocks (soy / canola / multi-seed)
- What output products (crude oil / refined oil / lecithin / hulls /
  meal protein spec)

---

## References / sources to cross-link

- HOBO Renewables feasibility study — HEFA process is downstream of crusher
- AGP Title V permits (Iowa DNR) — canonical multi-facility format
- Cargill Title V permits (Iowa DNR) — alternative format with same flow
- Bunge Title V — older single-train layout
- Crown Iron Works equipment manuals (proprietary) — OEM standard
- Desmet Ballestra extractor design specs (publicly available)
- USDA NASS Fats & Oils report — facility-aggregated meal/oil yield data
- KG nodes: `oilseed_crushing_plant_model`, `crusher_feasibility_model`,
  `crush_economics`, `feedstock_supply_chain_model`

---

## Maintenance

This document gets richer as we extract more permits. After a batch of
extractions:
1. Aggregate observed equipment categories across facilities → flag any
   appearing in multiple plants but missing from this doc → add them.
2. Aggregate observed capacity ranges → tighten the diagnostic ratio bands.
3. Aggregate variations → note in the Variations section.

Eventually this should be reflected as KG nodes (one per process step) so
the facility agents can reason about it programmatically.
