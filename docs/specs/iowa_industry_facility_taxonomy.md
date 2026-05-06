# Iowa Commodity Industry Facility Taxonomy

Comprehensive inventory of facility types relevant to the Round Lakes
commodity-market intelligence system in Iowa, with per-industry seed
lists, authoritative data sources, permit availability, and per-industry
LLM extraction targets.

This is the master document for expanding the Market Field beyond
oilseed crush into the full Iowa commodity ecosystem. Sequencing of
build order is at the bottom.

---

## 1. Why this matters

The Market Field's facility graph currently has 24 oilseed crush
plants. Real commodity market dynamics in Iowa flow through a much
larger network of nodes — ethanol plants compete with oilseed crush
for corn, packers drive meal demand, CAFOs concentrate demand
geographically, rail terminals dictate basis, river terminals set
export pricing, and so on. Modeling Iowa's commodity markets without
these nodes leaves most of the network invisible.

Each facility is a decision-maker that buys inputs and sells outputs.
The Market Field's network propagation needs all of them to model
how information actually moves through the state's supply chain.

---

## 2. Industry taxonomy

Eight industry categories, ordered by data-richness and integration
priority (highest first).

### 2.1 Oilseed crush — DONE

Already in production. 24 canonical plants. See
`reference.oilseed_crush_facilities`.

### 2.2 Ethanol production — Tier 1 (highest priority)

**Why first:** Iowa is the #1 ethanol-producing state in the US
(~4.7 billion gallons/year, ~30% of national production). Direct
competitor for corn supply with feed buyers; sets the soybean meal
demand baseline; co-products (DDGS, distillers oil) are key inputs
to other industries we already track.

**Major operators in Iowa:**

| Operator | Plants | Approx. capacity (mmgy) |
|---|---|---|
| POET Biorefining | Coon Rapids, Ashton, Emmetsburg, Hanlontown, Jewell, Gowrie | ~500 each, fleet ~3 BG total |
| ADM (Archer-Daniels-Midland) | Cedar Rapids, Clinton | 350-400 each |
| Valero Renewable Fuels | Albert City, Charles City, Fort Dodge, Hartley | ~140 each |
| Green Plains | Shenandoah, Superior | ~80 each |
| Cargill | Eddyville (also crush) | ~140 |
| Big River Resources | West Burlington | ~110 |
| Lincolnway Energy | Nevada | ~50 |
| Pine Lake Corn Processors | Steamboat Rock | ~110 |
| Plymouth Energy | Merrill | ~45 |
| Quad County Corn Processors | Galva | ~35 |
| Western Iowa Energy | Wall Lake | ~30 (also biodiesel) |
| Absolute Energy | St. Ansgar | ~120 |
| Iowa Renewable Energy | Mason City | ~100 |
| Lincolnland Agri-Energy | Palestine IL (not IA) | skip |
| Siouxland Ethanol | Jackson NE (not IA) | skip |

**Authoritative sources:**
- **Renewable Fuels Association (RFA)** plant directory:
  https://ethanolrfa.org/markets-and-statistics/biorefinery-locations
- **EIA Form EIA-819** (monthly biofuels production by state)
- **EPA RFS RIN-generation registrations** — every active plant
  is on the EPA RFS website with company name + facility ID
- **Iowa Renewable Fuels Association** — state-specific advocacy
  group with plant list

**Permit availability:**
- All ethanol plants are Title V major sources (CO, VOC, PM10
  emissions exceed 100 tpy threshold)
- Iowa DNR Air Permits database: searchable by company / county
- EPA ECHO Air program: cross-reference for federal data

**LLM extraction targets per permit:**
- Nameplate capacity in million gallons per year (mmgy)
- Bushels of corn per day at full utilization
- Distillers grains output (lb/bu corn)
- Distillers oil output (lb/bu corn) — the "corn oil"
  / DCO subset that ties back to oilseed-oil markets
- CO2 emissions allowance (relevant for 45Z eligibility)
- Boiler/dryer fuel type (natural gas vs solid fuel — 45Z bonus)
- Year commissioned / last expansion
- Solvent extraction technology (DDGS vs WDGS dominant)
- Co-located biofuel production (ethanol + biodiesel sites)

### 2.3 Biodiesel and renewable diesel — Tier 1

**Why:** Major demand source for vegetable oil + animal fats.
Expansion drivers depend on EPA RVO + LCFS + 45Z; tightly coupled
to crush economics through oil-share dynamics.

**Major operators in Iowa:**

| Operator | Plant | Type | Capacity (mmgy) |
|---|---|---|---|
| Chevron Renewable Energy Group (formerly REG) | Newton | Biodiesel | 30 |
| Chevron Renewable Energy Group | Mason City | Biodiesel | 30 |
| Chevron Renewable Energy Group | Ralston | Biodiesel | 12 |
| Western Iowa Energy | Wall Lake | Biodiesel + ethanol combo | 30 |
| Cargill | Iowa Falls | Biodiesel (small) | ~30 |
| Stockton Plant — IA | Tama | Biodiesel | ~20 |

**Note:** Iowa has NO major renewable diesel (RD) production. RD
plants concentrated in Gulf Coast (Diamond Green Diesel, Marathon),
Pacific Northwest, and Western Iowa Energy has announced expansion
plans (worth tracking).

**Authoritative sources:**
- **National Biodiesel Board (now Clean Fuels Alliance America)**
  member directory
- **EPA RFS RIN-generation registrations** — biodiesel and
  renewable diesel plants registered separately under D4 (BBD)
- **Iowa Renewable Fuels Association** state directory
- **Form EIA-22M Monthly Biodiesel Production Report**

**Permit availability:**
- All biodiesel plants > 1 mmgy are Title V air sources
- Iowa DNR Air Permits — same database as ethanol
- Smaller plants (<1 mmgy) may be minor sources, not Title V

**LLM extraction targets:**
- Nameplate capacity (mmgy)
- Feedstock flexibility (soybean oil only? UCO? animal fats? mixed?)
- Storage capacity (gallons)
- Glycerin co-product output
- Pretreatment technology (degumming, refining, esterification)
- Year commissioned / last upgrade
- Hydrogen consumption (RD pretreatment indicator)
- Connection to oilseed crush (if co-located)

### 2.4 Pork packing / slaughter — Tier 1

**Why:** Iowa is the #1 pork-producing state, ~33% of US pork
production. Pork packers are the largest US consumers of soybean
meal (via the cattle/hog feeding pipeline). Their throughput sets
soybean meal regional demand and influences cash basis at IA
crush plants.

**Major operators in Iowa:**

| Operator | Plant location(s) | Daily slaughter capacity (head) |
|---|---|---|
| Tyson Foods | Storm Lake, Waterloo, Perry, Columbus Junction | 18-20K each, ~70K combined |
| JBS USA | Marshalltown, Ottumwa | 17K + 21K |
| Smithfield Foods | Algona, Denison, Sioux City (Bertolino) | 12-21K each |
| Seaboard Triumph Foods | Sioux City | 22K |
| Wholestone Farms | Council Bluffs | 9K (newer plant) |
| Hormel / QPP | Ottumwa, Force City, Burlington | 6-21K each |
| Iowa Premium Beef | Tama (beef but adjacent) | 1K beef |
| Pork by Sioux-Preme | Sioux Center | 5K |
| Smithfield (specialty) | Algona | smaller |

**Authoritative sources:**
- **USDA AMS Livestock Slaughter** monthly report
  (federal-inspected plants by state)
- **AMS National Daily Hog Report** (LM_HG201 — daily by region)
- **USDA FSIS** plant directory (food safety inspections)
- **National Pork Producers Council** member directory
- Each company's annual ESG / sustainability report (some
  publish facility-level capacity)

**Permit availability:**
- All major plants are Title V (large boilers, refrigeration
  systems, wastewater treatment all trigger thresholds)
- Iowa DNR Air Permits database — searchable
- Wastewater discharge permits also available (NPDES via EPA ECHO)
- USDA FSIS food-safety records public

**LLM extraction targets:**
- Daily slaughter capacity (head)
- Annual processing capacity (head/year, MT meat)
- Boiler / refrigeration fuel and capacity
- Cooler/freezer storage (sq ft, MT)
- Wastewater discharge volume (gpd)
- Cogeneration if any
- Connection to feed mills / further processing on-site
- Live animal procurement radius (impacts feed demand modeling)
- Year commissioned / last expansion
- Workforce size (proxy for capacity utilization)

### 2.5 Egg layer operations — Tier 2

**Why:** Iowa is the #1 egg-producing state with 50+ million laying
hens. Egg layers are large, concentrated CAFOs with significant feed
demand (corn + soybean meal). Highly relevant to soybean meal market
because layer flocks are stable demand vs. volatile broiler demand.
After 2022 HPAI outbreak, IA egg production dropped significantly
and tracks recovery now.

**Major operators in Iowa:**

| Operator | Approximate flock |
|---|---|
| Versova Holdings (largest IA layer) | 30+ million hens, multiple sites |
| Rembrandt Foods (now part of Versova) | major |
| Center Fresh Group | Sioux Center | major |
| Rose Acre Farms | Stuart, Winterset | medium |
| Sparboe Farms | New Hampton | medium |
| Hickman's Family Farms | smaller |
| Daybreak Foods | Lake Mills | medium |
| Farmers Hen House | Kalona, organic | smaller |

**Authoritative sources:**
- **United Egg Producers** member directory
- **USDA NASS Chickens and Eggs** monthly report (state totals)
- **Iowa Poultry Association** state directory
- **USDA APHIS HPAI tracker** (active depopulation events)
- **State CAFO permits** at IA DNR — every operation > 750 hens
  must be permitted

**Permit availability:**
- Iowa DNR CAFO database — public, searchable, contains animal
  unit counts and locations
- Air permits for layer barns (PM10, ammonia, hydrogen sulfide)
- NPDES wastewater for liquid manure systems

**LLM extraction targets per permit:**
- Hen capacity (peak, max permitted)
- Manure management system (dry vs. liquid)
- Feed mill on-site (proxy for soybean meal demand)
- Egg processing on-site
- Year commissioned / last expansion
- Distance to nearest crush plant (for meal sourcing)
- Distance to nearest river / rail (egg export logistics)

### 2.6 Beef packing — Tier 2

**Why:** Iowa is not a major beef state (Nebraska, Texas, Kansas
dominate), but Iowa has feedlot finishing and a few key plants.
Iowa Premium Beef in Tama is the major one for high-end cattle.
Beef packing is less critical to soybean dynamics than pork but
still drives feedlot demand, which drives feed grain.

**Major operators in Iowa:**

| Operator | Plant | Daily slaughter (head) |
|---|---|---|
| Tyson Foods | Dakota City NE/IA border | 6K (technically Nebraska) |
| Iowa Premium Beef | Tama | 1.1K (premium grade focus) |
| Greater Omaha Packing | Omaha NE | 2K (NE side, IA-relevant) |

**Note:** Major beef packers in NE/KS/TX have a much larger footprint
than IA. For Iowa-specific work, prioritize Tyson Dakota City + IA Premium.

**Authoritative sources:**
- **USDA AMS Cattle Slaughter** monthly report
- **AMS National Daily Cattle Report** (LM_CT169 — 5-area)
- **National Cattlemen's Beef Association** directory
- **Cattle Buyers Weekly** publication (paid)

**LLM extraction targets:** Same as pork packing.

### 2.7 Grain handling — Tier 2 (large fleet, lower individual leverage)

**Why:** Hundreds of country elevators dictate basis at the field
level. Each elevator has a draw radius and competes with adjacent
ones for farmer business. The basis differentials between elevators
are signal for the basis-field layer of the Market Field.

**Major operators in Iowa:**

| Operator | Type | Approx. IA elevator count |
|---|---|---|
| ADM | Country + river terminals | 30+ |
| Cargill | Country + river + rail terminals | 25+ |
| Bunge | Country + river terminals | 15+ |
| Andersons | Country | 10 |
| Landus Cooperative | Cooperative-owned | 50+ |
| Heartland Cooperative | Cooperative | 40+ |
| Mid-Iowa Coop | Cooperative | 25+ |
| New Cooperative | Cooperative | 60+ |
| West Central Cooperative | Cooperative (now Landus) | merged |
| MaxYield Cooperative | Cooperative | 25+ |

**Authoritative sources:**
- **USDA RMA Reinsurance Database** — every elevator with a
  CCC/USDA approved warehouse receipt is registered
- **USDA AMS Approved Storage Facilities** list
- **GIPSA (Grain Inspection)** licensed warehouse list
- **Iowa Department of Agriculture and Land Stewardship (IDALS)**
  — state-licensed grain dealer/warehouse list
- **Iowa Grain Quality Initiative** maintains a state directory

**Permit availability:**
- Air permits exist for grain handling (PM10 from dust)
- Title V threshold: only the largest elevators / terminals
- Most are state-only minor-source permits
- IDALS warehouse licenses public

**LLM extraction targets:**
- Storage capacity (bushels)
- Throughput per year (vs. capacity = turnover)
- Receipt mode (truck only vs. truck + rail vs. river)
- Rail siding (carloads/day, shuttle vs. unit-train)
- Drying capacity (bu/hr)
- Year built / expanded
- Specialty handling (organic, non-GMO, food-grade)

### 2.8 Pig finishing operations (CAFOs) — Tier 3 (long tail)

**Why:** Iowa has 22+ million hogs in production, ~3,000+ permitted
CAFO sites. Each is a feed-grain consumer. The long tail makes
individual modeling impractical, but aggregate by county is useful
for the basis-field layer.

**Major operators / integrators in Iowa:**

| Operator | IA hog count |
|---|---|
| Iowa Select Farms | 5+ million |
| Smithfield Hog Production | 1+ million |
| Christensen Farms | 1+ million |
| Pipestone Holdings | spread across MN/IA |
| Triumph Foods | integrator |
| Tyson Pork Group | integrator |
| Independent operations | balance |

**Authoritative sources:**
- **Iowa DNR CAFO database** — public, searchable, ~3,000 sites
  with animal unit counts and lat/lon
- **USDA NASS Hogs and Pigs** quarterly report
- **National Hog Farmer** publication for industry overview
- **Iowa Pork Producers Association** member directory

**Permit availability:**
- Iowa DNR Manure Management Plans (MMP) — every site > 1,250 head
- Air permits for sites > 5,000 head (PM10, NH3, H2S)
- NPDES wastewater for liquid systems

**LLM extraction targets:**
- Animal capacity (head) at peak
- Production type (farrow-to-wean, wean-to-finish, finishing only)
- Feed mill on-site (relevant for soymeal demand)
- Manure storage / land application coverage
- Distance to nearest packer (transport cost)
- Year permitted / last modification

### 2.9 Rail terminals and shuttle elevators — Tier 2

**Why:** Rail handles ~40% of Iowa grain export volume. Shuttle-
loading elevators (110-car unit trains) get pricing premiums and
dictate the supply chain back to producers. Rail terminal locations
are critical for the basis-field model.

**Major operators / locations in Iowa:**

| Terminal | Operator | Rail | Type |
|---|---|---|---|
| Cedar Rapids | UP / CRANDIC | UP main | grain/ethanol/manufacturing |
| Council Bluffs | UP / BNSF | UP main + BNSF | grain/coal/intermodal |
| Sioux City | UP / BNSF / CN | three Class 1 | grain/livestock/biofuels |
| Davenport / Quad Cities | BNSF | BNSF Chicago-Omaha | grain/intermodal |
| Fort Madison | BNSF | BNSF Transcontinental | grain/coal |
| Marshalltown | UP | UP main | grain/manufacturing |
| Boone | UP | UP main | grain/coal |
| Fort Dodge | UP / IAIS | UP/IAIS | grain |

**Shuttle elevators (110-car capable):**
~80+ shuttle facilities in Iowa, run by major Cs (ADM, Cargill,
Bunge, Andersons) and large coops (Landus, Heartland, NEW).

**Authoritative sources:**
- **USDA AMS Grain Transportation Report** — quarterly,
  rail-by-state data
- **STB (Surface Transportation Board)** — Class 1 rail filings
- **BNSF / UP** facility directories
- **Iowa Northern Railway** + short-line operator websites
- **Iowa DOT Freight Plan** identifies major terminals

**Permit availability:**
- Air permits for rail-served grain terminals (PM10)
- Title V for largest combined operations
- STB filings for rail capacity

**LLM extraction targets:**
- Loading capacity (carloads/day, shuttle-capable yes/no)
- Storage capacity served by rail (bushels)
- Class 1 connection (UP / BNSF / CN / NS)
- Receipt + ship modes (truck-rail, river-rail, all-three)
- Year expanded to shuttle
- Cargo mix (grain only vs. multi-commodity)

### 2.10 River terminals — Tier 2

**Why:** Mississippi and Missouri river barge terminals export
~30% of US grain. Iowa has terminals on both rivers; barge basis
sets the pricing floor for US Gulf export competitiveness.

**Major terminals in Iowa:**

| Terminal | River | Operator |
|---|---|---|
| Davenport | Mississippi | ADM, Cargill, Consolidated Grain & Barge |
| Muscatine | Mississippi | ADM, Bunge |
| Burlington | Mississippi | ADM, Big River Resources |
| Fort Madison | Mississippi | BNSF + barge |
| Keokuk | Mississippi | smaller |
| Council Bluffs | Missouri | various |
| Sioux City | Missouri | end of navigation |
| Dubuque | Mississippi | smaller |
| Clinton | Mississippi | ADM, Cargill |

**Authoritative sources:**
- **USDA AMS Grain Transportation Report**
- **US Army Corps of Engineers Lock and Dam reports**
- **Waterways Council** terminal directory
- **Mississippi River Commission** infrastructure reports
- **Inland Marine Service** publication

**Permit availability:**
- Air permits for grain handling at river terminals
- USACE permits for river infrastructure
- NPDES for stormwater discharge

**LLM extraction targets:**
- Barge loading capacity (bu/hr)
- Storage capacity at terminal (bushels)
- Receipt modes (rail + truck combinations)
- Lock/dam constraints upstream/downstream
- Multi-commodity handling
- Connection to country elevator network (gathering radius)

---

## 3. Schema additions needed

Current `reference.oilseed_crush_facilities` is industry-specific.
Multi-industry expansion needs a more general structure. Two
options:

**Option A — separate table per industry** (current pattern):
- `reference.ethanol_facilities`
- `reference.biodiesel_facilities`
- `reference.pork_packing_facilities`
- ... etc.
- Each table has industry-specific columns (capacity in MMGY vs. head/day)

**Option B — unified `reference.facility_master`** with industry-
specific extension tables joined on facility_id:
- `reference.facility_master` (id, name, lat, lon, county, state,
   operator, parent_company, industry_code, status, ...)
- `reference.facility_capacity_<industry>` for industry-specific
   metrics (mmgy, daily_head, bushels_storage, etc.)

**Recommendation: Option B.** The Market Field's facility-graph
machinery is industry-agnostic — give it one master table to query
and the network propagation generalizes. Per-industry capacity
tables hang off it. Queries like "all facilities within 50 mi of X
that consume corn" become trivial.

Migration order:
1. Create `reference.facility_master`
2. Migrate `oilseed_crush_facilities` rows to it (FK preserved)
3. Add per-industry capacity tables incrementally as we ingest
   each industry

---

## 4. Build sequencing

Sprint 5 (post Drew Lerner backfill):

1. **Schema migration** — `reference.facility_master` + 8
   per-industry capacity tables
2. **Ethanol seed** — populate ~40 plants from RFA directory
   + EPA RFS registry (machine-readable; ~30 min)
3. **Pork packing seed** — populate ~12 IA plants from public
   knowledge + USDA AMS Livestock Slaughter report
4. **Air permit collector for ethanol** — extend existing
   IA DNR permit pipeline to ethanol-specific extraction
   templates (per Section 2.2 LLM extraction targets)
5. **Repeat for biodiesel, then pork, then beef, then layers**

Sprint 6:

6. **Iowa DNR CAFO scraper** — bulk download of CAFO database,
   3,000+ sites, geocoded
7. **Grain handling (warehouse) seed** — IDALS license database
8. **Rail + river terminal manual list + air permits**

Sprint 7:

9. **Cross-industry network edges** — ethanol plant within X mi
   of CAFO consumes Y bu/year of corn; egg-layer farm within Z mi
   of crush plant has Z* meal demand
10. **Network propagation extended** to include cross-industry
    influence in the Market Field update equation

---

## 5. Per-industry permit-extraction prompt template

Use this with desktop LLM for bulk Title V permit parsing once we
have a corpus to feed it. Per-industry extraction targets are
defined in each section above.

```
You are a permit analyst extracting structured capacity data from a
Title V air permit issued by the Iowa Department of Natural Resources.

Industry of facility (provided): {industry}
Industry-specific extraction targets (provided): {targets}

For each target, return:
  - field_name
  - extracted_value
  - extracted_unit
  - source_page_number
  - source_passage (verbatim quote, max 300 chars)
  - confidence (0-1)

Return JSON only. If a target is not findable, set value=null
and confidence=0.
```

This template plugs into the existing local-LLM permit pipeline
(qwen3-coder:30b on the 5080) per memory notes. Per-industry
target lists are in Section 2 above.

---

## 6. Open questions

- Who owns the truck fleets in IA? Refrigerated trucking (cold
  chain meat), grain hauling, energy/fuels (ethanol terminal to
  blender), feed delivery — different operators per use case.
  Probably need a separate "carrier" reference table eventually.
- Pipeline infrastructure: Iowa has limited petroleum pipelines
  but new CO2 capture pipelines are being permitted. Capture in
  taxonomy when projects break ground.
- Co-located facilities: many IA sites have ethanol + biodiesel
  + crush combined. Schema must support multi-industry tagging
  per facility_id.
