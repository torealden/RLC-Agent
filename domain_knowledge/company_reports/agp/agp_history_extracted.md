# AGP — Cooperative History & Plant Lineage

> Extracted 2026-05-10 from Shurtleff & Aoyagi (2021), *History of
> Cooperative Soybean Processing in the United States 1923-2021*,
> SoyInfo Center, 410 p. (free PDF, ISBN 978-1-948436-60-1). The
> SoyInfo book itself draws on Margaret Finnerty's 1992 *Soybeans,
> Cooperatives and Ag Processing, Inc.* (Heritage Publishers, 178 p.,
> ISBN 0929690141 — out of print, used copies via AbeBooks).
> Source PDF cached at
> `domain_knowledge/company_reports/agp/soyinfo_cooperative_soybean_processing_1923_2021.pdf`.

## Founding (one-liner)

**Ag Processing Inc a cooperative ("AGP")** was formed on **March 7,
1984**, when **Boone Valley Cooperative Processing Association**
(Eagle Grove, Iowa, founded 1944) absorbed five cooperative crush
plants from Land O'Lakes and Farmland Industries in the August 31,
1983 mega-merger and renamed itself. HQ: Omaha, Nebraska.

## Why AGP exists (the 1982-83 consolidation story)

By the early 1980s, most US cooperative soybean crushers were
struggling. **Bill Lester** — a long-time co-op processor — joined
Boone Valley as GM in May 1982 with Farmland's blessing and started
advocating consolidation. A 1982 study led to the August 31, 1983
deal: Boone Valley bought five plants from Land O'Lakes (Sergeant
Bluff IA, St. Joseph MO, Van Buren AR) and Farmland Industries
(Sheldon IA, Dawson MN). On October 1983, **James Lindsay** (former
ADM VP) was named GM/CEO of the combined entity. Renamed AGP March 7,
1984.

## Per-plant lineage map (matched to our DB)

| Our facility_id | Original founder | Year | Path to AGP |
|---|---|---|---|
| `ia.agp_eagle_grove` | Boone Valley Cooperative Processing Association | 1944 Mar | Founder → became AGP itself 1984. Plant destroyed by fire 1947 Aug 23, rebuilt. **AGP's "home" plant.** |
| `ia.agp_sheldon` | Cooperative (one of 7 IA original cooperative crushers, 1945 era) | by 1945 Feb | Farmers Regional Cooperative (Big 4 Div., Fort Dodge IA) ran it; later Farmland; → AGP 1983-08-31 merger |
| `mn.agp_dawson` | Tri-Country (Tri-County) Soy Bean Co-operative Association | 1951 Nov | Renamed Dawson Mills 1969 → Land O'Lakes Soybean Division 1980-03-01 → Farmland → AGP 1983-08-31. Joe Givens was first GM (1952). Dawson Food Ingredients (soy isolate plant) was a 1974-1981 spinoff that LoL closed and sold to AMPI Aug 1981. |
| `mo.agp_st_joseph` | Dannen Mills | pre-1963 | CMA (Consumers Marketing Association KC MO) buys Dannen Mills 1963 Sept → Far-Mar-Co 1968 → Farmland 1977 May 2 → Land O'Lakes briefly → AGP 1983-08-31 |
| `ia.agp_sergeant_bluff` | Farmland Industries | 1975 Aug | Farmland → Land O'Lakes (briefly via 1983 deal mechanics) → AGP 1983-08-31. **First plant to make biodiesel** (SoyGold brand) — started 1996 Nov. Soy methyl ester expansion 2017. |
| `ia.agp_manning` | AGRI Industries Inc (Iowa) | (existing plant) | AGP buys from AGRI Industries 1985-12-31 |
| `ia.agp_mason_city` | AGRI Industries Inc (Iowa) | (existing plant) | AGP buys from AGRI Industries 1985-12-31 |
| `ia.agp_emmetsburg` | **Built by AGP** | 1996-97 | Construction began 1996; **first plant AGP built from scratch** (vs. acquired). Opened Oct 1997, ribbon cutting Sept 17 1997. |
| `ne.agp_hastings` | **Built by AGP** | 1995-99 | Corn processing/ethanol plant came online Nov 1995. Soy crush plant began operating June 1999. **First farmer-owned soybean processing plant in Nebraska.** **Westernmost soybean processing plant in US.** AGP's 9th soybean plant. Has *two solvent extraction plants side by side* in 2021. |
| `sd.agp_aberdeen` | **Built by AGP** | 2017-20 | Ground breaking 2017-05-03. AGP's 10th soybean processing plant. ~25 miles from ND border. |

## CORRECTIONS our database needs (this is the entire calibration value of doing this)

### 🔴 ia.agp_algona is NOT a crusher

Our DB: `nameplate_mmbu_yr = 46` (we set this in mig 064 as
"public_knowledge" pending verification).

**Source says:** AGP acquired the **former East Fork Biodiesel LLC**
plant near Algona in **July 2011**. It is **biodiesel-only, ~60 mgy**,
no soybean crush. Per AGP's own communications: "AGP's operations
today consist of three biodiesel production plants (Algona and
Sergeant Bluff, Iowa, and St. Joseph, Missouri) with a combined
annual capacity of 175 million gallons."

**Action:** zero out `nameplate_mmbu_yr`, set `biodiesel_capacity_mgy
= 60`, predecessor = East Fork Biodiesel LLC (idle prior to 2011
acquisition).

### 🔴 ne.agp_david_city does NOT exist

Our DB: `ne.agp_david_city` with `nameplate_mmbu_yr = 50`,
`refining_capability = 'Degumming'`.

**Source says:** **zero mentions of David City in 410 pages.** AGP's
only Nebraska plant is Hastings. The 2021 plant inventory in the
book lists: "Iowa (multiple), Minnesota (Dawson), Missouri (St.
Joseph), Nebraska (Hastings), South Dakota (Aberdeen)."

**Action:** mark `is_canonical=FALSE`. The David City NE row is
likely a different operator (possibly historical AGRI Industries or
Cooperative Producers Inc) misattributed to AGP. Needs separate
research before deletion — there genuinely is/was a crush plant in
David City NE.

### 🔴 ia.agp_mason_city biodiesel attribution is wrong

Our DB (mig 064): `biodiesel_capacity_mgy = 30` "co-located per AGP
public; PENDING VERIFICATION."

**Source says:** the only Mason City + biodiesel reference is
"Freedom Fuels, LLC, Mason City... 30,000,000" — a **separate
company's** 30 mgy biodiesel facility, not AGP's. Mason City crush
came from AGRI Industries 1985-12-31, no biodiesel ever associated.

**Action:** zero out `biodiesel_capacity_mgy`. Note the prior 30 mgy
was Freedom Fuels LLC — possibly co-located, possibly a separate
plant in the same town.

### 🟡 mo.agp_st_joseph IS a biodiesel plant (we missed)

Our DB: no biodiesel attribution.

**Source says:** St. Joseph is one of AGP's three biodiesel
production plants. Combined Algona + Sergeant Bluff + St. Joseph =
175 mgy. If Sergeant Bluff = 60 mgy and Algona = 60 mgy, St. Joseph
≈ 55 mgy.

**Action:** add `biodiesel_capacity_mgy ≈ 55` (or refine when we
have a per-plant breakdown — this is a triangulation, not a
direct quote). Tag as PENDING VERIFICATION with AGP.

### 🟡 ar.ag_processing_van_buren — likely closed

Van Buren AR was Consumers Cooperative's second crush plant (Oct
1959) → Farmland → AGP via the 1983 merger. **No mentions of Van
Buren in any AGP plant lists post-1983.** Most likely closed
sometime between 1984 and 2002 (Farmland's bankruptcy era was hard
on co-op processing). We already marked it `is_canonical=FALSE` in
mig 064.

## Predecessor cooperatives that no longer exist as independent entities

These show up as historical operators tied to AGP plants. Worth
KG nodes because they appear in older filings, news archives, and
permits:

- **Boone Valley Cooperative Processing Association** (Eagle Grove
  IA, 1944) — became AGP itself
- **Tri-County Soy Bean Co-operative Association** (Dawson MN, 1951)
  — renamed Dawson Mills 1969
- **Dawson Mills** (Dawson MN, 1969-1980) — to Land O'Lakes
- **Land O'Lakes Soybean Division** (1980-1983) — divested to AGP
- **AGRI Industries Inc** (Iowa) — owned Manning IA + Mason City IA
  before 1985, sold to AGP 1985-12-31
- **Dannen Mills** (St. Joseph MO, pre-1963) — owned by Dannen family,
  bought by CMA 1963
- **CMA / Consumers Marketing Association** (Kansas City MO, 1963-68)
  — became part of Far-Mar-Co
- **Far-Mar-Co Inc** (Hutchinson KS, 1968-77) — merged into Farmland
  May 1977; later 3 employees bought it back via leveraged buyout in
  1983 and renamed it PMS Foods Inc
- **Farmland Industries Inc** (Kansas City MO) — was Consumers
  Cooperative Association before 1966-09-01 rename. Filed Ch 11
  bankruptcy 2002-05-31.
- **East Fork Biodiesel LLC** (Algona IA) — went idle, sold to AGP
  July 2011

## Leadership chronology

- **Joe Givens** — first GM of Dawson Mills, Jan 1952
- **Bill Lester** — old-time co-op processor; GM at Boone Valley
  May 1982 (drove the consolidation talks)
- **James Lindsay** — first GM/CEO of new (post-1983) Boone Valley,
  Oct 1983; former ADM VP
- **Marty Spackler / J. Keith Spackler** — referenced as CEO around
  Aberdeen ground-breaking (2017) and as predecessor in 2021
- **Chris Schaffer** — current CEO and General Manager (since
  2021-08-01)

## AGP's strategic shape, 2021

- "Largest farmer-owned soybean processor in the world"
- "Roughly the fourth-largest soybean processor in the US based on
  capacity"
- Members: 302 local + 12 regional cooperatives, ~300,000 farmers,
  16 states + Canada
- 5.5+ million acres of members' soybeans/year (per Dun & Bradstreet)
- Motto: "Partners in food production"
- $1.1 billion paid back to members in cash over 35 years (per Bill
  Lester, 2021-11-24)

## Other AGP business segments (not in our facility DB)

- **Vegetable oil refineries** — Eagle Grove (built late 1990s),
  Denison TX (purchased March 1991 from Conway Oil)
- **Port of Grays Harbor, Aberdeen WA** — export terminal for
  soybean meal to Pacific Rim, opened Nov 2003. Not the same as
  Aberdeen SD!
- **Pelleting projects** at Dawson MN and St. Joseph MO (1996 era)
- **AminoPlus** and **SoyGold** — branded products
- **ProAgro** — joint venture in Venezuela (1996 era)

## Strategic note for the calibration conversation

This book is itself a calibration source — the public-knowledge
record of AGP's plant lineage. When we present the FIC packet to
Courtney Lawson, the sequence to walk through:

1. "Here's what we have for AGP (12 canonical plants)."
2. "Here's what *public records* say about each plant's origin
   (this document)."
3. "Here are the gaps where we don't match — Algona is biodiesel,
   not crush; David City may not be AGP at all; Mason City
   biodiesel is Freedom Fuels not AGP; St. Joseph biodiesel we
   missed; Van Buren we think is closed."
4. "Are we right? What else are we missing?"

That framing is honest — "we used public knowledge, where are we
wrong" — and gets the conversation onto the substance.

## Replicable pattern for ADM, Cargill, etc.

The user noted Cargill and ADM probably have published histories
too. Confirmed:
- **Cargill: Trading the World's Grain** (Wayne Broehl Jr., 1992) +
  later Broehl volumes through 2000s — the canonical Cargill history
- **Cargill: Going Global** (2008) — by Charles Boucher, covers
  1973-2003
- **ADM** — less hagiographic; *Rats in the Grain* (James Lieber,
  2000) covers the 1990s lysine price-fixing era. *Supermarket to
  the World: The Story of Archer Daniels Midland* (Mick Andic, 1999)
  is more flattering. Plus extensive business-press archives.

The SoyInfo Center has 200+ similar free annotated bibliographies
indexed at `https://www.soyinfocenter.com/books/` — one per major
soy/cooperative-processing topic. Worth scraping the index when we
turn to ADM (Decatur etc.) or Bunge.
