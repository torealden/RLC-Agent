# RLC Deliverables — Helios AI × PepsiCo Engagement

**Status:** working inventory, v1 · drafted 2026-07-21
**Sources:** SOW No. 1 draft (`clients/Contracts/Helios/RLC_Helios_SOW1_PepsiCo_DRAFT.docx`),
Helios context doc (`Helios x Round Lake - Veg Oils Forecasting Context.docx`), 2026-06-29 meeting,
2026-07-13 weekly catch-up (`data/meeting_notes/RLC_Helios_Weekly_Catch-Up_Summary_2026-07-13.txt`),
sample report (`rlc_reports/RLC-Helios Sample.pdf`).

This is RLC's understanding of what we are on the hook to produce. Where the contract, the meeting
record, and the sample report disagree, that is called out rather than smoothed over.

---

## 1. The commercial frame

| | |
|---|---|
| **Counterparty** | Helios AI, Inc. — RLC contracts with Helios, not PepsiCo |
| **End client** | PepsiCo, Inc. + designated procurement/finance affiliates |
| **Delivery chain** | RLC → Helios → PepsiCo. Helios repackages into the client-facing surface |
| **Attribution** | **White-label from inception** (SOW §7.2). RLC attribution omitted from End-Client-facing deliverables |
| **Fees** | $5,000 onboarding (50/50) + $1,000/mo engine access (mo. 1–24, then $2,500) + $1,500/mo production |
| **Build-out target** | 6 weeks from SOW execution |

**The one commercial thing to keep front of mind:** white-label + "João explains to the client, not
RLC" means RLC is the invisible backend by design. That is efficient and it is the
disintermediation vector. The "Intel Inside" ambition only survives if attribution becomes a term
at the *next* SOW, not a hope.

---

## 2. Recurring deliverables (the standing obligation)

| # | Deliverable | Cadence | Format | Contract ref |
|---|---|---|---|---|
| D1 | **Weekly Vegetable Oils Report** | Weekly, Monday 12:00 ET | PDF | §3.1 |
| D2 | **Weekly Guidance-Price Data File** | Weekly, with D1 | XLSX or CSV | §3.2 |
| D3 | **Monthly Budget / NCI Planning Table** | Monthly, with first weekly of month | table in D1 or standalone | §3.3 |
| D4 | **Scenario Analyses** | Up to 2 requested/month + event-triggered at RLC discretion | in D1 | §3.4 |
| D5 | **Consultation Session** | 1 × 60 min/month, ≤5 participants, no rollover | video | §3.5 |

### D1 — Weekly Report, required contents
Per §3.1 and the format agreed on the 2026-07-13 call:
1. Executive summary up front
2. Forward-curve summary + market commentary, per complex
3. **Guidance Price** at the Decision Window, per complex
4. **Market-vs-guidance signal read**, per complex (§4.4)
5. Base forecast + scenario impacts, with **50% and 90% confidence bands**
6. Notable supply / policy / trade developments by origin
7. "What we're watching" commentary
8. Buyer-speak framing of coverage decisions

*Removed from the sample and parked:* the versus-budget chart (forecast cost vs Pepsi's budgeted
cost) — reinstate only if Pepsi shares budget figures.

*Not RLC's:* the **supply-shock assessment** — Helios supplies and integrates that. RLC provides
essentially everything else.

### D2 — Data file contents
Guidance Prices, reference-series values, forward-window identifiers, signal indicators, per
complex. Schema agreed during build-out.

### D3 — Budget / NCI table
Guidance-vs-budget tracking for the complexes designated for budget/NCI planning (palm, rapeseed,
sunflower, soybean oil — **not corn oil**). Requires Pepsi budget inputs through Helios; absent
those, guidance-only tables ship instead. That fallback exists specifically to stop a dependency
stall — do not let the table slip waiting on Pepsi.

---

## 3. Scope — complexes, origins, use cases

Per SOW §2:

| Complex | Origins in scope | Use cases |
|---|---|---|
| **Palm oil** | Malaysia; Indonesia; + Colombia, Guatemala, Mexico (Americas) | Forecast · scenarios · budget/NCI · **buying playbook** |
| **Rapeseed / canola oil** | EU; Russia; Turkey; Canada; Brazil | Forecast · scenarios · budget/NCI · **buying playbook** |
| **Sunflower oil** | EU; Russia; Ukraine; Turkey; Argentina | Forecast · scenarios · budget/NCI |
| **Soybean oil** | United States (CBOT); Brazil; Argentina | Forecast · scenarios · budget/NCI |
| **Corn oil** | Mexico; Brazil | Forecast · scenarios |

Scenario-only extra origins: **Colombia** (palm, sunflower) and **Mexico** (all five).

Note the use-case ladder: only palm and rapeseed carry the full four including **buying playbook**.
Sunflower and soybean oil stop at budget/NCI. Corn oil is forecast + scenarios only. Don't build
playbook machinery for complexes that don't carry the obligation.

---

## 4. Analytical specifications RLC must satisfy

**Decision Window (§4.1).** ~8 months forward, ±2. Every guidance price and signal read is produced
**on the forward window Pepsi would actually transact — never spot.** This is the single most
important spec in the document; a spot-anchored number is a wrong answer regardless of accuracy.

**Guidance Price (§4.2).** RLC's informed estimate of where the market trades over the horizon,
against the applicable reference series, updated weekly. Feeds both the timing signal and the
finance read.

**Signal Framework (§4.4).** Asymmetric by construction because Pepsi buys and never sells:
- Market **below** guidance → conditions favor extending coverage beyond ordinary cadence
- Market **above** guidance → hold to the playbook, add nothing optional

Framed as decision support, never a recommendation to transact (§10.4, §11.5).

**Scenario models (§4.3)** — three to be built and back-tested during build-out:
1. Supply shock (production, weather, logistics, by origin)
2. Trade policy (export restrictions, duties, levies, mandate changes)
3. Macro / energy (energy-complex linkage, currency)

Agreed direction from 2026-07-13: **start from a finite curated list of disruptive events**, show it
to Pepsi, expand on their reaction — do not offer open-ended scenario building. Scenarios run
individually first; **stacking** comes later, but design the data contract for stacking now.
Cross-commodity knock-on (sunflower shock → soybean oil substitution) is in scope and RLC confirmed
it can simulate it.

---

## 5. Reference series and forward-window conventions (§5)

| Complex | Reference series | Forward window |
|---|---|---|
| Rapeseed / canola oil | RSO FOB Dutch / EU ex-mill | ~12–13 months forward |
| Sunflower oil | Sunoil "six ports"; FOB Argentina | ~6–7 months forward |
| Palm oil | CPO CIF Rotterdam; FOB Malaysia | per prevailing quotation |
| Soybean oil | CBOT BO, nearby + deferred | exchange contract months |
| Corn oil | **TBD — RLC to propose candidates for MX and BR** | TBD |

### ⚠️ Reference-series / citation conflict — needs resolving in build-out
SOW §9 limits public-facing citations to **government and exchange sources only** (USDA, EIA, CFTC,
CME/Bursa/ICE, EC/Eurostat, StatCan/AAFC, CONAB/ABIOVE, INDEC, MPOB).

But three of the five reference series — **RSO FOB Dutch, sunoil six ports, CPO CIF Rotterdam** —
are **private price assessments** (Fastmarkets / Argus / Platts class). We cannot republish or
redistribute them, and internal policy is that Fastmarkets data never goes client-facing.

The clean resolution, and what should be written into the build-out record:
- RLC **forecasts to** those series but **publishes only its own guidance number** plus the delta to
  whatever market level Pepsi/Helios supplies.
- The market-level column in D1/D2 is an **input supplied by Helios or Pepsi**, not RLC-published
  data.
- CBOT BO is the only reference series RLC can publish outright (exchange source, §9-approved).

This is not a nitpick — it determines the schema of the weekly data file (D2) and who owns the
"market" column. Resolve before the pilot report, not after.

---

## 6. Build-out milestones (§6) and completion weights

| # | Milestone | Weight |
|---|---|---|
| 1 | Data infrastructure + source onboarding for the complexes/origins | 20% |
| 2 | IFV engine calibration for each of the five complexes, validated vs reference series | 25% |
| 3 | The three scenario models, back-tested and documented | 30% |
| 4 | Reference-series mapping + forward-window alignment; guidance-price framework at the decision window | 15% |
| 5 | Pilot weekly report + data file delivered, format accepted | 10% |

Milestones 1–2 = 45%. Completing 3 crosses the >50% test in Agreement §4.9. That sequencing is
deliberate; it also means **milestone 3 (scenarios) is the largest single block of work** and is
where the schedule will actually be won or lost — not on the balance sheets.

---

## 7. Two-way data — what RLC owes and receives

**RLC → Helios**
- USDA production maps per commodity/country (starting US) + the ag-district map, with lat/long
  boundaries or shapefiles — *outstanding action from 2026-07-13*
- Demand-side / IFV feed into Helios's models
- Exploration of a **structured JSON/CSV export of week-over-week fundamental changes** driving the
  report narrative, for Helios's LLM narrative layer — *outstanding, and honestly scoped: RLC does
  not produce a discrete weekly input dataset; the "dataset" is the full S&D model complex with
  analyst adjustments that don't nest cleanly. What is feasible is a machine-readable duplicate of
  the "what changed" narrative, not the model internals.*

**Helios → RLC**
- Climate index API access (Tore onboarded at 100% discount): 4 dimensions (too wet / dry / hot /
  cold), 0–100, at farm / regional (≈state) / country / global levels; regional→country weighted by
  production, country→global by export share. 5 years of history now, 10 available on request.
- Coverage verification that every RLC ag district has Helios coverage, 2–3 points per district
- Supply-shock assessment component of the weekly report
- Discovery session with Helios data science on index construction

**Recalibration reality:** RLC's scenario layer is calibrated to classical inputs (rainfall,
temperature, soil moisture). Moving it onto a 0–100 composite takes time but is not a rebuild.
Tore's archive of historical commentary (2012 drought, 2019 wet year) is the validation asset — and
a plausible joint case study.

---

## 8. Closed / illiquid markets

**US distillers corn oil** is the primary case: no futures, no consistent cash quote. RLC delivers
the **implied maximum price a biofuel producer could pay** (IFV), with supporting series built
underneath. Occasional Pepsi supplier quotes would help calibrate but are not required — start
without them and let Pepsi true up.

This is also the honest answer for the **corn oil reference series (§5, TBD)**: there probably isn't
a liquid Mexico or Brazil benchmark to find. The proposal should be an RLC-constructed indicative
series, labeled as such, exactly as the sample report already does ("HOLD (PROV.) — provisional
pending series buildout").

---

## 9. ⚠️ Wheat — in the meeting record, NOT in the contract

**The problem.** Wheat appears in the 2026-07-13 meeting notes as a live workstream: Helios has not
split wheat into subclasses (spring / winter / durum), Dominic and Nathan are doing discovery on
whether a front-end field split avoids data-science work, and it is **flagged as a gate that must
close before any wheat deliverable ships**. Corn is "ready now."

But **wheat and corn appear nowhere in SOW No. 1.** The SOW is five vegetable-oil complexes. The
context doc is veg oils. The sample report is veg oils, 7 pages, five oils.

So one of these is true and it needs a decision, not an assumption:
- **(a)** Wheat/grains are a *second* SOW or a change order under Agreement §2.5 — priced separately;
- **(b)** SOW No. 1 gets amended before execution to add a grains section with its own origins,
  reference series, and forward-window conventions;
- **(c)** Wheat is exploratory Helios-side interest that RLC should not be building against yet.

**Recommendation: (b) if it goes in the same weekly report, (a) if it ships separately.** What is
not acceptable is building wheat coverage into the deliverable while the SOW prices five oils —
that is unpaid scope on a $1,500/month production fee, on a commodity group whose balance-sheet
build is as large as any single oil complex.

### If wheat proceeds — the classes that actually matter to Pepsi

PepsiCo's wheat draw is **biscuit, cracker, and snack flour** (Gamesa in Mexico is the clearest
case), plus regional bread/tortilla flour. That basket is dominated by **soft, low-protein wheat** —
not the hard bread wheats that dominate global trade headlines, and almost certainly **not durum**
(pasta, which is not a meaningful Pepsi category). Building general "wheat" price forecasts, or
following Helios's spring/winter/durum split, would forecast the wrong thing.

Proposed class-and-market matrix — **this is RLC's read, and should be put to Pepsi through Dominic
for confirmation before any of it gets built:**

| Class | Reference series | Origin / market | Why it's in Pepsi's basket |
|---|---|---|---|
| **US Soft Red Winter (SRW)** | CBOT wheat (ZW) — SRW is the deliverable grade | US, exports to LatAm | The core cookie/cracker/snack flour wheat; also the only exchange-quoted, §9-citable wheat series |
| **US Hard Red Winter (HRW)** | KC wheat (KE) | US → **Mexico** (largest US HRW buyer) | Tortilla and bread flour for the Mexico snack/bakery footprint |
| **US Soft White** | no futures — PNW cash | US PNW → Asia, LatAm | Cake/cracker flour; low protein; export-oriented |
| **EU soft milling wheat** | Euronext / MATIF milling wheat (EBM), €/t | EU (FR/DE/PL) | European biscuit and snack flour; exchange-quoted, §9-citable |
| **Black Sea milling wheat, 12.5%** | FOB Novorossiysk — **private assessment, same §9 problem as the oils** | Russia → Turkey, Egypt, MENA | Sets the global floor; Turkey is already an origin in scope for two oil complexes |
| ~~Durum~~ | — | — | **Recommend excluding.** Pasta-driven; not a Pepsi category. Helios raised it; that is Helios's product surface, not Pepsi's basket |

Note the pleasant coincidence: **CBOT ZW, KC KE, and MATIF EBM are all exchange series** and
therefore citable under §9 — wheat is actually *cleaner* on the citation problem than the oils are.

**Sample-report implication.** The current 7-page sample has no wheat. Adding it means either a
6th and 7th module in the same structure (fundamental support ×3, scenario table, IFV build) or a
separate grains section. Do not add it until the scope question above is answered — the report is
the artifact Pepsi reacts to, and putting wheat in it is the fastest way to make unpaid scope
permanent.

---

## 10. Outstanding actions carried forward from 2026-07-13

**RLC**
- [ ] Send USDA production maps per commodity/country (start US) + ag-district map with lat/long or shapefiles
- [ ] Send AI note-taker output to the Helios team
- [ ] Send materials to Francisco *(promised "within a day or two" as of Jul 13 — now ~8 days overdue; check whether this went out)*
- [ ] Explore structured JSON/CSV export of week-over-week fundamental changes
- [ ] Begin recalibration planning: scenario logic onto the Helios 0–100 composite
- [ ] Once API access is live, run the Helios index through RLC models; share at "the 10th version, not the 1st"
- [ ] Confirm Thursday availability from Dominic's slots

**Helios**
- [ ] Account + API key at 100% discount (Eden/Dominic)
- [ ] Platform/API walkthrough slots (Dominic)
- [ ] One more push on Pepsi's internal price series — low expectations, especially LATAM
- [ ] **Wheat subclass discovery — the delivery gate** (Dominic/Nathan)
- [ ] Data-science discovery session on index construction (Eden)
- [ ] Verify climate coverage for all RLC ag districts once maps arrive
- [ ] Detailed sample-report feedback against Pepsi use cases (Dominic)

**Parked / ready differentiators**
- Versus-budget module — reinstate if Pepsi shares budget figures
- Scenario dropdown UI with stacking — mirrors the IFV policy-slot pattern
- Supplier quotes for DCO calibration — low-burden ask once trust is established
- Historical validation of the Helios index against Tore's commentary archive — joint case study

---

## 11. Honest read on the whole package

Three things will decide whether this engagement works:

1. **The scenario models (milestone 3, 30% weight) are the critical path**, not the balance sheets.
   Balance sheets are known work with a known method; three back-tested scenario models on five
   complexes in six weeks is the part that is genuinely hard.
2. **The reference-series citation conflict (§5 above) is unresolved and structural.** It changes
   the data-file schema and who owns the market column. It will not resolve itself.
3. **Scope is already leaking** — wheat and corn are in the meeting record and not in the contract,
   and the coverage tracker is missing two of the five contracted complexes (palm, corn oil). Both
   directions of drift need closing before build-out starts, or the six-week milestone is fiction.
