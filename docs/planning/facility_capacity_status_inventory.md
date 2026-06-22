# Facility Data: the two signals we actually need (capacity + operating status)

**2026-06-22.** Reframe (Tore): the only two facts we need per facility are **capacity**
and **operating status**. The permit equipment-list is one *means* to those (it confirms
capacity, distinguishes operating from shuttered) — not the goal. So before grinding 50
state permit portals, inventory what we already have for those two signals. Answer: **we
mostly have them, nationally, from cleaner sources than permits.**

## Operating status — ~SOLVED nationally (EPA ECHO)
`gold.facility_capacity` (= 2,870 ECHO facilities) carries `operating_status` on **2,864
(99.8%)**: 1,895 Operating, 898 Permanently Closed, 15 Temp Closed, 17 Planned, 8 Under
Construction. National, free, already collected. Quality caveat: ICIS status can lag; the
CARB-pathway-absence closure signal (`silver.facility_carb_status`, cf. REG Ralston/Madison)
is a cross-check for biofuels.

## Capacity — already covered for the CORE sectors (curated national plant lists, not permits)
| Source table | Facilities | Has | 
|---|---|---|
| `reference.ethanol_facilities` | 191 (24 states) | nameplate_mmgy, status, feedstock, RIN d-code, eia/rfs ids |
| `reference.oilseed_crush_facilities` | 137 (25 states) | nameplate bpd/tpd/mmbu_yr, refining, NOPA, eia_plant_id, draw radius, crush_model path |
| `reference.renewable_diesel_facilities` | 66 | nameplate_mmgy (+ Jacobsen/JPMorgan), feedstock, technology, status |
| `reference.biodiesel_facilities` | 192 | nameplate_mmgy |
| **Total** | **586** | **capacity + status + provenance** |

Sourced from RFA/EIA/NOPA/Jacobsen-type industry lists. Only **29** facilities had
permit-derived capacity (IA 17 + NE 12) — confirming permits have barely contributed to
capacity, and don't need to.

## State coverage (eth / crush / RD plants with capacity) — the quick inventory
IA 43/34/1 · NE 24/7/1 · IN 14/14/1 · MN 19/9 · IL 14/12 · SD 14/3 · KS 9/5/3 · WI 9/4 ·
OH 7/6 · ND 5/5/2 · MO 5/6 · MI 5/4 · CA 4//4 · TX 4//2 · LA //1/5 · AR /5 · NC 1/4 · CO 4.
(Corn Belt + biofuel states well-covered; RD concentrated LA/CA/KS/ND/TX.)

## Implication — the permit grind is ENRICHMENT, not the critical path
The "what if every state is a mess like IDEM?" worry is largely defused: we do NOT need to
crack 50 state portals to get capacity + status. We have both for the sectors that matter.
What permits add (real, but not gating):
1. **Confirmation/refinement** of capacity vs the curated nameplate.
2. **Granular equipment detail** for detailed facility economic models (unit-level).
3. **Human expert examination** — a permit PDF holds far more than the LLM extracts into the
   equipment schema (actual-vs-permitted capacity, debottlenecking/expansion provisions,
   fuel-switching capability, control devices, monitoring/operating-hour limits, enforcement
   history, co-location clues, feedstock flexibility). Tore's commodity expertise pulls
   economic implications the LLM can't. The archive has standalone value for expert review.
4. **Relationship inference** — as equipment lists accumulate, corporate-owner / similar-
   facility patterns let us template a plant's likely equipment. Enrichment on top of the base.

## Recommended next step (unblocked, high-value)
Stop treating per-state permit portals as the critical path. Instead:
- **Unify** the curated capacity lists + ECHO status into one facility master (reconciliation
  / xref — `silver.facility_external_xref`/`facility_frs_xref` exist for this). The lists and
  ECHO are separate ID spaces; matching them is the real remaining work, and it directly feeds
  the economic model. This is a matching job, not a scraping job.
- **Acquire permits opportunistically** where clean (PA open directory) or surgically for
  high-value facilities where equipment detail / expert examination pays — never as the gate.
- **The crush economic model can be built NOW** on the 137 crush facilities (capacity+status
  in hand) + the 288-facility IA permit depth we already drained.

See `state_permit_data_source_inventory.md` (access tiers) and `us_model_completion_plan.md`.
