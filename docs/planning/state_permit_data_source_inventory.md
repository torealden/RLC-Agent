# State / Country Permit Data-Source Inventory

**Started 2026-06-21.** A living artifact: for each jurisdiction, where and how to obtain
facility-level air-permit DEPTH (equipment lists + capacity), and how hard it is. Keyed
to the company-network workflow — when we hit a multi-state operator (e.g. Cargill: ~13
states + 4 countries), we look up each jurisdiction here and instantly know whether it's
an IA-type (easy) or MN-type (FOIA) source and what the lead time is.

## The two layers (important)

- **Federal = the CENSUS (who/where/what). Already done.** EPA ECHO (`bronze.epa_echo_facility`)
  is the canonical facility source — 2,865 facilities across 53 jurisdictions, 1,925
  operating, industry-tagged via `search_profile`, with `caa_permit_ids` (Clean Air Act
  handles), SIC/NAICS, operating status, lat/long. **We do NOT need state portals to find
  facilities.** See `reference_echo_canonical_facility_source` memory. No more hand-curated
  facility lists (the IDEM Indiana dump was deleted 2026-06-21 as redundant to ECHO).
- **State = the DEPTH (equipment lists + capacity).** ECHO has no equipment lists. The
  Title V / minor-source operating-permit PDFs do. This inventory is about sourcing that
  DEPTH per state. Only IA done so far (288 facilities / 8,847 units).

## Access tiers

- **Tier 1 — bulk/programmatic:** permit docs via bulk download or predictable URL/dataset/API.
  Scrapable end-to-end. **Reference: Iowa DNR** (what we drained).
- **Tier 2 — online search portal:** public search/DB, download permit PDFs one at a time.
  Scrapable with more effort; no records request.
- **Tier 3 — FOIA / manual request:** documents not online; must file a public-records
  request. Long lead time → start the clock EARLY and batch (see method below).

## Sequencing method (the plan, refined with Tore 2026-06-21)

1. **Drive off ECHO**, not curated lists. ECHO already enumerates target facilities per state.
2. **Sequence by value × ease, not ease alone.** A high-value facility in a Tier-3 state
   still gets pulled early — start its FOIA clock in the background while scraping Tier-1/2
   states. FOIA latency is dead time to run in parallel, not a reason to defer the facility.
3. **One FOIA request per Tier-3 state, complete.** Because ECHO enumerates all our target
   facilities in a state up front, assemble a SINGLE comprehensive records request covering
   all of them — no five-round-trip back-and-forth. (MN is the suspected archetype here.)
4. **Company-network is the unit of completeness.** When modeling a multi-state operator,
   pull all its nodes (incl. out-of-state) so the company is whole — look each state up here.
5. **International:** inventory country sources only when a company network forces it; do
   NOT speculatively inventory all countries. Model the US first; export data supplies the
   international demand signal (see `us_model_completion_plan.md`).

---

## US state table

Facility counts from `bronze.epa_echo_facility` (2026-06-21), `n` = total / `op` = operating.
Access tier + portal columns populated by research pass (in progress 2026-06-21).

| State | ECHO n/op | Tier | Agency / portal | Notes |
|---|---|---|---|---|
| IA | 158/127 | **1 — DONE** | Iowa DNR permit document repository | Drained: 288 facilities, 8,847 units. Reference implementation. |
| IL | 281/143 | _TBD_ | | highest facility count |
| TX | 244/208 | _TBD_ | | |
| LA | 181/117 | _TBD_ | | |
| NJ | 137/119 | _TBD_ | | high count likely blending terminals |
| GA | 131/62 | _TBD_ | | |
| KS | 112/62 | _TBD_ | | |
| MO | 106/56 | _TBD_ | | |
| NE | 106/48 | _TBD_ | | ethanol-dense |
| PA | 94/64 | _TBD_ | | |
| IN | 93/66 | _TBD_ | | next deep-dive target |
| OH | 83/71 | _TBD_ | | |
| TN | 78/49 | _TBD_ | | |
| CO | 73/20 | _TBD_ | | |
| AL | 73/55 | _TBD_ | | |
| CA | 66/50 | _TBD_ | | LCFS-relevant (RD/SAF) |
| MN | 62/44 | _TBD (suspect 3)_ | MPCA | suspected FOIA-type — verify |
| AR | 61/41 | _TBD_ | | |
| NC | 57/38 | _TBD_ | | |
| NY | 55/45 | _TBD_ | | |
| SC | 50/34 | _TBD_ | | |
| KY | 49/40 | _TBD_ | | |
| MI | 48/41 | _TBD_ | | |
| VA | 41/29 | _TBD_ | | |
| OK | 37/20 | _TBD_ | | |
| FL | 36/17 | _TBD_ | | |
| WV | 34/27 | _TBD_ | | |
| MS | 32/19 | _TBD_ | | |
| MA | 32/25 | _TBD_ | | |
| SD | 29/22 | _TBD_ | | ethanol |
| WI | 21/16 | _TBD_ | | |
| CT | 20/17 | _TBD_ | | |
| MD | 20/16 | _TBD_ | | |
| ND | 17/14 | _TBD_ | | RD/crush growth |

**Long tail (<17 facilities, classify in a later pass):** WA 16, NM 15, PR 14, OR 13,
ID 11, DE 10, UT 10, WY 10, RI 8, ME 8, AZ 7, HI 6, MT 5, AK 4, NH 4, VI 3, NV 2, GU 1, DC 1.

---

## Country table (populate only when a company network forces it)

| Country | Trigger company | Source | Tier | Notes |
|---|---|---|---|---|
| _(none yet — add when a multi-national network is modeled, e.g. Cargill's 4 countries)_ | | | | |
