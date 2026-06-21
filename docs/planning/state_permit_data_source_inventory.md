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

## ⚠️ CRITICAL FINDING (2026-06-21, from verifying NY): "Title V" ≠ "our facilities"

Verified the NY source directly (the gating spot-check). The NY Socrata "Issued Title V"
dataset (297 records) contains **ZERO** of our ag/biofuel facilities — no ADM, Cargill,
ConAgra, ethanol, grain, or crush. Why:
- **Many of our target facilities are MINOR / synthetic-minor sources, not Title V majors.**
  In NY only **9 of 55** ECHO target facilities are "Major Emissions"; 25 are Minor, 11
  synthetic-minor. Minor sources don't get Title V permits — their equipment-list depth
  lives in a state's *minor-source / State Facility permit* system, a DIFFERENT dataset.
- **IA worked because its DNR repository was COMPREHENSIVE** (Title V + minor operating
  permits — that's why we got machine shops + data centers among the 288). NY's Socrata is
  Title-V-only, so it misses our minor sources entirely.
- **ECHO industry tags have false positives.** NY "ethanol" is mostly chemical plants
  (Occidental, Chemours, Arch Chemicals) caught by the SIC 2869 sweep — not ethanol producers.

**Reframe (drives sequencing):**
1. **For economic MODELING, we primarily want MAJOR sources** (big crush/ethanol/RD/mill
   plants) — and those ARE Title V, so Title-V sources cover the modeling-critical targets.
   Minor sources (small elevators, feed mills) are lower priority — defer their depth.
2. **Prioritize states by MAJOR-source target density, not raw ECHO count.** NY (9 majors,
   barely ag) is low-value despite 55 ECHO rows. IN (43 ethanol incl. 22 major + crush
   majors) is high-value. The raw-count column below overstates low-major states.
3. **Per state, the real question is two-dimensional:** (a) Title V access tier [done below]
   AND (b) does the source also cover minor sources, or Title-V-only? IA = comprehensive;
   NY = Title-V-only. Flag this as we verify each.
4. **Verify before building, always** — this finding would have wasted a full NY adapter.

## Key findings (research pass 2026-06-21, 22 of 33 priority states; VA/OK/FL/WV/MS/MA/SD/WI/CT/MD/ND pending)

- **Tier 1 (Iowa-twins, scrapable end-to-end — build these first):** PA (open dir, ~7,300 PDFs),
  NY (Socrata + deterministic PDFs), AR (predictable PDFs + bulk Access DB), IN (predictable
  PDF URL), CO (Google Drive by company). Five clean targets already — more than enough to
  start national depth-acquisition without solving the hard backends.

- **The IA acquirer will NOT transfer cleanly — this is the real cost of going national.**
  Each state runs a different backend: Oracle WebCenter (TX), ASPX/session (LA, GA, OH),
  Hyland OnBase Angular+JSON (NE), Drupal media fields (MO), report-driven forms (NJ),
  open IIS directory (PA). Realistic architecture = a small set of **per-backend adapter
  types**, not one universal scraper. (Directly answers the "did we generalize at MN?"
  question: generalization means ~5-6 adapter patterns, not one.)
- **FOIA is rare:** across 22 states, **zero are purely Tier 3.** Only Kansas (issued
  permits) needs a records request outright; Tier-3 *edges* exist for MN historical, KY's
  Jefferson County (LMAPCD), and CA's small rural districts. The "everything is FOIA" fear
  is overblown — most states are scrapable with per-backend effort.
- **California is the structural outlier:** no statewide repository — ~35 local Air
  Districts, each its own portal. Per-district adapters (start SCAQMD); small districts → CPRA.
- **Jurisdictional gaps to track:** state systems often exclude self-administered local
  programs — Allegheny/Philadelphia (PA), Jefferson Co. (KY), 4 TN county programs, OH
  delegated locals. Source these separately.
- **GATING CAVEAT (applies to every state):** confidence is high on the *access
  mechanism*, NOT on whether the equipment/capacity tables live in the issued permit PDF
  vs. a separate application/"specific conditions" attachment. Since equipment-list DEPTH
  is the whole point, **one manual spot-check per state on a known facility is a gating
  step before building that state's scraper** — not a nicety.

## US state table

Facility counts from `bronze.epa_echo_facility` (2026-06-21), `n` = total / `op` = operating.
Access tier + portal columns populated by research pass (in progress 2026-06-21).

| State | ECHO n/op | Tier | Agency / portal | Notes |
|---|---|---|---|---|
| IA | 158/127 | **1 — DONE** | Iowa DNR permit document repository | Drained: 288 facilities, 8,847 units. Reference implementation. |
| PA | 94/64 | **1** | PA DEP Bureau of Air Quality | `files.dep.state.pa.us/air/AirQuality/AQPortalFiles/Permits/PermitDocuments/` | **Best target.** Open IIS dir, 7,318 `*_Issued_v#.pdf`, nightly-refreshed, Title V = `-05` infix. Excl. Allegheny + Philadelphia (self-admin). Plain GET. |
| IN | 93/66 | **2** (verified) | IDEM | Oracle WebCenter ECM `ecm.idem.in.gov/cs/idcplg` (anonymous) | **CRACKED + download-verified 2026-06-21.** Research's `permits.air.idem.in.gov/<permitno>f.pdf` is WRONG (403). CAATS = JSF, ignore. Real recipe in `reference_idem_oracle_webcenter_permits`. 43 majors. Reusable for TX. |
| IL | 281/143 | 2 | Illinois EPA Bureau of Air (CAAPP) | `webapps.illinois.gov/EPA/DocumentExplorer/Attributes` + `GetAirPermitDocument/{id}` API | Live doc API confirmed. Map facility→doc IDs. Pre-~2015 → FOIA. |
| TX | 244/208 | 2 | TCEQ | CFR Online `records.tceq.texas.gov` + Central Registry RN lookup | 2-step (RN→docs), Oracle WebCenter session. Pre-2012 = FOIA. |
| LA | 181/117 | 2 | LDEQ Air Permits (Part 70) | EDMS `edms.deq.louisiana.gov/edmsv2`; quarterly issued XLSX as AI-number seed | Resolve facility→AI#, query EDMS. ASPX/session. Check if equipment tbl in main PDF vs attachment. |
| NJ | 137/119 | 2 | NJDEP Bureau of Stationary Sources | DocMiner `docminer.nj.gov` | Built to replace OPRA; serves native PDFs. Report-driven forms, no static URLs. High count likely blending terminals. |
| GA | 131/62 | 2 (T1-adjacent) | Georgia EPD Air Protection Branch | `permitsearch.gaepd.org`; PDFs `permit.aspx?id=PDF-OP-#####` | Predictable PDF URL + AIRS addressing → nearly enumerable. No bulk API. |
| KS | 112/62 | **3** (partial 2) | KDHE Bureau of Air (Class I) | KEIMS (account-gated) `keims.kdhe.ks.gov`; public notices show drafts only | **Only hard one.** Issued permits need KORA records request (days-weeks). Drafts opportunistic. 5-min manual check to confirm. |
| MO | 106/56 | 2 (T1-adjacent) | MoDNR Air Pollution Control | `dnr.mo.gov/air/business-industry/permits/issued`; PDFs at stable VFC path | Walk Drupal table → stable PDF href. 1996+. ethanol/BD = `OPYYYY-NNN`. |
| NE | 106/48 | 2 | NDEE Air Quality Division | ECMP `ecmp.nebraska.gov` (OnBase); lookup `deq-iis.ne.gov/zs/permit/main_search.php` | OnBase Angular+JSON; reverse-engineer query API. ethanol-dense. |
| OH | 83/71 | 2 | Ohio EPA Div. Air Pollution Control | eDoc `edocpub.epa.ohio.gov/publicportal`; PDFs `ViewDocument.aspx?docid=` | Returns application/pdf confirmed; opaque docids → scrape result list. Watch delegated local agencies (RAPCA etc.). |
| NY | 55/45 | **1** | NYSDEC Bureau of Stationary Sources | Socrata `data.ny.gov/.../4n3a-en4b` + PDFs `extapps.dec.ny.gov/data/dar/afs/permits/{facility_id}_{rev}.pdf` | **Iowa twin.** Socrata API/CSV index + deterministic PDF path. Build early. |
| AR | 61/41 | **1** | ADEQ Office of Air Quality | PDS `adeq.state.ar.us/home/pdssql/pds.aspx`; PDFs `.../PermitsOnline/Air/<PermitNo>.pdf`; bulk `Air_Permitting_web.zip` | Deterministic PDF URL + full Access DB ZIP (facility→permit→AFIN). Fully scrapable. |
| CO | 73/20 | **1** | CDPHE Air Pollution Control | Title V company index → public Google Drive; construction perms on Hyland Cloud portal | Title V docs bulk-downloadable from Drive by company. Two systems if minor perms needed. |
| TN | 78/49 | 2 | TDEC Div. Air Pollution Control | Oracle APEX viewer `dataviewers.tdec.tn.gov/.../19031:34001`; docs `BGAPC.GET_DOCUMENTS?p_file=<id>` | Predictable `p_file` pattern (seed index). Excl. 4 local county programs (Nashville, Chattanooga, Knox, Memphis). |
| AL | 73/55 | 2 | ADEM | eFile `app.adem.alabama.gov/efile/` (Media=Air, DocType=Permitting) | 1M+ docs; one-at-a-time, no API. |
| CA | 66/50 | 2 (fragmented) | ~35 local Air Districts (CARB oversees only) | SCAQMD/BAAQMD/SJVAPCD/SacMetro each separate | **Special case.** No statewide repo. Per-district adapters; start SCAQMD. Small rural districts → CPRA (Tier 3). LCFS-relevant (RD/SAF). |
| MN | 62/44 | 2 recent / 3 historical | MPCA Air Quality | "What's in My Neighborhood" `webapp.pca.state.mn.us/wimn/` (site/`<id>`) | **Not pure-FOIA** (corrects the memory). Recent PDFs online; full historical via records request / in-person St. Paul. |
| NC | 57/38 | 2 | NC DEQ Div. Air Quality | Laserfiche `edocs.deq.nc.gov/AirQuality/` | Final signed PDFs searchable by Facility ID/Name; script WebLink per facility, no bulk. |
| SC | 50/34 | 2 | SCDES Bureau of Air Quality (ex-DHEC) | ePermitting `epermitting.des.sc.gov`; legacy DHEC portal | Title V since 2023-24; DHEC→DES URL-stability risk. |
| KY | 49/40 | 2 | KY Div. for Air Quality (+ Louisville Metro APCD for Jefferson Co.) | DEP Gateway `dep.gateway.ky.gov/eSearch/Approvals/Issued` (program=Air) | One-by-one; no bulk. Jefferson Co. separate (LMAPCD, partial Tier 3). |
| MI | 48/41 | 2 | EGLE Air Quality Division | MiEnviro Site Explorer + master ROP/PTI index PDFs | Master active-permit PDFs = seed index; per-facility download, no bulk. |
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
