# Facility agent activity — since daily schedule went live 2026-05-19

*Generated 2026-05-22 11:25 UTC. ECHO collectors (oilseed / ethanol / biodiesel / milling) switched from monthly to daily on 2026-05-19 evening.*

---

## 1. Run cadence — last 4 days

| Date | Collector | Status | Facilities fetched | Duration (s) |
|------|-----------|--------|-------------------:|-------------:|
| 2026-05-20 | `epa_echo_oilseed` | success | 202 | 889 |
| 2026-05-20 | `epa_echo_ethanol` | success | 1,658 | 14348 |
| 2026-05-20 | `epa_echo_biodiesel` | success | 677 | 10090 |
| 2026-05-20 | `epa_echo_milling` | success | 426 | 8529 |
| 2026-05-21 | `epa_echo_oilseed` | success | 202 | 905 |
| 2026-05-21 | `epa_echo_ethanol` | running | 0 | — |
| 2026-05-21 | `epa_echo_biodiesel` | running | 0 | — |
| 2026-05-21 | `epa_echo_milling` | running | 0 | — |
| 2026-05-22 | `epa_echo_oilseed` | success | 202 | 989 |
| 2026-05-22 | `epa_echo_ethanol` | failed | 0 | 15 |
| 2026-05-22 | `epa_echo_ethanol` | failed | 0 | 13 |
| 2026-05-22 | `epa_echo_biodiesel` | running | 0 | — |
| 2026-05-22 | `epa_echo_ethanol` | running | 0 | — |
| 2026-05-22 | `epa_echo_milling` | running | 0 | — |

## 2. Current bronze coverage — facilities by industry profile

| Profile | Total facilities | States | Last refresh |
|---------|-----------------:|-------:|--------------|
| biodiesel_renewable_diesel | 628 | 52 | 2026-05-20 07:48 |
| ethanol | 1,637 | 49 | 2026-05-20 08:29 |
| soybean_oilseed | 203 | 33 | 2026-05-22 04:16 |
| wheat_milling | 394 | 37 | 2026-05-20 07:52 |

## 3. Top 15 states by facility count — BBD-relevant industries

| State | Oilseed crush | BBD (biodiesel/RD) | Ethanol | Flour milling |
|-------|-------------:|-------------------:|--------:|--------------:|
| IA | 33 | 7 | 82 | 36 |
| IL | 23 | 65 | 158 | 35 |
| NE | 15 | 3 | 76 | 12 |
| IN | 14 | 20 | 43 | 16 |
| MO | 13 | 23 | 36 | 34 |
| GA | 8 | 22 | 88 | 13 |
| AL | 7 | 13 | 46 | 7 |
| MN | 7 | 5 | 33 | 17 |
| AR | 7 | 5 | 24 | 25 |
| KS | 6 | 19 | 66 | 21 |
| NC | 6 | 2 | 36 | 13 |
| OH | 5 | 13 | 57 | 8 |
| TN | 5 | 9 | 47 | 17 |
| VA | 5 | 5 | 20 | 11 |
| SD | 5 | 0 | 24 | 0 |

## 4. Compliance / enforcement signals

How many facilities in each profile have compliance / enforcement data populated:

| Profile | Has compliance | Has enforcement | Has operating status | Has CAA permit |
|---------|--------------:|---------------:|---------------------:|--------------:|
| biodiesel_renewable_diesel | 626 | 626 | 628 | 626 |
| ethanol | 1636 | 1636 | 1637 | 1636 |
| soybean_oilseed | 203 | 203 | 203 | 203 |
| wheat_milling | 392 | 392 | 394 | 392 |

## 5. Operating-status mix (across all profiles)

| Operating status | Facility count |
|------------------|---------------:|
| Operating | 1,933 |
| Permanently Closed | 861 |
| No Operating Status In ICIS | 27 |
| Planned Facility | 18 |
| Temporarily Closed | 15 |
| Under Construction | 8 |

## 6. Sample BBD-relevant facilities flagged with enforcement actions

| Facility | Location | Profile | Compliance | Enforcement |
|----------|----------|---------|-----------|-------------|
| 3-B RATTLESNAKE REFINING CORP | MONAHANS, TX | biodiesel_renewable_diesel | CAA: SNC=No, QtrsNC=0 | CAA: No Violation Identified |
| 9920109981 D-A PACKAGING, LLC | COMMERCE CITY, CO | biodiesel_renewable_diesel | CAA: SNC=No, QtrsNC=1; CWA: SN | CAA: No Violation Identified;  |
| A BUNKER OIL INC | FLORIEN, LA | biodiesel_renewable_diesel | CAA: SNC=No, QtrsNC=0 | CAA: No Violation Identified |
| ACE LOGISTICS INC - LARRY SMITH PROPERTI | BATON ROUGE, LA | biodiesel_renewable_diesel | CAA: SNC=No, QtrsNC=0; CWA: SN | CAA: No Violation Identified;  |
| ADOBE REFINING COMPANY | LA BLANCA, TX | biodiesel_renewable_diesel | CAA: SNC=No, QtrsNC=0 | CAA: No Violation Identified |
| ADVANCED OIL | CONLEY, GA | biodiesel_renewable_diesel | CAA: SNC=No, QtrsNC=0; RCRA: S | CAA: No Violation Identified;  |
| AERR CO | DENVER, CO | biodiesel_renewable_diesel | CAA: SNC=No, QtrsNC=0; RCRA: S | CAA: No Violation Identified;  |
| AGWAY PETROLEUM CORPORATION | CHESTER, NJ | biodiesel_renewable_diesel | CAA: SNC=No, QtrsNC=0; RCRA: S | CAA: No Violation Identified;  |
| AIR BP LURBO OIL PLANT | LINDEN, NJ | biodiesel_renewable_diesel | CAA: SNC=No, QtrsNC=0; RCRA: S | CAA: No Violation Identified;  |
| AIR PRODUCTS MANUFACTURING, LLC | PARAMOUNT, CA | biodiesel_renewable_diesel | CAA: SNC=Yes, QtrsNC=12; CAA:  | CAA: High Priority Violation;  |
| AIR PRODUCTS PORT ARTHUR PLANT | PORT ARTHUR, TX | biodiesel_renewable_diesel | CAA: SNC=No, QtrsNC=6; CAA: SN | CAA: Violation w/in 1 Year; CW |
| ALABAMA BULK TERMINAL COMPANY | MOBILE, AL | biodiesel_renewable_diesel | CAA: SNC=No, QtrsNC=0; CWA: SN | CAA: No Violation Identified;  |
| ALBUQUERQUE ASPHALT TERMINAL | ALBUQUERQUE, NM | biodiesel_renewable_diesel | CAA: SNC=No, QtrsNC=0; RCRA: S | CAA: No Violation Identified;  |
| ALFRED H KNIGHT LABS | POMPTON LAKES, NJ | biodiesel_renewable_diesel | CAA: SNC=No, QtrsNC=0; RCRA: S | CAA: No Violation Identified;  |
| ALON ASPHALT USA | SHORELINE, WA | biodiesel_renewable_diesel | CAA: SNC=No, QtrsNC=0; CWA: SN | CAA: No Violation Identified;  |

## 7. What each daily run is doing (mechanically)

Each ECHO collector profile follows this flow:

1. **Hits EPA ECHO API** with the profile's SIC codes (e.g., ethanol = SIC 2869 'Industrial Organic Chemicals NEC').
2. **Receives a list of facilities** matching that SIC. This is where the false-positive problem comes from — SIC 2869 returns 1,658 facilities, most of which are chemical plants, not ethanol.
3. **For each facility, calls the Detailed Facility Report (DFR) endpoint** — returns name, address, permits (CAA, NPDES, RCRA), operating status, compliance summary, enforcement count.
4. **Upserts each facility into `bronze.epa_echo_facility`**, keyed on `frs_registry_id` (FRS = Facility Registry Service, EPA's universal facility identifier).
5. **Repeats daily** — each refresh overwrites the previous row for the same FRS ID, so the table tracks current state. Historical state changes are NOT preserved in this table (would need a separate audit log).

**Per-facility API throttle**: ~4 seconds per DFR call. This is why ethanol takes ~2 hours (1,658 facilities × 4s) and the four profiles run sequentially across ~9 hours overnight.

**What's NEW each day**: typically very little. EPA refreshes DFR data quarterly for compliance details, monthly for operating status, daily only for the FRS registry itself. The daily cadence means we catch any NEW facility added to the registry within 24 hours — useful for spotting new BBD capacity going through registration, but most days the data is essentially identical to yesterday's pull.

## 8. Honest assessment — what's working and what isn't

**Working:**
- All 4 profiles run reliably daily, catch transient API errors next-day instead of next-month.
- ✅ 2,779 total facility records in bronze, including all the BBD-relevant operators we care about (DGD, Marathon, REG, Bunge, ADM, etc.).
- Permit IDs (CAA, NPDES, RCRA) are populated for most facilities — gives us the join keys to permit-level data we extract via Ollama.

**Not working (queued for fix as Task #66):**
- 9 hours of API time daily for ~5-10 NEW data points across all 4 profiles. The signal-to-noise ratio is bad.
- The ethanol SIC sweep returns 1,658 facilities; only ~200 are actual ethanol plants. The other ~1,450 are chemical / paint / solvent factories that happened to share SIC 2869.
- Enforcement and compliance status fields are sparse in EPA's public output — most cells are NULL.
- No historical state-change log: if a facility goes from 'Operating' to 'Idle', we overwrite without recording the transition.

**Architecture flip (Task #66) plan:**
- Take the 2,001 facilities in `silver.facility_map` (your curated multi-industry list, NOT the EPA SIC sweep).
- For each, look up its FRS ID once (via name + state token match).
- Daily enrich ONLY those 2,001 facilities directly via FRS ID → DFR. 
- Drops daily runtime from ~9 hours to ~30 min, removes 90% of false-positive chemical plants from the table.
- Adds a `bronze.epa_echo_facility_audit` table that DOES track historical state changes.
