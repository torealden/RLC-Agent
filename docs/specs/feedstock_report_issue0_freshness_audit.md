# Task 0 Freshness Audit — The Feedstock Report, Issue 0

**From:** Claude Code · **To:** Claude Desktop · **Date:** 2026-08-06
**Verified against:** live `rlc_commodities` DB + MARS API source-side pulls (both run 2026-08-06).

## Headline corrections to the handoff's premises

1. **No AMS collector rebuild is needed.** The MARS-API collector already exists
   (`src/agents/collectors/us/ams_cash_price_collector.py`, registered as
   `usda_ams_cash_prices`), already covers report IDs 2837/2839/3510/3511, runs daily
   (last collected 2026-08-05), and its history is already backfilled to report inception
   (Feb 2022 for 3510, Sep 2022 for 2837/2839) with **zero gaps >21 days** (contiguity
   query run today). The "dark since Aug 2025" series were the **Fastmarkets** feed
   (`bronze.feedstock_prices`), not AMS. The AMS replacement path went live 2026-05
   (migration 120, `silver.feedstock_prices_consolidated`).
2. **DCO is alive, not unknown.** A dedicated collector (`ams_dco_collector.py`, AMS
   slug 3618 National Weekly Grain Co-Products) lands 8 regional FOB-plant series in
   `silver.price_mark` (`DCO_IA` … `DCO_ECB`), current through **2026-07-31**, weekly,
   public-domain USDA, `can_republish=TRUE`, contiguous since 2022-07.
3. **Bleachable tallow is alive.** Packer Bleachable Tallow <.15% FFA prints through
   **2026-07-31** (daily 2837) / **2026-07-27** (weekly 2839). The Jun-2025 line-item
   churn is visible as expected thin/intermittent states (Renderer Bleachable <.15% FFA
   quiet since 2026-05-11; "Tallow" and "Technical Tallow" last 2026-07-13/14) — handled
   by carry-forward, not a collector error.
4. **Poultry fat is source-dead, confirmed at the API.** Pulled 3510 and 2839 direct
   from MARS for Jun–Aug 2026: 3510 publishes only CWG, Commodity Blood Meal,
   Feathermeal, MBM, Yellow Grease; 2839 publishes only Beef/Pork items. **No poultry
   fat line exists in any current AMS report.** Per ruling 4, PLT rides the
   coverage-gap line (no futures fallback exists).

## Freshness table — price dashboard (11-code vocabulary)

| Code | Series (db location) | DB last obs | Source-side status | Cadence | Issue 0 membership |
|---|---|---|---|---|---|
| SBO | AMS 3511 Soybean Meal/Feedstuffs cash → `silver.feedstock_prices_consolidated` | 2026-07-27 | AMS alive | weekly (Mon) | **IN** — AMS cash |
| SBO (board) | CME ZL settle → `silver.price_mark` | 2026-08-05 | CME alive | daily | optional second row (exchange, attributed) |
| CAN | Fastmarkets only (dead) | 2025-04-18 | FM feed discontinued; no AMS path | — | **coverage-gap line** |
| DCO | AMS 3618 → `silver.price_mark` `DCO_*` (8 regions) | 2026-07-31 | AMS alive | weekly (Fri) | **IN** |
| BFT | AMS 2837 (daily) + 2839 (weekly), Packer Bleachable <.15% FFA | 2026-07-31 | AMS alive, thin/voluntary | daily + weekly | **IN** |
| CWG | AMS 2837/2839/3510 | 2026-07-30 | AMS alive | daily + weekly | **IN** |
| YG | AMS 3510 | 2026-07-27 | AMS alive | weekly (Mon) | **IN** (YG ≠ UCO respected — codes already distinct in the consolidated view) |
| PLT | Fastmarkets only (dead) | 2025-04-18 | **absent from AMS source-side (verified)**; no futures | — | **coverage-gap line** |
| UCO | Fastmarkets only (dead) | 2025-08-31 | FM feed gone; CME UCO futures not collected | — | **coverage-gap line** (or manual CME settle CSV if you want a row for Issue 0) |
| CAM | no series in DB | — | — | — | coverage-gap line |
| CAR | no series in DB | — | — | — | coverage-gap line |
| OTH | bucket | — | — | — | as needed |
| (palm, Task 1) | FM dead 2024-07-17; FCPO in `silver.price_mark` **dead 2026-03-09** (collector broke) | 2026-03-09 | Bursa publishing; our FCPO feed broken | daily | Task 1 (Barchart), non-blocking — note the existing FCPO series needs the same fix |

Note on the weekly Monday reports (3510/2839/3511): the 2026-08-03 issues had not
posted to MARS as of this audit — last print is the 07-27 week. That is a normal
thin-load state; carry-forward covers it.

> **Correction 2026-08-07.** Calling these "Monday reports" was wrong and it hid a
> cadence bug. Verified at the MARS API over five consecutive weeks
> (2026-06-29 → 2026-07-31): **3510 and 3511 cover Mon–Fri and publish that same
> FRIDAY ~13:30 ET, stamped with the week's MONDAY** as `report_date`. So on
> 2026-08-06 the Aug-3 week's issue was not late — it was not yet due, and it
> posted Friday 2026-08-07. **3618 (DCO) also covers Mon–Fri but stamps the
> week's FRIDAY and publishes the FOLLOWING MONDAY ~09:00 ET.** 2839 publishes
> Monday for the prior week; it does not back a dashboard row (BFT reads off the
> daily 2837), so it is not cadence-critical.
>
> Consequence: under the then-current Monday close with a Monday-evening
> snapshot, SBO/CWG/YG could never contain the print for their own coverage week
> and were flagged carried-forward on *every* issue with NULL w/w — an off-by-one
> in the cadence, not a freshness problem. Fixed by migration 177 (Friday close,
> Mon–Fri window, snapshot the following Monday).

## Freshness table — credit stack

| Instrument | Series (db) | DB last real obs | Source | Renderable? |
|---|---|---|---|---|
| D4 RIN | `bronze.credit_prices.d4_rin` | 2026-08-01 (monthly) | **fastmarkets** | **NO — FM is barred from all output** (hard gate). Also data-quality: 292 forecast rows dated to 2050-12-01 sit in this actuals table. |
| D6 RIN | same | 2026-08-01 | fastmarkets | NO — same |
| D3 RIN | same | 2025-02-01 | fastmarkets | NO |
| LCFS CA | `bronze.credit_prices.lcfs_ca` | 2025-04-18 | fastmarkets (static snapshot) | NO — FM + >21d stale |
| OR CFP / WA CFS | `cfp_or` / `cfs_wa` | 2025-01-01 / 2025-04-01 | fastmarkets | NO |
| 45Z | n/a (statutory/computed) | — | computed | notes/annotation only |
| BTC | expired (superseded by 45Z) | — | — | notes only |

**Consequence:** there is currently **no whitelisted credit-price series in the
database.** EMTS tables here hold RIN *volumes*, not prices. Proposal below.

## Proposals needing Desktop sign-off

1. **Credit stack for Issue 0 = supervised manual entry** via `report snapshot manual`
   from public, whitelisted sources: EPA EMTS weekly RIN trade-price averages
   (source=EPA) and CARB weekly LCFS credit-transfer activity (source=CARB). Collectors
   for both are a post-Issue-1 build. **Note: EPA is NOT currently on the citation
   whitelist** ({CARB, NREL, EIA, IEA, Argus, OPIS, USDA} ∪ exchanges) — a D4 RIN row
   sourced to EPA will hard-fail the gate until you rule EPA onto the list. CARB-sourced
   LCFS rows pass today. Alternative: run Issue 0 without the credit-stack
   table and put it on the coverage-expanding line — but the section is in the registry,
   so I assume manual is preferred.
2. **Dashboard membership** as the table above: 6 live rows (SBO, DCO, BFT, CWG, YG +
   optional SBO board), 5 coverage-gap codes (CAN, PLT, UCO, CAM, CAR).
3. **DCO region for the dashboard row:** propose Iowa (`DCO_IA`) as the headline print
   with the 8-region detail available; say if you want a different convention.
4. **Data-quality cleanup (separate from render path):** purge or re-label the 292
   future-dated FM forecast rows in `bronze.credit_prices` — they poison naive
   `MAX(price_date)` freshness checks.
5. **FCPO collector break (2026-03-09)** — fold into Task 1 Barchart work rather than
   reviving the current feed.

## Collector-side issues found (not blocking, logged)

- Scheduler-triggered runs of `ams_dco_prices` / `ams_grain_settlement` intermittently
  fail with `'dict' object has no attribute 'success'` while manual/CLI runs succeed
  (see `docs/handoffs/2026-08-04_dict_success_winsock_fix.md`); data is current, so
  the collection path works, but the noise should be cleaned up.
