# Handoff 2026-08-05 (second session) — SAGyP FOB + helios_wapr collectors, WAPR report pull

## What shipped (commit 5310c668, pushed)

- **Task 1 (report-blocking) — Helios WAPR readings table**: pulled live from api.helios.sc,
  factor-level current readings (actuals through 2026-08-04) + 4-week trend for all requested
  regions; delivered as `wapr_report_table.md` (sent to Tore in-session, paste-ready for
  Desktop). Coverage gaps found and stated plainly: **Helios has NO sunflower commodity at
  all** (Black Sea + Argentina sunflower cannot be served), **no Russia canola pair**, wheat
  still not split spring/winter. All requested "regions" are country-level aggregates —
  Helios carries no sub-national granularity.

- **sagyp_fob_oficial collector** (mig 173, `src/agents/collectors/south_america/
  sagyp_fob_collector.py`): Argentine official FOB circulars with forward shipment bands.
  - bronze.sagyp_fob_raw: every posicion, bands preserved; PK (fecha, posicion, mes_desde,
    anio_desde); upsert touches collected_at per [[feedback_timestamp_every_touch]].
  - silver.price_mark via reference.sagyp_position_map — **exact posicion codes, not
    prefixes** (variants inside a prefix price differently: bulk vs bagged; and the bulk/bag
    digit moves position between products — wheat 2nd digit, corn 1st). Codes verified stable
    1993→2026 by live probes. 12 series: SAGYP_{SUNOIL_CRUDE, SUNOIL_REFINED, SBO_CRUDE,
    SBO_REFINED, WHEAT, WHEAT_DURUM, CORN, SOYBEANS, SOYMEAL, SUNSEED, SUNMEAL, WHEAT_FLOUR}.
  - **Spec deviations, deliberate**: (1) tenor_type='WINDOW' not 'shipment_band' — the spec
    value violates price_mark_tenor_type_ck (SPOT/CONTRACT/WINDOW/NEARBY); WINDOW is the
    estate's vocabulary for a shipment window. tenor='YYYY-MM:YYYY-MM'. (2) "1001* →
    SAGYP_WHEAT" would have merged durum (1001.19, 275) with bread wheat (1001.99, 239);
    split into SAGYP_WHEAT (bread, the AR benchmark) + SAGYP_WHEAT_DURUM.
  - Daily 18:00 ET (= 19:00 ART) + T-1 revision re-pull; empty weekday response = Argentine
    holiday → INFO + SUCCESS 0 rows. data_source registered (daily weekday-aware freshness,
    expected 18:30 ET; note the mig-171/172 rule is ET-based — the spec's "in ART" has no
    column to live in, source tz recorded on the data_source row).
  - **Acceptance test PASSED** vs Circular No. 2030 (2026-08-04): SAGYP_SUNOIL_CRUDE
    1,349 (2026-08:2026-09) and 1,338 (2026-10:2027-07), exact match, bands from data.
  - Endpoint quirks learned in backfill (both fixed + committed): no-publication days return
    a bare JSON `[]` (not `{"posts":[]}`); one day's response can carry the SAME band under
    two circulars (original + revision) → in-payload dedup keeps highest circular number.

- **helios_wapr collector** (mig 174, `src/agents/collectors/global/helios_wapr_collector.py`):
  daily 07:00 ET full-index pull (mon-fri; Monday self-heals the weekend since every pull
  re-delivers the whole index). First run via CollectorRunner: **success, 230,490 rows, 89s,
  90 pairs** (was 88 on 7/21 — Helios added two), actuals 7/22→8/4 backfilled, clean
  collection_complete event.
  - **Forecast-vintage ruling made in-session, TORE TO CONFIRM**: bronze.helios_climate_risk
    stays current-state (upsert, consumers unchanged); is_forecasted rows archived per pull
    date in bronze.helios_climate_risk_vintage (~65.8k rows/day ≈ 24M/yr). Chosen because
    unstored vintages are unrecoverable and stored ones are prunable — if you want a horizon
    cap (e.g. vintages only within 90 days of date_on) it's one DELETE + one WHERE. Vintages
    7/22→8/4 never existed; first vintage = 2026-08-05.
  - July 2021 actuals (2,728 rows) aged out of the API's rolling window and persist from the
    7/21 load — correct behavior, do NOT purge non-refreshed actuals.

- **Code-drift guards (Desktop follow-up, accepted)**: mig 175 turns
  reference.sagyp_position_map into a disposition registry (series_key NULL + is_active
  false = reviewed, bronze-only; 26 variants seeded). The collector warns (-> 'partial') on
  any pulled posicion sharing a curated HS6 family with NO disposition row — new/drifted
  codes surface instead of dying silently. `scripts/audit_sagyp_series_coverage.py` prints
  pub-days per series per year + sweeps new known-family variants into disposition rows;
  **run it after the 1993→2019 pass completes** — any multi-year hole is a drifted code
  needing a historical map entry.

- **Task 3 queued only**: `docs/specs/sagyp_estimaciones_collector_v1.md` — canonical CKAN
  endpoint verified live (resource filename is versioned → resolve via package_show at
  runtime); current vintage is comma/UTF-8, NOT the latin-1/semicolon in the Desktop note
  (that describes older vintages; collector must sniff). Added to memory priority queue.

- **Dispatcher restarted** (schtasks End/Run): new instance 06:21:35 ET runs both new
  schedules. The detached EPA enrich process (PID 68004) was untouched by the restart.

- **Orphan closed**: collection_status 1566 (epa_echo_enrich_by_frs, 09:26:29 UTC 'running',
  dead first launch attempt) hand-closed as failed/orphan; superseded by live run 1567.

## State by workstream

- **EPA Echo manual enrich (run 1567) — hardening VALIDATED**: partial, 75 min (est. was
  ~105), 1,876/1,902 facilities enriched, 26 DFR failures (1.4%, the reason for 'partial'),
  ONE row, finalized cleanly at 10:44:32 UTC. First full exercise of single-connection +
  runtime-cap + reconnect-retry passed. Orphan row 1566 (dead first launch attempt,
  09:26 UTC) hand-closed as failed.
- **SAGyP backfill**: 2020→present pass COMPLETE + audited — 1,721 requests, 208,268 bronze
  rows, 124 holiday weekdays, 29.2 min; audit shows every curated series present on every
  publication day 2020→2026 (241/244/242/243/244/241/142), zero holes. 1993→2019 pass
  running at session end (~2h at 1 req/s); if it died with the session, resume with
  `python scripts/backfill_sagyp_fob.py --start 1993-01-04 --end 2019-12-31` (state file in
  `data/backfill_state/` makes it pick up where it stopped). Then re-run
  `scripts/audit_sagyp_series_coverage.py`. Early 1993 signal already visible: SUNOIL_REFINED
  quoted only 14/40 days, WHEAT 39/40 — thin early-90s quoting, not code drift; judge against
  the full pass.

## Open decisions needing Tore

1. **Helios forecast-vintage retention — RULED 2026-08-05 (Tore, in-session)**: confirmed
   as built — full-horizon daily archive. Skill-by-lead-time for the WAPR accuracy paper is
   computable only from archived vintages; storage is cheap/reclaimable, discarded vintages
   are gone forever. Keep daily archiving through the paper; set retention (e.g. thin to
   weekly vintages after 18 months) with hindsight, not foresight. Partition by month if
   volume ever annoys.
2. sagyp position map curation: bulk variants chosen per series (see mig 173 seeds) —
   sanity-check SAGYP_CORN = 10059010190Y ("los demás", 208 on 8/4) vs the 120A variant
   (217); if the premium variant is the market's quoted benchmark, flip the map row.
3. WASDE-day overlap: sagyp 18:00 ET pull is fine, but Aug 12 rerun of
   build_usda_comp_tabs.py still queued from the prior handoff.

## Known-broken / unverified

- sagyp_fob_oficial ran via CollectorRunner at 06:22 ET: success, 114 rows (T-1 = 8/4
  re-pull; 8/5 circular not yet published that early ART morning — expected, the schedule
  runs 18:00 ET).
- The 18:00 ET scheduled firing of sagyp_fob_oficial and the 07:00 ET firing of helios_wapr
  have NOT yet been observed end-to-end from the dispatcher (registered + manually verified
  only). Check tomorrow's briefing.
- helios_wapr MIN_EXPECTED_PAIRS=80 coverage guard is a heuristic, not sourced from Helios.
- sagyp backfill 1993→2019: in flight at session end; verify completion + spot-check an
  early-90s date against a manual pull next session.
