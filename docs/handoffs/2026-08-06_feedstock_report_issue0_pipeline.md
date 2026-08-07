# Handoff — Feedstock Report Issue 0 pipeline (2026-08-06)

**Spec:** `docs/specs/HANDOFF-feedstock-report-issue0.md` (Desktop → Code).
**Deadline status:** pipeline green 2 days ahead of the Fri Aug 7 EOD gate.

## What shipped

- **Task 0 freshness audit** → `docs/specs/feedstock_report_issue0_freshness_audit.md`.
  Headline: the "dead AMS collector" premise was wrong — the dead series were the
  Fastmarkets feed; the MARS-API collector already exists, runs daily, and is
  contiguous to Feb 2022 (zero >21d gaps, verified by query). DCO is alive
  (AMS 3618 → silver.price_mark, through 2026-07-31). Poultry fat confirmed
  absent from AMS source-side (MARS API pulled directly). No collector rebuild
  or backfill was needed.
- **Migration 176** (applied to live DB): free_mode + coverage_start + locked
  status + html/kit paths on `feedstock_issue`; feedstock_code / last_observed /
  is_carried_forward / is_manual_entry on the dashboard snapshot; per-instrument
  row grain on the credit stack snapshot.
- **Pipeline** under `src/reports/feedstock_report/`:
  `report_config.py` (brand INK/GOLD/PAPER, citation whitelist, banned strings,
  dashboard series registry — all gate inputs are config), `snapshot.py` (ETL +
  manual CSV path + IFV leaderboard via the kg_callable), `gates.py` (IFVS-008),
  `render.py` (single-file HTML w/ data-URI chart + LinkedIn kit w/ 1400px PNGs),
  `cli.py` (issue create / snapshot prices|credits|ifv|manual / news add /
  section set / render / lock).
- **Issue 0 populated and rendered**: all five tables filled; both outputs at
  `output/reports/feedstock_report/issues/issue_0/`. Zero gate errors.

## Acceptance criteria — all verified

1. Freshness audit delivered (dashboard membership PROPOSED, awaiting Desktop/Tore).
2. Five tables populated; both outputs render, zero gate errors.
3. Stale paths verified: carried rows show "Jul 27 †" + "—" w/w (SBO/CWG/YG real
   data); manual test row (OTH, 2026-06-01) excluded to the coverage line.
4. `--test-ifv-numeric-injection` → GateError, exit 2, no output written.
5. Table/chart PNGs reviewed at export size; 15-16pt fonts at 700px display.

## Open decisions needing Tore / Desktop

1. **Dashboard membership sign-off** (audit doc): 5 live rows + optional CME ZL
   board row; CAN/PLT/UCO/CAM/CAR on the coverage line.
2. **EPA on the citation whitelist** — without it, no D4 RIN row can ever render
   (EMTS is the only public RIN price source). CARB LCFS passes today.
3. **Credit stack Issue 0**: manual CSV numbers from CARB/EPA public pages, or
   ship Issue 0 with the stack on the coverage-expanding line (current state).
4. Issue 0 written sections are **[DRAFT] placeholders** — replace via
   `report section set --issue 0 --code signal --file signal.md` etc.
5. Purge/relabel the 292 future-dated FM forecast rows in bronze.credit_prices
   (separate cleanup; they poison naive freshness checks).

## Known-broken / unverified

- FCPO (palm) feed dead since 2026-03-09 — folded into Task 1 (Barchart), which
  remains unstarted (demoted, non-blocking per spec).
- Scheduler-triggered ams_dco/ams_grain runs still log `'dict' has no attribute
  'success'` noise (data flows anyway; see 2026-08-04 handoff).
- Weekly AMS Monday reports (3510/2839/3511) had not posted the Aug 3 issue as of
  today — Issue 0 carries Jul 27 prints for SBO/CWG/YG. Re-run
  `snapshot prices --issue 0` before Sat if the prints land.
- HTML reviewed as text + PNGs reviewed as images; not yet opened in an email
  client (fonts/spacing may need one polish pass on a real render).

## Rerun sequence (Monday-evening cadence)

> **Superseded 2026-08-07** — the cadence below is wrong (see
> `docs/handoffs/2026-08-07_feedstock_report_friday_close.md` and migration 177).
> Coverage now closes **Friday** over a Mon–Fri window, and the snapshot runs the
> **following Monday from ~10:00 ET**. `--date` is the publish date, so Issue 1
> is `--date 2026-08-17` (coverage 2026-08-10 → 2026-08-14), not 2026-08-11.

```
python -m src.reports.feedstock_report.cli issue create --n 1 --date 2026-08-11 --free
python -m src.reports.feedstock_report.cli snapshot prices --issue 1
python -m src.reports.feedstock_report.cli snapshot credits --issue 1
python -m src.reports.feedstock_report.cli snapshot ifv --issue 1
# ... section set / news add ...
python -m src.reports.feedstock_report.cli render --issue 1
python -m src.reports.feedstock_report.cli lock --issue 1
```
