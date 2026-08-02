# Handoff 2026-08-02 — WASDE-day pull race resolved (mig 166)

## What this session was
First item from `2026-08-01_usda_comp_tabs.md` next-session prompt: resolve
the WASDE-day pull race (§4 of that handoff) — verify PSD `month` attribute
semantics, relabel vintages, move the scheduled pull, rerun the comp builder.

## The verdict (verified, not inferred)
PSD's `(calendar_year, month)` fields are a per-row **"last updated in this
PSD release cycle"** stamp, NOT the release month of the pull:

- A single full pull carries stamps spanning years (2026-01-30 pull has
  month=11/12 rows stamped 2023/2024/2025 — rows untouched since then).
- **Same stamp ⇒ identical values.** The 184 Aug-1 rows deleted as exact
  duplicates last session were exactly the rows whose July stamp matched
  the Jul-10 pull. Every value difference across pulls had a different
  stamp. The prior handoff's "PSD carries real intra-month revisions"
  reading is dead — those were cycle differences mislabeled by pull date.
- **The race is per-commodity, not wholesale.** On the Jul-10 12:14 ET
  pull, soybeans/wheat/rice were already at month=7 (post-release) while
  ALL corn rows were month≤6. Corn US MY2025 trace: May-12 pull → April
  cycle, Jun-11 → May, Jul-10 → June. Corn was stale-by-one on every 2026
  WASDE-day pull; the true July corn arrived in the Aug-1 manual pull
  (previously mislabeled WASDE_AUG_26).

## What shipped (commit fb054583 + builder commit)

### 1. Migration 166 (`166_psd_vintage_cycle_relabel.sql`) — APPLIED
`gold.psd_wasde_vintages` now labels, dedupes, and ranks by the PSD cycle:
- `psd_cycle = make_date(calendar_year, month, 1)`; when calendar_year is
  NULL (the five scheduled 2026 pulls — collector bug, see below) it is
  derived from report_date (stamp month > pull month ⇒ prior year).
  month=0 rows (2,046 sugar 1990-2003 backfill rows) fall back to the
  pull-month bucket; all closed → FINAL regardless.
- Dedup partition is now (commodity, country, MY, psd_cycle) — multiple
  pulls of one cycle collapse; one pull spanning many cycles splits.
- `psd_cycle` APPENDED as a column (CREATE OR REPLACE-safe; mig-165 views
  and gold.fas_us_wasde_comp untouched and verified working).
- Verified post-apply: corn US MY2025 = clean Jan→Jul ladder, no phantom
  WASDE_AUG_26 anywhere, zero duplicate vintages estate-wide.
- Side effect (correct, looks odd): discontinued PSD series now show
  honest old-cycle labels (WASDE_JUN_06 etc.) on their last-two-MY
  "active" rows instead of fake 2026 pull-date labels.
- China-style starved books: prior column is now honestly WASDE_FEB_26
  (the last cycle actually captured before the country-code fix), not
  WASDE_MAR_26-by-pull-date. The Mar–Jun 2026 cycles for those countries
  are unrecoverable from the API (snapshot-only).

### 2. Collector fix (`usda_wasde_collector.py`)
`calendar_year` was hardcoded `None` even though the API sends
`calendarYear`. Now parsed and persisted. All future pulls carry the full
cycle stamp, so the view's year-derivation fallback only ever covers the
five historical 2026 scheduled pulls.

### 3. Scheduled pull moved 12:00 → 15:00 ET — **orphan-path trap found**
The dispatcher (`\RLC\RLC Dispatcher` task → `src.dispatcher`) reads
`RELEASE_SCHEDULES` from **`src/schedulers/master_scheduler.py`** — NOT
`rlc_scheduler/agent_scheduler.py`, which the prior docs pointed at. Both
files updated (plus rlc_scheduler README/CSV docs); dispatcher restarted
AFTER the live edit and verified loading 15:00. Even a 15:00 miss is now
merely a freshness gap, never a mislabel — but the fresh cycle would then
not land until the next month's pull, so freshness still matters.

### 4. Builder relabeling + a unit-check bug found on rerun
`build_usda_comp_tabs.py`:
- Month labels ("Δ from June") now come from `psd_cycle`, not report_date;
  the note row shows both cycle and pull date.
- **Unit-check unanimity rule.** The India soy oil sheet exposed a flaw:
  its production ties PSD in tonnes to 0.2%, but domestic use sits 2.5%
  off the SHORT-TONS factor (because the sheet's trade/use numbers are a
  different source vintage ~7-28% off PSD) — the old "largest snapped
  field wins" rule vetoed the correct thousand-tonnes label and skipped
  the sheet. New rule: a conflicting strong snap only vetoes the label
  when NO field agrees with the label; agreement + conflict ⇒ label kept,
  "mixed snaps" note emitted. Label-less sheets now also require the
  snapped fields to agree with each other.
- Why run 1 and run 2 disagreed on India: pre-run-1 the book (last saved
  by openpyxl, which computes nothing) had NO cached formula values — the
  check saw an empty sheet. Run 1's COM save materialized Excel-computed
  caches; run 2 then read real values. Not damage — COM writes what Excel
  itself would show — but it means **the first COM pass over an
  openpyxl-written book changes what validation sees on the next pass.**

### 5. Fleet rebuilt (3 passes; final pass is authoritative)
All books rebuilt under the final code. argentina_soybean built fine this
time (lock gone). Cottonseed builds under the ruled thousand-short-tons
convention. Note-only tabs (corn oil/flaxseed/safflower) and skips
(lauric rebuilding, soybean complex VBA) unchanged.

## Known-broken / needs Tore
1. **India soy oil sheet source discrepancy**: imports MY2023 4,250 vs PSD
   3,308 (+28%), domestic use ~+13-18%, while production ties PSD exactly.
   Same class as the Argentina peanut oil-sheet discrepancy from the prior
   handoff — probably legacy source values. The comp tab now builds and
   will simply show large USDA-vs-RLC gaps on those lines; worth a look.
2. **Comp tabs no longer share a uniform "current" month** — a series shows
   the cycle USDA last actually revised it (india soybeans: WASDE_JUN_26,
   because PSD didn't touch it in the July cycle; china: WASDE_JUL_26).
   Under pull-date labels every book pretended to be current-month. This
   is honest, but expect vintage names to vary across (and within) books.
3. **wasde_comp in the soybean book (VBA)** now reads relabeled vintages:
   Ctrl+Shift+W will show "July vs June" (correct) instead of "August vs
   July". No VBA change needed — it selects by rank.
4. Aug 12 WASDE is the first live test of the 15:00 pull + cycle labels:
   expect every majors row stamped (2026, 8) and vintage WASDE_AUG_26.
   If any commodity still shows month=7 at 15:00, consider a day-after
   catch-up pull instead of nudging the time again.

## Not done / deferred (unchanged from prior handoff)
- ISO-coded orphan rows cleanup in bronze.fas_psd (harmless, mig queued).
- Queued workstreams: (a) historical WASDE vintage backfill source hunt
  (Cornell/OCE), (b) USDACompUpdater.bas in-book VBA, (c) LLM forecast
  generation into core.forecasts.

## Next-session prompt
> Read docs/handoffs/2026-08-02_wasde_pull_race_fix.md. Pick per Tore's
> priority: (a) historical WASDE vintage backfill source hunt (Cornell
> Mann Library / OCE machine-readable archive — FIRST check whether a
> clean pre-compiled by-release-month dataset exists), (b)
> USDACompUpdater.bas generalized in-book VBA updater, or (c) LLM forecast
> generation into core.forecasts (memory: project_forecast_layer.md,
> project_symbiotic_forecasting.md).
