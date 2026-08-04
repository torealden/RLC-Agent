# Handoff 2026-08-04 — USDACompUpdater.bas + donor styling + union-view rebuild (+ perf fix)

Next-session prompt from `2026-08-03_wasde_backfill_loader.md`. Ran ~03:10–04:00 ET.

## Shipped (all verified unless flagged)

1. **Mig 170 — gold.psd_wasde_vintages performance rewrite (unplanned, load-bearing).**
   The first real workload against the mig-168 union hit **165 s per member query
   / 408 s per workbook** (dry run measured, EXPLAIN ANALYZE attached in session):
   `per_cycle`/`horizon`/`kept` were each referenced twice → PG materialized them
   with no statistics → the archive/live `NOT EXISTS` anti-join planned as a
   nested loop scanning the 62k-row `kept` CTE once per archive row (36M
   join-filter comparisons). Rewrite: every CTE referenced once (inlinable,
   predicate pushdown on commodity/country_code — partition keys of every
   window), anti-join replaced by UNION ALL + `row_number()` dedup ordered by
   source priority (PSD first). **Output identical, verified two ways**:
   AR-soybeans golden (79 rows captured pre-migration, byte-identical after) and
   15 assertions on US corn MY2012/2025/2026 (FINAL 90, seventeen rank-79 ties
   ordered by psd_cycle, MY2025 archive 61–67 + live 68–74, drought path values).
   Member query now **0.1 s** (was 165 s). Full-view aggregate 0.9 s.
   Row counts: 62,793 PSD + 45,310 WASDE_ARCHIVE surfaced (46,023 archive rows
   minus 713 shadowed by live on shared cycles — plausible: 2026 overlap cycles).

2. **Mig 169 — psd_cycle + vintage_source appended to gold.fas_us_wasde_comp.**
   That view feeds WASDECompUpdater.bas, which ordered by `vintage_rank DESC`
   alone — the same latent rank-79 tie bug the Python builder fixed. US corn
   MY2012 already carries 17 cycles tied at 79; ACTIVE MYs start tying ~Jan 2027.
   **WASDECompUpdater.bas updated in repo**: tie-break `psd_cycle DESC` added to
   the query, month labels now derived from psd_cycle (mig-166 ruling) with
   report_date fallback. ⚠️ NOT yet re-imported into the three WASDE books
   (corn / wheat / soybean complex) — they run the old module until re-import.
   No urgency (no active-MY ties until 2027), but fold it into the next
   VBA-import pass.

3. **build_usda_comp_tabs.py — donor styling** (donor = argentina_soybean_complex
   usda_comp, Tore-formatted 2026-08-03; full cell-grammar captured before
   coding). Both writers (COM default, openpyxl escape hatch) now emit: Aptos
   Display throughout (donor's "Bierstadt Display" = Aptos' pre-release name),
   merged green MY headers B:D/E:G/I:J (#3C7D22, white bold, centered), banded
   data rows (Background-2-darker-10% ≈ D0CECE, every 2nd row), medium box +
   thin separator borders, centered values with red-parenthesis negatives
   (`#,##0_);[Red](#,##0)`, 1-dp variant for sub-1 factors like million tonnes,
   3-dp area, 0.0% STU), unit line in A of the header row, notes Aptos Narrow 8.
   Layout: A1 = big sheet title — **preserved across rebuilds when hand-set**
   (generated banners containing "USDA (PSD/WASDE)" are not preserved); single
   sheet-level refresh stamp line at the bottom, rewritten every run.
   Deliberate donor-fidelity choice: **only columns A (~40.7) and B (~12.7) get
   explicit widths; C:J stay near Excel default** — that's how Tore left the
   donor. Long delta headers ("Δ from September") will clip at that width;
   flag for Tore if it bothers him in practice.

4. **USDACompUpdater.bas + USDACompWorkbookEvents.bas (NEW, src/tools/).**
   Universal in-book refresher, one module for all ~38 books (WASDECompUpdater
   dispatcher pattern, Ctrl+Shift+U). Refresh-only by design: reads country from
   the folder name, commodity from block titles, MYs from the merged headers,
   units from the builder's unit line (loud skip on anything unrecognized —
   never guesses); writes only non-formula leaf cells; queries
   gold.psd_wasde_vintages with the rank-tie-break. Meta-stamp: rewrites the
   note rows (vintage/cycle/pull) surgically (keeps the unit-check evidence it
   cannot re-derive) and the sheet stamp line on EVERY run, changed data or not
   (feedback_timestamp_every_touch). If sheet MYs no longer match the ladder's
   active MYs it says "re-run the Python builder" instead of improvising.
   **Verified end-to-end via COM on a copy** (import → UpdateUSDAComp_Silent):
   3 blocks, 27/24/24 cells, JUL_26 vs JUN_26, note + stamp rewritten, values
   idempotent vs the builder's. Import into the real books is Tore's pass
   (re-save .xlsx as .xlsm where wanted + import + paste events into
   ThisWorkbook — same procedure as WASDECompUpdater README).
   Two VBA landmines hit and documented in-module: `Array(...)` literals cap at
   24 line continuations (module was UNIMPORTABLE until the commodity map became
   an If-chain), and `IIf` evaluates both branches (error 91 on no-prior blocks).
   File must stay pure ASCII + CRLF (Δ and — built via ChrW).

5. **Fleet rebuild against the union view — done.** 31 books with comp blocks +
   3 note-only + 2 design skips (soybean complex VBA, lauric rebuild) = all 38
   accounted for, zero failures, zero locks. us_cottonseed now builds 3 blocks
   (Tore's thousand-short-tons conversion confirmed by value snap, psd=353).
   Donor book: Tore's "ARGENTINA OILSEEDS COMPLEX" A1 title preserved, styling
   reproduced, and comp values tie the ladder to the digit
   (JUL_26 50000/9000/23820/42000; prior I-col = JUN_26 23821).

## Deep-history / tie-break hand-check (the thing the first rebuild had to prove)

US corn MY2012 through the view: FINAL 90 (PSD, cycle 2016-07, prod 273,192) on
top; **17 cycles tied at rank 79** (Dec-13 → Apr-15) ordered correctly by
psd_cycle (newest = APR_15); drought path intact (May-12 375,680 → Jul-12
329,450 → Jan-13 ladder). MY2025: archive 61–67 (May–Dec 25, Oct missing =
shutdown skip, correct), live PSD 68–74. All 15 scripted assertions passed
post-mig-170, matching the pre-migration capture row-for-row.

## Side finding (data state, not a bug)

**The majors' live PSD chain is one cycle behind the archive** post-mig-166
relabel: BR soybeans' newest LIVE cycle is MAY_26; JUN_26 and JUL_26 on the
ladder come from WASDE_ARCHIVE rows (no area_harvested, published 2-dp
rounding). Comp tabs correctly show the newest known cycle — but Harvested Area
sits blank for such blocks (archive has no area) while its Δ formula shows
0.000, and the note's "pulled <date>" is the archive release date. Self-heals
at the next WASDE pull (Aug 12, 15:00 ET); worth confirming BR/CH/E4 flip back
to vintage_source=PSD after it lands.

## Known-broken / unverified

1. **07:30 drift-check first post-fix scheduled fire** — had not fired yet at
   session close (~04:00 ET); a persistent monitor in the session watches
   core.collection_status and reports (expect one success/scheduler row; a
   FAILED row or no row by 08:30 ET means the fix didn't take).
2. WASDECompUpdater tie-break fix lives in the repo only — the three WASDE
   books still carry the old module (see §2).
3. USDACompUpdater not yet imported into any real book (verified on a copy).
4. openpyxl writer path (`--engine openpyxl`) carries the new styling but was
   NOT exercised this session (fleet ran COM); styling parity on that path is
   by-inspection only.
5. AR peanut-oil ~10% offset and the ISO-orphan mislabeling cleanup remain open
   from prior handoffs.

## Next-session prompt

> Read docs/handoffs/2026-08-04_usda_comp_builder_session.md. Close out the
> drift-check fire if the monitor didn't (query core.collection_status for
> today's claude_md_drift_check row). Then per the priority queue: Argentina
> sunflower complex book (AR = #1 seed exporter; skip rapeseed/lauric), or the
> ECHO enrich hygiene + zombie-run checker pair. After Aug-12 WASDE: rerun
> `python scripts/build_usda_comp_tabs.py`, confirm majors flip back to
> vintage_source=PSD, and have Tore import USDACompUpdater.bas (+ updated
> WASDECompUpdater.bas) on his next VBA pass.
