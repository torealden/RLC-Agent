# Handoff — Credit-price provenance audit + EMTS load (2026-08-07/08)

Companion to `2026-08-07_feedstock_report_friday_close.md`. That one covers the
coverage-window ruling; this one covers the EMTS load, the workbook repair, and
the credit-price audit that came out of it.

---

## 1. The headline: `bronze.credit_prices` is one static snapshot, undifferentiated

- **450 monthly rows, all loaded 2026-03-20** (one distinct `created_at`),
  spanning `price_date` 2013-07-01 → **2050-12-01**.
- **330 weekly rows, loaded 2026-03-22**, spanning 2019-01-04 → 2025-04-18.
- Nothing has been loaded since. There is no live feed.

Consequence: **every monthly row after ~2026-03 is a forecast**, and no column
distinguishes it from an observation. The value I reported as "the current D4
price, 97.92¢ @ 2026-08-01" is a projection off a March curve, not a print.
`MAX(price_date) WHERE d4_rin IS NOT NULL` returns **2050-12-01**.

`lcfs_ca` last real print is **2025-04-18** (57.8333 $/MT) — 16 months stale, and
it is the number frozen into `silver.tallow_implied_value.lcfs_value_per_gal` for
every month since (verified: one distinct value across all months from 2025-01).

---

## 2. The Fastmarkets vs EPA divergence — resolved

EPA (canonical, public, now whitelisted) vs Fastmarkets, D4, monthly, full overlap
**2019-01 → 2026-06 (90 months)**:

- Ratio median **1.015**, first-12-month mean **1.044** → the two agree closely for
  five years. This confirms `d4_rin` is stored in **cents/RIN**. There is no unit
  problem.
- Divergence begins **December 2025** and accelerates: 1.28 (Jan-26), 1.45 (Feb),
  1.44 (Mar), 1.64 (Apr), **1.94 (May and Jun)**.
- Direction: EPA rises $1.11 → $1.86 across H1-2026 while Fastmarkets crawls
  87¢ → 96¢. It is Fastmarkets failing to rise, not EPA spiking.

**Both candidate explanations were tested and rejected:**

| Candidate | Test | Result |
|---|---|---|
| Frozen feed | mean month-over-month move, actual portion | **$0.083/mo — it moves.** Not frozen. |
| Vintage mismatch | ratio vs EPA 2024 / 2025 / 2026 vintages separately | Tracks 2024 best (1.20–1.23) but **diverges from all three at once** from Dec-2025. A pure vintage mismatch would sit at ~1.0 against one vintage. |

Remaining hypothesis (untested, needs Fastmarkets' methodology note): the series
assesses a **forward strip rather than spot**. Tore's corollary is important — if
so, the five years of agreement were coincidental (spot and forward converge in a
flat market), the series was never what it appeared to be, and **any historical
analysis built on it needs the same question asked of it.**

**Ruling: EPA is canonical from January 2026. Distrust Fastmarkets.**

---

## 3. The August sawtooth — the most important finding

The forward curve drops **~50% every August, for twenty-five consecutive years**,
then rebuilds. Verified values ($/RIN, from `d4_rin`/100):

| Year | July | August | Drop |
|---|---|---|---|
| 2034 | 1.946 | 1.086 | −44% |
| 2035 | 2.145 | 0.743 | −65% |
| 2036 | 2.312 | 0.958 | −59% |
| 2037 | 1.572 | 0.771 | −51% |
| 2038 | 1.805 | 0.790 | −56% |
| 2039 | 1.825 | 0.835 | −54% |
| 2040 | 1.735 | 0.844 | −51% |

Pattern holds every year through 2050. Tore's read: this is **not** a rollover
artifact to be smoothed away — it is the curve *quoting a different instrument*
each August. If it rolls to the next vintage annually, and the next-year vintage
trades at a discount because it is not yet usable for compliance, this shape is
exactly what you get. In which case the sawtooth is not wrong, it is undeclared:
nothing marks the transition.

**Either way, anything integrating over the curve is wrong** — a sum or average
across it picks up a 50% annual discontinuity with no market meaning, and the
error compounds rather than cancels.

---

## 4. Consumer hunt — who reads `bronze.credit_prices`

No database views or matviews depend on it (checked via `pg_depend`). All
consumers are application code.

**Good news: nothing sums or averages across the curve.** The compounding-error
case does not materialise in current code.

**Already protected:**
- `scripts/build_price_workbooks.py` — explicitly filters to `frequency='weekly'`
  "to skip the forward-curve rows (frequency='monthly' going to 2050)". Someone
  already knew about this.

**Point lookups — correct shape, but consume forecast as observation:**
- `src/kg/callables/implied_feedstock_value.py` (`_resolve_d4_rin_price`,
  `_resolve_lcfs_credit_price`) — `WHERE price_date <= as_of_date ORDER BY
  price_date DESC LIMIT 1`. Bounded, so it never reaches 2050. **But** for any
  current as-of date it returns a forecast point and stamps provenance as
  `bronze.credit_prices.d4_rin @ <date>`, which reads like a print. Its docstring
  already says "static FM snapshot … plus a forward curve out to 2050 — the curve
  handles forward-looking queries", so this was deliberate; it is the *labelling*
  that is wrong, not the lookup.
  **Sawtooth exposure:** a point lookup on a discontinuous curve is sensitive to
  which side of the discontinuity the as-of date falls. An IFV for July 2034 vs
  August 2034 differs ~50% on the D4 leg with no market event.
- `src/engines/feedstock_allocation/allocator.py:503` — tries `weekly` then
  `monthly`. Weekly ends 2025-04-18, so **every period after that silently falls
  through to the forecast curve.**

**Live bugs — unbounded queries:**
- `src/dashboard/biofuels.py:184` — "Latest credit prices",
  `ORDER BY price_date DESC LIMIT 1` with **no `<= CURRENT_DATE` filter**. This
  tile is displaying the **2050-12-01** row as the latest credit price.
- `src/dashboard/biofuels.py:358` — "Credit Stack Tracker" chart,
  `WHERE price_date >= '2019-01-01'` with no upper bound. Plots 2019 → 2050,
  actuals and forecast undifferentiated, rendering **all 25 sawteeth as price
  history**, under the heading "D4 RIN Prices (cents/RIN)".

**Client-facing note:** `dashboards/helios_demo/app.py` describes its source to the
viewer as "bronze.credit_prices forward curve" (line 797) — at least labelled —
but the IFV underneath it uses the point lookup above.

**Writers, not readers** (no action): `ingest_historical.py`,
`ingest_training_prices_v2.py`, `ingest_profitability_workbook.py`.

**Not re-verified this session:** `src/dashboard/showcase.py`,
`src/agents/facility/crush_economics.py`, `hefa_economics.py`,
`scripts/build_per_facility_template.py`. They appear in the grep; I confirmed the
four above and did not read these.

---

## 5. How the file-8 truncation was missed (process note)

`table_export (8).csv` (RIN prices) stopped at 2023-09-18. I concluded EPA had
discontinued the price series. **Wrong** — `table_export (11).csv` is the same
export run complete, is a strict superset (0 keys absent, 0 price differences on
6,664 overlapping rows), and runs to 2026-06-22.

What went wrong, worth keeping because it recurred three times this week:
- I treated a property of the **export** as a fact about the **source**.
- I invented a discriminating tell — "a mid-month Monday cutoff looks like real
  data end, a filter would land on a round boundary" — which was fabricated and is
  falsified by file 8 holding only 480 of 634 rows for 2023.
- **The disconfirming evidence was in a table I printed myself**: prices stopping
  in 2023 next to volumes from the *same weekly EMTS report* running to June 2026.
  Two exports from one report family disagreeing by three years is far better
  evidence of a bad export than a discontinued series. I noted the divergence and
  used it to support the wrong conclusion.

Generalised rule (broader than the memory written 2026-08-07, which was
table-specific): **an artifact's extent is evidence about the artifact, not about
the source.**

---

## 6. The design principle for the 2026-08-24 architecture session

Every failure this week was the same one: **the schema records values without
recording what kind of value they are.**

- proposed vs final (WASDE/PSD vintages)
- actual vs forecast (`credit_prices` 2013–2050 undifferentiated)
- gallons vs RINs (`RFS2_RVO` mixes `billion_gal` and `billion_rins` in one column)
- collected vs hand-entered (`is_manual_entry` written but never read)
- one RIN vintage vs another (D4 Unverified was $2.30 / $1.72 / $1.93 for
  vintages 2024 / 2025 / 2026 **on the same date**)

**Rule to adopt:** every value carries its own provenance and type, and nothing in
a rendering path may consume a value whose type is unmarked. Mandatory,
non-nullable, no defaults. Cheaper to enforce once at the schema level than to
keep discovering as incidents.

Immediate schema consequences:
- Any RIN price row **must** carry a vintage column. EPA supplies it natively
  (`RIN Year` vs `Transfer Year`), so it costs nothing to carry.
- Any price table must carry observation-type (actual / forecast / modelled).

---

## 7. What actually changed in the database and on disk

- **EMTS loaded**: `table_export (5).csv` → `data/raw/rfs_data/rin_generation_06_2026.csv`
  → `python src/tools/emts_csv_loader.py` (the documented process; found via
  Notion, not reinvented). 323 inserted / 3,201 updated. Bronze now 3,524 rows
  through **2026-06**. Silver and gold are all views, so the chain went current
  with no rebuild.
- **Naphtha de-dup**: EPA renamed `Naphtha (EV 1.4/1.5)` → `Renewable Naphtha
  (EV 1.4/1.5)`; the upsert key includes `fuel_category`, so the rename inserted a
  parallel history. Safety check first (0 old-label rows lacked a new-label twin;
  7 differed only by EPA revision). Repointed 4 `reference.emts_column_mapping`
  rows, deleted 215 duplicates. `gold.emts_monthly_matrix` 2026 rows 176 → 200,
  zero unmapped combos.
- **`models/Biofuels/rfs_data.xlsm`**: `ThisWorkbook` is **corrupt** — raises
  "Automation error, Catastrophic failure". 47 document modules for a 23-sheet
  workbook, plus `ThisWorkbook1`/`ThisWorkbook2` (which cannot be deleted in the
  VBE — document modules have Remove disabled). Root cause of both "no banner on
  open" (`Workbook_Open` lives there) and "zero rows updated"
  (`ThisWorkbook.Sheets(tabName)` returned Nothing for all 10 tabs, silently).
  Patched with an `RLC_HostBook()` fallback; 6,976 cells written and saved.
  Predates this session — orphan modules were present in the first read.
- **`models/Biofuels/eia_data.xlsm`**: had `DB_SERVER = "localhost"`; swapped to
  the repo module + imported the missing `ShortcutsHelper`.
- Backups: `rfs_data.xlsm.bak_20260807`, `eia_data.xlsm.bak_20260807`.
- **EPA on `CITATION_WHITELIST`**, `RANGE_MIN_WEEKS = 40` guard, dagger legend in
  the kit PNG — see commits `5c048a1d`, `069ab721`, `e0c497bb`, `2a72100f`.

---

## 8. Known-broken / unverified / open

- **`rfs_data.xlsm` needs a VBA project rebuild.** Select all sheets at once →
  Move or Copy to a new workbook (all at once, or cross-sheet formulas become
  external links) → save as `.xlsm` → import `EMTSDataUpdater.bas` and
  `ShortcutsHelper.bas` → **paste** `EMTSWorkbookEvents.bas` into ThisWorkbook.
  **Importing that events file is almost certainly what created the stray
  ThisWorkbook modules — do not import it.** Until rebuilt, `Workbook_Open` will
  not fire; run `AssignEMTSShortcuts` manually per session.
- **EPA RIN prices lag ~7 weeks** (latest 2026-06-22; the volume file ends the
  same week, so it is a genuine source lag). Against `STALE_EXCLUDE_DAYS = 21`,
  EPA RIN prices can **never** render. Structural: the IFV's largest
  market-priced leg has no current public source. Three options — publish lagged
  and dated; license Argus/OPIS for levels (`LICENSED_LEVEL_SOURCES` /
  `LICENSED_LEVELS_OK` already wired, so it is a contract not a build); or publish
  as change/index. Commercial decision, wanted before 2026-08-25. Note free-tier
  IFV renders rank + arrows only under IFVS-008, so the lag bites only the paid
  tier.
- **RIN price table does not exist.** `table_export (11).csv` (8,449 rows,
  2010-06-28 → 2026-06-22, D3/D4/D5/D6 × Q-RIN/Unverified × vintage) is unloaded.
  Needs a new table with vintage + observation-type mandatory.
- **Files 9 and 10 unloaded** (transaction volumes to 2026-06-22; annual RIN sales
  2010–2026). No schema for either.
- **292 future-dated rows still in `bronze.credit_prices`** — the cleanup was
  logged on the punch list in July and has not been done.
- **Issue 0 was never re-snapshotted/re-rendered** after the 3510/3511 drop on
  2026-08-07 ~13:30. It still carries Jul 27 prints with daggers.
- **`silver.tallow_implied_value` and the IFV are consuming the March forecast as
  a price** — internal correctness issue independent of the commercial decision.
- Unverified: whether Fastmarkets assesses spot or a forward strip. Needs their
  methodology note.
