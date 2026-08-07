# Handoff — Feedstock Report coverage close moved to Friday (2026-08-07)

**Ruled by Tore 2026-08-07.** Supersedes the 2026-08-06 Monday-close ruling that
migration 176 encoded. Migration **177** applied to the live DB.

## Why this changed

Checking whether the "Aug 3 AMS prints" had landed turned up a cadence error, not
a freshness problem. Verified directly at the MARS API over five consecutive
weeks (2026-06-29 → 2026-07-31):

| Report | Backs | Covers | Stamped with | Publishes |
|---|---|---|---|---|
| 3510 / 3511 | SBO, CWG, YG | Mon–Fri | that **Monday** | that **Friday** ~13:30 ET |
| 3618 | DCO | Mon–Fri | that **Friday** | the **following Monday** ~09:00 ET |
| 2839 | — (BFT reads the daily 2837) | Mon–Fri | that Monday | following Monday |

The 2026-08-06 audit called 3510/3511 "Monday reports" and read the missing Aug-3
issue as a late publication. It was not late — it was not yet due, and it posted
Friday 2026-08-07.

The real defect: under a Monday close the window was Tue→Mon and the snapshot ran
Monday evening, at which point the newest 3510/3511 stamp available was the
**prior** Monday — one day before `coverage_start`. SBO, CWG and YG were therefore
flagged `is_carried_forward` with NULL w/w on **every issue, permanently**. An
off-by-one in the cadence, not stale data.

## What shipped

- **Migration 177** (`177_feedstock_report_friday_close.sql`, applied + registered
  in `core.schema_migrations`): realigns `feedstock_issue` rows to the Friday
  close and syncs `week_ending` on the dashboard snapshot. Comment block records
  the *why* — this is the third time the close has moved (Friday → Monday on
  08-06 → Friday now), so the reasoning is in the migration to stop a fourth.
- **`report_config.COVERAGE_WINDOW_DAYS = 4`** — coverage window is the AMS
  reporting week, Mon–Fri inclusive.
- **`snapshot.friday_close_on_or_before()`** replaces `monday_close_on_or_before()`
  (no other callers). `create_issue` derives `week_ending` = Friday on/before the
  issue date, `coverage_start` = that Monday.
- Manual-entry carry-forward windows in `load_manual_csv` now use the same
  constant (were hardcoded `days=6`, in two places).
- Masthead now reads "weekly close", not "settlement close" — these are cash
  prints, not futures settlements.
- Docstrings in `cli.py` / `snapshot.py`, plus the spec and the freshness audit,
  carry the new cadence and an explicit correction of the "Monday reports" claim.

## Cadence going forward

Coverage = AMS week Mon–Fri, closing Friday. **Run the snapshot the FOLLOWING
Monday from ~10:00 ET** — the earliest moment both AMS families are published for
the same week — and publish Monday afternoon or Tuesday.

Running Friday evening (my first recommendation to Tore, before I checked 3618)
would have fixed SBO/CWG/YG and broken DCO the same way — the mirror of the bug.
There is no snapshot moment earlier than Monday that catches both families.

`--date` is the **publish** date; the window is derived from it. Issue 1 is
`--date 2026-08-17` → coverage 2026-08-10 → 2026-08-14.

```
python -m src.reports.feedstock_report.cli issue create --n 1 --date 2026-08-17 --free
python -m src.reports.feedstock_report.cli snapshot prices  --issue 1
python -m src.reports.feedstock_report.cli snapshot credits --issue 1
python -m src.reports.feedstock_report.cli snapshot ifv     --issue 1
# ... section set / news add ...
python -m src.reports.feedstock_report.cli render --issue 1
python -m src.reports.feedstock_report.cli lock   --issue 1
```

## Verification actually run

- `friday_close_on_or_before` asserted across all 7 weekdays: close always lands
  on a Friday on/before the issue date; start always a Monday; span always 4 days.
- **Cadence proven against real data** with a scratch issue 99 dated 2026-08-03
  (coverage 2026-07-27 → 07-31 — the last week where both families are published).
  All five live rows came back **in-window with real w/w**: SBO −6.27%, DCO −3.63%,
  CWG −4.05%, YG +3.73%. BFT w/w NULL — genuine, it is the thin voluntary series
  with no prior-week print, not a cadence artifact. Scratch issue deleted.
- Issue 0 re-rendered clean, zero gate errors, masthead reads
  "Coverage: Aug 03 – Aug 07, 2026 weekly close".
- Acceptance criterion 4 still holds: `--test-ifv-numeric-injection` → GateError,
  **exit 2**, no output written.

## Issue 0 state and the open call for Tore

Issue 0 now covers **Aug 3 → Aug 7** (`issue_date` 2026-08-08 unchanged; the
Friday close derives correctly from it). Snapshot re-run at 10:00 ET today shows
all five rows carried, which is correct-and-expected at that hour:

- SBO / CWG / YG — 3510/3511 for the Aug-3 week published today ~13:30 ET. Re-run
  `snapshot prices --issue 0` after that and they go in-window with real w/w.
- DCO — 3618 for the Aug-3 week does **not** post until Monday 2026-08-10 ~09:00.
  Nothing can pull it forward.

**So Issue 0 cannot be fully in-window before Monday.** The choice is Tore's:

1. **Ship today** after the 13:30 drop — SBO/CWG/YG clean, DCO carried at Jul 31
   with a visible as-of date (7 days old, inside the 21-day stale threshold, so it
   renders rather than dropping to the coverage line). Meets the Fri-EOD gate.
2. **Ship Monday** — re-run after 10:00 ET Aug 10, every row in-window with real
   w/w, and Issue 0 becomes the template for the steady-state cadence.

I'd take (2) if the Friday gate is self-imposed: Issue 0 is the pilot that sets
the pattern, and one clean issue beats a daggered one shipped three days earlier.
Take (1) if the deadline is a commitment to someone else.

## Known-broken / unverified

- **The `OTH` test row is still in the Issue 0 dashboard snapshot** — 50.0¢/lb,
  `last_observed` 2026-06-01, manual entry. At 67 days it is past
  `STALE_EXCLUDE_DAYS` so the renderer pushes it to the coverage line and it never
  shows a price, but it should be deleted rather than relied on being filtered.
  Left in place — deleting a data row is Tore's call, not a code fix.
- MARS `published_date` timezone not confirmed; ~13:30 and ~09:00 assumed ET from
  the raw API timestamps. Cadence conclusions rest on the day-of-week pattern,
  which is unambiguous; only the hour-of-day guidance is soft.
- The written sections are still `[DRAFT]` placeholders — Tore is drafting them.
- `src/reports/feedstock_report/data_pack.py` is an orphan (nothing imports it;
  `cli.py` uses `snapshot` + `render`). It already assumed a Friday `week_ending`.
  Left untouched — dead code, out of scope, but it should be deleted or revived.
- Unchanged from 2026-08-06: FCPO palm feed dead since 2026-03-09 (Task 1);
  scheduler `ams_dco/ams_grain` `'dict' has no attribute 'success'` noise; EPA
  still off the citation whitelist, so no D4 RIN row can render.
