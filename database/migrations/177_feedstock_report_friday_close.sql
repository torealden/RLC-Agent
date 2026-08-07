-- Migration 177: The Feedstock Report — coverage window back to a FRIDAY close
--
-- Ruled 2026-08-07 by Tore. Supersedes the 2026-08-06 Monday-close ruling that
-- migration 176 encoded (which had itself replaced an earlier Friday close).
--
-- WHY, so this does not flip a third time:
--   The AMS weeklies that back most of the price dashboard — 3510 (National
--   Animal By-Product Feedstuff, backs CWG + YG) and 3511 (Soybean Meal/
--   Feedstuffs, backs SBO) — cover Monday through Friday, publish that SAME
--   Friday at ~13:30 ET, and are stamped with the week's MONDAY as report_date.
--   Verified at the MARS API over five consecutive weeks 2026-06-29..2026-07-31.
--
--   Under the Monday close the coverage window was Tue..Mon, and the snapshot
--   ran Monday evening. The newest AMS stamp available at that moment was the
--   PRIOR Monday — one day before coverage_start. So SBO, CWG and YG could
--   never contain the print for their own coverage week and were flagged
--   is_carried_forward on every issue, permanently, with NULL w/w. That is an
--   off-by-one in the cadence, not a data-freshness problem.
--
--   Friday close + Mon-Fri window puts each AMS weekly inside its own window:
--   a print stamped Monday Aug 3 lands on coverage_start of the Aug 3 -> Aug 7
--   window, and one stamped Friday Aug 7 lands on its close.
--
--   The Mon-Fri window is also the only one that holds BOTH AMS families,
--   which cover the same week but stamp and publish differently:
--     3510/3511 (SBO, CWG, YG) — stamped the week's MONDAY, published that
--                                FRIDAY ~13:30 ET.
--     3618      (DCO)          — stamped the week's FRIDAY, published the
--                                FOLLOWING MONDAY ~09:00 ET.
--
-- Cadence after this migration:
--   coverage_start = Monday, week_ending = that Friday (close). Run the
--   snapshot the MONDAY AFTER the coverage week from ~10:00 ET — the earliest
--   moment both families are out for that week — and publish that afternoon or
--   Tuesday. A Friday-evening run would leave DCO carried forward every issue,
--   the mirror of the bug this migration fixes.
--   Derivation lives in snapshot.friday_close_on_or_before() +
--   report_config.COVERAGE_WINDOW_DAYS = 4.
--
-- Data effect: realigns existing feedstock_issue rows to the new convention.
-- Snapshot rows are NOT rewritten here — re-run `snapshot prices --issue N`
-- after the Friday AMS drop to repopulate them against the new window.

BEGIN;

-- Realign every issue to the Friday close derived from its own issue_date:
-- back up to the most recent Friday on or before the issue date, then take
-- that week's Monday as coverage_start.
UPDATE reports.feedstock_issue
SET week_ending    = issue_date - ((EXTRACT(ISODOW FROM issue_date)::int - 5 + 7) % 7),
    coverage_start = issue_date - ((EXTRACT(ISODOW FROM issue_date)::int - 5 + 7) % 7) - 4,
    updated_at     = NOW();

-- The dashboard/credit snapshots stamp week_ending as the coverage close; keep
-- them consistent with the parent issue so a re-render before the next snapshot
-- run cannot show two different closes.
UPDATE reports.feedstock_price_dashboard_snapshot s
SET week_ending = i.week_ending
FROM reports.feedstock_issue i
WHERE i.id = s.issue_id AND s.week_ending IS DISTINCT FROM i.week_ending;

COMMIT;

-- Verification (expect close on Friday, start on Monday, span of 4 days):
--   SELECT issue_number, issue_date, coverage_start, week_ending,
--          to_char(coverage_start,'Dy') AS start_dow,
--          to_char(week_ending,'Dy')    AS close_dow,
--          week_ending - coverage_start AS span_days
--   FROM reports.feedstock_issue ORDER BY issue_number;
