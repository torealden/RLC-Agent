-- 166_psd_vintage_cycle_relabel.sql
--
-- Fixes the WASDE-day pull race documented in docs/handoffs/2026-08-01_usda_comp_tabs.md §4.
--
-- THE BUG: gold.psd_wasde_vintages (migration 149) labeled vintages from the PULL date
-- ('WASDE_' || to_char(report_date, 'Mon_YY')). The scheduled collector fires at 12:00 ET
-- on WASDE day and the dispatcher tick lands ~12:14 ET, while PSD Online is still
-- propagating tables. The race is PER-COMMODITY: on the 2026-07-10 pull, soybeans/wheat/
-- rice were already at PSD cycle month=7 (post-release) but ALL corn rows were month<=6
-- (pre-release). So 'WASDE_JUL_26' for corn actually held JUNE WASDE values, and the true
-- July values arrived under an Aug-1 manual pull mislabeled 'WASDE_AUG_26'.
--
-- VERIFIED SEMANTICS of PSD's month/calendar_year attributes (2026-08-02, live queries):
--   * (calendar_year, month) is a per-row "last updated in this PSD release cycle" stamp,
--     NOT the release month of the pull. A single full pull carries stamps spanning years
--     (2026-01-30 pull has month=11/12 rows stamped 2023/2024/2025 — rows untouched since
--     those cycles).
--   * Same stamp => identical values: the 184 Aug-1 rows deleted as exact duplicates in
--     the prior session were precisely the rows whose July-cycle stamp matched the Jul-10
--     pull. Rows that DIFFERED across pulls always carried different cycle stamps.
--     ("Intra-month PSD revisions" from the prior handoff dissolve under this reading —
--     they were cycle differences mislabeled by pull date.)
--   * Corn US MY2025 trace: pulls 5/12, 6/11, 7/10 (all ~12:14 ET) stamped month 4, 5, 6
--     respectively — every scheduled WASDE-day pull captured the PRIOR cycle for corn.
--
-- THE FIX: identity of a vintage is the PSD CYCLE STAMP, not the pull date.
--   * psd_cycle = make_date(calendar_year, month, 1). calendar_year is NULL on the five
--     scheduled pulls (collector dropped it — fixed in usda_wasde_collector.py alongside
--     this migration); derive it from report_date: a stamp month later in the calendar
--     than the pull month must be the prior year (Dec stamp on a Jan pull). Correct as
--     long as no NULL-calendar_year row is >12 months stale, true for the affected pulls
--     (all 2026, active MYs only).
--   * month=0 rows (2,046 sugar rows, MY1990-2003, from the 2026-03-15 backfill) carry no
--     usable stamp; they fall back to the pull-month bucket. All are closed MYs, so they
--     collapse to FINAL regardless.
--   * Dedup partition changes from (…, pull calendar month) to (…, psd_cycle): multiple
--     pulls carrying the same cycle collapse to the newest pull; one pull carrying rows
--     from several cycles (normal — unrevised rows keep old stamps) is now split correctly.
--
-- CONSEQUENCES for existing labels: corn-like raced rows shift one month earlier
-- (WASDE_AUG_26 -> WASDE_JUL_26 etc.). There is NO WASDE_AUG_26 anywhere until the
-- Aug 12 WASDE is pulled. vintage_rank stays 61..79 in cycle order, higher = newer;
-- FINAL stays 90. Column list is unchanged except psd_cycle APPENDED at the end
-- (CREATE OR REPLACE-safe; the mig-165 comparison view and gold.fas_us_wasde_comp
-- reference columns by name and are unaffected).
--
-- Consumers (scripts/build_usda_comp_tabs.py, WASDECompUpdater.bas) select by
-- vintage/vintage_rank and need no changes; displayed vintage names simply become correct.

BEGIN;

CREATE OR REPLACE VIEW gold.psd_wasde_vintages AS
WITH cycled AS (
    SELECT p.*,
           CASE
               WHEN p.month BETWEEN 1 AND 12 THEN
                   make_date(
                       COALESCE(
                           p.calendar_year,
                           CASE WHEN p.month <= EXTRACT(MONTH FROM p.report_date)::int
                                THEN EXTRACT(YEAR FROM p.report_date)::int
                                ELSE EXTRACT(YEAR FROM p.report_date)::int - 1
                           END),
                       p.month, 1)
               -- month=0/NULL (sugar backfill history): no cycle stamp; bucket by pull month
               ELSE date_trunc('month', p.report_date)::date
           END AS psd_cycle
    FROM bronze.fas_psd p
    WHERE p.report_date IS NOT NULL
),
latest_in_cycle AS (
    -- Several pulls can carry the same PSD cycle for a row (same stamp => identical
    -- values, verified); keep the newest pull. This replaces mig 149's
    -- latest-pull-per-calendar-month partition, which conflated pull month with cycle.
    SELECT c.*,
           row_number() OVER (PARTITION BY c.commodity, c.country_code, c.marketing_year,
                                           c.psd_cycle
                              ORDER BY c.report_date DESC) AS rn_in_cycle
    FROM cycled c
),
per_cycle AS (
    SELECT * FROM latest_in_cycle WHERE rn_in_cycle = 1
),
horizon AS (
    SELECT commodity, country_code, max(marketing_year) AS max_my
    FROM per_cycle
    GROUP BY 1, 2
),
tagged AS (
    SELECT m.*,
           (m.marketing_year >= h.max_my - 1) AS is_active,
           row_number() OVER (PARTITION BY m.commodity, m.country_code, m.marketing_year
                              ORDER BY m.psd_cycle DESC, m.report_date DESC) AS rn_my
    FROM per_cycle m
    JOIN horizon h
      ON h.commodity = m.commodity
     AND h.country_code = m.country_code
),
kept AS (
    -- every cycle for an active MY; newest cycle only for a closed MY
    SELECT * FROM tagged WHERE is_active OR rn_my = 1
)
SELECT
    commodity,
    commodity_code,
    country,
    country_code,
    marketing_year,
    report_date,
    is_active AS is_active_my,
    CASE WHEN is_active
         THEN 'WASDE_' || upper(to_char(psd_cycle, 'Mon_YY'))
         ELSE 'FINAL'
    END AS vintage,
    CASE WHEN is_active
         -- cycle order, newest cycle = highest rank; capped at 79 below the actuals band
         -- (80 CENSUS_CIR / 85 CIR / 90 FINAL / 95 EIA), same ladder as mig 149.
         THEN least(60 + dense_rank() OVER (PARTITION BY commodity, country_code, marketing_year
                                            ORDER BY psd_cycle)::int, 79)
         ELSE 90
    END AS vintage_rank,
    area_planted,
    area_harvested,
    yield,
    beginning_stocks,
    production,
    imports,
    total_supply,
    feed_dom_consumption,
    fsi_consumption,
    crush,
    domestic_consumption,
    exports,
    total_distribution,
    ending_stocks,
    ty_imports,
    ty_exports,
    unit,
    psd_cycle
FROM kept;

COMMENT ON VIEW gold.psd_wasde_vintages IS
'PSD/WASDE releases on the shared vintage ladder, labeled by PSD''s own release-cycle stamp '
'(calendar_year+month = last cycle the row was updated in), NOT by pull date — the 12:00 ET '
'scheduled pull races PSD propagation and can capture the prior cycle per-commodity (mig 166). '
'higher vintage_rank = more recent. Active MYs (current + next) get WASDE_<MON>_<YY> at ranks '
'61-79 in cycle order; closed MYs collapse to FINAL at 90. report_date is the pull that carried '
'the kept row; psd_cycle is the authoritative cycle. All countries and commodities. See mig 149/166.';

COMMIT;
