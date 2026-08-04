-- 170: gold.psd_wasde_vintages restructured for sane query plans.
--      OUTPUT IS IDENTICAL to mig 168 -- this is a performance rewrite only.
--
-- WHY: the mig-168 shape referenced `per_cycle`, `horizon` and `kept` from two
-- places each, which forces PostgreSQL to MATERIALIZE those CTEs. Materialized
-- CTEs have no statistics (est. rows=1), so the archive/live anti-join
-- (`WHERE NOT EXISTS ... FROM kept`) planned as a nested loop that scanned the
-- 62k-row `kept` result once PER archive row: 36M join-filter comparisons and
-- ~165 s for a single (commodity, country) member query. The first comp-tab
-- rebuild against the union (2026-08-04) hit this at 408 s per workbook.
--
-- FIX: every CTE is now referenced exactly ONCE, so PG (12+) inlines the whole
-- chain and pushes (commodity, country_code) predicates down to the base
-- scans -- they are partition keys of every window in the chain, which makes
-- the pushdown legal. The live-wins-on-shared-cycles anti-join is replaced by
-- a UNION ALL + row_number() dedup ordered by source priority (PSD first),
-- which is plan-stable regardless of statistics.
--
-- SEMANTICS PRESERVED (verified against pre-migration goldens: US corn
-- MY2012/2025/2026 ladder, AR soybeans MY2012/2024+):
--   * live PSD rows win shared (commodity, country_code, MY, psd_cycle) --
--     dedup only sees KEPT live rows, exactly like the old NOT EXISTS vs kept;
--   * FINAL = live newest row of a closed MY, rank 90;
--   * everything else WASDE_<MON>_<YY> at 61 + dense_rank(psd_cycle) per MY
--     capped 79; ties at 79 past 19 cycles (order by rank DESC, psd_cycle DESC);
--   * archive-row is_active = MY >= live max MY - 1, false when the pair has
--     no live rows (max FILTER (WHERE PSD) over the deduped partition equals
--     the old `horizon` max: kept retains >= 1 row of every live MY).

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
               ELSE date_trunc('month', p.report_date)::date
           END AS psd_cycle
    FROM bronze.fas_psd p
    WHERE p.report_date IS NOT NULL
),
per_cycle AS (
    SELECT * FROM (
        SELECT c.*,
               row_number() OVER (PARTITION BY c.commodity, c.country_code,
                                               c.marketing_year, c.psd_cycle
                                  ORDER BY c.report_date DESC) AS rn_in_cycle
        FROM cycled c
    ) x
    WHERE rn_in_cycle = 1
),
tagged AS (
    -- is_active from a window max instead of the old `horizon` CTE join
    SELECT m.*,
           (m.marketing_year >=
              max(m.marketing_year) OVER (PARTITION BY m.commodity,
                                                       m.country_code) - 1)
               AS is_active,
           row_number() OVER (PARTITION BY m.commodity, m.country_code,
                                           m.marketing_year
                              ORDER BY m.psd_cycle DESC,
                                       m.report_date DESC) AS rn_my
    FROM per_cycle m
),
merged AS (
    -- live rows: every cycle for an active MY; newest cycle only for a closed
    -- MY. Archive rows: everything; dedup below makes live win shared cycles.
    SELECT commodity, commodity_code, country, country_code, marketing_year,
           report_date, is_active, area_planted, area_harvested, yield,
           beginning_stocks, production, imports, total_supply,
           feed_dom_consumption, fsi_consumption, crush, domestic_consumption,
           exports, total_distribution, ending_stocks, ty_imports, ty_exports,
           unit, psd_cycle,
           'PSD'::text AS vintage_source,
           (NOT is_active) AS is_final,
           1 AS src_priority
    FROM tagged
    WHERE is_active OR rn_my = 1
    UNION ALL
    -- casts pin the union to the live branch's types (varchar widths from
    -- bronze.fas_psd); CREATE OR REPLACE VIEW refuses column-type changes.
    -- Archive is_active is a placeholder recomputed after the dedup.
    SELECT h.commodity::varchar(50), NULL::varchar(20),
           h.country::varchar(100), h.country_code::varchar(10),
           h.marketing_year, h.release_date, false,
           NULL::numeric(18,2), NULL::numeric(18,2), NULL::numeric(18,4),
           h.beginning_stocks::numeric(18,2), h.production::numeric(18,2),
           h.imports::numeric(18,2), h.total_supply::numeric(18,2),
           h.feed_dom_consumption::numeric(18,2), h.fsi_consumption::numeric(18,2),
           h.crush::numeric(18,2), h.domestic_consumption::numeric(18,2),
           h.exports::numeric(18,2), h.total_distribution::numeric(18,2),
           h.ending_stocks::numeric(18,2), NULL::numeric(18,2), NULL::numeric(18,2),
           h.unit::varchar(20), h.psd_cycle,
           'WASDE_ARCHIVE'::text, false, 2
    FROM silver.wasde_historical_vintage h
),
deduped AS (
    SELECT * FROM (
        SELECT m.*,
               row_number() OVER (PARTITION BY m.commodity, m.country_code,
                                               m.marketing_year, m.psd_cycle
                                  ORDER BY m.src_priority) AS rn_dup
        FROM merged m
    ) y
    WHERE rn_dup = 1
),
activated AS (
    -- archive is_active vs the LIVE horizon (max live MY per pair); pairs
    -- with no live rows at all -> false, exactly the old COALESCE(...)
    SELECT d.*,
           CASE WHEN d.vintage_source = 'PSD' THEN d.is_active
                ELSE COALESCE(
                       d.marketing_year >=
                         max(d.marketing_year)
                             FILTER (WHERE d.vintage_source = 'PSD')
                             OVER (PARTITION BY d.commodity,
                                                d.country_code) - 1,
                       false)
           END AS is_active_u
    FROM deduped d
)
SELECT
    commodity,
    commodity_code,
    country,
    country_code,
    marketing_year,
    report_date,
    is_active_u AS is_active_my,
    CASE WHEN is_final THEN 'FINAL'
         ELSE 'WASDE_' || upper(to_char(psd_cycle, 'Mon_YY'))
    END AS vintage,
    CASE WHEN is_final THEN 90
         -- cycle order over the union, newest = highest; capped at 79 below the
         -- actuals band (80 CENSUS_CIR / 85 CIR / 90 FINAL / 95 EIA). MYs with
         -- >19 cycles tie at 79: rank is order-only, break ties on psd_cycle.
         ELSE least(60 + dense_rank() OVER (PARTITION BY commodity, country_code, marketing_year
                                            ORDER BY psd_cycle)::int, 79)
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
    psd_cycle,
    vintage_source
FROM activated;

COMMENT ON VIEW gold.psd_wasde_vintages IS
'PSD/WASDE releases on the shared vintage ladder: mig-166 live chain (labeled by PSD cycle '
'stamp) UNIONed with the backfilled WASDE archive (silver.wasde_historical_vintage, Apr 2010+); '
'live wins on shared cycles. FINAL = live newest row of a closed MY at rank 90; all other rows '
'WASDE_<MON>_<YY> at 61-79 in cycle order per MY (ties at 79 past 19 cycles — order by '
'vintage_rank DESC, psd_cycle DESC). vintage_source: PSD = API precision, WASDE_ARCHIVE = '
'published rounding (up to +/-5 units coarser). See migs 149/166/168/170 (170 = perf rewrite, '
'identical output).';

COMMIT;
