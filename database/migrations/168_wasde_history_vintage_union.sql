-- 168_wasde_history_vintage_union.sql
--
-- Puts the backfilled WASDE archive (bronze.wasde_historical, mig 167) onto the
-- shared vintage ladder. Two objects:
--
--   1. silver.wasde_historical_vintage — PSD-shaped rows transformed from the
--      archive's seven World Supply-and-Use tables by
--      scripts/transform_wasde_history_to_vintages.py (assertions live there).
--   2. gold.psd_wasde_vintages redefined as the UNION of the mig-166 live chain
--      and the archive rows, ONE ladder recomputed (Tore ruling 2026-08-03):
--        * live PSD rows win on shared (commodity, country_code, MY, psd_cycle);
--        * FINAL stays the live newest row of a closed MY, rank 90;
--        * every other row is WASDE_<MON>_<YY> at 61 + dense_rank(psd_cycle)
--          per MY, capped 79 — mig-166 semantics over the union. Existing live
--          ranks RENUMBER (rank = order only, identity is the vintage text;
--          reference_vintage_rank_ladder). MYs with >19 cycles tie at 79, so
--          consumers ordering by rank need a psd_cycle tie-break
--          (build_usda_comp_tabs.py fixed alongside this migration).
--
-- Column list preserved; vintage_source appended at the end (CREATE OR REPLACE-
-- safe, same pattern as mig 166's psd_cycle append). vintage_source labels the
-- provenance: 'PSD' (API pull, full precision) vs 'WASDE_ARCHIVE' (published
-- rounding: MMT/M-bales at 2 dp — up to +/-5 units coarser; do not chase
-- sub-0.01-MMT "revisions" across the two sources).
--
-- Units: archive rows are 1000 MT except cotton in 1000 480-lb Bales (native
-- WASDE precision). Live cotton is ALREADY mixed (383 rows 1000 MT, 59 rows
-- bales since the 2026-03 cycle) — pre-existing collector inconsistency, not
-- addressed here; the unit column is truthful per row.

BEGIN;

CREATE TABLE IF NOT EXISTS silver.wasde_historical_vintage (
    commodity            text        NOT NULL,  -- PSD label: corn, wheat, soybeans, soybean_meal, soybean_oil, rice, cotton
    country              text        NOT NULL,  -- WASDE region label, e.g. 'United States'
    country_code         text        NOT NULL,  -- PSD/FAS (FIPS-style) code: US, BR, CH, E4, SF, NI, ...
    marketing_year       integer     NOT NULL,
    psd_cycle            date        NOT NULL,  -- first of the release month
    wasde_number         integer     NOT NULL,
    release_date         date        NOT NULL,
    beginning_stocks     numeric,
    production           numeric,
    imports              numeric,
    exports              numeric,
    ending_stocks        numeric,
    domestic_consumption numeric,
    feed_dom_consumption numeric,               -- corn/wheat only (world tables)
    fsi_consumption      numeric,               -- derived: domestic - feed, where feed exists
    crush                numeric,               -- soybeans only
    total_supply         numeric,               -- derived: beg + prod + imports
    total_distribution   numeric,               -- derived: dom + exports (+ loss for cotton)
    unit                 text        NOT NULL,
    loaded_at            timestamptz NOT NULL DEFAULT now(),
    last_touched_at      timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (commodity, country_code, marketing_year, psd_cycle)
);

COMMENT ON TABLE silver.wasde_historical_vintage IS
'WASDE as-published S&D vintages transformed from bronze.wasde_historical world tables '
'(Apr 2010 onward). Units: 1000 MT, cotton 1000 480-lb Bales. WASDE-published rounding '
'(coarser than PSD API rows). World (WD) domestic/fsi are definitionally different from '
'PSD WD aggregates (WASDE world trade imbalance treatment) — tie-outs assert country rows '
'strictly, WD only on production/stocks. See docs/specs/wasde_vintage_backfill_v1.md.';

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
latest_in_cycle AS (
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
    -- live rows: every cycle for an active MY; newest cycle only for a closed MY
    SELECT * FROM tagged WHERE is_active OR rn_my = 1
),
hist AS (
    -- archive rows the live chain does not already cover (live wins on shared cycles)
    SELECT h.*,
           COALESCE(h.marketing_year >= hz.max_my - 1, false) AS is_active
    FROM silver.wasde_historical_vintage h
    LEFT JOIN horizon hz
      ON hz.commodity = h.commodity AND hz.country_code = h.country_code
    WHERE NOT EXISTS (
        SELECT 1 FROM kept k
        WHERE k.commodity = h.commodity
          AND k.country_code = h.country_code
          AND k.marketing_year = h.marketing_year
          AND k.psd_cycle = h.psd_cycle)
),
unioned AS (
    SELECT commodity, commodity_code, country, country_code, marketing_year,
           report_date, is_active, area_planted, area_harvested, yield,
           beginning_stocks, production, imports, total_supply,
           feed_dom_consumption, fsi_consumption, crush, domestic_consumption,
           exports, total_distribution, ending_stocks, ty_imports, ty_exports,
           unit, psd_cycle,
           'PSD'::text AS vintage_source,
           (NOT is_active) AS is_final
    FROM kept
    UNION ALL
    -- casts pin the union to the live branch's types (varchar widths from
    -- bronze.fas_psd); CREATE OR REPLACE VIEW refuses column-type changes
    SELECT commodity::varchar(50), NULL::varchar(20), country::varchar(100),
           country_code::varchar(10), marketing_year,
           release_date, is_active,
           NULL::numeric(18,2), NULL::numeric(18,2), NULL::numeric(18,4),
           beginning_stocks::numeric(18,2), production::numeric(18,2),
           imports::numeric(18,2), total_supply::numeric(18,2),
           feed_dom_consumption::numeric(18,2), fsi_consumption::numeric(18,2),
           crush::numeric(18,2), domestic_consumption::numeric(18,2),
           exports::numeric(18,2), total_distribution::numeric(18,2),
           ending_stocks::numeric(18,2), NULL::numeric(18,2), NULL::numeric(18,2),
           unit::varchar(20), psd_cycle,
           'WASDE_ARCHIVE'::text, false
    FROM hist
)
SELECT
    commodity,
    commodity_code,
    country,
    country_code,
    marketing_year,
    report_date,
    is_active AS is_active_my,
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
FROM unioned;

COMMENT ON VIEW gold.psd_wasde_vintages IS
'PSD/WASDE releases on the shared vintage ladder: mig-166 live chain (labeled by PSD cycle '
'stamp) UNIONed with the backfilled WASDE archive (silver.wasde_historical_vintage, Apr 2010+); '
'live wins on shared cycles. FINAL = live newest row of a closed MY at rank 90; all other rows '
'WASDE_<MON>_<YY> at 61-79 in cycle order per MY (ties at 79 past 19 cycles — order by '
'vintage_rank DESC, psd_cycle DESC). vintage_source: PSD = API precision, WASDE_ARCHIVE = '
'published rounding (up to +/-5 units coarser). See migs 149/166/168.';

COMMIT;
