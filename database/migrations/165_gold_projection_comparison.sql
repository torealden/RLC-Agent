-- 165_gold_projection_comparison.sql
--
-- Unified long-format surface for the Market Dashboard's Projection Comparison
-- page: Tore's estimates vs LLM forecasts vs USDA WASDE vintages vs realized
-- actuals, one row per (commodity, country, MY, source_type, vintage, metric).
--
-- WHY A VIEW: unit and country normalization across the four books is the exact
-- place earlier work went wrong (gold.user_vs_psd_comparison subtracts mil bu
-- from 1000 MT with no conversion). Writing the normalization down ONCE, in a
-- reviewable migration, means the dashboard, MCP tools, and future scripts all
-- read the same answer instead of re-deriving it.
--
-- NORMALIZATION DECISIONS (verified against live data 2026-08-01):
--   country  -> country_code style ('US','BR',...): silver.monthly_realized and
--               core.forecasts already use codes; gold.psd_wasde_vintages
--               carries both; silver.user_sd_estimate uses full names and is
--               mapped via CASE below.
--   units    -> value_1000mt is CONSERVATIVE: only unambiguous mass conversions
--               are performed ('1000 MT' passthrough; raw LB; 'mil lbs';
--               'mil bu' for known bushel weights). 'TONS' is left native
--               (NULL value_1000mt) because short-vs-metric is unverified for
--               those NASS rows -- a wrong 10% factor is worse than no number.
--               yield/area/ratio metrics are never converted.
--   metrics  -> USDA columns are renamed to the user-table vocabulary
--               (domestic_consumption->domestic_use, total_distribution->
--               total_use, feed_dom_consumption->feed_residual,
--               fsi_consumption->fsi) so the same metric key lines up across
--               sources. Realized keeps its attribute vocabulary as the metric
--               key ('crush' aligns; oil_* attributes are their own series).
--   realized -> multiple sources publish the SAME month (verified: ERS_OCY
--               republishes NASS_SOY_CRUSH monthly soy crush to the pound;
--               a naive SUM across sources exactly doubles MY2022-23 crush).
--               One source is picked per (commodity, country, MY, attribute,
--               month): NASS_* > ERS_* > everything else, then alphabetical.
--               After dedup, flow attributes are SUMmed over the MY with
--               month_count exposed so consumers can flag partial years;
--               attributes containing 'stock' take the LAST reported month
--               instead (summing stocks is meaningless).
--   llm      -> core.forecasts rows whose source contains 'actual' are
--               excluded (they are actuals parked in the forecast table, e.g.
--               nopa_release_actual). marketing_year is free TEXT there; the
--               first 4-digit run is parsed as the MY, unparseable -> NULL
--               (still visible in the coverage view).
--
-- WASDE CAVEAT for consumers: vintage depth in bronze.fas_psd is 2026-only
-- (8 report dates Jan-Jul 2026). Closed MYs collapse to a single FINAL row --
-- a per-release revision path exists ONLY for active marketing years.

BEGIN;

CREATE OR REPLACE VIEW gold.projection_comparison_long AS
WITH user_src AS (
    SELECT LOWER(u.commodity) AS commodity,
           CASE u.country
               WHEN 'United States' THEN 'US'
               WHEN 'Brazil'        THEN 'BR'
               WHEN 'Argentina'     THEN 'AR'
               WHEN 'China'         THEN 'CN'
               ELSE u.country
           END                       AS country_code,
           u.marketing_year,
           'user'::text              AS source_type,
           COALESCE(u.source_file, 'RLC estimate') AS source_detail,
           u.estimate_date           AS vintage_date,
           u.is_current              AS is_latest,
           NULL::int                 AS vintage_rank,
           m.metric,
           m.value                   AS value_native,
           u.unit                    AS unit_native,
           NULL::numeric             AS confidence_low,
           NULL::numeric             AS confidence_high,
           NULL::bigint              AS month_count
    FROM silver.user_sd_estimate u
    CROSS JOIN LATERAL (VALUES
        ('area_planted',     u.area_planted),
        ('area_harvested',   u.area_harvested),
        ('yield',            u.yield),
        ('beginning_stocks', u.beginning_stocks),
        ('production',       u.production),
        ('imports',          u.imports),
        ('total_supply',     u.total_supply),
        ('crush',            u.crush),
        ('feed_residual',    u.feed_residual),
        ('fsi',              u.fsi),
        ('ethanol',          u.ethanol),
        ('domestic_use',     u.domestic_use),
        ('exports',          u.exports),
        ('total_use',        u.total_use),
        ('ending_stocks',    u.ending_stocks),
        ('stocks_use_ratio', u.stocks_use_ratio)
    ) AS m(metric, value)
    WHERE m.value IS NOT NULL
),
llm_src AS (
    SELECT LOWER(f.commodity),
           f.country,
           NULLIF(substring(f.marketing_year FROM '\d{4}'), '')::int,
           'llm',
           f.source || COALESCE(' / ' || f.analyst, ''),
           f.forecast_date,
           TRUE,
           NULL::int,
           f.forecast_type,
           f.value,
           f.unit,
           f.confidence_low,
           f.confidence_high,
           NULL::bigint
    FROM core.forecasts f
    WHERE f.source NOT ILIKE '%actual%'
),
usda_src AS (
    SELECT LOWER(p.commodity),
           p.country_code,
           p.marketing_year,
           'usda',
           p.vintage,
           p.report_date,
           p.vintage_rank = MAX(p.vintage_rank) OVER
               (PARTITION BY p.commodity, p.country_code, p.marketing_year),
           p.vintage_rank,
           m.metric,
           m.value,
           p.unit,
           NULL::numeric,
           NULL::numeric,
           NULL::bigint
    FROM gold.psd_wasde_vintages p
    CROSS JOIN LATERAL (VALUES
        ('area_planted',     p.area_planted),
        ('area_harvested',   p.area_harvested),
        ('yield',            p.yield),
        ('beginning_stocks', p.beginning_stocks),
        ('production',       p.production),
        ('imports',          p.imports),
        ('total_supply',     p.total_supply),
        ('crush',            p.crush),
        ('feed_residual',    p.feed_dom_consumption),
        ('fsi',              p.fsi_consumption),
        ('domestic_use',     p.domestic_consumption),
        ('exports',          p.exports),
        ('total_use',        p.total_distribution),
        ('ending_stocks',    p.ending_stocks)
    ) AS m(metric, value)
    WHERE m.value IS NOT NULL
),
realized_dedup AS (
    -- one source per month-cell; NASS_* beats ERS_* republication
    SELECT DISTINCT ON (LOWER(r.commodity), r.country, r.marketing_year,
                        r.attribute, r.calendar_year, r.month)
           LOWER(r.commodity) AS commodity,
           r.country, r.marketing_year, r.attribute,
           r.calendar_year, r.month, r.realized_value, r.unit,
           r.source, r.report_date
    FROM silver.monthly_realized r
    ORDER BY LOWER(r.commodity), r.country, r.marketing_year, r.attribute,
             r.calendar_year, r.month,
             CASE WHEN r.source LIKE 'NASS%' THEN 1
                  WHEN r.source LIKE 'ERS%'  THEN 2
                  ELSE 3 END,
             r.source
),
realized_flows AS (
    SELECT r.commodity,
           r.country,
           r.marketing_year,
           'realized',
           string_agg(DISTINCT r.source, '+'),
           MAX(r.report_date),
           TRUE,
           NULL::int,
           r.attribute,
           SUM(r.realized_value),
           MAX(r.unit),
           NULL::numeric,
           NULL::numeric,
           COUNT(DISTINCT r.calendar_year * 100 + r.month)
    FROM realized_dedup r
    WHERE r.attribute NOT ILIKE '%stock%'
    GROUP BY r.commodity, r.country, r.marketing_year, r.attribute
),
realized_stocks AS (
    -- stocks are levels, not flows: take the last reported month of the MY
    SELECT DISTINCT ON (r.commodity, r.country, r.marketing_year, r.attribute)
           r.commodity,
           r.country,
           r.marketing_year,
           'realized',
           r.source,
           r.report_date,
           TRUE,
           NULL::int,
           r.attribute,
           r.realized_value,
           r.unit,
           NULL::numeric,
           NULL::numeric,
           1::bigint
    FROM realized_dedup r
    WHERE r.attribute ILIKE '%stock%'
    ORDER BY r.commodity, r.country, r.marketing_year, r.attribute,
             r.calendar_year DESC, r.month DESC
),
unioned (commodity, country_code, marketing_year, source_type, source_detail,
         vintage_date, is_latest, vintage_rank, metric, value_native,
         unit_native, confidence_low, confidence_high, month_count) AS (
    SELECT * FROM user_src
    UNION ALL SELECT * FROM llm_src
    UNION ALL SELECT * FROM usda_src
    UNION ALL SELECT * FROM realized_flows
    UNION ALL SELECT * FROM realized_stocks
)
SELECT s.*,
       CASE
           WHEN s.metric IN ('yield', 'area_planted', 'area_harvested',
                             'stocks_use_ratio') THEN NULL
           WHEN s.unit_native = '1000 MT' THEN s.value_native
           WHEN s.unit_native IN ('LB', 'LB, NET WEIGHT', 'LB, RAW BASIS')
               THEN s.value_native * 0.00000045359237      -- raw lb -> 1000 MT
           WHEN s.unit_native IN ('mil lbs', 'million_lbs')
               THEN s.value_native * 0.45359237            -- mil lb -> 1000 MT
           WHEN s.unit_native = 'mil bu' AND s.commodity IN ('soybeans', 'wheat')
               THEN s.value_native * 27.2155               -- 60-lb bushel
           WHEN s.unit_native = 'mil bu' AND s.commodity = 'corn'
               THEN s.value_native * 25.4012               -- 56-lb bushel
           ELSE NULL                                       -- native only
       END AS value_1000mt
FROM unioned s;

COMMENT ON VIEW gold.projection_comparison_long IS
    'User/LLM/USDA projections + realized actuals in one long format for the '
    'Market Dashboard comparison page. value_1000mt is NULL wherever a '
    'conversion would be a guess -- consumers must fall back to native units '
    'and say so. See migration 165 header for normalization decisions.';

CREATE OR REPLACE VIEW gold.projection_comparison_coverage AS
SELECT commodity, country_code, metric, source_type,
       COUNT(*)                     AS n_rows,
       COUNT(DISTINCT vintage_date) AS n_vintages,
       MAX(vintage_date)            AS latest_vintage,
       MIN(marketing_year)          AS my_min,
       MAX(marketing_year)          AS my_max
FROM gold.projection_comparison_long
GROUP BY commodity, country_code, metric, source_type;

COMMENT ON VIEW gold.projection_comparison_coverage IS
    'Per-source coverage counts behind the comparison page''s honesty chips '
    '(e.g. "LLM: 0 forecasts for this series").';

COMMIT;
