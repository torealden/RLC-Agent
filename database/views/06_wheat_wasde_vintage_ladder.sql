-- =============================================================================
-- Wheat WASDE Vintage Ladder (Gold)
-- =============================================================================
-- Created: 2026-07-06
-- Purpose: Unpivot bronze.fas_psd (US wheat) into the 13-column LONG flat-file
--          contract used by us_wheat_*.xlsx, one row per (series, report_date)
--          i.e. one row per WASDE release. Feeds us_wheat_wasde.xlsx.
--
-- Additive only -- does not alter bronze.fas_psd or any existing view.
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS gold;

CREATE OR REPLACE VIEW gold.wheat_wasde_vintage_ladder AS
SELECT
    'wheat'                                          AS commodity,
    'ALL'                                             AS class,
    series,
    marketing_year,
    'annual'                                          AS period_type,
    'ANNUAL'                                          AS period,
    'WASDE_' || to_char(report_date, 'YYYY_MM')       AS vintage,
    to_char(report_date, 'YYYYMM')::int               AS vintage_rank,
    value,
    s.unit,
    'USDA_FAS_PSD'                                    AS source,
    report_date                                       AS release_date,
    NULL::numeric                                     AS revision
FROM bronze.fas_psd
CROSS JOIN LATERAL (VALUES
    ('area_harvested',       area_harvested,       '1000 HA'),
    ('yield',                yield,                'MT/HA'),
    ('beginning_stocks',     beginning_stocks,      unit),
    ('production',           production,            unit),
    ('imports',              imports,               unit),
    ('total_supply',         total_supply,          unit),
    ('feed_dom_consumption', feed_dom_consumption,  unit),
    ('fsi_consumption',      fsi_consumption,        unit),
    ('domestic_consumption', domestic_consumption,   unit),
    ('exports',              exports,                unit),
    ('total_distribution',   total_distribution,     unit),
    ('ending_stocks',        ending_stocks,          unit)
) AS s(series, value, unit)
WHERE commodity = 'wheat'
  AND country_code = 'US'
  AND s.value IS NOT NULL
ORDER BY series, vintage_rank DESC;

COMMENT ON VIEW gold.wheat_wasde_vintage_ladder IS
    'US wheat WASDE S&D lines in the 13-col LONG flat-file contract, one row per (series, WASDE release month). Native PSD units (1000 MT / HA), unconverted. Source: bronze.fas_psd via USDA FAS PSD API.';

GRANT SELECT ON gold.wheat_wasde_vintage_ladder TO readonly_role;
