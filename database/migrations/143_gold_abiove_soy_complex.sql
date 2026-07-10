-- 143_gold_abiove_soy_complex.sql
-- Analytics view over the Abiove Brazilian soy-complex monthly series, shaped to the
-- flat-file contract (docs/specs/flat_file_contract.md, 13-column LONG). Units are
-- thousand metric tons (per "thousand tonnes for all non-US commodities" rule).
--
-- vintage ladder: final Abiove months = ACTUAL (rank 99); "(amostra)" sample months
-- (is_preliminary) = SAMPLE (rank 90) so a later revised ACTUAL supersedes via MAXIFS.

CREATE SCHEMA IF NOT EXISTS gold;

CREATE OR REPLACE VIEW gold.abiove_soy_complex_monthly AS
SELECT
    'soybeans'::text                              AS commodity,
    'ALL'::text                                   AS class,
    CASE attribute
        WHEN 'crush'                THEN 'crush'
        WHEN 'meal_production'      THEN 'meal_production'
        WHEN 'oil_production_crude' THEN 'oil_production'
        WHEN 'seed_stocks'          THEN 'seed_stocks'
        WHEN 'meal_stocks'          THEN 'meal_stocks'
        WHEN 'oil_stocks'           THEN 'oil_stocks'
    END                                           AS series,
    calendar_year                                 AS marketing_year,   -- BR calendar-native
    'month'::text                                 AS period_type,
    month                                         AS period,           -- calendar month 1..12
    CASE WHEN is_preliminary THEN 'SAMPLE' ELSE 'ACTUAL' END AS vintage,
    CASE WHEN is_preliminary THEN 90 ELSE 99 END  AS vintage_rank,
    realized_value                                AS value,            -- thousand metric tons
    '1000 MT'::text                               AS unit,
    'ABIOVE'::text                                AS source,
    report_date                                   AS release_date,
    NULL::int                                     AS revision
FROM silver.monthly_realized
WHERE source = 'ABIOVE' AND country = 'BR'
  AND attribute IN ('crush','meal_production','oil_production_crude',
                    'seed_stocks','meal_stocks','oil_stocks');

COMMENT ON VIEW gold.abiove_soy_complex_monthly IS
    'Abiove Brazil soy-complex monthly (crush, meal/oil production, bean/meal/oil stocks) in thousand MT, flat-file-contract shape. Feeds scripts/write_abiove_flat_file.py.';
