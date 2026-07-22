-- 149_psd_wasde_vintage_ladder.sql
--
-- Fixes gold.fas_us_wasde_comp, which shipped a column named `vintage_rank` whose
-- semantics were INVERTED against every other vintage_rank in the estate:
--
--     dense_rank() OVER (... ORDER BY report_date DESC)   -- 1 = newest, 2 = prior
--
-- Everywhere else -- silver.wheat_series, silver.tallow_balance, silver.tallow_production,
-- silver.animal_fat_production, gold.abiove_soy_complex_monthly -- higher wins, and every
-- consumer picks with MAX()/ORDER BY vintage_rank DESC. The inverted view was read by
-- src/tools/WASDECompUpdater.bas, which is imported into three IS_CANONICAL workbooks
-- (us_corn_balance_sheet.xlsm, us_wheat_balance_sheet.xlsm, us_soybean_complex_bal_sheets.xlsm),
-- so ranks 1 and 2 were landing in the same files the forecast ladder will write into.
--
-- Ruling (Tore, 2026-07-22): nothing reaching a spreadsheet may emit a vintage below 10.
--
-- What this migration does:
--   1. Adds gold.psd_wasde_vintages -- ALL countries and commodities, not just US. The
--      original view hardcoded country_code='US' and a five-commodity whitelist; the
--      requirement is every country eventually.
--   2. Puts PSD on the shared ladder:
--        active MYs   -> vintage 'WASDE_<MON>_<YY>', rank 60 + release ordinal (61..79)
--        closed MYs   -> vintage 'FINAL',            rank 90
--      Rank encodes ORDER ONLY; identity lives in `vintage`. Trying to make one integer
--      carry both order and identity is what produced CIR at both 85 and 95.
--   3. Redefines gold.fas_us_wasde_comp as a US slice of that ladder, same column names,
--      corrected direction.
--
-- Why "closed MY" collapses to a single FINAL row: PSD republishes the full history in
-- every release, so a closed marketing year has N identical rows differing only by which
-- pull carried them. Ranking those by recency compares a number to itself. Verified on
-- corn MY2022: 2026-01-30 and 2026-03-02 both carry production 346,739 / ending_stocks
-- 34,551 / exports 42,214. Only `yield` differed (NULL in the January pull), which is why
-- the newest pull is the one kept.
--
-- "Active" is data-driven -- marketing_year >= max(marketing_year) - 1 per commodity+country
-- -- not a marketing-year calendar. WASDE actively projects exactly the old crop and the
-- new crop, and this is the same rule the existing VBA already used to bound its query.
--
-- BREAKING: a WASDECompUpdater build predating this migration asks for vintage_rank 1 and 2,
-- gets no match, and writes 0 cells. It reports "<commodity>: 0 cells" in its summary rather
-- than corrupting anything -- FieldValue returns Null for a missing vintage and WriteIfLiteral
-- skips Nulls. Re-import the updated .bas into the three workbooks.

BEGIN;

CREATE OR REPLACE VIEW gold.psd_wasde_vintages AS
WITH latest_in_month AS (
    -- PSD can be pulled more than once in a calendar month; the last pull in a month is
    -- the one that reflects that month's WASDE.
    SELECT p.*,
           row_number() OVER (PARTITION BY p.commodity, p.country_code, p.marketing_year,
                                           date_trunc('month', p.report_date)
                              ORDER BY p.report_date DESC) AS rn_in_month
    FROM bronze.fas_psd p
    WHERE p.report_date IS NOT NULL
),
monthly AS (
    SELECT * FROM latest_in_month WHERE rn_in_month = 1
),
horizon AS (
    SELECT commodity, country_code, max(marketing_year) AS max_my
    FROM monthly
    GROUP BY 1, 2
),
tagged AS (
    SELECT m.*,
           (m.marketing_year >= h.max_my - 1) AS is_active,
           row_number() OVER (PARTITION BY m.commodity, m.country_code, m.marketing_year
                              ORDER BY m.report_date DESC) AS rn_my
    FROM monthly m
    JOIN horizon h
      ON h.commodity = m.commodity
     AND h.country_code = m.country_code
),
kept AS (
    -- every release for an active MY; newest pull only for a closed MY
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
         THEN 'WASDE_' || upper(to_char(report_date, 'Mon_YY'))
         ELSE 'FINAL'
    END AS vintage,
    CASE WHEN is_active
         -- ORDER BY report_date ASC so the newest release carries the HIGHEST rank,
         -- matching higher-wins everywhere else. Capped at 79 so a runaway ingest can
         -- never climb into the actuals band (80 CENSUS_CIR / 85 CIR / 90 FINAL / 95 EIA).
         THEN least(60 + dense_rank() OVER (PARTITION BY commodity, country_code, marketing_year
                                            ORDER BY report_date)::int, 79)
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
    unit
FROM kept;

COMMENT ON VIEW gold.psd_wasde_vintages IS
'PSD/WASDE releases on the shared vintage ladder. higher vintage_rank = more recent/authoritative, '
'consistent with silver.*_series. Active marketing years (current + next) get WASDE_<MON>_<YY> at '
'ranks 61-79 in release order; closed marketing years collapse to a single FINAL row at rank 90 '
'because PSD republishes identical history in every release. All countries and commodities. '
'Replaces the inverted 1=newest ranking that shipped in gold.fas_us_wasde_comp. See migration 149.';

-- US slice, same shape the workbooks consume. Direction corrected, `vintage` added.
-- DROP rather than REPLACE because adding the `vintage` column changes the column list.
-- Deliberately no CASCADE: if anything in the database depends on this view, this fails
-- loudly rather than silently dropping the dependent.
DROP VIEW IF EXISTS gold.fas_us_wasde_comp;

CREATE VIEW gold.fas_us_wasde_comp AS
SELECT
    commodity,
    marketing_year,
    report_date,
    vintage,
    vintage_rank,
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
    unit
FROM gold.psd_wasde_vintages
WHERE country_code = 'US'
ORDER BY commodity, marketing_year DESC, vintage_rank DESC;

COMMENT ON VIEW gold.fas_us_wasde_comp IS
'US slice of gold.psd_wasde_vintages. vintage_rank is HIGHER = MORE RECENT (migration 149 '
'reversed the original 1=newest ordering). Consumers must take the top ranks DESC, not rank 1. '
'Read by src/tools/WASDECompUpdater.bas.';

COMMIT;
