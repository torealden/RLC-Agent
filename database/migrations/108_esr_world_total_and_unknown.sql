-- Migration 108: ESR world-total synthesis + UNKNOWN destination row
--
-- Two pieces:
--   1. Add 'UNKNOWN' row to silver.trade_country_reference at spreadsheet_row 218.
--      FAS country_code 9990 = the residual bucket where ESR sales/cancellations
--      that aren't yet allocated to a named country land. Without including
--      this bucket, WORLD TOTAL doesn't balance.
--
--   2. Rebuild gold.export_sales_matrix to synthesize a WORLD TOTAL row per
--      (commodity, marketing_year, week_ending) — SUM across all countries
--      including UNKNOWN — emitted with spreadsheet_row=217. Mirrors the
--      pattern used by gold.trade_export_matrix for Census trade.

-- 1. UNKNOWN destination row
INSERT INTO silver.trade_country_reference
    (country_name, country_name_alt, region, region_sort_order,
     country_sort_order, spreadsheet_row, is_regional_total, is_active)
VALUES
    ('UNKNOWN', 'UNKNOWN', 'UNALLOCATED', 8, 0, 218, TRUE, TRUE)
ON CONFLICT DO NOTHING;

-- 2. Drop and rebuild the matrix view to include WORLD TOTAL synthesis
DROP VIEW IF EXISTS gold.export_sales_matrix;

CREATE OR REPLACE VIEW gold.export_sales_matrix AS
WITH named AS (
    -- Per-country rows joined to canonical names + spreadsheet_row mapping
    SELECT
        fes.commodity,
        fes.commodity_code,
        fes.country_code,
        c.country_name              AS fas_country_name,
        fes.marketing_year,
        fes.week_ending,
        fes.unit,
        fes.weekly_exports,
        fes.accumulated_exports,
        fes.outstanding_sales,
        fes.gross_new_sales,
        fes.net_sales,
        fes.prev_my_accumulated
    FROM bronze.fas_export_sales fes
    LEFT JOIN reference.fas_esr_country c
        ON c.country_code = fes.country_code::INTEGER
),
country_rows AS (
    SELECT
        n.commodity,
        n.commodity_code,
        n.country_code,
        cr.country_name           AS country_name,
        n.fas_country_name        AS fas_country_name,
        cr.region,
        cr.region_sort_order,
        cr.country_sort_order,
        cr.spreadsheet_row,
        cr.is_regional_total,
        n.marketing_year,
        n.week_ending,
        EXTRACT(YEAR FROM n.week_ending)::INT  AS year,
        EXTRACT(MONTH FROM n.week_ending)::INT AS month,
        n.unit,
        n.net_sales                                                   AS sales,
        n.weekly_exports                                              AS shipments,
        COALESCE(n.net_sales, 0) + COALESCE(n.weekly_exports, 0)      AS commitments,
        n.net_sales                                                   AS nmy_sales,
        n.outstanding_sales,
        n.accumulated_exports,
        n.gross_new_sales,
        n.prev_my_accumulated
    FROM named n
    LEFT JOIN silver.trade_country_reference cr
        ON UPPER(TRIM(cr.country_name)) = UPPER(TRIM(n.fas_country_name))
           OR UPPER(TRIM(cr.country_name_alt)) = UPPER(TRIM(n.fas_country_name))
    WHERE (cr.is_active = TRUE OR cr.is_active IS NULL)
),
world_total AS (
    -- One synthetic row per (commodity, MY, week) summing across ALL countries
    -- (named + UNKNOWN). spreadsheet_row=217 hardcoded.
    SELECT
        commodity,
        commodity_code,
        NULL::VARCHAR(20)         AS country_code,
        'WORLD TOTAL'::TEXT       AS country_name,
        'WORLD TOTAL'::TEXT       AS fas_country_name,
        'WORLD'::TEXT             AS region,
        99                        AS region_sort_order,
        99                        AS country_sort_order,
        217                       AS spreadsheet_row,
        TRUE                      AS is_regional_total,
        marketing_year,
        week_ending,
        EXTRACT(YEAR FROM week_ending)::INT  AS year,
        EXTRACT(MONTH FROM week_ending)::INT AS month,
        MIN(unit)                 AS unit,
        SUM(sales)                AS sales,
        SUM(shipments)            AS shipments,
        SUM(commitments)          AS commitments,
        SUM(nmy_sales)            AS nmy_sales,
        SUM(outstanding_sales)    AS outstanding_sales,
        SUM(accumulated_exports)  AS accumulated_exports,
        SUM(gross_new_sales)      AS gross_new_sales,
        SUM(prev_my_accumulated)  AS prev_my_accumulated
    FROM country_rows
    -- Include all countries (named + UNKNOWN) so the total balances.
    WHERE country_name IS NOT NULL AND country_name <> 'WORLD TOTAL'
    GROUP BY commodity, commodity_code, marketing_year, week_ending
)
SELECT * FROM country_rows WHERE country_name IS NOT NULL
UNION ALL
SELECT * FROM world_total
ORDER BY commodity, week_ending, region_sort_order, country_sort_order, spreadsheet_row;
