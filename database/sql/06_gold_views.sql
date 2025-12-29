-- ============================================================================
-- Round Lakes Commodities - Gold Layer: Business Views and Marts
-- ============================================================================
-- File: 06_gold_views.sql
-- Purpose: Curated business views, Excel-compatible outputs, calculated metrics
-- Execute: After 05_silver_observation.sql
-- ============================================================================
-- Gold layer views provide:
-- 1. Excel-friendly pivot-style layouts
-- 2. Pre-calculated deltas and changes
-- 3. Cross-commodity comparisons
-- 4. Reconciliation helpers
-- ============================================================================

-- ============================================================================
-- WASDE CORN PIVOT: Excel-style supply/demand balance sheet
-- ============================================================================
CREATE OR REPLACE VIEW gold.wasde_corn_balance AS
WITH corn_series AS (
    -- Map series to row categories
    SELECT
        s.id AS series_id,
        s.series_key,
        CASE
            WHEN s.series_key LIKE '%area_planted%' THEN 'Area Planted'
            WHEN s.series_key LIKE '%area_harvested%' THEN 'Area Harvested'
            WHEN s.series_key LIKE '%yield%' THEN 'Yield per Harvested Acre'
            WHEN s.series_key LIKE '%beginning_stocks%' THEN 'Beginning Stocks'
            WHEN s.series_key LIKE '%production%' THEN 'Production'
            WHEN s.series_key LIKE '%imports%' THEN 'Imports'
            WHEN s.series_key LIKE '%supply_total%' THEN 'Supply, Total'
            WHEN s.series_key LIKE '%feed_residual%' THEN 'Feed and Residual'
            WHEN s.series_key LIKE '%ethanol%' THEN 'Ethanol for Fuel'
            WHEN s.series_key LIKE '%fsi%' THEN 'Food, Seed & Industrial'
            WHEN s.series_key LIKE '%exports%' THEN 'Exports'
            WHEN s.series_key LIKE '%use_total%' THEN 'Use, Total'
            WHEN s.series_key LIKE '%ending_stocks%' THEN 'Ending Stocks'
            WHEN s.series_key LIKE '%avg_farm_price%' THEN 'Avg. Farm Price ($/bu)'
            ELSE s.name
        END AS row_label,
        CASE
            WHEN s.series_key LIKE '%area%' THEN 1
            WHEN s.series_key LIKE '%yield%' THEN 2
            WHEN s.series_key LIKE '%beginning_stocks%' THEN 3
            WHEN s.series_key LIKE '%production%' THEN 4
            WHEN s.series_key LIKE '%imports%' THEN 5
            WHEN s.series_key LIKE '%supply_total%' THEN 6
            WHEN s.series_key LIKE '%feed%' THEN 7
            WHEN s.series_key LIKE '%ethanol%' THEN 8
            WHEN s.series_key LIKE '%fsi%' THEN 9
            WHEN s.series_key LIKE '%exports%' THEN 10
            WHEN s.series_key LIKE '%use_total%' THEN 11
            WHEN s.series_key LIKE '%ending_stocks%' THEN 12
            WHEN s.series_key LIKE '%price%' THEN 13
            ELSE 99
        END AS sort_order
    FROM core.series s
    JOIN core.data_source ds ON s.data_source_id = ds.id
    WHERE ds.code = 'wasde'
      AND s.commodity_id = (SELECT id FROM core.commodity WHERE code = 'corn')
      AND s.location_id = (SELECT id FROM core.location WHERE code = 'US')
),
observations AS (
    SELECT
        cs.row_label,
        cs.sort_order,
        EXTRACT(YEAR FROM o.observation_time)::TEXT ||
            CASE
                WHEN EXTRACT(MONTH FROM o.observation_time) >= 9 THEN '/' || (EXTRACT(YEAR FROM o.observation_time) + 1)::TEXT
                ELSE '/' || EXTRACT(YEAR FROM o.observation_time)::TEXT
            END AS marketing_year,
        o.value,
        o.is_forecast,
        o.is_estimated
    FROM silver.observation o
    JOIN corn_series cs ON o.series_id = cs.series_id
    WHERE o.is_latest = TRUE
)
SELECT
    row_label,
    sort_order,
    marketing_year,
    value,
    is_forecast,
    is_estimated
FROM observations
ORDER BY sort_order, marketing_year;

COMMENT ON VIEW gold.wasde_corn_balance IS
    'US Corn supply/demand balance sheet from WASDE. Excel-friendly layout with '
    'rows matching standard USDA presentation. Filter by marketing_year for specific columns.';


-- ============================================================================
-- WASDE CORN HEADLINE: Key metrics in wide format
-- ============================================================================
CREATE OR REPLACE VIEW gold.wasde_corn_headline AS
WITH latest_releases AS (
    SELECT
        id AS release_id,
        report_date,
        ROW_NUMBER() OVER (ORDER BY report_date DESC) AS release_rank
    FROM bronze.wasde_release
    WHERE is_validated = TRUE OR is_complete = TRUE
),
headline_cells AS (
    SELECT
        lr.report_date,
        lr.release_rank,
        c.marketing_year,
        c.row_category,
        c.value_numeric AS value
    FROM bronze.wasde_cell c
    JOIN latest_releases lr ON c.release_id = lr.release_id
    WHERE c.table_id = '04'  -- US Corn table
      AND c.row_category IN ('area_planted', 'area_harvested', 'yield',
                              'production', 'ending_stocks', 'avg_farm_price')
      AND c.value_numeric IS NOT NULL
      AND lr.release_rank <= 3  -- Last 3 releases
)
SELECT
    report_date,
    marketing_year,
    MAX(CASE WHEN row_category = 'area_planted' THEN value END) AS area_planted_mil_ac,
    MAX(CASE WHEN row_category = 'area_harvested' THEN value END) AS area_harvested_mil_ac,
    MAX(CASE WHEN row_category = 'yield' THEN value END) AS yield_bu_ac,
    MAX(CASE WHEN row_category = 'production' THEN value END) AS production_mil_bu,
    MAX(CASE WHEN row_category = 'ending_stocks' THEN value END) AS ending_stocks_mil_bu,
    MAX(CASE WHEN row_category = 'avg_farm_price' THEN value END) AS avg_farm_price_usd
FROM headline_cells
GROUP BY report_date, marketing_year
ORDER BY report_date DESC, marketing_year DESC;

COMMENT ON VIEW gold.wasde_corn_headline IS
    'Key US corn metrics pivoted wide. Shows area, yield, production, stocks, price '
    'by marketing year for recent WASDE releases.';


-- ============================================================================
-- WASDE CHANGES: Month-over-month changes for corn
-- ============================================================================
CREATE OR REPLACE VIEW gold.wasde_corn_changes AS
WITH ranked_releases AS (
    SELECT
        id AS release_id,
        report_date,
        LAG(id) OVER (ORDER BY report_date) AS prev_release_id,
        LAG(report_date) OVER (ORDER BY report_date) AS prev_report_date
    FROM bronze.wasde_release
    WHERE is_complete = TRUE
),
cell_changes AS (
    SELECT
        rr.report_date AS current_release,
        rr.prev_report_date AS previous_release,
        curr.marketing_year,
        curr.row_label,
        curr.row_category,
        curr.value_numeric AS current_value,
        prev.value_numeric AS previous_value,
        curr.value_numeric - prev.value_numeric AS change_value,
        CASE
            WHEN prev.value_numeric IS NOT NULL AND prev.value_numeric != 0
            THEN ROUND(((curr.value_numeric - prev.value_numeric) / prev.value_numeric) * 100, 2)
            ELSE NULL
        END AS change_pct
    FROM bronze.wasde_cell curr
    JOIN ranked_releases rr ON curr.release_id = rr.release_id
    LEFT JOIN bronze.wasde_cell prev ON
        prev.release_id = rr.prev_release_id
        AND prev.table_id = curr.table_id
        AND prev.row_id = curr.row_id
        AND prev.column_id = curr.column_id
    WHERE curr.table_id = '04'
      AND curr.value_numeric IS NOT NULL
      AND rr.prev_release_id IS NOT NULL
)
SELECT
    current_release,
    previous_release,
    marketing_year,
    row_label,
    row_category,
    current_value,
    previous_value,
    change_value,
    change_pct,
    CASE
        WHEN ABS(change_pct) > 5 THEN 'SIGNIFICANT'
        WHEN ABS(change_pct) > 1 THEN 'NOTABLE'
        ELSE 'MINOR'
    END AS change_magnitude
FROM cell_changes
WHERE change_value != 0
ORDER BY current_release DESC, ABS(change_pct) DESC NULLS LAST;

COMMENT ON VIEW gold.wasde_corn_changes IS
    'Month-over-month changes in WASDE corn estimates. Highlights significant revisions.';


-- ============================================================================
-- WASDE SOYBEAN HEADLINE: Key soybean metrics
-- ============================================================================
CREATE OR REPLACE VIEW gold.wasde_soybean_headline AS
WITH latest_releases AS (
    SELECT
        id AS release_id,
        report_date,
        ROW_NUMBER() OVER (ORDER BY report_date DESC) AS release_rank
    FROM bronze.wasde_release
    WHERE is_validated = TRUE OR is_complete = TRUE
),
headline_cells AS (
    SELECT
        lr.report_date,
        lr.release_rank,
        c.marketing_year,
        c.row_category,
        c.value_numeric AS value
    FROM bronze.wasde_cell c
    JOIN latest_releases lr ON c.release_id = lr.release_id
    WHERE c.table_id = '10'  -- US Soybeans table
      AND c.row_category IN ('area_planted', 'area_harvested', 'yield',
                              'production', 'crushings', 'exports', 'ending_stocks', 'avg_farm_price')
      AND c.value_numeric IS NOT NULL
      AND lr.release_rank <= 3
)
SELECT
    report_date,
    marketing_year,
    MAX(CASE WHEN row_category = 'area_planted' THEN value END) AS area_planted_mil_ac,
    MAX(CASE WHEN row_category = 'area_harvested' THEN value END) AS area_harvested_mil_ac,
    MAX(CASE WHEN row_category = 'yield' THEN value END) AS yield_bu_ac,
    MAX(CASE WHEN row_category = 'production' THEN value END) AS production_mil_bu,
    MAX(CASE WHEN row_category = 'crushings' THEN value END) AS crushings_mil_bu,
    MAX(CASE WHEN row_category = 'exports' THEN value END) AS exports_mil_bu,
    MAX(CASE WHEN row_category = 'ending_stocks' THEN value END) AS ending_stocks_mil_bu,
    MAX(CASE WHEN row_category = 'avg_farm_price' THEN value END) AS avg_farm_price_usd
FROM headline_cells
GROUP BY report_date, marketing_year
ORDER BY report_date DESC, marketing_year DESC;

COMMENT ON VIEW gold.wasde_soybean_headline IS
    'Key US soybean metrics pivoted wide.';


-- ============================================================================
-- CROSS-COMMODITY COMPARISON
-- ============================================================================
CREATE OR REPLACE VIEW gold.us_grains_summary AS
SELECT
    'Corn' AS commodity,
    marketing_year,
    area_planted_mil_ac,
    area_harvested_mil_ac,
    yield_bu_ac,
    production_mil_bu,
    ending_stocks_mil_bu,
    avg_farm_price_usd,
    CASE
        WHEN production_mil_bu > 0
        THEN ROUND((ending_stocks_mil_bu / production_mil_bu) * 100, 1)
        ELSE NULL
    END AS stocks_to_production_pct
FROM gold.wasde_corn_headline
WHERE report_date = (SELECT MAX(report_date) FROM gold.wasde_corn_headline)

UNION ALL

SELECT
    'Soybeans' AS commodity,
    marketing_year,
    area_planted_mil_ac,
    area_harvested_mil_ac,
    yield_bu_ac,
    production_mil_bu,
    ending_stocks_mil_bu,
    avg_farm_price_usd,
    CASE
        WHEN production_mil_bu > 0
        THEN ROUND((ending_stocks_mil_bu / production_mil_bu) * 100, 1)
        ELSE NULL
    END AS stocks_to_production_pct
FROM gold.wasde_soybean_headline
WHERE report_date = (SELECT MAX(report_date) FROM gold.wasde_soybean_headline)

ORDER BY commodity, marketing_year DESC;

COMMENT ON VIEW gold.us_grains_summary IS
    'Side-by-side comparison of US corn and soybeans from latest WASDE.';


-- ============================================================================
-- EXCEL RECONCILIATION: Row counts and totals for validation
-- ============================================================================
CREATE OR REPLACE VIEW gold.reconciliation_wasde_summary AS
SELECT
    r.report_date,
    r.release_number,
    r.is_complete,
    r.is_validated,
    COUNT(c.id) AS total_cells,
    COUNT(c.id) FILTER (WHERE c.value_numeric IS NOT NULL) AS numeric_cells,
    COUNT(c.id) FILTER (WHERE c.value_numeric IS NULL AND c.value_text IS NOT NULL) AS text_cells,
    COUNT(DISTINCT c.table_id) AS tables_loaded,
    COUNT(DISTINCT c.marketing_year) AS marketing_years,
    MIN(c.created_at) AS first_cell_ingested,
    MAX(c.created_at) AS last_cell_ingested,
    r.ingest_run_id
FROM bronze.wasde_release r
LEFT JOIN bronze.wasde_cell c ON c.release_id = r.id
GROUP BY r.id, r.report_date, r.release_number, r.is_complete, r.is_validated, r.ingest_run_id
ORDER BY r.report_date DESC;

COMMENT ON VIEW gold.reconciliation_wasde_summary IS
    'Summary statistics for WASDE releases. Use to reconcile row counts with Excel.';


-- ============================================================================
-- EXCEL RECONCILIATION: Detailed cell comparison
-- ============================================================================
CREATE OR REPLACE VIEW gold.reconciliation_wasde_corn_detail AS
SELECT
    r.report_date,
    c.table_id,
    c.marketing_year,
    c.row_label,
    c.row_category,
    c.value_text AS source_text,
    c.value_numeric AS parsed_value,
    c.value_unit_text AS unit,
    c.is_footnoted,
    c.parse_warning
FROM bronze.wasde_cell c
JOIN bronze.wasde_release r ON c.release_id = r.id
WHERE c.table_id = '04'
ORDER BY r.report_date DESC, c.row_order, c.marketing_year;

COMMENT ON VIEW gold.reconciliation_wasde_corn_detail IS
    'Detailed cell-level data for US Corn table. Compare directly with Excel spreadsheet.';


-- ============================================================================
-- MATERIALIZED VIEW: WASDE History (for performance)
-- ============================================================================
-- This is a template; create when needed:

-- CREATE MATERIALIZED VIEW gold.mv_wasde_corn_history AS
-- SELECT
--     r.report_date,
--     r.release_number,
--     c.marketing_year,
--     c.row_label,
--     c.row_category,
--     c.value_numeric AS value,
--     c.value_unit_text AS unit
-- FROM bronze.wasde_cell c
-- JOIN bronze.wasde_release r ON c.release_id = r.id
-- WHERE c.table_id = '04'
--   AND c.value_numeric IS NOT NULL
-- ORDER BY r.report_date, c.row_order, c.marketing_year;
--
-- CREATE UNIQUE INDEX ON gold.mv_wasde_corn_history(report_date, marketing_year, row_category);
--
-- -- Refresh with: REFRESH MATERIALIZED VIEW CONCURRENTLY gold.mv_wasde_corn_history;


-- ============================================================================
-- PRICE SERIES VIEW: For trading analysis
-- ============================================================================
CREATE OR REPLACE VIEW gold.commodity_prices AS
SELECT
    c.code AS commodity,
    l.code AS location,
    u.code AS unit,
    ds.code AS source,
    o.observation_date,
    o.value AS price,
    o.is_latest,
    s.series_key
FROM silver.observation o
JOIN core.series s ON o.series_id = s.id
JOIN core.data_source ds ON s.data_source_id = ds.id
LEFT JOIN core.commodity c ON s.commodity_id = c.id
LEFT JOIN core.location l ON s.location_id = l.id
LEFT JOIN core.unit u ON s.unit_id = u.id
WHERE u.unit_type = 'price'
  AND o.is_latest = TRUE
  AND o.value IS NOT NULL
ORDER BY c.code, o.observation_date DESC;

COMMENT ON VIEW gold.commodity_prices IS
    'All price series across commodities. For trading and price analysis.';


-- ============================================================================
-- STOCKS-TO-USE RATIO: Derived metric
-- ============================================================================
CREATE OR REPLACE VIEW gold.stocks_to_use AS
WITH ending_stocks AS (
    SELECT
        s.commodity_id,
        o.observation_time,
        o.value AS stocks
    FROM silver.observation o
    JOIN core.series s ON o.series_id = s.id
    WHERE s.series_key LIKE '%ending_stocks%'
      AND o.is_latest = TRUE
),
total_use AS (
    SELECT
        s.commodity_id,
        o.observation_time,
        o.value AS total_use
    FROM silver.observation o
    JOIN core.series s ON o.series_id = s.id
    WHERE s.series_key LIKE '%use_total%'
      AND o.is_latest = TRUE
)
SELECT
    c.code AS commodity,
    es.observation_time,
    es.stocks AS ending_stocks,
    tu.total_use,
    CASE
        WHEN tu.total_use > 0
        THEN ROUND((es.stocks / tu.total_use) * 100, 2)
        ELSE NULL
    END AS stocks_to_use_pct
FROM ending_stocks es
JOIN total_use tu ON es.commodity_id = tu.commodity_id
    AND es.observation_time = tu.observation_time
JOIN core.commodity c ON es.commodity_id = c.id
ORDER BY c.code, es.observation_time DESC;

COMMENT ON VIEW gold.stocks_to_use IS
    'Calculated stocks-to-use ratio for commodities. Key indicator of supply tightness.';


-- ============================================================================
-- DATA FRESHNESS: Monitoring view
-- ============================================================================
CREATE OR REPLACE VIEW gold.data_freshness AS
SELECT
    ds.code AS data_source,
    ds.name AS source_name,
    s.commodity_id,
    c.code AS commodity,
    COUNT(DISTINCT s.id) AS series_count,
    MAX(o.observation_time) AS latest_observation,
    MAX(o.created_at) AS latest_ingest,
    EXTRACT(DAY FROM NOW() - MAX(o.created_at)) AS days_since_ingest,
    ds.update_frequency AS expected_frequency
FROM core.data_source ds
LEFT JOIN core.series s ON s.data_source_id = ds.id
LEFT JOIN core.commodity c ON s.commodity_id = c.id
LEFT JOIN silver.observation o ON o.series_id = s.id AND o.is_latest = TRUE
WHERE ds.is_active = TRUE
GROUP BY ds.id, ds.code, ds.name, ds.update_frequency, s.commodity_id, c.code
ORDER BY ds.code, c.code;

COMMENT ON VIEW gold.data_freshness IS
    'Monitor data freshness by source and commodity. Identify stale data.';


-- ============================================================================
-- Verification
-- ============================================================================
DO $$
BEGIN
    RAISE NOTICE 'Gold layer views created:';
    RAISE NOTICE '  - gold.wasde_corn_balance';
    RAISE NOTICE '  - gold.wasde_corn_headline';
    RAISE NOTICE '  - gold.wasde_corn_changes';
    RAISE NOTICE '  - gold.wasde_soybean_headline';
    RAISE NOTICE '  - gold.us_grains_summary';
    RAISE NOTICE '  - gold.reconciliation_wasde_summary';
    RAISE NOTICE '  - gold.reconciliation_wasde_corn_detail';
    RAISE NOTICE '  - gold.commodity_prices';
    RAISE NOTICE '  - gold.stocks_to_use';
    RAISE NOTICE '  - gold.data_freshness';
END $$;
