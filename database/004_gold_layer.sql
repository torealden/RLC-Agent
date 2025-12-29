-- =============================================================================
-- RLC Commodities Database Schema - Gold Layer
-- Version: 1.0.0
-- =============================================================================
--
-- GOLD LAYER PHILOSOPHY
-- ---------------------
-- Gold contains business-ready, curated datasets:
-- - Pre-joined, denormalized views for reporting
-- - Excel-compatible pivoted outputs
-- - Pre-calculated deltas and changes
-- - Materialized views for heavy aggregations
--
-- Gold views should match Excel spreadsheet layouts where possible.
--
-- =============================================================================

-- =============================================================================
-- WASDE HEADLINE METRICS
-- =============================================================================

-- -----------------------------------------------------------------------------
-- US Corn Key Metrics: Excel-friendly pivoted view
-- Matches typical balance sheet layout
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW gold.us_corn_balance_sheet AS
WITH latest_report AS (
    SELECT MAX(report_date) AS report_date
    FROM bronze.wasde_release
    WHERE is_current = TRUE OR report_date = (SELECT MAX(report_date) FROM bronze.wasde_release)
),
corn_data AS (
    SELECT
        w.marketing_year,
        w.row_label,
        w.numeric_value,
        w.report_date,
        ROW_NUMBER() OVER (
            PARTITION BY w.marketing_year, w.row_label
            ORDER BY w.report_date DESC
        ) AS rn
    FROM bronze.wasde_cell w
    CROSS JOIN latest_report lr
    WHERE w.report_date = lr.report_date
      AND w.table_name LIKE '%U.S. Corn%Supply and Use%'
      AND w.numeric_value IS NOT NULL
)
SELECT
    marketing_year,
    MAX(CASE WHEN row_label LIKE '%Area Planted%' THEN numeric_value END) AS area_planted_mil_acres,
    MAX(CASE WHEN row_label LIKE '%Area Harvested%' THEN numeric_value END) AS area_harvested_mil_acres,
    MAX(CASE WHEN row_label LIKE '%Yield%' THEN numeric_value END) AS yield_bu_acre,
    MAX(CASE WHEN row_label LIKE '%Beginning Stocks%' THEN numeric_value END) AS beginning_stocks_mil_bu,
    MAX(CASE WHEN row_label LIKE '%Production%' AND row_label NOT LIKE '%Ethanol%' THEN numeric_value END) AS production_mil_bu,
    MAX(CASE WHEN row_label LIKE '%Imports%' THEN numeric_value END) AS imports_mil_bu,
    MAX(CASE WHEN row_label LIKE '%Total Supply%' OR row_label = 'Supply, Total' THEN numeric_value END) AS total_supply_mil_bu,
    MAX(CASE WHEN row_label LIKE '%Feed%Residual%' THEN numeric_value END) AS feed_residual_mil_bu,
    MAX(CASE WHEN row_label LIKE '%Ethanol%' AND row_label LIKE '%FSI%' THEN numeric_value END) AS ethanol_fsi_mil_bu,
    MAX(CASE WHEN row_label LIKE '%Exports%' THEN numeric_value END) AS exports_mil_bu,
    MAX(CASE WHEN row_label LIKE '%Ending Stocks%' THEN numeric_value END) AS ending_stocks_mil_bu,
    MAX(CASE WHEN row_label LIKE '%Season-Average%Price%' OR row_label LIKE '%Farm Price%' THEN numeric_value END) AS farm_price_usd_bu,
    report_date AS wasde_report_date
FROM corn_data
WHERE rn = 1
GROUP BY marketing_year, report_date
ORDER BY marketing_year DESC;

COMMENT ON VIEW gold.us_corn_balance_sheet IS 'US Corn S&D from latest WASDE, pivoted like Excel balance sheet';

-- -----------------------------------------------------------------------------
-- US Soybeans Key Metrics
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW gold.us_soybeans_balance_sheet AS
WITH latest_report AS (
    SELECT MAX(report_date) AS report_date
    FROM bronze.wasde_release
    WHERE is_current = TRUE OR report_date = (SELECT MAX(report_date) FROM bronze.wasde_release)
),
soy_data AS (
    SELECT
        w.marketing_year,
        w.row_label,
        w.numeric_value,
        w.report_date,
        ROW_NUMBER() OVER (
            PARTITION BY w.marketing_year, w.row_label
            ORDER BY w.report_date DESC
        ) AS rn
    FROM bronze.wasde_cell w
    CROSS JOIN latest_report lr
    WHERE w.report_date = lr.report_date
      AND w.table_name LIKE '%U.S. Soybean%Supply and Use%'
      AND w.numeric_value IS NOT NULL
)
SELECT
    marketing_year,
    MAX(CASE WHEN row_label LIKE '%Area Planted%' THEN numeric_value END) AS area_planted_mil_acres,
    MAX(CASE WHEN row_label LIKE '%Area Harvested%' THEN numeric_value END) AS area_harvested_mil_acres,
    MAX(CASE WHEN row_label LIKE '%Yield%' THEN numeric_value END) AS yield_bu_acre,
    MAX(CASE WHEN row_label LIKE '%Beginning Stocks%' THEN numeric_value END) AS beginning_stocks_mil_bu,
    MAX(CASE WHEN row_label LIKE '%Production%' THEN numeric_value END) AS production_mil_bu,
    MAX(CASE WHEN row_label LIKE '%Imports%' THEN numeric_value END) AS imports_mil_bu,
    MAX(CASE WHEN row_label LIKE '%Total Supply%' OR row_label = 'Supply, Total' THEN numeric_value END) AS total_supply_mil_bu,
    MAX(CASE WHEN row_label LIKE '%Crush%' THEN numeric_value END) AS crushings_mil_bu,
    MAX(CASE WHEN row_label LIKE '%Exports%' THEN numeric_value END) AS exports_mil_bu,
    MAX(CASE WHEN row_label LIKE '%Seed%' THEN numeric_value END) AS seed_mil_bu,
    MAX(CASE WHEN row_label LIKE '%Residual%' THEN numeric_value END) AS residual_mil_bu,
    MAX(CASE WHEN row_label LIKE '%Ending Stocks%' THEN numeric_value END) AS ending_stocks_mil_bu,
    MAX(CASE WHEN row_label LIKE '%Season-Average%Price%' OR row_label LIKE '%Farm Price%' THEN numeric_value END) AS farm_price_usd_bu,
    report_date AS wasde_report_date
FROM soy_data
WHERE rn = 1
GROUP BY marketing_year, report_date
ORDER BY marketing_year DESC;

-- =============================================================================
-- WASDE CHANGE TRACKING
-- =============================================================================

-- -----------------------------------------------------------------------------
-- WASDE Changes: Compare current release to previous
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW gold.wasde_changes AS
WITH ranked_reports AS (
    SELECT
        report_date,
        ROW_NUMBER() OVER (ORDER BY report_date DESC) AS report_rank
    FROM bronze.wasde_release
),
current_report AS (
    SELECT report_date FROM ranked_reports WHERE report_rank = 1
),
previous_report AS (
    SELECT report_date FROM ranked_reports WHERE report_rank = 2
),
current_data AS (
    SELECT
        table_name,
        row_label,
        marketing_year,
        numeric_value AS current_value,
        report_date AS current_report_date
    FROM bronze.wasde_cell
    WHERE report_date = (SELECT report_date FROM current_report)
      AND numeric_value IS NOT NULL
),
previous_data AS (
    SELECT
        table_name,
        row_label,
        marketing_year,
        numeric_value AS previous_value,
        report_date AS previous_report_date
    FROM bronze.wasde_cell
    WHERE report_date = (SELECT report_date FROM previous_report)
      AND numeric_value IS NOT NULL
)
SELECT
    c.table_name,
    c.marketing_year,
    c.row_label,
    p.previous_value,
    c.current_value,
    (c.current_value - COALESCE(p.previous_value, c.current_value)) AS change,
    CASE
        WHEN p.previous_value IS NULL OR p.previous_value = 0 THEN NULL
        ELSE ROUND(((c.current_value - p.previous_value) / p.previous_value * 100)::NUMERIC, 2)
    END AS change_pct,
    p.previous_report_date,
    c.current_report_date
FROM current_data c
LEFT JOIN previous_data p ON
    c.table_name = p.table_name AND
    c.row_label = p.row_label AND
    c.marketing_year = p.marketing_year
WHERE c.current_value != COALESCE(p.previous_value, c.current_value)
ORDER BY ABS(c.current_value - COALESCE(p.previous_value, c.current_value)) DESC;

COMMENT ON VIEW gold.wasde_changes IS 'Shows all WASDE values that changed between the two most recent reports';

-- =============================================================================
-- TRADE FLOW SUMMARIES
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Monthly Export Inspections by Destination (Excel-like pivot)
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW gold.soybean_inspections_by_destination AS
SELECT
    DATE_TRUNC('month', week_ending_date)::DATE AS month,
    destination_country,
    SUM(metric_tons) AS total_mt,
    ROUND(SUM(metric_tons) / 1000, 1) AS total_tmt,
    ROUND(SUM(metric_tons) * 36.744 / 1000000, 2) AS total_mil_bu,
    COUNT(DISTINCT week_ending_date) AS weeks_with_data
FROM bronze.fgis_inspection_raw
WHERE commodity = 'SOYBEANS'
  AND week_ending_date >= DATE_TRUNC('year', CURRENT_DATE) - INTERVAL '2 years'
GROUP BY DATE_TRUNC('month', week_ending_date), destination_country
ORDER BY month DESC, total_mt DESC;

-- -----------------------------------------------------------------------------
-- Marketing Year Trade Summary
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW gold.marketing_year_trade_summary AS
WITH my_calc AS (
    SELECT
        commodity_code,
        location_code,
        flow_direction,
        marketing_year,
        SUM(quantity_mt) AS total_quantity_mt,
        SUM(value_usd) AS total_value_usd,
        COUNT(*) AS period_count
    FROM silver.trade_flow
    WHERE period_type = 'MONTH'
    GROUP BY commodity_code, location_code, flow_direction, marketing_year
)
SELECT
    m.*,
    ROUND(m.total_quantity_mt / 1000, 1) AS total_quantity_tmt,
    ROUND(m.total_value_usd / 1000000, 1) AS total_value_mil_usd,
    CASE
        WHEN m.total_quantity_mt > 0
        THEN ROUND(m.total_value_usd / m.total_quantity_mt, 2)
    END AS avg_unit_value_usd_mt,
    c.name AS commodity_name,
    l.name AS location_name
FROM my_calc m
JOIN public.commodity c ON m.commodity_code = c.code
JOIN public.location l ON m.location_code = l.code
ORDER BY m.marketing_year DESC, m.commodity_code, m.total_quantity_mt DESC;

-- =============================================================================
-- RECONCILIATION VIEWS (for Excel comparison)
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Reconciliation: Census trade totals by month
-- Compare these totals against Excel spreadsheet column sums
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW gold.reconcile_census_monthly_totals AS
SELECT
    year,
    month,
    commodity_code,
    flow,
    COUNT(*) AS country_count,
    SUM(quantity) AS total_quantity_raw,
    SUM(value_usd) AS total_value_usd,
    ROUND(SUM(quantity) / 1000, 3) AS total_quantity_tmt,
    MAX(updated_at) AS last_updated
FROM bronze.census_trade_raw
WHERE commodity_code IS NOT NULL
GROUP BY year, month, commodity_code, flow
ORDER BY year DESC, month DESC, commodity_code, flow;

COMMENT ON VIEW gold.reconcile_census_monthly_totals IS 'For reconciling Census data against Excel column totals';

-- -----------------------------------------------------------------------------
-- Reconciliation: FGIS inspections weekly totals
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW gold.reconcile_fgis_weekly_totals AS
SELECT
    week_ending_date,
    commodity,
    COUNT(DISTINCT destination_country) AS destination_count,
    SUM(metric_tons) AS total_mt,
    ROUND(SUM(metric_tons) * 36.744 / 1000, 2) AS total_thousand_bu,
    MAX(updated_at) AS last_updated
FROM bronze.fgis_inspection_raw
GROUP BY week_ending_date, commodity
ORDER BY week_ending_date DESC, commodity;

COMMENT ON VIEW gold.reconcile_fgis_weekly_totals IS 'For reconciling FGIS inspections against Excel weekly totals';

-- =============================================================================
-- MATERIALIZED VIEW GUIDANCE
-- =============================================================================

-- For heavy aggregations that don't need real-time updates, use materialized views:
--
-- CREATE MATERIALIZED VIEW gold.mv_annual_trade_summary AS
-- SELECT ... expensive aggregation query ...
-- WITH DATA;
--
-- -- Refresh after each data load:
-- REFRESH MATERIALIZED VIEW CONCURRENTLY gold.mv_annual_trade_summary;
--
-- Benefits:
-- - Much faster read queries
-- - Pre-computed aggregations
-- - CONCURRENTLY allows reads during refresh
--
-- When to use:
-- - Queries that aggregate millions of rows
-- - Reports run multiple times per day
-- - Data that updates daily or less frequently
--
-- When NOT to use:
-- - Data that changes frequently (hourly or more)
-- - Queries that need real-time data
-- - Small tables (regular views are fine)

-- =============================================================================
-- VALIDATION STATUS VIEW
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Data Quality Dashboard: Shows validation status of recent ingests
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW gold.data_quality_dashboard AS
SELECT
    ds.name AS data_source,
    ir.job_type,
    ir.started_at,
    ir.completed_at,
    ir.status AS ingest_status,
    ir.records_fetched,
    ir.records_inserted,
    ir.records_failed,
    COALESCE(vs.status, 'NOT_VALIDATED') AS validation_status,
    vs.checked_at AS validated_at,
    ir.error_message
FROM audit.ingest_run ir
JOIN public.data_source ds ON ir.data_source_id = ds.id
LEFT JOIN audit.validation_status vs ON ir.id = vs.ingest_run_id
WHERE ir.started_at >= CURRENT_DATE - INTERVAL '7 days'
ORDER BY ir.started_at DESC;

COMMENT ON VIEW gold.data_quality_dashboard IS 'Overview of recent data ingestion and validation status';

-- =============================================================================
-- END OF GOLD LAYER SCRIPT
-- =============================================================================
