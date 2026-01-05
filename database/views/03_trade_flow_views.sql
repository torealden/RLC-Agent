-- ============================================================================
-- TRADE FLOW VIEWS FOR POWER BI
-- ============================================================================
-- These views structure the trade data for visualization in Power BI
-- including Flow Maps, bar charts, and trend analysis.
--
-- Run in psql: \i database/views/03_trade_flow_views.sql
-- ============================================================================

-- Ensure gold schema exists
CREATE SCHEMA IF NOT EXISTS gold;

-- ============================================================================
-- VIEW 1: TRADE FLOWS WITH PROPER ORIGIN/DESTINATION
-- ============================================================================
-- Parses sheet_name to extract reporting country and creates proper
-- origin/destination pairs for Flow Map visualization.

DROP VIEW IF EXISTS gold.trade_flows CASCADE;
CREATE VIEW gold.trade_flows AS
WITH parsed_trade AS (
    SELECT
        id,
        commodity,
        country AS partner_country,
        flow_type,
        period_type,
        year,
        month,
        marketing_year,
        value,
        unit,
        source_file,
        sheet_name,
        -- Extract reporting country from sheet name
        -- Pattern: "Country Commodity Imports/Exports"
        CASE
            -- Handle specific patterns
            WHEN sheet_name LIKE 'World SM %' THEN 'World'
            WHEN sheet_name LIKE 'EU-%' THEN 'European Union'
            WHEN sheet_name LIKE 'EU %' THEN 'European Union'
            -- General extraction: first word(s) before commodity keywords
            WHEN sheet_name ~ '^([A-Za-z ]+) (Soybean|Soy|Corn|Wheat|Rapeseed|Canola|Sunflower|Palm|Tallow|UCO|Cottonseed|Peanut|Flax|Coconut)'
                THEN TRIM(REGEXP_REPLACE(sheet_name, ' (Soybean|Soy|Corn|Wheat|Rapeseed|Canola|Sunflower|Palm|Tallow|UCO|Cottonseed|Peanut|Flax|Coconut|Oil|Meal|Seed|Kernel|Imports|Exports|Trade|CWG|DCO|YG).*$', '', 'gi'))
            ELSE SPLIT_PART(sheet_name, ' ', 1)
        END AS reporting_country,
        created_at
    FROM bronze.trade_data_raw
    WHERE value IS NOT NULL AND value != 0
)
SELECT
    id,
    commodity,
    -- For imports: reporting country imports FROM partner country
    -- For exports: reporting country exports TO partner country
    CASE
        WHEN flow_type = 'import' THEN partner_country
        ELSE reporting_country
    END AS origin_country,
    CASE
        WHEN flow_type = 'import' THEN reporting_country
        ELSE partner_country
    END AS destination_country,
    partner_country,
    reporting_country,
    flow_type,
    period_type,
    year,
    month,
    marketing_year,
    value,
    unit,
    -- Convert to common unit (Million MT)
    CASE
        WHEN unit = 'Million MT' THEN value
        WHEN unit = 'Thousand MT' THEN value / 1000.0
        WHEN unit = 'MT' THEN value / 1000000.0
        ELSE value
    END AS value_million_mt,
    source_file,
    sheet_name,
    created_at
FROM parsed_trade;

-- ============================================================================
-- VIEW 2: TRADE SUMMARY BY COMMODITY AND MARKETING YEAR
-- ============================================================================
-- Aggregated view for high-level dashboards

DROP VIEW IF EXISTS gold.trade_summary_by_year CASCADE;
CREATE VIEW gold.trade_summary_by_year AS
SELECT
    commodity,
    marketing_year,
    flow_type,
    COUNT(DISTINCT partner_country) AS num_countries,
    SUM(value_million_mt) AS total_volume_mmt,
    AVG(value_million_mt) AS avg_volume_mmt,
    MAX(value_million_mt) AS max_volume_mmt
FROM gold.trade_flows
WHERE period_type = 'marketing_year'
  AND marketing_year IS NOT NULL
GROUP BY commodity, marketing_year, flow_type
ORDER BY commodity, marketing_year DESC, flow_type;

-- ============================================================================
-- VIEW 3: TOP EXPORTERS BY COMMODITY
-- ============================================================================

DROP VIEW IF EXISTS gold.top_exporters CASCADE;
CREATE VIEW gold.top_exporters AS
SELECT
    commodity,
    origin_country AS exporter,
    marketing_year,
    SUM(value_million_mt) AS total_exports_mmt,
    RANK() OVER (PARTITION BY commodity, marketing_year ORDER BY SUM(value_million_mt) DESC) AS rank
FROM gold.trade_flows
WHERE flow_type = 'export'
  AND period_type = 'marketing_year'
  AND marketing_year IS NOT NULL
GROUP BY commodity, origin_country, marketing_year
ORDER BY commodity, marketing_year DESC, total_exports_mmt DESC;

-- ============================================================================
-- VIEW 4: TOP IMPORTERS BY COMMODITY
-- ============================================================================

DROP VIEW IF EXISTS gold.top_importers CASCADE;
CREATE VIEW gold.top_importers AS
SELECT
    commodity,
    destination_country AS importer,
    marketing_year,
    SUM(value_million_mt) AS total_imports_mmt,
    RANK() OVER (PARTITION BY commodity, marketing_year ORDER BY SUM(value_million_mt) DESC) AS rank
FROM gold.trade_flows
WHERE flow_type = 'import'
  AND period_type = 'marketing_year'
  AND marketing_year IS NOT NULL
GROUP BY commodity, destination_country, marketing_year
ORDER BY commodity, marketing_year DESC, total_imports_mmt DESC;

-- ============================================================================
-- VIEW 5: TRADE FLOW MATRIX (FOR FLOW MAPS)
-- ============================================================================
-- Origin-destination pairs with volumes for Flow Map visualization

DROP VIEW IF EXISTS gold.trade_flow_matrix CASCADE;
CREATE VIEW gold.trade_flow_matrix AS
SELECT
    commodity,
    origin_country,
    destination_country,
    marketing_year,
    SUM(value_million_mt) AS volume_mmt,
    COUNT(*) AS num_records
FROM gold.trade_flows
WHERE period_type = 'marketing_year'
  AND marketing_year IS NOT NULL
  AND origin_country IS NOT NULL
  AND destination_country IS NOT NULL
  AND origin_country != destination_country
GROUP BY commodity, origin_country, destination_country, marketing_year
HAVING SUM(value_million_mt) > 0
ORDER BY commodity, marketing_year DESC, volume_mmt DESC;

-- ============================================================================
-- VIEW 6: SOYBEAN COMPLEX TRADE FLOWS
-- ============================================================================
-- Specific view for soybean, soybean meal, and soybean oil trade

DROP VIEW IF EXISTS gold.soybean_trade_flows CASCADE;
CREATE VIEW gold.soybean_trade_flows AS
SELECT
    CASE
        WHEN LOWER(commodity) LIKE '%meal%' OR LOWER(sheet_name) LIKE '%meal%' THEN 'Soybean Meal'
        WHEN LOWER(commodity) LIKE '%oil%' OR LOWER(sheet_name) LIKE '%oil%' THEN 'Soybean Oil'
        ELSE 'Soybeans'
    END AS product,
    origin_country,
    destination_country,
    reporting_country,
    partner_country,
    flow_type,
    marketing_year,
    value_million_mt,
    sheet_name
FROM gold.trade_flows
WHERE LOWER(commodity) LIKE '%soy%'
   OR LOWER(sheet_name) LIKE '%soy%'
ORDER BY marketing_year DESC, value_million_mt DESC;

-- ============================================================================
-- VIEW 7: SOYBEAN TRADE MATRIX FOR FLOW MAPS
-- ============================================================================

DROP VIEW IF EXISTS gold.soybean_flow_matrix CASCADE;
CREATE VIEW gold.soybean_flow_matrix AS
SELECT
    product,
    origin_country,
    destination_country,
    marketing_year,
    SUM(value_million_mt) AS volume_mmt
FROM gold.soybean_trade_flows
WHERE marketing_year IS NOT NULL
  AND origin_country IS NOT NULL
  AND destination_country IS NOT NULL
GROUP BY product, origin_country, destination_country, marketing_year
HAVING SUM(value_million_mt) > 0
ORDER BY product, marketing_year DESC, volume_mmt DESC;

-- ============================================================================
-- VIEW 8: RAPESEED/CANOLA TRADE FLOWS
-- ============================================================================

DROP VIEW IF EXISTS gold.rapeseed_trade_flows CASCADE;
CREATE VIEW gold.rapeseed_trade_flows AS
SELECT
    CASE
        WHEN LOWER(sheet_name) LIKE '%meal%' THEN 'Rapeseed Meal'
        WHEN LOWER(sheet_name) LIKE '%oil%' THEN 'Rapeseed Oil'
        ELSE 'Rapeseed/Canola'
    END AS product,
    origin_country,
    destination_country,
    flow_type,
    marketing_year,
    value_million_mt,
    sheet_name
FROM gold.trade_flows
WHERE LOWER(commodity) LIKE '%rapeseed%'
   OR LOWER(commodity) LIKE '%canola%'
   OR LOWER(sheet_name) LIKE '%rapeseed%'
   OR LOWER(sheet_name) LIKE '%canola%'
ORDER BY marketing_year DESC, value_million_mt DESC;

-- ============================================================================
-- VIEW 9: PALM OIL TRADE FLOWS
-- ============================================================================

DROP VIEW IF EXISTS gold.palm_oil_trade_flows CASCADE;
CREATE VIEW gold.palm_oil_trade_flows AS
SELECT
    CASE
        WHEN LOWER(sheet_name) LIKE '%kernel%' THEN 'Palm Kernel Oil'
        ELSE 'Palm Oil'
    END AS product,
    origin_country,
    destination_country,
    flow_type,
    marketing_year,
    value_million_mt,
    sheet_name
FROM gold.trade_flows
WHERE LOWER(commodity) LIKE '%palm%'
   OR LOWER(sheet_name) LIKE '%palm%'
ORDER BY marketing_year DESC, value_million_mt DESC;

-- ============================================================================
-- VIEW 10: YEAR-OVER-YEAR TRADE CHANGES
-- ============================================================================

DROP VIEW IF EXISTS gold.trade_yoy_changes CASCADE;
CREATE VIEW gold.trade_yoy_changes AS
WITH yearly_totals AS (
    SELECT
        commodity,
        flow_type,
        marketing_year,
        SUM(value_million_mt) AS total_volume
    FROM gold.trade_flows
    WHERE period_type = 'marketing_year'
      AND marketing_year IS NOT NULL
    GROUP BY commodity, flow_type, marketing_year
),
with_lag AS (
    SELECT
        commodity,
        flow_type,
        marketing_year,
        total_volume,
        LAG(total_volume) OVER (PARTITION BY commodity, flow_type ORDER BY marketing_year) AS prev_year_volume
    FROM yearly_totals
)
SELECT
    commodity,
    flow_type,
    marketing_year,
    total_volume AS current_volume_mmt,
    prev_year_volume AS prior_year_volume_mmt,
    total_volume - COALESCE(prev_year_volume, 0) AS volume_change_mmt,
    CASE
        WHEN prev_year_volume > 0
        THEN ROUND(((total_volume - prev_year_volume) / prev_year_volume * 100)::numeric, 1)
        ELSE NULL
    END AS pct_change
FROM with_lag
WHERE prev_year_volume IS NOT NULL
ORDER BY commodity, marketing_year DESC;

-- ============================================================================
-- VIEW 11: RECENT TRADE (LAST 5 YEARS)
-- ============================================================================

DROP VIEW IF EXISTS gold.recent_trade_flows CASCADE;
CREATE VIEW gold.recent_trade_flows AS
SELECT *
FROM gold.trade_flows
WHERE marketing_year IN (
    SELECT DISTINCT marketing_year
    FROM gold.trade_flows
    WHERE marketing_year IS NOT NULL
    ORDER BY marketing_year DESC
    LIMIT 5
);

-- ============================================================================
-- VIEW 12: TRADE STATISTICS DASHBOARD
-- ============================================================================
-- Summary statistics for KPI cards

DROP VIEW IF EXISTS gold.trade_dashboard_stats CASCADE;
CREATE VIEW gold.trade_dashboard_stats AS
SELECT
    commodity,
    COUNT(DISTINCT partner_country) AS num_trading_partners,
    COUNT(DISTINCT marketing_year) AS years_of_data,
    MIN(marketing_year) AS earliest_year,
    MAX(marketing_year) AS latest_year,
    SUM(CASE WHEN flow_type = 'export' THEN value_million_mt ELSE 0 END) AS total_exports_mmt,
    SUM(CASE WHEN flow_type = 'import' THEN value_million_mt ELSE 0 END) AS total_imports_mmt,
    COUNT(*) AS total_records
FROM gold.trade_flows
WHERE period_type = 'marketing_year'
GROUP BY commodity
ORDER BY total_exports_mmt DESC;

-- ============================================================================
-- GRANT PERMISSIONS
-- ============================================================================

-- Grant read access to all gold views
GRANT SELECT ON ALL TABLES IN SCHEMA gold TO PUBLIC;

-- ============================================================================
-- SUMMARY
-- ============================================================================

SELECT 'Trade flow views created successfully!' AS status;

SELECT
    schemaname,
    viewname,
    'SELECT COUNT(*) FROM ' || schemaname || '.' || viewname AS test_query
FROM pg_views
WHERE schemaname = 'gold'
  AND viewname LIKE '%trade%'
ORDER BY viewname;
