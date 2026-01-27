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
-- HELPER FUNCTION: Check if a value is a valid country name
-- ============================================================================
DROP FUNCTION IF EXISTS gold.is_valid_country(TEXT);
CREATE FUNCTION gold.is_valid_country(val TEXT) RETURNS BOOLEAN AS $$
BEGIN
    IF val IS NULL OR LENGTH(TRIM(val)) < 2 THEN
        RETURN FALSE;
    END IF;

    -- Exclude aggregates and totals
    IF LOWER(val) IN (
        'total', 'totals', 'sum total', 'grand total', 'subtotal',
        'world', 'world total', 'other', 'others', 'unknown', 'unspecified'
    ) THEN RETURN FALSE; END IF;

    -- Exclude regional aggregates
    IF LOWER(val) IN (
        'western hemisphere', 'eastern hemisphere', 'asia and oceania',
        'asia', 'europe', 'africa', 'americas', 'oceania',
        'north america', 'south america', 'central america', 'middle east',
        'sub-saharan africa', 'north africa', 'east asia', 'southeast asia',
        'former soviet union', 'fsu', 'cis', 'asean'
    ) THEN RETURN FALSE; END IF;

    -- Exclude commodity names
    IF LOWER(val) IN (
        'soybean', 'soybeans', 'soybean oil', 'soybean meal', 'soymeal', 'soyoil', 'soy',
        'corn', 'wheat', 'rice', 'barley', 'sorghum', 'oats',
        'rapeseed', 'canola', 'sunflower', 'cottonseed', 'peanut', 'peanuts',
        'palm oil', 'palm kernel', 'coconut', 'coconut oil', 'palm',
        'tallow', 'lard', 'grease', 'uco', 'dco', 'cwg',
        'oil', 'meal', 'seed', 'kernel', 'veg', 'crude', 'refined',
        'edible', 'inedible', 'major'
    ) THEN RETURN FALSE; END IF;

    -- Exclude month names
    IF LOWER(val) IN (
        'january', 'february', 'march', 'april', 'may', 'june',
        'july', 'august', 'september', 'october', 'november', 'december',
        'jan', 'feb', 'mar', 'apr', 'jun', 'jul', 'aug', 'sep', 'oct', 'nov', 'dec'
    ) THEN RETURN FALSE; END IF;

    -- Exclude other non-country values
    IF LOWER(val) IN (
        'census', 'nan', 'none', 'n/a', 'na', 'data', 'source', ''
    ) THEN RETURN FALSE; END IF;

    -- Exclude if contains commodity keywords
    IF LOWER(val) LIKE '%soy%' THEN RETURN FALSE; END IF;
    IF LOWER(val) LIKE '%meal%' THEN RETURN FALSE; END IF;
    IF LOWER(val) LIKE '%seed%' THEN RETURN FALSE; END IF;
    IF LOWER(val) LIKE '%bean%' THEN RETURN FALSE; END IF;
    IF LOWER(val) LIKE '%cotton%' THEN RETURN FALSE; END IF;
    IF LOWER(val) LIKE '%oil%' THEN RETURN FALSE; END IF;
    IF LOWER(val) LIKE '%rape%' THEN RETURN FALSE; END IF;
    IF LOWER(val) LIKE '%sun%' THEN RETURN FALSE; END IF;
    IF LOWER(val) LIKE '%palm%' THEN RETURN FALSE; END IF;
    IF LOWER(val) LIKE '%tallow%' THEN RETURN FALSE; END IF;
    IF LOWER(val) LIKE '%corn%' THEN RETURN FALSE; END IF;
    IF LOWER(val) LIKE '%wheat%' THEN RETURN FALSE; END IF;
    IF LOWER(val) LIKE '%kernel%' THEN RETURN FALSE; END IF;
    IF LOWER(val) LIKE '%grease%' THEN RETURN FALSE; END IF;
    IF LOWER(val) LIKE '%lard%' THEN RETURN FALSE; END IF;

    -- Exclude if looks like a year or number
    IF val ~ '^\d+$' THEN RETURN FALSE; END IF;
    IF val ~ '^\d{2}/\d{2}$' THEN RETURN FALSE; END IF;

    RETURN TRUE;
END;
$$ LANGUAGE plpgsql IMMUTABLE;

-- ============================================================================
-- VIEW 1: TRADE FLOWS WITH PROPER ORIGIN/DESTINATION
-- ============================================================================
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
        CASE
            WHEN sheet_name LIKE 'World SM %' THEN 'World'
            WHEN sheet_name LIKE 'EU-%' THEN 'European Union'
            WHEN sheet_name LIKE 'EU %' THEN 'European Union'
            WHEN sheet_name ~ '^([A-Za-z ]+) (Soybean|Soy|Corn|Wheat|Rapeseed|Canola|Sunflower|Palm|Tallow|UCO|Cottonseed|Peanut|Flax|Coconut)'
                THEN TRIM(REGEXP_REPLACE(sheet_name, ' (Soybean|Soy|Corn|Wheat|Rapeseed|Canola|Sunflower|Palm|Tallow|UCO|Cottonseed|Peanut|Flax|Coconut|Oil|Meal|Seed|Kernel|Imports|Exports|Trade|CWG|DCO|YG).*$', '', 'gi'))
            ELSE SPLIT_PART(sheet_name, ' ', 1)
        END AS reporting_country,
        created_at
    FROM bronze.trade_data_raw
    WHERE value IS NOT NULL
      AND value != 0
      AND (marketing_year IS NULL OR (
          SPLIT_PART(marketing_year, '/', 1) ~ '^\d+$'
          AND CAST(SPLIT_PART(marketing_year, '/', 1) AS INTEGER) >= 1980
          AND CAST(SPLIT_PART(marketing_year, '/', 1) AS INTEGER) <= 2050
      ))
)
SELECT
    id,
    commodity,
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
    -- Convert all values to Million MT using NUMERIC division for consistent types
    CAST(
        CASE
            WHEN unit = 'Million MT' THEN value
            WHEN unit = 'Thousand MT' THEN value / CAST(1000 AS NUMERIC)
            WHEN unit = 'MT' THEN value / CAST(1000000 AS NUMERIC)
            WHEN unit IS NULL THEN value / CAST(1000000 AS NUMERIC)  -- Default assumption: MT
            ELSE value / CAST(1000000 AS NUMERIC)  -- Unknown unit: assume MT
        END
    AS NUMERIC(20, 6)) AS value_million_mt,
    source_file,
    sheet_name,
    created_at
FROM parsed_trade
WHERE gold.is_valid_country(partner_country)
  AND gold.is_valid_country(reporting_country);

-- ============================================================================
-- VIEW 2: TRADE SUMMARY BY COMMODITY AND MARKETING YEAR
-- ============================================================================
DROP VIEW IF EXISTS gold.trade_summary_by_year CASCADE;
CREATE VIEW gold.trade_summary_by_year AS
SELECT
    commodity,
    marketing_year,
    flow_type,
    COUNT(DISTINCT partner_country) AS num_countries,
    CAST(SUM(value_million_mt) AS NUMERIC(20, 6)) AS total_volume_mmt,
    CAST(AVG(value_million_mt) AS NUMERIC(20, 6)) AS avg_volume_mmt,
    CAST(MAX(value_million_mt) AS NUMERIC(20, 6)) AS max_volume_mmt
FROM gold.trade_flows
WHERE period_type = 'marketing_year'
  AND marketing_year IS NOT NULL
  AND value_million_mt IS NOT NULL
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
    CAST(SUM(value_million_mt) AS NUMERIC(20, 6)) AS total_exports_mmt,
    RANK() OVER (PARTITION BY commodity, marketing_year ORDER BY SUM(value_million_mt) DESC) AS rank
FROM gold.trade_flows
WHERE flow_type = 'export'
  AND period_type = 'marketing_year'
  AND marketing_year IS NOT NULL
  AND value_million_mt IS NOT NULL
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
    CAST(SUM(value_million_mt) AS NUMERIC(20, 6)) AS total_imports_mmt,
    RANK() OVER (PARTITION BY commodity, marketing_year ORDER BY SUM(value_million_mt) DESC) AS rank
FROM gold.trade_flows
WHERE flow_type = 'import'
  AND period_type = 'marketing_year'
  AND marketing_year IS NOT NULL
  AND value_million_mt IS NOT NULL
GROUP BY commodity, destination_country, marketing_year
ORDER BY commodity, marketing_year DESC, total_imports_mmt DESC;

-- ============================================================================
-- VIEW 5: TRADE FLOW MATRIX (FOR FLOW MAPS)
-- ============================================================================
DROP VIEW IF EXISTS gold.trade_flow_matrix CASCADE;
CREATE VIEW gold.trade_flow_matrix AS
SELECT
    commodity,
    origin_country,
    destination_country,
    marketing_year,
    CAST(SUM(value_million_mt) AS NUMERIC(20, 6)) AS volume_mmt,
    COUNT(*) AS num_records
FROM gold.trade_flows
WHERE period_type = 'marketing_year'
  AND marketing_year IS NOT NULL
  AND origin_country IS NOT NULL
  AND destination_country IS NOT NULL
  AND origin_country != destination_country
  AND value_million_mt IS NOT NULL
GROUP BY commodity, origin_country, destination_country, marketing_year
HAVING SUM(value_million_mt) > 0
ORDER BY commodity, marketing_year DESC, volume_mmt DESC;

-- ============================================================================
-- VIEW 6: SOYBEAN COMPLEX TRADE FLOWS
-- ============================================================================
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
WHERE (LOWER(commodity) LIKE '%soy%' OR LOWER(sheet_name) LIKE '%soy%')
  AND gold.is_valid_country(origin_country)
  AND gold.is_valid_country(destination_country)
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
    CAST(SUM(value_million_mt) AS NUMERIC(20, 6)) AS volume_mmt
FROM gold.soybean_trade_flows
WHERE marketing_year IS NOT NULL
  AND origin_country IS NOT NULL
  AND destination_country IS NOT NULL
  AND origin_country != destination_country
  AND value_million_mt IS NOT NULL
  AND value_million_mt > 0
  AND value_million_mt < 1000
GROUP BY product, origin_country, destination_country, marketing_year
HAVING SUM(value_million_mt) > 0.001
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
WHERE (LOWER(commodity) LIKE '%rapeseed%' OR LOWER(commodity) LIKE '%canola%'
       OR LOWER(sheet_name) LIKE '%rapeseed%' OR LOWER(sheet_name) LIKE '%canola%')
  AND gold.is_valid_country(origin_country)
  AND gold.is_valid_country(destination_country)
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
WHERE (LOWER(commodity) LIKE '%palm%' OR LOWER(sheet_name) LIKE '%palm%')
  AND gold.is_valid_country(origin_country)
  AND gold.is_valid_country(destination_country)
ORDER BY marketing_year DESC, value_million_mt DESC;

-- ============================================================================
-- VIEW 10: TRADE DASHBOARD STATS
-- ============================================================================
DROP VIEW IF EXISTS gold.trade_dashboard_stats CASCADE;
CREATE VIEW gold.trade_dashboard_stats AS
SELECT
    commodity,
    COUNT(DISTINCT partner_country) AS num_trading_partners,
    COUNT(DISTINCT marketing_year) AS years_of_data,
    MIN(marketing_year) AS earliest_year,
    MAX(marketing_year) AS latest_year,
    CAST(SUM(CASE WHEN flow_type = 'export' THEN value_million_mt ELSE 0 END) AS NUMERIC(20, 6)) AS total_exports_mmt,
    CAST(SUM(CASE WHEN flow_type = 'import' THEN value_million_mt ELSE 0 END) AS NUMERIC(20, 6)) AS total_imports_mmt,
    COUNT(*) AS total_records
FROM gold.trade_flows
WHERE period_type = 'marketing_year'
  AND value_million_mt IS NOT NULL
GROUP BY commodity
ORDER BY total_exports_mmt DESC;

-- ============================================================================
-- GRANT PERMISSIONS
-- ============================================================================
GRANT EXECUTE ON FUNCTION gold.is_valid_country(TEXT) TO PUBLIC;
GRANT SELECT ON ALL TABLES IN SCHEMA gold TO PUBLIC;

-- ============================================================================
-- VERIFICATION
-- ============================================================================
SELECT 'Trade flow views created successfully!' AS status;

-- Test the function
SELECT 'Testing is_valid_country function:' AS test;
SELECT
    val,
    gold.is_valid_country(val) AS is_valid
FROM (VALUES
    ('China'), ('Brazil'), ('United States'), ('Japan'),
    ('Cottonseed'), ('Soyoil'), ('Soymeal'), ('Sum Total'),
    ('Western Hemisphere'), ('January'), ('February')
) AS t(val);
