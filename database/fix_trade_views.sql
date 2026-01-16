-- ============================================================================
-- FIX TRADE VIEWS - Run this to fix all the errors
-- ============================================================================
-- Run: psql -h localhost -U postgres -d rlc_commodities -f database/fix_trade_views.sql
-- ============================================================================

-- First, let's see what's in the bronze data
\echo '=== DIAGNOSTIC: Sample of bronze.trade_data_raw ==='
SELECT country, commodity, flow_type, marketing_year, value, unit, sheet_name
FROM bronze.trade_data_raw
WHERE country IN ('November', 'Soybean', 'December', 'January', 'October')
   OR country LIKE '%oil%'
   OR country LIKE '%meal%'
LIMIT 20;

\echo ''
\echo '=== CLEANING: Deleting rows with months as country names ==='
DELETE FROM bronze.trade_data_raw
WHERE LOWER(country) IN (
    'january', 'february', 'march', 'april', 'may', 'june',
    'july', 'august', 'september', 'october', 'november', 'december',
    'jan', 'feb', 'mar', 'apr', 'jun', 'jul', 'aug', 'sep', 'oct', 'nov', 'dec'
);

\echo '=== CLEANING: Deleting rows with commodity names as country names ==='
DELETE FROM bronze.trade_data_raw
WHERE LOWER(country) IN (
    'soybean', 'soybeans', 'soybean oil', 'soybean meal', 'soymeal', 'soyoil',
    'corn', 'wheat', 'rice', 'barley', 'sorghum', 'oats',
    'rapeseed', 'canola', 'sunflower', 'cottonseed', 'peanut', 'peanuts',
    'palm oil', 'palm kernel', 'coconut', 'tallow', 'lard', 'grease',
    'oil', 'meal', 'seed', 'kernel', 'edible', 'inedible', 'major'
);

\echo ''
\echo '=== DIAGNOSTIC: Countries that look like months or commodities ==='
SELECT DISTINCT country, COUNT(*) as record_count
FROM bronze.trade_data_raw
WHERE LOWER(country) IN (
    'january', 'february', 'march', 'april', 'may', 'june',
    'july', 'august', 'september', 'october', 'november', 'december',
    'soybean', 'soybeans', 'soybean oil', 'soybean meal', 'corn', 'wheat'
)
GROUP BY country
ORDER BY record_count DESC;

\echo ''
\echo '=== DIAGNOSTIC: Value ranges by unit ==='
SELECT unit,
       COUNT(*) as records,
       MIN(value) as min_val,
       MAX(value) as max_val,
       AVG(value) as avg_val
FROM bronze.trade_data_raw
WHERE period_type = 'marketing_year'
GROUP BY unit;

\echo ''
\echo '=== Now applying fixes ==='

-- ============================================================================
-- DROP old/broken views that aren't in codebase
-- ============================================================================
DROP VIEW IF EXISTS gold.trade_yoy_changes CASCADE;
DROP VIEW IF EXISTS gold.recent_trade_flows CASCADE;

-- ============================================================================
-- RECREATE the is_valid_country function with all filters
-- ============================================================================
DROP FUNCTION IF EXISTS gold.is_valid_country(TEXT) CASCADE;

CREATE FUNCTION gold.is_valid_country(val TEXT) RETURNS BOOLEAN AS $$
BEGIN
    IF val IS NULL OR LENGTH(TRIM(val)) < 2 THEN
        RETURN FALSE;
    END IF;

    -- Exclude aggregates and totals
    IF LOWER(val) IN (
        'total', 'totals', 'sum total', 'grand total', 'subtotal',
        'world', 'world total', 'other', 'others', 'unknown', 'unspecified',
        'n/a', 'na', 'nan', 'none', '', 'data', 'source', 'census'
    ) THEN RETURN FALSE; END IF;

    -- Exclude regional aggregates
    IF LOWER(val) IN (
        'western hemisphere', 'eastern hemisphere', 'asia and oceania',
        'asia', 'europe', 'africa', 'americas', 'oceania',
        'north america', 'south america', 'central america', 'middle east',
        'sub-saharan africa', 'north africa', 'east asia', 'southeast asia',
        'former soviet union', 'fsu', 'cis', 'asean'
    ) THEN RETURN FALSE; END IF;

    -- Exclude commodity names (exact matches)
    IF LOWER(val) IN (
        'soybean', 'soybeans', 'soybean oil', 'soybean meal', 'soymeal', 'soyoil', 'soy',
        'corn', 'wheat', 'rice', 'barley', 'sorghum', 'oats',
        'rapeseed', 'canola', 'sunflower', 'cottonseed', 'peanut', 'peanuts',
        'palm oil', 'palm kernel', 'coconut', 'coconut oil', 'palm',
        'tallow', 'lard', 'grease', 'uco', 'dco', 'cwg',
        'oil', 'meal', 'seed', 'kernel', 'veg', 'crude', 'refined',
        'edible', 'inedible', 'major', 'vegetable', 'animal'
    ) THEN RETURN FALSE; END IF;

    -- Exclude FULL month names
    IF LOWER(val) IN (
        'january', 'february', 'march', 'april', 'may', 'june',
        'july', 'august', 'september', 'october', 'november', 'december'
    ) THEN RETURN FALSE; END IF;

    -- Exclude abbreviated month names
    IF LOWER(val) IN (
        'jan', 'feb', 'mar', 'apr', 'jun', 'jul', 'aug', 'sep', 'oct', 'nov', 'dec'
    ) THEN RETURN FALSE; END IF;

    -- Exclude if contains commodity keywords (catches things like "US Soybeans")
    IF LOWER(val) LIKE '%soy%' THEN RETURN FALSE; END IF;
    IF LOWER(val) LIKE '%meal%' THEN RETURN FALSE; END IF;
    IF LOWER(val) LIKE '%bean%' THEN RETURN FALSE; END IF;
    IF LOWER(val) LIKE '%cotton%' THEN RETURN FALSE; END IF;
    IF LOWER(val) LIKE '%rape%' THEN RETURN FALSE; END IF;
    IF LOWER(val) LIKE '%palm%' THEN RETURN FALSE; END IF;
    IF LOWER(val) LIKE '%tallow%' THEN RETURN FALSE; END IF;
    IF LOWER(val) LIKE '%kernel%' THEN RETURN FALSE; END IF;
    IF LOWER(val) LIKE '%grease%' THEN RETURN FALSE; END IF;
    IF LOWER(val) LIKE '%lard%' THEN RETURN FALSE; END IF;

    -- Exclude if looks like a year or number
    IF val ~ '^\d+$' THEN RETURN FALSE; END IF;
    IF val ~ '^\d{2}/\d{2}$' THEN RETURN FALSE; END IF;
    IF val ~ '^\d{4}/\d{2}$' THEN RETURN FALSE; END IF;

    RETURN TRUE;
END;
$$ LANGUAGE plpgsql IMMUTABLE;

\echo 'Function gold.is_valid_country created'

-- ============================================================================
-- Test the function
-- ============================================================================
\echo ''
\echo '=== Testing is_valid_country function ==='
SELECT val, gold.is_valid_country(val) as is_valid
FROM (VALUES
    ('China'), ('Brazil'), ('United States'), ('Canada'), ('Mexico'),
    ('November'), ('December'), ('January'), ('Soybean'), ('Soybeans'),
    ('Soybean Oil'), ('Total'), ('World'), ('2019/20')
) AS t(val);

-- ============================================================================
-- RECREATE trade_flows view with better filtering
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
    -- Convert all values to Million MT
    CAST(
        CASE
            WHEN unit = 'Million MT' THEN value
            WHEN unit = 'Thousand MT' THEN value / 1000.0
            WHEN unit = 'MT' THEN value / 1000000.0
            WHEN unit IS NULL THEN value / 1000000.0
            ELSE value / 1000000.0
        END
    AS NUMERIC(20, 6)) AS value_million_mt,
    source_file,
    sheet_name,
    created_at
FROM parsed_trade
WHERE gold.is_valid_country(partner_country)
  AND gold.is_valid_country(reporting_country);

\echo 'View gold.trade_flows recreated'

-- ============================================================================
-- RECREATE dependent views
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

\echo 'View gold.trade_summary_by_year recreated'

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

\echo 'View gold.top_exporters recreated'

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

\echo 'View gold.top_importers recreated'

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

\echo 'View gold.trade_dashboard_stats recreated'

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

\echo 'View gold.trade_flow_matrix recreated'

-- ============================================================================
-- Final diagnostic
-- ============================================================================
\echo ''
\echo '=== FINAL CHECK: Sample from gold.trade_flows ==='
SELECT commodity, origin_country, destination_country, flow_type, marketing_year, value_million_mt
FROM gold.trade_flows
WHERE period_type = 'marketing_year'
LIMIT 10;

\echo ''
\echo '=== FINAL CHECK: trade_dashboard_stats ==='
SELECT * FROM gold.trade_dashboard_stats LIMIT 10;

\echo ''
\echo '=== FIX COMPLETE ==='
\echo 'Refresh Power BI to see updated data'
