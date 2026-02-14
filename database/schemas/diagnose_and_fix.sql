-- ============================================================================
-- DIAGNOSTIC AND FIX SCRIPT
-- ============================================================================
-- This script will:
-- 1. Show what schemas exist
-- 2. Show what tables/views exist in each schema
-- 3. Identify any OLD/BROKEN data that shouldn't be there
-- 4. Clean it up if needed
--
-- Run in psql: \i 'C:/Users/torem/Dropbox/RLC Documents/LLM Model and Documents/Projects/RLC-Agent/database/diagnose_and_fix.sql'
-- ============================================================================

\echo ''
\echo '============================================================'
\echo 'DIAGNOSTIC: Current Database State'
\echo '============================================================'

\echo ''
\echo '--- SCHEMAS ---'
SELECT schema_name FROM information_schema.schemata
WHERE schema_name NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
ORDER BY schema_name;

\echo ''
\echo '--- BRONZE TABLES ---'
SELECT table_name FROM information_schema.tables
WHERE table_schema = 'bronze' AND table_type = 'BASE TABLE'
ORDER BY table_name;

\echo ''
\echo '--- GOLD VIEWS ---'
SELECT table_name FROM information_schema.tables
WHERE table_schema = 'gold' AND table_type = 'VIEW'
ORDER BY table_name;

\echo ''
\echo '--- BRONZE TABLE ROW COUNTS ---'

-- Check if trade_data_raw exists (it should NOT exist in clean database)
SELECT 'trade_data_raw EXISTS - THIS IS OLD DATA!' AS warning
FROM information_schema.tables
WHERE table_schema = 'bronze' AND table_name = 'trade_data_raw';

-- Count ERS tables
SELECT 'ers_oilcrops_raw' AS table_name, COUNT(*) AS row_count FROM bronze.ers_oilcrops_raw
UNION ALL
SELECT 'ers_wheat_raw', COUNT(*) FROM bronze.ers_wheat_raw;

\echo ''
\echo '============================================================'
\echo 'CHECKING FOR OLD/BAD DATA'
\echo '============================================================'

-- Check if trade_data_raw has any rows
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'bronze' AND table_name = 'trade_data_raw') THEN
        RAISE NOTICE 'WARNING: bronze.trade_data_raw EXISTS - this is OLD broken data!';
    ELSE
        RAISE NOTICE 'GOOD: bronze.trade_data_raw does not exist';
    END IF;
END $$;

\echo ''
\echo '============================================================'
\echo 'SAMPLE DATA FROM ERS TABLES (should be clean)'
\echo '============================================================'

\echo ''
\echo '--- US Soybeans 2024/25 from ERS data ---'
SELECT attribute_desc, amount, unit_desc
FROM bronze.ers_oilcrops_raw
WHERE commodity = 'Soybeans'
  AND marketing_year = '2024/25'
  AND geography_desc = 'United States'
  AND attribute_desc IN ('Production', 'Exports', 'Crush', 'Ending stocks')
ORDER BY attribute_desc;

\echo ''
\echo '============================================================'
\echo 'CLEANUP: Removing old broken views if they exist'
\echo '============================================================'

-- Drop old views that reference trade_data_raw (these shouldn't exist)
DROP VIEW IF EXISTS gold.trade_flows CASCADE;
DROP VIEW IF EXISTS gold.trade_summary_by_year CASCADE;
DROP VIEW IF EXISTS gold.top_exporters CASCADE;
DROP VIEW IF EXISTS gold.top_importers CASCADE;
DROP VIEW IF EXISTS gold.trade_flow_matrix CASCADE;
DROP VIEW IF EXISTS gold.soybean_trade_flows CASCADE;
DROP VIEW IF EXISTS gold.soybean_flow_matrix CASCADE;
DROP VIEW IF EXISTS gold.rapeseed_trade_flows CASCADE;
DROP VIEW IF EXISTS gold.palm_oil_trade_flows CASCADE;
DROP VIEW IF EXISTS gold.trade_dashboard_stats CASCADE;
DROP VIEW IF EXISTS gold.recent_trade_flows CASCADE;
DROP VIEW IF EXISTS gold.trade_yoy_changes CASCADE;

-- Drop the helper function for old trade views
DROP FUNCTION IF EXISTS gold.is_valid_country(TEXT) CASCADE;

-- Drop old trade_data_raw table if it exists
DROP TABLE IF EXISTS bronze.trade_data_raw CASCADE;

\echo 'Old trade views and tables dropped (if they existed)'

\echo ''
\echo '============================================================'
\echo 'FINAL STATE: What should exist'
\echo '============================================================'

\echo ''
\echo '--- BRONZE TABLES (should be ers_oilcrops_raw and ers_wheat_raw only) ---'
SELECT table_name FROM information_schema.tables
WHERE table_schema = 'bronze' AND table_type = 'BASE TABLE'
ORDER BY table_name;

\echo ''
\echo '--- GOLD VIEWS (should be ERS-based views only) ---'
SELECT table_name FROM information_schema.tables
WHERE table_schema = 'gold' AND table_type = 'VIEW'
ORDER BY table_name;

\echo ''
\echo '============================================================'
\echo 'VERIFICATION: Gold view sample data'
\echo '============================================================'

\echo ''
\echo '--- gold.us_soybean_balance_sheet (2024/25) ---'
SELECT * FROM gold.us_soybean_balance_sheet WHERE marketing_year = '2024/25';

\echo ''
\echo '--- gold.commodity_dashboard_stats ---'
SELECT * FROM gold.commodity_dashboard_stats;

\echo ''
\echo '============================================================'
\echo 'DIAGNOSTIC COMPLETE'
\echo '============================================================'
\echo ''
\echo 'If you see clean data above, refresh Power BI:'
\echo '1. In Power BI, click "Refresh" or close and reopen the file'
\echo '2. In Navigator, look for gold.us_soybean_balance_sheet'
\echo '3. Do NOT select old views like gold.trade_flows'
\echo ''
