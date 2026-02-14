-- ============================================================================
-- RLC-Agent CNS: Collection Status (Dispatcher Layer)
-- ============================================================================
-- File: 018_cns_collection_status.sql
-- Purpose: Track every collector run — what was collected, when, how many rows.
--          This is the foundation of the Central Nervous System: the LLM can
--          query data_freshness to know what's current and what's stale.
-- Depends: 01_schemas.sql (core schema), 02_core_dimensions.sql (data_source)
-- Source:  WIRING_PLAN.md Part 1
-- ============================================================================

-- ---------------------------------------------------------------------------
-- 1. Add schedule columns to core.data_source (for data_freshness view)
-- ---------------------------------------------------------------------------
ALTER TABLE public.data_source
    ADD COLUMN IF NOT EXISTS category VARCHAR(50),
    ADD COLUMN IF NOT EXISTS expected_frequency VARCHAR(20),
    ADD COLUMN IF NOT EXISTS expected_release_day VARCHAR(50),
    ADD COLUMN IF NOT EXISTS expected_release_time_et VARCHAR(20);

COMMENT ON COLUMN public.data_source.category IS 'Domain category: grains, oilseeds, energy, biofuels, weather, positioning, trade';
COMMENT ON COLUMN public.data_source.expected_frequency IS 'Expected update cadence: daily, weekly, monthly, event-driven';
COMMENT ON COLUMN public.data_source.expected_release_day IS 'Day of week or specific pattern: Thursday, Monday (Apr-Nov), WASDE dates';
COMMENT ON COLUMN public.data_source.expected_release_time_et IS 'Expected release time in ET: 08:30, 12:00, 15:30';

-- Seed schedule metadata for known collectors
UPDATE public.data_source SET category = 'grains', expected_frequency = 'monthly', expected_release_day = 'WASDE dates', expected_release_time_et = '12:00' WHERE code = 'wasde';
UPDATE public.data_source SET category = 'grains', expected_frequency = 'weekly', expected_release_day = 'Monday' WHERE code = 'nass';
UPDATE public.data_source SET category = 'grains', expected_frequency = 'daily' WHERE code = 'ams';
UPDATE public.data_source SET category = 'energy', expected_frequency = 'weekly', expected_release_day = 'Wednesday', expected_release_time_et = '10:30' WHERE code = 'eia';
UPDATE public.data_source SET category = 'weather', expected_frequency = 'daily' WHERE code = 'noaa';
UPDATE public.data_source SET category = 'grains', expected_frequency = 'daily', expected_release_time_et = '17:00' WHERE code = 'cme';
UPDATE public.data_source SET category = 'grains', expected_frequency = 'daily', expected_release_time_et = '17:00' WHERE code = 'cbot';

-- Add missing data sources for collectors not yet registered
INSERT INTO public.data_source (code, name, source_type, description, update_frequency, category, expected_frequency, expected_release_day, expected_release_time_et)
VALUES
    ('cftc', 'CFTC Commitments of Traders', 'api', 'Weekly positioning data for futures markets', 'weekly', 'positioning', 'weekly', 'Friday', '15:30'),
    ('fas', 'USDA Foreign Agricultural Service', 'api', 'Export sales, PSD global balance sheets', 'weekly', 'trade', 'weekly', 'Thursday', '08:30'),
    ('conab', 'CONAB Brazil', 'api', 'Brazilian crop estimates and S&D', 'monthly', 'grains', 'monthly', '~10th of month', '09:00'),
    ('census', 'US Census Bureau', 'api', 'International trade data by HS code', 'monthly', 'trade', 'monthly', 'Specific dates', '08:30'),
    ('epa', 'EPA RFS/EMTS', 'api', 'RIN generation and transaction data', 'monthly', 'biofuels', 'monthly', NULL, NULL),
    ('drought_monitor', 'US Drought Monitor', 'scrape', 'Weekly drought conditions', 'weekly', 'weather', 'weekly', 'Thursday', NULL),
    ('fgis', 'FGIS Export Inspections', 'api', 'Weekly export inspection data', 'weekly', 'trade', 'weekly', 'Monday', '11:00'),
    ('cgc', 'Canadian Grain Commission', 'api', 'Canadian grain handling data', 'weekly', 'trade', 'weekly', NULL, NULL),
    ('statscan', 'Statistics Canada', 'api', 'Canadian trade and production data', 'monthly', 'trade', 'monthly', NULL, NULL),
    ('mpob', 'Malaysia Palm Oil Board', 'api', 'Malaysian palm oil data', 'monthly', 'oilseeds', 'monthly', '~10th of month', NULL),
    ('ndvi', 'NDVI Satellite Data', 'api', 'Vegetation index satellite imagery', 'weekly', 'weather', 'weekly', NULL, NULL)
ON CONFLICT (code) DO UPDATE SET
    category = EXCLUDED.category,
    expected_frequency = EXCLUDED.expected_frequency,
    expected_release_day = EXCLUDED.expected_release_day,
    expected_release_time_et = EXCLUDED.expected_release_time_et;


-- ---------------------------------------------------------------------------
-- 2. COLLECTION_STATUS: One row per collector run
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS core.collection_status (
    id              SERIAL PRIMARY KEY,
    collector_name  TEXT NOT NULL,           -- e.g. 'cftc_cot', 'usda_fas_psd'
    run_started_at  TIMESTAMPTZ NOT NULL,
    run_finished_at TIMESTAMPTZ,
    status          TEXT NOT NULL DEFAULT 'running',  -- running | success | failed | partial
    rows_collected  INTEGER DEFAULT 0,
    rows_inserted   INTEGER DEFAULT 0,
    error_message   TEXT,
    data_period     TEXT,                    -- e.g. 'week_ending_2026-02-13'
    commodities     TEXT[],                  -- e.g. {'corn','wheat','soybeans'}
    is_new_data     BOOLEAN DEFAULT TRUE,    -- false if re-run with no new records
    triggered_by    TEXT DEFAULT 'scheduler',-- scheduler | manual | backfill
    notes           TEXT
);

CREATE INDEX IF NOT EXISTS idx_collection_status_recent
    ON core.collection_status (collector_name, run_started_at DESC);

COMMENT ON TABLE core.collection_status IS
    'CNS Dispatcher Layer: Tracks every collector run. One row per execution. '
    'The LLM queries this (via data_freshness view) to know what data is current.';


-- ---------------------------------------------------------------------------
-- 3. LATEST_COLLECTIONS: Most recent run per collector
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW core.latest_collections AS
SELECT DISTINCT ON (collector_name)
    collector_name,
    run_finished_at,
    status,
    rows_collected,
    data_period,
    commodities,
    is_new_data
FROM core.collection_status
ORDER BY collector_name, run_started_at DESC;


-- ---------------------------------------------------------------------------
-- 4. DATA_FRESHNESS: Is data stale? When should it update?
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW core.data_freshness AS
SELECT
    cs.collector_name,
    ds.name AS display_name,
    ds.category,
    cs.run_finished_at AS last_collected,
    cs.status AS last_status,
    cs.rows_collected AS last_row_count,
    cs.data_period,
    cs.is_new_data,
    -- How stale is this data?
    EXTRACT(EPOCH FROM (NOW() - cs.run_finished_at)) / 3600 AS hours_since_collection,
    -- When should it run next?
    ds.expected_frequency,
    ds.expected_release_day,
    ds.expected_release_time_et,
    -- Is it overdue?
    CASE
        WHEN ds.expected_frequency = 'daily'
            AND cs.run_finished_at < CURRENT_DATE THEN TRUE
        WHEN ds.expected_frequency = 'weekly'
            AND cs.run_finished_at < CURRENT_DATE - INTERVAL '8 days' THEN TRUE
        WHEN ds.expected_frequency = 'monthly'
            AND cs.run_finished_at < CURRENT_DATE - INTERVAL '35 days' THEN TRUE
        ELSE FALSE
    END AS is_overdue
FROM core.latest_collections cs
LEFT JOIN public.data_source ds ON ds.collector_key = cs.collector_name;


-- ---------------------------------------------------------------------------
-- 5. COLLECTOR_KEY: Maps data_source rows to dispatcher collector names
-- ---------------------------------------------------------------------------
ALTER TABLE public.data_source ADD COLUMN IF NOT EXISTS collector_key TEXT;
COMMENT ON COLUMN public.data_source.collector_key IS 'Maps to dispatcher collector_name in collection_status';

UPDATE public.data_source SET collector_key = 'cftc_cot' WHERE code = 'CFTC';
UPDATE public.data_source SET collector_key = 'usda_fas_export_sales' WHERE code = 'USDA_FAS';
UPDATE public.data_source SET collector_key = 'usda_nass_crop_progress' WHERE code = 'USDA_NASS';
UPDATE public.data_source SET collector_key = 'usda_wasde' WHERE code = 'USDA_WASDE';
UPDATE public.data_source SET collector_key = 'eia_ethanol' WHERE code = 'EIA';
UPDATE public.data_source SET collector_key = 'conab' WHERE code = 'CONAB';
UPDATE public.data_source SET collector_key = 'cme_settlements' WHERE code = 'CME';
UPDATE public.data_source SET collector_key = 'census_trade' WHERE code = 'CENSUS';
UPDATE public.data_source SET collector_key = 'epa_rfs' WHERE code = 'EPA';
UPDATE public.data_source SET collector_key = 'drought_monitor' WHERE code = 'DROUGHT_MONITOR';
UPDATE public.data_source SET collector_key = 'canada_cgc' WHERE code = 'CGC';
UPDATE public.data_source SET collector_key = 'canada_statscan' WHERE code = 'STATSCAN';
UPDATE public.data_source SET collector_key = 'mpob' WHERE code = 'MPOB';


-- ---------------------------------------------------------------------------
-- Verification
-- ---------------------------------------------------------------------------
DO $$
BEGIN
    RAISE NOTICE '018_cns_collection_status.sql executed successfully:';
    RAISE NOTICE '  - core.data_source: schedule columns added';
    RAISE NOTICE '  - core.collection_status: table created';
    RAISE NOTICE '  - core.latest_collections: view created';
    RAISE NOTICE '  - core.data_freshness: view created';
END $$;
