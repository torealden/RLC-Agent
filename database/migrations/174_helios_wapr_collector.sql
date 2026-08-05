-- 174: helios_wapr daily collector support — forecast vintage archive + freshness registration
--
-- bronze.helios_climate_risk was a ONE-TIME 2026-07-21 load (scripts/collect_helios_climate.py):
-- no collector, no schedule, actuals since 7/22 never observed, and every held forecast row is
-- 7/21-vintage. The daily collector (src/agents/collectors/global/helios_wapr_collector.py)
-- re-pulls the full index and upserts it, which would OVERWRITE forecast history.
--
-- Vintage ruling (made in-session 2026-08-05, Tore to confirm — see handoff): keep
-- bronze.helios_climate_risk as the CURRENT-state table (upsert; existing consumers unchanged)
-- and archive is_forecasted rows per pull date here. Full-horizon archive ≈ 63k rows/day
-- (~23M rows/yr): not small, but unstored vintages are unrecoverable while stored ones can be
-- pruned — when forced to choose without a ruling, store. If Tore wants a horizon cap
-- (e.g. keep vintages only within 90 days of date_on) it is one DELETE + one WHERE clause.
-- Vintages 7/22 -> 8/4 do not exist and cannot be backfilled.

BEGIN;

CREATE TABLE IF NOT EXISTS bronze.helios_climate_risk_vintage (
    vintage_date    date NOT NULL,          -- pull date (UTC)
    commodity_slug  text NOT NULL,
    country_code    text NOT NULL,
    date_on         date NOT NULL,
    wapr            numeric,
    wapr_hist_avg   numeric,
    too_hot_wapr    numeric,
    too_cold_wapr   numeric,
    too_wet_wapr    numeric,
    too_dry_wapr    numeric,
    severity        text,
    phase           text,
    harvest_year    int,
    collected_at    timestamptz DEFAULT now(),
    PRIMARY KEY (vintage_date, commodity_slug, country_code, date_on)
);

COMMENT ON TABLE bronze.helios_climate_risk_vintage IS
    'Helios WAPR forecast rows (is_forecasted=true) archived per pull date. The current-state '
    'table bronze.helios_climate_risk keeps only the latest pull; this preserves forecast '
    'vintage history for revision/skill work. First vintage: 2026-08-05.';

INSERT INTO data_source (code, name, description, base_url, api_type, update_frequency,
                         timezone, is_active, category, expected_frequency,
                         expected_release_time_et, collector_key)
SELECT 'HELIOS_WAPR',
       'Helios Climate Risk Index (WAPR)',
       'Daily country-level WAPR composite + four risk factors (too hot/cold/wet/dry) for '
       '7 commodities x ~45 countries, ~5yr history + ~2yr forward. Full-index upsert daily; '
       'forecast rows also archived by vintage.',
       'https://api.helios.sc', 'json', 'daily', 'UTC', true, 'weather',
       'daily', '07:30', 'helios_wapr'
WHERE NOT EXISTS (SELECT 1 FROM data_source WHERE collector_key = 'helios_wapr');

COMMIT;
