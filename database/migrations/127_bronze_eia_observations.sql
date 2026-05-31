-- Migration 127: bronze.eia_observations — clean long/tidy EIA series store
--
-- The existing bronze.eia_raw_ingestion stores raw JSON payloads and is
-- limited (only 612 rows, no historical backfill). The petroleum/ethanol/
-- natural_gas collectors land into ad-hoc tables.
--
-- This table is the canonical long/tidy store for all EIA v2 observations.
-- One row per (series_id, period). Schema is series-agnostic — every EIA
-- time series fits, from daily WTI spot to monthly biofuel feedstock use.
--
-- Backfill horizon (Tore 2026-05-31): 1990-01-01 forward. Don't extend
-- earlier than the ag models (unless an energy-specific reason emerges).

BEGIN;

CREATE TABLE IF NOT EXISTS bronze.eia_observations (
    id               BIGSERIAL PRIMARY KEY,
    series_id        VARCHAR(120) NOT NULL,
    period           DATE         NOT NULL,
    value            NUMERIC,
    unit             VARCHAR(40),
    frequency        VARCHAR(20),                  -- 'daily', 'weekly', 'monthly', 'annual'
    series_name      VARCHAR(200),                 -- short human label (e.g., 'wti_spot')
    description      TEXT,                         -- EIA's series-description field
    api_route        VARCHAR(200),                 -- v2 endpoint path, e.g., 'petroleum/pri/spt'
    raw_payload      JSONB,                        -- full row metadata from EIA (duoarea, process, etc.)
    source           VARCHAR(20) DEFAULT 'EIA',
    collected_at     TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (series_id, period)
);

CREATE INDEX IF NOT EXISTS idx_eia_obs_series_period
    ON bronze.eia_observations (series_id, period DESC);

CREATE INDEX IF NOT EXISTS idx_eia_obs_period
    ON bronze.eia_observations (period DESC);

CREATE INDEX IF NOT EXISTS idx_eia_obs_name
    ON bronze.eia_observations (series_name);

COMMENT ON TABLE bronze.eia_observations IS
'Canonical long/tidy store for all EIA v2 API observations. One row per (series_id, period). Replaces ad-hoc eia_petroleum / eia_raw_ingestion patterns. Backfilled from 1990-01-01 forward per Tore.';

COMMIT;

-- Verification:
-- SELECT COUNT(*) FROM bronze.eia_observations;
-- SELECT series_id, COUNT(*) AS n, MIN(period), MAX(period)
-- FROM bronze.eia_observations GROUP BY series_id ORDER BY series_id;
