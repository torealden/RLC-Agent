-- 040_eia_natural_gas.sql
-- Bronze table for EIA natural gas series (monthly + weekly + daily).
-- Similar structure to 039_eia_monthly_biofuels but with a `frequency` column
-- to distinguish M/W/D data.

CREATE SCHEMA IF NOT EXISTS bronze;

CREATE TABLE IF NOT EXISTS bronze.eia_natural_gas (
    id            BIGSERIAL PRIMARY KEY,
    period        DATE        NOT NULL,       -- month-start for M, week-end for W, date for D
    frequency     VARCHAR(4)  NOT NULL,       -- M | W | D
    series_id     VARCHAR(64) NOT NULL,
    attribute     VARCHAR(32) NOT NULL,
        -- marketed_production | dry_production | total_consumption |
        -- lng_exports | pipeline_exports | total_imports |
        -- working_gas_storage | henry_hub_spot | residential_price |
        -- commercial_price | industrial_price | citygate_price
    region        VARCHAR(16) NOT NULL DEFAULT 'NUS',
    value         NUMERIC(18,6),
    units         VARCHAR(16),                -- MMCF | BCF | $/MMBTU | $/MCF
    description   TEXT,
    raw_payload   JSONB,
    collected_at  TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE (period, series_id, region)
);

CREATE INDEX IF NOT EXISTS idx_eia_ng_period
    ON bronze.eia_natural_gas(period);
CREATE INDEX IF NOT EXISTS idx_eia_ng_freq_attr
    ON bronze.eia_natural_gas(frequency, attribute);

COMMENT ON TABLE bronze.eia_natural_gas IS
    'EIA natural gas data (monthly production/consumption/trade + weekly storage + daily Henry Hub spot). '
    'Loaded by EIANaturalGasCollector registered as eia_natural_gas in dispatcher.';
