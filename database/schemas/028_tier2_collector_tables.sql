-- 028_tier2_collector_tables.sql
-- Bronze tables for Tier 2 collectors: FAOSTAT, IBGE SIDRA, MAGyP, IMEA
-- Created 2026-03-10

BEGIN;

-- ============================================================
-- 1. FAOSTAT — Global production, trade, food balance, prices
-- ============================================================
CREATE TABLE IF NOT EXISTS bronze.faostat_data (
    id SERIAL PRIMARY KEY,
    country VARCHAR(10) NOT NULL,
    country_name VARCHAR(100),
    commodity VARCHAR(50) NOT NULL,
    commodity_name VARCHAR(100),
    domain VARCHAR(10) NOT NULL,       -- QCL, TCL, FBS, PP
    element VARCHAR(100) NOT NULL,
    year INT NOT NULL,
    value NUMERIC(18,4),
    unit VARCHAR(50),
    flag VARCHAR(10),
    flow VARCHAR(20),                  -- import/export (trade only)
    measure VARCHAR(20),               -- quantity/value (trade only)
    partner VARCHAR(100),              -- partner country (trade only)
    source VARCHAR(20) DEFAULT 'FAOSTAT',
    collected_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(country, commodity, domain, element, year)
);

CREATE INDEX IF NOT EXISTS idx_faostat_commodity ON bronze.faostat_data(commodity);
CREATE INDEX IF NOT EXISTS idx_faostat_country ON bronze.faostat_data(country);
CREATE INDEX IF NOT EXISTS idx_faostat_year ON bronze.faostat_data(year);

-- ============================================================
-- 2. IBGE SIDRA — Brazil state-level production (PAM + LSPA)
-- ============================================================
CREATE TABLE IF NOT EXISTS bronze.ibge_sidra (
    id SERIAL PRIMARY KEY,
    commodity VARCHAR(50) NOT NULL,
    product_code VARCHAR(10),
    product_name VARCHAR(100),
    state VARCHAR(5) NOT NULL,
    state_name VARCHAR(100),
    year INT NOT NULL,
    period VARCHAR(20) NOT NULL,
    period_name VARCHAR(100),
    variable VARCHAR(30) NOT NULL,     -- production, area, yield, value, monthly
    value NUMERIC(18,4),
    source VARCHAR(20) DEFAULT 'IBGE_SIDRA',
    collected_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(commodity, state, period, variable)
);

CREATE INDEX IF NOT EXISTS idx_ibge_commodity ON bronze.ibge_sidra(commodity);
CREATE INDEX IF NOT EXISTS idx_ibge_state ON bronze.ibge_sidra(state);
CREATE INDEX IF NOT EXISTS idx_ibge_year ON bronze.ibge_sidra(year);

-- ============================================================
-- 3. MAGyP Argentina — Stocks, production, area estimates
-- ============================================================
CREATE TABLE IF NOT EXISTS bronze.magyp_argentina (
    id SERIAL PRIMARY KEY,
    commodity VARCHAR(50) NOT NULL,
    country VARCHAR(5) NOT NULL DEFAULT 'AR',
    data_type VARCHAR(30) NOT NULL,    -- stocks, production, area, prices
    crop_year VARCHAR(15),
    report_date VARCHAR(30),           -- text, may be YYYY-MM or full date
    province VARCHAR(100),
    production_tonnes NUMERIC(18,2),
    planted_area_ha NUMERIC(18,2),
    harvested_area_ha NUMERIC(18,2),
    yield_kg_ha NUMERIC(12,2),
    stocks_tonnes NUMERIC(18,2),
    production_mmt NUMERIC(12,4),
    area_mha NUMERIC(12,4),
    estimate_type VARCHAR(50),
    source VARCHAR(50) DEFAULT 'MAGyP',
    note TEXT,
    collected_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(commodity, data_type, crop_year, report_date, province)
);

CREATE INDEX IF NOT EXISTS idx_magyp_commodity ON bronze.magyp_argentina(commodity);
CREATE INDEX IF NOT EXISTS idx_magyp_crop_year ON bronze.magyp_argentina(crop_year);

-- ============================================================
-- 4. IMEA Mato Grosso — Progress, production, costs, prices
-- ============================================================
CREATE TABLE IF NOT EXISTS bronze.imea_mato_grosso (
    id SERIAL PRIMARY KEY,
    commodity VARCHAR(50) NOT NULL,
    data_type VARCHAR(30) NOT NULL,    -- progress, production, costs, prices, supply_demand
    crop_year VARCHAR(15),
    report_date DATE,
    region VARCHAR(50),
    detail_key VARCHAR(100),           -- flexible sub-key (cost_type, location, etc.)
    value_numeric NUMERIC(18,4),
    value_text TEXT,
    unit VARCHAR(30),
    source VARCHAR(20) DEFAULT 'IMEA',
    note TEXT,
    collected_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(commodity, data_type, crop_year, detail_key)
);

CREATE INDEX IF NOT EXISTS idx_imea_commodity ON bronze.imea_mato_grosso(commodity);
CREATE INDEX IF NOT EXISTS idx_imea_data_type ON bronze.imea_mato_grosso(data_type);

-- Freshness tracking is auto-populated from core.collection_status
-- when the dispatcher runs each collector.

COMMIT;
