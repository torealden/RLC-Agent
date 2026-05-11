-- ============================================================================
-- Migration 043: BBD forecast scenarios + RFS volume projections
-- ============================================================================
-- Source: D:\Biomass-Based Diesel\Mandate Projections.xlsx (snapshot 2020-12-11)
--
-- Scope: focused ingest of UNIQUE forecast/projection content. Skipped:
--   - EMTS DATA.xlsx — historical RIN data already live in bronze.epa_emts_monthly
--     (16 years, 3,416 rows from 2010-2025)
--   - Biodiesel EIA Monthly Production Clean — already live in bronze.eia_monthly_biofuels
--     (5,088 rows)
--   - Biodiesel and RD Forward Curve Forecast.xlsx — broken (external link error)
--   - RIN Balance Sheet Forecast.xlsx — contains chartsheets that openpyxl can't process
--   - EMTS Forecast.xlsx — 24 sheets with non-uniform schemas; defer to v2
--
-- Captured here:
--   - 10-Year RFS Curves (D3/D4/D5/D6/D7 RIN generation by year, 2018-2030)
--   - Soybean Oil S&D Balance Sheets in 3 scenarios (High/Mid/Low) by year
-- ============================================================================

-- ----------------------------------------------------------------------------
-- 1. silver.rfs_volume_projections
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS silver.rfs_volume_projections (
    id                  BIGSERIAL PRIMARY KEY,
    snapshot_date       DATE NOT NULL,         -- when this projection was made
    snapshot_label      TEXT NOT NULL,         -- e.g., 'mandate_projections_2020_12'
    scenario            TEXT NOT NULL,         -- 'Upper' / 'Lower' / etc.
    rin_d_code          TEXT NOT NULL,         -- 'D3' / 'D4' / 'D5' / 'D6' / 'D7'
    measure             TEXT NOT NULL,         -- 'rin_generation_billion_rins' / 'gallons_billion'
    year                INTEGER NOT NULL,
    value               NUMERIC(10,4),
    source_file         TEXT,
    ingested_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (snapshot_date, snapshot_label, scenario, rin_d_code, measure, year)
);
CREATE INDEX IF NOT EXISTS idx_rfs_proj_year ON silver.rfs_volume_projections (year);
CREATE INDEX IF NOT EXISTS idx_rfs_proj_d_code ON silver.rfs_volume_projections (rin_d_code);

COMMENT ON TABLE silver.rfs_volume_projections IS
'Long-term RFS volume projections by D-code and scenario. Snapshot-based — each row records a projection made on snapshot_date for target year. Useful for Section 7 Policy Monitor of the BBD weekly: shows what was anticipated at the time and how the mandate trajectory has shifted.';


-- ----------------------------------------------------------------------------
-- 2. silver.scenario_balance_sheets — multi-commodity, multi-scenario S&D
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS silver.scenario_balance_sheets (
    id                  BIGSERIAL PRIMARY KEY,
    snapshot_date       DATE NOT NULL,
    snapshot_label      TEXT NOT NULL,
    commodity           TEXT NOT NULL,         -- 'soybean_oil' / 'corn' / etc.
    scenario            TEXT NOT NULL,         -- 'High' / 'Mid' / 'Low'
    line_item           TEXT NOT NULL,         -- 'Carryin' / 'Production' / 'Imports' / 'Total Supply' / etc.
    year                INTEGER NOT NULL,
    value               NUMERIC(14,4),
    unit                TEXT NOT NULL,         -- 'million_pounds' / 'million_bushels' / etc.
    source_file         TEXT,
    ingested_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (snapshot_date, snapshot_label, commodity, scenario, line_item, year)
);
CREATE INDEX IF NOT EXISTS idx_scen_bs_year      ON silver.scenario_balance_sheets (year);
CREATE INDEX IF NOT EXISTS idx_scen_bs_commodity ON silver.scenario_balance_sheets (commodity);
CREATE INDEX IF NOT EXISTS idx_scen_bs_scenario  ON silver.scenario_balance_sheets (scenario);

COMMENT ON TABLE silver.scenario_balance_sheets IS
'Multi-scenario S&D balance sheets. Each row = (commodity, scenario, line_item, year). Drives Section 6 S&D Watch of the BBD weekly when comparing actuals against multi-scenario projections. snapshot_label distinguishes vintages.';


-- ----------------------------------------------------------------------------
-- Permissions
-- ----------------------------------------------------------------------------
GRANT SELECT, INSERT, UPDATE ON silver.rfs_volume_projections     TO PUBLIC;
GRANT SELECT, INSERT, UPDATE ON silver.scenario_balance_sheets    TO PUBLIC;
GRANT USAGE, SELECT ON SEQUENCE silver.rfs_volume_projections_id_seq  TO PUBLIC;
GRANT USAGE, SELECT ON SEQUENCE silver.scenario_balance_sheets_id_seq TO PUBLIC;
