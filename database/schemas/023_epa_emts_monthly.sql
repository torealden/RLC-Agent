-- =============================================================================
-- EPA EMTS Monthly RIN Generation Detail
-- =============================================================================
-- Monthly data with full fuel category and producer type breakdown.
-- Source: EPA interactive table CSV download (manual)
--   https://www.epa.gov/fuels-registration-reporting-and-compliance-help/
--   rins-generated-transactions
-- Data starts: July 2010 (EMTS launch)
--
-- Workflow:
--   1. Download CSV from EPA website
--   2. python src/tools/emts_csv_loader.py path/to/csv
--   3. In Excel: Ctrl+E to pull from database into spreadsheet
-- =============================================================================

-- =============================================================================
-- BRONZE: Monthly detail by fuel category and producer type
-- =============================================================================
CREATE TABLE IF NOT EXISTS bronze.epa_emts_monthly (
    id BIGSERIAL PRIMARY KEY,

    -- Natural key
    rin_year INT NOT NULL,
    month INT NOT NULL,
    producer_type VARCHAR(50) NOT NULL,   -- Domestic, Importer, Foreign Generation
    d_code VARCHAR(5) NOT NULL,           -- D3, D4, D5, D6, D7
    fuel_category VARCHAR(100) NOT NULL,  -- e.g. 'Biodiesel (EV 1.5)'

    -- Values
    rins BIGINT,                          -- RIN count
    volume BIGINT,                        -- Volume in gallons

    -- Metadata
    source_file VARCHAR(200),
    collected_at TIMESTAMPTZ DEFAULT NOW(),

    UNIQUE (rin_year, month, producer_type, d_code, fuel_category)
);

CREATE INDEX IF NOT EXISTS idx_epa_emts_monthly_ym
    ON bronze.epa_emts_monthly(rin_year, month);
CREATE INDEX IF NOT EXISTS idx_epa_emts_monthly_dcode
    ON bronze.epa_emts_monthly(d_code);

COMMENT ON TABLE bronze.epa_emts_monthly IS
    'EPA EMTS monthly RIN generation by D-code, fuel category, and producer type. '
    'Source: EPA interactive table CSV download. 46 unique combinations across 32 fuel category names.';

-- =============================================================================
-- REFERENCE: Spreadsheet column mapping
-- =============================================================================
-- Maps (d_code, fuel_category, producer_type) -> (tab_name, column_number)
-- Multiple EPA names can map to the same column (aggregated via SUM in gold view)
-- 100 mapping rows covering all 10 monthly tabs in EMTS Data spreadsheet
-- =============================================================================
CREATE TABLE IF NOT EXISTS reference.emts_column_mapping (
    id SERIAL PRIMARY KEY,
    d_code VARCHAR(5) NOT NULL,
    fuel_category VARCHAR(100) NOT NULL,
    producer_type VARCHAR(50) NOT NULL,
    tab_name VARCHAR(50) NOT NULL,
    column_number INT NOT NULL,
    value_type VARCHAR(10) NOT NULL CHECK (value_type IN ('volume', 'rins')),
    data_start_row INT NOT NULL DEFAULT 4
);

CREATE INDEX IF NOT EXISTS idx_emts_colmap_lookup
    ON reference.emts_column_mapping(d_code, fuel_category, producer_type);

COMMENT ON TABLE reference.emts_column_mapping IS
    'Maps EPA fuel categories to EMTS Data spreadsheet columns. '
    'Multiple EPA names may map to the same column (name evolution). '
    'The gold.emts_monthly_matrix view aggregates via SUM.';

-- Populate mappings (truncate for idempotency)
TRUNCATE reference.emts_column_mapping RESTART IDENTITY;

-- =============================================================================
-- Tab names match user's rfs_data.xlsm workbook:
--   D3_fuel_month, D3_rin_month, D4_fuel_month, D4_rins_month,
--   D5_fuel_month, D5_rins_month, D6_fuel_month, D6_rins_month,
--   D7_fuel_month, D7_rins_month
-- All tabs use data_start_row = 5 (4 header rows)
-- =============================================================================

INSERT INTO reference.emts_column_mapping
    (d_code, fuel_category, producer_type, tab_name, column_number, value_type, data_start_row)
VALUES
-- =============================================================================
-- D3 Volume (D3_fuel_month, data_start_row = 5)
-- B=2 CellEth Dom, C=3 CellEth Imp, E=5 CellRenGas Dom, F=6 CellRenGas Imp,
-- H=8 RenCNG Dom, I=9 RenCNG Imp, K=11 RenLNG Dom, L=12 RenLNG Imp
-- =============================================================================
('D3', 'Cellulosic Ethanol (EV 1.0)', 'Domestic', 'D3_fuel_month', 2, 'volume', 5),
('D3', 'Cellulosic Ethanol (EV 1.0)', 'Importer', 'D3_fuel_month', 3, 'volume', 5),
('D3', 'Cellulosic Renewable Gasoline Blendstock (EV application required)', 'Domestic', 'D3_fuel_month', 5, 'volume', 5),
('D3', 'Cellulosic Renewable Gasoline Blendstock (EV application required)', 'Importer', 'D3_fuel_month', 6, 'volume', 5),
('D3', 'Renewable Compressed Natural Gas', 'Domestic', 'D3_fuel_month', 8, 'volume', 5),
('D3', 'Renewable Compressed Natural Gas - CNG', 'Domestic', 'D3_fuel_month', 8, 'volume', 5),
('D3', 'Renewable Compressed Natural Gas', 'Importer', 'D3_fuel_month', 9, 'volume', 5),
('D3', 'Renewable Liquefied Natural Gas', 'Domestic', 'D3_fuel_month', 11, 'volume', 5),
('D3', 'Renewable Natural Gas - RNG', 'Domestic', 'D3_fuel_month', 11, 'volume', 5),
('D3', 'Renewable Liquefied Natural Gas', 'Importer', 'D3_fuel_month', 12, 'volume', 5),
('D3', 'Renewable Natural Gas - RNG', 'Importer', 'D3_fuel_month', 12, 'volume', 5),

-- =============================================================================
-- D3 RINs (D3_rin_month, same columns as D3 volume)
-- =============================================================================
('D3', 'Cellulosic Ethanol (EV 1.0)', 'Domestic', 'D3_rin_month', 2, 'rins', 5),
('D3', 'Cellulosic Ethanol (EV 1.0)', 'Importer', 'D3_rin_month', 3, 'rins', 5),
('D3', 'Cellulosic Renewable Gasoline Blendstock (EV application required)', 'Domestic', 'D3_rin_month', 5, 'rins', 5),
('D3', 'Cellulosic Renewable Gasoline Blendstock (EV application required)', 'Importer', 'D3_rin_month', 6, 'rins', 5),
('D3', 'Renewable Compressed Natural Gas', 'Domestic', 'D3_rin_month', 8, 'rins', 5),
('D3', 'Renewable Compressed Natural Gas - CNG', 'Domestic', 'D3_rin_month', 8, 'rins', 5),
('D3', 'Renewable Compressed Natural Gas', 'Importer', 'D3_rin_month', 9, 'rins', 5),
('D3', 'Renewable Liquefied Natural Gas', 'Domestic', 'D3_rin_month', 11, 'rins', 5),
('D3', 'Renewable Natural Gas - RNG', 'Domestic', 'D3_rin_month', 11, 'rins', 5),
('D3', 'Renewable Liquefied Natural Gas', 'Importer', 'D3_rin_month', 12, 'rins', 5),
('D3', 'Renewable Natural Gas - RNG', 'Importer', 'D3_rin_month', 12, 'rins', 5),

-- =============================================================================
-- D4 Volume (D4_fuel_month, data_start_row = 5)
-- B=2 Biodiesel Dom, C=3 Biodiesel FG, D=4 Biodiesel Imp,
-- F=6 NERD Dom EV1.5, G=7 NERD Dom EV1.6, H=8 NERD Dom EV1.7,
-- J=10 NERD FG EV1.5, K=11 NERD FG EV1.6, L=12 NERD FG EV1.7,
-- N=14 NERD Imp (all EVs aggregated into one column),
-- P=16 RJF Dom EV1.6, Q=17 RJF Dom EV App Req (=EV1.0),
-- S=19 RJF Foreign Gen, U=21 RHO Dom
-- =============================================================================
('D4', 'Biodiesel (EV 1.5)', 'Domestic', 'D4_fuel_month', 2, 'volume', 5),
('D4', 'Biodiesel (EV 1.5)', 'Foreign Generation', 'D4_fuel_month', 3, 'volume', 5),
('D4', 'Biodiesel (EV 1.5)', 'Importer', 'D4_fuel_month', 4, 'volume', 5),
('D4', 'Non-ester Renewable Diesel (EV 1.5)', 'Domestic', 'D4_fuel_month', 6, 'volume', 5),
('D4', 'Non-ester Renewable Diesel (EV 1.6)', 'Domestic', 'D4_fuel_month', 7, 'volume', 5),
('D4', 'Non-ester Renewable Diesel (EV 1.7)', 'Domestic', 'D4_fuel_month', 8, 'volume', 5),
('D4', 'Non-ester Renewable Diesel (EV 1.5)', 'Foreign Generation', 'D4_fuel_month', 10, 'volume', 5),
('D4', 'Non-ester Renewable Diesel (EV 1.6)', 'Foreign Generation', 'D4_fuel_month', 11, 'volume', 5),
('D4', 'Non-ester Renewable Diesel (EV 1.7)', 'Foreign Generation', 'D4_fuel_month', 12, 'volume', 5),
('D4', 'Non-ester Renewable Diesel (EV 1.5)', 'Importer', 'D4_fuel_month', 14, 'volume', 5),
('D4', 'Non-ester Renewable Diesel (EV 1.6)', 'Importer', 'D4_fuel_month', 14, 'volume', 5),
('D4', 'Non-ester Renewable Diesel (EV 1.7)', 'Importer', 'D4_fuel_month', 14, 'volume', 5),
('D4', 'Renewable Jet Fuel (EV 1.6)', 'Domestic', 'D4_fuel_month', 16, 'volume', 5),
('D4', 'Renewable Jet Fuel (EV 1.0)', 'Domestic', 'D4_fuel_month', 17, 'volume', 5),
('D4', 'Renewable Jet Fuel (EV 1.6)', 'Foreign Generation', 'D4_fuel_month', 19, 'volume', 5),
('D4', 'Renewable Heating Oil (EV 1.6)', 'Domestic', 'D4_fuel_month', 21, 'volume', 5),

-- =============================================================================
-- D4 RINs (D4_rins_month, SAME layout as D4_fuel_month)
-- =============================================================================
('D4', 'Biodiesel (EV 1.5)', 'Domestic', 'D4_rins_month', 2, 'rins', 5),
('D4', 'Biodiesel (EV 1.5)', 'Foreign Generation', 'D4_rins_month', 3, 'rins', 5),
('D4', 'Biodiesel (EV 1.5)', 'Importer', 'D4_rins_month', 4, 'rins', 5),
('D4', 'Non-ester Renewable Diesel (EV 1.5)', 'Domestic', 'D4_rins_month', 6, 'rins', 5),
('D4', 'Non-ester Renewable Diesel (EV 1.6)', 'Domestic', 'D4_rins_month', 7, 'rins', 5),
('D4', 'Non-ester Renewable Diesel (EV 1.7)', 'Domestic', 'D4_rins_month', 8, 'rins', 5),
('D4', 'Non-ester Renewable Diesel (EV 1.5)', 'Foreign Generation', 'D4_rins_month', 10, 'rins', 5),
('D4', 'Non-ester Renewable Diesel (EV 1.6)', 'Foreign Generation', 'D4_rins_month', 11, 'rins', 5),
('D4', 'Non-ester Renewable Diesel (EV 1.7)', 'Foreign Generation', 'D4_rins_month', 12, 'rins', 5),
('D4', 'Non-ester Renewable Diesel (EV 1.5)', 'Importer', 'D4_rins_month', 14, 'rins', 5),
('D4', 'Non-ester Renewable Diesel (EV 1.6)', 'Importer', 'D4_rins_month', 14, 'rins', 5),
('D4', 'Non-ester Renewable Diesel (EV 1.7)', 'Importer', 'D4_rins_month', 14, 'rins', 5),
('D4', 'Renewable Jet Fuel (EV 1.6)', 'Domestic', 'D4_rins_month', 16, 'rins', 5),
('D4', 'Renewable Jet Fuel (EV 1.0)', 'Domestic', 'D4_rins_month', 17, 'rins', 5),
('D4', 'Renewable Jet Fuel (EV 1.6)', 'Foreign Generation', 'D4_rins_month', 19, 'rins', 5),
('D4', 'Renewable Heating Oil (EV 1.6)', 'Domestic', 'D4_rins_month', 21, 'rins', 5),

-- =============================================================================
-- D5 Volume (D5_fuel_month, data_start_row = 5)
-- B=2 NonCellEth Dom, C=3 NonCellEth Imp, E=5 NERD Dom EV1.6, F=6 NERD Dom EV1.7,
-- J=10 RHO Dom, M=13 Biogas Dom, P=16 Naphtha EV1.4, Q=17 Naphtha EV1.5,
-- U=21 RenCNG Dom (3 EPA names aggregate), X=24 LPG Dom
-- =============================================================================
('D5', 'Non-cellulosic Ethanol (EV 1.0)', 'Domestic', 'D5_fuel_month', 2, 'volume', 5),
('D5', 'Non-cellulosic Ethanol (EV 1.0)', 'Importer', 'D5_fuel_month', 3, 'volume', 5),
('D5', 'Non-ester Renewable Diesel (EV 1.6)', 'Domestic', 'D5_fuel_month', 5, 'volume', 5),
('D5', 'Non-ester Renewable Diesel (EV 1.7)', 'Domestic', 'D5_fuel_month', 6, 'volume', 5),
('D5', 'Renewable Heating Oil (EV 1.6)', 'Domestic', 'D5_fuel_month', 10, 'volume', 5),
('D5', 'Biogas (77,000 Btu LHV/1 gallon)', 'Domestic', 'D5_fuel_month', 13, 'volume', 5),
('D5', 'Naphtha (EV 1.4)', 'Domestic', 'D5_fuel_month', 16, 'volume', 5),
('D5', 'Naphtha (EV 1.5)', 'Domestic', 'D5_fuel_month', 17, 'volume', 5),
('D5', 'Renewable Compressed Natural Gas', 'Domestic', 'D5_fuel_month', 21, 'volume', 5),
('D5', 'Renewable Compressed Natural Gas - CNG', 'Domestic', 'D5_fuel_month', 21, 'volume', 5),
('D5', 'Renewable Natural Gas - RNG', 'Domestic', 'D5_fuel_month', 21, 'volume', 5),
('D5', 'LPG (EV 1.1)', 'Domestic', 'D5_fuel_month', 24, 'volume', 5),

-- =============================================================================
-- D5 RINs (D5_rins_month, same columns as D5 volume)
-- =============================================================================
('D5', 'Non-cellulosic Ethanol (EV 1.0)', 'Domestic', 'D5_rins_month', 2, 'rins', 5),
('D5', 'Non-cellulosic Ethanol (EV 1.0)', 'Importer', 'D5_rins_month', 3, 'rins', 5),
('D5', 'Non-ester Renewable Diesel (EV 1.6)', 'Domestic', 'D5_rins_month', 5, 'rins', 5),
('D5', 'Non-ester Renewable Diesel (EV 1.7)', 'Domestic', 'D5_rins_month', 6, 'rins', 5),
('D5', 'Renewable Heating Oil (EV 1.6)', 'Domestic', 'D5_rins_month', 10, 'rins', 5),
('D5', 'Biogas (77,000 Btu LHV/1 gallon)', 'Domestic', 'D5_rins_month', 13, 'rins', 5),
('D5', 'Naphtha (EV 1.4)', 'Domestic', 'D5_rins_month', 16, 'rins', 5),
('D5', 'Naphtha (EV 1.5)', 'Domestic', 'D5_rins_month', 17, 'rins', 5),
('D5', 'Renewable Compressed Natural Gas', 'Domestic', 'D5_rins_month', 21, 'rins', 5),
('D5', 'Renewable Compressed Natural Gas - CNG', 'Domestic', 'D5_rins_month', 21, 'rins', 5),
('D5', 'Renewable Natural Gas - RNG', 'Domestic', 'D5_rins_month', 21, 'rins', 5),
('D5', 'LPG (EV 1.1)', 'Domestic', 'D5_rins_month', 24, 'rins', 5),

-- =============================================================================
-- D6 Volume (D6_fuel_month, data_start_row = 5)
-- B=2 Biodiesel Dom, C=3 Biodiesel Imp, E=5 NonCellEth Dom, F=6 NonCellEth Imp,
-- H=8 Butanol Dom, K=11 NERD Dom, L=12 NERD FG, O=15 RJF Dom, S=19 RenGas Dom
-- =============================================================================
('D6', 'Biodiesel (EV 1.5)', 'Domestic', 'D6_fuel_month', 2, 'volume', 5),
('D6', 'Biodiesel (EV 1.5)', 'Importer', 'D6_fuel_month', 3, 'volume', 5),
('D6', 'Non-cellulosic Ethanol (EV 1.0)', 'Domestic', 'D6_fuel_month', 5, 'volume', 5),
('D6', 'Non-cellulosic Ethanol (EV 1.0)', 'Importer', 'D6_fuel_month', 6, 'volume', 5),
('D6', 'Butanol (EV 1.3)', 'Domestic', 'D6_fuel_month', 8, 'volume', 5),
('D6', 'Non-ester Renewable Diesel (EV 1.7)', 'Domestic', 'D6_fuel_month', 11, 'volume', 5),
('D6', 'Non-ester Renewable Diesel (EV 1.7)', 'Foreign Generation', 'D6_fuel_month', 12, 'volume', 5),
('D6', 'Renewable Jet Fuel (EV 1.7)', 'Domestic', 'D6_fuel_month', 15, 'volume', 5),
('D6', 'Renewable Gasoline (EV 1.5)', 'Domestic', 'D6_fuel_month', 19, 'volume', 5),

-- =============================================================================
-- D6 RINs (D6_rins_month, same columns as D6 volume)
-- =============================================================================
('D6', 'Biodiesel (EV 1.5)', 'Domestic', 'D6_rins_month', 2, 'rins', 5),
('D6', 'Biodiesel (EV 1.5)', 'Importer', 'D6_rins_month', 3, 'rins', 5),
('D6', 'Non-cellulosic Ethanol (EV 1.0)', 'Domestic', 'D6_rins_month', 5, 'rins', 5),
('D6', 'Non-cellulosic Ethanol (EV 1.0)', 'Importer', 'D6_rins_month', 6, 'rins', 5),
('D6', 'Butanol (EV 1.3)', 'Domestic', 'D6_rins_month', 8, 'rins', 5),
('D6', 'Non-ester Renewable Diesel (EV 1.7)', 'Domestic', 'D6_rins_month', 11, 'rins', 5),
('D6', 'Non-ester Renewable Diesel (EV 1.7)', 'Foreign Generation', 'D6_rins_month', 12, 'rins', 5),
('D6', 'Renewable Jet Fuel (EV 1.7)', 'Domestic', 'D6_rins_month', 15, 'rins', 5),
('D6', 'Renewable Gasoline (EV 1.5)', 'Domestic', 'D6_rins_month', 19, 'rins', 5),

-- =============================================================================
-- D7 Volume (D7_fuel_month, data_start_row = 5)
-- B=2 CellDiesel Dom, H=8 CellHeatOil Imp
-- =============================================================================
('D7', 'Cellulosic Diesel (EV application required)', 'Domestic', 'D7_fuel_month', 2, 'volume', 5),
('D7', 'Cellulosic Heating Oil (EV application required)', 'Importer', 'D7_fuel_month', 8, 'volume', 5),

-- =============================================================================
-- D7 RINs (D7_rins_month, same columns as D7 volume)
-- =============================================================================
('D7', 'Cellulosic Diesel (EV application required)', 'Domestic', 'D7_rins_month', 2, 'rins', 5),
('D7', 'Cellulosic Heating Oil (EV application required)', 'Importer', 'D7_rins_month', 8, 'rins', 5);

-- =============================================================================
-- GOLD: Matrix view for VBA ODBC consumption
-- =============================================================================
-- Returns (year, month, tab_name, data_start_row, column_number, value)
-- VBA queries this view and writes each row to the specified cell.
-- SUM handles fuel category name aggregation (CNG/CNG-CNG, LNG/RNG).
-- =============================================================================
CREATE OR REPLACE VIEW gold.emts_monthly_matrix AS
SELECT
    e.rin_year AS year,
    e.month,
    m.tab_name,
    m.data_start_row,
    m.column_number,
    SUM(CASE
        WHEN m.value_type = 'volume' THEN e.volume
        WHEN m.value_type = 'rins' THEN e.rins
    END) AS value
FROM bronze.epa_emts_monthly e
JOIN reference.emts_column_mapping m
    ON e.d_code = m.d_code
    AND e.fuel_category = m.fuel_category
    AND e.producer_type = m.producer_type
GROUP BY e.rin_year, e.month, m.tab_name, m.data_start_row, m.column_number
HAVING SUM(CASE
    WHEN m.value_type = 'volume' THEN e.volume
    WHEN m.value_type = 'rins' THEN e.rins
END) IS NOT NULL
ORDER BY m.tab_name, e.rin_year, e.month, m.column_number;

COMMENT ON VIEW gold.emts_monthly_matrix IS
    'EMTS monthly data pre-mapped to spreadsheet columns. '
    'Consumed by VBA ODBC (EMTSDataUpdater.bas). '
    'Each row = one cell to write: (year,month) -> find row, column_number -> write value.';
