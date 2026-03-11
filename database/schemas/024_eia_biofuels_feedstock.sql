-- =============================================================================
-- EIA Monthly Biofuels Capacity and Feedstock Data (Form EIA-819)
-- =============================================================================
-- Monthly feedstock consumption by type and operable production capacity.
-- Source: https://www.eia.gov/biofuels/update/
--   table1.xlsx = capacity (MMGY), table2.xlsx = feedstocks (million lbs/month)
--
-- Workflow:
--   1. python src/tools/eia_biofuels_collector.py --download
--   2. In Excel: Ctrl+Shift+D to pull from database into spreadsheet
-- =============================================================================

-- =============================================================================
-- BRONZE: Monthly feedstock consumption
-- =============================================================================
CREATE TABLE IF NOT EXISTS bronze.eia_feedstock_monthly (
    id BIGSERIAL PRIMARY KEY,

    -- Natural key
    year INT NOT NULL,
    month INT NOT NULL,
    source_sheet VARCHAR(10) NOT NULL,       -- table_2a, table_2b, table_2c, table_2d
    feedstock_name VARCHAR(100) NOT NULL,    -- Normalized EIA label
    plant_type VARCHAR(30) NOT NULL DEFAULT 'total',  -- total, biodiesel, renewable_diesel

    -- Value
    quantity_mil_lbs NUMERIC(12,4),          -- NULL if withheld or no data
    is_withheld BOOLEAN DEFAULT FALSE,       -- TRUE when EIA shows "W"
    is_no_data BOOLEAN DEFAULT FALSE,        -- TRUE when EIA shows "-"

    -- Metadata
    source_file VARCHAR(200),
    collected_at TIMESTAMPTZ DEFAULT NOW(),

    UNIQUE (year, month, source_sheet, feedstock_name, plant_type)
);

CREATE INDEX IF NOT EXISTS idx_eia_feedstock_monthly_ym
    ON bronze.eia_feedstock_monthly(year, month);

COMMENT ON TABLE bronze.eia_feedstock_monthly IS
    'EIA Form 819 monthly feedstock consumption for biofuel production. '
    'Source: table2.xlsx from https://www.eia.gov/biofuels/update/. '
    'Units: million pounds per month.';

-- =============================================================================
-- BRONZE: Monthly production capacity
-- =============================================================================
CREATE TABLE IF NOT EXISTS bronze.eia_capacity_monthly (
    id BIGSERIAL PRIMARY KEY,

    -- Natural key
    year INT NOT NULL,
    month INT NOT NULL,
    biofuel_type VARCHAR(60) NOT NULL,      -- Biodiesel, Fuel Ethanol, Renewable Diesel and Other Biofuels

    -- Value
    capacity_mmgy NUMERIC(12,3),            -- Million gallons per year

    -- Metadata
    source_file VARCHAR(200),
    collected_at TIMESTAMPTZ DEFAULT NOW(),

    UNIQUE (year, month, biofuel_type)
);

CREATE INDEX IF NOT EXISTS idx_eia_capacity_monthly_ym
    ON bronze.eia_capacity_monthly(year, month);

COMMENT ON TABLE bronze.eia_capacity_monthly IS
    'EIA Form 819 monthly operable biofuel production capacity. '
    'Source: table1.xlsx from https://www.eia.gov/biofuels/update/. '
    'Units: million gallons per year (MMGY).';

-- =============================================================================
-- REFERENCE: Spreadsheet column mapping
-- =============================================================================
CREATE TABLE IF NOT EXISTS reference.feedstock_column_mapping (
    id SERIAL PRIMARY KEY,
    source_table VARCHAR(20) NOT NULL,      -- 'feedstock' or 'capacity'
    feedstock_name VARCHAR(100) NOT NULL,    -- Must match bronze table
    plant_type VARCHAR(30) NOT NULL DEFAULT 'total',
    tab_name VARCHAR(50) NOT NULL,
    column_number INT NOT NULL,
    data_start_row INT NOT NULL DEFAULT 5,
    conversion_factor NUMERIC(18,10) NOT NULL DEFAULT 1.0
);

CREATE INDEX IF NOT EXISTS idx_feedstock_colmap_lookup
    ON reference.feedstock_column_mapping(source_table, feedstock_name, plant_type);

COMMENT ON TABLE reference.feedstock_column_mapping IS
    'Maps EIA feedstock/capacity data to spreadsheet columns. '
    'conversion_factor converts bronze units to display units '
    '(e.g., 1000.0 converts million lbs to thousand lbs).';

-- Populate mappings (truncate for idempotency)
TRUNCATE reference.feedstock_column_mapping RESTART IDENTITY;

INSERT INTO reference.feedstock_column_mapping
    (source_table, feedstock_name, plant_type, tab_name, column_number, data_start_row, conversion_factor)
VALUES
-- =============================================================================
-- feedstock_monthly tab (data_start_row = 5)
-- Vegetable Oils: B=2 SBO, C=3 Canola, D=4 Corn Oil, E=5 Other Veg
-- F=6 Veg Total (FORMULA)
-- Fats & Greases: G=7 Tallow, H=8 White Grease, I=9 Yellow Grease, J=10 Poultry, K=11 Other
-- L=12 F&G Total (FORMULA), M=13 Grand Total (FORMULA)
-- Ethanol: N=14 Corn, O=15 Grain Sorghum
-- =============================================================================
('feedstock', 'Soybean Oil', 'total', 'feedstock_monthly', 2, 5, 1.0),
('feedstock', 'Canola Oil', 'total', 'feedstock_monthly', 3, 5, 1.0),
('feedstock', 'Corn Oil', 'total', 'feedstock_monthly', 4, 5, 1.0),
('feedstock', 'Other Vegetable Oil', 'total', 'feedstock_monthly', 5, 5, 1.0),
('feedstock', 'Tallow', 'total', 'feedstock_monthly', 7, 5, 1.0),
('feedstock', 'White Grease', 'total', 'feedstock_monthly', 8, 5, 1.0),
('feedstock', 'Yellow Grease', 'total', 'feedstock_monthly', 9, 5, 1.0),
('feedstock', 'Poultry', 'total', 'feedstock_monthly', 10, 5, 1.0),
('feedstock', 'Other Waste', 'total', 'feedstock_monthly', 11, 5, 1.0),
('feedstock', 'Corn', 'total', 'feedstock_monthly', 14, 5, 1.0),
('feedstock', 'Grain Sorghum', 'total', 'feedstock_monthly', 15, 5, 1.0),

-- =============================================================================
-- feedstock_veg_split tab (data_start_row = 5)
-- SBO: B=2 BD, C=3 RD, D=4 Total (FORMULA)
-- Canola: E=5 BD, F=6 RD, G=7 Total (FORMULA)
-- Corn Oil: H=8 BD, I=9 RD, J=10 Total (FORMULA)
-- Other: K=11 BD, L=12 RD, M=13 Total (FORMULA)
-- =============================================================================
('feedstock', 'Soybean Oil', 'biodiesel', 'feedstock_veg_split', 2, 5, 1.0),
('feedstock', 'Soybean Oil', 'renewable_diesel', 'feedstock_veg_split', 3, 5, 1.0),
('feedstock', 'Canola Oil', 'biodiesel', 'feedstock_veg_split', 5, 5, 1.0),
('feedstock', 'Canola Oil', 'renewable_diesel', 'feedstock_veg_split', 6, 5, 1.0),
('feedstock', 'Corn Oil', 'biodiesel', 'feedstock_veg_split', 8, 5, 1.0),
('feedstock', 'Corn Oil', 'renewable_diesel', 'feedstock_veg_split', 9, 5, 1.0),
('feedstock', 'Other Vegetable Oil', 'biodiesel', 'feedstock_veg_split', 11, 5, 1.0),
('feedstock', 'Other Vegetable Oil', 'renewable_diesel', 'feedstock_veg_split', 12, 5, 1.0),

-- =============================================================================
-- capacity_monthly tab (data_start_row = 5)
-- B=2 Biodiesel, C=3 Fuel Ethanol, D=4 RD & Other, E=5 Total (FORMULA)
-- =============================================================================
('capacity', 'Biodiesel', 'total', 'capacity_monthly', 2, 5, 1.0),
('capacity', 'Fuel Ethanol', 'total', 'capacity_monthly', 3, 5, 1.0),
('capacity', 'Renewable Diesel and Other Biofuels', 'total', 'capacity_monthly', 4, 5, 1.0);

-- =============================================================================
-- GOLD: Feedstock matrix view for VBA ODBC consumption
-- =============================================================================
CREATE OR REPLACE VIEW gold.feedstock_monthly_matrix AS
SELECT
    f.year,
    f.month,
    m.tab_name,
    m.data_start_row,
    m.column_number,
    SUM(f.quantity_mil_lbs * m.conversion_factor) AS value
FROM bronze.eia_feedstock_monthly f
JOIN reference.feedstock_column_mapping m
    ON m.source_table = 'feedstock'
    AND f.feedstock_name = m.feedstock_name
    AND f.plant_type = m.plant_type
WHERE f.quantity_mil_lbs IS NOT NULL
GROUP BY f.year, f.month, m.tab_name, m.data_start_row, m.column_number
HAVING SUM(f.quantity_mil_lbs * m.conversion_factor) IS NOT NULL
ORDER BY m.tab_name, f.year, f.month, m.column_number;

COMMENT ON VIEW gold.feedstock_monthly_matrix IS
    'EIA feedstock data pre-mapped to spreadsheet columns. '
    'Consumed by VBA ODBC (EIAFeedstockUpdater.bas).';

-- =============================================================================
-- GOLD: Capacity matrix view for VBA ODBC consumption
-- =============================================================================
CREATE OR REPLACE VIEW gold.capacity_monthly_matrix AS
SELECT
    c.year,
    c.month,
    m.tab_name,
    m.data_start_row,
    m.column_number,
    c.capacity_mmgy * m.conversion_factor AS value
FROM bronze.eia_capacity_monthly c
JOIN reference.feedstock_column_mapping m
    ON m.source_table = 'capacity'
    AND c.biofuel_type = m.feedstock_name
    AND m.plant_type = 'total'
ORDER BY m.tab_name, c.year, c.month, m.column_number;

COMMENT ON VIEW gold.capacity_monthly_matrix IS
    'EIA capacity data pre-mapped to spreadsheet columns. '
    'Consumed by VBA ODBC (EIAFeedstockUpdater.bas).';

-- =============================================================================
-- GOLD: Combined matrix view (UNION of feedstock + capacity)
-- =============================================================================
CREATE OR REPLACE VIEW gold.eia_biofuels_matrix AS
SELECT * FROM gold.feedstock_monthly_matrix
UNION ALL
SELECT * FROM gold.capacity_monthly_matrix
ORDER BY tab_name, year, month, column_number;

COMMENT ON VIEW gold.eia_biofuels_matrix IS
    'Combined EIA feedstock + capacity matrix. Single query for VBA ODBC.';
