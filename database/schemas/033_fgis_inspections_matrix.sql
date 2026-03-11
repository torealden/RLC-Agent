-- =============================================================================
-- FGIS Inspections Matrix Views
-- Maps FGIS destination names to spreadsheet rows for VBA updater (Ctrl+G)
-- =============================================================================

-- ---------------------------------------------------------------------------
-- Destination → Spreadsheet Row Mapping
-- FGIS uses abbreviated names; we map them to trade_country_reference rows
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS silver.fgis_destination_mapping (
    fgis_destination    VARCHAR(100) PRIMARY KEY,
    standard_name       VARCHAR(100) NOT NULL,
    spreadsheet_row     INTEGER NOT NULL,
    is_regional_total   BOOLEAN DEFAULT FALSE
);

-- Populate from trade_country_reference for exact matches
INSERT INTO silver.fgis_destination_mapping (fgis_destination, standard_name, spreadsheet_row, is_regional_total)
SELECT tcr.country_name, tcr.country_name, tcr.spreadsheet_row, tcr.is_regional_total
FROM silver.trade_country_reference tcr
WHERE tcr.spreadsheet_row BETWEEN 4 AND 217
  AND tcr.is_regional_total = FALSE
ON CONFLICT (fgis_destination) DO NOTHING;

-- FGIS-specific name overrides (abbreviated forms used in FGIS data)
INSERT INTO silver.fgis_destination_mapping (fgis_destination, standard_name, spreadsheet_row) VALUES
    ('KOREA REP',   'KOREA, SOUTH',         80),
    ('UN KINGDOM',  'UNITED KINGDOM',        45),
    ('UN ARAB EM',  'UNITED ARAB EMIRATES', 104),
    ('TRINIDAD',    'TRINIDAD AND TOBAGO',   209),
    ('DOM REP',     'DOMINICAN REPUBLIC',    186),
    ('IVORY COAST', 'COTE D IVOIRE',         122),
    ('CZECH REP',   'CZECH REPUBLIC',         10),
    ('BOSNIA',      'BOSNIA AND HERZEGOVINA',  36),
    ('MACEDONIA',   'NORTH MACEDONIA',         40),
    ('BURMA',       'MYANMAR',                 88),
    ('ZAIRE',       'CONGO (KINSHASA)',       121),
    ('SWAZILAND',   'ESWATINI',              127),
    ('UPPER VOLTA', 'BURKINA FASO',          113),
    ('DAHOMEY',     'BENIN',                 111),
    ('KAMPUCHEA',   'CAMBODIA',               68),
    ('W GERMANY',   'GERMANY',                15),
    ('E GERMANY',   'GERMANY',                15),
    ('HONG KONG',   'HONG KONG',              71),
    ('RUSSIA',      'RUSSIA',                 55),
    ('COSTA RICA',  'COSTA RICA',            182),
    ('NEW ZEALAND', 'NEW ZEALAND',            90),
    ('SOUTH AFRICA','SOUTH AFRICA',          155),
    ('SRI LANKA',   'SRI LANKA',              99),
    ('EL SALVADOR', 'EL SALVADOR',           188),
    ('SAUDI ARABIA','SAUDI ARABIA',           97),
    ('SIERRA LEONE','SIERRA LEONE',          153)
ON CONFLICT (fgis_destination) DO UPDATE SET
    standard_name = EXCLUDED.standard_name,
    spreadsheet_row = EXCLUDED.spreadsheet_row;


-- ---------------------------------------------------------------------------
-- Monthly Matrix View — for "Monthly Soybean Inspections" tab
-- Output: year, month, grain, destination, spreadsheet_row, quantity (000 MT)
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW gold.fgis_inspections_monthly_matrix AS
WITH monthly_data AS (
    SELECT
        EXTRACT(YEAR FROM h.cert_date)::int AS year,
        EXTRACT(MONTH FROM h.cert_date)::int AS month,
        UPPER(h.grain::text) AS grain,
        h.destination,
        SUM(h.metric_tons) AS metric_tons,
        SUM(h.bushels_1000) AS bushels_1000
    FROM bronze.fgis_inspections_history h
    WHERE h.type_service IN ('IW', 'I')
    GROUP BY 1, 2, UPPER(h.grain::text), h.destination
),
mapped AS (
    -- Country-level rows (rows 5-214, excluding regional subtotals)
    SELECT
        md.year,
        md.month,
        md.grain,
        dm.standard_name AS country_name,
        dm.spreadsheet_row,
        FALSE AS is_regional_total,
        md.metric_tons / 1000.0 AS quantity  -- 000 MT
    FROM monthly_data md
    JOIN silver.fgis_destination_mapping dm ON UPPER(md.destination) = UPPER(dm.fgis_destination)
    WHERE dm.spreadsheet_row BETWEEN 5 AND 214
      AND dm.is_regional_total = FALSE
),
world_totals AS (
    -- Row 217: WORLD TOTAL (all destinations)
    SELECT
        year, month, grain,
        'WORLD TOTAL' AS country_name,
        217 AS spreadsheet_row,
        TRUE AS is_regional_total,
        SUM(metric_tons) / 1000.0 AS quantity
    FROM monthly_data
    GROUP BY year, month, grain
),
mexico_comparison AS (
    -- Row 224: Mexico inspections (thousand bushels) for comparison section
    SELECT
        year, month, grain,
        'MEXICO INSPECTIONS' AS country_name,
        224 AS spreadsheet_row,
        FALSE AS is_regional_total,
        SUM(bushels_1000) AS quantity  -- thousand bushels for comparison row
    FROM monthly_data
    WHERE UPPER(destination) = 'MEXICO'
    GROUP BY year, month, grain
),
canada_comparison AS (
    -- Row 230: Canada inspections (thousand bushels) for comparison section
    SELECT
        year, month, grain,
        'CANADA INSPECTIONS' AS country_name,
        230 AS spreadsheet_row,
        FALSE AS is_regional_total,
        SUM(bushels_1000) AS quantity  -- thousand bushels for comparison row
    FROM monthly_data
    WHERE UPPER(destination) = 'CANADA'
    GROUP BY year, month, grain
)
SELECT * FROM mapped
UNION ALL
SELECT * FROM world_totals
UNION ALL
SELECT * FROM mexico_comparison
UNION ALL
SELECT * FROM canada_comparison
ORDER BY year, month, spreadsheet_row;


-- ---------------------------------------------------------------------------
-- Weekly Matrix View — for "Weekly Soybean Inspections" tab
-- Output: week_ending, grain, destination, spreadsheet_row, quantity (000 MT)
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW gold.fgis_inspections_weekly_matrix AS
WITH weekly_data AS (
    SELECT
        -- Align to Thursday (ISO day 4) for week-ending
        h.cert_date + ((4 - EXTRACT(ISODOW FROM h.cert_date))::int % 7 + 7) % 7 * INTERVAL '1 day' AS week_ending,
        UPPER(h.grain::text) AS grain,
        h.destination,
        SUM(h.metric_tons) AS metric_tons,
        SUM(h.bushels_1000) AS bushels_1000
    FROM bronze.fgis_inspections_history h
    WHERE h.type_service IN ('IW', 'I')
    GROUP BY 1, UPPER(h.grain::text), h.destination
),
mapped AS (
    SELECT
        wd.week_ending,
        EXTRACT(YEAR FROM wd.week_ending)::int AS year,
        EXTRACT(MONTH FROM wd.week_ending)::int AS month,
        wd.grain,
        dm.standard_name AS country_name,
        dm.spreadsheet_row,
        FALSE AS is_regional_total,
        wd.metric_tons / 1000.0 AS quantity
    FROM weekly_data wd
    JOIN silver.fgis_destination_mapping dm ON UPPER(wd.destination) = UPPER(dm.fgis_destination)
    WHERE dm.spreadsheet_row BETWEEN 5 AND 214
      AND dm.is_regional_total = FALSE
),
world_totals AS (
    SELECT
        week_ending,
        EXTRACT(YEAR FROM week_ending)::int AS year,
        EXTRACT(MONTH FROM week_ending)::int AS month,
        grain,
        'WORLD TOTAL' AS country_name,
        217 AS spreadsheet_row,
        TRUE AS is_regional_total,
        SUM(metric_tons) / 1000.0 AS quantity
    FROM weekly_data
    GROUP BY week_ending, grain
),
mexico_comparison AS (
    SELECT
        week_ending,
        EXTRACT(YEAR FROM week_ending)::int AS year,
        EXTRACT(MONTH FROM week_ending)::int AS month,
        grain,
        'MEXICO INSPECTIONS' AS country_name,
        224 AS spreadsheet_row,
        FALSE AS is_regional_total,
        SUM(bushels_1000) AS quantity
    FROM weekly_data
    WHERE UPPER(destination) = 'MEXICO'
    GROUP BY week_ending, grain
),
canada_comparison AS (
    SELECT
        week_ending,
        EXTRACT(YEAR FROM week_ending)::int AS year,
        EXTRACT(MONTH FROM week_ending)::int AS month,
        grain,
        'CANADA INSPECTIONS' AS country_name,
        230 AS spreadsheet_row,
        FALSE AS is_regional_total,
        SUM(bushels_1000) AS quantity
    FROM weekly_data
    WHERE UPPER(destination) = 'CANADA'
    GROUP BY week_ending, grain
)
SELECT * FROM mapped
UNION ALL
SELECT * FROM world_totals
UNION ALL
SELECT * FROM mexico_comparison
UNION ALL
SELECT * FROM canada_comparison
ORDER BY week_ending, spreadsheet_row;


-- =============================================================================
-- THOUSAND BUSHELS VIEWS — for spreadsheet import
-- Same structure as above but quantity is in thousand bushels throughout.
-- The VBA updater (Ctrl+G) queries these instead of the MT views.
-- =============================================================================

-- ---------------------------------------------------------------------------
-- Monthly Matrix (Thousand Bushels)
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW gold.fgis_inspections_monthly_matrix_kbu AS
WITH monthly_data AS (
    SELECT
        EXTRACT(YEAR FROM h.cert_date)::int AS year,
        EXTRACT(MONTH FROM h.cert_date)::int AS month,
        UPPER(h.grain::text) AS grain,
        h.destination,
        SUM(h.bushels_1000) AS bushels_1000
    FROM bronze.fgis_inspections_history h
    WHERE h.type_service IN ('IW', 'I')
    GROUP BY 1, 2, UPPER(h.grain::text), h.destination
),
mapped AS (
    SELECT
        md.year,
        md.month,
        md.grain,
        dm.standard_name AS country_name,
        dm.spreadsheet_row,
        FALSE AS is_regional_total,
        md.bushels_1000 AS quantity   -- thousand bushels
    FROM monthly_data md
    JOIN silver.fgis_destination_mapping dm ON UPPER(md.destination) = UPPER(dm.fgis_destination)
    WHERE dm.spreadsheet_row BETWEEN 5 AND 214
      AND dm.is_regional_total = FALSE
),
world_totals AS (
    SELECT
        year, month, grain,
        'WORLD TOTAL' AS country_name,
        217 AS spreadsheet_row,
        TRUE AS is_regional_total,
        SUM(bushels_1000) AS quantity
    FROM monthly_data
    GROUP BY year, month, grain
),
mexico_comparison AS (
    SELECT
        year, month, grain,
        'MEXICO INSPECTIONS' AS country_name,
        224 AS spreadsheet_row,
        FALSE AS is_regional_total,
        SUM(bushels_1000) AS quantity
    FROM monthly_data
    WHERE UPPER(destination) = 'MEXICO'
    GROUP BY year, month, grain
),
canada_comparison AS (
    SELECT
        year, month, grain,
        'CANADA INSPECTIONS' AS country_name,
        230 AS spreadsheet_row,
        FALSE AS is_regional_total,
        SUM(bushels_1000) AS quantity
    FROM monthly_data
    WHERE UPPER(destination) = 'CANADA'
    GROUP BY year, month, grain
)
SELECT * FROM mapped
UNION ALL
SELECT * FROM world_totals
UNION ALL
SELECT * FROM mexico_comparison
UNION ALL
SELECT * FROM canada_comparison
ORDER BY year, month, spreadsheet_row;


-- ---------------------------------------------------------------------------
-- Weekly Matrix (Thousand Bushels)
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW gold.fgis_inspections_weekly_matrix_kbu AS
WITH weekly_data AS (
    SELECT
        h.cert_date + ((4 - EXTRACT(ISODOW FROM h.cert_date))::int % 7 + 7) % 7 * INTERVAL '1 day' AS week_ending,
        UPPER(h.grain::text) AS grain,
        h.destination,
        SUM(h.bushels_1000) AS bushels_1000
    FROM bronze.fgis_inspections_history h
    WHERE h.type_service IN ('IW', 'I')
    GROUP BY 1, UPPER(h.grain::text), h.destination
),
mapped AS (
    SELECT
        wd.week_ending,
        EXTRACT(YEAR FROM wd.week_ending)::int AS year,
        EXTRACT(MONTH FROM wd.week_ending)::int AS month,
        wd.grain,
        dm.standard_name AS country_name,
        dm.spreadsheet_row,
        FALSE AS is_regional_total,
        wd.bushels_1000 AS quantity   -- thousand bushels
    FROM weekly_data wd
    JOIN silver.fgis_destination_mapping dm ON UPPER(wd.destination) = UPPER(dm.fgis_destination)
    WHERE dm.spreadsheet_row BETWEEN 5 AND 214
      AND dm.is_regional_total = FALSE
),
world_totals AS (
    SELECT
        week_ending,
        EXTRACT(YEAR FROM week_ending)::int AS year,
        EXTRACT(MONTH FROM week_ending)::int AS month,
        grain,
        'WORLD TOTAL' AS country_name,
        217 AS spreadsheet_row,
        TRUE AS is_regional_total,
        SUM(bushels_1000) AS quantity
    FROM weekly_data
    GROUP BY week_ending, grain
),
mexico_comparison AS (
    SELECT
        week_ending,
        EXTRACT(YEAR FROM week_ending)::int AS year,
        EXTRACT(MONTH FROM week_ending)::int AS month,
        grain,
        'MEXICO INSPECTIONS' AS country_name,
        224 AS spreadsheet_row,
        FALSE AS is_regional_total,
        SUM(bushels_1000) AS quantity
    FROM weekly_data
    WHERE UPPER(destination) = 'MEXICO'
    GROUP BY week_ending, grain
),
canada_comparison AS (
    SELECT
        week_ending,
        EXTRACT(YEAR FROM week_ending)::int AS year,
        EXTRACT(MONTH FROM week_ending)::int AS month,
        grain,
        'CANADA INSPECTIONS' AS country_name,
        230 AS spreadsheet_row,
        FALSE AS is_regional_total,
        SUM(bushels_1000) AS quantity
    FROM weekly_data
    WHERE UPPER(destination) = 'CANADA'
    GROUP BY week_ending, grain
)
SELECT * FROM mapped
UNION ALL
SELECT * FROM world_totals
UNION ALL
SELECT * FROM mexico_comparison
UNION ALL
SELECT * FROM canada_comparison
ORDER BY week_ending, spreadsheet_row;
