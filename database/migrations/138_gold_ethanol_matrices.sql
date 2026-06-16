-- Migration 138: gold.weekly_ethanol_matrix + gold.monthly_ethanol_matrix
-- =============================================================================
-- Long-format views feeding the us_grain_crush ethanol tabs. The tabs are built
-- around native EIA sourcekeys, so values are passed RAW (no unit conversion);
-- in-sheet formulas handle ratios/derived columns. One row per (period,
-- target spreadsheet column). The VBA finds the sheet row by date and writes
-- value into target_col. Only data columns are emitted; computed columns
-- (weekly H/K/N-V; monthly D/H Census + J/L-Y) are left to in-sheet formulas
-- (D/H Census ethanol trade is a separate source).
-- =============================================================================

CREATE OR REPLACE VIEW gold.weekly_ethanol_matrix AS
WITH m(series_id, target_col) AS (VALUES
    ('W_EPOOXE_YOP_NUS_MBBLD',     'B'),  -- oxygenate plant production
    ('W_EPOOXE_SAE_NUS_MBBL',      'C'),  -- ending stocks
    ('WGFRPUS2',                   'D'),  -- adj net production finished gasoline
    ('W_EPM0F_YPR_NUS_MBBLD',      'E'),
    ('WGRRPUS2',                   'F'),
    ('WG1TP_NUS_2',                'G'),
    ('WG4TP_NUS_2',                'I'),
    ('WG5TP_NUS_2',                'J'),
    ('W_EPM0CAL55_YPT_NUS_MBBLD',  'L'),
    ('WGFUPUS2',                   'M'),  -- product supplied finished gasoline
    ('W_EPOOXE_IM0_NUS-Z00_MBBLD', 'R'),  -- ethanol imports
    ('W_EPOOXE_YIR_NUS_MBBLD',     'S')   -- ethanol refiner/blender net input
)
SELECT o.period AS week_ending, m.target_col, o.value
FROM bronze.eia_observations o
JOIN m ON o.series_id = m.series_id
WHERE o.frequency = 'weekly';

CREATE OR REPLACE VIEW gold.monthly_ethanol_matrix AS
WITH m(series_id, target_col) AS (VALUES
    ('MFEIMUS1',                  'C'),  -- imports
    ('M_EPOOXE_VUA_NUS_MBBL',     'E'),  -- supply adjustment
    ('MFERIUS1',                  'F'),  -- refiner/blender net input
    ('M_EPOOXE_EEX_NUS-Z00_MBBL', 'G'),  -- exports
    ('MFESTUS1',                  'I'),  -- ending stocks
    ('MGFUPUS1',                  'K')   -- finished gasoline product supplied
)
SELECT o.period AS month_date, m.target_col, o.value
FROM bronze.eia_observations o
JOIN m ON o.series_id = m.series_id
WHERE o.frequency = 'monthly'
UNION ALL
-- monthly production (col B) lives in eia_monthly_biofuels
SELECT period_month AS month_date, 'B' AS target_col, value
FROM bronze.eia_monthly_biofuels
WHERE series_id = 'M_EPOOXE_YOP_NUS_1';
