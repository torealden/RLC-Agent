-- Migration 137: gold.corn_grind_monthly
-- =============================================================================
-- Long-format view feeding the us_grain_crush 'corn_grind' tab. One row per
-- (year, month, target spreadsheet column). The VBA loader finds the sheet row
-- by date (col A) and writes display_value into target_col.
--
-- Sources (all in bronze.nass_processing):
--   GCCP PDF (source='NASS_GCCP')  -> cols C-H (corn consumed, 1000 bu -> mil bu)
--                                     cols J-T (co-products, tons -> 000 st)
--   Fats & Oils (source='NASS_FATS_OILS', class_desc='CORN') -> cols U-AA
--                                     (corn oil, lb -> mil lb)
-- Cols B (total), I (blank), S, X, AB, AC are in-sheet computed -- not written.
-- =============================================================================

CREATE OR REPLACE VIEW gold.corn_grind_monthly AS
WITH gccp_map(commodity_desc, target_col, label, display_unit, factor) AS (VALUES
    ('CORN FOR BEVERAGE ALCOHOL',        'C', 'Beverage Alcohol Production',          'million bushels',     0.001),
    ('CORN FOR FUEL ALCOHOL',            'D', 'Fuel alcohol',                          'million bushels',     0.001),
    ('CORN FUEL ALCOHOL DRY MILL',       'E', 'Dry mill',                              'million bushels',     0.001),
    ('CORN FUEL ALCOHOL WET MILL',       'F', 'Wet mill',                              'million bushels',     0.001),
    ('CORN FOR INDUSTRIAL ALCOHOL',      'G', 'Industrial alcohol',                    'million bushels',     0.001),
    ('CORN WET MILL OTHER THAN FUEL',    'H', 'Wet mill products other than fuel',     'million bushels',     0.001),
    ('CONDENSED DISTILLERS SOLUBLES',    'J', 'Condensed distillers solubles (CDS)',   'thousand short tons', 0.001),
    ('DISTILLERS CORN OIL',              'K', 'Corn oil (DCO)',                        'thousand short tons', 0.001),
    ('DISTILLERS DRIED GRAINS',          'L', 'Distillers dried grains (DDG)',         'thousand short tons', 0.001),
    ('DISTILLERS DRIED GRAINS W SOLUBLES','M','Distillers dried grains w/ solubles',   'thousand short tons', 0.001),
    ('DISTILLERS WET GRAINS',            'N', 'Distillers wet grains',                 'thousand short tons', 0.001),
    ('MODIFIED DISTILLERS WET GRAINS',   'O', 'Modified distillers wet grains',        'thousand short tons', 0.001),
    ('CORN GERM MEAL',                   'P', 'Corn germ meal',                        'thousand short tons', 0.001),
    ('CORN GLUTEN FEED',                 'Q', 'Corn gluten feed',                      'thousand short tons', 0.001),
    ('CORN GLUTEN MEAL',                 'R', 'Corn gluten meal',                      'thousand short tons', 0.001),
    ('WET CORN GLUTEN FEED',             'T', 'Wet corn gluten feed',                  'thousand short tons', 0.001)
),
oil_map(short_desc, target_col, label, display_unit, factor) AS (VALUES
    ('OIL, CORN - PRODUCTION, MEASURED IN LB',                                          'U', 'Corn Oil Production',          'million pounds', 0.000001),
    ('OIL, CORN, CRUDE, PROCESSED IN REFINING - REMOVAL FOR PROCESSING, MEASURED IN LB','V', 'Crude Corn Oil Processed',     'million pounds', 0.000001),
    ('OIL, CORN, ONCE REFINED - PRODUCTION, MEASURED IN LB',                            'W', 'Refined Oil Produced',         'million pounds', 0.000001),
    ('OIL, CORN, ONCE REFINED - REMOVAL FOR PROCESSING, MEASURED IN LB',                'Y', 'Refined Oil Used in Production','million pounds', 0.000001),
    ('OIL, CORN, CRUDE - STOCKS, MEASURED IN LB',                                       'Z', 'Crude Corn Oil Stocks',        'million pounds', 0.000001),
    ('OIL, CORN, ONSITE & OFFSITE, ONCE REFINED - STOCKS, MEASURED IN LB',              'AA','Refined Oil Stocks',           'million pounds', 0.000001)
)
SELECT b.year, b.month, make_date(b.year, b.month, 1) AS month_date,
       m.target_col, m.label, m.display_unit,
       ROUND((b.value * m.factor)::numeric, 4) AS display_value
FROM bronze.nass_processing b
JOIN gccp_map m ON b.source = 'NASS_GCCP' AND b.commodity_desc = m.commodity_desc
WHERE b.month IS NOT NULL
UNION ALL
SELECT b.year, b.month, make_date(b.year, b.month, 1) AS month_date,
       o.target_col, o.label, o.display_unit,
       ROUND((b.value * o.factor)::numeric, 4) AS display_value
FROM bronze.nass_processing b
JOIN oil_map o ON b.source = 'NASS_FATS_OILS' AND b.class_desc = 'CORN'
              AND b.short_desc = o.short_desc
WHERE b.month IS NOT NULL;
