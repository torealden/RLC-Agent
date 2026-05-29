-- Migration 121: Peanut stocks reference rows for crush_attribute_reference
--
-- The peanut_crush tab in us_oilseed_crush.xlsm had four empty columns
-- (M/N/O/S) for stocks data from NASS Peanut Stocks report pages 2-3 and
-- page 5. Page 5 (Cake & meal stocks) already had a reference row but no
-- data because the collector's freq_desc='MONTHLY' filter excluded
-- stocks (NASS reports peanut stocks as freq_desc='POINT IN TIME' with
-- reference_period_desc='END OF [MONTH]'). The collector now handles
-- both shapes (see commit pairing nass_processing_collector.py changes).
--
-- This migration adds the three missing reference rows so the updater
-- can match them and the gold view surfaces the values.
--
-- Column P (Peanut food use) is intentionally NOT added per Tore's
-- 'no fill' marker in row 1 — that's a downstream rollup he'll spec
-- separately.
--
-- Unit conversion: NASS reports LB. Spreadsheet displays "thousand pounds"
-- in M/N/O, so conversion_factor = 0.001.

BEGIN;

INSERT INTO silver.crush_attribute_reference (
    commodity, attribute_code, display_name,
    source_unit, display_unit, conversion_factor,
    spreadsheet_column, is_formula,
    nass_commodity_desc, nass_class_desc,
    nass_statisticcat_desc, nass_short_desc_filter,
    nass_domaincat_filter, is_active, header_pattern
) VALUES
    -- Col M: Total shelled peanuts (pg 2/3) — shelled stocks
    ('peanut', 'shelled_stocks_total', 'Total shelled peanuts',
     'LB', 'thousand pounds', 0.001,
     13, FALSE,
     'PEANUTS', NULL,
     'STOCKS', 'PEANUTS, SHELLED - STOCKS, MEASURED IN LB',
     NULL, TRUE,
     'Total shelled peanuts'),

    -- Col N: Farmer stock (pg 2) — total stocks expressed in in-shell basis
    -- (this is NASS's standard farmer-stock-equivalent total). Choosing the
    -- IN SHELL BASIS aggregate over PEANUTS, IN SHELL - STOCKS because the
    -- spreadsheet header "Farmer stock" conventionally refers to the
    -- shelled-included in-shell-equivalent figure on NASS pg 2.
    ('peanut', 'farmer_stock_total', 'Farmer stock',
     'LB', 'thousand pounds', 0.001,
     14, FALSE,
     'PEANUTS', NULL,
     'STOCKS', 'PEANUTS - STOCKS, MEASURED IN LB, IN SHELL BASIS',
     NULL, TRUE,
     'Farmer stock'),

    -- Col O: Roasting stock (in shell) (pg 2)
    ('peanut', 'roasting_stock_in_shell_stocks', 'Roasting stock (in shell)',
     'LB', 'thousand pounds', 0.001,
     15, FALSE,
     'PEANUTS', NULL,
     'STOCKS', 'PEANUTS, IN SHELL, ROASTING - STOCKS, MEASURED IN LB',
     NULL, TRUE,
     'Roasting stock (in shell)')
ON CONFLICT DO NOTHING;

COMMIT;

-- Verification:
-- SELECT spreadsheet_column, header_pattern, nass_short_desc_filter, conversion_factor
-- FROM silver.crush_attribute_reference
-- WHERE commodity = 'peanut' AND spreadsheet_column IN (13, 14, 15, 19)
-- ORDER BY spreadsheet_column;
