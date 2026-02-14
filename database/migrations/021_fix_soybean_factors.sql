-- ============================================================================
-- Migration: 021_fix_soybean_factors.sql
-- Date: 2026-02-10
-- Description: Fix soybean conversion factors for gold.trade_export_mapped
--
-- Issue: All soybean codes had factor 3.6744E-8 (designed for a * 1000
--        multiplier that was never deployed in the view). The actual view
--        formula is: quantity * factor = result_in_display_unit.
--
-- Additionally, HS 1201900095 (bulk soybean exports) reports quantity in
-- METRIC TONS from the Census API, not KG like other codes.
--
-- Correct factors:
--   1201900095 EXPORTS (MT): 0.0367437 (MT * 36.7437 bu/MT / 1000)
--   All other SOYBEANS (KG): 0.0000367437 (KG / 27.2155 kg/bu / 1000)
-- ============================================================================

-- Fix the primary bulk export code (reports in METRIC TONS)
UPDATE silver.trade_commodity_reference SET
    conversion_factor = 0.0367437,
    source_unit = 'MT',
    notes = 'MT to 000 Bushels (36.7437 bu/MT / 1000)'
WHERE commodity_group = 'SOYBEANS'
  AND hs_code_10 = '1201900095'
  AND flow_type = 'EXPORTS';

-- Fix all KG-based soybean codes
UPDATE silver.trade_commodity_reference SET
    conversion_factor = 0.0000367437,
    source_unit = 'KG',
    notes = 'KG to 000 Bushels (1/27215.5/1000)'
WHERE commodity_group = 'SOYBEANS'
  AND NOT (hs_code_10 = '1201900095' AND flow_type = 'EXPORTS');
