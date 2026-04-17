-- =============================================================================
-- Migration 027: Peanut crush header_pattern + unit alignment
-- =============================================================================
-- Peanut data now writes to the dedicated peanut_crush tab (not NASS Other Veg
-- Oils). The tab's row 3 headers use long-form NASS labels (e.g. "Once refined
-- oil produced (Fats and Oils)") and the row 4 units are "(thousand pounds)".
--
-- The 4 peanut rows in silver.crush_attribute_reference had short-form
-- header_patterns (e.g. "Refined Oil Production") and display_unit=mil lbs,
-- so FatsOilsUpdaterSQL could not match columns and values would have been
-- off by 1000x. This migration aligns both fields.
--
-- Column mapping on peanut_crush:
--   col V -> crude_oil_refined              (Crude oil processed in refining)
--   col W -> refined_oil_production         (Once refined oil produced)
--   col X -> refined_oil_further_processing (Once refined oil removed for use)
--   col Y -> crude_oil_stocks               (Crude oil on hand end of month)
-- =============================================================================

UPDATE silver.crush_attribute_reference
SET header_pattern = 'Crude oil processed in refining (Fats and Oils)',
    display_unit   = 'thousand pounds',
    conversion_factor = 0.001
WHERE commodity = 'peanut' AND attribute_code = 'crude_oil_refined';

UPDATE silver.crush_attribute_reference
SET header_pattern = 'Once refined oil produced (Fats and Oils)',
    display_unit   = 'thousand pounds',
    conversion_factor = 0.001
WHERE commodity = 'peanut' AND attribute_code = 'refined_oil_production';

UPDATE silver.crush_attribute_reference
SET header_pattern = 'Once refined oil removed for use in processing (Fats and Oils)',
    display_unit   = 'thousand pounds',
    conversion_factor = 0.001
WHERE commodity = 'peanut' AND attribute_code = 'refined_oil_further_processing';

UPDATE silver.crush_attribute_reference
SET header_pattern = 'Crude oil on hand end of month (Fats and Oils)',
    display_unit   = 'thousand pounds',
    conversion_factor = 0.001
WHERE commodity = 'peanut' AND attribute_code = 'crude_oil_stocks';
