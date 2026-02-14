-- ============================================================================
-- Migration 007: Fix Canola Seed Import HS Codes
-- ============================================================================
-- Issue: Export codes (Schedule B) and Import codes (HTS) are different for
-- canola/rapeseed. We were using export codes (1205100000, 1205900000) for
-- imports, but the actual import HTS codes are more granular.
--
-- Export codes (Schedule B):
--   1205100000 - Rapeseed/colza, low erucic acid
--   1205900000 - Rapeseed/colza, other
--
-- Import codes (HTS):
--   1205100010 - Rapeseed/colza, low erucic acid, for sowing
--   1205100020 - Rapeseed/colza, low erucic acid, for oil (main volume)
--   1205100090 - Rapeseed/colza, low erucic acid, other
--   1205900010 - Rapeseed/colza other, for sowing
--   1205900020 - Rapeseed/colza other, for oil
--   1205900090 - Rapeseed/colza other, NESOI
-- ============================================================================

-- Remove incorrect import mappings (using export codes)
DELETE FROM silver.trade_commodity_reference
WHERE hs_code_10 IN ('1205100000', '1205900000')
  AND flow_type = 'IMPORTS';

-- Add correct import HTS codes for canola seed
INSERT INTO silver.trade_commodity_reference
    (hs_code_10, hs_code_6, commodity_group, commodity_name, flow_type,
     source_unit, display_unit, conversion_factor, is_active, notes)
VALUES
    -- Low erucic acid rapeseed/canola (1205.10.xx)
    ('1205100010', '120510', 'CANOLA', 'Canola seed - for sowing', 'IMPORTS',
     'KG', 'Thousand Lbs', 0.000002204622, TRUE,
     'HTS import code for canola seed (low erucic acid) for sowing'),

    ('1205100020', '120510', 'CANOLA', 'Canola seed - for oil', 'IMPORTS',
     'KG', 'Thousand Lbs', 0.000002204622, TRUE,
     'HTS import code for canola seed (low erucic acid) for oil - PRIMARY IMPORT CODE'),

    ('1205100090', '120510', 'CANOLA', 'Canola seed - other', 'IMPORTS',
     'KG', 'Thousand Lbs', 0.000002204622, TRUE,
     'HTS import code for canola seed (low erucic acid) NESOI'),

    -- Other rapeseed/canola (1205.90.xx)
    ('1205900010', '120590', 'CANOLA', 'Rapeseed other - for sowing', 'IMPORTS',
     'KG', 'Thousand Lbs', 0.000002204622, TRUE,
     'HTS import code for other rapeseed for sowing'),

    ('1205900020', '120590', 'CANOLA', 'Rapeseed other - for oil', 'IMPORTS',
     'KG', 'Thousand Lbs', 0.000002204622, TRUE,
     'HTS import code for other rapeseed for oil'),

    ('1205900090', '120590', 'CANOLA', 'Rapeseed other - NESOI', 'IMPORTS',
     'KG', 'Thousand Lbs', 0.000002204622, TRUE,
     'HTS import code for other rapeseed NESOI')
ON CONFLICT DO NOTHING;

-- Verify the changes
SELECT hs_code_10, flow_type, commodity_group, commodity_name
FROM silver.trade_commodity_reference
WHERE hs_code_10 LIKE '1205%'
ORDER BY flow_type, hs_code_10;
