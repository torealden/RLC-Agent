-- Migration 136: Fix WHEAT and BARLEY import HS codes (HTSUS, not Schedule B)
-- =============================================================================
-- Same systemic bug as mig 135 (corn): IMPORTS rows used Schedule B (export)
-- codes that return nothing on the Census import endpoint, so wheat/barley
-- import lines were near-zero. See reference_census_import_export_hs_codes note.
--
-- Real HTSUS import codes from Census imports HS10 enumeration (2024). All KG.
-- WHEAT  -> KG to 000 Bushels (60 lb/bu), factor 0.000036743700
-- BARLEY -> KG to 000 Bushels (48 lb/bu), factor 0.000045929630
-- =============================================================================

-- 1. Deactivate dead Schedule-B import rows.
UPDATE silver.trade_commodity_reference
SET is_active = false,
    notes = COALESCE(notes,'') || ' [mig136: deactivated — Schedule B code, not valid HTSUS import]'
WHERE flow_type = 'IMPORTS'
  AND ((commodity_group='WHEAT'  AND hs_code_10 IN ('1001190000','1001992015','1001992055'))
    OR (commodity_group='BARLEY' AND hs_code_10 IN ('1003900000')));

-- 2. Add correct HTSUS wheat import codes (durum 100119, other 100199).
INSERT INTO silver.trade_commodity_reference
    (hs_code_10, hs_code_6, commodity_group, commodity_name, flow_type,
     source_unit, display_unit, conversion_factor, is_active, notes,
     price_unit_label, price_unit_factor)
VALUES
    ('1001190051','100119','WHEAT','Durum wheat, grade 1, except seed','IMPORTS','KG','000 Bushels',0.000036743700,true,'HTSUS import (mig136)','usd_per_bushel',27.2155422),
    ('1001190053','100119','WHEAT','Durum wheat, grade 2, except seed','IMPORTS','KG','000 Bushels',0.000036743700,true,'HTSUS import (mig136)','usd_per_bushel',27.2155422),
    ('1001190069','100119','WHEAT','Durum wheat, except seed, NESOI','IMPORTS','KG','000 Bushels',0.000036743700,true,'HTSUS import (mig136)','usd_per_bushel',27.2155422),
    ('1001190025','100119','WHEAT','Durum wheat, certified organic, except seed','IMPORTS','KG','000 Bushels',0.000036743700,true,'HTSUS import (mig136)','usd_per_bushel',27.2155422),
    ('1001990097','100199','WHEAT','Wheat or meslin, except seed, NESOI','IMPORTS','KG','000 Bushels',0.000036743700,true,'HTSUS import, dominant wheat import line (mig136)','usd_per_bushel',27.2155422),
    ('1001990033','100199','WHEAT','Red spring wheat, grade 2, specified protein','IMPORTS','KG','000 Bushels',0.000036743700,true,'HTSUS import (mig136)','usd_per_bushel',27.2155422),
    ('1001990018','100199','WHEAT','Red spring wheat, grade 1, specified protein','IMPORTS','KG','000 Bushels',0.000036743700,true,'HTSUS import (mig136)','usd_per_bushel',27.2155422),
    ('1001990055','100199','WHEAT','Canadian Western red winter wheat, except seed','IMPORTS','KG','000 Bushels',0.000036743700,true,'HTSUS import (mig136)','usd_per_bushel',27.2155422),
    ('1001990036','100199','WHEAT','Red spring wheat, except seed, NESOI','IMPORTS','KG','000 Bushels',0.000036743700,true,'HTSUS import (mig136)','usd_per_bushel',27.2155422),
    ('1001990005','100199','WHEAT','Canadian Western extra strong hard red spring','IMPORTS','KG','000 Bushels',0.000036743700,true,'HTSUS import (mig136)','usd_per_bushel',27.2155422),
    ('1001990017','100199','WHEAT','Red spring wheat, grade 1, specified protein','IMPORTS','KG','000 Bushels',0.000036743700,true,'HTSUS import (mig136)','usd_per_bushel',27.2155422),
    ('1001990007','100199','WHEAT','Red spring wheat, certified organic','IMPORTS','KG','000 Bushels',0.000036743700,true,'HTSUS import (mig136)','usd_per_bushel',27.2155422),
    ('1001990010','100199','WHEAT','Red spring wheat, grade 1, specified protein','IMPORTS','KG','000 Bushels',0.000036743700,true,'HTSUS import (mig136)','usd_per_bushel',27.2155422),
    ('1001990009','100199','WHEAT','Wheat or meslin, certified organic, except seed','IMPORTS','KG','000 Bushels',0.000036743700,true,'HTSUS import (mig136)','usd_per_bushel',27.2155422),
    ('1001990030','100199','WHEAT','Red spring wheat, grade 2, specified protein','IMPORTS','KG','000 Bushels',0.000036743700,true,'HTSUS import (mig136)','usd_per_bushel',27.2155422),
    ('1001990065','100199','WHEAT','Soft white spring wheat, except seed, NESOI','IMPORTS','KG','000 Bushels',0.000036743700,true,'HTSUS import (mig136)','usd_per_bushel',27.2155422),
    ('1001990045','100199','WHEAT','White winter wheat, except seed, NESOI','IMPORTS','KG','000 Bushels',0.000036743700,true,'HTSUS import (mig136)','usd_per_bushel',27.2155422),
    ('1001990027','100199','WHEAT','Red spring wheat, grade 2, specified protein','IMPORTS','KG','000 Bushels',0.000036743700,true,'HTSUS import (mig136)','usd_per_bushel',27.2155422);

-- 3. Add correct HTSUS barley import codes (100390).
INSERT INTO silver.trade_commodity_reference
    (hs_code_10, hs_code_6, commodity_group, commodity_name, flow_type,
     source_unit, display_unit, conversion_factor, is_active, notes,
     price_unit_label, price_unit_factor)
VALUES
    ('1003902000','100390','BARLEY','Barley for malting purposes','IMPORTS','KG','000 Bushels',0.000045929630,true,'HTSUS import (mig136)','usd_per_bushel',21.7724376),
    ('1003904030','100390','BARLEY','Barley, NESOI','IMPORTS','KG','000 Bushels',0.000045929630,true,'HTSUS import (mig136)','usd_per_bushel',21.7724376),
    ('1003904020','100390','BARLEY','Barley, certified organic','IMPORTS','KG','000 Bushels',0.000045929630,true,'HTSUS import (mig136)','usd_per_bushel',21.7724376);
