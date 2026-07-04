-- 140_feedstock_code_registry.sql
-- Canonical feedstock vocabulary for the BBD feedstock allocation system (design v1.5 §5.1).
-- Two tables:
--   reference.feedstock_codes      — the canonical registry: ONE code per feedstock, the full
--                                    BBD-feedstock universe (active + reserved), so the vocab
--                                    problem is solved once. Reserved codes are assigned but not
--                                    tracked; they exist so a future feedstock never forces a
--                                    collision or a re-vocabulary.
--   reference.feedstock_code_xref  — migration crosswalk: every SOURCE code (mix tables,
--                                    feedstock_properties, feedstock_conversion_rates, EIA lines)
--                                    -> canonical code + disposition. Migration, never runtime
--                                    translation. Ambiguous rows -> disposition='needs_ruling'.
-- NEEDS_RULING items flagged for Tore: LCI (retire vs distinct CI bucket), the EIA aggregate
-- lines (Animal Fats / Vegetable Oils / Recycled Feeds), and conv_rates 'uco_yellow_grease' (H2).

BEGIN;

CREATE TABLE IF NOT EXISTS reference.feedstock_codes (
    code          text PRIMARY KEY,
    name          text NOT NULL,
    category      text NOT NULL,          -- waste_fat | edible_veg | cover_crop | advanced | control
    status        text NOT NULL DEFAULT 'reserved',  -- active | reserved | deprecated | retired
    eia_group     text,                   -- the EIA 'total' feedstock_name this rolls into, if any
    notes         text
);

CREATE TABLE IF NOT EXISTS reference.feedstock_code_xref (
    source_vocab   text NOT NULL,         -- mix | allocator | conv_rates | eia
    source_code    text NOT NULL,
    canonical_code text REFERENCES reference.feedstock_codes(code),
    disposition    text NOT NULL,         -- map | rollup | retire | needs_ruling
    ruled_by       text,
    ruled_at       date,
    note           text,
    PRIMARY KEY (source_vocab, source_code)
);

-- ---------------------------------------------------------------------------
-- REGISTRY  (code, name, category, status, eia_group, notes)
-- ---------------------------------------------------------------------------
INSERT INTO reference.feedstock_codes (code,name,category,status,eia_group,notes) VALUES
-- Waste fats & recycled oils
 ('UCO','Used Cooking Oil','waste_fat','active','Yellow Grease','EIA lumps into Yellow Grease'),
 ('YG','Yellow Grease','waste_fat','active','Yellow Grease','rendered/recycled grease; boundary w/ UCO (H2)'),
 ('BFT','Beef Tallow (combined)','waste_fat','active','Tallow','EBFT/IBFT are silver-internal subcodes; roll up to BFT at gold'),
 ('EBFT','Edible Beef Tallow','waste_fat','active','Tallow','silver-internal only; rolls to BFT'),
 ('IBFT','Inedible Beef Tallow','waste_fat','active','Tallow','silver-internal only; rolls to BFT'),
 ('MTL','Mutton Tallow','waste_fat','reserved','Tallow',NULL),
 ('CWG','Choice White Grease (pork)','waste_fat','active','White Grease',NULL),
 ('LRD','Lard','waste_fat','reserved','White Grease',NULL),
 ('PLT','Poultry Fat','waste_fat','active','Poultry','allocator PF migrates here'),
 ('FSH','Fish Oil','waste_fat','reserved',NULL,'e.g. catfish oil (Hero BX Moundville region)'),
 ('BRG','Brown / Trap Grease','waste_fat','reserved',NULL,NULL),
 ('ACO','Acid Oil (soapstock)','waste_fat','reserved',NULL,NULL),
 ('TLO','Tall Oil / Black Liquor Soap','waste_fat','reserved',NULL,NULL),
-- Edible vegetable oils
 ('SBO','Soybean Oil','edible_veg','active','Soybean Oil',NULL),
 ('CAN','Canola / Rapeseed Oil','edible_veg','active','Canola Oil','allocator CO migrates here (documented canola)'),
 ('DCO','Distillers Corn Oil','edible_veg','active','Corn Oil','the only corn oil in the fuel complex; crude corn oil (CO) retired'),
 ('PMO','Crude Palm Oil','edible_veg','reserved','Palm Oil','allocator PALM migrates here'),
 ('PML','Palm Olein','edible_veg','reserved','Palm Oil',NULL),
 ('PMS','Palm Stearin','edible_veg','reserved','Palm Oil',NULL),
 ('SFO','Sunflower Seed Oil','edible_veg','reserved',NULL,NULL),
 ('SFF','Safflower Oil','edible_veg','reserved',NULL,'code SFF (SAF reserved for aviation fuel)'),
 ('PNO','Peanut Oil','edible_veg','reserved',NULL,NULL),
 ('CNO','Coconut Oil','edible_veg','reserved',NULL,NULL),
 ('OLO','Olive Oil','edible_veg','reserved',NULL,NULL),
 ('CSO','Cottonseed Oil','edible_veg','reserved','Cottonseed Oil','legacy in feedstock_properties/allocator'),
-- Non-edible & cover-crop oils
 ('CAM','Camelina Sativa Oil','cover_crop','active',NULL,'Montana Renewables, Bakersfield'),
 ('CAR','Carinata (Brassica) Oil','cover_crop','active',NULL,'BP Cherry Point pathway'),
 ('JAT','Jatropha Curcas Oil','cover_crop','reserved',NULL,NULL),
 ('CST','Castor Oil','cover_crop','reserved',NULL,NULL),
 ('PON','Pongamia / Karanja Oil','cover_crop','reserved',NULL,NULL),
 ('PNC','Pennycress Oil','cover_crop','reserved',NULL,NULL),
 ('NEM','Neem Oil','cover_crop','reserved',NULL,NULL),
 ('RBS','Rubber Seed Oil','cover_crop','reserved',NULL,NULL),
-- Advanced & cellulosic (mostly non-lipid; coded for completeness, not BBD-lipid pathways)
 ('ALG','Microalgae Lipid Oil','advanced','reserved',NULL,NULL),
 ('MAG','Macroalgae / Seaweed Lipid','advanced','reserved',NULL,NULL),
 ('CYN','Cyanobacteria Lipid','advanced','reserved',NULL,NULL),
 ('STV','Corn Stover','advanced','reserved',NULL,'cellulosic'),
 ('WST','Wheat Straw','advanced','reserved',NULL,'cellulosic'),
 ('RST','Rice Straw','advanced','reserved',NULL,'cellulosic'),
 ('BAG','Bagasse','advanced','reserved',NULL,'cellulosic'),
 ('MSC','Miscanthus','advanced','reserved',NULL,'energy grass'),
 ('SWG','Switchgrass','advanced','reserved',NULL,'energy grass'),
 ('WDP','Wood Pellets','advanced','reserved',NULL,NULL),
 ('FSL','Forestry Slash','advanced','reserved',NULL,NULL),
 ('SWD','Sawdust','advanced','reserved',NULL,NULL),
 ('MLR','Mill Residues','advanced','reserved',NULL,NULL),
 ('MSW','Municipal Solid Waste (biogenic)','advanced','reserved',NULL,NULL),
-- Control codes
 ('OTH','Other / Unspecified','control','active',NULL,'explicit catch-all; PALM/CSO/SFO/LARD roll here until volumes justify promotion'),
 ('LCI','Low-CI (unspecified) placeholder','control','retired',NULL,'RETIRED (Tore 2026-07-04): placeholder on 2 HF Sinclair RD plants, backfilled to BFT/UCO/DCO. Never reused.'),
 ('CO','Crude Corn Oil (RETIRED)','control','retired',NULL,'POISONED: meant Corn Oil in mix, Canola in allocator. Never reused. No crude corn oil is a fuel feedstock.')
ON CONFLICT (code) DO UPDATE SET name=EXCLUDED.name, category=EXCLUDED.category, status=EXCLUDED.status, eia_group=EXCLUDED.eia_group, notes=EXCLUDED.notes;

-- ---------------------------------------------------------------------------
-- XREF  (source_vocab, source_code -> canonical, disposition)
-- ---------------------------------------------------------------------------
INSERT INTO reference.feedstock_code_xref (source_vocab,source_code,canonical_code,disposition,ruled_by,ruled_at,note) VALUES
-- mix (facility_assumed_mix)
 ('mix','SBO','SBO','map','desktop','2026-07-04',NULL),
 ('mix','CAN','CAN','map','desktop','2026-07-04',NULL),
 ('mix','CO','DCO','map','code','2026-07-04','sole CO row = Hero BX Moundville; BD plant "corn oil" = distillers corn oil'),
 ('mix','DCO','DCO','map','desktop','2026-07-04',NULL),
 ('mix','BFT','BFT','map','desktop','2026-07-04',NULL),
 ('mix','CWG','CWG','map','desktop','2026-07-04',NULL),
 ('mix','YG','YG','map','desktop','2026-07-04',NULL),
 ('mix','PLT','PLT','map','desktop','2026-07-04',NULL),
 ('mix','UCO','UCO','map','desktop','2026-07-04',NULL),
 ('mix','CAM','CAM','map','desktop','2026-07-04',NULL),
 ('mix','CAR','CAR','map','desktop','2026-07-04',NULL),
 ('mix','LCI','LCI','retire','tore','2026-07-04','retired+backfilled to BFT12/UCO8/DCO5 on HF Sinclair Artesia+Cheyenne'),
-- allocator (feedstock_properties + gold.feedstock_allocation)
 ('allocator','SBO','SBO','map','desktop','2026-07-04',NULL),
 ('allocator','CO','CAN','map','code','2026-07-04','feedstock_properties documents CO=Canola; 4256 rows'),
 ('allocator','DCO','DCO','map','desktop','2026-07-04',NULL),
 ('allocator','EBFT','BFT','rollup','desktop','2026-07-04','edible tallow rolls to combined BFT at gold'),
 ('allocator','IBFT','BFT','rollup','desktop','2026-07-04','inedible tallow rolls to combined BFT at gold'),
 ('allocator','BFT','BFT','map','desktop','2026-07-04','legacy combined rows kept untouched (no back-apportionment)'),
 ('allocator','CWG','CWG','map','desktop','2026-07-04',NULL),
 ('allocator','PF','PLT','map','desktop','2026-07-04','poultry code standardized to PLT'),
 ('allocator','YG','YG','map','desktop','2026-07-04',NULL),
 ('allocator','UCO','UCO','map','desktop','2026-07-04',NULL),
 ('allocator','PALM','PMO','map','code','2026-07-04',NULL),
 ('allocator','CSO','CSO','map','code','2026-07-04',NULL),
 ('allocator','SFO','SFO','map','code','2026-07-04',NULL),
 ('allocator','LARD','LRD','map','code','2026-07-04',NULL),
 ('allocator','OTHER','OTH','map','code','2026-07-04',NULL),
-- conv_rates (feedstock_conversion_rates, long names)
 ('conv_rates','soybean_oil','SBO','map','code','2026-07-04',NULL),
 ('conv_rates','canola_oil','CAN','map','code','2026-07-04',NULL),
 ('conv_rates','corn_oil','DCO','map','code','2026-07-04','generic corn oil in a fuel table = distillers'),
 ('conv_rates','distillers_corn_oil','DCO','map','code','2026-07-04',NULL),
 ('conv_rates','used_cooking_oil','UCO','map','code','2026-07-04',NULL),
 ('conv_rates','yellow_grease','YG','map','code','2026-07-04',NULL),
 ('conv_rates','uco_yellow_grease','YG','needs_ruling',NULL,NULL,'H2: merged UCO+YG name. Split or keep merged? — Tore/Desktop'),
 ('conv_rates','white_grease','CWG','map','code','2026-07-04',NULL),
 ('conv_rates','choice_white_grease','CWG','map','code','2026-07-04',NULL),
 ('conv_rates','poultry_fat','PLT','map','code','2026-07-04',NULL),
 ('conv_rates','tallow','BFT','map','code','2026-07-04',NULL),
 ('conv_rates','lard','LRD','map','code','2026-07-04',NULL),
 ('conv_rates','cottonseed_oil','CSO','map','code','2026-07-04',NULL),
 ('conv_rates','palm_oil','PMO','map','code','2026-07-04',NULL),
 ('conv_rates','sunflower_oil','SFO','map','code','2026-07-04',NULL),
 ('conv_rates','other_grease','OTH','map','code','2026-07-04',NULL),
-- eia (bronze.eia_feedstock_monthly feedstock_name -> canonical)
 ('eia','Soybean Oil','SBO','map','code','2026-07-04',NULL),
 ('eia','Canola Oil','CAN','map','code','2026-07-04',NULL),
 ('eia','Corn Oil','DCO','map','code','2026-07-04',NULL),
 ('eia','Tallow','BFT','map','code','2026-07-04',NULL),
 ('eia','White Grease','CWG','map','code','2026-07-04',NULL),
 ('eia','Yellow Grease','YG','map','code','2026-07-04','includes UCO in EIA grouping'),
 ('eia','Poultry','PLT','map','code','2026-07-04',NULL),
 ('eia','Palm Oil','PMO','map','code','2026-07-04',NULL),
 ('eia','Cottonseed Oil','CSO','map','code','2026-07-04',NULL),
 ('eia','Algae Oil','ALG','map','code','2026-07-04',NULL),
 ('eia','Other Grease','OTH','map','code','2026-07-04',NULL),
 ('eia','Other','OTH','map','code','2026-07-04',NULL),
 ('eia','Recycled Feeds','OTH','needs_ruling',NULL,NULL,'EIA aggregate line — verify composition before folding to OTH'),
 ('eia','Animal Fats','BFT','needs_ruling',NULL,NULL,'EIA aggregate (pre-2014) — splits across BFT/CWG/PLT; do not map naively'),
 ('eia','Vegetable Oils','SBO','needs_ruling',NULL,NULL,'EIA aggregate (pre-2014) — splits across veg oils; do not map naively'),
 ('eia','Alcohol','OTH','needs_ruling',NULL,NULL,'non-feedstock (methanol reagent) — likely exclude, not a lipid'),
 ('eia','Catalysts','OTH','needs_ruling',NULL,NULL,'non-feedstock — exclude'),
-- EIA 'total' plant_type non-BBD lines (ethanol grain / cellulosic / RNG) — out of scope for BBD,
-- accounted for so the crosswalk is exhaustive. These belong to the broader all-biofuels total.
 ('eia','Municipal Solid Waste','MSW','map','code','2026-07-04','biogenic fraction'),
 ('eia','Yard Food Waste','MSW','rollup','code','2026-07-04','biogenic waste -> MSW'),
 ('eia','Other Vegetable Oil','OTH','map','code','2026-07-04','veg-oil aggregate'),
 ('eia','Other Recycled','OTH','map','code','2026-07-04',NULL),
 ('eia','Other Waste','OTH','map','code','2026-07-04',NULL),
 ('eia','Other NESOI','OTH','map','code','2026-07-04',NULL),
 ('eia','Corn','OTH','map','code','2026-07-04','non-BBD (ethanol grain); out of scope'),
 ('eia','Grain Sorghum','OTH','map','code','2026-07-04','non-BBD (ethanol grain); out of scope'),
 ('eia','Ag Forestry Residues','OTH','map','code','2026-07-04','non-BBD (cellulosic); out of scope'),
 ('eia','Energy Crops','OTH','map','code','2026-07-04','non-BBD (cellulosic); out of scope'),
 ('eia','Other Ag','OTH','map','code','2026-07-04','non-BBD; out of scope'),
 ('eia','Biogas','OTH','map','code','2026-07-04','non-BBD (RNG); out of scope')
ON CONFLICT (source_vocab,source_code) DO UPDATE SET canonical_code=EXCLUDED.canonical_code, disposition=EXCLUDED.disposition, note=EXCLUDED.note;

COMMIT;
