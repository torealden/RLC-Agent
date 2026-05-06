-- Migration 057: Seed reference.facility_master with major Iowa industry facilities
--
-- Date: 2026-05-06
--
-- Why:
--   Initial population of the multi-industry facility ledger built in
--   migration 056. Seeds the highest-priority known operators per
--   industry from public knowledge. Long-tail facilities (3,000+ CAFOs,
--   500+ country elevators) come from bulk scraping of state regulator
--   databases in Sprint 6 — not in this seed.
--
--   Capacities populated where publicly known with high confidence.
--   Otherwise NULL — the air-permit extraction pipeline (Sprint 5) fills
--   these in from Title V permits.
--
--   Coordinates are approximate city centroids; the existing
--   scripts/geocode_iowa_facilities.py extends to handle facility_master
--   in a follow-up.
--
-- Industries seeded:
--   ethanol         (~22 known IA plants)
--   biodiesel       (~6 plants)
--   pork_packing    (~12 plants)
--   beef_packing    (~3 plants)
--   egg_layers      (~5 major operators; per-site detail needs DNR scrape)
--
-- See docs/specs/iowa_industry_facility_taxonomy.md for the full
-- per-industry source list, capacity data, and extraction targets.

-- =============================================================================
-- ETHANOL — 22 known IA plants
-- =============================================================================

INSERT INTO reference.facility_master
    (facility_id, name, industry_code, operator, parent_company,
     city, county, state, lat, lon, status, data_source, notes)
VALUES
    -- POET Biorefining — largest IA ethanol operator
    ('ia.poet_coon_rapids',     'POET Coon Rapids',           'ethanol', 'POET Biorefining', 'POET LLC',
     'Coon Rapids', 'Carroll', 'IA', 41.870, -94.679, 'active', 'public_knowledge', 'POET fleet'),
    ('ia.poet_ashton',          'POET Ashton',                'ethanol', 'POET Biorefining', 'POET LLC',
     'Ashton',      'Osceola',  'IA', 43.305, -95.793, 'active', 'public_knowledge', 'POET fleet'),
    ('ia.poet_emmetsburg',      'POET Emmetsburg',            'ethanol', 'POET Biorefining', 'POET LLC',
     'Emmetsburg', 'Palo Alto', 'IA', 43.111, -94.683, 'active', 'public_knowledge', 'Was cellulosic Project Liberty; now corn ethanol'),
    ('ia.poet_hanlontown',      'POET Hanlontown',            'ethanol', 'POET Biorefining', 'POET LLC',
     'Hanlontown', 'Worth',     'IA', 43.330, -93.371, 'active', 'public_knowledge', 'POET fleet'),
    ('ia.poet_jewell',          'POET Jewell',                'ethanol', 'POET Biorefining', 'POET LLC',
     'Jewell',     'Hamilton',  'IA', 42.310, -93.642, 'active', 'public_knowledge', 'POET fleet'),
    ('ia.poet_gowrie',          'POET Gowrie',                'ethanol', 'POET Biorefining', 'POET LLC',
     'Gowrie',     'Webster',   'IA', 42.281, -94.288, 'active', 'public_knowledge', 'POET fleet'),

    -- ADM (Archer-Daniels-Midland) — co-located with crush at Cedar Rapids + Clinton
    ('ia.adm_cedar_rapids_ethanol', 'ADM Cedar Rapids Ethanol', 'ethanol', 'ADM', 'Archer-Daniels-Midland',
     'Cedar Rapids', 'Linn', 'IA', 41.974, -91.665, 'active', 'public_knowledge', 'Co-located with corn wet mill + crush; large complex'),
    ('ia.adm_clinton',          'ADM Clinton',                'ethanol', 'ADM', 'Archer-Daniels-Midland',
     'Clinton', 'Clinton', 'IA', 41.844, -90.188, 'active', 'public_knowledge', 'Wet mill complex on Mississippi River'),

    -- Valero Renewable Fuels — 4 IA plants from Hawkeye/AltraBio acquisitions
    ('ia.valero_albert_city',   'Valero Albert City',         'ethanol', 'Valero Renewable Fuels', 'Valero Energy',
     'Albert City', 'Buena Vista', 'IA', 42.778, -94.949, 'active', 'public_knowledge', 'Acquired from Hawkeye'),
    ('ia.valero_charles_city',  'Valero Charles City',        'ethanol', 'Valero Renewable Fuels', 'Valero Energy',
     'Charles City', 'Floyd',  'IA', 43.066, -92.674, 'active', 'public_knowledge', 'Acquired from Hawkeye'),
    ('ia.valero_fort_dodge',    'Valero Fort Dodge',          'ethanol', 'Valero Renewable Fuels', 'Valero Energy',
     'Fort Dodge', 'Webster',  'IA', 42.504, -94.191, 'active', 'public_knowledge', 'Acquired from Hawkeye'),
    ('ia.valero_hartley',       'Valero Hartley',             'ethanol', 'Valero Renewable Fuels', 'Valero Energy',
     'Hartley', 'O''Brien',    'IA', 43.181, -95.481, 'active', 'public_knowledge', 'Acquired from Hawkeye'),

    -- Green Plains
    ('ia.green_plains_shenandoah', 'Green Plains Shenandoah', 'ethanol', 'Green Plains', 'Green Plains Inc',
     'Shenandoah', 'Page',     'IA', 40.766, -95.372, 'active', 'public_knowledge', NULL),
    ('ia.green_plains_superior',   'Green Plains Superior',   'ethanol', 'Green Plains', 'Green Plains Inc',
     'Superior',   'Dickinson','IA', 43.434, -95.000, 'active', 'public_knowledge', NULL),

    -- Cargill (Eddyville exists in oilseed_crush; ethanol production also there)
    ('ia.cargill_eddyville_ethanol', 'Cargill Eddyville Ethanol', 'ethanol', 'Cargill', 'Cargill',
     'Eddyville', 'Wapello',   'IA', 41.140, -92.648, 'active', 'public_knowledge', 'Co-located with crush + biorefining at the Eddyville biocomplex'),

    -- Big River Resources
    ('ia.big_river_west_burlington', 'Big River Resources West Burlington', 'ethanol', 'Big River Resources', NULL,
     'West Burlington', 'Des Moines', 'IA', 40.825, -91.149, 'active', 'public_knowledge', 'Coop-owned'),

    -- Lincolnway Energy
    ('ia.lincolnway_nevada',    'Lincolnway Energy',          'ethanol', 'Lincolnway Energy LLC', NULL,
     'Nevada', 'Story',        'IA', 42.024, -93.452, 'active', 'public_knowledge', 'Locally-owned'),

    -- Pine Lake Corn Processors
    ('ia.pine_lake_steamboat_rock', 'Pine Lake Corn Processors', 'ethanol', 'Pine Lake Corn Processors', NULL,
     'Steamboat Rock', 'Hardin', 'IA', 42.404, -93.080, 'active', 'public_knowledge', NULL),

    -- Plymouth Energy
    ('ia.plymouth_merrill',     'Plymouth Energy',            'ethanol', 'Plymouth Energy LLC', NULL,
     'Merrill', 'Plymouth',    'IA', 42.722, -96.247, 'active', 'public_knowledge', NULL),

    -- Quad County Corn Processors
    ('ia.quad_county_galva',    'Quad County Corn Processors', 'ethanol', 'Quad County Corn Processors', NULL,
     'Galva',  'Ida',          'IA', 42.506, -95.412, 'active', 'public_knowledge', 'Pioneered cellulosic ethanol bolt-on'),

    -- Western Iowa Energy (combo ethanol + biodiesel)
    ('ia.western_iowa_wall_lake', 'Western Iowa Energy',     'ethanol', 'Western Iowa Energy LLC', NULL,
     'Wall Lake', 'Sac',       'IA', 42.272, -95.092, 'active', 'public_knowledge', 'Combo plant — also biodiesel'),

    -- Absolute Energy
    ('ia.absolute_st_ansgar',   'Absolute Energy',            'ethanol', 'Absolute Energy', NULL,
     'St. Ansgar', 'Mitchell', 'IA', 43.378, -92.918, 'active', 'public_knowledge', NULL),

    -- Homeland Energy Solutions
    ('ia.homeland_lawler',      'Homeland Energy Solutions',  'ethanol', 'Homeland Energy Solutions LLC', NULL,
     'Lawler', 'Chickasaw',    'IA', 43.083, -92.146, 'active', 'public_knowledge', NULL),

    -- Iowa Renewable Energy / Iowa Ethanol
    ('ia.iowa_ethanol_hanlontown', 'Iowa Ethanol Hanlontown', 'ethanol', 'Iowa Ethanol LLC', NULL,
     'Hanlontown', 'Worth',    'IA', 43.330, -93.371, 'active', 'public_knowledge', 'Verify status — early IA ethanol plant')
ON CONFLICT (facility_id) DO UPDATE SET
    name = EXCLUDED.name,
    operator = EXCLUDED.operator,
    parent_company = EXCLUDED.parent_company,
    notes = EXCLUDED.notes,
    updated_at = NOW();

-- =============================================================================
-- BIODIESEL — 6 known IA plants
-- =============================================================================

INSERT INTO reference.facility_master
    (facility_id, name, industry_code, operator, parent_company,
     city, county, state, lat, lon, status, data_source, notes)
VALUES
    -- Chevron Renewable Energy Group (acquired REG in 2022)
    ('ia.chevron_reg_newton',   'Chevron REG Newton',         'biodiesel', 'Chevron Renewable Energy Group', 'Chevron Corp',
     'Newton', 'Jasper',       'IA', 41.696, -93.048, 'active', 'public_knowledge', 'Largest IA biodiesel plant; former REG flagship'),
    ('ia.chevron_reg_mason_city', 'Chevron REG Mason City',   'biodiesel', 'Chevron Renewable Energy Group', 'Chevron Corp',
     'Mason City', 'Cerro Gordo', 'IA', 43.153, -93.200, 'active', 'public_knowledge', 'Former REG site'),
    ('ia.chevron_reg_ralston',  'Chevron REG Ralston',        'biodiesel', 'Chevron Renewable Energy Group', 'Chevron Corp',
     'Ralston', 'Carroll',     'IA', 42.046, -94.633, 'active', 'public_knowledge', 'Former REG site; HQ before acquisition'),

    -- Western Iowa Energy (combo ethanol + biodiesel; ethanol record above)
    ('ia.western_iowa_wall_lake_biodiesel', 'Western Iowa Energy Biodiesel', 'biodiesel', 'Western Iowa Energy LLC', NULL,
     'Wall Lake', 'Sac',       'IA', 42.272, -95.092, 'active', 'public_knowledge', 'Combo plant — see ia.western_iowa_wall_lake for ethanol'),

    -- Cargill Iowa Falls (biodiesel side; oilseed crush record exists separately)
    ('ia.cargill_iowa_falls_biodiesel', 'Cargill Iowa Falls Biodiesel', 'biodiesel', 'Cargill', 'Cargill',
     'Iowa Falls', 'Hardin',   'IA', 42.521, -93.265, 'active', 'public_knowledge', 'Smaller — co-located with crush'),

    -- Stockton (smaller IA biodiesel)
    ('ia.stockton_biodiesel_tama', 'Stockton Biodiesel Tama', 'biodiesel', 'Stockton Biodiesel', NULL,
     'Tama', 'Tama',           'IA', 41.967, -92.578, 'active', 'public_knowledge', 'Smaller plant; verify status')
ON CONFLICT (facility_id) DO UPDATE SET
    name = EXCLUDED.name,
    operator = EXCLUDED.operator,
    parent_company = EXCLUDED.parent_company,
    notes = EXCLUDED.notes,
    updated_at = NOW();

-- =============================================================================
-- PORK PACKING — 12 known IA plants
-- =============================================================================

INSERT INTO reference.facility_master
    (facility_id, name, industry_code, operator, parent_company,
     city, county, state, lat, lon, status, data_source, notes)
VALUES
    -- Tyson Foods
    ('ia.tyson_storm_lake',     'Tyson Storm Lake',           'pork_packing', 'Tyson Foods', 'Tyson Foods Inc',
     'Storm Lake', 'Buena Vista', 'IA', 42.640, -95.207, 'active', 'public_knowledge', 'Pork; large IA Tyson plant'),
    ('ia.tyson_waterloo',       'Tyson Waterloo',             'pork_packing', 'Tyson Foods', 'Tyson Foods Inc',
     'Waterloo', 'Black Hawk', 'IA', 42.493, -92.339, 'active', 'public_knowledge', NULL),
    ('ia.tyson_perry',          'Tyson Perry',                'pork_packing', 'Tyson Foods', 'Tyson Foods Inc',
     'Perry', 'Dallas',        'IA', 41.838, -94.107, 'active', 'public_knowledge', NULL),
    ('ia.tyson_columbus_jct',   'Tyson Columbus Junction',    'pork_packing', 'Tyson Foods', 'Tyson Foods Inc',
     'Columbus Junction', 'Louisa', 'IA', 41.279, -91.361, 'active', 'public_knowledge', NULL),

    -- JBS USA
    ('ia.jbs_marshalltown',     'JBS Marshalltown',           'pork_packing', 'JBS USA', 'JBS S.A.',
     'Marshalltown', 'Marshall', 'IA', 42.049, -92.908, 'active', 'public_knowledge', NULL),
    ('ia.jbs_ottumwa',          'JBS Ottumwa',                'pork_packing', 'JBS USA', 'JBS S.A.',
     'Ottumwa', 'Wapello',     'IA', 41.020, -92.411, 'active', 'public_knowledge', NULL),

    -- Smithfield Foods
    ('ia.smithfield_algona',    'Smithfield Algona',          'pork_packing', 'Smithfield Foods', 'WH Group',
     'Algona', 'Kossuth',      'IA', 43.069, -94.234, 'active', 'public_knowledge', NULL),
    ('ia.smithfield_denison',   'Smithfield Denison',         'pork_packing', 'Smithfield Foods', 'WH Group',
     'Denison', 'Crawford',    'IA', 41.985, -95.355, 'active', 'public_knowledge', NULL),

    -- Seaboard Triumph Foods
    ('ia.seaboard_triumph_sioux_city', 'Seaboard Triumph Foods', 'pork_packing', 'Seaboard Triumph Foods', 'Seaboard Corp / Triumph Foods JV',
     'Sioux City', 'Woodbury', 'IA', 42.503, -96.405, 'active', 'public_knowledge', 'Newer plant ~22K head/day'),

    -- Wholestone Farms
    ('ia.wholestone_council_bluffs', 'Wholestone Farms',     'pork_packing', 'Wholestone Farms', NULL,
     'Council Bluffs', 'Pottawattamie', 'IA', 41.262, -95.861, 'active', 'public_knowledge', 'Newer plant'),

    -- Hormel / Quality Pork Processors
    ('ia.hormel_force_city',    'Hormel Force City',          'pork_packing', 'Hormel Foods', 'Hormel Foods Corp',
     'Force City', 'Wright',   'IA', 42.745, -93.717, 'active', 'public_knowledge', 'Verify city - may be Forest City'),

    -- Sioux-Preme Packing
    ('ia.sioux_preme_sioux_center', 'Sioux-Preme Packing',  'pork_packing', 'Sioux-Preme Packing Co', NULL,
     'Sioux Center', 'Sioux',  'IA', 43.078, -96.175, 'active', 'public_knowledge', 'Smaller, ~5K head/day')
ON CONFLICT (facility_id) DO UPDATE SET
    name = EXCLUDED.name,
    operator = EXCLUDED.operator,
    parent_company = EXCLUDED.parent_company,
    notes = EXCLUDED.notes,
    updated_at = NOW();

-- =============================================================================
-- BEEF PACKING — 3 known IA-relevant plants
-- =============================================================================

INSERT INTO reference.facility_master
    (facility_id, name, industry_code, operator, parent_company,
     city, county, state, lat, lon, status, data_source, notes)
VALUES
    ('ia.iowa_premium_tama',    'Iowa Premium Beef',          'beef_packing', 'Iowa Premium Beef', NULL,
     'Tama', 'Tama',           'IA', 41.967, -92.578, 'active', 'public_knowledge', '~1.1K head/day; premium Black Angus focus'),
    ('ia.tyson_dakota_city',    'Tyson Dakota City',          'beef_packing', 'Tyson Foods', 'Tyson Foods Inc',
     'Dakota City', 'Dakota',  'NE', 42.413, -96.418, 'active', 'public_knowledge', 'NE side of MO river; IA-influencing for cattle procurement; not in IA'),
    ('ia.greater_omaha_packing', 'Greater Omaha Packing',     'beef_packing', 'Greater Omaha Packing', NULL,
     'Omaha', 'Douglas',       'NE', 41.276, -95.953, 'active', 'public_knowledge', 'NE not IA but IA-influencing')
ON CONFLICT (facility_id) DO UPDATE SET
    name = EXCLUDED.name,
    operator = EXCLUDED.operator,
    parent_company = EXCLUDED.parent_company,
    notes = EXCLUDED.notes,
    updated_at = NOW();

-- =============================================================================
-- EGG LAYERS — 5 major operators (sites are CAFO-permitted; need DNR pull
--                                  for full per-site list)
-- =============================================================================

INSERT INTO reference.facility_master
    (facility_id, name, industry_code, operator, parent_company,
     city, county, state, lat, lon, status, data_source, notes)
VALUES
    ('ia.versova_corporate',    'Versova Holdings - corporate', 'egg_layers', 'Versova Holdings', 'Versova Holdings LLC',
     'Sioux Center', 'Sioux',  'IA', 43.078, -96.175, 'active', 'public_knowledge', 'Largest IA egg producer; multiple sites — needs DNR scrape for per-site detail. Includes Rembrandt Foods.'),
    ('ia.center_fresh_sioux_center', 'Center Fresh Group',    'egg_layers', 'Center Fresh Group', NULL,
     'Sioux Center', 'Sioux',  'IA', 43.078, -96.175, 'active', 'public_knowledge', 'Major NW IA layer'),
    ('ia.rose_acre_stuart',     'Rose Acre Farms - Stuart',   'egg_layers', 'Rose Acre Farms', NULL,
     'Stuart', 'Adair',        'IA', 41.502, -94.319, 'active', 'public_knowledge', NULL),
    ('ia.rose_acre_winterset',  'Rose Acre Farms - Winterset', 'egg_layers', 'Rose Acre Farms', NULL,
     'Winterset', 'Madison',   'IA', 41.339, -94.013, 'active', 'public_knowledge', NULL),
    ('ia.sparboe_new_hampton',  'Sparboe Farms - New Hampton', 'egg_layers', 'Sparboe Farms', NULL,
     'New Hampton', 'Chickasaw', 'IA', 43.059, -92.318, 'active', 'public_knowledge', NULL),
    ('ia.daybreak_lake_mills',  'Daybreak Foods - Lake Mills', 'egg_layers', 'Daybreak Foods', NULL,
     'Lake Mills', 'Winnebago', 'IA', 43.421, -93.531, 'active', 'public_knowledge', NULL),
    ('ia.farmers_hen_house_kalona', 'Farmers Hen House',     'egg_layers', 'Farmers Hen House', NULL,
     'Kalona', 'Washington',   'IA', 41.484, -91.708, 'active', 'public_knowledge', 'Organic-focused')
ON CONFLICT (facility_id) DO UPDATE SET
    name = EXCLUDED.name,
    operator = EXCLUDED.operator,
    parent_company = EXCLUDED.parent_company,
    notes = EXCLUDED.notes,
    updated_at = NOW();

-- =============================================================================
-- Summary diagnostic
-- =============================================================================

DO $$
DECLARE
    cnt INTEGER;
BEGIN
    SELECT COUNT(*) INTO cnt FROM reference.facility_master;
    RAISE NOTICE 'reference.facility_master now contains % rows', cnt;
END$$;
