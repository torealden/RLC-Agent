-- Migration 066: canonical equipment-function dictionary + unit mapping
-- Date: 2026-05-09
--
-- Purpose: Define a canonical taxonomy of equipment functions per
-- industry so we can:
--   1. Validate Title V permit extractions (did we find all expected
--      process steps?)
--   2. Compare equipment lists across operators on the SAME axis
--      regardless of permit format / state agency naming.
--   3. Surface gaps ("Eagle Grove has no deodorizer extracted, but
--      AGP press claims integrated refining → re-extraction needed").
--
-- Per-industry seeds:
--   - oilseed_crush: 19 process steps + utilities + co-located refining
--   - (later) biofuel, slaughter_render, fats_greases, etc.
--
-- Source: domain_knowledge/process_flows/oilseed_crush.md

BEGIN;

-- ============================================================================
-- 1. Canonical function dictionary
-- ============================================================================

CREATE TABLE IF NOT EXISTS reference.equipment_function_canonical (
    function_id           TEXT PRIMARY KEY,
    industry_code         TEXT NOT NULL,
    step_order            INTEGER NOT NULL,
    function_label        TEXT NOT NULL,
    function_category     TEXT NOT NULL,  -- process / utility / control_device / storage / loadout
    is_optional           BOOLEAN NOT NULL DEFAULT FALSE,
    typical_capacity_unit TEXT,
    typical_control_devices TEXT[],
    diagnostic_notes      TEXT,
    description           TEXT,
    created_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at            TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_equipment_function_canonical_industry
    ON reference.equipment_function_canonical (industry_code, step_order);

-- ============================================================================
-- 2. Unit → canonical-function mapping (many-to-many)
-- ============================================================================

CREATE TABLE IF NOT EXISTS silver.permit_unit_canonical_map (
    permit_unit_id  BIGINT NOT NULL,
    function_id     TEXT   NOT NULL REFERENCES reference.equipment_function_canonical(function_id),
    confidence      NUMERIC(3,2) NOT NULL DEFAULT 1.00,
    is_primary      BOOLEAN NOT NULL DEFAULT TRUE,
    mapped_by       TEXT NOT NULL DEFAULT 'llm',
    mapped_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    notes           TEXT,
    PRIMARY KEY (permit_unit_id, function_id)
);

CREATE INDEX IF NOT EXISTS idx_permit_unit_canonical_map_function
    ON silver.permit_unit_canonical_map (function_id);

-- ============================================================================
-- 3. Seed: oilseed_crush canonical functions
-- ============================================================================

INSERT INTO reference.equipment_function_canonical
    (function_id, industry_code, step_order, function_label, function_category,
     is_optional, typical_capacity_unit, typical_control_devices,
     diagnostic_notes, description)
VALUES
    -- Process steps 1-19
    ('oilseed_crush.receiving', 'oilseed_crush', 1, 'Raw seed receiving', 'process',
     FALSE, 'tons/hr',
     ARRAY['cyclone','baghouse','aspirator'],
     'Total receiving rate >= 1.5x crush rate (surge headroom)',
     'Truck/rail/barge unloading, weigh, sample.'),

    ('oilseed_crush.cleaning', 'oilseed_crush', 2, 'Seed cleaning', 'process',
     FALSE, 'tons/hr',
     ARRAY['cyclone','baghouse'],
     'Typical 1-3% material removal as screenings.',
     'Vibratory screen, magnet, aspiration cleaner, destoner.'),

    ('oilseed_crush.drying', 'oilseed_crush', 3, 'Seed drying', 'process',
     TRUE, 'tons/hr; MMBtu/hr',
     ARRAY['cyclone'],
     'Dryer fuel input ~ 1-2 MMBtu/ton seed.',
     'Continuous flow grain dryer; often skipped if seed arrives dry.'),

    ('oilseed_crush.dehulling_cracking', 'oilseed_crush', 4, 'Dehulling / cracking', 'process',
     FALSE, 'tons/hr',
     ARRAY['baghouse','cyclone aspirator'],
     'Soy hull yield ~6% of clean seed.',
     'Cracking mill, hull aspirator, hull conveyor.'),

    ('oilseed_crush.conditioning', 'oilseed_crush', 5, 'Conditioning', 'process',
     FALSE, 'tons/hr',
     ARRAY[]::TEXT[],
     'Steam ~150 lb/ton seed; residence ~30 min.',
     'Heat cracked seed to 165-175F for flaking.'),

    ('oilseed_crush.flaking', 'oilseed_crush', 6, 'Flaking', 'process',
     FALSE, 'tons/hr per roll',
     ARRAY[]::TEXT[],
     'Modern flakers ~50-100 t/hr each; usually 4-6 in parallel.',
     'Roll cracks into thin flakes for solvent extraction.'),

    ('oilseed_crush.expanding', 'oilseed_crush', 7, 'Expanding (extruder)', 'process',
     TRUE, 'tons/hr',
     ARRAY[]::TEXT[],
     'Adds ~5-10% extraction efficiency; not all plants have it.',
     'Texture flakes into porous collets.'),

    ('oilseed_crush.solvent_extraction', 'oilseed_crush', 8, 'Solvent extraction', 'process',
     FALSE, 'tons/day',
     ARRAY['mineral oil scrubber','condenser','vapor recovery'],
     'Hexane usage 0.6-1.2 gal/ton crushed; oil yield 18-19% of bean weight.',
     'Continuous loop / horizontal-belt extractor + miscella tanks.'),

    ('oilseed_crush.desolventizer_toaster', 'oilseed_crush', 9, 'Desolventizer-Toaster (DT)', 'process',
     FALSE, 'tons/hr',
     ARRAY['mineral oil scrubber','condenser'],
     'Residual hexane in finished meal < 500 ppm (regulatory).',
     'Strip residual hexane from spent flakes; toast protein.'),

    ('oilseed_crush.meal_drying_cooling', 'oilseed_crush', 10, 'Meal drying / cooling', 'process',
     FALSE, 'tons/hr',
     ARRAY['cyclone'],
     'Drying air ~5,000-15,000 ACFM per t/hr meal.',
     'Reduce meal moisture to 10-12% and cool to <100F.'),

    ('oilseed_crush.meal_grinding', 'oilseed_crush', 11, 'Meal grinding / sizing', 'process',
     FALSE, 'tons/hr',
     ARRAY['baghouse'],
     'Hammer / roller / fine grinder.',
     'Grind meal to 44% standard or 48%+ Hi-Pro spec.'),

    ('oilseed_crush.meal_storage', 'oilseed_crush', 12, 'Meal storage', 'storage',
     FALSE, 'tons',
     ARRAY[]::TEXT[],
     'Typical storage = 7-14 days production.',
     'Silos, bins, flat warehouse for meal output.'),

    ('oilseed_crush.meal_loadout', 'oilseed_crush', 13, 'Meal loadout', 'loadout',
     FALSE, 'tons/hr',
     ARRAY['baghouse','cyclone'],
     'Truck 25t / rail 110t / barge 1500t.',
     'Truck, rail, barge loading of finished meal.'),

    ('oilseed_crush.solvent_recovery', 'oilseed_crush', 14, 'Solvent recovery', 'process',
     FALSE, 'gal/hr',
     ARRAY['condenser'],
     'Closed-loop hexane recycle; losses = facility consumption metric.',
     'Distill miscella to separate hexane from crude oil.'),

    ('oilseed_crush.degumming', 'oilseed_crush', 15, 'Degumming', 'process',
     TRUE, 'tons/hr',
     ARRAY[]::TEXT[],
     'Gum yield 1-2% of crude oil = lecithin.',
     'Remove phosphatides via water/acid wash + centrifuge.'),

    ('oilseed_crush.neutralization', 'oilseed_crush', 16, 'Neutralization (alkali refining)', 'process',
     TRUE, 'tons/hr',
     ARRAY[]::TEXT[],
     'Soapstock 1-3% of oil to acidulation.',
     'Caustic soda mix tank + centrifuge to remove FFA.'),

    ('oilseed_crush.bleaching', 'oilseed_crush', 17, 'Bleaching', 'process',
     TRUE, 'tons/hr',
     ARRAY['baghouse'],
     'Bleaching clay 0.5-1.0% of oil.',
     'Activated clay reactor + niagara filter; spent clay handling.'),

    ('oilseed_crush.deodorizing', 'oilseed_crush', 18, 'Deodorizing', 'process',
     TRUE, 'tons/hr',
     ARRAY['scrubber','vapor condenser'],
     'Presence of deodorizer = full-refining (RBD) capability.',
     'Stripper column under deep vacuum + steam ejectors.'),

    ('oilseed_crush.refined_oil_storage_loadout', 'oilseed_crush', 19, 'Refined oil storage / loadout', 'storage',
     TRUE, 'tons',
     ARRAY[]::TEXT[],
     'Refined oil tanks ~ 1-2 weeks production; often N2-blanketed.',
     'Hold and ship refined oil.'),

    -- Utilities (step_order 20+)
    ('oilseed_crush.boiler_steam', 'oilseed_crush', 20, 'Steam boiler', 'utility',
     FALSE, 'MMBtu/hr',
     ARRAY['SCR','SNCR','baghouse'],
     'Total boiler 4-6 MMBtu/ton seed; usually 2 boilers.',
     'Coal/NG/fuel-oil package + economizer boilers.'),

    ('oilseed_crush.cooling_tower', 'oilseed_crush', 21, 'Cooling tower', 'utility',
     FALSE, 'MMBtu/hr; tons cooling',
     ARRAY['drift eliminator'],
     'Mechanical-draft cooling for process condensers.',
     'Cooling tower(s) for plant-wide heat rejection.'),

    ('oilseed_crush.compressed_air', 'oilseed_crush', 22, 'Compressed air', 'utility',
     FALSE, 'hp; SCFM',
     ARRAY[]::TEXT[],
     'Plant air for instrumentation + pneumatics.',
     'Air compressor(s) for plant-wide service air.'),

    ('oilseed_crush.wastewater', 'oilseed_crush', 23, 'Wastewater treatment', 'utility',
     FALSE, 'gpm',
     ARRAY[]::TEXT[],
     'API separator + biological treatment typical.',
     'Process wastewater treatment.'),

    -- Storage tanks
    ('oilseed_crush.storage_hexane', 'oilseed_crush', 30, 'Hexane storage', 'storage',
     FALSE, 'gallons',
     ARRAY['vapor recovery','floating roof'],
     'Typical 2-4 above-ground tanks 30-100k gal each.',
     'Solvent storage tank farm.'),

    ('oilseed_crush.storage_crude_oil', 'oilseed_crush', 31, 'Crude oil storage', 'storage',
     FALSE, 'gallons; tons',
     ARRAY[]::TEXT[],
     'Buffers crude output to refining or shipment.',
     'Crude soybean oil storage tanks.'),

    ('oilseed_crush.storage_fuel', 'oilseed_crush', 32, 'Fuel storage', 'storage',
     TRUE, 'gallons',
     ARRAY[]::TEXT[],
     'Sized to a few days boiler / heater fuel.',
     'NG / fuel oil storage for boilers and heaters.'),

    ('oilseed_crush.storage_chemical', 'oilseed_crush', 33, 'Chemical storage (caustic, bleaching clay, etc)', 'storage',
     TRUE, 'gallons; tons',
     ARRAY[]::TEXT[],
     'Caustic for neutralization, clay for bleaching.',
     'Process chemical storage silos and tanks.'),

    -- Loadout (raw bean side)
    ('oilseed_crush.bean_loadout_handling', 'oilseed_crush', 40, 'Bean handling / conveyance', 'process',
     FALSE, 'tons/hr',
     ARRAY['baghouse','cyclone'],
     'Drag conveyors and elevators between process steps.',
     'In-plant grain handling (drags, elevators, transfers).'),

    ('oilseed_crush.hull_handling', 'oilseed_crush', 41, 'Hull handling / pelleting', 'process',
     TRUE, 'tons/hr',
     ARRAY['baghouse'],
     'Hi-Pro plants; hulls pelleted for feed market.',
     'Hull conveyor, pelleter, separate storage / loadout.'),

    -- Generic control devices (mapped as secondary function on units)
    ('oilseed_crush.control_baghouse', 'oilseed_crush', 90, 'Baghouse (PM control)', 'control_device',
     FALSE, 'ACFM',
     ARRAY[]::TEXT[],
     'Most common PM control on receiving / handling / loadout.',
     'Particulate-matter baghouse on dust-generating sources.'),

    ('oilseed_crush.control_cyclone', 'oilseed_crush', 91, 'Cyclone (PM control)', 'control_device',
     FALSE, 'ACFM',
     ARRAY[]::TEXT[],
     'Pre-filter ahead of baghouse; lower PM2.5 efficiency.',
     'Mechanical PM control via centrifugal separation.'),

    ('oilseed_crush.control_scrubber', 'oilseed_crush', 92, 'Mineral oil scrubber (VOC/hexane)', 'control_device',
     FALSE, 'ACFM',
     ARRAY[]::TEXT[],
     'Critical control on extractor + DT vents.',
     'Mineral oil absorption for hexane vapor.'),

    ('oilseed_crush.control_aspiration', 'oilseed_crush', 93, 'Aspiration / fugitive PM', 'control_device',
     FALSE, 'ACFM',
     ARRAY[]::TEXT[],
     'Often integral to dehulling / handling steps.',
     'Aspiration vents pulling air through equipment.')

ON CONFLICT (function_id) DO UPDATE SET
    function_label = EXCLUDED.function_label,
    function_category = EXCLUDED.function_category,
    is_optional = EXCLUDED.is_optional,
    typical_capacity_unit = EXCLUDED.typical_capacity_unit,
    typical_control_devices = EXCLUDED.typical_control_devices,
    diagnostic_notes = EXCLUDED.diagnostic_notes,
    description = EXCLUDED.description,
    updated_at = NOW();

COMMIT;
