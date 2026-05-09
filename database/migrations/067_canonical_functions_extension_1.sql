-- Migration 067: extend canonical functions based on observed unmapped units
-- Date: 2026-05-09
--
-- Found via running the keyword mapper on existing IA AGP/Cargill/Bunge/
-- ADM permits — pattern misses fell into 5 categories:
--
--   1. Whole-bean storage tanks (GP02 "500k bushel Bean Storage")
--   2. Boiler-fuel handling (coal receiving, fly ash, limestone)
--   3. Emergency / backup equipment (diesel generators, fire pumps)
--   4. Fugitive emissions (road dust, equipment leaks)
--   5. Generic loadout (truck/rail without "meal" qualifier)
--
-- Plus: AGP Algona has co-located biodiesel — needs the biofuel
-- industry taxonomy. Seeded here in stub form; will be expanded when
-- we tackle the biofuel scraper.

BEGIN;

-- ============================================================================
-- 1. New oilseed_crush canonical functions
-- ============================================================================

INSERT INTO reference.equipment_function_canonical
    (function_id, industry_code, step_order, function_label, function_category,
     is_optional, typical_capacity_unit, typical_control_devices,
     diagnostic_notes, description)
VALUES
    -- Whole-bean / seed storage (silos before processing)
    ('oilseed_crush.storage_seed', 'oilseed_crush', 0, 'Whole-seed storage', 'storage',
     FALSE, 'bushels; tons',
     ARRAY['baghouse','aspiration'],
     'Typical 1-3 weeks crush rate as bean storage; many large concrete silos.',
     'Whole soybean / canola storage silos pre-processing.'),

    -- Boiler fuel + ash handling (utility ancillaries)
    ('oilseed_crush.boiler_fuel_handling', 'oilseed_crush', 24, 'Boiler fuel handling', 'utility',
     TRUE, 'tons/hr',
     ARRAY['baghouse','cyclone'],
     'Coal-fired boilers carry receiving + storage + feeders + ash.',
     'Coal receiving, conveying, storage, feeders; limestone for SO2 control.'),

    ('oilseed_crush.ash_handling', 'oilseed_crush', 25, 'Fly / bottom ash handling', 'utility',
     TRUE, 'tons/hr',
     ARRAY['baghouse'],
     'Coal-fired plants only; closed-cycle ash conveying + silos + loadout.',
     'Fly ash conveying, silo, loadout; bottom ash handling.'),

    ('oilseed_crush.emergency_equipment', 'oilseed_crush', 26, 'Emergency / backup equipment', 'utility',
     FALSE, 'kW; gpm',
     ARRAY[]::TEXT[],
     'Permitted as separate emission units; runtime-limited.',
     'Diesel generators, fire pumps, emergency engines.'),

    ('oilseed_crush.fugitive_emissions', 'oilseed_crush', 95, 'Fugitive emissions', 'control_device',
     FALSE, 'tons/yr',
     ARRAY[]::TEXT[],
     'Road dust + equipment leaks + storage piles; tracked separately.',
     'Fugitive PM (roads, piles, equipment leaks).'),

    -- Generic loadout (some permits use undifferentiated "Truck Loading")
    ('oilseed_crush.generic_loadout', 'oilseed_crush', 50, 'Generic product loadout', 'loadout',
     FALSE, 'tons/hr',
     ARRAY['baghouse','cyclone'],
     'Use when permit description does not distinguish meal vs hull vs oil.',
     'Truck/rail/barge loading without product-specific tag.')

ON CONFLICT (function_id) DO UPDATE SET
    function_label = EXCLUDED.function_label,
    function_category = EXCLUDED.function_category,
    is_optional = EXCLUDED.is_optional,
    typical_capacity_unit = EXCLUDED.typical_capacity_unit,
    typical_control_devices = EXCLUDED.typical_control_devices,
    diagnostic_notes = EXCLUDED.diagnostic_notes,
    description = EXCLUDED.description,
    updated_at = NOW();

-- ============================================================================
-- 2. Stub: biofuel industry canonical functions (for co-located biodiesel
--    at AGP Algona / Sergeant Bluff / Mason City). Just enough to map
--    existing units; full taxonomy comes when we tackle the biofuel
--    scraper.
-- ============================================================================

INSERT INTO reference.equipment_function_canonical
    (function_id, industry_code, step_order, function_label, function_category,
     is_optional, typical_capacity_unit, diagnostic_notes, description)
VALUES
    ('biofuel.biodiesel_reactor', 'biofuel', 10, 'Biodiesel transesterification reactor', 'process',
     FALSE, 'mgy; gph',
     'Methanol + KOH/NaOH catalyst with feedstock oil.',
     'Reactor vessel for FAME production via transesterification.'),

    ('biofuel.methanol_handling', 'biofuel', 11, 'Methanol storage / stripping / rectification', 'process',
     FALSE, 'gallons',
     'Recycle methanol from biodiesel + glycerin streams.',
     'Methanol storage, stripper column, rectification.'),

    ('biofuel.glycerin_handling', 'biofuel', 12, 'Glycerin processing', 'process',
     FALSE, 'gallons',
     'Crude glycerin byproduct, ~10% of biodiesel volume.',
     'Glycerin storage, methanol stripping, sale or further refining.'),

    ('biofuel.fame_storage', 'biofuel', 13, 'Finished biodiesel storage', 'storage',
     FALSE, 'gallons',
     'B100 storage before truck/rail loadout.',
     'Finished biodiesel storage tanks.')

ON CONFLICT (function_id) DO UPDATE SET
    function_label = EXCLUDED.function_label,
    function_category = EXCLUDED.function_category,
    typical_capacity_unit = EXCLUDED.typical_capacity_unit,
    diagnostic_notes = EXCLUDED.diagnostic_notes,
    description = EXCLUDED.description,
    updated_at = NOW();

COMMIT;
