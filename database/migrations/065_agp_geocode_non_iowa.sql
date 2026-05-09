-- Migration 065: geocode non-IA AGP plants
-- Date: 2026-05-09
--
-- Coordinates are city-centroid public-knowledge (USGS GNIS); good
-- enough for haversine-based draw-region edges. Plant-precise lat/lon
-- can be ground-truthed later.

BEGIN;

UPDATE reference.oilseed_crush_facilities
SET lat = 44.9333, lon = -96.0556,
    notes = COALESCE(notes || E'\n', '') ||
            '[2026-05-09] Coordinates set to Dawson MN city centroid; ' ||
            'plant-precise lat/lon TBD.',
    updated_at = NOW()
WHERE facility_id = 'mn.agp_dawson';

UPDATE reference.oilseed_crush_facilities
SET lat = 39.7639, lon = -94.8467,
    notes = COALESCE(notes || E'\n', '') ||
            '[2026-05-09] Coordinates set to St. Joseph MO city centroid; ' ||
            'plant-precise lat/lon TBD.',
    updated_at = NOW()
WHERE facility_id = 'mo.agp_st_joseph';

UPDATE reference.oilseed_crush_facilities
SET lat = 41.2533, lon = -97.1303,
    notes = COALESCE(notes || E'\n', '') ||
            '[2026-05-09] Coordinates set to David City NE city centroid; ' ||
            'plant-precise lat/lon TBD.',
    updated_at = NOW()
WHERE facility_id = 'ne.agp_david_city';

UPDATE reference.oilseed_crush_facilities
SET lat = 40.5861, lon = -98.3839,
    notes = COALESCE(notes || E'\n', '') ||
            '[2026-05-09] Coordinates set to Hastings NE city centroid; ' ||
            'plant-precise lat/lon TBD.',
    updated_at = NOW()
WHERE facility_id = 'ne.agp_hastings';

UPDATE reference.oilseed_crush_facilities
SET lat = 45.4647, lon = -98.4861,
    notes = COALESCE(notes || E'\n', '') ||
            '[2026-05-09] Coordinates set to Aberdeen SD city centroid; ' ||
            'plant-precise lat/lon TBD.',
    updated_at = NOW()
WHERE facility_id = 'sd.agp_aberdeen';

COMMIT;
