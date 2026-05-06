-- Migration 056: Multi-industry facility master + per-industry capacity tables
--
-- Date: 2026-05-06
--
-- Why:
--   reference.oilseed_crush_facilities is industry-specific. The Market
--   Field's network propagation needs to span multiple industries (ethanol
--   competes with crush for corn; pork packers drive meal demand; CAFOs
--   consume grain at the field level; rail terminals dictate basis).
--
--   This migration adds a unified reference.facility_master keyed on
--   facility_id and industry_code, plus per-industry capacity extension
--   tables. The existing oilseed_crush_facilities table stays as-is for
--   backward compatibility — its rows will be replicated into
--   facility_master in a follow-up migration so the Market Field's
--   facility-graph builder can iterate one source.
--
-- Industries covered:
--   oilseed_crush, ethanol, biodiesel, renewable_diesel,
--   pork_packing, beef_packing, poultry_packing,
--   egg_layers, pig_finishing,
--   grain_handling, rail_terminal, river_terminal,
--   feed_mill, cold_storage, pipeline, other
--
-- See docs/specs/iowa_industry_facility_taxonomy.md for the full
-- taxonomy + per-industry data-source list + LLM extraction targets.

CREATE TABLE IF NOT EXISTS reference.facility_master (
    facility_id            TEXT PRIMARY KEY,
    name                   TEXT NOT NULL,
    industry_code          TEXT NOT NULL CHECK (industry_code IN (
        'oilseed_crush', 'ethanol', 'biodiesel', 'renewable_diesel',
        'pork_packing', 'beef_packing', 'poultry_packing',
        'egg_layers', 'pig_finishing', 'cattle_feedlot', 'dairy',
        'grain_handling', 'rail_terminal', 'river_terminal',
        'feed_mill', 'cold_storage', 'pipeline', 'other'
    )),
    operator               TEXT,
    parent_company         TEXT,
    operator_kg_key        TEXT,
    address                TEXT,
    city                   TEXT,
    county                 TEXT,
    state                  CHAR(2),
    zip                    TEXT,
    country                TEXT NOT NULL DEFAULT 'US',
    lat                    NUMERIC,
    lon                    NUMERIC,
    county_kg_key          TEXT,
    status                 TEXT NOT NULL DEFAULT 'active'
        CHECK (status IN ('active', 'idle', 'closed', 'announced',
                          'under_construction', 'unknown')),
    commissioned_year      INTEGER,
    last_expansion_year    INTEGER,
    notes                  TEXT,
    sources                TEXT,
    is_canonical           BOOLEAN NOT NULL DEFAULT TRUE,
    superseded_by          TEXT,
    data_source            TEXT,
    verified_at            TIMESTAMP WITH TIME ZONE,
    verified_by            TEXT,
    verification_method    TEXT,
    created_at             TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at             TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_facility_master_industry_state
    ON reference.facility_master (industry_code, state);
CREATE INDEX IF NOT EXISTS idx_facility_master_canonical
    ON reference.facility_master (is_canonical, status)
    WHERE is_canonical = TRUE;
CREATE INDEX IF NOT EXISTS idx_facility_master_operator
    ON reference.facility_master (operator);
CREATE INDEX IF NOT EXISTS idx_facility_master_parent
    ON reference.facility_master (parent_company);

COMMENT ON TABLE reference.facility_master IS
'Unified, multi-industry facility ledger. One row per physical facility regardless of industry. Per-industry capacity metrics live in reference.facility_capacity_<industry> tables joined on facility_id. is_canonical=TRUE filters to non-superseded records (same pattern as oilseed_crush_facilities).';

-- =============================================================================
-- Per-industry capacity extension tables
-- =============================================================================

CREATE TABLE IF NOT EXISTS reference.facility_capacity_ethanol (
    facility_id            TEXT PRIMARY KEY REFERENCES reference.facility_master(facility_id) ON DELETE CASCADE,
    nameplate_mmgy         NUMERIC,         -- million gallons/year
    bushels_per_day        NUMERIC,         -- corn input rate at full utilization
    operating_days_year    INTEGER,
    ddgs_lb_per_bu         NUMERIC,         -- distillers dried grains yield
    distillers_oil_lb_per_bu NUMERIC,       -- corn oil / DCO yield (relevant to oilseed-oil markets)
    co2_capture_tpy        NUMERIC,         -- CO2 capture capacity if installed
    boiler_fuel_type       TEXT,            -- 'natural_gas' / 'coal' / 'biomass' (45Z relevance)
    has_solvent_extraction BOOLEAN,
    co_located_biodiesel   BOOLEAN DEFAULT FALSE,
    rfs_rin_facility_id    TEXT,            -- EPA RFS RIN-generation registration ID
    eia_plant_id           TEXT,            -- EIA Form 819 ID
    title_v_permit_no      TEXT,
    notes                  TEXT,
    updated_at             TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS reference.facility_capacity_biodiesel (
    facility_id            TEXT PRIMARY KEY REFERENCES reference.facility_master(facility_id) ON DELETE CASCADE,
    nameplate_mmgy         NUMERIC,
    feedstock_flexibility  TEXT,            -- 'soy_only' / 'multi_oil' / 'tallow_capable' / 'uco_capable'
    pretreatment_capability TEXT,            -- 'none' / 'degumming' / 'refining' / 'full'
    glycerin_capacity_lbs_year NUMERIC,
    storage_capacity_gal   NUMERIC,
    co_located_ethanol     BOOLEAN DEFAULT FALSE,
    co_located_crush       BOOLEAN DEFAULT FALSE,
    rfs_rin_facility_id    TEXT,
    biodiesel_capacity_mgy NUMERIC,         -- alias / legacy field for compat
    notes                  TEXT,
    updated_at             TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS reference.facility_capacity_renewable_diesel (
    facility_id            TEXT PRIMARY KEY REFERENCES reference.facility_master(facility_id) ON DELETE CASCADE,
    nameplate_mmgy         NUMERIC,
    technology             TEXT,            -- 'HEFA' / 'gasification' / etc.
    feedstock_mix          TEXT,
    hydrogen_supply        TEXT,            -- 'on_site_smr' / 'merchant' / 'electrolysis'
    saf_capable            BOOLEAN DEFAULT FALSE,
    pretreatment_capability TEXT,
    notes                  TEXT,
    updated_at             TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS reference.facility_capacity_pork_packing (
    facility_id            TEXT PRIMARY KEY REFERENCES reference.facility_master(facility_id) ON DELETE CASCADE,
    daily_slaughter_head   INTEGER,
    annual_slaughter_head  INTEGER,
    cooler_storage_lbs     NUMERIC,
    has_further_processing BOOLEAN DEFAULT FALSE,
    has_rendering          BOOLEAN DEFAULT FALSE,
    rendering_capacity_lbs_day NUMERIC,
    procurement_radius_mi  INTEGER,
    fsis_establishment_no  TEXT,            -- USDA FSIS plant number
    notes                  TEXT,
    updated_at             TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS reference.facility_capacity_beef_packing (
    facility_id            TEXT PRIMARY KEY REFERENCES reference.facility_master(facility_id) ON DELETE CASCADE,
    daily_slaughter_head   INTEGER,
    annual_slaughter_head  INTEGER,
    cooler_storage_lbs     NUMERIC,
    has_further_processing BOOLEAN DEFAULT FALSE,
    has_rendering          BOOLEAN DEFAULT FALSE,
    rendering_capacity_lbs_day NUMERIC,
    grade_focus            TEXT,            -- 'commodity' / 'choice_only' / 'natural' / 'organic'
    fsis_establishment_no  TEXT,
    notes                  TEXT,
    updated_at             TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS reference.facility_capacity_egg_layers (
    facility_id            TEXT PRIMARY KEY REFERENCES reference.facility_master(facility_id) ON DELETE CASCADE,
    bird_capacity          INTEGER,         -- max permitted laying hens
    layer_type             TEXT,            -- 'cage' / 'cage_free' / 'free_range' / 'organic'
    has_egg_processing     BOOLEAN DEFAULT FALSE,
    has_feed_mill          BOOLEAN DEFAULT FALSE,
    manure_system          TEXT,            -- 'dry' / 'liquid' / 'belt'
    annual_egg_production_dozen NUMERIC,
    notes                  TEXT,
    updated_at             TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS reference.facility_capacity_pig_finishing (
    facility_id            TEXT PRIMARY KEY REFERENCES reference.facility_master(facility_id) ON DELETE CASCADE,
    head_capacity          INTEGER,         -- max permitted hogs
    production_type        TEXT,            -- 'farrow_to_wean' / 'wean_to_finish' / 'finishing' / 'farrow_to_finish'
    has_feed_mill          BOOLEAN DEFAULT FALSE,
    integrator             TEXT,            -- e.g., 'Iowa Select Farms', 'Smithfield', 'Christensen'
    manure_system          TEXT,            -- 'pit' / 'lagoon' / 'composting'
    notes                  TEXT,
    updated_at             TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS reference.facility_capacity_grain_handling (
    facility_id            TEXT PRIMARY KEY REFERENCES reference.facility_master(facility_id) ON DELETE CASCADE,
    storage_bushels        NUMERIC,
    drying_capacity_bu_hr  NUMERIC,
    receipt_modes          TEXT[],          -- {'truck', 'rail', 'river'}
    rail_carloads_day      INTEGER,
    is_shuttle_loader      BOOLEAN DEFAULT FALSE,    -- 110-car capable
    river_loading          BOOLEAN DEFAULT FALSE,
    specialty_handling     TEXT[],          -- {'organic', 'non_gmo', 'food_grade'}
    idals_license_no       TEXT,
    notes                  TEXT,
    updated_at             TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS reference.facility_capacity_rail_terminal (
    facility_id            TEXT PRIMARY KEY REFERENCES reference.facility_master(facility_id) ON DELETE CASCADE,
    class_1_carriers       TEXT[],          -- {'UP', 'BNSF', 'CN', 'NS'} (which Class 1 lines)
    short_line_carriers    TEXT[],
    is_shuttle_capable     BOOLEAN DEFAULT FALSE,
    daily_carloads_capacity INTEGER,
    cargo_mix              TEXT[],          -- {'grain', 'ethanol', 'coal', 'intermodal'}
    has_intermodal         BOOLEAN DEFAULT FALSE,
    notes                  TEXT,
    updated_at             TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS reference.facility_capacity_river_terminal (
    facility_id            TEXT PRIMARY KEY REFERENCES reference.facility_master(facility_id) ON DELETE CASCADE,
    river                  TEXT,            -- 'Mississippi' / 'Missouri'
    river_mile             NUMERIC,
    barge_loading_bu_hr    NUMERIC,
    storage_bushels        NUMERIC,
    receipt_modes          TEXT[],          -- {'truck', 'rail'}
    cargo_mix              TEXT[],          -- {'grain', 'fertilizer', 'coal'}
    notes                  TEXT,
    updated_at             TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

COMMENT ON TABLE reference.facility_capacity_ethanol IS
'Per-facility ethanol capacity metrics. nameplate_mmgy = million gallons/year. distillers_oil_lb_per_bu ties back to corn-oil/DCO trade markets we track.';

COMMENT ON TABLE reference.facility_capacity_pork_packing IS
'Per-facility pork-packing capacity. daily_slaughter_head drives soybean-meal demand modeling. fsis_establishment_no enables join to USDA FSIS food-safety records.';
