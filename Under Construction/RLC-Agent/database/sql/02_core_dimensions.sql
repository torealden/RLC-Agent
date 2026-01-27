-- ============================================================================
-- Round Lakes Commodities - Core Dimension Tables
-- ============================================================================
-- File: 02_core_dimensions.sql
-- Purpose: Create shared dimension tables for data sources, units, locations, series
-- Execute: After 01_schemas.sql
-- ============================================================================

-- ============================================================================
-- DATA SOURCE: Defines each external data feed/API
-- ============================================================================
CREATE TABLE IF NOT EXISTS core.data_source (
    id              SERIAL PRIMARY KEY,
    code            VARCHAR(50) NOT NULL,
    name            VARCHAR(255) NOT NULL,
    description     TEXT,
    source_type     VARCHAR(50) NOT NULL,  -- api, file, manual, scrape
    base_url        VARCHAR(500),
    documentation_url VARCHAR(500),
    update_frequency VARCHAR(50),           -- daily, weekly, monthly, on_demand
    is_active       BOOLEAN NOT NULL DEFAULT TRUE,
    metadata        JSONB DEFAULT '{}',     -- API keys location, rate limits, etc.
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT data_source_code_unique UNIQUE (code),
    CONSTRAINT data_source_type_valid CHECK (
        source_type IN ('api', 'file', 'manual', 'scrape', 'calculated')
    )
);

COMMENT ON TABLE core.data_source IS
    'Registry of all external data sources (APIs, files, feeds). Each source has '
    'a unique code used in series definitions.';

COMMENT ON COLUMN core.data_source.code IS
    'Short unique identifier, e.g., wasde, eia, noaa, cme';
COMMENT ON COLUMN core.data_source.source_type IS
    'How data is acquired: api, file, manual, scrape, calculated';
COMMENT ON COLUMN core.data_source.metadata IS
    'Source-specific config: rate_limits, auth_type, endpoints';

-- Index for active sources lookup
CREATE INDEX IF NOT EXISTS idx_data_source_active
    ON core.data_source(is_active) WHERE is_active = TRUE;

-- ----------------------------------------------------------------------------
-- Seed common data sources
-- ----------------------------------------------------------------------------
INSERT INTO core.data_source (code, name, source_type, description, update_frequency)
VALUES
    ('wasde', 'USDA WASDE Report', 'api',
     'World Agricultural Supply and Demand Estimates', 'monthly'),
    ('nass', 'USDA NASS', 'api',
     'National Agricultural Statistics Service', 'varies'),
    ('ams', 'USDA AMS', 'api',
     'Agricultural Marketing Service - Cash prices', 'daily'),
    ('eia', 'EIA Energy Data', 'api',
     'Energy Information Administration', 'varies'),
    ('noaa', 'NOAA Weather', 'api',
     'National Oceanic and Atmospheric Administration', 'daily'),
    ('cme', 'CME Group', 'api',
     'Chicago Mercantile Exchange - Futures', 'daily'),
    ('cbot', 'CBOT', 'api',
     'Chicago Board of Trade - Grain futures', 'daily'),
    ('usda_fsa', 'USDA FSA', 'api',
     'Farm Service Agency', 'varies'),
    ('internal', 'Internal Calculated', 'calculated',
     'Internally derived metrics and calculations', 'on_demand')
ON CONFLICT (code) DO NOTHING;


-- ============================================================================
-- UNIT: Measurement units with conversion factors
-- ============================================================================
CREATE TABLE IF NOT EXISTS core.unit (
    id                  SERIAL PRIMARY KEY,
    code                VARCHAR(30) NOT NULL,
    name                VARCHAR(100) NOT NULL,
    unit_type           VARCHAR(50) NOT NULL,  -- mass, volume, currency, area, ratio, count
    symbol              VARCHAR(20),
    base_unit_id        INTEGER REFERENCES core.unit(id),
    conversion_factor   DECIMAL(20,10),        -- multiply by this to get base unit
    description         TEXT,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT unit_code_unique UNIQUE (code),
    CONSTRAINT unit_type_valid CHECK (
        unit_type IN ('mass', 'volume', 'currency', 'area', 'ratio', 'count',
                      'temperature', 'yield', 'price', 'other')
    ),
    -- If base_unit_id is set, conversion_factor must also be set
    CONSTRAINT unit_conversion_complete CHECK (
        (base_unit_id IS NULL AND conversion_factor IS NULL) OR
        (base_unit_id IS NOT NULL AND conversion_factor IS NOT NULL)
    )
);

COMMENT ON TABLE core.unit IS
    'Units of measurement with optional conversion factors to base units. '
    'Supports mass (bu, mt, cwt), volume (bbl, gal), currency, area, ratios.';

COMMENT ON COLUMN core.unit.base_unit_id IS
    'Reference to the base unit for this unit type (e.g., kg for mass)';
COMMENT ON COLUMN core.unit.conversion_factor IS
    'Multiply value in this unit by this factor to get base unit value';

-- Index for lookups by type
CREATE INDEX IF NOT EXISTS idx_unit_type ON core.unit(unit_type);

-- ----------------------------------------------------------------------------
-- Seed common units
-- ----------------------------------------------------------------------------
INSERT INTO core.unit (code, name, unit_type, symbol, description)
VALUES
    -- Mass base units
    ('kg', 'Kilogram', 'mass', 'kg', 'SI base unit for mass'),
    ('mt', 'Metric Ton', 'mass', 'MT', '1000 kilograms'),
    ('lb', 'Pound', 'mass', 'lb', 'US/Imperial pound'),
    ('cwt', 'Hundredweight', 'mass', 'cwt', '100 pounds'),
    ('st', 'Short Ton', 'mass', 'ST', '2000 pounds'),

    -- Volume
    ('bbl', 'Barrel', 'volume', 'bbl', '42 US gallons (petroleum)'),
    ('gal', 'Gallon', 'volume', 'gal', 'US gallon'),
    ('l', 'Liter', 'volume', 'L', 'SI volume unit'),

    -- Bushels (commodity-specific mass/volume)
    ('bu_corn', 'Bushel (Corn)', 'mass', 'bu', 'Corn bushel = 56 lbs'),
    ('bu_wheat', 'Bushel (Wheat)', 'mass', 'bu', 'Wheat bushel = 60 lbs'),
    ('bu_soy', 'Bushel (Soybeans)', 'mass', 'bu', 'Soybean bushel = 60 lbs'),
    ('bu_oats', 'Bushel (Oats)', 'mass', 'bu', 'Oats bushel = 32 lbs'),

    -- Area
    ('acre', 'Acre', 'area', 'ac', 'US acre'),
    ('ha', 'Hectare', 'area', 'ha', '10,000 square meters'),

    -- Currency
    ('usd', 'US Dollar', 'currency', '$', 'United States Dollar'),
    ('usd_per_bu', 'USD per Bushel', 'price', '$/bu', 'Price per bushel'),
    ('usd_per_mt', 'USD per Metric Ton', 'price', '$/MT', 'Price per metric ton'),
    ('usd_per_cwt', 'USD per Hundredweight', 'price', '$/cwt', 'Price per hundredweight'),
    ('usd_per_bbl', 'USD per Barrel', 'price', '$/bbl', 'Price per barrel'),

    -- Yield
    ('bu_per_acre', 'Bushels per Acre', 'yield', 'bu/ac', 'Yield in bushels per acre'),
    ('mt_per_ha', 'Metric Tons per Hectare', 'yield', 'MT/ha', 'Yield in MT per hectare'),

    -- Ratio/Percentage
    ('pct', 'Percent', 'ratio', '%', 'Percentage (0-100)'),
    ('ratio', 'Ratio', 'ratio', NULL, 'Dimensionless ratio'),

    -- Count
    ('head', 'Head', 'count', 'head', 'Livestock count'),
    ('units', 'Units', 'count', NULL, 'Generic count'),

    -- Large quantities
    ('mil_bu', 'Million Bushels', 'mass', 'mil bu', 'Million bushels'),
    ('mil_mt', 'Million Metric Tons', 'mass', 'MMT', 'Million metric tons'),
    ('mil_acres', 'Million Acres', 'area', 'mil ac', 'Million acres'),
    ('bil_gal', 'Billion Gallons', 'volume', 'bil gal', 'Billion gallons'),
    ('tho_head', 'Thousand Head', 'count', '000 head', 'Thousand head of livestock'),
    ('mil_head', 'Million Head', 'count', 'mil head', 'Million head of livestock')
ON CONFLICT (code) DO NOTHING;

-- Update conversion factors (after initial insert so references exist)
UPDATE core.unit SET base_unit_id = (SELECT id FROM core.unit WHERE code = 'kg'),
       conversion_factor = 1000 WHERE code = 'mt';
UPDATE core.unit SET base_unit_id = (SELECT id FROM core.unit WHERE code = 'kg'),
       conversion_factor = 0.453592 WHERE code = 'lb';
UPDATE core.unit SET base_unit_id = (SELECT id FROM core.unit WHERE code = 'kg'),
       conversion_factor = 25.4012 WHERE code = 'bu_corn';  -- 56 lbs
UPDATE core.unit SET base_unit_id = (SELECT id FROM core.unit WHERE code = 'kg'),
       conversion_factor = 27.2155 WHERE code = 'bu_wheat'; -- 60 lbs
UPDATE core.unit SET base_unit_id = (SELECT id FROM core.unit WHERE code = 'kg'),
       conversion_factor = 27.2155 WHERE code = 'bu_soy';   -- 60 lbs


-- ============================================================================
-- LOCATION: Flexible geographic hierarchy
-- ============================================================================
CREATE TABLE IF NOT EXISTS core.location (
    id              SERIAL PRIMARY KEY,
    code            VARCHAR(50) NOT NULL,
    name            VARCHAR(255) NOT NULL,
    location_type   VARCHAR(50) NOT NULL,  -- world, region, country, state, district, point
    parent_id       INTEGER REFERENCES core.location(id),
    iso_country     VARCHAR(3),            -- ISO 3166-1 alpha-3
    iso_subdivision VARCHAR(6),            -- ISO 3166-2 subdivision
    fips_code       VARCHAR(10),           -- US FIPS code
    latitude        DECIMAL(9,6),
    longitude       DECIMAL(9,6),
    area_sq_km      DECIMAL(15,2),
    metadata        JSONB DEFAULT '{}',
    is_active       BOOLEAN NOT NULL DEFAULT TRUE,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT location_code_unique UNIQUE (code),
    CONSTRAINT location_type_valid CHECK (
        location_type IN ('world', 'continent', 'region', 'country', 'state',
                          'county', 'district', 'port', 'point', 'custom')
    )
);

COMMENT ON TABLE core.location IS
    'Geographic locations with flexible hierarchy. Supports countries, states, '
    'regions, ports, and point locations (weather stations).';

COMMENT ON COLUMN core.location.code IS
    'Unique code, typically ISO codes or custom identifiers';
COMMENT ON COLUMN core.location.parent_id IS
    'Hierarchical parent (e.g., state -> country)';
COMMENT ON COLUMN core.location.location_type IS
    'world, continent, region, country, state, county, district, port, point, custom';

-- Indexes
CREATE INDEX IF NOT EXISTS idx_location_type ON core.location(location_type);
CREATE INDEX IF NOT EXISTS idx_location_parent ON core.location(parent_id);
CREATE INDEX IF NOT EXISTS idx_location_country ON core.location(iso_country);

-- ----------------------------------------------------------------------------
-- Seed key locations
-- ----------------------------------------------------------------------------
INSERT INTO core.location (code, name, location_type, iso_country, metadata)
VALUES
    ('WORLD', 'World', 'world', NULL, '{}'),
    ('US', 'United States', 'country', 'USA', '{"currency": "USD"}'),
    ('BR', 'Brazil', 'country', 'BRA', '{"currency": "BRL"}'),
    ('AR', 'Argentina', 'country', 'ARG', '{"currency": "ARS"}'),
    ('CN', 'China', 'country', 'CHN', '{"currency": "CNY"}'),
    ('EU', 'European Union', 'region', NULL, '{"note": "Economic region"}'),
    ('UA', 'Ukraine', 'country', 'UKR', '{"currency": "UAH"}'),
    ('RU', 'Russia', 'country', 'RUS', '{"currency": "RUB"}'),
    ('IN', 'India', 'country', 'IND', '{"currency": "INR"}'),
    ('AU', 'Australia', 'country', 'AUS', '{"currency": "AUD"}'),
    ('CA', 'Canada', 'country', 'CAN', '{"currency": "CAD"}'),
    ('MX', 'Mexico', 'country', 'MEX', '{"currency": "MXN"}')
ON CONFLICT (code) DO NOTHING;

-- US States (key agricultural states)
INSERT INTO core.location (code, name, location_type, iso_country, iso_subdivision, parent_id)
SELECT
    s.code, s.name, 'state', 'USA', 'US-' || s.code,
    (SELECT id FROM core.location WHERE code = 'US')
FROM (VALUES
    ('IA', 'Iowa'),
    ('IL', 'Illinois'),
    ('NE', 'Nebraska'),
    ('MN', 'Minnesota'),
    ('IN', 'Indiana'),
    ('OH', 'Ohio'),
    ('SD', 'South Dakota'),
    ('ND', 'North Dakota'),
    ('KS', 'Kansas'),
    ('MO', 'Missouri'),
    ('WI', 'Wisconsin'),
    ('MI', 'Michigan'),
    ('TX', 'Texas'),
    ('OK', 'Oklahoma'),
    ('MT', 'Montana'),
    ('CO', 'Colorado'),
    ('AR_STATE', 'Arkansas'),
    ('MS', 'Mississippi'),
    ('LA', 'Louisiana'),
    ('TN', 'Tennessee'),
    ('KY', 'Kentucky'),
    ('NC', 'North Carolina'),
    ('GA', 'Georgia'),
    ('AL', 'Alabama'),
    ('FL', 'Florida'),
    ('PA', 'Pennsylvania'),
    ('NY', 'New York'),
    ('CA_STATE', 'California'),
    ('WA', 'Washington'),
    ('OR', 'Oregon'),
    ('ID', 'Idaho')
) AS s(code, name)
ON CONFLICT (code) DO NOTHING;

-- Key regions
INSERT INTO core.location (code, name, location_type, iso_country, parent_id, metadata)
SELECT
    r.code, r.name, 'region', 'USA',
    (SELECT id FROM core.location WHERE code = 'US'),
    r.metadata::jsonb
FROM (VALUES
    ('US_CORN_BELT', 'US Corn Belt', '{"states": ["IA", "IL", "IN", "OH", "NE", "MN"]}'),
    ('US_DELTA', 'US Delta Region', '{"states": ["AR", "MS", "LA", "MO"]}'),
    ('US_GULF', 'US Gulf Coast', '{"states": ["TX", "LA", "MS", "AL", "FL"]}'),
    ('US_PNW', 'US Pacific Northwest', '{"states": ["WA", "OR", "ID"]}'),
    ('US_PLAINS', 'US Great Plains', '{"states": ["KS", "NE", "SD", "ND", "MT"]}')
) AS r(code, name, metadata)
ON CONFLICT (code) DO NOTHING;


-- ============================================================================
-- COMMODITY: Commodity reference table
-- ============================================================================
CREATE TABLE IF NOT EXISTS core.commodity (
    id              SERIAL PRIMARY KEY,
    code            VARCHAR(30) NOT NULL,
    name            VARCHAR(100) NOT NULL,
    commodity_type  VARCHAR(50) NOT NULL,  -- grain, oilseed, livestock, energy, etc.
    description     TEXT,
    primary_unit_id INTEGER REFERENCES core.unit(id),
    metadata        JSONB DEFAULT '{}',
    is_active       BOOLEAN NOT NULL DEFAULT TRUE,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT commodity_code_unique UNIQUE (code),
    CONSTRAINT commodity_type_valid CHECK (
        commodity_type IN ('grain', 'oilseed', 'livestock', 'dairy', 'poultry',
                           'energy', 'biofuel', 'cotton', 'sugar', 'other')
    )
);

COMMENT ON TABLE core.commodity IS
    'Reference table for commodities tracked by the system.';

-- Seed commodities
INSERT INTO core.commodity (code, name, commodity_type, description)
VALUES
    ('corn', 'Corn', 'grain', 'Yellow corn / maize'),
    ('soybeans', 'Soybeans', 'oilseed', 'Soybeans'),
    ('wheat', 'Wheat', 'grain', 'All wheat classes'),
    ('wheat_hrw', 'Hard Red Winter Wheat', 'grain', 'HRW wheat'),
    ('wheat_hrs', 'Hard Red Spring Wheat', 'grain', 'HRS wheat'),
    ('wheat_srw', 'Soft Red Winter Wheat', 'grain', 'SRW wheat'),
    ('sorghum', 'Sorghum', 'grain', 'Grain sorghum'),
    ('barley', 'Barley', 'grain', 'Barley'),
    ('oats', 'Oats', 'grain', 'Oats'),
    ('rice', 'Rice', 'grain', 'All rice'),
    ('cotton', 'Cotton', 'cotton', 'Upland cotton'),
    ('soyoil', 'Soybean Oil', 'oilseed', 'Soybean oil'),
    ('soymeal', 'Soybean Meal', 'oilseed', 'Soybean meal'),
    ('cattle', 'Cattle', 'livestock', 'Live cattle'),
    ('hogs', 'Hogs', 'livestock', 'Lean hogs'),
    ('ethanol', 'Ethanol', 'biofuel', 'Fuel ethanol'),
    ('biodiesel', 'Biodiesel', 'biofuel', 'Biodiesel'),
    ('crude_oil', 'Crude Oil', 'energy', 'WTI/Brent crude'),
    ('natural_gas', 'Natural Gas', 'energy', 'Henry Hub natural gas'),
    ('sugar', 'Sugar', 'sugar', 'Raw sugar')
ON CONFLICT (code) DO NOTHING;


-- ============================================================================
-- SERIES: Central metadata for all time-series
-- ============================================================================
CREATE TABLE IF NOT EXISTS core.series (
    id                  SERIAL PRIMARY KEY,
    data_source_id      INTEGER NOT NULL REFERENCES core.data_source(id),
    series_key          VARCHAR(255) NOT NULL,  -- Unique within data source
    name                VARCHAR(500) NOT NULL,
    description         TEXT,

    -- Classification
    commodity_id        INTEGER REFERENCES core.commodity(id),
    location_id         INTEGER REFERENCES core.location(id),
    unit_id             INTEGER REFERENCES core.unit(id),

    -- Time characteristics
    frequency           VARCHAR(20) NOT NULL,   -- daily, weekly, monthly, quarterly, annual
    start_date          DATE,
    end_date            DATE,                   -- NULL if ongoing

    -- Source reference
    source_series_id    VARCHAR(255),           -- Original ID from source (e.g., WASDE table/row)
    source_url          VARCHAR(1000),

    -- Behavior
    is_calculated       BOOLEAN DEFAULT FALSE,
    is_revised          BOOLEAN DEFAULT FALSE,  -- Does source publish revisions?
    revision_lag_days   INTEGER,                -- How long until revisions stop?

    -- Status
    is_active           BOOLEAN NOT NULL DEFAULT TRUE,

    -- Flexible metadata
    metadata            JSONB DEFAULT '{}',

    -- Timestamps
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    -- Natural key: unique within each data source
    CONSTRAINT series_source_key_unique UNIQUE (data_source_id, series_key),
    CONSTRAINT series_frequency_valid CHECK (
        frequency IN ('tick', 'minute', 'hourly', 'daily', 'weekly',
                      'monthly', 'quarterly', 'annual', 'irregular')
    )
);

COMMENT ON TABLE core.series IS
    'Central registry of all time-series tracked by the system. Each series has '
    'a unique (data_source_id, series_key) and maps to observations in silver layer.';

COMMENT ON COLUMN core.series.series_key IS
    'Unique identifier within the data source. Convention: '
    '{category}.{commodity}.{metric}.{geography}.{frequency}';
COMMENT ON COLUMN core.series.source_series_id IS
    'Original identifier from the data source (for traceability)';
COMMENT ON COLUMN core.series.is_revised IS
    'TRUE if source publishes revisions to historical values';
COMMENT ON COLUMN core.series.metadata IS
    'Source-specific attributes: wasde_table, cell_coordinates, notes';

-- Indexes for common queries
CREATE INDEX IF NOT EXISTS idx_series_source ON core.series(data_source_id);
CREATE INDEX IF NOT EXISTS idx_series_commodity ON core.series(commodity_id);
CREATE INDEX IF NOT EXISTS idx_series_location ON core.series(location_id);
CREATE INDEX IF NOT EXISTS idx_series_active ON core.series(is_active) WHERE is_active = TRUE;
CREATE INDEX IF NOT EXISTS idx_series_frequency ON core.series(frequency);
CREATE INDEX IF NOT EXISTS idx_series_key ON core.series(series_key);


-- ============================================================================
-- SERIES_TAG: Flexible tagging for series
-- ============================================================================
CREATE TABLE IF NOT EXISTS core.series_tag (
    id          SERIAL PRIMARY KEY,
    series_id   INTEGER NOT NULL REFERENCES core.series(id) ON DELETE CASCADE,
    tag_name    VARCHAR(100) NOT NULL,
    tag_value   VARCHAR(255),
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT series_tag_unique UNIQUE (series_id, tag_name)
);

COMMENT ON TABLE core.series_tag IS
    'Flexible key-value tags for series classification and filtering.';

-- Index for tag queries
CREATE INDEX IF NOT EXISTS idx_series_tag_name ON core.series_tag(tag_name);
CREATE INDEX IF NOT EXISTS idx_series_tag_value ON core.series_tag(tag_value);


-- ============================================================================
-- Verification
-- ============================================================================
DO $$
BEGIN
    RAISE NOTICE 'Core dimension tables created:';
    RAISE NOTICE '  - core.data_source: % rows', (SELECT COUNT(*) FROM core.data_source);
    RAISE NOTICE '  - core.unit: % rows', (SELECT COUNT(*) FROM core.unit);
    RAISE NOTICE '  - core.location: % rows', (SELECT COUNT(*) FROM core.location);
    RAISE NOTICE '  - core.commodity: % rows', (SELECT COUNT(*) FROM core.commodity);
    RAISE NOTICE '  - core.series: ready for population';
END $$;
