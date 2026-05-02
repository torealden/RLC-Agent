-- =============================================================================
-- Migration 046: basis_field — unified US basis observation + interpolation
--
-- Per the basis-field design (project_basis_field.md, 2026-05-01):
-- basis is a *property of economic geography*, not facilities. Build the
-- field once; facilities, prospects, and any agent plug in by sampling
-- it at their lat/lon.
--
-- Layered design:
--   1. reference.basis_region_centroid — maps AMS region names (e.g.
--      "North Central Iowa") to lat/lon centroids
--   2. bronze.cash_bid_observation — every observed bid as a sample point
--      with lat/lon, time, commodity, delivery month
--   3. silver.basis_field_grid — gridded interpolation (IDW initially,
--      kriging in v2) — one row per (date, commodity, delivery, grid_cell)
--   4. gold.facility_basis — convenience view: each facility's local basis
--      pulled from the grid
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS reference;
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

-- -----------------------------------------------------------------------------
-- 1. Region centroid mapping — translate AMS regional labels to coordinates
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS reference.basis_region_centroid (
    region_id            SERIAL PRIMARY KEY,
    source               TEXT NOT NULL,            -- 'AMS_2850' / 'AMS_3192' / etc.
    state                CHAR(2) NOT NULL,         -- two-letter
    region_name          TEXT NOT NULL,            -- e.g. 'North Central'
    delivery_point       TEXT,                     -- 'Country Elevators' / 'Barge' / etc.
    centroid_lat         NUMERIC NOT NULL,
    centroid_lon         NUMERIC NOT NULL,
    coverage_radius_mi   NUMERIC,                  -- approximate region radius
    counties             TEXT[],                   -- counties this region covers (informational)
    notes                TEXT,
    created_at           TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(source, state, region_name, delivery_point)
);

CREATE INDEX IF NOT EXISTS idx_basis_region_centroid_state
    ON reference.basis_region_centroid(state);

COMMENT ON TABLE reference.basis_region_centroid IS
    'Maps AMS regional bid labels (North Central IA, Mississippi River, etc.) '
    'to lat/lon centroids. Required to use AMS regional aggregate data as '
    'spatial samples in the basis field.';


-- -----------------------------------------------------------------------------
-- 2. Cash bid observations — raw spatial-temporal samples
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS bronze.cash_bid_observation (
    id                   BIGSERIAL PRIMARY KEY,
    observation_date     DATE NOT NULL,
    commodity            TEXT NOT NULL,            -- 'soybeans', 'corn', 'wheat', etc.
    delivery_month       TEXT,                     -- 'spot', 'K26', 'N26', 'X26', etc.
    grade                TEXT,                     -- 'US #2 Yellow' etc.

    -- Source
    source               TEXT NOT NULL,            -- 'ams' / 'agp_scrape' / 'state_doa' / etc.
    source_record_id     TEXT,                     -- original record id (e.g. ams_price_record.id)
    region_id            INTEGER REFERENCES reference.basis_region_centroid(region_id),

    -- Location
    lat                  NUMERIC NOT NULL,
    lon                  NUMERIC NOT NULL,
    location_label       TEXT NOT NULL,            -- human-readable (e.g. "AMS_2850 North Central IA")

    -- The actual numbers
    cash_price           NUMERIC,                  -- $/bu
    basis_cents          NUMERIC,                  -- cents over/under futures (negative = under)
    futures_settle       NUMERIC,                  -- futures price used for basis derivation
    futures_contract     TEXT,                     -- e.g. 'ZSK26'

    -- Quality flags
    is_indicative        BOOLEAN DEFAULT FALSE,    -- regional aggregate vs single-elevator
    sample_weight        NUMERIC DEFAULT 1.0,      -- field interpolation weight; aggregates < single-elevator

    collected_at         TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(observation_date, source, source_record_id, commodity, delivery_month)
);

CREATE INDEX IF NOT EXISTS idx_cash_bid_obs_date_commodity
    ON bronze.cash_bid_observation(observation_date, commodity);
CREATE INDEX IF NOT EXISTS idx_cash_bid_obs_location
    ON bronze.cash_bid_observation(lat, lon);
CREATE INDEX IF NOT EXISTS idx_cash_bid_obs_source
    ON bronze.cash_bid_observation(source);


-- -----------------------------------------------------------------------------
-- 3. Gridded basis field — interpolated daily snapshot
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS silver.basis_field_grid (
    id                   BIGSERIAL PRIMARY KEY,
    observation_date     DATE NOT NULL,
    commodity            TEXT NOT NULL,
    delivery_month       TEXT NOT NULL,            -- 'spot' / 'K26' / 'N26' / etc.

    -- Grid cell
    cell_lat             NUMERIC NOT NULL,         -- center lat of cell
    cell_lon             NUMERIC NOT NULL,
    grid_resolution_deg  NUMERIC NOT NULL,         -- e.g. 0.25 = ~17mi at IA latitude

    -- Interpolated value
    basis_cents          NUMERIC,                  -- predicted basis at this cell
    cash_price           NUMERIC,                  -- predicted cash price (basis + futures)
    std_err              NUMERIC,                  -- uncertainty (kriging variance)
    n_samples            INTEGER,                  -- number of obs samples within influence radius
    nearest_sample_mi    NUMERIC,                  -- distance to closest observed sample

    -- Provenance
    method               TEXT NOT NULL,            -- 'idw_v1' / 'kriging_v1' / etc.
    method_version       TEXT,
    inputs_hash          TEXT,                     -- sha256 of input observation set
    computed_at          TIMESTAMPTZ DEFAULT NOW(),

    UNIQUE(observation_date, commodity, delivery_month, cell_lat, cell_lon, method)
);

CREATE INDEX IF NOT EXISTS idx_basis_grid_date
    ON silver.basis_field_grid(observation_date, commodity, delivery_month);
CREATE INDEX IF NOT EXISTS idx_basis_grid_location
    ON silver.basis_field_grid(cell_lat, cell_lon);


-- -----------------------------------------------------------------------------
-- 4. Per-facility basis convenience view
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW gold.facility_basis AS
SELECT
    f.facility_id,
    f.name AS facility_name,
    f.lat AS facility_lat,
    f.lon AS facility_lon,
    g.observation_date,
    g.commodity,
    g.delivery_month,
    g.basis_cents,
    g.cash_price,
    g.std_err,
    g.n_samples,
    g.nearest_sample_mi,
    g.method,
    -- Distance from facility to grid cell center (rough Haversine)
    SQRT(POWER((f.lat - g.cell_lat)*69, 2) +
         POWER((f.lon - g.cell_lon)*COS(RADIANS(f.lat))*69, 2)) AS dist_to_cell_mi
FROM reference.oilseed_crush_facilities f
JOIN silver.basis_field_grid g
  ON ABS(f.lat - g.cell_lat) < g.grid_resolution_deg
 AND ABS(f.lon - g.cell_lon) < g.grid_resolution_deg
WHERE f.lat IS NOT NULL AND f.lon IS NOT NULL;

COMMENT ON VIEW gold.facility_basis IS
    'For each facility, the closest basis-field grid cell. Used by facility '
    'agents and the dashboard to get local basis without re-interpolating.';
