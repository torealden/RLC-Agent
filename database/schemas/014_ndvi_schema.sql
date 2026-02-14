-- =============================================================================
-- NDVI (Normalized Difference Vegetation Index) Schema
-- =============================================================================
-- Stores vegetation health indices for crop monitoring
-- Sources: USDA Crop Explorer, NASA MODIS via AppEEARS
-- =============================================================================

-- Bronze: Raw NDVI observations from various sources
CREATE TABLE IF NOT EXISTS bronze.ndvi_observation (
    id BIGSERIAL PRIMARY KEY,

    -- Geographic identification
    region_type VARCHAR(20) NOT NULL,  -- 'country', 'state', 'crop_region', 'custom'
    region_code VARCHAR(20) NOT NULL,  -- Country code (US, BR) or region ID
    region_name VARCHAR(100),

    -- Crop association (optional - for crop-specific NDVI)
    commodity VARCHAR(50),
    commodity_code VARCHAR(20),

    -- Temporal
    observation_date DATE NOT NULL,
    year INT NOT NULL,
    week_of_year INT,
    day_of_year INT,

    -- NDVI values (scaled 0-1 or -1 to 1 depending on source)
    ndvi_value NUMERIC(8,4),           -- Current NDVI
    ndvi_anomaly NUMERIC(8,4),         -- Deviation from normal
    ndvi_pct_of_normal NUMERIC(8,2),   -- Percentage of historical average

    -- Comparison values
    ndvi_prev_year NUMERIC(8,4),       -- Same period last year
    ndvi_5yr_avg NUMERIC(8,4),         -- 5-year average for period
    ndvi_10yr_avg NUMERIC(8,4),        -- 10-year average for period

    -- Quality indicators
    cloud_cover_pct NUMERIC(5,2),
    quality_flag VARCHAR(20),          -- 'good', 'marginal', 'poor'

    -- Source tracking
    source VARCHAR(50) NOT NULL,       -- 'usda_crop_explorer', 'nasa_modis', 'appeears'
    source_url TEXT,
    satellite VARCHAR(50),             -- 'MODIS', 'Sentinel-2', 'Landsat'
    resolution_m INT,                  -- Spatial resolution in meters

    -- Metadata
    collected_at TIMESTAMPTZ DEFAULT NOW(),
    ingest_run_id UUID,

    UNIQUE (region_code, commodity_code, observation_date, source)
);

-- Index for common queries
CREATE INDEX IF NOT EXISTS idx_ndvi_region_date
    ON bronze.ndvi_observation(region_code, observation_date DESC);
CREATE INDEX IF NOT EXISTS idx_ndvi_commodity_date
    ON bronze.ndvi_observation(commodity_code, observation_date DESC);
CREATE INDEX IF NOT EXISTS idx_ndvi_year_week
    ON bronze.ndvi_observation(year, week_of_year);

-- Bronze: NDVI chart images from USDA Crop Explorer
CREATE TABLE IF NOT EXISTS bronze.ndvi_chart (
    id BIGSERIAL PRIMARY KEY,

    -- Identification
    region_code VARCHAR(20) NOT NULL,
    commodity VARCHAR(50) NOT NULL,
    commodity_code VARCHAR(20) NOT NULL,
    chart_type VARCHAR(50) NOT NULL,   -- 'ndvi', 'ndvi_anomaly', 'precipitation', 'soil_moisture'

    -- Temporal
    year INT NOT NULL,
    chart_date DATE,                   -- Date chart was generated

    -- File info
    file_path TEXT NOT NULL,           -- Local path to saved image
    source_url TEXT NOT NULL,          -- Original URL
    file_size_bytes INT,

    -- Metadata
    collected_at TIMESTAMPTZ DEFAULT NOW(),

    UNIQUE (region_code, commodity_code, chart_type, year)
);

-- Silver: Processed NDVI with crop region aggregations
CREATE TABLE IF NOT EXISTS silver.ndvi_crop_region (
    id BIGSERIAL PRIMARY KEY,

    -- Region identification
    region_name VARCHAR(100) NOT NULL,  -- 'US Corn Belt', 'Brazil Mato Grosso', etc.
    region_code VARCHAR(20) NOT NULL,
    country_code VARCHAR(10) NOT NULL,

    -- Primary commodity for this region
    primary_commodity VARCHAR(50),

    -- Temporal
    observation_date DATE NOT NULL,
    year INT NOT NULL,
    week_of_year INT NOT NULL,

    -- Aggregated NDVI (area-weighted average across region)
    ndvi_mean NUMERIC(8,4),
    ndvi_median NUMERIC(8,4),
    ndvi_std_dev NUMERIC(8,4),
    ndvi_min NUMERIC(8,4),
    ndvi_max NUMERIC(8,4),

    -- Anomalies
    ndvi_anomaly NUMERIC(8,4),
    ndvi_pct_of_normal NUMERIC(8,2),

    -- Crop condition interpretation
    condition_rating VARCHAR(20),      -- 'excellent', 'good', 'fair', 'poor', 'very_poor'
    condition_score INT,               -- 1-5 scale

    -- Trend
    week_over_week_change NUMERIC(8,4),
    year_over_year_change NUMERIC(8,4),

    -- Source tracking
    source VARCHAR(50) NOT NULL,
    processed_at TIMESTAMPTZ DEFAULT NOW(),

    UNIQUE (region_code, observation_date)
);

-- Gold: NDVI analysis view for LLM queries
CREATE OR REPLACE VIEW gold.ndvi_crop_health AS
SELECT
    n.region_name,
    n.region_code,
    n.country_code,
    n.primary_commodity,
    n.observation_date,
    n.year,
    n.week_of_year,
    n.ndvi_mean,
    n.ndvi_anomaly,
    n.ndvi_pct_of_normal,
    n.condition_rating,
    n.week_over_week_change,
    n.year_over_year_change,
    CASE
        WHEN n.ndvi_pct_of_normal >= 105 THEN 'Above Normal - Favorable conditions'
        WHEN n.ndvi_pct_of_normal >= 95 THEN 'Normal - Average conditions'
        WHEN n.ndvi_pct_of_normal >= 85 THEN 'Below Normal - Some stress'
        WHEN n.ndvi_pct_of_normal >= 75 THEN 'Poor - Significant stress'
        ELSE 'Very Poor - Severe stress/drought'
    END AS condition_assessment,
    n.source,
    n.processed_at
FROM silver.ndvi_crop_region n
ORDER BY n.observation_date DESC, n.region_name;

-- Gold: Latest NDVI by key crop region
CREATE OR REPLACE VIEW gold.ndvi_latest_by_region AS
SELECT DISTINCT ON (region_code)
    region_name,
    region_code,
    country_code,
    primary_commodity,
    observation_date,
    ndvi_mean,
    ndvi_anomaly,
    ndvi_pct_of_normal,
    condition_rating,
    week_over_week_change,
    year_over_year_change
FROM silver.ndvi_crop_region
ORDER BY region_code, observation_date DESC;

-- Reference table for crop regions
CREATE TABLE IF NOT EXISTS reference.crop_region (
    id SERIAL PRIMARY KEY,
    region_code VARCHAR(20) UNIQUE NOT NULL,
    region_name VARCHAR(100) NOT NULL,
    country_code VARCHAR(10) NOT NULL,

    -- Primary crops grown
    primary_commodity VARCHAR(50),
    secondary_commodities TEXT[],      -- Array of other crops

    -- Geography
    bounding_box JSONB,                -- {min_lat, max_lat, min_lon, max_lon}
    states_provinces TEXT[],           -- Sub-regions included
    area_hectares NUMERIC(12,2),

    -- Growing season
    planting_start_month INT,
    planting_end_month INT,
    harvest_start_month INT,
    harvest_end_month INT,

    -- Importance
    pct_national_production NUMERIC(5,2),  -- What % of country's production

    -- USDA IDs for data retrieval
    usda_region_id VARCHAR(20),
    crop_explorer_id VARCHAR(20),

    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Insert key crop regions
INSERT INTO reference.crop_region (region_code, region_name, country_code, primary_commodity, secondary_commodities, states_provinces, planting_start_month, harvest_start_month, pct_national_production)
VALUES
    ('US_CORN_BELT', 'US Corn Belt', 'US', 'corn', ARRAY['soybeans'], ARRAY['IA', 'IL', 'NE', 'MN', 'IN', 'OH', 'SD', 'WI', 'MO', 'KS'], 4, 9, 75.0),
    ('US_SOY_BELT', 'US Soybean Belt', 'US', 'soybeans', ARRAY['corn'], ARRAY['IL', 'IA', 'MN', 'IN', 'NE', 'OH', 'MO', 'SD', 'ND', 'AR'], 5, 10, 80.0),
    ('US_WHEAT_WINTER', 'US Winter Wheat Belt', 'US', 'wheat', ARRAY['sorghum'], ARRAY['KS', 'OK', 'TX', 'CO', 'NE', 'MT'], 9, 6, 45.0),
    ('US_WHEAT_SPRING', 'US Spring Wheat Belt', 'US', 'wheat', ARRAY['barley'], ARRAY['ND', 'MT', 'MN', 'SD'], 4, 8, 25.0),
    ('BR_MATO_GROSSO', 'Brazil Mato Grosso', 'BR', 'soybeans', ARRAY['corn', 'cotton'], ARRAY['MT'], 9, 2, 28.0),
    ('BR_PARANA', 'Brazil Parana', 'BR', 'soybeans', ARRAY['corn', 'wheat'], ARRAY['PR'], 9, 3, 15.0),
    ('BR_RIO_GRANDE', 'Brazil Rio Grande do Sul', 'BR', 'soybeans', ARRAY['rice', 'wheat'], ARRAY['RS'], 10, 3, 12.0),
    ('BR_GOIAS', 'Brazil Goias', 'BR', 'soybeans', ARRAY['corn'], ARRAY['GO'], 9, 2, 10.0),
    ('AR_PAMPAS', 'Argentina Pampas', 'AR', 'soybeans', ARRAY['corn', 'wheat'], ARRAY['Buenos Aires', 'Santa Fe', 'Cordoba'], 10, 4, 85.0),
    ('UA_CENTRAL', 'Ukraine Central', 'UA', 'wheat', ARRAY['corn', 'sunflowerseed'], ARRAY['Kyiv', 'Poltava', 'Cherkasy'], 9, 7, 40.0),
    ('RU_SOUTHERN', 'Russia Southern District', 'RU', 'wheat', ARRAY['sunflowerseed', 'corn'], ARRAY['Krasnodar', 'Rostov', 'Stavropol'], 9, 7, 35.0),
    ('AU_EASTERN', 'Australia Eastern Wheat Belt', 'AU', 'wheat', ARRAY['barley'], ARRAY['NSW', 'QLD', 'VIC'], 5, 11, 60.0),
    ('IN_PUNJAB', 'India Punjab-Haryana', 'IN', 'wheat', ARRAY['rice'], ARRAY['Punjab', 'Haryana'], 11, 4, 35.0),
    ('CN_NORTHEAST', 'China Northeast', 'CN', 'corn', ARRAY['soybeans', 'rice'], ARRAY['Heilongjiang', 'Jilin', 'Liaoning'], 4, 9, 40.0)
ON CONFLICT (region_code) DO NOTHING;

-- Comments
COMMENT ON TABLE bronze.ndvi_observation IS 'Raw NDVI observations from satellite sources';
COMMENT ON TABLE bronze.ndvi_chart IS 'NDVI chart images from USDA Crop Explorer';
COMMENT ON TABLE silver.ndvi_crop_region IS 'Processed NDVI aggregated by crop region';
COMMENT ON VIEW gold.ndvi_crop_health IS 'NDVI analysis with crop condition assessment';
COMMENT ON TABLE reference.crop_region IS 'Reference data for major crop-producing regions';
