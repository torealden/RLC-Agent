-- =============================================================================
-- RLC Commodities Database Schema - Feed Grains Data Extension
-- Version: 1.0.0
-- =============================================================================
--
-- PURPOSE
-- -------
-- This migration adds support for:
-- 1. USDA ERS Feed Grains Yearbook data (prices, balance sheets, trade, industrial uses)
-- 2. USDA NASS Crop Production data (state-level acreage, yield, production)
-- 3. Census Bureau trade data for feed grains (monthly imports/exports)
--
-- DATA SOURCES
-- ------------
-- - ERS Feed Grains Database: https://www.ers.usda.gov/data-products/feed-grains-database/
-- - NASS Crop Production: https://www.nass.usda.gov/Publications/Reports_By_Date/
-- - Census Trade: https://usatrade.census.gov/
--
-- =============================================================================

-- =============================================================================
-- ADDITIONAL DATA SOURCES
-- =============================================================================

INSERT INTO public.data_source (code, name, description, api_type, update_frequency, base_url) VALUES
    ('USDA_ERS_FG', 'USDA ERS Feed Grains Database', 'Feed grains yearbook tables, prices, balance sheets', 'FILE', 'MONTHLY', 'https://www.ers.usda.gov/data-products/feed-grains-database/'),
    ('USDA_NASS_CROP', 'USDA NASS Crop Production', 'State-level crop production, acreage, and yield data', 'REST', 'MONTHLY', 'https://quickstats.nass.usda.gov/api/')
ON CONFLICT (code) DO UPDATE SET
    description = EXCLUDED.description,
    base_url = EXCLUDED.base_url,
    updated_at = NOW();

-- =============================================================================
-- ADDITIONAL COMMODITIES
-- =============================================================================

INSERT INTO public.commodity (code, name, category, default_unit_code, bushel_weight_lbs, marketing_year_start_month, hs_codes) VALUES
    ('SORGHUM', 'Sorghum', 'GRAIN', 'BU', 56.0, 9, ARRAY['1007']),
    ('BARLEY', 'Barley', 'GRAIN', 'BU', 48.0, 6, ARRAY['1003']),
    ('OATS', 'Oats', 'GRAIN', 'BU', 32.0, 6, ARRAY['1004']),
    ('HAY', 'Hay (All)', 'FORAGE', 'ST', NULL, 5, ARRAY['1214']),
    ('ALFALFA', 'Alfalfa Hay', 'FORAGE', 'ST', NULL, 5, ARRAY['121410']),
    ('CORN_SILAGE', 'Corn Silage', 'FORAGE', 'ST', NULL, 9, NULL),
    ('HFCS', 'High Fructose Corn Syrup', 'PROCESSED', 'LB', NULL, 9, ARRAY['170260']),
    ('CORN_STARCH', 'Corn Starch', 'PROCESSED', 'LB', NULL, 9, ARRAY['110812']),
    ('GLUCOSE', 'Glucose and Dextrose', 'PROCESSED', 'LB', NULL, 9, ARRAY['170230', '170240'])
ON CONFLICT (code) DO UPDATE SET
    name = EXCLUDED.name,
    bushel_weight_lbs = EXCLUDED.bushel_weight_lbs;

-- =============================================================================
-- US STATE LOCATIONS
-- =============================================================================

INSERT INTO public.location (code, name, location_type, parent_code, iso_alpha2) VALUES
    ('US_AL', 'Alabama', 'STATE', 'US', 'AL'),
    ('US_AK', 'Alaska', 'STATE', 'US', 'AK'),
    ('US_AZ', 'Arizona', 'STATE', 'US', 'AZ'),
    ('US_AR', 'Arkansas', 'STATE', 'US', 'AR'),
    ('US_CA', 'California', 'STATE', 'US', 'CA'),
    ('US_CO', 'Colorado', 'STATE', 'US', 'CO'),
    ('US_CT', 'Connecticut', 'STATE', 'US', 'CT'),
    ('US_DE', 'Delaware', 'STATE', 'US', 'DE'),
    ('US_FL', 'Florida', 'STATE', 'US', 'FL'),
    ('US_GA', 'Georgia', 'STATE', 'US', 'GA'),
    ('US_HI', 'Hawaii', 'STATE', 'US', 'HI'),
    ('US_ID', 'Idaho', 'STATE', 'US', 'ID'),
    ('US_IL', 'Illinois', 'STATE', 'US', 'IL'),
    ('US_IN', 'Indiana', 'STATE', 'US', 'IN'),
    ('US_IA', 'Iowa', 'STATE', 'US', 'IA'),
    ('US_KS', 'Kansas', 'STATE', 'US', 'KS'),
    ('US_KY', 'Kentucky', 'STATE', 'US', 'KY'),
    ('US_LA', 'Louisiana', 'STATE', 'US', 'LA'),
    ('US_ME', 'Maine', 'STATE', 'US', 'ME'),
    ('US_MD', 'Maryland', 'STATE', 'US', 'MD'),
    ('US_MA', 'Massachusetts', 'STATE', 'US', 'MA'),
    ('US_MI', 'Michigan', 'STATE', 'US', 'MI'),
    ('US_MN', 'Minnesota', 'STATE', 'US', 'MN'),
    ('US_MS', 'Mississippi', 'STATE', 'US', 'MS'),
    ('US_MO', 'Missouri', 'STATE', 'US', 'MO'),
    ('US_MT', 'Montana', 'STATE', 'US', 'MT'),
    ('US_NE', 'Nebraska', 'STATE', 'US', 'NE'),
    ('US_NV', 'Nevada', 'STATE', 'US', 'NV'),
    ('US_NH', 'New Hampshire', 'STATE', 'US', 'NH'),
    ('US_NJ', 'New Jersey', 'STATE', 'US', 'NJ'),
    ('US_NM', 'New Mexico', 'STATE', 'US', 'NM'),
    ('US_NY', 'New York', 'STATE', 'US', 'NY'),
    ('US_NC', 'North Carolina', 'STATE', 'US', 'NC'),
    ('US_ND', 'North Dakota', 'STATE', 'US', 'ND'),
    ('US_OH', 'Ohio', 'STATE', 'US', 'OH'),
    ('US_OK', 'Oklahoma', 'STATE', 'US', 'OK'),
    ('US_OR', 'Oregon', 'STATE', 'US', 'OR'),
    ('US_PA', 'Pennsylvania', 'STATE', 'US', 'PA'),
    ('US_RI', 'Rhode Island', 'STATE', 'US', 'RI'),
    ('US_SC', 'South Carolina', 'STATE', 'US', 'SC'),
    ('US_SD', 'South Dakota', 'STATE', 'US', 'SD'),
    ('US_TN', 'Tennessee', 'STATE', 'US', 'TN'),
    ('US_TX', 'Texas', 'STATE', 'US', 'TX'),
    ('US_UT', 'Utah', 'STATE', 'US', 'UT'),
    ('US_VT', 'Vermont', 'STATE', 'US', 'VT'),
    ('US_VA', 'Virginia', 'STATE', 'US', 'VA'),
    ('US_WA', 'Washington', 'STATE', 'US', 'WA'),
    ('US_WV', 'West Virginia', 'STATE', 'US', 'WV'),
    ('US_WI', 'Wisconsin', 'STATE', 'US', 'WI'),
    ('US_WY', 'Wyoming', 'STATE', 'US', 'WY')
ON CONFLICT (code) DO UPDATE SET
    name = EXCLUDED.name,
    parent_code = EXCLUDED.parent_code;

-- Additional trade partner countries
INSERT INTO public.location (code, name, location_type, iso_alpha2, iso_alpha3) VALUES
    ('GT', 'Guatemala', 'COUNTRY', 'GT', 'GTM'),
    ('HN', 'Honduras', 'COUNTRY', 'HN', 'HND'),
    ('CR', 'Costa Rica', 'COUNTRY', 'CR', 'CRI'),
    ('DO', 'Dominican Republic', 'COUNTRY', 'DO', 'DOM'),
    ('MA', 'Morocco', 'COUNTRY', 'MA', 'MAR'),
    ('SV', 'El Salvador', 'COUNTRY', 'SV', 'SLV'),
    ('PE', 'Peru', 'COUNTRY', 'PE', 'PER'),
    ('EC', 'Ecuador', 'COUNTRY', 'EC', 'ECU'),
    ('NI', 'Nicaragua', 'COUNTRY', 'NI', 'NIC'),
    ('PA', 'Panama', 'COUNTRY', 'PA', 'PAN'),
    ('JM', 'Jamaica', 'COUNTRY', 'JM', 'JAM'),
    ('SA', 'Saudi Arabia', 'COUNTRY', 'SA', 'SAU'),
    ('IL', 'Israel', 'COUNTRY', 'IL', 'ISR'),
    ('DZ', 'Algeria', 'COUNTRY', 'DZ', 'DZA'),
    ('TN', 'Tunisia', 'COUNTRY', 'TN', 'TUN')
ON CONFLICT (code) DO NOTHING;

-- =============================================================================
-- ADDITIONAL UNITS
-- =============================================================================

INSERT INTO public.unit (code, name, category, base_unit_code, to_base_factor, description) VALUES
    ('MBU', 'Million Bushels', 'VOLUME', 'BU', 1000000.0, 'Common for US balance sheets'),
    ('TBU', 'Thousand Bushels', 'VOLUME', 'BU', 1000.0, 'Common for trade data'),
    ('USD_ST', 'Dollars per Short Ton', 'CURRENCY', NULL, NULL, 'Price per short ton'),
    ('USD_LB', 'Dollars per Pound', 'CURRENCY', NULL, NULL, 'Price per pound'),
    ('TAA', 'Thousand Acres', 'AREA', 'ACRE', 1000.0, 'Common for state acreage')
ON CONFLICT (code) DO NOTHING;

-- =============================================================================
-- BRONZE LAYER: ERS Feed Grains Yearbook Raw Data
-- =============================================================================

-- -----------------------------------------------------------------------------
-- ERS Feed Grains Yearbook Raw: One row per cell from yearbook tables
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS bronze.ers_feed_grains_raw (
    id BIGSERIAL PRIMARY KEY,

    -- Source identification
    table_number VARCHAR(10) NOT NULL,       -- 'Table01', 'Table09', etc.
    table_name VARCHAR(500) NOT NULL,        -- Full table title

    -- Row/column identification
    commodity VARCHAR(100),                   -- 'Corn', 'Sorghum', etc.
    row_label VARCHAR(300) NOT NULL,         -- Row description
    column_label VARCHAR(100) NOT NULL,      -- Column header

    -- Time identification
    marketing_year VARCHAR(20),              -- '2024/25' format
    calendar_year INT,                       -- For annual data
    quarter VARCHAR(20),                     -- 'Q1 Sep-Nov', 'MY Sep-Aug', etc.
    month VARCHAR(20),                       -- 'January', 'February', etc.

    -- Values
    raw_value VARCHAR(100),                  -- Original text value
    numeric_value NUMERIC(20, 6),            -- Parsed numeric
    unit_text VARCHAR(50),                   -- Unit from source

    -- Location context
    geography VARCHAR(200),                  -- 'United States', state name, or country
    market_location VARCHAR(200),            -- For cash prices: 'Central Illinois', 'Gulf'

    -- Tracking
    source_file VARCHAR(200),
    source_sheet VARCHAR(100),
    ingest_run_id UUID REFERENCES audit.ingest_run(id),

    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),

    -- Natural key for idempotent upserts
    UNIQUE (table_number, commodity, row_label, column_label,
            COALESCE(marketing_year, ''), COALESCE(quarter, ''),
            COALESCE(month, ''), COALESCE(geography, ''))
);

CREATE INDEX idx_ers_fg_table ON bronze.ers_feed_grains_raw(table_number);
CREATE INDEX idx_ers_fg_commodity ON bronze.ers_feed_grains_raw(commodity);
CREATE INDEX idx_ers_fg_my ON bronze.ers_feed_grains_raw(marketing_year);
CREATE INDEX idx_ers_fg_year ON bronze.ers_feed_grains_raw(calendar_year);

COMMENT ON TABLE bronze.ers_feed_grains_raw IS 'Raw data from ERS Feed Grains Yearbook Excel tables';

-- -----------------------------------------------------------------------------
-- NASS Crop Production Raw: State-level crop data
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS bronze.nass_crop_production_raw (
    id BIGSERIAL PRIMARY KEY,

    -- Natural key components
    crop_year INT NOT NULL,
    state_name VARCHAR(100) NOT NULL,
    commodity VARCHAR(100) NOT NULL,
    statistic_type VARCHAR(100) NOT NULL,    -- 'AREA PLANTED', 'AREA HARVESTED', 'YIELD', 'PRODUCTION'

    -- Values
    raw_value VARCHAR(100),
    numeric_value NUMERIC(20, 4),
    unit_text VARCHAR(50),

    -- Additional attributes
    practice_type VARCHAR(50),               -- 'ALL', 'IRRIGATED', 'NON-IRRIGATED'
    utilization_type VARCHAR(50),            -- 'GRAIN', 'SILAGE', 'ALL PURPOSES'

    -- Source tracking
    source_report VARCHAR(200),              -- 'Crop Production Annual Summary'
    report_date DATE,

    -- Tracking
    ingest_run_id UUID REFERENCES audit.ingest_run(id),

    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),

    -- Natural key
    UNIQUE (crop_year, state_name, commodity, statistic_type,
            COALESCE(practice_type, 'ALL'), COALESCE(utilization_type, 'ALL'))
);

CREATE INDEX idx_nass_crop_year ON bronze.nass_crop_production_raw(crop_year);
CREATE INDEX idx_nass_crop_state ON bronze.nass_crop_production_raw(state_name);
CREATE INDEX idx_nass_crop_commodity ON bronze.nass_crop_production_raw(commodity);

COMMENT ON TABLE bronze.nass_crop_production_raw IS 'Raw state-level crop production data from NASS';

-- -----------------------------------------------------------------------------
-- Census Trade Monthly Raw: Monthly trade by country (for scraping)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS bronze.census_trade_monthly (
    id BIGSERIAL PRIMARY KEY,

    -- Natural key
    year INT NOT NULL,
    month INT NOT NULL,
    flow VARCHAR(10) NOT NULL,               -- 'EXPORT', 'IMPORT'
    hs_code VARCHAR(10) NOT NULL,
    partner_code VARCHAR(20) NOT NULL,       -- Census partner code or 'WORLD'

    -- Partner info
    partner_name VARCHAR(200),

    -- Values (as reported)
    quantity NUMERIC(20, 4),
    quantity_unit VARCHAR(50),               -- 'MT', 'KG', etc.
    value_usd NUMERIC(20, 2),

    -- Mapped commodity
    commodity_code VARCHAR(30),

    -- Source tracking
    query_timestamp TIMESTAMPTZ,
    ingest_run_id UUID REFERENCES audit.ingest_run(id),

    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),

    -- Natural key
    UNIQUE (year, month, flow, hs_code, partner_code)
);

CREATE INDEX idx_census_monthly_date ON bronze.census_trade_monthly(year, month);
CREATE INDEX idx_census_monthly_flow ON bronze.census_trade_monthly(flow);
CREATE INDEX idx_census_monthly_hs ON bronze.census_trade_monthly(hs_code);
CREATE INDEX idx_census_monthly_commodity ON bronze.census_trade_monthly(commodity_code);

COMMENT ON TABLE bronze.census_trade_monthly IS 'Monthly trade data from Census Bureau for price monitoring';

-- =============================================================================
-- SILVER LAYER: Normalized Feed Grains Data
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Crop Production: Normalized state-level production data
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS silver.crop_production (
    id BIGSERIAL PRIMARY KEY,

    -- Dimensions
    commodity_code VARCHAR(30) NOT NULL REFERENCES public.commodity(code),
    location_code VARCHAR(50) NOT NULL REFERENCES public.location(code),
    crop_year INT NOT NULL,

    -- Production metrics (NULL if not applicable)
    area_planted_acres NUMERIC(15, 2),
    area_harvested_acres NUMERIC(15, 2),
    yield_per_acre NUMERIC(12, 4),
    production NUMERIC(18, 4),
    production_unit_code VARCHAR(30) REFERENCES public.unit(code),

    -- Additional context
    utilization_type VARCHAR(50) DEFAULT 'GRAIN',  -- 'GRAIN', 'SILAGE', 'ALL'
    practice_type VARCHAR(50) DEFAULT 'ALL',       -- 'IRRIGATED', 'NON-IRRIGATED', 'ALL'

    -- Tracking
    data_source VARCHAR(50) DEFAULT 'USDA_NASS',
    report_date DATE,
    ingest_run_id UUID REFERENCES audit.ingest_run(id),

    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),

    -- Natural key
    UNIQUE (commodity_code, location_code, crop_year, utilization_type, practice_type)
);

CREATE INDEX idx_crop_prod_commodity ON silver.crop_production(commodity_code);
CREATE INDEX idx_crop_prod_location ON silver.crop_production(location_code);
CREATE INDEX idx_crop_prod_year ON silver.crop_production(crop_year);

COMMENT ON TABLE silver.crop_production IS 'Normalized crop production by state/year';

-- -----------------------------------------------------------------------------
-- Farm Price: Prices received by farmers (monthly)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS silver.farm_price (
    id BIGSERIAL PRIMARY KEY,

    -- Dimensions
    commodity_code VARCHAR(30) NOT NULL REFERENCES public.commodity(code),
    location_code VARCHAR(50) NOT NULL REFERENCES public.location(code),

    -- Time
    marketing_year VARCHAR(10) NOT NULL,
    price_month INT,                          -- 1-12 (NULL for annual)
    price_date DATE,                          -- First of month

    -- Price
    price NUMERIC(12, 4) NOT NULL,
    unit_code VARCHAR(30) REFERENCES public.unit(code),

    -- Type
    price_type VARCHAR(50) DEFAULT 'RECEIVED',  -- 'RECEIVED', 'LOAN_RATE'
    is_annual_average BOOLEAN DEFAULT FALSE,

    -- Tracking
    data_source VARCHAR(50) DEFAULT 'USDA_ERS',
    ingest_run_id UUID REFERENCES audit.ingest_run(id),

    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),

    -- Natural key
    UNIQUE (commodity_code, location_code, marketing_year, COALESCE(price_month, 0), price_type)
);

CREATE INDEX idx_farm_price_commodity ON silver.farm_price(commodity_code);
CREATE INDEX idx_farm_price_my ON silver.farm_price(marketing_year);
CREATE INDEX idx_farm_price_date ON silver.farm_price(price_date);

COMMENT ON TABLE silver.farm_price IS 'Monthly/annual prices received by farmers';

-- -----------------------------------------------------------------------------
-- Cash Price: Market cash prices (daily/monthly)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS silver.cash_price (
    id BIGSERIAL PRIMARY KEY,

    -- Dimensions
    commodity_code VARCHAR(30) NOT NULL REFERENCES public.commodity(code),
    market_location VARCHAR(200) NOT NULL,    -- 'Central Illinois', 'Gulf', 'Kansas City'
    price_basis VARCHAR(50),                  -- 'No. 2 Yellow', 'No. 1 Milling', etc.

    -- Time
    price_date DATE NOT NULL,
    marketing_year VARCHAR(10),
    price_month INT,

    -- Prices
    price NUMERIC(12, 4) NOT NULL,
    price_low NUMERIC(12, 4),
    price_high NUMERIC(12, 4),
    unit_code VARCHAR(30) REFERENCES public.unit(code),

    -- Type flags
    is_monthly_average BOOLEAN DEFAULT FALSE,
    is_annual_average BOOLEAN DEFAULT FALSE,

    -- Tracking
    data_source VARCHAR(50) DEFAULT 'USDA_ERS',
    ingest_run_id UUID REFERENCES audit.ingest_run(id),

    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),

    -- Natural key
    UNIQUE (commodity_code, market_location, price_date, COALESCE(price_basis, ''))
);

CREATE INDEX idx_cash_price_commodity ON silver.cash_price(commodity_code);
CREATE INDEX idx_cash_price_market ON silver.cash_price(market_location);
CREATE INDEX idx_cash_price_date ON silver.cash_price(price_date);
CREATE INDEX idx_cash_price_my ON silver.cash_price(marketing_year);

COMMENT ON TABLE silver.cash_price IS 'Daily/monthly cash prices at principal markets';

-- -----------------------------------------------------------------------------
-- Industrial Use: Corn industrial usage breakdown
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS silver.industrial_use (
    id BIGSERIAL PRIMARY KEY,

    -- Dimensions
    commodity_code VARCHAR(30) NOT NULL REFERENCES public.commodity(code),
    use_category VARCHAR(100) NOT NULL,       -- 'HFCS', 'GLUCOSE_DEXTROSE', 'STARCH', 'FUEL_ALCOHOL', 'BEVERAGE_ALCOHOL', 'CEREALS', 'SEED'

    -- Time
    marketing_year VARCHAR(10) NOT NULL,
    quarter VARCHAR(20),                      -- 'Q1', 'Q2', 'Q3', 'Q4', 'MY' (full year)

    -- Value
    quantity NUMERIC(18, 4) NOT NULL,
    unit_code VARCHAR(30) REFERENCES public.unit(code),

    -- Tracking
    data_source VARCHAR(50) DEFAULT 'USDA_ERS',
    ingest_run_id UUID REFERENCES audit.ingest_run(id),

    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),

    -- Natural key
    UNIQUE (commodity_code, use_category, marketing_year, COALESCE(quarter, 'MY'))
);

CREATE INDEX idx_industrial_use_commodity ON silver.industrial_use(commodity_code);
CREATE INDEX idx_industrial_use_category ON silver.industrial_use(use_category);
CREATE INDEX idx_industrial_use_my ON silver.industrial_use(marketing_year);

COMMENT ON TABLE silver.industrial_use IS 'Corn food/seed/industrial use breakdown by category';

-- -----------------------------------------------------------------------------
-- Feed Price Ratio: Livestock feed-price ratios
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS silver.feed_price_ratio (
    id BIGSERIAL PRIMARY KEY,

    -- Dimensions
    ratio_type VARCHAR(100) NOT NULL,         -- 'HOG_CORN', 'CATTLE_CORN', 'EGG_FEED', 'BROILER_FEED', 'MILK_FEED'

    -- Time
    price_date DATE NOT NULL,
    marketing_year VARCHAR(10),
    price_month INT,

    -- Value
    ratio_value NUMERIC(12, 4) NOT NULL,

    -- Tracking
    data_source VARCHAR(50) DEFAULT 'USDA_ERS',
    ingest_run_id UUID REFERENCES audit.ingest_run(id),

    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),

    -- Natural key
    UNIQUE (ratio_type, price_date)
);

CREATE INDEX idx_feed_ratio_type ON silver.feed_price_ratio(ratio_type);
CREATE INDEX idx_feed_ratio_date ON silver.feed_price_ratio(price_date);

COMMENT ON TABLE silver.feed_price_ratio IS 'Feed-price ratios for livestock, poultry, milk';

-- =============================================================================
-- GOLD LAYER: Aggregated Views
-- =============================================================================

-- -----------------------------------------------------------------------------
-- View: Latest Farm Prices by Commodity
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW gold.latest_farm_prices AS
SELECT
    fp.commodity_code,
    c.name AS commodity_name,
    fp.location_code,
    fp.marketing_year,
    fp.price_month,
    fp.price,
    fp.unit_code,
    fp.price_date
FROM silver.farm_price fp
JOIN public.commodity c ON fp.commodity_code = c.code
WHERE fp.is_annual_average = FALSE
  AND (fp.commodity_code, fp.location_code, fp.price_date) IN (
      SELECT commodity_code, location_code, MAX(price_date)
      FROM silver.farm_price
      WHERE is_annual_average = FALSE
      GROUP BY commodity_code, location_code
  );

COMMENT ON VIEW gold.latest_farm_prices IS 'Most recent monthly farm prices by commodity';

-- -----------------------------------------------------------------------------
-- View: US Crop Production Summary
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW gold.us_crop_production_summary AS
SELECT
    cp.crop_year,
    cp.commodity_code,
    c.name AS commodity_name,
    SUM(cp.area_planted_acres) AS total_area_planted,
    SUM(cp.area_harvested_acres) AS total_area_harvested,
    CASE
        WHEN SUM(cp.area_harvested_acres) > 0
        THEN SUM(cp.production) / SUM(cp.area_harvested_acres)
        ELSE NULL
    END AS national_yield,
    SUM(cp.production) AS total_production,
    cp.production_unit_code
FROM silver.crop_production cp
JOIN public.commodity c ON cp.commodity_code = c.code
WHERE cp.utilization_type = 'GRAIN'
  AND cp.location_code LIKE 'US_%'  -- State-level data
GROUP BY cp.crop_year, cp.commodity_code, c.name, cp.production_unit_code
ORDER BY cp.crop_year DESC, cp.commodity_code;

COMMENT ON VIEW gold.us_crop_production_summary IS 'National crop production summary by year';

-- -----------------------------------------------------------------------------
-- View: State Crop Production Rankings
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW gold.state_crop_rankings AS
WITH ranked AS (
    SELECT
        cp.crop_year,
        cp.commodity_code,
        cp.location_code,
        l.name AS state_name,
        cp.production,
        cp.area_harvested_acres,
        cp.yield_per_acre,
        RANK() OVER (
            PARTITION BY cp.crop_year, cp.commodity_code
            ORDER BY cp.production DESC NULLS LAST
        ) AS production_rank
    FROM silver.crop_production cp
    JOIN public.location l ON cp.location_code = l.code
    WHERE cp.utilization_type = 'GRAIN'
      AND cp.location_code LIKE 'US_%'
)
SELECT * FROM ranked
WHERE production_rank <= 10;

COMMENT ON VIEW gold.state_crop_rankings IS 'Top 10 producing states by commodity and year';

-- -----------------------------------------------------------------------------
-- View: Monthly Price Comparison
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW gold.monthly_price_comparison AS
SELECT
    fp.marketing_year,
    fp.price_month,
    fp.commodity_code,
    c.name AS commodity_name,
    fp.price AS farm_price,
    cp.price AS cash_price_central_il,
    cp.price - fp.price AS basis_estimate
FROM silver.farm_price fp
JOIN public.commodity c ON fp.commodity_code = c.code
LEFT JOIN silver.cash_price cp ON
    fp.commodity_code = cp.commodity_code
    AND fp.marketing_year = cp.marketing_year
    AND fp.price_month = cp.price_month
    AND cp.market_location = 'Central Illinois'
WHERE fp.location_code = 'US'
  AND fp.is_annual_average = FALSE;

COMMENT ON VIEW gold.monthly_price_comparison IS 'Compare farm prices to cash market prices';

-- =============================================================================
-- HELPER FUNCTIONS
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Function: Parse marketing year to start/end dates
-- -----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION public.parse_marketing_year(
    p_marketing_year VARCHAR,
    p_commodity_code VARCHAR DEFAULT 'CORN'
) RETURNS TABLE (start_date DATE, end_date DATE) AS $$
DECLARE
    v_start_year INT;
    v_start_month INT;
BEGIN
    -- Extract start year from format like '2024/25'
    v_start_year := SUBSTRING(p_marketing_year FROM 1 FOR 4)::INT;

    -- Get marketing year start month from commodity
    SELECT COALESCE(marketing_year_start_month, 9) INTO v_start_month
    FROM public.commodity WHERE code = p_commodity_code;

    start_date := make_date(v_start_year, v_start_month, 1);
    end_date := (start_date + INTERVAL '1 year' - INTERVAL '1 day')::DATE;

    RETURN NEXT;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION public.parse_marketing_year IS 'Convert marketing year string to date range';

-- -----------------------------------------------------------------------------
-- Function: Get state code from state name
-- -----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION public.get_state_code(p_state_name VARCHAR)
RETURNS VARCHAR AS $$
DECLARE
    v_code VARCHAR;
BEGIN
    SELECT code INTO v_code
    FROM public.location
    WHERE location_type = 'STATE'
      AND (
          LOWER(name) = LOWER(TRIM(p_state_name))
          OR LOWER(REPLACE(name, ' ', '')) = LOWER(REPLACE(TRIM(p_state_name), ' ', ''))
      );

    RETURN v_code;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION public.get_state_code IS 'Lookup state code by name (case-insensitive)';

-- =============================================================================
-- END OF FEED GRAINS DATA MIGRATION
-- =============================================================================
