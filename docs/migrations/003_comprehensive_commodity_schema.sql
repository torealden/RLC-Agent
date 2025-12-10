-- ============================================================================
-- COMPREHENSIVE COMMODITY DATABASE SCHEMA
-- Migration: 003_comprehensive_commodity_schema.sql
--
-- This migration creates a flexible, comprehensive schema for storing
-- agricultural commodity data from all sources including:
-- - Trade flows (imports/exports)
-- - Supply/Demand fundamentals
-- - Futures prices and settlements
-- - Production and crush data
-- - Weather/drought conditions
-- - Biofuel feedstocks and co-products
-- - Positioning data (COT)
-- ============================================================================

-- ============================================================================
-- CORE REFERENCE TABLES
-- ============================================================================

-- Master commodity reference table
CREATE TABLE IF NOT EXISTS commodities (
    id SERIAL PRIMARY KEY,
    code VARCHAR(20) UNIQUE NOT NULL,          -- Internal code (e.g., 'ZC', 'CORN')
    name VARCHAR(100) NOT NULL,                 -- Display name
    category VARCHAR(50) NOT NULL,              -- 'grains', 'oilseeds', 'energy', 'biofuels', etc.
    subcategory VARCHAR(50),                    -- 'feed_grains', 'food_grains', etc.
    hs_code VARCHAR(10),                        -- Harmonized System code for trade
    cbot_symbol VARCHAR(10),                    -- CBOT futures symbol
    contract_size NUMERIC,                      -- Futures contract size
    contract_unit VARCHAR(20),                  -- 'bushels', 'barrels', 'gallons'
    price_unit VARCHAR(30),                     -- 'cents/bushel', '$/barrel'
    marketing_year_start INTEGER,               -- Month MY starts (1-12)
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Insert common commodities
INSERT INTO commodities (code, name, category, subcategory, hs_code, cbot_symbol, contract_size, contract_unit, price_unit, marketing_year_start)
VALUES
    -- Feed Grains
    ('CORN', 'Corn', 'grains', 'feed_grains', '1005', 'ZC', 5000, 'bushels', 'cents/bushel', 9),
    ('SORGHUM', 'Grain Sorghum', 'grains', 'feed_grains', '1007', NULL, 5000, 'bushels', 'cents/bushel', 9),
    ('BARLEY', 'Barley', 'grains', 'feed_grains', '1003', NULL, 5000, 'bushels', 'cents/bushel', 6),
    ('OATS', 'Oats', 'grains', 'feed_grains', '1004', 'ZO', 5000, 'bushels', 'cents/bushel', 6),

    -- Food Grains / Wheat
    ('WHEAT_HRW', 'Hard Red Winter Wheat', 'grains', 'food_grains', '1001', 'KE', 5000, 'bushels', 'cents/bushel', 6),
    ('WHEAT_HRS', 'Hard Red Spring Wheat', 'grains', 'food_grains', '1001', 'MWE', 5000, 'bushels', 'cents/bushel', 6),
    ('WHEAT_SRW', 'Soft Red Winter Wheat', 'grains', 'food_grains', '1001', 'ZW', 5000, 'bushels', 'cents/bushel', 6),
    ('WHEAT_DURUM', 'Durum Wheat', 'grains', 'food_grains', '100110', NULL, 5000, 'bushels', 'cents/bushel', 6),
    ('WHEAT_WHITE', 'White Wheat', 'grains', 'food_grains', '1001', NULL, 5000, 'bushels', 'cents/bushel', 6),

    -- Oilseeds
    ('SOYBEANS', 'Soybeans', 'oilseeds', 'oilseeds', '1201', 'ZS', 5000, 'bushels', 'cents/bushel', 9),
    ('CANOLA', 'Canola/Rapeseed', 'oilseeds', 'oilseeds', '1205', NULL, 20, 'metric tons', '$/metric ton', 8),
    ('SUNFLOWER', 'Sunflower Seed', 'oilseeds', 'oilseeds', '1206', NULL, NULL, 'pounds', 'cents/pound', 9),

    -- Vegetable Oils
    ('SOYBEAN_OIL', 'Soybean Oil', 'oils', 'vegetable_oils', '1507', 'ZL', 60000, 'pounds', 'cents/pound', 10),
    ('CANOLA_OIL', 'Canola Oil', 'oils', 'vegetable_oils', '1514', NULL, NULL, 'pounds', 'cents/pound', 8),
    ('PALM_OIL', 'Palm Oil', 'oils', 'vegetable_oils', '1511', 'CPO', 25, 'metric tons', '$/metric ton', 1),
    ('COCONUT_OIL', 'Coconut Oil', 'oils', 'vegetable_oils', '1513', NULL, NULL, 'pounds', 'cents/pound', 1),
    ('SUNFLOWER_OIL', 'Sunflower Oil', 'oils', 'vegetable_oils', '1512', NULL, NULL, 'pounds', 'cents/pound', 9),

    -- Meals
    ('SOYBEAN_MEAL', 'Soybean Meal', 'meals', 'protein_meals', '2304', 'ZM', 100, 'short tons', '$/short ton', 10),
    ('CANOLA_MEAL', 'Canola Meal', 'meals', 'protein_meals', '2306', NULL, NULL, 'short tons', '$/short ton', 8),
    ('SUNFLOWER_MEAL', 'Sunflower Meal', 'meals', 'protein_meals', '2306', NULL, NULL, 'short tons', '$/short ton', 9),

    -- Biofuels
    ('ETHANOL', 'Fuel Ethanol', 'biofuels', 'ethanol', '2207', 'EH', 29000, 'gallons', '$/gallon', 1),
    ('BIODIESEL', 'Biodiesel (FAME)', 'biofuels', 'biodiesel', '382600', NULL, NULL, 'gallons', '$/gallon', 1),
    ('RENEWABLE_DIESEL', 'Renewable Diesel (HVO)', 'biofuels', 'renewable_diesel', NULL, NULL, NULL, 'gallons', '$/gallon', 1),
    ('SAF', 'Sustainable Aviation Fuel', 'biofuels', 'saf', NULL, NULL, NULL, 'gallons', '$/gallon', 1),

    -- Biofuel Feedstocks
    ('YELLOW_GREASE', 'Yellow Grease', 'feedstocks', 'used_cooking_oil', NULL, NULL, NULL, 'pounds', 'cents/pound', 1),
    ('CHOICE_WHITE_GREASE', 'Choice White Grease', 'feedstocks', 'rendered_fats', NULL, NULL, NULL, 'pounds', 'cents/pound', 1),
    ('TALLOW', 'Beef Tallow', 'feedstocks', 'rendered_fats', NULL, NULL, NULL, 'pounds', 'cents/pound', 1),
    ('LARD', 'Lard', 'feedstocks', 'rendered_fats', NULL, NULL, NULL, 'pounds', 'cents/pound', 1),
    ('POULTRY_FAT', 'Poultry Fat', 'feedstocks', 'rendered_fats', NULL, NULL, NULL, 'pounds', 'cents/pound', 1),
    ('UCO', 'Used Cooking Oil', 'feedstocks', 'used_cooking_oil', NULL, NULL, NULL, 'pounds', 'cents/pound', 1),

    -- Energy
    ('CRUDE_WTI', 'WTI Crude Oil', 'energy', 'crude_oil', '2709', 'CL', 1000, 'barrels', '$/barrel', 1),
    ('CRUDE_BRENT', 'Brent Crude Oil', 'energy', 'crude_oil', '2709', 'BZ', 1000, 'barrels', '$/barrel', 1),
    ('ULSD', 'Ultra Low Sulfur Diesel', 'energy', 'refined_products', '2710', 'HO', 42000, 'gallons', '$/gallon', 1),
    ('RBOB', 'RBOB Gasoline', 'energy', 'refined_products', '2710', 'RB', 42000, 'gallons', '$/gallon', 1),
    ('JET_FUEL', 'Jet Fuel', 'energy', 'refined_products', '271019', NULL, NULL, 'gallons', '$/gallon', 1),
    ('NATURAL_GAS', 'Natural Gas', 'energy', 'natural_gas', '2711', 'NG', 10000, 'MMBtu', '$/MMBtu', 1),

    -- Co-products
    ('DDGS', 'Distillers Dried Grains w/Solubles', 'co_products', 'ethanol_co_products', '2303', NULL, NULL, 'short tons', '$/short ton', 9),
    ('DCO', 'Distillers Corn Oil', 'co_products', 'ethanol_co_products', NULL, NULL, NULL, 'pounds', 'cents/pound', 9),
    ('GLYCERIN', 'Glycerin', 'co_products', 'biodiesel_co_products', '2905', NULL, NULL, 'pounds', 'cents/pound', 1),
    ('CORN_GLUTEN_FEED', 'Corn Gluten Feed', 'co_products', 'wet_milling', '2303', NULL, NULL, 'short tons', '$/short ton', 9),
    ('CORN_GLUTEN_MEAL', 'Corn Gluten Meal', 'co_products', 'wet_milling', '2303', NULL, NULL, 'short tons', '$/short ton', 9)
ON CONFLICT (code) DO NOTHING;

-- Countries/regions reference
CREATE TABLE IF NOT EXISTS countries (
    id SERIAL PRIMARY KEY,
    code VARCHAR(10) UNIQUE NOT NULL,          -- ISO code or custom
    name VARCHAR(100) NOT NULL,
    region VARCHAR(50),                         -- 'north_america', 'south_america', 'europe', etc.
    iso2 CHAR(2),
    iso3 CHAR(3),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Insert major trading countries
INSERT INTO countries (code, name, region, iso2, iso3) VALUES
    ('USA', 'United States', 'north_america', 'US', 'USA'),
    ('CAN', 'Canada', 'north_america', 'CA', 'CAN'),
    ('MEX', 'Mexico', 'north_america', 'MX', 'MEX'),
    ('BRA', 'Brazil', 'south_america', 'BR', 'BRA'),
    ('ARG', 'Argentina', 'south_america', 'AR', 'ARG'),
    ('CHN', 'China', 'asia', 'CN', 'CHN'),
    ('JPN', 'Japan', 'asia', 'JP', 'JPN'),
    ('KOR', 'South Korea', 'asia', 'KR', 'KOR'),
    ('TWN', 'Taiwan', 'asia', 'TW', 'TWN'),
    ('IDN', 'Indonesia', 'asia', 'ID', 'IDN'),
    ('MYS', 'Malaysia', 'asia', 'MY', 'MYS'),
    ('EU', 'European Union', 'europe', NULL, NULL),
    ('UKR', 'Ukraine', 'europe', 'UA', 'UKR'),
    ('RUS', 'Russia', 'europe', 'RU', 'RUS'),
    ('AUS', 'Australia', 'oceania', 'AU', 'AUS'),
    ('IND', 'India', 'asia', 'IN', 'IND'),
    ('EGY', 'Egypt', 'africa', 'EG', 'EGY'),
    ('NGA', 'Nigeria', 'africa', 'NG', 'NGA'),
    ('WORLD', 'World Total', 'global', NULL, NULL)
ON CONFLICT (code) DO NOTHING;

-- Data sources reference
CREATE TABLE IF NOT EXISTS data_sources (
    id SERIAL PRIMARY KEY,
    code VARCHAR(50) UNIQUE NOT NULL,
    name VARCHAR(100) NOT NULL,
    source_type VARCHAR(30),                    -- 'government', 'exchange', 'private', 'international'
    country_code VARCHAR(10),
    url VARCHAR(255),
    api_type VARCHAR(30),                       -- 'rest', 'soap', 'download', 'scrape'
    auth_required BOOLEAN DEFAULT FALSE,
    release_schedule VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Insert data sources
INSERT INTO data_sources (code, name, source_type, country_code, url, api_type, auth_required, release_schedule) VALUES
    ('USDA_FAS', 'USDA Foreign Agricultural Service', 'government', 'USA', 'https://apps.fas.usda.gov', 'rest', false, 'Weekly/Monthly'),
    ('USDA_NASS', 'USDA National Agricultural Statistics', 'government', 'USA', 'https://quickstats.nass.usda.gov', 'rest', true, 'Weekly/Monthly'),
    ('USDA_ERS', 'USDA Economic Research Service', 'government', 'USA', 'https://www.ers.usda.gov', 'download', false, 'Periodic'),
    ('USDA_AMS', 'USDA Agricultural Marketing Service', 'government', 'USA', 'https://www.ams.usda.gov', 'download', false, 'Daily/Weekly'),
    ('EIA', 'Energy Information Administration', 'government', 'USA', 'https://api.eia.gov', 'rest', true, 'Weekly'),
    ('EPA_RFS', 'EPA Renewable Fuel Standard', 'government', 'USA', 'https://www.epa.gov/fuels-registration-reporting-and-compliance-help', 'download', false, 'Monthly'),
    ('CFTC', 'Commodity Futures Trading Commission', 'government', 'USA', 'https://www.cftc.gov', 'rest', false, 'Weekly'),
    ('CENSUS_TRADE', 'US Census International Trade', 'government', 'USA', 'https://api.census.gov', 'rest', false, 'Monthly'),
    ('CGC', 'Canadian Grain Commission', 'government', 'CAN', 'https://www.grainscanada.gc.ca', 'scrape', false, 'Weekly'),
    ('STATSCAN', 'Statistics Canada', 'government', 'CAN', 'https://www150.statcan.gc.ca', 'rest', false, 'Varies'),
    ('CME', 'CME Group', 'exchange', 'USA', 'https://www.cmegroup.com', 'scrape', false, 'Daily'),
    ('MPOB', 'Malaysian Palm Oil Board', 'government', 'MYS', 'http://bepi.mpob.gov.my', 'scrape', false, 'Monthly'),
    ('DROUGHT_MONITOR', 'US Drought Monitor', 'government', 'USA', 'https://droughtmonitor.unl.edu', 'rest', false, 'Weekly'),
    ('CONAB', 'CONAB Brazil', 'government', 'BRA', 'https://www.conab.gov.br', 'download', false, 'Monthly'),
    ('UN_COMTRADE', 'UN Comtrade', 'international', 'WORLD', 'https://comtradeplus.un.org', 'rest', false, 'Monthly')
ON CONFLICT (code) DO NOTHING;


-- ============================================================================
-- TRADE FLOW TABLES
-- ============================================================================

-- International trade flows (imports/exports)
CREATE TABLE IF NOT EXISTS trade_flows (
    id BIGSERIAL PRIMARY KEY,
    commodity_code VARCHAR(20) NOT NULL,
    reporter_country VARCHAR(10) NOT NULL,      -- Country reporting the trade
    partner_country VARCHAR(10) NOT NULL,       -- Trade partner
    flow_type VARCHAR(10) NOT NULL,             -- 'import' or 'export'
    trade_date DATE NOT NULL,                   -- Period end date
    period_type VARCHAR(10) NOT NULL,           -- 'weekly', 'monthly', 'annual'
    marketing_year VARCHAR(10),                 -- e.g., '2024/25'

    -- Quantities
    quantity NUMERIC,
    quantity_unit VARCHAR(20),                  -- 'bushels', 'mt', 'gallons'
    quantity_mt NUMERIC,                        -- Standardized to metric tons

    -- Values
    value_usd NUMERIC,
    unit_value NUMERIC,                         -- Value per unit

    -- Metadata
    hs_code VARCHAR(10),
    source_code VARCHAR(50) NOT NULL,
    collected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT unique_trade_flow UNIQUE (commodity_code, reporter_country, partner_country,
                                         flow_type, trade_date, period_type, source_code)
);

-- Weekly export sales (USDA FAS specific)
CREATE TABLE IF NOT EXISTS export_sales (
    id BIGSERIAL PRIMARY KEY,
    commodity_code VARCHAR(20) NOT NULL,
    destination_country VARCHAR(10) NOT NULL,
    report_date DATE NOT NULL,
    week_ending DATE NOT NULL,
    marketing_year VARCHAR(10) NOT NULL,

    -- Sales and shipments
    net_sales_week NUMERIC,                     -- Net sales this week
    accumulated_sales NUMERIC,                  -- Total commitments MY to date
    shipments_week NUMERIC,                     -- Shipments this week
    accumulated_shipments NUMERIC,              -- Total shipments MY to date
    outstanding_sales NUMERIC,                  -- Unshipped commitments

    -- Changes
    sales_change_week NUMERIC,                  -- Week over week change
    sales_change_pct NUMERIC,

    unit VARCHAR(20) DEFAULT 'MT',
    source_code VARCHAR(50) DEFAULT 'USDA_FAS',
    collected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT unique_export_sales UNIQUE (commodity_code, destination_country,
                                           week_ending, marketing_year)
);


-- ============================================================================
-- SUPPLY AND DEMAND TABLES
-- ============================================================================

-- Production Supply Demand (PSD) balance sheets
CREATE TABLE IF NOT EXISTS supply_demand (
    id BIGSERIAL PRIMARY KEY,
    commodity_code VARCHAR(20) NOT NULL,
    country_code VARCHAR(10) NOT NULL,
    marketing_year VARCHAR(10) NOT NULL,        -- '2024/25'
    report_date DATE NOT NULL,                  -- When this estimate was made
    estimate_type VARCHAR(20),                  -- 'wasde', 'fao', 'attaché'

    -- Supply
    beginning_stocks NUMERIC,
    production NUMERIC,
    imports NUMERIC,
    total_supply NUMERIC,

    -- Demand
    domestic_use NUMERIC,
    feed_use NUMERIC,
    food_use NUMERIC,
    seed_use NUMERIC,
    industrial_use NUMERIC,
    exports NUMERIC,
    total_demand NUMERIC,

    -- Ending balance
    ending_stocks NUMERIC,
    stocks_to_use_ratio NUMERIC,

    -- Areas (for production commodities)
    area_harvested NUMERIC,                     -- Hectares or acres
    yield NUMERIC,                              -- Per hectare/acre
    area_unit VARCHAR(20),

    unit VARCHAR(20),                           -- 'MT', '1000MT', 'bushels'
    source_code VARCHAR(50) NOT NULL,
    collected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT unique_supply_demand UNIQUE (commodity_code, country_code, marketing_year,
                                            report_date, source_code)
);

-- Crop progress and condition
CREATE TABLE IF NOT EXISTS crop_progress (
    id BIGSERIAL PRIMARY KEY,
    commodity_code VARCHAR(20) NOT NULL,
    region VARCHAR(50),                         -- State, province, or country
    report_date DATE NOT NULL,
    week_ending DATE NOT NULL,
    crop_year INTEGER NOT NULL,

    -- Planting/Harvest progress
    planted_pct NUMERIC,
    emerged_pct NUMERIC,
    silking_pct NUMERIC,                        -- Corn specific
    setting_pods_pct NUMERIC,                   -- Soybeans specific
    coloring_pct NUMERIC,
    mature_pct NUMERIC,
    harvested_pct NUMERIC,

    -- Condition ratings
    condition_very_poor NUMERIC,
    condition_poor NUMERIC,
    condition_fair NUMERIC,
    condition_good NUMERIC,
    condition_excellent NUMERIC,
    condition_index NUMERIC,                    -- Weighted index

    -- Comparisons
    progress_vs_avg NUMERIC,                    -- Difference from 5yr avg
    progress_vs_prior NUMERIC,                  -- Difference from prior year

    source_code VARCHAR(50) DEFAULT 'USDA_NASS',
    collected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT unique_crop_progress UNIQUE (commodity_code, region, week_ending, crop_year)
);


-- ============================================================================
-- CRUSH AND PROCESSING TABLES
-- ============================================================================

-- Oilseed crush statistics
CREATE TABLE IF NOT EXISTS crush_data (
    id BIGSERIAL PRIMARY KEY,
    commodity_code VARCHAR(20) NOT NULL,        -- SOYBEANS, CANOLA, etc.
    country_code VARCHAR(10) NOT NULL,
    report_date DATE NOT NULL,
    period_type VARCHAR(10) NOT NULL,           -- 'monthly', 'weekly'
    period_start DATE,
    period_end DATE,

    -- Crush volumes
    crush_volume NUMERIC,                       -- Seed crushed
    crush_capacity_pct NUMERIC,                 -- Capacity utilization

    -- Products
    oil_production NUMERIC,
    meal_production NUMERIC,
    oil_yield_pct NUMERIC,
    meal_yield_pct NUMERIC,

    -- Oil disposition
    oil_exports NUMERIC,
    oil_domestic_use NUMERIC,
    oil_stocks NUMERIC,

    -- Meal disposition
    meal_exports NUMERIC,
    meal_domestic_use NUMERIC,
    meal_stocks NUMERIC,

    unit VARCHAR(20),                           -- 'MT', 'bushels', 'tonnes'
    source_code VARCHAR(50) NOT NULL,
    collected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT unique_crush_data UNIQUE (commodity_code, country_code, period_end, source_code)
);

-- Ethanol production and stocks
CREATE TABLE IF NOT EXISTS ethanol_data (
    id BIGSERIAL PRIMARY KEY,
    report_date DATE NOT NULL,
    week_ending DATE NOT NULL,

    -- Production
    production_kbd NUMERIC,                     -- Thousand barrels per day
    production_gallons NUMERIC,                 -- Million gallons
    production_change_pct NUMERIC,

    -- Stocks
    stocks_kb NUMERIC,                          -- Thousand barrels
    stocks_change_kb NUMERIC,
    days_supply NUMERIC,

    -- Implied demand
    implied_demand_kbd NUMERIC,

    -- Regional breakdown
    padd1_production NUMERIC,
    padd2_production NUMERIC,
    padd3_production NUMERIC,
    padd4_production NUMERIC,
    padd5_production NUMERIC,

    -- Fuel blend
    e10_blend_rate NUMERIC,                     -- % of gasoline with ethanol

    source_code VARCHAR(50) DEFAULT 'EIA',
    collected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT unique_ethanol_data UNIQUE (week_ending, source_code)
);

-- RIN generation and compliance
CREATE TABLE IF NOT EXISTS rin_data (
    id BIGSERIAL PRIMARY KEY,
    report_date DATE NOT NULL,
    period_type VARCHAR(10) NOT NULL,           -- 'monthly', 'annual'

    d_code VARCHAR(5) NOT NULL,                 -- D3, D4, D5, D6, D7
    fuel_category VARCHAR(50),

    -- Generation
    rins_generated NUMERIC,                     -- Gallons
    rins_generated_change_pct NUMERIC,

    -- Prices (if available)
    rin_price NUMERIC,

    source_code VARCHAR(50) DEFAULT 'EPA_RFS',
    collected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT unique_rin_data UNIQUE (report_date, period_type, d_code, source_code)
);


-- ============================================================================
-- PRICE TABLES
-- ============================================================================

-- Futures settlement prices
CREATE TABLE IF NOT EXISTS futures_settlements (
    id BIGSERIAL PRIMARY KEY,
    commodity_code VARCHAR(20) NOT NULL,
    contract_symbol VARCHAR(10) NOT NULL,       -- ZC, ZS, CL, etc.
    contract_month VARCHAR(10) NOT NULL,        -- 'H25', 'K25', etc.
    contract_expiry DATE,
    trade_date DATE NOT NULL,

    -- Prices
    settlement NUMERIC,
    open_price NUMERIC,
    high_price NUMERIC,
    low_price NUMERIC,
    close_price NUMERIC,

    -- Changes
    change NUMERIC,
    change_pct NUMERIC,

    -- Volume and interest
    volume INTEGER,
    open_interest INTEGER,

    exchange VARCHAR(10),                       -- CBOT, NYMEX, ICE
    source_code VARCHAR(50) DEFAULT 'CME',
    collected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT unique_futures_settlement UNIQUE (contract_symbol, contract_month, trade_date, source_code)
);

-- Cash/spot prices
CREATE TABLE IF NOT EXISTS cash_prices (
    id BIGSERIAL PRIMARY KEY,
    commodity_code VARCHAR(20) NOT NULL,
    location VARCHAR(100),                      -- Elevator, terminal, region
    price_date DATE NOT NULL,

    -- Prices
    cash_price NUMERIC,
    bid_price NUMERIC,
    ask_price NUMERIC,
    basis NUMERIC,                              -- Cash - futures
    futures_reference VARCHAR(20),              -- Which futures month

    price_type VARCHAR(30),                     -- 'delivered', 'fob', 'track', 'elevator'
    grade VARCHAR(30),                          -- #2 Yellow, etc.

    source_code VARCHAR(50) NOT NULL,
    collected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT unique_cash_price UNIQUE (commodity_code, location, price_date, source_code)
);

-- Feedstock prices (tallow, grease, UCO)
CREATE TABLE IF NOT EXISTS feedstock_prices (
    id BIGSERIAL PRIMARY KEY,
    commodity_code VARCHAR(20) NOT NULL,
    location VARCHAR(100),
    price_date DATE NOT NULL,

    price_low NUMERIC,
    price_high NUMERIC,
    price_mid NUMERIC,
    price_unit VARCHAR(30),                     -- 'cents/pound', '$/mt'

    grade VARCHAR(50),                          -- 'Bleachable Fancy', etc.
    delivery_terms VARCHAR(30),                 -- 'FOB', 'Delivered'

    source_code VARCHAR(50) DEFAULT 'USDA_AMS',
    collected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT unique_feedstock_price UNIQUE (commodity_code, location, price_date, source_code)
);

-- Energy prices
CREATE TABLE IF NOT EXISTS energy_prices (
    id BIGSERIAL PRIMARY KEY,
    commodity_code VARCHAR(20) NOT NULL,        -- CRUDE_WTI, ULSD, RBOB, etc.
    price_date DATE NOT NULL,

    spot_price NUMERIC,
    futures_price NUMERIC,
    retail_price NUMERIC,

    -- Regional breakdowns
    padd1_price NUMERIC,
    padd2_price NUMERIC,
    padd3_price NUMERIC,
    padd4_price NUMERIC,
    padd5_price NUMERIC,
    us_average NUMERIC,

    price_unit VARCHAR(30),                     -- '$/barrel', '$/gallon', '$/MMBtu'

    source_code VARCHAR(50) DEFAULT 'EIA',
    collected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT unique_energy_price UNIQUE (commodity_code, price_date, source_code)
);


-- ============================================================================
-- POSITIONING AND SENTIMENT
-- ============================================================================

-- CFTC Commitments of Traders
CREATE TABLE IF NOT EXISTS cot_positions (
    id BIGSERIAL PRIMARY KEY,
    commodity_code VARCHAR(20) NOT NULL,
    contract_market VARCHAR(100),               -- Full market name
    cftc_code VARCHAR(20),
    report_date DATE NOT NULL,

    -- Commercial positions
    commercial_long INTEGER,
    commercial_short INTEGER,
    commercial_net INTEGER,
    commercial_change INTEGER,

    -- Non-commercial (managed money/specs)
    noncommercial_long INTEGER,
    noncommercial_short INTEGER,
    noncommercial_spreading INTEGER,
    noncommercial_net INTEGER,
    noncommercial_change INTEGER,

    -- Non-reportable (small specs)
    nonreportable_long INTEGER,
    nonreportable_short INTEGER,
    nonreportable_net INTEGER,

    -- Open interest
    open_interest INTEGER,
    open_interest_change INTEGER,

    -- Producer/Merchant/Processor/User (disaggregated)
    prod_merc_long INTEGER,
    prod_merc_short INTEGER,
    swap_long INTEGER,
    swap_short INTEGER,
    swap_spread INTEGER,
    money_manager_long INTEGER,
    money_manager_short INTEGER,
    money_manager_spread INTEGER,
    other_reportable_long INTEGER,
    other_reportable_short INTEGER,
    other_reportable_spread INTEGER,

    report_type VARCHAR(20),                    -- 'legacy', 'disaggregated', 'TFF'
    source_code VARCHAR(50) DEFAULT 'CFTC',
    collected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT unique_cot_position UNIQUE (commodity_code, report_date, report_type, source_code)
);


-- ============================================================================
-- WEATHER AND CONDITIONS
-- ============================================================================

-- Drought conditions
CREATE TABLE IF NOT EXISTS drought_data (
    id BIGSERIAL PRIMARY KEY,
    region VARCHAR(100) NOT NULL,               -- State, county, or region code
    region_type VARCHAR(20),                    -- 'state', 'county', 'huc'
    report_date DATE NOT NULL,
    valid_start DATE,
    valid_end DATE,

    -- Drought category percentages
    none_pct NUMERIC,                           -- No drought
    d0_pct NUMERIC,                             -- Abnormally dry
    d1_pct NUMERIC,                             -- Moderate drought
    d2_pct NUMERIC,                             -- Severe drought
    d3_pct NUMERIC,                             -- Extreme drought
    d4_pct NUMERIC,                             -- Exceptional drought

    -- Summary metrics
    dsci NUMERIC,                               -- Drought Severity Coverage Index
    area_sq_miles NUMERIC,

    -- Week over week changes
    change_d0 NUMERIC,
    change_d1 NUMERIC,
    change_d2 NUMERIC,
    change_d3 NUMERIC,
    change_d4 NUMERIC,

    source_code VARCHAR(50) DEFAULT 'DROUGHT_MONITOR',
    collected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT unique_drought_data UNIQUE (region, report_date, source_code)
);


-- ============================================================================
-- INDEXES FOR PERFORMANCE
-- ============================================================================

-- Trade flows indexes
CREATE INDEX IF NOT EXISTS idx_trade_flows_commodity ON trade_flows(commodity_code);
CREATE INDEX IF NOT EXISTS idx_trade_flows_date ON trade_flows(trade_date);
CREATE INDEX IF NOT EXISTS idx_trade_flows_reporter ON trade_flows(reporter_country);
CREATE INDEX IF NOT EXISTS idx_trade_flows_partner ON trade_flows(partner_country);

-- Export sales indexes
CREATE INDEX IF NOT EXISTS idx_export_sales_commodity ON export_sales(commodity_code);
CREATE INDEX IF NOT EXISTS idx_export_sales_week ON export_sales(week_ending);
CREATE INDEX IF NOT EXISTS idx_export_sales_my ON export_sales(marketing_year);

-- Supply demand indexes
CREATE INDEX IF NOT EXISTS idx_supply_demand_commodity ON supply_demand(commodity_code);
CREATE INDEX IF NOT EXISTS idx_supply_demand_country ON supply_demand(country_code);
CREATE INDEX IF NOT EXISTS idx_supply_demand_my ON supply_demand(marketing_year);

-- Crop progress indexes
CREATE INDEX IF NOT EXISTS idx_crop_progress_commodity ON crop_progress(commodity_code);
CREATE INDEX IF NOT EXISTS idx_crop_progress_week ON crop_progress(week_ending);
CREATE INDEX IF NOT EXISTS idx_crop_progress_region ON crop_progress(region);

-- Crush data indexes
CREATE INDEX IF NOT EXISTS idx_crush_data_commodity ON crush_data(commodity_code);
CREATE INDEX IF NOT EXISTS idx_crush_data_period ON crush_data(period_end);

-- Futures indexes
CREATE INDEX IF NOT EXISTS idx_futures_symbol ON futures_settlements(contract_symbol);
CREATE INDEX IF NOT EXISTS idx_futures_date ON futures_settlements(trade_date);
CREATE INDEX IF NOT EXISTS idx_futures_month ON futures_settlements(contract_month);

-- COT indexes
CREATE INDEX IF NOT EXISTS idx_cot_commodity ON cot_positions(commodity_code);
CREATE INDEX IF NOT EXISTS idx_cot_date ON cot_positions(report_date);

-- Drought indexes
CREATE INDEX IF NOT EXISTS idx_drought_region ON drought_data(region);
CREATE INDEX IF NOT EXISTS idx_drought_date ON drought_data(report_date);


-- ============================================================================
-- COLLECTION TRACKING
-- ============================================================================

-- Track data collection runs
CREATE TABLE IF NOT EXISTS collection_runs (
    id BIGSERIAL PRIMARY KEY,
    collector_name VARCHAR(50) NOT NULL,
    started_at TIMESTAMP NOT NULL,
    completed_at TIMESTAMP,
    status VARCHAR(20) DEFAULT 'running',       -- 'running', 'completed', 'failed'
    records_fetched INTEGER,
    records_inserted INTEGER,
    records_updated INTEGER,
    error_message TEXT,
    parameters JSONB
);

-- Track last successful collection per source/commodity
CREATE TABLE IF NOT EXISTS collection_status (
    id SERIAL PRIMARY KEY,
    collector_name VARCHAR(50) NOT NULL,
    commodity_code VARCHAR(20),
    last_success_at TIMESTAMP,
    last_attempt_at TIMESTAMP,
    last_period_end DATE,
    status VARCHAR(20),
    error_count INTEGER DEFAULT 0,

    CONSTRAINT unique_collection_status UNIQUE (collector_name, commodity_code)
);

CREATE INDEX IF NOT EXISTS idx_collection_runs_collector ON collection_runs(collector_name);
CREATE INDEX IF NOT EXISTS idx_collection_runs_status ON collection_runs(status);
