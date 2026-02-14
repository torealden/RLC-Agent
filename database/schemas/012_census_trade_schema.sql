-- =============================================================================
-- US Census Bureau International Trade Schema
-- =============================================================================
-- Bronze layer tables for Census trade data
-- Monthly imports/exports by HS code and country
-- =============================================================================

-- -----------------------------------------------------------------------------
-- BRONZE LAYER - Raw Census trade data as received from API
-- -----------------------------------------------------------------------------

-- Monthly trade data by HS code and country
CREATE TABLE IF NOT EXISTS bronze.census_trade (
    id SERIAL PRIMARY KEY,
    year INTEGER NOT NULL,
    month INTEGER NOT NULL,
    flow VARCHAR(10) NOT NULL,  -- 'imports' or 'exports'
    hs_code VARCHAR(10) NOT NULL,
    country_code VARCHAR(10),
    country_name VARCHAR(100),
    value_usd DECIMAL(18,2),
    quantity DECIMAL(18,4),
    source VARCHAR(50) DEFAULT 'CENSUS_TRADE',
    collected_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE(year, month, flow, hs_code, country_code)
);

-- -----------------------------------------------------------------------------
-- INDEXES for query performance
-- -----------------------------------------------------------------------------

CREATE INDEX IF NOT EXISTS idx_census_trade_year_month
    ON bronze.census_trade(year, month);

CREATE INDEX IF NOT EXISTS idx_census_trade_hs_code
    ON bronze.census_trade(hs_code);

CREATE INDEX IF NOT EXISTS idx_census_trade_flow
    ON bronze.census_trade(flow);

CREATE INDEX IF NOT EXISTS idx_census_trade_country
    ON bronze.census_trade(country_code);

-- -----------------------------------------------------------------------------
-- SILVER LAYER - Cleaned and standardized views
-- -----------------------------------------------------------------------------

-- Monthly trade totals by commodity (excluding aggregates)
-- Includes avg_price_per_unit calculation for 10-digit HS codes with quantity
-- Includes converted quantities for US spreadsheets (short tons for meals, 1000 lbs for oils)
CREATE OR REPLACE VIEW silver.census_trade_monthly AS
SELECT
    year,
    month,
    flow,
    hs_code,
    CASE
        -- 10-digit codes (have quantity data)
        WHEN hs_code = '1005902030' THEN 'CORN_BULK'
        WHEN hs_code = '1005902035' THEN 'CORN_NO3'
        WHEN hs_code = '1005100010' THEN 'CORN_SEED'
        WHEN hs_code = '1001992055' THEN 'WHEAT_BULK'
        WHEN hs_code = '1001992015' THEN 'WHEAT_WHITE'
        WHEN hs_code = '1001910000' THEN 'WHEAT_SEED'
        WHEN hs_code = '1003900000' THEN 'BARLEY'
        WHEN hs_code = '1201900095' THEN 'SOYBEANS_BULK'
        WHEN hs_code = '1201900005' THEN 'SOYBEANS_OILSTOCK'
        WHEN hs_code = '1201100000' THEN 'SOYBEANS_SEED'
        WHEN hs_code = '1205100000' THEN 'CANOLA'
        WHEN hs_code = '1206000020' THEN 'SUNFLOWER_OILSTOCK'
        -- Vegetable oils (Chapter 15)
        WHEN hs_code = '1507904050' THEN 'SOYBEAN_OIL_REFINED'
        WHEN hs_code = '1507100000' THEN 'SOYBEAN_OIL_CRUDE'
        WHEN hs_code = '1511900000' THEN 'PALM_OIL_REFINED'
        WHEN hs_code = '1511100000' THEN 'PALM_OIL_CRUDE'
        WHEN hs_code = '1512190020' THEN 'SUNFLOWER_OIL'
        WHEN hs_code = '1514190000' THEN 'CANOLA_OIL'
        WHEN hs_code = '1514110000' THEN 'CANOLA_OIL_CRUDE'
        WHEN hs_code = '1515290040' THEN 'CORN_OIL'
        WHEN hs_code = '1513290000' THEN 'PALM_KERNEL_OIL'
        -- Meals & residues (Chapter 23 + soy flour from Chapter 12)
        WHEN hs_code = '2304000000' THEN 'SOYBEAN_MEAL'
        WHEN hs_code = '2302500000' THEN 'SOYBEAN_HULLS'      -- Bran from legumes
        WHEN hs_code = '1208100000' THEN 'SOYBEAN_FLOUR'      -- Soy flour/meal
        WHEN hs_code = '2306300000' THEN 'SUNFLOWER_MEAL'
        WHEN hs_code = '2306490000' THEN 'CANOLA_MEAL'
        WHEN hs_code = '2306100000' THEN 'COTTONSEED_MEAL'
        WHEN hs_code = '2303100010' THEN 'CORN_GLUTEN_FEED'
        WHEN hs_code = '2303100020' THEN 'CORN_GLUTEN_MEAL'
        WHEN hs_code = '2303300000' THEN 'DDGS'
        WHEN hs_code = '5201009000' THEN 'COTTON_RAW'
        -- Legacy 4-digit codes (value only)
        WHEN hs_code = '1001' THEN 'WHEAT'
        WHEN hs_code = '1005' THEN 'CORN'
        WHEN hs_code = '1201' THEN 'SOYBEANS'
        WHEN hs_code = '1507' THEN 'SOYBEAN_OIL'
        WHEN hs_code = '2304' THEN 'SOYBEAN_MEAL'
        ELSE hs_code
    END as commodity,
    -- Commodity category for grouping related HS codes
    CASE
        WHEN hs_code IN ('2304000000', '2302500000', '1208100000') THEN 'SOYBEAN_MEAL_ALL'
        WHEN hs_code IN ('1507904050', '1507100000') THEN 'SOYBEAN_OIL_ALL'
        WHEN hs_code IN ('1201900095', '1201900005', '1201100000') THEN 'SOYBEANS_ALL'
        ELSE NULL
    END as commodity_group,
    -- Unit based on HS code (T=metric tons, KG=kilograms)
    CASE
        WHEN hs_code IN ('1005902030', '1005902035', '1001992055', '1001992015',
                         '1003900000', '1201900095', '2303100010', '2303100020', '2303300000') THEN 'T'
        ELSE 'KG'
    END as unit,
    SUM(value_usd) as total_value_usd,
    SUM(quantity) as total_quantity,
    -- ==========================================================================
    -- CONVERTED QUANTITIES FOR US SPREADSHEETS
    -- ==========================================================================
    -- Metric tons (raw quantity / 1000 for KG, or raw for T)
    CASE
        WHEN hs_code IN ('1005902030', '1005902035', '1001992055', '1001992015',
                         '1003900000', '1201900095', '2303100010', '2303100020', '2303300000')
            THEN SUM(quantity)  -- Already in metric tons
        ELSE SUM(quantity) / 1000  -- Convert KG to MT
    END as quantity_mt,
    -- Short tons for MEALS (MT * 1.10231) - use for soybean meal spreadsheets
    CASE
        WHEN hs_code IN ('2304000000', '2302500000', '1208100000', '2306300000',
                         '2306490000', '2306100000', '2303100010', '2303100020', '2303300000')
            THEN CASE
                WHEN hs_code IN ('2303100010', '2303100020', '2303300000')
                    THEN SUM(quantity) * 1.10231  -- Already in MT
                ELSE SUM(quantity) / 1000 * 1.10231  -- KG -> MT -> Short tons
            END
        ELSE NULL
    END as quantity_short_tons,
    -- Thousand pounds for OILS/FATS (KG * 2.20462 / 1000) - use for oil spreadsheets
    CASE
        WHEN hs_code IN ('1507904050', '1507100000', '1511900000', '1511100000',
                         '1512190020', '1514190000', '1514110000', '1515290040', '1513290000')
            THEN SUM(quantity) * 2.20462 / 1000  -- KG -> lbs -> 1000 lbs
        ELSE NULL
    END as quantity_1000_lbs,
    -- ==========================================================================
    -- Average price per unit (handles NULL/0 quantity gracefully)
    CASE
        WHEN SUM(quantity) > 0 THEN SUM(value_usd) / SUM(quantity)
        ELSE NULL
    END as avg_price_per_unit,
    COUNT(DISTINCT country_code) as num_countries,
    MAX(collected_at) as last_updated
FROM bronze.census_trade
WHERE country_code NOT IN ('0000', '-')  -- Exclude totals/aggregates
  AND country_name NOT LIKE '%TOTAL%'
  AND country_name NOT LIKE '%OECD%'
  AND country_name NOT LIKE '%APEC%'
  AND country_name NOT LIKE '%NATO%'
  AND country_name NOT LIKE '%ASEAN%'
GROUP BY year, month, flow, hs_code;

-- =============================================================================
-- SILVER LAYER - Aggregated views for spreadsheet integration
-- =============================================================================
-- NOTE: These views use the "TOTAL FOR ALL COUNTRIES" row from Census data
-- which provides the accurate total without double-counting regional aggregates.

-- Monthly soybean meal trade (all HS codes combined) in SHORT TONS
-- Use this view for the "Soybean Meal Exports" and "Soybean Meal Imports" tabs
CREATE OR REPLACE VIEW silver.soybean_meal_monthly AS
SELECT
    year,
    month,
    flow,
    SUM(quantity / 1000) as total_mt,                          -- KG -> MT
    SUM(quantity / 1000 * 1.10231) as total_short_tons,        -- KG -> MT -> Short Tons
    SUM(value_usd) as total_value_usd
FROM bronze.census_trade
WHERE hs_code IN ('2304000000', '2302500000', '1208100000')
  AND country_name = 'TOTAL FOR ALL COUNTRIES'
GROUP BY year, month, flow
ORDER BY year DESC, month DESC, flow;

-- Monthly soybean oil trade (all HS codes combined) in 1000 LBS
-- Use this view for the "Soybean Oil Exports" and "Soybean Oil Imports" tabs
CREATE OR REPLACE VIEW silver.soybean_oil_monthly AS
SELECT
    year,
    month,
    flow,
    SUM(quantity / 1000) as total_mt,                          -- KG -> MT
    SUM(quantity * 2.20462 / 1000) as total_1000_lbs,          -- KG -> lbs -> 1000 lbs
    SUM(quantity * 2.20462 / 1000000) as total_million_lbs,    -- For accumulators/balance sheets
    SUM(value_usd) as total_value_usd
FROM bronze.census_trade
WHERE hs_code IN ('1507904050', '1507100000')
  AND country_name = 'TOTAL FOR ALL COUNTRIES'
GROUP BY year, month, flow
ORDER BY year DESC, month DESC, flow;

-- Monthly soybean meal by HS code (for detailed breakdown)
CREATE OR REPLACE VIEW silver.soybean_meal_by_hs_code AS
SELECT
    year,
    month,
    flow,
    hs_code,
    CASE
        WHEN hs_code = '2304000000' THEN 'Soybean Oilcake/Meal'
        WHEN hs_code = '2302500000' THEN 'Soybean Hulls/Bran'
        WHEN hs_code = '1208100000' THEN 'Soy Flour/Meal'
        ELSE hs_code
    END as description,
    quantity / 1000 as mt,
    quantity / 1000 * 1.10231 as short_tons,
    value_usd
FROM bronze.census_trade
WHERE hs_code IN ('2304000000', '2302500000', '1208100000')
  AND country_name = 'TOTAL FOR ALL COUNTRIES'
ORDER BY year DESC, month DESC, flow, hs_code;

-- Top trade partners by commodity and year
CREATE OR REPLACE VIEW silver.census_top_partners AS
SELECT
    year,
    flow,
    hs_code,
    country_name,
    SUM(value_usd) as annual_value_usd,
    SUM(quantity) as annual_quantity
FROM bronze.census_trade
WHERE country_code NOT IN ('0000', '-')
  AND country_name NOT LIKE '%TOTAL%'
  AND country_name NOT LIKE '%OECD%'
  AND country_name NOT LIKE '%APEC%'
  AND country_name NOT LIKE '%NATO%'
  AND country_name NOT LIKE '%ASEAN%'
  AND country_name NOT LIKE '%RIM%'
  AND country_name NOT LIKE '%LATIN%'
  AND country_name NOT LIKE '%LAFTA%'
  AND country_name NOT LIKE '%USMCA%'
  AND country_name NOT LIKE '%NORTH AMERICA%'
  AND country_name NOT LIKE '%ASIA%'
  AND country_name NOT LIKE '%EU-%'
GROUP BY year, flow, hs_code, country_name
ORDER BY year DESC, flow, hs_code, annual_value_usd DESC;

-- -----------------------------------------------------------------------------
-- GOLD LAYER - Analytics-ready views
-- -----------------------------------------------------------------------------

-- YoY trade comparison by commodity
CREATE OR REPLACE VIEW gold.census_trade_yoy AS
WITH current_year AS (
    SELECT
        flow,
        hs_code,
        SUM(value_usd) as value_usd,
        SUM(quantity) as quantity
    FROM bronze.census_trade
    WHERE year = EXTRACT(YEAR FROM CURRENT_DATE)
      AND country_code NOT IN ('0000', '-')
      AND country_name NOT LIKE '%TOTAL%'
    GROUP BY flow, hs_code
),
prior_year AS (
    SELECT
        flow,
        hs_code,
        SUM(value_usd) as value_usd,
        SUM(quantity) as quantity
    FROM bronze.census_trade
    WHERE year = EXTRACT(YEAR FROM CURRENT_DATE) - 1
      AND country_code NOT IN ('0000', '-')
      AND country_name NOT LIKE '%TOTAL%'
    GROUP BY flow, hs_code
)
SELECT
    c.flow,
    c.hs_code,
    c.value_usd as current_year_value,
    p.value_usd as prior_year_value,
    c.value_usd - COALESCE(p.value_usd, 0) as value_change,
    CASE WHEN p.value_usd > 0
        THEN (c.value_usd - p.value_usd) / p.value_usd * 100
        ELSE NULL
    END as value_change_pct
FROM current_year c
LEFT JOIN prior_year p ON c.flow = p.flow AND c.hs_code = p.hs_code;

-- Comments for documentation
COMMENT ON TABLE bronze.census_trade IS 'Monthly US import/export data by HS code from Census Bureau';
