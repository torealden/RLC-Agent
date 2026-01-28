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
        WHEN hs_code = '1507904050' THEN 'SOYBEAN_OIL_REFINED'
        WHEN hs_code = '1507100000' THEN 'SOYBEAN_OIL_CRUDE'
        WHEN hs_code = '1511900000' THEN 'PALM_OIL_REFINED'
        WHEN hs_code = '1511100000' THEN 'PALM_OIL_CRUDE'
        WHEN hs_code = '1512190020' THEN 'SUNFLOWER_OIL'
        WHEN hs_code = '1514190000' THEN 'CANOLA_OIL'
        WHEN hs_code = '1515290040' THEN 'CORN_OIL'
        WHEN hs_code = '2304000000' THEN 'SOYBEAN_MEAL'
        WHEN hs_code = '2306300000' THEN 'SUNFLOWER_MEAL'
        WHEN hs_code = '2306490000' THEN 'CANOLA_MEAL'
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
    -- Unit based on HS code (T=metric tons, KG=kilograms)
    CASE
        WHEN hs_code IN ('1005902030', '1005902035', '1001992055', '1001992015',
                         '1003900000', '1201900095', '2303100010', '2303100020', '2303300000') THEN 'T'
        ELSE 'KG'
    END as unit,
    SUM(value_usd) as total_value_usd,
    SUM(quantity) as total_quantity,
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
