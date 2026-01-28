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
CREATE OR REPLACE VIEW silver.census_trade_monthly AS
SELECT
    year,
    month,
    flow,
    hs_code,
    CASE hs_code
        WHEN '1001' THEN 'WHEAT'
        WHEN '1003' THEN 'BARLEY'
        WHEN '1004' THEN 'OATS'
        WHEN '1005' THEN 'CORN'
        WHEN '1007' THEN 'SORGHUM'
        WHEN '1201' THEN 'SOYBEANS'
        WHEN '1205' THEN 'CANOLA'
        WHEN '1206' THEN 'SUNFLOWER'
        WHEN '1507' THEN 'SOYBEAN_OIL'
        WHEN '1511' THEN 'PALM_OIL'
        WHEN '1512' THEN 'SUNFLOWER_OIL'
        WHEN '1514' THEN 'CANOLA_OIL'
        WHEN '2304' THEN 'SOYBEAN_MEAL'
        WHEN '2306' THEN 'CANOLA_MEAL'
        WHEN '382600' THEN 'BIODIESEL'
        ELSE hs_code
    END as commodity,
    SUM(value_usd) as total_value_usd,
    SUM(quantity) as total_quantity,
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
