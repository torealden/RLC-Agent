-- ============================================================================
-- CREATE COMPREHENSIVE WHEAT TABLE
-- ============================================================================
-- This table holds ALL wheat data from all ERS wheat data files
-- ============================================================================

DROP TABLE IF EXISTS bronze.wheat_all CASCADE;

CREATE TABLE bronze.wheat_all (
    id SERIAL PRIMARY KEY,
    source_file TEXT,
    commodity_desc TEXT,
    commodity_desc2 TEXT,
    attribute_desc TEXT,
    attribute_desc2 TEXT,
    geography_desc TEXT,
    unit_desc TEXT,
    marketing_year TEXT,
    fiscal_year TEXT,
    calendar_year TEXT,
    timeperiod_desc TEXT,
    amount NUMERIC,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_wheat_all_commodity ON bronze.wheat_all(commodity_desc);
CREATE INDEX idx_wheat_all_attribute ON bronze.wheat_all(attribute_desc);
CREATE INDEX idx_wheat_all_my ON bronze.wheat_all(marketing_year);
CREATE INDEX idx_wheat_all_geo ON bronze.wheat_all(geography_desc);

-- Grant permissions
GRANT SELECT ON bronze.wheat_all TO PUBLIC;
