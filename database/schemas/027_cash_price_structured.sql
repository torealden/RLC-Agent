-- ============================================================================
-- STRUCTURED AMS PRICE RECORDS + HB WEEKLY PRICE MAPPING
-- Round Lakes Commodities
-- ============================================================================
-- File: 027_cash_price_structured.sql
-- Purpose: Bronze table for individual structured AMS price records,
--          config table mapping HB spreadsheet rows to data sources,
--          and gold view for HB weekly price extract.
-- Depends: 001_schema_foundation.sql (bronze, silver, config, gold schemas)
--          007_price_schema.sql (silver.cash_price, silver.specialty_price)
-- ============================================================================

-- ============================================================================
-- BRONZE LAYER — Individual structured AMS price records
-- ============================================================================
-- One row per price record per report section per day.
-- Compared to bronze.price_report_raw (one row per report with JSONB blob),
-- this table stores each price line as its own row for direct querying.

CREATE TABLE IF NOT EXISTS bronze.ams_price_record (
    id                  SERIAL PRIMARY KEY,
    slug_id             VARCHAR(20) NOT NULL,
    report_date         DATE NOT NULL,
    report_section      VARCHAR(200),

    -- Commodity identification
    commodity           VARCHAR(100),
    location            VARCHAR(200),
    grade               VARCHAR(100),
    delivery_period     VARCHAR(100),
    delivery_point      VARCHAR(100),
    transaction_type    VARCHAR(100),
    product_type        VARCHAR(100),

    -- Price fields
    price               DECIMAL(12,4),
    price_low           DECIMAL(12,4),
    price_high          DECIMAL(12,4),
    price_avg           DECIMAL(12,4),
    price_mostly        DECIMAL(12,4),

    -- Basis fields
    basis               DECIMAL(12,4),
    basis_low           DECIMAL(12,4),
    basis_high          DECIMAL(12,4),
    basis_change        DECIMAL(12,4),

    -- Volume / weight (livestock)
    volume              DECIMAL(14,2),
    weight_avg          DECIMAL(10,2),
    weight_low          DECIMAL(10,2),
    weight_high         DECIMAL(10,2),

    -- Unit of measure
    unit                VARCHAR(50),

    -- Raw API record for debugging
    raw_record          JSONB,

    collected_at        TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Unique constraint using COALESCE to handle NULLs
CREATE UNIQUE INDEX IF NOT EXISTS idx_ams_price_record_uq
    ON bronze.ams_price_record (
        slug_id, report_date,
        COALESCE(report_section, ''),
        COALESCE(commodity, ''),
        COALESCE(location, ''),
        COALESCE(grade, ''),
        COALESCE(delivery_period, '')
    );

CREATE INDEX IF NOT EXISTS idx_ams_price_slug ON bronze.ams_price_record(slug_id);
CREATE INDEX IF NOT EXISTS idx_ams_price_date ON bronze.ams_price_record(report_date);
CREATE INDEX IF NOT EXISTS idx_ams_price_commodity ON bronze.ams_price_record(commodity);
CREATE INDEX IF NOT EXISTS idx_ams_price_slug_date ON bronze.ams_price_record(slug_id, report_date);


-- ============================================================================
-- CONFIG — HB Weekly Price Mapping
-- ============================================================================
-- Maps each row in the HB cash price spreadsheet to its source in the
-- bronze/silver layer. The extract tool reads this table to know which
-- price record to pull for each HB row.

CREATE TABLE IF NOT EXISTS config.hb_price_mapping (
    id                  SERIAL PRIMARY KEY,
    hb_row_label        VARCHAR(100) NOT NULL,       -- Display label, e.g. "Corn: Processor Central, IL"
    hb_sheet_row        INTEGER NOT NULL,             -- Row number in Sheet2 (5, 6, 8, ...)
    source_type         VARCHAR(30) NOT NULL,         -- ams_structured, futures
    slug_id             VARCHAR(20),                  -- AMS slug ID (NULL for futures)
    match_commodity     VARCHAR(100),                 -- Substring match on commodity field
    match_location      VARCHAR(200),                 -- Substring match on location field
    match_grade         VARCHAR(100),                 -- Substring match on grade field
    match_section       VARCHAR(200),                 -- Substring match on report_section
    price_field         VARCHAR(30) DEFAULT 'price_avg', -- Which price column to use
    unit                VARCHAR(30),                  -- Expected unit ($/bu, $/ton, etc.)
    carry_forward_weeks INTEGER DEFAULT 1,            -- How many weeks to look back (2 for bi-weekly like fertilizer)
    futures_symbol      VARCHAR(20),                  -- For futures-sourced prices (rice, milk)
    is_active           BOOLEAN DEFAULT TRUE,
    notes               TEXT,
    created_at          TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_hb_price_mapping_row
    ON config.hb_price_mapping (hb_sheet_row);


-- ============================================================================
-- SEED DATA — HB Price Mappings from Sheet2
-- ============================================================================

-- Match criteria use actual MARS API field values.
-- MARS stores sub-section names (Mills and Processors, Country Elevators)
-- in delivery_point, NOT report_section. The gold view and extract tool
-- search both fields when match_section is set.
-- Reports 2675 (hogs), 2485 (choice steers), 3024 (cotton) are NOT in MARS API;
-- they require USDA LMR system. Marked inactive until LMR collector is built.
INSERT INTO config.hb_price_mapping (hb_row_label, hb_sheet_row, source_type, slug_id, match_commodity, match_location, match_grade, match_section, price_field, unit, carry_forward_weeks, futures_symbol, is_active, notes)
VALUES
    -- Corn (delivery_point=Mills and Processors / Country Elevators)
    ('Corn: Processor Central, IL $/bu.', 5, 'ams_structured', '3192', 'Corn', 'Central', NULL, 'Mills and Processors', 'price_avg', '$/bu', 1, NULL, TRUE, 'Conventional corn'),
    ('Corn: Omaha, NE, $/bu.', 6, 'ams_structured', '3225', 'Corn', 'East', NULL, 'Country Elevators', 'price_avg', '$/bu', 1, NULL, TRUE, 'Conventional corn'),

    -- DDGs (from 3616, weekly; location is 'East'/'West' in MARS)
    ('DDGs: Northeast, IA, $/ton', 8, 'ams_structured', '3616', 'Distillers Grain', 'East', NULL, NULL, 'price_avg', '$/ton', 2, NULL, TRUE, 'Dried 10%'),
    ('DDGs: Northwest, IA, $/ton', 9, 'ams_structured', '3616', 'Distillers Grain', 'West', NULL, NULL, 'price_avg', '$/ton', 2, NULL, TRUE, 'Dried 10%'),

    -- Soybeans (delivery_point=Mills and Processors)
    ('Soybeans: Processor Central, IL $/bu.', 11, 'ams_structured', '3192', 'Soybean', 'Central', NULL, 'Mills and Processors', 'price_avg', '$/bu', 1, NULL, TRUE, 'Conventional soybeans'),
    ('Soybeans: St. Louis, MO, $/bu.', 12, 'ams_structured', '2932', 'Soybean', 'St. Louis', NULL, NULL, 'price_avg', '$/bu', 1, NULL, TRUE, 'MS River terminals'),

    -- Soybean Meal (from 3511; location is 'Minneapolis' in MARS)
    ('Soybean Meal: Decatur, IL, 48 pct, ton', 14, 'ams_structured', '3511', 'Soybean Meal', 'Minneapolis', NULL, NULL, 'price_avg', '$/ton', 2, NULL, TRUE, '46.5-48%'),

    -- Wheat (MARS uses class field: Hard Red Winter, Soft Red Winter, etc.)
    ('Wheat: Kansas City, MO, HRW, $/bu.', 16, 'ams_structured', '2932', 'Wheat', 'Kansas City', 'Hard Red Winter', NULL, 'price_avg', '$/bu', 1, NULL, TRUE, 'US #1 HRW'),
    ('Wheat: Minneapolis, MN, 14 pct, DNS, $/bu.', 17, 'ams_structured', '3046', 'Wheat', NULL, 'Northern Spring', NULL, 'price_avg', '$/bu', 1, NULL, TRUE, '14% protein preferred'),
    ('Wheat: St. Louis, MO, SRW, $/bu.', 18, 'ams_structured', '2932', 'Wheat', 'St. Louis', 'Soft Red Winter', NULL, 'price_avg', '$/bu', 1, NULL, TRUE, 'US #2 SRW'),
    ('Wheat: Portland, OR, SWW, $/bu.', 19, 'ams_structured', '3148', 'Wheat', 'Pacific Ports', 'Soft White', NULL, 'price', '$/bu', 1, NULL, TRUE, 'Current delivery month'),
    ('Wheat: Northeast MT, No. 1 HAD, $/bu.', 20, 'ams_structured', '2771', 'Wheat', 'Northeast', 'Durum', NULL, 'price_avg', '$/bu', 1, NULL, TRUE, 'US #1 Durum'),

    -- Sorghum
    ('Sorghum: Kansas City, MO ($/bu)', 22, 'ams_structured', '2932', 'Sorghum', 'Kansas City', NULL, NULL, 'price_avg', '$/bu', 1, NULL, TRUE, 'US #2 Sorghum'),

    -- Cotton (NOT in MARS API — needs LMR or manual)
    ('Cotton: ET-ST, 41-4-34, cents/lb.', 24, 'ams_structured', '3024', 'cotton', 'East TX', '41-4-34', NULL, 'price_avg', 'cents/lb', 1, NULL, FALSE, 'NOT IN MARS API. Needs USDA LMR or manual entry'),

    -- Rice (futures)
    ('Rice: AR, Long Grain, $/cwt.', 26, 'futures', NULL, NULL, NULL, NULL, NULL, 'settlement', '$/cwt', 1, 'ZR', TRUE, 'First non-spot month'),

    -- Barley
    ('Barley: MT, feed, $/cwt.', 28, 'ams_structured', '2771', 'Barley', 'Golden Triangle', 'Feed', NULL, 'price_avg', '$/cwt', 1, NULL, TRUE, 'US #2 Feed, may show N/A'),

    -- Oats (MARS uses 'White Oats' as commodity)
    ('Oats: Minneapolis, MN, No.2, heavy, $/bu.', 30, 'ams_structured', '3046', 'White Oats', NULL, NULL, NULL, 'price_avg', '$/bu', 1, NULL, TRUE, 'US #1/2 White Oats'),

    -- Sunflower (MARS uses 'Sunflower Seeds', location='Grain - National Sunflower')
    ('Sunflower: Fargo, ND, High Oleics, $/cwt.', 32, 'ams_structured', '2887', 'Sunflower Seeds', NULL, 'High Oleics', NULL, 'price_avg', '$/cwt', 1, NULL, TRUE, 'Nearest delivery'),

    -- Hogs (NOT in MARS API — needs LMR)
    ('Hogs: National Base, $/carcass wt.', 34, 'ams_structured', '2675', NULL, NULL, NULL, NULL, 'price_avg', '$/cwt', 1, NULL, FALSE, 'NOT IN MARS API. Needs USDA LMR (LM_HG201)'),

    -- Feeder Pigs (from 2810; MARS commodity='Feeder Pigs', grade='Pigs')
    ('Feeder Pigs: National Avg., 40 lbs., $/head', 36, 'ams_structured', '2810', 'Feeder Pigs', NULL, 'Pigs', 'Report Details', 'price_avg', '$/head', 2, NULL, TRUE, '40 lb per head'),

    -- Choice Steers (NOT in MARS API — needs LMR)
    ('Choice Steers: NE, $/cwt.', 38, 'ams_structured', '2485', NULL, NULL, NULL, NULL, 'price_avg', '$/cwt', 1, NULL, FALSE, 'NOT IN MARS API. Needs USDA LMR (LM_CT169)'),

    -- Feeder Cattle (from 1281; per-lot auction data, grade='Steers')
    ('Feeder Cattle: Oklahoma City, 750-800 lb., $/cwt.', 40, 'ams_structured', '1281', 'Feeder Cattle', NULL, 'Steers', NULL, 'price_avg', '$/cwt', 1, NULL, TRUE, 'Steers 750-800 lbs'),
    ('Feeder Cattle: Oklahoma City, 500-550 lb., $/cwt.', 41, 'ams_structured', '1281', 'Feeder Cattle', NULL, 'Steers', NULL, 'price_avg', '$/cwt', 1, NULL, TRUE, 'Steers 500-550 lbs'),
    ('Feeder Cattle: Oklahoma City, 450-500 lb., $/cwt.', 42, 'ams_structured', '1281', 'Feeder Cattle', NULL, 'Steers', NULL, 'price_avg', '$/cwt', 1, NULL, TRUE, 'Steers 450-500 lbs'),

    -- Milk (futures)
    ('Milk: Class III, CME Futures, $/cwt.', 44, 'futures', NULL, NULL, NULL, NULL, NULL, 'settlement', '$/cwt', 1, 'DC', TRUE, 'First non-spot month'),

    -- Ethanol (from 3616; MARS location is 'N/A' for national ethanol)
    ('Ethanol: IA, $/gal.', 46, 'ams_structured', '3616', 'Ethanol', NULL, NULL, NULL, 'price_avg', '$/gal', 2, NULL, TRUE, NULL),

    -- Farm Diesel (from 3195, bi-weekly)
    ('Farm Diesel: Midwest, $/gal.', 48, 'ams_structured', '3195', 'Diesel', NULL, NULL, 'Fuel', 'price_avg', '$/gal', 2, NULL, TRUE, 'No. 2 Diesel Farm'),

    -- Fertilizer (from 3195, bi-weekly — carry forward 3 weeks)
    ('Fertilizer: DAP Tampa, $/tonne', 50, 'ams_structured', '3195', 'DAP', NULL, NULL, 'Fertilizer', 'price_avg', '$/ton', 3, NULL, TRUE, 'Diammonium Phosphate 18-46-0'),
    ('Fertilizer: Urea New Orleans, $/tonne', 51, 'ams_structured', '3195', 'Urea', NULL, NULL, 'Fertilizer', 'price_avg', '$/ton', 3, NULL, TRUE, 'Urea 46-0-0'),
    ('Fertilizer: UAN New Orleans, $/tonne', 52, 'ams_structured', '3195', 'Liquid Nitrogen', NULL, NULL, 'Fertilizer', 'price_avg', '$/ton', 3, NULL, TRUE, 'Liquid Nitrogen 28-0-0')

ON CONFLICT DO NOTHING;


-- ============================================================================
-- GOLD VIEW — HB Cash Price Latest
-- ============================================================================
-- Joins config.hb_price_mapping with bronze.ams_price_record to produce
-- current week, last week, and year-ago prices for all HB rows.

CREATE OR REPLACE VIEW gold.hb_cash_price_latest AS
SELECT
    m.hb_row_label,
    m.hb_sheet_row,
    m.unit,
    cur.price_val AS this_week,
    wk.price_val  AS last_week,
    yr.price_val  AS year_ago,
    cur.report_date AS current_report_date,
    wk.report_date  AS week_ago_date,
    yr.report_date  AS year_ago_date,
    m.source_type,
    m.slug_id,
    m.notes
FROM config.hb_price_mapping m

-- Current week: most recent record in last 21 days
LEFT JOIN LATERAL (
    SELECT
        b.report_date,
        CASE m.price_field
            WHEN 'price' THEN b.price
            WHEN 'price_low' THEN b.price_low
            WHEN 'price_high' THEN b.price_high
            WHEN 'price_avg' THEN b.price_avg
            WHEN 'price_mostly' THEN b.price_mostly
            ELSE b.price_avg
        END AS price_val
    FROM bronze.ams_price_record b
    WHERE b.slug_id = m.slug_id
      AND b.report_date >= CURRENT_DATE - INTERVAL '21 days'
      AND (m.match_commodity IS NULL OR LOWER(b.commodity) LIKE '%' || LOWER(m.match_commodity) || '%')
      AND (m.match_location IS NULL OR LOWER(b.location) LIKE '%' || LOWER(m.match_location) || '%')
      AND (m.match_grade IS NULL OR LOWER(b.grade) LIKE '%' || LOWER(m.match_grade) || '%')
      AND (m.match_section IS NULL OR LOWER(b.report_section) LIKE '%' || LOWER(m.match_section) || '%'
                                   OR LOWER(b.delivery_point) LIKE '%' || LOWER(m.match_section) || '%')
    ORDER BY b.report_date DESC
    LIMIT 1
) cur ON TRUE

-- Week ago: most recent record 5-14 days ago
LEFT JOIN LATERAL (
    SELECT
        b.report_date,
        CASE m.price_field
            WHEN 'price' THEN b.price
            WHEN 'price_low' THEN b.price_low
            WHEN 'price_high' THEN b.price_high
            WHEN 'price_avg' THEN b.price_avg
            WHEN 'price_mostly' THEN b.price_mostly
            ELSE b.price_avg
        END AS price_val
    FROM bronze.ams_price_record b
    WHERE b.slug_id = m.slug_id
      AND b.report_date >= CURRENT_DATE - INTERVAL '14 days'
      AND b.report_date <= CURRENT_DATE - INTERVAL '5 days'
      AND (m.match_commodity IS NULL OR LOWER(b.commodity) LIKE '%' || LOWER(m.match_commodity) || '%')
      AND (m.match_location IS NULL OR LOWER(b.location) LIKE '%' || LOWER(m.match_location) || '%')
      AND (m.match_grade IS NULL OR LOWER(b.grade) LIKE '%' || LOWER(m.match_grade) || '%')
      AND (m.match_section IS NULL OR LOWER(b.report_section) LIKE '%' || LOWER(m.match_section) || '%'
                                   OR LOWER(b.delivery_point) LIKE '%' || LOWER(m.match_section) || '%')
    ORDER BY b.report_date DESC
    LIMIT 1
) wk ON TRUE

-- Year ago: closest record to 1 year ago (350-380 day window)
LEFT JOIN LATERAL (
    SELECT
        b.report_date,
        CASE m.price_field
            WHEN 'price' THEN b.price
            WHEN 'price_low' THEN b.price_low
            WHEN 'price_high' THEN b.price_high
            WHEN 'price_avg' THEN b.price_avg
            WHEN 'price_mostly' THEN b.price_mostly
            ELSE b.price_avg
        END AS price_val
    FROM bronze.ams_price_record b
    WHERE b.slug_id = m.slug_id
      AND b.report_date >= CURRENT_DATE - INTERVAL '380 days'
      AND b.report_date <= CURRENT_DATE - INTERVAL '350 days'
      AND (m.match_commodity IS NULL OR LOWER(b.commodity) LIKE '%' || LOWER(m.match_commodity) || '%')
      AND (m.match_location IS NULL OR LOWER(b.location) LIKE '%' || LOWER(m.match_location) || '%')
      AND (m.match_grade IS NULL OR LOWER(b.grade) LIKE '%' || LOWER(m.match_grade) || '%')
      AND (m.match_section IS NULL OR LOWER(b.report_section) LIKE '%' || LOWER(m.match_section) || '%'
                                   OR LOWER(b.delivery_point) LIKE '%' || LOWER(m.match_section) || '%')
    ORDER BY b.report_date DESC
    LIMIT 1
) yr ON TRUE

WHERE m.is_active = TRUE
  AND m.source_type = 'ams_structured'
ORDER BY m.hb_sheet_row;


-- ============================================================================
-- GRANTS
-- ============================================================================

GRANT SELECT ON bronze.ams_price_record TO PUBLIC;
GRANT SELECT ON config.hb_price_mapping TO PUBLIC;
GRANT SELECT ON gold.hb_cash_price_latest TO PUBLIC;
