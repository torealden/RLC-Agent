-- ============================================================================
-- TRADE REFERENCE TABLES - Silver Layer
-- ============================================================================
-- These tables provide the mapping and conversion data for trade flow updates
-- Run this file to create the reference tables and populate with initial data
-- ============================================================================

-- ============================================================================
-- 1. COUNTRY REFERENCE TABLE
-- ============================================================================
-- Maps Census country codes to standard names, assigns regions and sort order

DROP TABLE IF EXISTS silver.trade_country_reference CASCADE;

CREATE TABLE silver.trade_country_reference (
    id SERIAL PRIMARY KEY,
    country_code VARCHAR(10),           -- Census Bureau country code
    country_name VARCHAR(100) NOT NULL, -- Standard name for display
    country_name_alt VARCHAR(100),      -- Alternative name (historical, etc.)
    region VARCHAR(50) NOT NULL,        -- Regional grouping
    region_sort_order INT NOT NULL,     -- Order of regions (1=EU, 2=Europe Other, etc.)
    country_sort_order INT NOT NULL,    -- Order within region
    spreadsheet_row INT,                -- Row number in trade spreadsheets
    is_regional_total BOOLEAN DEFAULT FALSE, -- True for aggregate rows
    is_active BOOLEAN DEFAULT TRUE,     -- False for historical (Czechoslovakia, etc.)
    notes TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),

    UNIQUE (country_name, region)
);

CREATE INDEX idx_trade_country_region ON silver.trade_country_reference(region);
CREATE INDEX idx_trade_country_code ON silver.trade_country_reference(country_code);
CREATE INDEX idx_trade_country_row ON silver.trade_country_reference(spreadsheet_row);

-- ============================================================================
-- 2. COMMODITY REFERENCE TABLE
-- ============================================================================
-- Maps HS codes to commodities with conversion factors

DROP TABLE IF EXISTS silver.trade_commodity_reference CASCADE;

CREATE TABLE silver.trade_commodity_reference (
    id SERIAL PRIMARY KEY,
    hs_code_10 VARCHAR(10) NOT NULL,    -- Full 10-digit HS/Schedule B code
    hs_code_6 VARCHAR(6),               -- 6-digit international HS code
    commodity_group VARCHAR(50) NOT NULL, -- SOYBEANS, CORN, WHEAT, etc.
    commodity_name VARCHAR(100) NOT NULL, -- Detailed name
    flow_type VARCHAR(20) NOT NULL,     -- EXPORT or IMPORT
    source_unit VARCHAR(20) DEFAULT 'KG', -- Unit from Census API
    display_unit VARCHAR(30) NOT NULL,  -- Unit for spreadsheets
    conversion_factor NUMERIC(18,8) NOT NULL, -- Multiply source by this
    is_active BOOLEAN DEFAULT TRUE,
    notes TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW(),

    UNIQUE (hs_code_10, flow_type)
);

CREATE INDEX idx_trade_commodity_group ON silver.trade_commodity_reference(commodity_group);
CREATE INDEX idx_trade_commodity_hs ON silver.trade_commodity_reference(hs_code_10);

-- ============================================================================
-- 3. REGIONAL GROUPS REFERENCE
-- ============================================================================
-- Defines regions and their display order

DROP TABLE IF EXISTS silver.trade_region_reference CASCADE;

CREATE TABLE silver.trade_region_reference (
    id SERIAL PRIMARY KEY,
    region_code VARCHAR(20) NOT NULL UNIQUE,
    region_name VARCHAR(100) NOT NULL,
    sort_order INT NOT NULL,
    parent_region VARCHAR(20),          -- For sub-regions if needed
    is_active BOOLEAN DEFAULT TRUE,
    notes TEXT
);

-- ============================================================================
-- POPULATE REGION REFERENCE
-- ============================================================================

INSERT INTO silver.trade_region_reference (region_code, region_name, sort_order, notes) VALUES
('EU27', 'European Union (27)', 1, 'EU members excluding UK'),
('EUROPE_OTHER', 'Other Europe', 2, 'Non-EU European countries'),
('UK', 'United Kingdom', 3, 'Separated from EU post-Brexit'),
('FSU', 'Former Soviet Union', 4, 'FSU-12 countries'),
('ASIA_OCEANIA', 'Asia & Oceania', 5, 'Asia and Pacific region'),
('AFRICA', 'Africa', 6, 'African continent'),
('WESTERN_HEMISPHERE', 'Western Hemisphere', 7, 'North, Central, South America'),
('WORLD', 'World Total', 8, 'Global aggregate');

-- ============================================================================
-- POPULATE COUNTRY REFERENCE
-- ============================================================================
-- Countries organized by region with proper sort order
-- UK is now separate from EU

INSERT INTO silver.trade_country_reference
(country_code, country_name, region, region_sort_order, country_sort_order, spreadsheet_row, is_regional_total, is_active, notes)
VALUES

-- EUROPEAN UNION (27) - Row 4 is regional total
('EU27', 'EUROPEAN UNION-27', 'EU27', 1, 0, 4, TRUE, TRUE, 'Regional total'),
('4031', 'AUSTRIA', 'EU27', 1, 1, 5, FALSE, TRUE, NULL),
('4211', 'BELGIUM', 'EU27', 1, 2, 6, FALSE, TRUE, NULL),
('4841', 'BULGARIA', 'EU27', 1, 3, 7, FALSE, TRUE, NULL),
('4791', 'CROATIA', 'EU27', 1, 4, 8, FALSE, TRUE, NULL),
('4221', 'CYPRUS', 'EU27', 1, 5, 9, FALSE, TRUE, NULL),
('4351', 'CZECH REPUBLIC', 'EU27', 1, 6, 10, FALSE, TRUE, NULL),
('4091', 'DENMARK', 'EU27', 1, 7, 11, FALSE, TRUE, NULL),
('4472', 'ESTONIA', 'EU27', 1, 8, 12, FALSE, TRUE, NULL),
('4011', 'FINLAND', 'EU27', 1, 9, 13, FALSE, TRUE, NULL),
('4271', 'FRANCE', 'EU27', 1, 10, 14, FALSE, TRUE, NULL),
('4280', 'GERMANY', 'EU27', 1, 11, 15, FALSE, TRUE, NULL),
('4841', 'GREECE', 'EU27', 1, 12, 16, FALSE, TRUE, NULL),
('4371', 'HUNGARY', 'EU27', 1, 13, 17, FALSE, TRUE, NULL),
('4190', 'IRELAND', 'EU27', 1, 14, 18, FALSE, TRUE, NULL),
('4759', 'ITALY', 'EU27', 1, 15, 19, FALSE, TRUE, NULL),
('4474', 'LATVIA', 'EU27', 1, 16, 20, FALSE, TRUE, NULL),
('4476', 'LITHUANIA', 'EU27', 1, 17, 21, FALSE, TRUE, NULL),
('4223', 'LUXEMBOURG', 'EU27', 1, 18, 22, FALSE, TRUE, NULL),
('4791', 'MALTA', 'EU27', 1, 19, 23, FALSE, TRUE, NULL),
('4210', 'NETHERLANDS', 'EU27', 1, 20, 24, FALSE, TRUE, NULL),
('4550', 'POLAND', 'EU27', 1, 21, 25, FALSE, TRUE, NULL),
('4710', 'PORTUGAL', 'EU27', 1, 22, 26, FALSE, TRUE, NULL),
('4880', 'ROMANIA', 'EU27', 1, 23, 27, FALSE, TRUE, NULL),
('4781', 'SLOVAKIA', 'EU27', 1, 24, 28, FALSE, TRUE, NULL),
('4793', 'SLOVENIA', 'EU27', 1, 25, 29, FALSE, TRUE, NULL),
('4700', 'SPAIN', 'EU27', 1, 26, 30, FALSE, TRUE, NULL),
('4010', 'SWEDEN', 'EU27', 1, 27, 31, FALSE, TRUE, NULL),

-- UNITED KINGDOM - Separate region (Row 32-33)
('UK', 'UNITED KINGDOM TOTAL', 'UK', 3, 0, 32, TRUE, TRUE, 'UK regional total'),
('4120', 'UNITED KINGDOM', 'UK', 3, 1, 33, FALSE, TRUE, NULL),

-- OTHER EUROPE - Row 34 is regional total
('EUR_OTHER', 'OTHER EUROPE TOTAL', 'EUROPE_OTHER', 2, 0, 34, TRUE, TRUE, 'Regional total'),
('4217', 'ALBANIA', 'EUROPE_OTHER', 2, 1, 35, FALSE, TRUE, NULL),
('4231', 'ANDORRA', 'EUROPE_OTHER', 2, 2, 36, FALSE, TRUE, NULL),
('4227', 'BOSNIA AND HERZEGOVINA', 'EUROPE_OTHER', 2, 3, 37, FALSE, TRUE, NULL),
('4032', 'ICELAND', 'EUROPE_OTHER', 2, 4, 38, FALSE, TRUE, NULL),
('4785', 'KOSOVO', 'EUROPE_OTHER', 2, 5, 39, FALSE, TRUE, NULL),
('4787', 'MONTENEGRO', 'EUROPE_OTHER', 2, 6, 40, FALSE, TRUE, NULL),
('4789', 'NORTH MACEDONIA', 'EUROPE_OTHER', 2, 7, 41, FALSE, TRUE, NULL),
('4030', 'NORWAY', 'EUROPE_OTHER', 2, 8, 42, FALSE, TRUE, NULL),
('4783', 'SERBIA', 'EUROPE_OTHER', 2, 9, 43, FALSE, TRUE, NULL),
('4419', 'SWITZERLAND', 'EUROPE_OTHER', 2, 10, 44, FALSE, TRUE, NULL),
('4890', 'TURKEY', 'EUROPE_OTHER', 2, 11, 45, FALSE, TRUE, NULL),

-- FORMER SOVIET UNION (FSU) - Row 46 is regional total
('FSU', 'FSU TOTAL', 'FSU', 4, 0, 46, TRUE, TRUE, 'Regional total'),
('4631', 'ARMENIA', 'FSU', 4, 1, 47, FALSE, TRUE, NULL),
('4633', 'AZERBAIJAN', 'FSU', 4, 2, 48, FALSE, TRUE, NULL),
('4622', 'BELARUS', 'FSU', 4, 3, 49, FALSE, TRUE, NULL),
('4635', 'GEORGIA', 'FSU', 4, 4, 50, FALSE, TRUE, NULL),
('4637', 'KAZAKHSTAN', 'FSU', 4, 5, 51, FALSE, TRUE, NULL),
('4639', 'KYRGYZSTAN', 'FSU', 4, 6, 52, FALSE, TRUE, NULL),
('4641', 'MOLDOVA', 'FSU', 4, 7, 53, FALSE, TRUE, NULL),
('4621', 'RUSSIA', 'FSU', 4, 8, 54, FALSE, TRUE, NULL),
('4643', 'TAJIKISTAN', 'FSU', 4, 9, 55, FALSE, TRUE, NULL),
('4645', 'TURKMENISTAN', 'FSU', 4, 10, 56, FALSE, TRUE, NULL),
('4623', 'UKRAINE', 'FSU', 4, 11, 57, FALSE, TRUE, NULL),
('4647', 'UZBEKISTAN', 'FSU', 4, 12, 58, FALSE, TRUE, NULL),

-- ASIA & OCEANIA - Row 59 is regional total
('ASIA', 'ASIA & OCEANIA TOTAL', 'ASIA_OCEANIA', 5, 0, 59, TRUE, TRUE, 'Regional total'),
('5310', 'AFGHANISTAN', 'ASIA_OCEANIA', 5, 1, 60, FALSE, TRUE, NULL),
('6021', 'AUSTRALIA', 'ASIA_OCEANIA', 5, 2, 61, FALSE, TRUE, NULL),
('5380', 'BANGLADESH', 'ASIA_OCEANIA', 5, 3, 62, FALSE, TRUE, NULL),
('5550', 'BRUNEI', 'ASIA_OCEANIA', 5, 4, 63, FALSE, TRUE, NULL),
('5560', 'CAMBODIA', 'ASIA_OCEANIA', 5, 5, 64, FALSE, TRUE, NULL),
('5700', 'CHINA', 'ASIA_OCEANIA', 5, 6, 65, FALSE, TRUE, NULL),
('5820', 'HONG KONG', 'ASIA_OCEANIA', 5, 7, 66, FALSE, TRUE, NULL),
('5330', 'INDIA', 'ASIA_OCEANIA', 5, 8, 67, FALSE, TRUE, NULL),
('5600', 'INDONESIA', 'ASIA_OCEANIA', 5, 9, 68, FALSE, TRUE, NULL),
('5070', 'IRAN', 'ASIA_OCEANIA', 5, 10, 69, FALSE, TRUE, NULL),
('5050', 'IRAQ', 'ASIA_OCEANIA', 5, 11, 70, FALSE, TRUE, NULL),
('5030', 'ISRAEL', 'ASIA_OCEANIA', 5, 12, 71, FALSE, TRUE, NULL),
('5880', 'JAPAN', 'ASIA_OCEANIA', 5, 13, 72, FALSE, TRUE, NULL),
('5020', 'JORDAN', 'ASIA_OCEANIA', 5, 14, 73, FALSE, TRUE, NULL),
('5800', 'KOREA, SOUTH', 'ASIA_OCEANIA', 5, 15, 74, FALSE, TRUE, NULL),
('5790', 'KOREA, NORTH', 'ASIA_OCEANIA', 5, 16, 75, FALSE, TRUE, NULL),
('5130', 'KUWAIT', 'ASIA_OCEANIA', 5, 17, 76, FALSE, TRUE, NULL),
('5570', 'LAOS', 'ASIA_OCEANIA', 5, 18, 77, FALSE, TRUE, NULL),
('5040', 'LEBANON', 'ASIA_OCEANIA', 5, 19, 78, FALSE, TRUE, NULL),
('5800', 'MACAU', 'ASIA_OCEANIA', 5, 20, 79, FALSE, TRUE, NULL),
('5570', 'MALAYSIA', 'ASIA_OCEANIA', 5, 21, 80, FALSE, TRUE, NULL),
('5360', 'MALDIVES', 'ASIA_OCEANIA', 5, 22, 81, FALSE, TRUE, NULL),
('5570', 'MYANMAR', 'ASIA_OCEANIA', 5, 23, 82, FALSE, TRUE, NULL),
('5350', 'NEPAL', 'ASIA_OCEANIA', 5, 24, 83, FALSE, TRUE, NULL),
('6141', 'NEW ZEALAND', 'ASIA_OCEANIA', 5, 25, 84, FALSE, TRUE, NULL),
('5140', 'OMAN', 'ASIA_OCEANIA', 5, 26, 85, FALSE, TRUE, NULL),
('5340', 'PAKISTAN', 'ASIA_OCEANIA', 5, 27, 86, FALSE, TRUE, NULL),
('5660', 'PHILIPPINES', 'ASIA_OCEANIA', 5, 28, 87, FALSE, TRUE, NULL),
('5170', 'QATAR', 'ASIA_OCEANIA', 5, 29, 88, FALSE, TRUE, NULL),
('5110', 'SAUDI ARABIA', 'ASIA_OCEANIA', 5, 30, 89, FALSE, TRUE, NULL),
('5590', 'SINGAPORE', 'ASIA_OCEANIA', 5, 31, 90, FALSE, TRUE, NULL),
('5370', 'SRI LANKA', 'ASIA_OCEANIA', 5, 32, 91, FALSE, TRUE, NULL),
('5010', 'SYRIA', 'ASIA_OCEANIA', 5, 33, 92, FALSE, TRUE, NULL),
('5830', 'TAIWAN', 'ASIA_OCEANIA', 5, 34, 93, FALSE, TRUE, NULL),
('5520', 'THAILAND', 'ASIA_OCEANIA', 5, 35, 94, FALSE, TRUE, NULL),
('5200', 'TIMOR-LESTE', 'ASIA_OCEANIA', 5, 36, 95, FALSE, TRUE, NULL),
('5180', 'UNITED ARAB EMIRATES', 'ASIA_OCEANIA', 5, 37, 96, FALSE, TRUE, NULL),
('5500', 'VIETNAM', 'ASIA_OCEANIA', 5, 38, 97, FALSE, TRUE, NULL),
('5190', 'YEMEN', 'ASIA_OCEANIA', 5, 39, 98, FALSE, TRUE, NULL),

-- AFRICA - Row 99 is regional total
('AFRICA', 'AFRICA TOTAL', 'AFRICA', 6, 0, 99, TRUE, TRUE, 'Regional total'),
('7210', 'ALGERIA', 'AFRICA', 6, 1, 100, FALSE, TRUE, NULL),
('7620', 'ANGOLA', 'AFRICA', 6, 2, 101, FALSE, TRUE, NULL),
('7670', 'BENIN', 'AFRICA', 6, 3, 102, FALSE, TRUE, NULL),
('7640', 'BOTSWANA', 'AFRICA', 6, 4, 103, FALSE, TRUE, NULL),
('7450', 'BURKINA FASO', 'AFRICA', 6, 5, 104, FALSE, TRUE, NULL),
('7560', 'BURUNDI', 'AFRICA', 6, 6, 105, FALSE, TRUE, NULL),
('7490', 'CAMEROON', 'AFRICA', 6, 7, 106, FALSE, TRUE, NULL),
('7460', 'CAPE VERDE', 'AFRICA', 6, 8, 107, FALSE, TRUE, NULL),
('7510', 'CENTRAL AFRICAN REPUBLIC', 'AFRICA', 6, 9, 108, FALSE, TRUE, NULL),
('7520', 'CHAD', 'AFRICA', 6, 10, 109, FALSE, TRUE, NULL),
('7810', 'COMOROS', 'AFRICA', 6, 11, 110, FALSE, TRUE, NULL),
('7530', 'CONGO (BRAZZAVILLE)', 'AFRICA', 6, 12, 111, FALSE, TRUE, NULL),
('7540', 'CONGO (KINSHASA)', 'AFRICA', 6, 13, 112, FALSE, TRUE, NULL),
('7680', 'COTE D IVOIRE', 'AFRICA', 6, 14, 113, FALSE, TRUE, NULL),
('7820', 'DJIBOUTI', 'AFRICA', 6, 15, 114, FALSE, TRUE, NULL),
('7290', 'EGYPT', 'AFRICA', 6, 16, 115, FALSE, TRUE, NULL),
('7480', 'EQUATORIAL GUINEA', 'AFRICA', 6, 17, 116, FALSE, TRUE, NULL),
('7848', 'ERITREA', 'AFRICA', 6, 18, 117, FALSE, TRUE, NULL),
('7658', 'ESWATINI', 'AFRICA', 6, 19, 118, FALSE, TRUE, NULL),
('7840', 'ETHIOPIA', 'AFRICA', 6, 20, 119, FALSE, TRUE, NULL),
('7500', 'GABON', 'AFRICA', 6, 21, 120, FALSE, TRUE, NULL),
('7420', 'GAMBIA', 'AFRICA', 6, 22, 121, FALSE, TRUE, NULL),
('7690', 'GHANA', 'AFRICA', 6, 23, 122, FALSE, TRUE, NULL),
('7430', 'GUINEA', 'AFRICA', 6, 24, 123, FALSE, TRUE, NULL),
('7440', 'GUINEA-BISSAU', 'AFRICA', 6, 25, 124, FALSE, TRUE, NULL),
('7850', 'KENYA', 'AFRICA', 6, 26, 125, FALSE, TRUE, NULL),
('7650', 'LESOTHO', 'AFRICA', 6, 27, 126, FALSE, TRUE, NULL),
('7550', 'LIBERIA', 'AFRICA', 6, 28, 127, FALSE, TRUE, NULL),
('7250', 'LIBYA', 'AFRICA', 6, 29, 128, FALSE, TRUE, NULL),
('7830', 'MADAGASCAR', 'AFRICA', 6, 30, 129, FALSE, TRUE, NULL),
('7570', 'MALAWI', 'AFRICA', 6, 31, 130, FALSE, TRUE, NULL),
('7470', 'MALI', 'AFRICA', 6, 32, 131, FALSE, TRUE, NULL),
('7410', 'MAURITANIA', 'AFRICA', 6, 33, 132, FALSE, TRUE, NULL),
('7860', 'MAURITIUS', 'AFRICA', 6, 34, 133, FALSE, TRUE, NULL),
('7140', 'MOROCCO', 'AFRICA', 6, 35, 134, FALSE, TRUE, NULL),
('7580', 'MOZAMBIQUE', 'AFRICA', 6, 36, 135, FALSE, TRUE, NULL),
('7660', 'NAMIBIA', 'AFRICA', 6, 37, 136, FALSE, TRUE, NULL),
('7240', 'NIGER', 'AFRICA', 6, 38, 137, FALSE, TRUE, NULL),
('7700', 'NIGERIA', 'AFRICA', 6, 39, 138, FALSE, TRUE, NULL),
('7870', 'REUNION', 'AFRICA', 6, 40, 139, FALSE, TRUE, NULL),
('7560', 'RWANDA', 'AFRICA', 6, 41, 140, FALSE, TRUE, NULL),
('7710', 'SAO TOME AND PRINCIPE', 'AFRICA', 6, 42, 141, FALSE, TRUE, NULL),
('7400', 'SENEGAL', 'AFRICA', 6, 43, 142, FALSE, TRUE, NULL),
('7880', 'SEYCHELLES', 'AFRICA', 6, 44, 143, FALSE, TRUE, NULL),
('7460', 'SIERRA LEONE', 'AFRICA', 6, 45, 144, FALSE, TRUE, NULL),
('7890', 'SOMALIA', 'AFRICA', 6, 46, 145, FALSE, TRUE, NULL),
('7910', 'SOUTH AFRICA', 'AFRICA', 6, 47, 146, FALSE, TRUE, NULL),
('7329', 'SOUTH SUDAN', 'AFRICA', 6, 48, 147, FALSE, TRUE, NULL),
('7320', 'SUDAN', 'AFRICA', 6, 49, 148, FALSE, TRUE, NULL),
('7900', 'TANZANIA', 'AFRICA', 6, 50, 149, FALSE, TRUE, NULL),
('7720', 'TOGO', 'AFRICA', 6, 51, 150, FALSE, TRUE, NULL),
('7230', 'TUNISIA', 'AFRICA', 6, 52, 151, FALSE, TRUE, NULL),
('7920', 'UGANDA', 'AFRICA', 6, 53, 152, FALSE, TRUE, NULL),
('7610', 'ZAMBIA', 'AFRICA', 6, 54, 153, FALSE, TRUE, NULL),
('7600', 'ZIMBABWE', 'AFRICA', 6, 55, 154, FALSE, TRUE, NULL),

-- WESTERN HEMISPHERE - Row 155 is regional total
('WHEM', 'WESTERN HEMISPHERE TOTAL', 'WESTERN_HEMISPHERE', 7, 0, 155, TRUE, TRUE, 'Regional total'),
('2770', 'ANGUILLA', 'WESTERN_HEMISPHERE', 7, 1, 156, FALSE, TRUE, NULL),
('2590', 'ANTIGUA AND BARBUDA', 'WESTERN_HEMISPHERE', 7, 2, 157, FALSE, TRUE, NULL),
('3570', 'ARGENTINA', 'WESTERN_HEMISPHERE', 7, 3, 158, FALSE, TRUE, NULL),
('2771', 'ARUBA', 'WESTERN_HEMISPHERE', 7, 4, 159, FALSE, TRUE, NULL),
('2400', 'BAHAMAS', 'WESTERN_HEMISPHERE', 7, 5, 160, FALSE, TRUE, NULL),
('2780', 'BARBADOS', 'WESTERN_HEMISPHERE', 7, 6, 161, FALSE, TRUE, NULL),
('2050', 'BELIZE', 'WESTERN_HEMISPHERE', 7, 7, 162, FALSE, TRUE, NULL),
('2430', 'BERMUDA', 'WESTERN_HEMISPHERE', 7, 8, 163, FALSE, TRUE, NULL),
('3510', 'BOLIVIA', 'WESTERN_HEMISPHERE', 7, 9, 164, FALSE, TRUE, NULL),
('3515', 'BONAIRE', 'WESTERN_HEMISPHERE', 7, 10, 165, FALSE, TRUE, NULL),
('3510', 'BRAZIL', 'WESTERN_HEMISPHERE', 7, 11, 166, FALSE, TRUE, NULL),
('2721', 'BRITISH VIRGIN ISLANDS', 'WESTERN_HEMISPHERE', 7, 12, 167, FALSE, TRUE, NULL),
('2010', 'CANADA', 'WESTERN_HEMISPHERE', 7, 13, 168, FALSE, TRUE, NULL),
('2450', 'CAYMAN ISLANDS', 'WESTERN_HEMISPHERE', 7, 14, 169, FALSE, TRUE, NULL),
('3370', 'CHILE', 'WESTERN_HEMISPHERE', 7, 15, 170, FALSE, TRUE, NULL),
('3010', 'COLOMBIA', 'WESTERN_HEMISPHERE', 7, 16, 171, FALSE, TRUE, NULL),
('2230', 'COSTA RICA', 'WESTERN_HEMISPHERE', 7, 17, 172, FALSE, TRUE, NULL),
('2481', 'CUBA', 'WESTERN_HEMISPHERE', 7, 18, 173, FALSE, TRUE, NULL),
('2773', 'CURACAO', 'WESTERN_HEMISPHERE', 7, 19, 174, FALSE, TRUE, NULL),
('2480', 'DOMINICA', 'WESTERN_HEMISPHERE', 7, 20, 175, FALSE, TRUE, NULL),
('2470', 'DOMINICAN REPUBLIC', 'WESTERN_HEMISPHERE', 7, 21, 176, FALSE, TRUE, NULL),
('3310', 'ECUADOR', 'WESTERN_HEMISPHERE', 7, 22, 177, FALSE, TRUE, NULL),
('2110', 'EL SALVADOR', 'WESTERN_HEMISPHERE', 7, 23, 178, FALSE, TRUE, NULL),
('3220', 'FRENCH GUIANA', 'WESTERN_HEMISPHERE', 7, 24, 179, FALSE, TRUE, NULL),
('2790', 'GRENADA', 'WESTERN_HEMISPHERE', 7, 25, 180, FALSE, TRUE, NULL),
('2750', 'GUADELOUPE', 'WESTERN_HEMISPHERE', 7, 26, 181, FALSE, TRUE, NULL),
('2150', 'GUATEMALA', 'WESTERN_HEMISPHERE', 7, 27, 182, FALSE, TRUE, NULL),
('3130', 'GUYANA', 'WESTERN_HEMISPHERE', 7, 28, 183, FALSE, TRUE, NULL),
('2460', 'HAITI', 'WESTERN_HEMISPHERE', 7, 29, 184, FALSE, TRUE, NULL),
('2170', 'HONDURAS', 'WESTERN_HEMISPHERE', 7, 30, 185, FALSE, TRUE, NULL),
('2500', 'JAMAICA', 'WESTERN_HEMISPHERE', 7, 31, 186, FALSE, TRUE, NULL),
('2760', 'MARTINIQUE', 'WESTERN_HEMISPHERE', 7, 32, 187, FALSE, TRUE, NULL),
('2010', 'MEXICO', 'WESTERN_HEMISPHERE', 7, 33, 188, FALSE, TRUE, NULL),
('2775', 'MONTSERRAT', 'WESTERN_HEMISPHERE', 7, 34, 189, FALSE, TRUE, NULL),
('2190', 'NICARAGUA', 'WESTERN_HEMISPHERE', 7, 35, 190, FALSE, TRUE, NULL),
('2270', 'PANAMA', 'WESTERN_HEMISPHERE', 7, 36, 191, FALSE, TRUE, NULL),
('3350', 'PARAGUAY', 'WESTERN_HEMISPHERE', 7, 37, 192, FALSE, TRUE, NULL),
('3330', 'PERU', 'WESTERN_HEMISPHERE', 7, 38, 193, FALSE, TRUE, NULL),
('2610', 'ST KITTS AND NEVIS', 'WESTERN_HEMISPHERE', 7, 39, 194, FALSE, TRUE, NULL),
('2795', 'ST LUCIA', 'WESTERN_HEMISPHERE', 7, 40, 195, FALSE, TRUE, NULL),
('2600', 'ST VINCENT AND GRENADINES', 'WESTERN_HEMISPHERE', 7, 41, 196, FALSE, TRUE, NULL),
('3150', 'SURINAME', 'WESTERN_HEMISPHERE', 7, 42, 197, FALSE, TRUE, NULL),
('2800', 'TRINIDAD AND TOBAGO', 'WESTERN_HEMISPHERE', 7, 43, 198, FALSE, TRUE, NULL),
('2455', 'TURKS AND CAICOS', 'WESTERN_HEMISPHERE', 7, 44, 199, FALSE, TRUE, NULL),
('3530', 'URUGUAY', 'WESTERN_HEMISPHERE', 7, 45, 200, FALSE, TRUE, NULL),
('3070', 'VENEZUELA', 'WESTERN_HEMISPHERE', 7, 46, 201, FALSE, TRUE, NULL),
('2720', 'VIRGIN ISLANDS US', 'WESTERN_HEMISPHERE', 7, 47, 202, FALSE, TRUE, NULL),

-- WORLD TOTAL - Row 203
('WORLD', 'WORLD TOTAL', 'WORLD', 8, 0, 203, TRUE, TRUE, 'Sum of all regional totals');

-- ============================================================================
-- POPULATE COMMODITY REFERENCE
-- ============================================================================
-- HS codes for agricultural commodities with conversion factors

INSERT INTO silver.trade_commodity_reference
(hs_code_10, hs_code_6, commodity_group, commodity_name, flow_type, source_unit, display_unit, conversion_factor, notes)
VALUES

-- ============================================================================
-- SOYBEANS (exports and imports)
-- ============================================================================
('1201900095', '120190', 'SOYBEANS', 'Soybeans, other than seed', 'EXPORT', 'KG', 'Million Bushels', 0.0000367437, 'KG to million bushels: /1000/27.2155'),
('1201100000', '120110', 'SOYBEANS', 'Soybeans, seed', 'EXPORT', 'KG', 'Million Bushels', 0.0000367437, 'KG to million bushels'),
('1201900095', '120190', 'SOYBEANS', 'Soybeans, other than seed', 'IMPORT', 'KG', 'Million Bushels', 0.0000367437, 'KG to million bushels'),
('1201100000', '120110', 'SOYBEANS', 'Soybeans, seed', 'IMPORT', 'KG', 'Million Bushels', 0.0000367437, 'KG to million bushels'),

-- ============================================================================
-- SOYBEAN MEAL (exports and imports)
-- Note: Import code 2304000000 was split in 2023:
--   Pre-2023: 2304000000 (all)
--   2023+: 2304000010 (organic) + 2304000090 (NES/conventional)
-- Export code 2304000000 remains unchanged.
-- 2302500000 (bran/hulls) reports in MT, not KG!
-- ============================================================================
-- Exports
('2304000000', '230400', 'SOYBEAN_MEAL', 'Soybean oilcake and meal', 'EXPORT', 'KG', '1,000 MT', 0.000001, 'KG to 1000 MT'),
('2302500000', '230250', 'SOYBEAN_MEAL', 'Bran from legumes (soybean hulls)', 'EXPORT', 'MT', '1,000 MT', 0.001, 'MT to 1000 MT - Census reports in MT not KG'),
('1208100000', '120810', 'SOYBEAN_MEAL', 'Soy flour and meal', 'EXPORT', 'KG', '1,000 MT', 0.000001, 'KG to 1000 MT - legacy code'),
('1208100010', '120810', 'SOYBEAN_MEAL', 'Soy flour and meal, organic', 'EXPORT', 'KG', '1,000 MT', 0.000001, 'KG to 1000 MT'),
('1208100090', '120810', 'SOYBEAN_MEAL', 'Soy flour and meal, NES', 'EXPORT', 'KG', '1,000 MT', 0.000001, 'KG to 1000 MT'),
-- Imports (pre-2023)
('2304000000', '230400', 'SOYBEAN_MEAL', 'Soybean oilcake and meal', 'IMPORT', 'KG', '1,000 MT', 0.000001, 'KG to 1000 MT - pre-2023 imports'),
-- Imports (2023+ split codes)
('2304000010', '230400', 'SOYBEAN_MEAL', 'Soybean oilcake and meal, organic', 'IMPORT', 'KG', '1,000 MT', 0.000001, 'KG to 1000 MT - imports 2023+ organic'),
('2304000090', '230400', 'SOYBEAN_MEAL', 'Soybean oilcake and meal, NES', 'IMPORT', 'KG', '1,000 MT', 0.000001, 'KG to 1000 MT - imports 2023+ conventional'),
-- Imports (other)
('2302500000', '230250', 'SOYBEAN_MEAL', 'Bran from legumes (soybean hulls)', 'IMPORT', 'MT', '1,000 MT', 0.001, 'MT to 1000 MT - Census reports in MT not KG'),
('1208100000', '120810', 'SOYBEAN_MEAL', 'Soy flour and meal', 'IMPORT', 'KG', '1,000 MT', 0.000001, 'KG to 1000 MT - legacy code'),
('1208100010', '120810', 'SOYBEAN_MEAL', 'Soy flour and meal, organic', 'IMPORT', 'KG', '1,000 MT', 0.000001, 'KG to 1000 MT'),
('1208100090', '120810', 'SOYBEAN_MEAL', 'Soy flour and meal, NES', 'IMPORT', 'KG', '1,000 MT', 0.000001, 'KG to 1000 MT'),

-- ============================================================================
-- SOYBEAN OIL (exports and imports)
-- ============================================================================
('1507100000', '150710', 'SOYBEAN_OIL', 'Crude soybean oil', 'EXPORT', 'KG', 'Million Pounds', 0.0000022046, 'KG to million lbs'),
('1507904050', '150790', 'SOYBEAN_OIL', 'Soybean oil, refined', 'EXPORT', 'KG', 'Million Pounds', 0.0000022046, 'KG to million lbs'),
('1507100000', '150710', 'SOYBEAN_OIL', 'Crude soybean oil', 'IMPORT', 'KG', 'Million Pounds', 0.0000022046, 'KG to million lbs'),
('1507904050', '150790', 'SOYBEAN_OIL', 'Soybean oil, refined', 'IMPORT', 'KG', 'Million Pounds', 0.0000022046, 'KG to million lbs'),

-- ============================================================================
-- CORN (exports and imports)
-- ============================================================================
('1005902030', '100590', 'CORN', 'Corn, other than seed', 'EXPORT', 'KG', 'Million Bushels', 0.0000393683, 'KG to million bushels: /1000/25.4'),
('1005100000', '100510', 'CORN', 'Corn seed', 'EXPORT', 'KG', 'Million Bushels', 0.0000393683, 'KG to million bushels'),
('1005902030', '100590', 'CORN', 'Corn, other than seed', 'IMPORT', 'KG', 'Million Bushels', 0.0000393683, 'KG to million bushels'),
('1005100000', '100510', 'CORN', 'Corn seed', 'IMPORT', 'KG', 'Million Bushels', 0.0000393683, 'KG to million bushels'),

-- ============================================================================
-- WHEAT (exports and imports)
-- ============================================================================
('1001992055', '100199', 'WHEAT', 'Wheat, other than seed', 'EXPORT', 'KG', 'Million Bushels', 0.0000367437, 'KG to million bushels'),
('1001910000', '100191', 'WHEAT', 'Wheat seed', 'EXPORT', 'KG', 'Million Bushels', 0.0000367437, 'KG to million bushels'),
('1001992055', '100199', 'WHEAT', 'Wheat, other than seed', 'IMPORT', 'KG', 'Million Bushels', 0.0000367437, 'KG to million bushels'),
('1001910000', '100191', 'WHEAT', 'Wheat seed', 'IMPORT', 'KG', 'Million Bushels', 0.0000367437, 'KG to million bushels'),

-- ============================================================================
-- DDGS (exports and imports)
-- ============================================================================
('2303300000', '230330', 'DDGS', 'Distillers dried grains with solubles', 'EXPORT', 'KG', '1,000 MT', 0.000001, 'KG to 1000 MT'),
('2303300000', '230330', 'DDGS', 'Distillers dried grains with solubles', 'IMPORT', 'KG', '1,000 MT', 0.000001, 'KG to 1000 MT'),

-- ============================================================================
-- CANOLA/RAPESEED (exports and imports)
-- ============================================================================
('1205100000', '120510', 'CANOLA', 'Canola/rapeseed', 'EXPORT', 'KG', '1,000 MT', 0.000001, 'KG to 1000 MT'),
('1205100000', '120510', 'CANOLA', 'Canola/rapeseed', 'IMPORT', 'KG', '1,000 MT', 0.000001, 'KG to 1000 MT');

-- ============================================================================
-- GRANT PERMISSIONS
-- ============================================================================
GRANT SELECT ON silver.trade_country_reference TO PUBLIC;
GRANT SELECT ON silver.trade_commodity_reference TO PUBLIC;
GRANT SELECT ON silver.trade_region_reference TO PUBLIC;

-- ============================================================================
-- VERIFICATION QUERIES
-- ============================================================================
-- Run these to verify the data loaded correctly:

-- SELECT region, COUNT(*) as country_count FROM silver.trade_country_reference GROUP BY region ORDER BY MIN(region_sort_order);
-- SELECT commodity_group, COUNT(*) as hs_code_count FROM silver.trade_commodity_reference GROUP BY commodity_group;
-- SELECT * FROM silver.trade_region_reference ORDER BY sort_order;
