-- ============================================================================
-- HISTORICAL COUNTRY CODE MAPPINGS - Silver Layer
-- ============================================================================
-- Maps dissolved/renamed countries to their current successor countries
-- for Census Bureau trade data from 1993 to present.
--
-- Source: Census Bureau country code conventions
-- Special Markers:
--   (!) - Obsolete country code in Census data
--   (*) - Value is summarized from component countries
--
-- Created: 2026-02-07
-- ============================================================================

-- Main table for historical country mappings
CREATE TABLE IF NOT EXISTS silver.trade_country_historical (
    id SERIAL PRIMARY KEY,
    historical_code VARCHAR(10) NOT NULL,
    historical_name VARCHAR(100) NOT NULL,
    current_code VARCHAR(10),
    current_name VARCHAR(100),
    valid_from DATE NOT NULL DEFAULT '1993-01-01',
    valid_to DATE,
    dissolution_date DATE,
    is_primary_successor BOOLEAN DEFAULT FALSE,
    successor_countries JSONB,
    region VARCHAR(50),
    is_obsolete BOOLEAN DEFAULT FALSE,
    notes TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),

    UNIQUE(historical_code, current_code, valid_from)
);

-- Table for country name variations/aliases
CREATE TABLE IF NOT EXISTS silver.trade_country_alias (
    id SERIAL PRIMARY KEY,
    alias_name VARCHAR(100) NOT NULL,
    canonical_name VARCHAR(100) NOT NULL,
    census_code VARCHAR(10),
    notes TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW(),

    UNIQUE(alias_name)
);

-- Create indexes for common lookups
CREATE INDEX IF NOT EXISTS idx_country_hist_code ON silver.trade_country_historical(historical_code);
CREATE INDEX IF NOT EXISTS idx_country_hist_current ON silver.trade_country_historical(current_code);
CREATE INDEX IF NOT EXISTS idx_country_hist_dates ON silver.trade_country_historical(valid_from, valid_to);
CREATE INDEX IF NOT EXISTS idx_country_hist_obsolete ON silver.trade_country_historical(is_obsolete);
CREATE INDEX IF NOT EXISTS idx_country_alias_name ON silver.trade_country_alias(alias_name);

-- ============================================================================
-- HELPER FUNCTION: Map historical country to current successor
-- ============================================================================

CREATE OR REPLACE FUNCTION silver.map_historical_country(
    p_historical_code VARCHAR,
    p_trade_date DATE DEFAULT NULL
) RETURNS TABLE(
    current_code VARCHAR,
    current_name VARCHAR,
    is_primary BOOLEAN,
    region VARCHAR
) AS $$
BEGIN
    -- If no trade date provided, use current date
    IF p_trade_date IS NULL THEN
        p_trade_date := CURRENT_DATE;
    END IF;

    -- Check if this is a historical/obsolete code
    RETURN QUERY
    SELECT
        tch.current_code::VARCHAR,
        tch.current_name::VARCHAR,
        tch.is_primary_successor,
        tch.region::VARCHAR
    FROM silver.trade_country_historical tch
    WHERE tch.historical_code = p_historical_code
      AND (tch.dissolution_date IS NULL OR p_trade_date >= tch.dissolution_date)
      AND tch.current_code IS NOT NULL
    ORDER BY tch.is_primary_successor DESC;

    -- If no mapping found, return the original code
    IF NOT FOUND THEN
        RETURN QUERY SELECT
            p_historical_code::VARCHAR,
            NULL::VARCHAR,
            TRUE,
            NULL::VARCHAR;
    END IF;
END;
$$ LANGUAGE plpgsql;

-- ============================================================================
-- HELPER FUNCTION: Get primary successor country
-- ============================================================================

CREATE OR REPLACE FUNCTION silver.get_primary_successor(
    p_historical_code VARCHAR
) RETURNS VARCHAR AS $$
DECLARE
    v_successor VARCHAR;
BEGIN
    SELECT current_code INTO v_successor
    FROM silver.trade_country_historical
    WHERE historical_code = p_historical_code
      AND is_primary_successor = TRUE
    LIMIT 1;

    -- Return original if no mapping found
    RETURN COALESCE(v_successor, p_historical_code);
END;
$$ LANGUAGE plpgsql;

-- ============================================================================
-- HELPER FUNCTION: Normalize country name from alias
-- ============================================================================

CREATE OR REPLACE FUNCTION silver.normalize_country_name(
    p_name VARCHAR
) RETURNS VARCHAR AS $$
DECLARE
    v_canonical VARCHAR;
BEGIN
    SELECT canonical_name INTO v_canonical
    FROM silver.trade_country_alias
    WHERE UPPER(alias_name) = UPPER(p_name);

    RETURN COALESCE(v_canonical, p_name);
END;
$$ LANGUAGE plpgsql;

-- ============================================================================
-- POPULATE HISTORICAL MAPPINGS
-- ============================================================================

-- Clear existing data for fresh insert
DELETE FROM silver.trade_country_historical;
DELETE FROM silver.trade_country_alias;

-- USSR (dissolved 1991-12-26)
INSERT INTO silver.trade_country_historical
(historical_code, historical_name, current_code, current_name, valid_from, valid_to, dissolution_date, is_primary_successor, region, is_obsolete, notes)
VALUES
('4620', 'U.S.S.R.', '4621', 'Russia', '1993-01-01', '1991-12-26', '1991-12-26', TRUE, 'FSU', TRUE, 'Primary successor'),
('4620', 'U.S.S.R.', '4622', 'Belarus', '1993-01-01', '1991-12-26', '1991-12-26', FALSE, 'FSU', TRUE, NULL),
('4620', 'U.S.S.R.', '4623', 'Ukraine', '1993-01-01', '1991-12-26', '1991-12-26', FALSE, 'FSU', TRUE, NULL),
('4620', 'U.S.S.R.', '4631', 'Armenia', '1993-01-01', '1991-12-26', '1991-12-26', FALSE, 'FSU', TRUE, NULL),
('4620', 'U.S.S.R.', '4633', 'Azerbaijan', '1993-01-01', '1991-12-26', '1991-12-26', FALSE, 'FSU', TRUE, NULL),
('4620', 'U.S.S.R.', '4635', 'Georgia', '1993-01-01', '1991-12-26', '1991-12-26', FALSE, 'FSU', TRUE, NULL),
('4620', 'U.S.S.R.', '4637', 'Kazakhstan', '1993-01-01', '1991-12-26', '1991-12-26', FALSE, 'FSU', TRUE, NULL),
('4620', 'U.S.S.R.', '4639', 'Kyrgyzstan', '1993-01-01', '1991-12-26', '1991-12-26', FALSE, 'FSU', TRUE, NULL),
('4620', 'U.S.S.R.', '4641', 'Moldova', '1993-01-01', '1991-12-26', '1991-12-26', FALSE, 'FSU', TRUE, NULL),
('4620', 'U.S.S.R.', '4643', 'Tajikistan', '1993-01-01', '1991-12-26', '1991-12-26', FALSE, 'FSU', TRUE, NULL),
('4620', 'U.S.S.R.', '4645', 'Turkmenistan', '1993-01-01', '1991-12-26', '1991-12-26', FALSE, 'FSU', TRUE, NULL),
('4620', 'U.S.S.R.', '4647', 'Uzbekistan', '1993-01-01', '1991-12-26', '1991-12-26', FALSE, 'FSU', TRUE, NULL),
('4620', 'U.S.S.R.', '4472', 'Estonia', '1993-01-01', '1991-12-26', '1991-12-26', FALSE, 'EU27', TRUE, 'Now EU member'),
('4620', 'U.S.S.R.', '4474', 'Latvia', '1993-01-01', '1991-12-26', '1991-12-26', FALSE, 'EU27', TRUE, 'Now EU member'),
('4620', 'U.S.S.R.', '4476', 'Lithuania', '1993-01-01', '1991-12-26', '1991-12-26', FALSE, 'EU27', TRUE, 'Now EU member');

-- Yugoslavia (dissolved 1992-04-27)
INSERT INTO silver.trade_country_historical
(historical_code, historical_name, current_code, current_name, valid_from, valid_to, dissolution_date, is_primary_successor, region, is_obsolete, notes)
VALUES
('4790', 'Yugoslavia', '4783', 'Serbia', '1993-01-01', '1992-04-27', '1992-04-27', TRUE, 'EUROPE_OTHER', TRUE, 'Primary successor'),
('4790', 'Yugoslavia', '4793', 'Slovenia', '1993-01-01', '1992-04-27', '1992-04-27', FALSE, 'EU27', TRUE, 'Now EU member, independence 1991-06-25'),
('4790', 'Yugoslavia', '4791', 'Croatia', '1993-01-01', '1992-04-27', '1992-04-27', FALSE, 'EU27', TRUE, 'Now EU member, independence 1991-06-25'),
('4790', 'Yugoslavia', '4227', 'Bosnia and Herzegovina', '1993-01-01', '1992-04-27', '1992-04-27', FALSE, 'EUROPE_OTHER', TRUE, 'Independence 1992-03-01'),
('4790', 'Yugoslavia', '4787', 'Montenegro', '1993-01-01', '1992-04-27', '1992-04-27', FALSE, 'EUROPE_OTHER', TRUE, 'Separated from Serbia 2006-06-03'),
('4790', 'Yugoslavia', '4785', 'Kosovo', '1993-01-01', '1992-04-27', '1992-04-27', FALSE, 'EUROPE_OTHER', TRUE, 'Declared independence 2008-02-17, disputed'),
('4790', 'Yugoslavia', '4789', 'North Macedonia', '1993-01-01', '1992-04-27', '1992-04-27', FALSE, 'EUROPE_OTHER', TRUE, 'Formerly FYROM, independence 1991-09-08');

-- Czechoslovakia (dissolved 1993-01-01 - Velvet Divorce)
INSERT INTO silver.trade_country_historical
(historical_code, historical_name, current_code, current_name, valid_from, valid_to, dissolution_date, is_primary_successor, region, is_obsolete, notes)
VALUES
('4350', 'Czechoslovakia', '4351', 'Czech Republic', '1993-01-01', '1992-12-31', '1993-01-01', TRUE, 'EU27', TRUE, 'Also known as Czechia, now EU member'),
('4350', 'Czechoslovakia', '4781', 'Slovakia', '1993-01-01', '1992-12-31', '1993-01-01', FALSE, 'EU27', TRUE, 'Now EU member');

-- East Germany (dissolved 1990-10-03 - German reunification)
INSERT INTO silver.trade_country_historical
(historical_code, historical_name, current_code, current_name, valid_from, valid_to, dissolution_date, is_primary_successor, region, is_obsolete, notes)
VALUES
('4281', 'German Democratic Republic', '4280', 'Germany', '1993-01-01', '1990-10-03', '1990-10-03', TRUE, 'EU27', TRUE, 'German reunification - merged into FRG');

-- Serbia and Montenegro (dissolved 2006-06-03)
INSERT INTO silver.trade_country_historical
(historical_code, historical_name, current_code, current_name, valid_from, valid_to, dissolution_date, is_primary_successor, region, is_obsolete, notes)
VALUES
('4795', 'Serbia and Montenegro', '4783', 'Serbia', '2003-02-04', '2006-06-03', '2006-06-03', TRUE, 'EUROPE_OTHER', TRUE, 'State Union that succeeded Federal Republic of Yugoslavia'),
('4795', 'Serbia and Montenegro', '4787', 'Montenegro', '2003-02-04', '2006-06-03', '2006-06-03', FALSE, 'EUROPE_OTHER', TRUE, NULL);

-- Netherlands Antilles (dissolved 2010-10-10)
INSERT INTO silver.trade_country_historical
(historical_code, historical_name, current_code, current_name, valid_from, valid_to, dissolution_date, is_primary_successor, region, is_obsolete, notes)
VALUES
('2772', 'Netherlands Antilles', '2773', 'Curacao', '1993-01-01', '2010-10-10', '2010-10-10', TRUE, 'WESTERN_HEMISPHERE', TRUE, 'Primary successor'),
('2772', 'Netherlands Antilles', '2774', 'Sint Maarten', '1993-01-01', '2010-10-10', '2010-10-10', FALSE, 'WESTERN_HEMISPHERE', TRUE, NULL),
('2772', 'Netherlands Antilles', '3515', 'Bonaire, Sint Eustatius and Saba', '1993-01-01', '2010-10-10', '2010-10-10', FALSE, 'WESTERN_HEMISPHERE', TRUE, 'BES islands - special municipalities of Netherlands');

-- ============================================================================
-- POPULATE COUNTRY NAME ALIASES
-- ============================================================================

INSERT INTO silver.trade_country_alias (alias_name, canonical_name, notes)
VALUES
('BURMA', 'MYANMAR', 'Official name change 1989'),
('ZAIRE', 'CONGO (KINSHASA)', 'Renamed to Democratic Republic of the Congo 1997'),
('IVORY COAST', 'COTE D IVOIRE', 'French name official'),
('SWAZILAND', 'ESWATINI', 'Official name change 2018'),
('FYROM', 'NORTH MACEDONIA', 'Former Yugoslav Republic of Macedonia'),
('MACEDONIA', 'NORTH MACEDONIA', 'Prespa Agreement 2019'),
('CZECH REPUBLIC', 'CZECHIA', 'Short name adopted 2016'),
('KOREA, REP OF (SOUTH)', 'KOREA, SOUTH', 'Standardized name'),
('KOREA, DEM PEOPLES REP (NORTH)', 'KOREA, NORTH', 'Standardized name'),
('RUSSIAN FEDERATION', 'RUSSIA', 'Short name'),
('GREAT BRITAIN', 'UNITED KINGDOM', 'GB is part of UK'),
('ENGLAND', 'UNITED KINGDOM', 'England is part of UK'),
('HOLLAND', 'NETHERLANDS', 'Holland is two provinces of Netherlands'),
('UAE', 'UNITED ARAB EMIRATES', 'Abbreviation');

-- ============================================================================
-- GRANT PERMISSIONS
-- ============================================================================

GRANT SELECT ON silver.trade_country_historical TO PUBLIC;
GRANT SELECT ON silver.trade_country_alias TO PUBLIC;
GRANT EXECUTE ON FUNCTION silver.map_historical_country TO PUBLIC;
GRANT EXECUTE ON FUNCTION silver.get_primary_successor TO PUBLIC;
GRANT EXECUTE ON FUNCTION silver.normalize_country_name TO PUBLIC;

COMMENT ON TABLE silver.trade_country_historical IS 'Historical country code mappings for Census Bureau trade data. Maps dissolved countries (USSR, Yugoslavia, etc.) to their successor states with date ranges.';
COMMENT ON TABLE silver.trade_country_alias IS 'Country name variations and aliases for normalizing trade partner names.';
COMMENT ON FUNCTION silver.map_historical_country IS 'Maps a historical country code to its current successor(s). Returns primary successor first.';
COMMENT ON FUNCTION silver.get_primary_successor IS 'Returns the primary successor country code for a dissolved country.';
COMMENT ON FUNCTION silver.normalize_country_name IS 'Normalizes country name aliases to canonical names (e.g., Burma -> Myanmar).';
