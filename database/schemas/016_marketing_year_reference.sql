-- ============================================================================
-- MARKETING YEAR REFERENCE - Silver Layer
-- ============================================================================
-- Comprehensive mapping of country/commodity to marketing year definitions
-- Source: USDA FAS PSD Database conventions
-- ============================================================================

-- Trade year conventions by commodity group (global standards)
CREATE TABLE IF NOT EXISTS silver.trade_year_convention (
    id SERIAL PRIMARY KEY,
    commodity_group VARCHAR(50) NOT NULL UNIQUE,
    ty_begin_month INT NOT NULL,
    ty_end_month INT NOT NULL,
    period_description VARCHAR(100),
    notes TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Marketing year definitions by country/commodity
CREATE TABLE IF NOT EXISTS silver.marketing_year_reference (
    id SERIAL PRIMARY KEY,
    country VARCHAR(100) NOT NULL,
    country_code VARCHAR(10) NOT NULL,
    commodity VARCHAR(100) NOT NULL,
    commodity_group VARCHAR(50) NOT NULL,
    my_begin_month INT NOT NULL,  -- 1-12
    my_end_month INT NOT NULL,    -- 1-12
    my_label_format VARCHAR(20),  -- e.g., "2024/25" or "2024"
    ty_begin_month INT NOT NULL,  -- Trade year begin
    ty_end_month INT NOT NULL,    -- Trade year end
    southern_hemisphere BOOLEAN DEFAULT FALSE,
    notes TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW(),

    UNIQUE (country_code, commodity)
);

-- Create indexes for common lookups
CREATE INDEX IF NOT EXISTS idx_my_ref_country ON silver.marketing_year_reference(country_code);
CREATE INDEX IF NOT EXISTS idx_my_ref_commodity ON silver.marketing_year_reference(commodity);
CREATE INDEX IF NOT EXISTS idx_my_ref_group ON silver.marketing_year_reference(commodity_group);

-- ============================================================================
-- HELPER FUNCTION: Calculate marketing year from date
-- ============================================================================

CREATE OR REPLACE FUNCTION silver.get_marketing_year(
    p_country_code VARCHAR,
    p_commodity VARCHAR,
    p_year INT,
    p_month INT
) RETURNS VARCHAR AS $$
DECLARE
    v_my_begin INT;
    v_southern BOOLEAN;
    v_my_year INT;
    v_my_label VARCHAR;
BEGIN
    -- Get marketing year definition
    SELECT my_begin_month, southern_hemisphere
    INTO v_my_begin, v_southern
    FROM silver.marketing_year_reference
    WHERE country_code = p_country_code
    AND UPPER(commodity) = UPPER(p_commodity);

    -- Default to Sep-Aug if not found
    IF v_my_begin IS NULL THEN
        v_my_begin := 9;
        v_southern := FALSE;
    END IF;

    -- Calculate which marketing year this date falls into
    IF p_month >= v_my_begin THEN
        v_my_year := p_year;
    ELSE
        v_my_year := p_year - 1;
    END IF;

    -- Format label (handles both split-year and calendar formats)
    IF v_my_begin = 1 THEN
        -- Calendar year commodity
        v_my_label := v_my_year::TEXT;
    ELSE
        -- Split year format
        v_my_label := v_my_year || '/' || RIGHT((v_my_year + 1)::TEXT, 2);
    END IF;

    RETURN v_my_label;
END;
$$ LANGUAGE plpgsql;

-- ============================================================================
-- HELPER FUNCTION: Get marketing year end year (for sorting)
-- ============================================================================

CREATE OR REPLACE FUNCTION silver.get_marketing_year_end(
    p_country_code VARCHAR,
    p_commodity VARCHAR,
    p_year INT,
    p_month INT
) RETURNS INT AS $$
DECLARE
    v_my_begin INT;
BEGIN
    SELECT my_begin_month
    INTO v_my_begin
    FROM silver.marketing_year_reference
    WHERE country_code = p_country_code
    AND UPPER(commodity) = UPPER(p_commodity);

    -- Default to Sep-Aug if not found
    IF v_my_begin IS NULL THEN
        v_my_begin := 9;
    END IF;

    -- Return the end year of the marketing year
    IF p_month >= v_my_begin THEN
        RETURN p_year + 1;
    ELSE
        RETURN p_year;
    END IF;
END;
$$ LANGUAGE plpgsql;

-- ============================================================================
-- GRANT PERMISSIONS
-- ============================================================================
GRANT SELECT ON silver.trade_year_convention TO PUBLIC;
GRANT SELECT ON silver.marketing_year_reference TO PUBLIC;
GRANT EXECUTE ON FUNCTION silver.get_marketing_year TO PUBLIC;
GRANT EXECUTE ON FUNCTION silver.get_marketing_year_end TO PUBLIC;

COMMENT ON TABLE silver.marketing_year_reference IS 'USDA PSD marketing year definitions by country/commodity. Key insight: For Southern Hemisphere summer crops (Brazil/Argentina soybeans, corn), the SECOND year in the label indicates MY start.';
COMMENT ON FUNCTION silver.get_marketing_year IS 'Calculate marketing year label for a given country/commodity/date';
