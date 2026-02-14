-- ============================================================================
-- PRICE DATABASE SCHEMA
-- Medallion architecture for commodity price data
-- Round Lakes Commodities
-- ============================================================================

-- ============================================================================
-- BRONZE LAYER - Raw price report data
-- ============================================================================

CREATE TABLE IF NOT EXISTS bronze.price_report_raw (
    id SERIAL PRIMARY KEY,
    slug_id VARCHAR(20) NOT NULL,
    slug_name VARCHAR(50),
    report_title VARCHAR(200),
    report_date DATE NOT NULL,
    published_date TIMESTAMP WITH TIME ZONE,
    office_name VARCHAR(100),
    office_city VARCHAR(50),
    office_state VARCHAR(10),
    market_type VARCHAR(100),
    report_narrative TEXT,
    raw_response JSONB,
    source VARCHAR(50) DEFAULT 'USDA_AMS',
    collected_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    batch_id UUID,
    UNIQUE(slug_id, report_date)
);

CREATE INDEX IF NOT EXISTS idx_price_raw_slug ON bronze.price_report_raw(slug_id);
CREATE INDEX IF NOT EXISTS idx_price_raw_date ON bronze.price_report_raw(report_date);
CREATE INDEX IF NOT EXISTS idx_price_raw_collected ON bronze.price_report_raw(collected_at);

-- ============================================================================
-- SILVER LAYER - Parsed/standardized price observations
-- ============================================================================

CREATE TABLE IF NOT EXISTS silver.cash_price (
    id SERIAL PRIMARY KEY,
    report_date DATE NOT NULL,
    commodity VARCHAR(50) NOT NULL,           -- corn, soybeans, wheat_srw, wheat_hrw, etc.
    commodity_class VARCHAR(50),              -- yellow, #2, etc.
    location_name VARCHAR(100),               -- State or specific market
    location_city VARCHAR(50),
    location_state VARCHAR(10),
    region VARCHAR(50),                       -- Corn Belt, Delta, PNW, etc.

    -- Price data
    price_cash DECIMAL(10,4),                 -- Cash price ($/bu or $/unit)
    price_low DECIMAL(10,4),
    price_high DECIMAL(10,4),
    basis DECIMAL(10,4),                      -- Basis to futures
    basis_month VARCHAR(10),                  -- H, K, N, U, Z (futures month)
    change_daily DECIMAL(10,4),               -- Daily change
    change_direction VARCHAR(10),             -- up, down, unchanged, steady

    -- Units and source
    unit VARCHAR(20) DEFAULT '$/bushel',
    delivery_period VARCHAR(50),              -- spot, deferred, etc.

    -- Metadata
    source VARCHAR(50) DEFAULT 'USDA_AMS',
    slug_id VARCHAR(20),
    bronze_id INTEGER REFERENCES bronze.price_report_raw(id),
    parsed_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),

    UNIQUE(report_date, commodity, location_state, slug_id)
);

CREATE INDEX IF NOT EXISTS idx_cash_price_date ON silver.cash_price(report_date);
CREATE INDEX IF NOT EXISTS idx_cash_price_commodity ON silver.cash_price(commodity);
CREATE INDEX IF NOT EXISTS idx_cash_price_state ON silver.cash_price(location_state);
CREATE INDEX IF NOT EXISTS idx_cash_price_date_commodity ON silver.cash_price(report_date, commodity);

-- ============================================================================
-- SILVER LAYER - Futures prices (for basis calculations)
-- ============================================================================

CREATE TABLE IF NOT EXISTS silver.futures_price (
    id SERIAL PRIMARY KEY,
    trade_date DATE NOT NULL,
    symbol VARCHAR(20) NOT NULL,              -- ZC, ZS, ZW, KE, etc.
    contract_month VARCHAR(10) NOT NULL,      -- H26, K26, etc.
    contract_date DATE,                       -- First day of contract month

    -- OHLC data
    open_price DECIMAL(10,4),
    high_price DECIMAL(10,4),
    low_price DECIMAL(10,4),
    settlement DECIMAL(10,4),

    -- Volume and interest
    volume INTEGER,
    open_interest INTEGER,

    -- Metadata
    exchange VARCHAR(20),                     -- CBOT, CME, NYMEX
    source VARCHAR(50),
    collected_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),

    UNIQUE(trade_date, symbol, contract_month)
);

CREATE INDEX IF NOT EXISTS idx_futures_date ON silver.futures_price(trade_date);
CREATE INDEX IF NOT EXISTS idx_futures_symbol ON silver.futures_price(symbol);

-- ============================================================================
-- SILVER LAYER - Specialty prices (feedstocks, co-products, etc.)
-- ============================================================================

CREATE TABLE IF NOT EXISTS silver.specialty_price (
    id SERIAL PRIMARY KEY,
    report_date DATE NOT NULL,
    category VARCHAR(50) NOT NULL,            -- feedstock, co_product, protein_meal, veg_oil, livestock, energy
    commodity VARCHAR(100) NOT NULL,          -- specific product name

    -- Price data
    price_low DECIMAL(12,4),
    price_high DECIMAL(12,4),
    price_avg DECIMAL(12,4),
    price_change DECIMAL(12,4),

    -- Location and delivery
    location VARCHAR(100),
    delivery_point VARCHAR(100),              -- FOB, Delivered, etc.

    -- Units
    unit VARCHAR(30),                         -- $/ton, cents/lb, $/cwt, etc.

    -- Metadata
    source VARCHAR(50),
    slug_id VARCHAR(20),
    bronze_id INTEGER REFERENCES bronze.price_report_raw(id),
    parsed_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),

    UNIQUE(report_date, category, commodity, location)
);

CREATE INDEX IF NOT EXISTS idx_specialty_date ON silver.specialty_price(report_date);
CREATE INDEX IF NOT EXISTS idx_specialty_category ON silver.specialty_price(category);
CREATE INDEX IF NOT EXISTS idx_specialty_commodity ON silver.specialty_price(commodity);

-- ============================================================================
-- GOLD LAYER - Price analysis views
-- ============================================================================

-- Current cash prices with basis
CREATE OR REPLACE VIEW gold.current_cash_prices AS
SELECT
    cp.commodity,
    cp.location_state,
    cp.location_name,
    cp.report_date,
    cp.price_cash,
    cp.basis,
    cp.basis_month,
    cp.change_daily,
    cp.change_direction,
    -- 5-day average
    AVG(cp.price_cash) OVER (
        PARTITION BY cp.commodity, cp.location_state
        ORDER BY cp.report_date
        ROWS 4 PRECEDING
    ) as price_5day_avg,
    -- Year-over-year comparison (approximate)
    LAG(cp.price_cash, 252) OVER (
        PARTITION BY cp.commodity, cp.location_state
        ORDER BY cp.report_date
    ) as price_year_ago
FROM silver.cash_price cp
WHERE cp.report_date >= CURRENT_DATE - INTERVAL '30 days';

-- Regional basis comparison
CREATE OR REPLACE VIEW gold.regional_basis AS
SELECT
    cp.report_date,
    cp.commodity,
    cp.location_state,
    cp.basis,
    cp.basis_month,
    -- Regional averages
    AVG(cp.basis) OVER (
        PARTITION BY cp.report_date, cp.commodity
    ) as national_avg_basis,
    cp.basis - AVG(cp.basis) OVER (
        PARTITION BY cp.report_date, cp.commodity
    ) as basis_vs_national
FROM silver.cash_price cp
WHERE cp.report_date >= CURRENT_DATE - INTERVAL '30 days'
  AND cp.basis IS NOT NULL;

-- Specialty price summary
CREATE OR REPLACE VIEW gold.specialty_price_summary AS
SELECT
    sp.category,
    sp.commodity,
    sp.report_date,
    sp.price_avg,
    sp.unit,
    sp.location,
    -- Week-over-week change
    LAG(sp.price_avg, 5) OVER (
        PARTITION BY sp.category, sp.commodity, sp.location
        ORDER BY sp.report_date
    ) as price_week_ago,
    sp.price_avg - LAG(sp.price_avg, 5) OVER (
        PARTITION BY sp.category, sp.commodity, sp.location
        ORDER BY sp.report_date
    ) as weekly_change
FROM silver.specialty_price sp
WHERE sp.report_date >= CURRENT_DATE - INTERVAL '90 days';

-- ============================================================================
-- REFERENCE TABLE - Report catalog
-- ============================================================================

CREATE TABLE IF NOT EXISTS config.price_report_catalog (
    id SERIAL PRIMARY KEY,
    slug_id VARCHAR(20) NOT NULL UNIQUE,
    slug_name VARCHAR(50),
    report_title VARCHAR(200),
    category VARCHAR(50),                     -- grain, livestock, energy, feedstock, etc.
    commodities TEXT[],                       -- Array of commodities in report
    region VARCHAR(50),
    state VARCHAR(10),
    frequency VARCHAR(20),                    -- daily, weekly
    is_active BOOLEAN DEFAULT true,
    last_collected TIMESTAMP WITH TIME ZONE,
    notes TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- ============================================================================
-- FUNCTION - Parse grain narrative to extract prices
-- ============================================================================

CREATE OR REPLACE FUNCTION silver.parse_grain_narrative(
    p_narrative TEXT,
    p_report_date DATE,
    p_slug_id VARCHAR(20),
    p_bronze_id INTEGER,
    p_location_state VARCHAR(10),
    p_location_name VARCHAR(100)
) RETURNS INTEGER AS $$
DECLARE
    v_records_inserted INTEGER := 0;
    v_corn_match TEXT[];
    v_soy_match TEXT[];
    v_wheat_match TEXT[];
    v_price DECIMAL;
    v_basis DECIMAL;
    v_basis_month VARCHAR(10);
    v_change DECIMAL;
    v_direction VARCHAR(10);
BEGIN
    -- Parse corn: "Corn -- $3.91 (-.33H) Up 3 cents"
    IF p_narrative ~* 'corn\s*[-–]+\s*\$?([\d.]+)\s*\(([+-]?[\d.]+)([A-Z])\)' THEN
        v_corn_match := regexp_match(p_narrative, 'corn\s*[-–]+\s*\$?([\d.]+)\s*\(([+-]?[\d.]+)([A-Z])\)', 'i');
        IF v_corn_match IS NOT NULL THEN
            v_price := v_corn_match[1]::DECIMAL;
            v_basis := v_corn_match[2]::DECIMAL;
            v_basis_month := v_corn_match[3];

            -- Extract change direction
            IF p_narrative ~* 'corn.*up\s+(\d+)' THEN
                v_direction := 'up';
                v_change := (regexp_match(p_narrative, 'corn.*up\s+(\d+)', 'i'))[1]::DECIMAL / 100;
            ELSIF p_narrative ~* 'corn.*down\s+(\d+)' THEN
                v_direction := 'down';
                v_change := -(regexp_match(p_narrative, 'corn.*down\s+(\d+)', 'i'))[1]::DECIMAL / 100;
            ELSE
                v_direction := 'unchanged';
                v_change := 0;
            END IF;

            INSERT INTO silver.cash_price (
                report_date, commodity, location_state, location_name,
                price_cash, basis, basis_month, change_daily, change_direction,
                source, slug_id, bronze_id
            ) VALUES (
                p_report_date, 'corn', p_location_state, p_location_name,
                v_price, v_basis, v_basis_month, v_change, v_direction,
                'USDA_AMS', p_slug_id, p_bronze_id
            )
            ON CONFLICT (report_date, commodity, location_state, slug_id)
            DO UPDATE SET
                price_cash = EXCLUDED.price_cash,
                basis = EXCLUDED.basis,
                change_daily = EXCLUDED.change_daily,
                parsed_at = NOW();

            v_records_inserted := v_records_inserted + 1;
        END IF;
    END IF;

    -- Parse soybeans: "Soybeans -- $9.91 (-.74H) Unchanged"
    IF p_narrative ~* 'soybean[s]?\s*[-–]+\s*\$?([\d.]+)\s*\(([+-]?[\d.]+)([A-Z])\)' THEN
        v_soy_match := regexp_match(p_narrative, 'soybean[s]?\s*[-–]+\s*\$?([\d.]+)\s*\(([+-]?[\d.]+)([A-Z])\)', 'i');
        IF v_soy_match IS NOT NULL THEN
            v_price := v_soy_match[1]::DECIMAL;
            v_basis := v_soy_match[2]::DECIMAL;
            v_basis_month := v_soy_match[3];

            IF p_narrative ~* 'soybean.*up\s+(\d+)' THEN
                v_direction := 'up';
                v_change := (regexp_match(p_narrative, 'soybean.*up\s+(\d+)', 'i'))[1]::DECIMAL / 100;
            ELSIF p_narrative ~* 'soybean.*down\s+(\d+)' THEN
                v_direction := 'down';
                v_change := -(regexp_match(p_narrative, 'soybean.*down\s+(\d+)', 'i'))[1]::DECIMAL / 100;
            ELSE
                v_direction := 'unchanged';
                v_change := 0;
            END IF;

            INSERT INTO silver.cash_price (
                report_date, commodity, location_state, location_name,
                price_cash, basis, basis_month, change_daily, change_direction,
                source, slug_id, bronze_id
            ) VALUES (
                p_report_date, 'soybeans', p_location_state, p_location_name,
                v_price, v_basis, v_basis_month, v_change, v_direction,
                'USDA_AMS', p_slug_id, p_bronze_id
            )
            ON CONFLICT (report_date, commodity, location_state, slug_id)
            DO UPDATE SET
                price_cash = EXCLUDED.price_cash,
                basis = EXCLUDED.basis,
                change_daily = EXCLUDED.change_daily,
                parsed_at = NOW();

            v_records_inserted := v_records_inserted + 1;
        END IF;
    END IF;

    RETURN v_records_inserted;
END;
$$ LANGUAGE plpgsql;

-- ============================================================================
-- GRANTS
-- ============================================================================

GRANT SELECT ON ALL TABLES IN SCHEMA bronze TO PUBLIC;
GRANT SELECT ON ALL TABLES IN SCHEMA silver TO PUBLIC;
GRANT SELECT ON ALL TABLES IN SCHEMA gold TO PUBLIC;
GRANT SELECT ON ALL TABLES IN SCHEMA config TO PUBLIC;
