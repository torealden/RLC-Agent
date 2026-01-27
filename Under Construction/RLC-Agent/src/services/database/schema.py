"""
Commodity Database Schema

Comprehensive schema for storing all commodity market data:
- Price data (futures, cash, basis)
- Fundamental data (production, consumption, stocks, trade)
- Position data (COT, fund flows)
- Time series data (ethanol, petroleum, etc.)

Designed for:
- SQLite (development/local)
- PostgreSQL (production)
- Easy Power BI integration
"""

# SQLite compatible schema
SQLITE_SCHEMA = """
-- =============================================================================
-- REFERENCE TABLES
-- =============================================================================

-- Commodity master list
CREATE TABLE IF NOT EXISTS commodities (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    code VARCHAR(20) UNIQUE NOT NULL,
    name VARCHAR(100) NOT NULL,
    category VARCHAR(50),  -- grains, oilseeds, energy, livestock, softs
    subcategory VARCHAR(50),
    unit_standard VARCHAR(20),  -- bushel, metric_ton, gallon, barrel
    mt_conversion DECIMAL(10,6),  -- conversion factor to metric tons
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Country/Region master list
CREATE TABLE IF NOT EXISTS countries (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    code VARCHAR(10) UNIQUE NOT NULL,
    name VARCHAR(100) NOT NULL,
    region VARCHAR(50),
    is_major_producer BOOLEAN DEFAULT FALSE,
    is_major_exporter BOOLEAN DEFAULT FALSE,
    is_major_importer BOOLEAN DEFAULT FALSE
);

-- Data sources
CREATE TABLE IF NOT EXISTS data_sources (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    code VARCHAR(50) UNIQUE NOT NULL,
    name VARCHAR(100),
    url VARCHAR(500),
    data_type VARCHAR(50),  -- api, file, scrape
    update_frequency VARCHAR(20),
    is_active BOOLEAN DEFAULT TRUE
);

-- =============================================================================
-- PRICE DATA
-- =============================================================================

-- Futures prices (daily OHLCV)
CREATE TABLE IF NOT EXISTS futures_prices (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    commodity_code VARCHAR(20) NOT NULL,
    contract_symbol VARCHAR(20) NOT NULL,  -- ZCH25, ZSF25
    exchange VARCHAR(20),  -- CME, ICE, etc.
    trade_date DATE NOT NULL,
    open DECIMAL(12,4),
    high DECIMAL(12,4),
    low DECIMAL(12,4),
    close DECIMAL(12,4),
    settle DECIMAL(12,4),
    volume INTEGER,
    open_interest INTEGER,
    source VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(commodity_code, contract_symbol, trade_date)
);

CREATE INDEX IF NOT EXISTS idx_futures_commodity_date ON futures_prices(commodity_code, trade_date);
CREATE INDEX IF NOT EXISTS idx_futures_contract ON futures_prices(contract_symbol);

-- Cash/spot prices
CREATE TABLE IF NOT EXISTS cash_prices (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    commodity_code VARCHAR(20) NOT NULL,
    location VARCHAR(100),
    price_type VARCHAR(30),  -- bid, offer, last
    price DECIMAL(12,4) NOT NULL,
    basis DECIMAL(8,4),
    basis_month VARCHAR(10),  -- H25, K25 (contract month for basis)
    currency VARCHAR(3) DEFAULT 'USD',
    unit VARCHAR(20),
    trade_date DATE NOT NULL,
    source VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(commodity_code, location, price_type, trade_date)
);

CREATE INDEX IF NOT EXISTS idx_cash_commodity_date ON cash_prices(commodity_code, trade_date);
CREATE INDEX IF NOT EXISTS idx_cash_location ON cash_prices(location);

-- =============================================================================
-- FUNDAMENTAL DATA (Balance Sheets)
-- =============================================================================

-- Universal fundamentals table (flattened format like your soybean data)
CREATE TABLE IF NOT EXISTS fundamentals (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    commodity_code VARCHAR(20) NOT NULL,
    country_code VARCHAR(10),
    country_name VARCHAR(100),
    marketing_year VARCHAR(10),  -- 2024/25 or 2024
    calendar_year INTEGER,
    calendar_month INTEGER,
    metric VARCHAR(100) NOT NULL,  -- production, exports, crush, etc.
    value DECIMAL(18,4),
    unit VARCHAR(30),
    source VARCHAR(50),
    report_date DATE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(commodity_code, country_code, marketing_year, calendar_month, metric, source)
);

CREATE INDEX IF NOT EXISTS idx_fund_commodity ON fundamentals(commodity_code);
CREATE INDEX IF NOT EXISTS idx_fund_country ON fundamentals(country_code);
CREATE INDEX IF NOT EXISTS idx_fund_year ON fundamentals(marketing_year);
CREATE INDEX IF NOT EXISTS idx_fund_metric ON fundamentals(metric);

-- =============================================================================
-- TRADE DATA
-- =============================================================================

-- Export/Import trade flows
CREATE TABLE IF NOT EXISTS trade_flows (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    commodity_code VARCHAR(20) NOT NULL,
    hs_code VARCHAR(20),
    flow_type VARCHAR(10),  -- export, import
    origin_country VARCHAR(50),
    destination_country VARCHAR(50),
    quantity DECIMAL(18,4),
    quantity_unit VARCHAR(20),
    value_usd DECIMAL(18,2),
    trade_month DATE,
    source VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_trade_commodity ON trade_flows(commodity_code);
CREATE INDEX IF NOT EXISTS idx_trade_month ON trade_flows(trade_month);

-- Export sales (weekly USDA)
CREATE TABLE IF NOT EXISTS export_sales (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    commodity_code VARCHAR(20) NOT NULL,
    destination VARCHAR(100),
    marketing_year VARCHAR(10),
    week_ending DATE NOT NULL,
    weekly_sales DECIMAL(18,4),
    accumulated_sales DECIMAL(18,4),
    outstanding_sales DECIMAL(18,4),
    weekly_exports DECIMAL(18,4),
    accumulated_exports DECIMAL(18,4),
    unit VARCHAR(20),
    source VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_expsales_commodity ON export_sales(commodity_code);
CREATE INDEX IF NOT EXISTS idx_expsales_week ON export_sales(week_ending);

-- =============================================================================
-- POSITIONING DATA
-- =============================================================================

-- CFTC Commitments of Traders
CREATE TABLE IF NOT EXISTS cot_positions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    commodity_code VARCHAR(20) NOT NULL,
    contract_name VARCHAR(100),
    cftc_code VARCHAR(20),
    report_date DATE NOT NULL,
    -- Commercial
    commercial_long INTEGER,
    commercial_short INTEGER,
    commercial_net INTEGER,
    -- Non-Commercial (Managed Money / Specs)
    noncommercial_long INTEGER,
    noncommercial_short INTEGER,
    noncommercial_net INTEGER,
    noncommercial_spread INTEGER,
    -- Other
    nonreportable_long INTEGER,
    nonreportable_short INTEGER,
    -- Totals
    total_long INTEGER,
    total_short INTEGER,
    open_interest INTEGER,
    -- Changes
    change_noncommercial_net INTEGER,
    source VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(commodity_code, report_date)
);

CREATE INDEX IF NOT EXISTS idx_cot_commodity ON cot_positions(commodity_code);
CREATE INDEX IF NOT EXISTS idx_cot_date ON cot_positions(report_date);

-- =============================================================================
-- TIME SERIES DATA (Weekly/Daily Reports)
-- =============================================================================

-- Ethanol data (weekly EIA)
CREATE TABLE IF NOT EXISTS ethanol_weekly (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    report_date DATE NOT NULL,
    production INTEGER,  -- thousand barrels
    stocks INTEGER,
    imports INTEGER,
    exports INTEGER,
    implied_demand INTEGER,
    implied_corn_grind DECIMAL(12,2),  -- million bushels
    blending_rate DECIMAL(8,4),
    source VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(report_date)
);

-- Petroleum data
CREATE TABLE IF NOT EXISTS petroleum_prices (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    series_code VARCHAR(50) NOT NULL,
    series_name VARCHAR(200),
    report_date DATE NOT NULL,
    value DECIMAL(12,4),
    unit VARCHAR(30),
    source VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(series_code, report_date)
);

-- Crop progress (weekly NASS)
CREATE TABLE IF NOT EXISTS crop_progress (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    commodity_code VARCHAR(20) NOT NULL,
    state_code VARCHAR(10),  -- US for national
    report_year INTEGER,
    week_ending DATE NOT NULL,
    metric VARCHAR(50),  -- planted, emerged, blooming, harvested
    value DECIMAL(8,2),  -- percent
    source VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(commodity_code, state_code, week_ending, metric)
);

-- Drought monitor
CREATE TABLE IF NOT EXISTS drought_data (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    region VARCHAR(50),  -- state or US
    report_date DATE NOT NULL,
    none_pct DECIMAL(6,2),
    d0_pct DECIMAL(6,2),  -- abnormally dry
    d1_pct DECIMAL(6,2),  -- moderate drought
    d2_pct DECIMAL(6,2),  -- severe drought
    d3_pct DECIMAL(6,2),  -- extreme drought
    d4_pct DECIMAL(6,2),  -- exceptional drought
    dsci DECIMAL(8,2),  -- drought severity index
    source VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(region, report_date)
);

-- =============================================================================
-- BRAZIL SPECIFIC
-- =============================================================================

-- Brazil state-level data (IMEA, CONAB)
CREATE TABLE IF NOT EXISTS brazil_state_data (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    commodity_code VARCHAR(20) NOT NULL,
    state_code VARCHAR(10),  -- MT, PR, RS, GO, etc.
    marketing_year VARCHAR(10),
    report_date DATE,
    metric VARCHAR(100),  -- planted_area, harvested_area, production, yield, progress
    value DECIMAL(18,4),
    unit VARCHAR(30),
    source VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Brazil crush data (ABIOVE)
CREATE TABLE IF NOT EXISTS brazil_crush (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    report_month DATE NOT NULL,
    soybeans_crushed DECIMAL(12,2),  -- thousand tons
    meal_production DECIMAL(12,2),
    oil_production DECIMAL(12,2),
    crush_capacity_utilization DECIMAL(6,2),
    source VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(report_month)
);

-- =============================================================================
-- ARGENTINA SPECIFIC
-- =============================================================================

CREATE TABLE IF NOT EXISTS argentina_data (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    commodity_code VARCHAR(20) NOT NULL,
    province VARCHAR(100),
    marketing_year VARCHAR(10),
    report_date DATE,
    metric VARCHAR(100),
    value DECIMAL(18,4),
    unit VARCHAR(30),
    source VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- =============================================================================
-- DATA IMPORT TRACKING
-- =============================================================================

CREATE TABLE IF NOT EXISTS data_imports (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    filename VARCHAR(500),
    source_type VARCHAR(50),  -- excel, csv, api
    records_imported INTEGER,
    records_updated INTEGER,
    records_failed INTEGER,
    import_started TIMESTAMP,
    import_completed TIMESTAMP,
    status VARCHAR(20),  -- success, partial, failed
    error_message TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- =============================================================================
-- VIEWS FOR COMMON QUERIES
-- =============================================================================

-- Latest futures prices by commodity
CREATE VIEW IF NOT EXISTS v_latest_futures AS
SELECT
    commodity_code,
    contract_symbol,
    trade_date,
    settle,
    volume,
    open_interest
FROM futures_prices fp
WHERE trade_date = (
    SELECT MAX(trade_date)
    FROM futures_prices
    WHERE commodity_code = fp.commodity_code
);

-- World balance sheet summary
CREATE VIEW IF NOT EXISTS v_world_balance AS
SELECT
    commodity_code,
    marketing_year,
    metric,
    SUM(value) as total_value,
    unit
FROM fundamentals
WHERE country_code = 'WORLD' OR country_name = 'World'
GROUP BY commodity_code, marketing_year, metric, unit;

-- Latest COT positions
CREATE VIEW IF NOT EXISTS v_latest_cot AS
SELECT *
FROM cot_positions
WHERE report_date = (SELECT MAX(report_date) FROM cot_positions);
"""

# Insert default reference data
DEFAULT_COMMODITIES = """
INSERT OR IGNORE INTO commodities (code, name, category, subcategory, unit_standard, mt_conversion) VALUES
('CORN', 'Corn', 'grains', 'coarse_grains', 'bushel', 39.368),
('SOYBEANS', 'Soybeans', 'oilseeds', 'soybeans', 'bushel', 36.744),
('WHEAT_SRW', 'Soft Red Winter Wheat', 'grains', 'wheat', 'bushel', 36.744),
('WHEAT_HRW', 'Hard Red Winter Wheat', 'grains', 'wheat', 'bushel', 36.744),
('WHEAT_HRS', 'Hard Red Spring Wheat', 'grains', 'wheat', 'bushel', 36.744),
('SOYBEAN_MEAL', 'Soybean Meal', 'oilseeds', 'soy_products', 'short_ton', 1.102),
('SOYBEAN_OIL', 'Soybean Oil', 'oilseeds', 'soy_products', 'pound', 2204.62),
('CANOLA', 'Canola/Rapeseed', 'oilseeds', 'canola', 'metric_ton', 1.0),
('PALM_OIL', 'Palm Oil', 'oilseeds', 'palm', 'metric_ton', 1.0),
('COTTON', 'Cotton', 'softs', 'fiber', 'pound', 480.0),
('SUGAR', 'Sugar #11', 'softs', 'sugar', 'pound', 2204.62),
('COFFEE', 'Coffee C', 'softs', 'coffee', 'pound', 2204.62),
('CRUDE_OIL', 'WTI Crude Oil', 'energy', 'petroleum', 'barrel', 7.33),
('NATURAL_GAS', 'Natural Gas', 'energy', 'natural_gas', 'mmbtu', NULL),
('ETHANOL', 'Ethanol', 'energy', 'biofuel', 'gallon', 264.172),
('BIODIESEL', 'Biodiesel', 'energy', 'biofuel', 'gallon', 264.172),
('LIVE_CATTLE', 'Live Cattle', 'livestock', 'cattle', 'pound', 2204.62),
('LEAN_HOGS', 'Lean Hogs', 'livestock', 'hogs', 'pound', 2204.62),
('FEEDER_CATTLE', 'Feeder Cattle', 'livestock', 'cattle', 'pound', 2204.62);
"""

DEFAULT_COUNTRIES = """
INSERT OR IGNORE INTO countries (code, name, region, is_major_producer, is_major_exporter, is_major_importer) VALUES
('US', 'United States', 'North America', TRUE, TRUE, FALSE),
('BR', 'Brazil', 'South America', TRUE, TRUE, FALSE),
('AR', 'Argentina', 'South America', TRUE, TRUE, FALSE),
('CN', 'China', 'Asia', TRUE, FALSE, TRUE),
('EU', 'European Union', 'Europe', TRUE, FALSE, TRUE),
('IN', 'India', 'Asia', TRUE, FALSE, TRUE),
('CA', 'Canada', 'North America', TRUE, TRUE, FALSE),
('AU', 'Australia', 'Oceania', TRUE, TRUE, FALSE),
('UA', 'Ukraine', 'Europe', TRUE, TRUE, FALSE),
('RU', 'Russia', 'Europe', TRUE, TRUE, FALSE),
('PY', 'Paraguay', 'South America', TRUE, TRUE, FALSE),
('UY', 'Uruguay', 'South America', TRUE, TRUE, FALSE),
('MX', 'Mexico', 'North America', FALSE, FALSE, TRUE),
('JP', 'Japan', 'Asia', FALSE, FALSE, TRUE),
('KR', 'South Korea', 'Asia', FALSE, FALSE, TRUE),
('EG', 'Egypt', 'Africa', FALSE, FALSE, TRUE),
('ID', 'Indonesia', 'Asia', TRUE, TRUE, TRUE),
('MY', 'Malaysia', 'Asia', TRUE, TRUE, FALSE),
('WORLD', 'World', 'Global', FALSE, FALSE, FALSE);
"""

DEFAULT_SOURCES = """
INSERT OR IGNORE INTO data_sources (code, name, url, data_type, update_frequency, is_active) VALUES
('USDA_FAS', 'USDA Foreign Agricultural Service', 'https://apps.fas.usda.gov', 'api', 'weekly', TRUE),
('USDA_NASS', 'USDA National Agricultural Statistics', 'https://quickstats.nass.usda.gov', 'api', 'weekly', TRUE),
('USDA_ERS', 'USDA Economic Research Service', 'https://www.ers.usda.gov', 'file', 'monthly', TRUE),
('EIA', 'Energy Information Administration', 'https://api.eia.gov', 'api', 'weekly', TRUE),
('CFTC', 'Commodity Futures Trading Commission', 'https://www.cftc.gov', 'api', 'weekly', TRUE),
('CME', 'CME Group', 'https://www.cmegroup.com', 'scrape', 'daily', TRUE),
('CONAB', 'Brazil CONAB', 'https://www.conab.gov.br', 'file', 'monthly', TRUE),
('ABIOVE', 'Brazil ABIOVE', 'https://abiove.org.br', 'scrape', 'monthly', TRUE),
('IMEA', 'Mato Grosso IMEA', 'https://www.imea.com.br', 'scrape', 'weekly', TRUE),
('MAGYP', 'Argentina MAGyP', 'https://datos.agroindustria.gob.ar', 'api', 'monthly', TRUE),
('IBKR', 'Interactive Brokers', 'https://localhost:5000', 'api', 'daily', TRUE),
('TRADESTATION', 'TradeStation', 'https://api.tradestation.com', 'api', 'daily', TRUE);
"""


def create_database(db_path: str = "./data/commodities.db"):
    """Create the database with all tables"""
    import sqlite3
    from pathlib import Path

    # Ensure directory exists
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Create all tables
    cursor.executescript(SQLITE_SCHEMA)

    # Insert default data
    cursor.executescript(DEFAULT_COMMODITIES)
    cursor.executescript(DEFAULT_COUNTRIES)
    cursor.executescript(DEFAULT_SOURCES)

    conn.commit()
    conn.close()

    print(f"Database created: {db_path}")
    return db_path


if __name__ == "__main__":
    create_database()
