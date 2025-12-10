-- HB Weekly Report Additional Database Tables
-- Run with: sqlite3 ./data/rlc_commodities.db < 002_hb_report_tables.sql
-- Date: December 10, 2025

-- =============================================================================
-- WASDE SUPPLY/DEMAND BALANCE SHEETS
-- =============================================================================
CREATE TABLE IF NOT EXISTS wasde_balance_sheet (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    report_date DATE NOT NULL,
    marketing_year VARCHAR(20) NOT NULL,
    commodity VARCHAR(50) NOT NULL,
    region VARCHAR(50) DEFAULT 'US',

    beginning_stocks DECIMAL(12,2),
    production DECIMAL(12,2),
    imports DECIMAL(12,2),
    total_supply DECIMAL(12,2),

    feed_residual DECIMAL(12,2),
    food_seed_industrial DECIMAL(12,2),
    ethanol DECIMAL(12,2),
    crush DECIMAL(12,2),
    exports DECIMAL(12,2),
    total_use DECIMAL(12,2),

    ending_stocks DECIMAL(12,2),
    stocks_to_use DECIMAL(6,3),

    planted_area DECIMAL(10,2),
    harvested_area DECIMAL(10,2),
    yield_per_acre DECIMAL(8,2),

    unit VARCHAR(20) DEFAULT 'million_bushels',
    source VARCHAR(50) DEFAULT 'USDA_WASDE',
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,

    UNIQUE(report_date, marketing_year, commodity, region)
);

-- =============================================================================
-- FUTURES PRICES
-- =============================================================================
CREATE TABLE IF NOT EXISTS futures_prices (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    price_date DATE NOT NULL,
    commodity VARCHAR(50) NOT NULL,
    contract_month VARCHAR(20) NOT NULL,
    exchange VARCHAR(20) DEFAULT 'CME',

    open_price DECIMAL(12,4),
    high_price DECIMAL(12,4),
    low_price DECIMAL(12,4),
    close_price DECIMAL(12,4),
    settle_price DECIMAL(12,4),

    volume INTEGER,
    open_interest INTEGER,

    unit VARCHAR(20),
    source VARCHAR(50) DEFAULT 'CME',
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,

    UNIQUE(price_date, commodity, contract_month, exchange)
);

-- =============================================================================
-- CALENDAR SPREADS
-- =============================================================================
CREATE TABLE IF NOT EXISTS calendar_spreads (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    price_date DATE NOT NULL,
    commodity VARCHAR(50) NOT NULL,
    front_month VARCHAR(20) NOT NULL,
    back_month VARCHAR(20) NOT NULL,
    exchange VARCHAR(20) DEFAULT 'CME',

    spread_value DECIMAL(12,4),
    full_carry DECIMAL(12,4),
    pct_of_carry DECIMAL(6,3),

    source VARCHAR(50) DEFAULT 'CME',
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,

    UNIQUE(price_date, commodity, front_month, back_month)
);

-- =============================================================================
-- EXPORT INSPECTIONS (WEEKLY FGIS)
-- =============================================================================
CREATE TABLE IF NOT EXISTS export_inspections (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    report_date DATE NOT NULL,
    week_ending DATE NOT NULL,
    commodity VARCHAR(50) NOT NULL,
    destination VARCHAR(100),

    weekly_volume DECIMAL(12,2),
    marketing_year_total DECIMAL(12,2),
    year_ago_total DECIMAL(12,2),
    pct_change_yoy DECIMAL(8,2),

    usda_projection DECIMAL(12,2),
    pct_of_projection DECIMAL(8,2),

    unit VARCHAR(20) DEFAULT 'thousand_mt',
    source VARCHAR(50) DEFAULT 'USDA_FGIS',
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,

    UNIQUE(week_ending, commodity, destination)
);

-- =============================================================================
-- EXPORT SALES (WEEKLY FAS)
-- =============================================================================
CREATE TABLE IF NOT EXISTS export_sales (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    report_date DATE NOT NULL,
    week_ending DATE NOT NULL,
    commodity VARCHAR(50) NOT NULL,
    marketing_year VARCHAR(20) NOT NULL,

    net_sales DECIMAL(12,2),
    exports DECIMAL(12,2),
    outstanding_sales DECIMAL(12,2),
    accumulated_exports DECIMAL(12,2),

    china_sales DECIMAL(12,2),
    mexico_sales DECIMAL(12,2),
    japan_sales DECIMAL(12,2),
    other_sales DECIMAL(12,2),

    unit VARCHAR(20) DEFAULT 'thousand_mt',
    source VARCHAR(50) DEFAULT 'USDA_FAS',
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,

    UNIQUE(week_ending, commodity, marketing_year)
);

-- =============================================================================
-- NOPA CRUSH DATA
-- =============================================================================
CREATE TABLE IF NOT EXISTS nopa_crush (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    report_date DATE NOT NULL,
    report_month DATE NOT NULL,

    soybeans_crushed DECIMAL(12,2),
    soybean_oil_stocks DECIMAL(12,2),

    crush_yoy_change DECIMAL(8,2),
    oil_stocks_yoy_change DECIMAL(8,2),

    source VARCHAR(50) DEFAULT 'NOPA',
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,

    UNIQUE(report_month)
);

-- =============================================================================
-- EIA ETHANOL DATA
-- =============================================================================
CREATE TABLE IF NOT EXISTS eia_ethanol (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    report_date DATE NOT NULL,
    week_ending DATE NOT NULL,

    production_mbpd DECIMAL(10,3),
    stocks_million_barrels DECIMAL(10,2),
    imports_mbpd DECIMAL(10,3),

    production_change_wow DECIMAL(8,2),
    stocks_change_wow DECIMAL(8,2),

    implied_corn_grind DECIMAL(12,2),

    source VARCHAR(50) DEFAULT 'EIA',
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,

    UNIQUE(week_ending)
);

-- =============================================================================
-- CFTC COMMITMENTS OF TRADERS
-- =============================================================================
CREATE TABLE IF NOT EXISTS cftc_cot (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    report_date DATE NOT NULL,
    as_of_date DATE NOT NULL,
    commodity VARCHAR(50) NOT NULL,
    exchange VARCHAR(20) DEFAULT 'CME',

    mm_long INTEGER,
    mm_short INTEGER,
    mm_net INTEGER,
    mm_net_change INTEGER,

    comm_long INTEGER,
    comm_short INTEGER,
    comm_net INTEGER,

    open_interest INTEGER,

    source VARCHAR(50) DEFAULT 'CFTC',
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,

    UNIQUE(as_of_date, commodity, exchange)
);

-- =============================================================================
-- INTERNATIONAL FOB PRICES
-- =============================================================================
CREATE TABLE IF NOT EXISTS intl_fob_prices (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    price_date DATE NOT NULL,
    commodity VARCHAR(50) NOT NULL,
    origin VARCHAR(50) NOT NULL,
    port VARCHAR(100),

    fob_price DECIMAL(12,2),
    currency VARCHAR(10) DEFAULT 'USD',
    unit VARCHAR(20),

    premium_to_us DECIMAL(12,2),
    freight_to_china DECIMAL(12,2),

    source VARCHAR(50),
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,

    UNIQUE(price_date, commodity, origin, port)
);

-- =============================================================================
-- WEATHER/CROP CONDITIONS
-- =============================================================================
CREATE TABLE IF NOT EXISTS crop_conditions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    report_date DATE NOT NULL,
    week_ending DATE NOT NULL,
    commodity VARCHAR(50) NOT NULL,
    region VARCHAR(50) DEFAULT 'US',

    planted_pct DECIMAL(5,1),
    emerged_pct DECIMAL(5,1),
    harvested_pct DECIMAL(5,1),

    good_excellent_pct DECIMAL(5,1),
    fair_pct DECIMAL(5,1),
    poor_very_poor_pct DECIMAL(5,1),

    five_year_avg_ge DECIMAL(5,1),
    year_ago_ge DECIMAL(5,1),

    source VARCHAR(50) DEFAULT 'USDA_NASS',
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,

    UNIQUE(week_ending, commodity, region)
);

-- =============================================================================
-- INDEXES FOR PERFORMANCE
-- =============================================================================
CREATE INDEX IF NOT EXISTS idx_wasde_date_commodity ON wasde_balance_sheet(report_date, commodity);
CREATE INDEX IF NOT EXISTS idx_futures_date ON futures_prices(price_date);
CREATE INDEX IF NOT EXISTS idx_futures_commodity ON futures_prices(commodity);
CREATE INDEX IF NOT EXISTS idx_spreads_date ON calendar_spreads(price_date);
CREATE INDEX IF NOT EXISTS idx_inspections_week ON export_inspections(week_ending);
CREATE INDEX IF NOT EXISTS idx_inspections_commodity ON export_inspections(commodity);
CREATE INDEX IF NOT EXISTS idx_sales_week ON export_sales(week_ending);
CREATE INDEX IF NOT EXISTS idx_sales_commodity ON export_sales(commodity);
CREATE INDEX IF NOT EXISTS idx_ethanol_week ON eia_ethanol(week_ending);
CREATE INDEX IF NOT EXISTS idx_cot_date ON cftc_cot(as_of_date);
CREATE INDEX IF NOT EXISTS idx_cot_commodity ON cftc_cot(commodity);
CREATE INDEX IF NOT EXISTS idx_fob_date ON intl_fob_prices(price_date);
CREATE INDEX IF NOT EXISTS idx_fob_origin ON intl_fob_prices(origin);
CREATE INDEX IF NOT EXISTS idx_conditions_week ON crop_conditions(week_ending);
CREATE INDEX IF NOT EXISTS idx_conditions_commodity ON crop_conditions(commodity);

-- Verify tables created
SELECT name FROM sqlite_master WHERE type='table' ORDER BY name;
