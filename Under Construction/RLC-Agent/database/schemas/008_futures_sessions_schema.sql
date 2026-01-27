-- ============================================================================
-- FUTURES SESSIONS DATABASE SCHEMA
-- Bronze layer for session-based futures price data
-- Round Lakes Commodities
-- ============================================================================
--
-- Trading Session Overview:
-- ========================
-- CBOT/CME Grain Futures Trading Hours (Central Time):
--   Overnight Session (Globex): 7:00 PM - 7:45 AM CT (next day)
--   US Day Session: 8:30 AM - 1:20 PM CT
--
-- Session Capture Strategy:
--   1. Capture overnight session OHLC at 7:45 AM CT (8:45 AM ET)
--   2. Capture US session OHLC at 1:30 PM CT (2:30 PM ET)
--   3. Capture daily settlement at 5:00 PM CT (6:00 PM ET)
--
-- ============================================================================

-- ============================================================================
-- BRONZE LAYER - Overnight Session OHLC (Globex)
-- ============================================================================
-- Captures the overnight electronic trading session
-- Typically 7:00 PM previous day to 7:45 AM current day (CT)

CREATE TABLE IF NOT EXISTS bronze.futures_overnight_session (
    id SERIAL PRIMARY KEY,
    trade_date DATE NOT NULL,                    -- Trading date (overnight belongs to next day)
    symbol VARCHAR(20) NOT NULL,                 -- ZC, ZS, ZW, KE, ZM, ZL, CL, NG, etc.
    contract_month VARCHAR(10) NOT NULL,         -- H26, K26, N26, U26, Z26, etc.

    -- Overnight Session OHLC
    overnight_open DECIMAL(10,4),                -- Opening price of overnight session
    overnight_high DECIMAL(10,4),                -- Session high
    overnight_low DECIMAL(10,4),                 -- Session low
    overnight_close DECIMAL(10,4),               -- Close at 7:45 AM CT

    -- Volume (if available)
    overnight_volume INTEGER,                    -- Volume during overnight session

    -- Session timing
    session_start TIMESTAMP WITH TIME ZONE,      -- Actual session start time
    session_end TIMESTAMP WITH TIME ZONE,        -- Actual session end time

    -- Metadata
    exchange VARCHAR(20),                        -- CBOT, CME, NYMEX
    source VARCHAR(50) DEFAULT 'yahoo_finance',
    collected_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    batch_id UUID,

    UNIQUE(trade_date, symbol, contract_month)
);

CREATE INDEX IF NOT EXISTS idx_futures_overnight_date ON bronze.futures_overnight_session(trade_date);
CREATE INDEX IF NOT EXISTS idx_futures_overnight_symbol ON bronze.futures_overnight_session(symbol);
CREATE INDEX IF NOT EXISTS idx_futures_overnight_collected ON bronze.futures_overnight_session(collected_at);

-- ============================================================================
-- BRONZE LAYER - US Day Session OHLC
-- ============================================================================
-- Captures the regular US trading session (pit session hours)
-- Typically 8:30 AM to 1:20 PM CT (grains)

CREATE TABLE IF NOT EXISTS bronze.futures_us_session (
    id SERIAL PRIMARY KEY,
    trade_date DATE NOT NULL,
    symbol VARCHAR(20) NOT NULL,
    contract_month VARCHAR(10) NOT NULL,

    -- US Session OHLC
    us_open DECIMAL(10,4),                       -- Opening price at 8:30 AM CT
    us_high DECIMAL(10,4),                       -- Session high
    us_low DECIMAL(10,4),                        -- Session low
    us_close DECIMAL(10,4),                      -- Close at 1:20 PM CT

    -- Volume
    us_volume INTEGER,

    -- Session timing
    session_start TIMESTAMP WITH TIME ZONE,
    session_end TIMESTAMP WITH TIME ZONE,

    -- Metadata
    exchange VARCHAR(20),
    source VARCHAR(50) DEFAULT 'yahoo_finance',
    collected_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    batch_id UUID,

    UNIQUE(trade_date, symbol, contract_month)
);

CREATE INDEX IF NOT EXISTS idx_futures_us_date ON bronze.futures_us_session(trade_date);
CREATE INDEX IF NOT EXISTS idx_futures_us_symbol ON bronze.futures_us_session(symbol);

-- ============================================================================
-- BRONZE LAYER - Daily Settlement (Official Close)
-- ============================================================================
-- Official daily settlement prices (typically released ~3 PM CT)

CREATE TABLE IF NOT EXISTS bronze.futures_daily_settlement (
    id SERIAL PRIMARY KEY,
    trade_date DATE NOT NULL,
    symbol VARCHAR(20) NOT NULL,
    contract_month VARCHAR(10) NOT NULL,

    -- Daily OHLC (full 23-hour session)
    daily_open DECIMAL(10,4),                    -- First trade of day
    daily_high DECIMAL(10,4),                    -- 23-hour high
    daily_low DECIMAL(10,4),                     -- 23-hour low
    settlement DECIMAL(10,4),                    -- Official settlement price

    -- Prior day comparison
    prior_settlement DECIMAL(10,4),              -- Previous day settlement
    change DECIMAL(10,4),                        -- Settlement change
    change_pct DECIMAL(8,4),                     -- Percentage change

    -- Volume and open interest
    total_volume INTEGER,                        -- Total daily volume
    open_interest INTEGER,                       -- Open interest
    oi_change INTEGER,                           -- Change in open interest

    -- Metadata
    exchange VARCHAR(20),
    source VARCHAR(50) DEFAULT 'yahoo_finance',
    collected_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),

    UNIQUE(trade_date, symbol, contract_month)
);

CREATE INDEX IF NOT EXISTS idx_futures_settle_date ON bronze.futures_daily_settlement(trade_date);
CREATE INDEX IF NOT EXISTS idx_futures_settle_symbol ON bronze.futures_daily_settlement(symbol);

-- ============================================================================
-- SILVER LAYER - Combined Session View (Validation)
-- ============================================================================
-- Combines overnight and US session data with validation against daily OHLC

CREATE OR REPLACE VIEW silver.futures_session_combined AS
SELECT
    COALESCE(ov.trade_date, us.trade_date, ds.trade_date) AS trade_date,
    COALESCE(ov.symbol, us.symbol, ds.symbol) AS symbol,
    COALESCE(ov.contract_month, us.contract_month, ds.contract_month) AS contract_month,

    -- Overnight session
    ov.overnight_open,
    ov.overnight_high,
    ov.overnight_low,
    ov.overnight_close,
    ov.overnight_volume,

    -- US session
    us.us_open,
    us.us_high,
    us.us_low,
    us.us_close,
    us.us_volume,

    -- Daily totals
    ds.daily_open,
    ds.daily_high,
    ds.daily_low,
    ds.settlement,
    ds.prior_settlement,
    ds.change,
    ds.total_volume,
    ds.open_interest,

    -- Derived: Combined session high/low
    GREATEST(COALESCE(ov.overnight_high, 0), COALESCE(us.us_high, 0)) AS combined_high,
    LEAST(
        CASE WHEN ov.overnight_low > 0 THEN ov.overnight_low ELSE 99999 END,
        CASE WHEN us.us_low > 0 THEN us.us_low ELSE 99999 END
    ) AS combined_low,

    -- Validation flags
    CASE
        WHEN ds.daily_high IS NOT NULL AND ov.overnight_high IS NOT NULL AND us.us_high IS NOT NULL
             AND ds.daily_high = GREATEST(ov.overnight_high, us.us_high)
        THEN 'VALID'
        WHEN ds.daily_high IS NULL THEN 'MISSING_DAILY'
        ELSE 'CHECK_HIGH'
    END AS high_validation,

    CASE
        WHEN ds.daily_low IS NOT NULL AND ov.overnight_low IS NOT NULL AND us.us_low IS NOT NULL
             AND ds.daily_low = LEAST(ov.overnight_low, us.us_low)
        THEN 'VALID'
        WHEN ds.daily_low IS NULL THEN 'MISSING_DAILY'
        ELSE 'CHECK_LOW'
    END AS low_validation,

    -- Volume check
    COALESCE(ov.overnight_volume, 0) + COALESCE(us.us_volume, 0) AS session_total_volume,
    ds.total_volume AS reported_total_volume,

    -- Metadata
    COALESCE(ov.exchange, us.exchange, ds.exchange) AS exchange

FROM bronze.futures_overnight_session ov
FULL OUTER JOIN bronze.futures_us_session us
    ON ov.trade_date = us.trade_date
    AND ov.symbol = us.symbol
    AND ov.contract_month = us.contract_month
FULL OUTER JOIN bronze.futures_daily_settlement ds
    ON COALESCE(ov.trade_date, us.trade_date) = ds.trade_date
    AND COALESCE(ov.symbol, us.symbol) = ds.symbol
    AND COALESCE(ov.contract_month, us.contract_month) = ds.contract_month;

-- ============================================================================
-- GOLD LAYER - Validated Futures Daily Summary
-- ============================================================================

CREATE OR REPLACE VIEW gold.futures_daily_validated AS
SELECT
    sc.trade_date,
    sc.symbol,
    sc.contract_month,

    -- Session data
    sc.overnight_open AS globex_open,
    sc.overnight_high AS globex_high,
    sc.overnight_low AS globex_low,
    sc.overnight_close AS globex_close,

    sc.us_open AS rth_open,  -- Regular Trading Hours
    sc.us_high AS rth_high,
    sc.us_low AS rth_low,
    sc.us_close AS rth_close,

    -- Official daily (for validation)
    sc.daily_open,
    sc.daily_high,
    sc.daily_low,
    sc.settlement,
    sc.change,

    -- Volume breakdown
    sc.overnight_volume AS globex_volume,
    sc.us_volume AS rth_volume,
    sc.total_volume,
    sc.open_interest,

    -- Validation
    sc.high_validation,
    sc.low_validation,
    CASE
        WHEN sc.high_validation = 'VALID' AND sc.low_validation = 'VALID'
        THEN 'VALIDATED'
        ELSE 'NEEDS_REVIEW'
    END AS overall_validation,

    sc.exchange

FROM silver.futures_session_combined sc
ORDER BY sc.trade_date DESC, sc.symbol;

-- ============================================================================
-- FUNCTION - Insert session data with automatic validation
-- ============================================================================

CREATE OR REPLACE FUNCTION bronze.insert_overnight_session(
    p_trade_date DATE,
    p_symbol VARCHAR(20),
    p_contract_month VARCHAR(10),
    p_open DECIMAL(10,4),
    p_high DECIMAL(10,4),
    p_low DECIMAL(10,4),
    p_close DECIMAL(10,4),
    p_volume INTEGER DEFAULT NULL,
    p_exchange VARCHAR(20) DEFAULT NULL,
    p_source VARCHAR(50) DEFAULT 'yahoo_finance'
) RETURNS INTEGER AS $$
DECLARE
    v_id INTEGER;
BEGIN
    INSERT INTO bronze.futures_overnight_session (
        trade_date, symbol, contract_month,
        overnight_open, overnight_high, overnight_low, overnight_close,
        overnight_volume, exchange, source, collected_at
    ) VALUES (
        p_trade_date, p_symbol, p_contract_month,
        p_open, p_high, p_low, p_close,
        p_volume, p_exchange, p_source, NOW()
    )
    ON CONFLICT (trade_date, symbol, contract_month)
    DO UPDATE SET
        overnight_open = EXCLUDED.overnight_open,
        overnight_high = EXCLUDED.overnight_high,
        overnight_low = EXCLUDED.overnight_low,
        overnight_close = EXCLUDED.overnight_close,
        overnight_volume = EXCLUDED.overnight_volume,
        collected_at = NOW()
    RETURNING id INTO v_id;

    RETURN v_id;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION bronze.insert_us_session(
    p_trade_date DATE,
    p_symbol VARCHAR(20),
    p_contract_month VARCHAR(10),
    p_open DECIMAL(10,4),
    p_high DECIMAL(10,4),
    p_low DECIMAL(10,4),
    p_close DECIMAL(10,4),
    p_volume INTEGER DEFAULT NULL,
    p_exchange VARCHAR(20) DEFAULT NULL,
    p_source VARCHAR(50) DEFAULT 'yahoo_finance'
) RETURNS INTEGER AS $$
DECLARE
    v_id INTEGER;
BEGIN
    INSERT INTO bronze.futures_us_session (
        trade_date, symbol, contract_month,
        us_open, us_high, us_low, us_close,
        us_volume, exchange, source, collected_at
    ) VALUES (
        p_trade_date, p_symbol, p_contract_month,
        p_open, p_high, p_low, p_close,
        p_volume, p_exchange, p_source, NOW()
    )
    ON CONFLICT (trade_date, symbol, contract_month)
    DO UPDATE SET
        us_open = EXCLUDED.us_open,
        us_high = EXCLUDED.us_high,
        us_low = EXCLUDED.us_low,
        us_close = EXCLUDED.us_close,
        us_volume = EXCLUDED.us_volume,
        collected_at = NOW()
    RETURNING id INTO v_id;

    RETURN v_id;
END;
$$ LANGUAGE plpgsql;

-- ============================================================================
-- GRANTS
-- ============================================================================

GRANT SELECT ON bronze.futures_overnight_session TO PUBLIC;
GRANT SELECT ON bronze.futures_us_session TO PUBLIC;
GRANT SELECT ON bronze.futures_daily_settlement TO PUBLIC;
GRANT SELECT ON silver.futures_session_combined TO PUBLIC;
GRANT SELECT ON gold.futures_daily_validated TO PUBLIC;
