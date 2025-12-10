-- Core price data table
CREATE TABLE commodity_prices (
    id SERIAL PRIMARY KEY,
    commodity VARCHAR(50) NOT NULL,
    price_type VARCHAR(30) NOT NULL,  -- spot, future, basis
    location VARCHAR(100),
    price DECIMAL(10,4) NOT NULL,
    currency VARCHAR(3) DEFAULT 'USD',
    unit VARCHAR(20),  -- $/bushel, $/ton, etc
    date DATE NOT NULL,
    source VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    quality_score DECIMAL(3,2),
    
    INDEX idx_commodity_date (commodity, date),
    INDEX idx_date (date),
    UNIQUE KEY unique_price (commodity, price_type, location, date, source)
);

-- Fundamental data table
CREATE TABLE fundamentals (
    id SERIAL PRIMARY KEY,
    commodity VARCHAR(50) NOT NULL,
    country VARCHAR(50) DEFAULT 'USA',
    season VARCHAR(10),  -- 2024/25
    metric VARCHAR(50),  -- production, consumption, exports, etc
    value DECIMAL(15,2),
    unit VARCHAR(20),
    date_reported DATE,
    source VARCHAR(50),
    
    INDEX idx_commodity_season (commodity, season),
    INDEX idx_metric (metric)
);

-- Trade flows table
CREATE TABLE trade_flows (
    id SERIAL PRIMARY KEY,
    commodity VARCHAR(50),
    hs_code VARCHAR(10),
    flow_type ENUM('export', 'import'),
    origin_country VARCHAR(50),
    destination_country VARCHAR(50),
    quantity DECIMAL(15,2),
    value_usd DECIMAL(15,2),
    month DATE,
    
    INDEX idx_commodity_month (commodity, month)
);

-- Analysis results table
CREATE TABLE analysis_results (
    id SERIAL PRIMARY KEY,
    analysis_type VARCHAR(50),
    commodity VARCHAR(50),
    analysis_date DATE,
    results JSON,
    confidence_score DECIMAL(3,2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    INDEX idx_type_date (analysis_type, analysis_date)
);