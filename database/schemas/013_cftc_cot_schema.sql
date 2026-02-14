-- =============================================================================
-- CFTC Commitments of Traders (COT) Schema
-- =============================================================================
-- Bronze layer tables for weekly positioning data
-- Includes: Managed Money, Commercial, Swap Dealer positions
-- =============================================================================

-- -----------------------------------------------------------------------------
-- BRONZE LAYER - Raw COT data as received from CFTC API
-- -----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS bronze.cftc_cot (
    id SERIAL PRIMARY KEY,
    report_date DATE NOT NULL,
    as_of_date VARCHAR(10),
    commodity VARCHAR(50) NOT NULL,
    exchange VARCHAR(10),
    contract_code VARCHAR(10),

    -- Managed Money / Non-Commercial positions
    mm_long INTEGER,
    mm_short INTEGER,
    mm_spread INTEGER,
    mm_net INTEGER,
    mm_net_change INTEGER,

    -- Producer/Merchant/Processor / Commercial positions
    prod_long INTEGER,
    prod_short INTEGER,
    prod_net INTEGER,

    -- Swap Dealer positions (disaggregated only)
    swap_long INTEGER,
    swap_short INTEGER,
    swap_spread INTEGER,
    swap_net INTEGER,

    -- Other Reportables (disaggregated only)
    other_long INTEGER,
    other_short INTEGER,

    -- Non-reportables
    nonrept_long INTEGER,
    nonrept_short INTEGER,

    -- Open Interest
    open_interest INTEGER,

    -- Metadata
    report_type VARCHAR(20) DEFAULT 'legacy',
    source VARCHAR(50) DEFAULT 'CFTC',
    collected_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),

    UNIQUE(report_date, commodity, report_type)
);

-- -----------------------------------------------------------------------------
-- INDEXES for query performance
-- -----------------------------------------------------------------------------

CREATE INDEX IF NOT EXISTS idx_cftc_cot_report_date
    ON bronze.cftc_cot(report_date);

CREATE INDEX IF NOT EXISTS idx_cftc_cot_commodity
    ON bronze.cftc_cot(commodity);

CREATE INDEX IF NOT EXISTS idx_cftc_cot_commodity_date
    ON bronze.cftc_cot(commodity, report_date);

-- -----------------------------------------------------------------------------
-- SILVER LAYER - Cleaned and standardized views
-- -----------------------------------------------------------------------------

-- Latest positions by commodity
CREATE OR REPLACE VIEW silver.cftc_latest_positions AS
SELECT DISTINCT ON (commodity)
    report_date,
    commodity,
    exchange,
    mm_net,
    mm_net_change,
    prod_net,
    swap_net,
    open_interest,
    mm_net::FLOAT / NULLIF(open_interest, 0) * 100 as mm_net_pct_oi,
    report_type,
    collected_at
FROM bronze.cftc_cot
ORDER BY commodity, report_date DESC;

-- Weekly position changes
CREATE OR REPLACE VIEW silver.cftc_position_history AS
SELECT
    report_date,
    commodity,
    mm_net,
    mm_net_change,
    prod_net,
    open_interest,
    LAG(mm_net, 1) OVER (PARTITION BY commodity ORDER BY report_date) as mm_net_prior,
    LAG(mm_net, 4) OVER (PARTITION BY commodity ORDER BY report_date) as mm_net_4w_ago,
    LAG(mm_net, 52) OVER (PARTITION BY commodity ORDER BY report_date) as mm_net_1y_ago
FROM bronze.cftc_cot
WHERE report_type = 'legacy'
ORDER BY commodity, report_date DESC;

-- -----------------------------------------------------------------------------
-- GOLD LAYER - Analytics-ready views
-- -----------------------------------------------------------------------------

-- MM Net position extremes (for percentile calculations)
CREATE OR REPLACE VIEW gold.cftc_mm_extremes AS
SELECT
    commodity,
    MIN(mm_net) as mm_net_min_1y,
    MAX(mm_net) as mm_net_max_1y,
    AVG(mm_net)::INTEGER as mm_net_avg_1y,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY mm_net)::INTEGER as mm_net_median_1y,
    STDDEV(mm_net)::INTEGER as mm_net_stddev_1y,
    COUNT(*) as weeks_of_data
FROM bronze.cftc_cot
WHERE report_date >= CURRENT_DATE - INTERVAL '1 year'
  AND report_type = 'legacy'
GROUP BY commodity;

-- Position sentiment indicator (current vs historical range)
CREATE OR REPLACE VIEW gold.cftc_sentiment AS
SELECT
    l.commodity,
    l.report_date,
    l.mm_net,
    l.mm_net_change,
    e.mm_net_min_1y,
    e.mm_net_max_1y,
    CASE
        WHEN e.mm_net_max_1y - e.mm_net_min_1y = 0 THEN 50
        ELSE ((l.mm_net - e.mm_net_min_1y)::FLOAT /
              (e.mm_net_max_1y - e.mm_net_min_1y) * 100)::INTEGER
    END as mm_percentile,
    CASE
        WHEN l.mm_net > e.mm_net_avg_1y + e.mm_net_stddev_1y THEN 'BULLISH'
        WHEN l.mm_net < e.mm_net_avg_1y - e.mm_net_stddev_1y THEN 'BEARISH'
        ELSE 'NEUTRAL'
    END as sentiment
FROM silver.cftc_latest_positions l
JOIN gold.cftc_mm_extremes e ON l.commodity = e.commodity;

-- Comments for documentation
COMMENT ON TABLE bronze.cftc_cot IS 'Weekly CFTC Commitments of Traders positioning data';
