-- =============================================================================
-- RLC Commodities Database Schema - Wheat Tender Monitoring
-- Version: 1.0.0
-- =============================================================================
--
-- WHEAT TENDER MONITORING
-- -----------------------
-- This migration adds tables for tracking international wheat tender
-- announcements and results from major importing countries.
--
-- Key importing countries tracked:
-- - Egypt (Mostakbal Misr, formerly GASC)
-- - Algeria (OAIC)
-- - Saudi Arabia (SAGO)
-- - Iraq (Grain Board of Iraq)
-- - Tunisia, Morocco, Jordan, Bangladesh, Indonesia, Philippines, Pakistan
--
-- Data is collected from news sources (Agricensus, AgroChart, Reuters, etc.)
-- and parsed using NLP/regex to extract tender details.
--
-- =============================================================================

-- =============================================================================
-- BRONZE LAYER: Raw tender data as captured from sources
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Wheat Tender Raw: Source-faithful tender records
-- -----------------------------------------------------------------------------
CREATE TABLE bronze.wheat_tender_raw (
    id BIGSERIAL PRIMARY KEY,

    -- Natural key components
    source_name VARCHAR(100) NOT NULL,       -- 'Agricensus', 'AgroChart', 'Reuters'
    source_article_id VARCHAR(200),          -- Unique article/item identifier
    captured_at TIMESTAMPTZ NOT NULL,        -- When we captured this article

    -- Article metadata
    headline TEXT,
    article_url TEXT,
    article_date TIMESTAMPTZ,
    raw_text TEXT,                           -- Full article text if available

    -- Parsed fields (raw values as extracted)
    country_raw VARCHAR(100),
    agency_raw VARCHAR(200),
    commodity_raw VARCHAR(100),
    wheat_type_raw VARCHAR(100),             -- 'milling', 'feed', 'durum'
    volume_raw VARCHAR(100),                 -- '60,000 mt', '480,000 tonnes'
    volume_value NUMERIC(15, 2),             -- Parsed numeric
    volume_unit VARCHAR(20),                 -- 'MT', 'tonnes'
    price_raw VARCHAR(100),                  -- '$275.50/mt', 'USD 280 per tonne'
    price_value NUMERIC(12, 4),              -- Parsed numeric
    price_type VARCHAR(20),                  -- 'FOB', 'C&F', 'CIF'
    origins_raw TEXT,                        -- 'Russia, France, Romania'
    suppliers_raw TEXT,                      -- 'Cargill, Viterra, Louis Dreyfus'
    shipment_period_raw VARCHAR(200),        -- 'Jan 15-31, 2025'
    freight_rate_raw VARCHAR(100),
    payment_terms_raw VARCHAR(200),

    -- Tender lifecycle
    tender_type VARCHAR(50),                 -- 'announcement', 'result', 'cancelled'
    tender_date DATE,                        -- Announcement or result date
    tender_deadline TIMESTAMPTZ,             -- Bid deadline if announcement

    -- Processing status
    parse_confidence NUMERIC(5, 4),          -- 0.0 to 1.0 confidence score
    is_processed BOOLEAN DEFAULT FALSE,
    processed_at TIMESTAMPTZ,

    -- Tracking
    ingest_run_id UUID REFERENCES audit.ingest_run(id),

    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),

    -- Natural key for idempotent upserts
    UNIQUE (source_name, source_article_id)
);

CREATE INDEX idx_wheat_tender_raw_date ON bronze.wheat_tender_raw(tender_date DESC);
CREATE INDEX idx_wheat_tender_raw_country ON bronze.wheat_tender_raw(country_raw);
CREATE INDEX idx_wheat_tender_raw_captured ON bronze.wheat_tender_raw(captured_at DESC);
CREATE INDEX idx_wheat_tender_raw_source ON bronze.wheat_tender_raw(source_name);
CREATE INDEX idx_wheat_tender_raw_processed ON bronze.wheat_tender_raw(is_processed);

COMMENT ON TABLE bronze.wheat_tender_raw IS 'Raw wheat tender data from news sources. Preserves original article text and parsed values.';

-- -----------------------------------------------------------------------------
-- Tender News Source: Registry of news sources we monitor
-- -----------------------------------------------------------------------------
CREATE TABLE bronze.tender_news_source (
    id SERIAL PRIMARY KEY,

    code VARCHAR(50) NOT NULL UNIQUE,
    name VARCHAR(200) NOT NULL,
    base_url VARCHAR(500),
    feed_url VARCHAR(500),                   -- RSS/Atom feed URL if available
    feed_type VARCHAR(20),                   -- 'rss', 'atom', 'html', 'api'

    -- Scraping configuration
    scrape_interval_minutes INT DEFAULT 60,
    requires_auth BOOLEAN DEFAULT FALSE,
    is_active BOOLEAN DEFAULT TRUE,

    -- Selector configuration for HTML scraping (stored as JSON)
    scrape_config JSONB DEFAULT '{}',

    -- Stats
    last_scraped_at TIMESTAMPTZ,
    last_success_at TIMESTAMPTZ,
    consecutive_failures INT DEFAULT 0,
    total_articles_found INT DEFAULT 0,

    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Seed tender news sources
INSERT INTO bronze.tender_news_source (code, name, base_url, feed_url, feed_type, scrape_config) VALUES
    ('AGRICENSUS', 'Agricensus', 'https://www.agricensus.com', 'https://www.agricensus.com/Article/latest', 'html',
     '{"article_selector": "a[href*=''/Article/'']", "headline_selector": ".article-title", "keywords": ["wheat", "tender", "gasc", "oaic", "sago", "egypt", "algeria"]}'),
    ('AGROCHART', 'AgroChart', 'https://www.agrochart.com', 'https://www.agrochart.com/en/news/', 'html',
     '{"article_selector": ".news-item a", "headline_selector": ".news-title", "keywords": ["wheat", "tender", "purchase"]}'),
    ('REUTERS_AG', 'Reuters Agriculture', 'https://www.reuters.com', NULL, 'api',
     '{"requires_subscription": true, "keywords": ["wheat tender", "gasc wheat", "egypt wheat"]}'),
    ('BLOOMBERG_CMDTY', 'Bloomberg Commodities', 'https://www.bloomberg.com', NULL, 'api',
     '{"requires_subscription": true, "keywords": ["wheat tender", "grain tender"]}'
    )
ON CONFLICT (code) DO UPDATE SET
    name = EXCLUDED.name,
    base_url = EXCLUDED.base_url,
    scrape_config = EXCLUDED.scrape_config,
    updated_at = NOW();

-- =============================================================================
-- SILVER LAYER: Standardized tender data
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Wheat Tender: Clean, standardized tender records
-- -----------------------------------------------------------------------------
CREATE TABLE silver.wheat_tender (
    id SERIAL PRIMARY KEY,

    -- Source reference
    raw_id BIGINT REFERENCES bronze.wheat_tender_raw(id),

    -- Tender identification
    tender_id VARCHAR(100),                  -- Agency-assigned tender ID if known
    tender_type VARCHAR(50) NOT NULL,        -- 'announcement', 'result', 'amendment', 'cancelled'

    -- Timing
    announcement_date DATE,
    result_date DATE,
    deadline_datetime TIMESTAMPTZ,

    -- Buyer details
    country_code VARCHAR(10) NOT NULL,       -- ISO alpha-2
    country_name VARCHAR(100) NOT NULL,
    agency_code VARCHAR(50),
    agency_name VARCHAR(200),

    -- Commodity details
    commodity_code VARCHAR(30) DEFAULT 'WHEAT' REFERENCES public.commodity(code),
    wheat_class VARCHAR(50),                 -- 'milling', 'feed', 'durum', 'mixed'
    wheat_spec TEXT,                         -- Quality specifications

    -- Volume
    volume_mt NUMERIC(15, 2),                -- Standardized to metric tons
    num_cargoes INT,                         -- Number of cargoes if specified
    cargo_size_mt NUMERIC(12, 2),            -- Size per cargo if specified

    -- Price
    price_usd_mt NUMERIC(12, 4),             -- Price in USD per metric ton
    price_type VARCHAR(20),                  -- 'FOB', 'C&F', 'CIF'
    freight_usd_mt NUMERIC(10, 4),           -- Freight rate if separate

    -- Origins and suppliers
    origins TEXT[],                          -- Array of origin countries
    suppliers TEXT[],                        -- Array of awarded suppliers

    -- Shipment
    shipment_start DATE,
    shipment_end DATE,
    shipment_port VARCHAR(200),

    -- Payment and terms
    payment_terms VARCHAR(200),
    lc_days INT,                             -- Letter of credit days

    -- Metadata
    source_urls TEXT[],                      -- Array of source article URLs
    notes TEXT,

    -- Data quality
    data_quality_score NUMERIC(5, 4),        -- 0.0 to 1.0
    is_verified BOOLEAN DEFAULT FALSE,
    verified_at TIMESTAMPTZ,
    verified_by VARCHAR(100),

    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),

    -- Unique constraint for tender results
    UNIQUE NULLS NOT EQUAL (country_code, agency_code, result_date, volume_mt)
);

CREATE INDEX idx_wheat_tender_country ON silver.wheat_tender(country_code);
CREATE INDEX idx_wheat_tender_agency ON silver.wheat_tender(agency_code);
CREATE INDEX idx_wheat_tender_result_date ON silver.wheat_tender(result_date DESC);
CREATE INDEX idx_wheat_tender_announcement ON silver.wheat_tender(announcement_date DESC);
CREATE INDEX idx_wheat_tender_type ON silver.wheat_tender(tender_type);
CREATE INDEX idx_wheat_tender_origins ON silver.wheat_tender USING GIN(origins);

COMMENT ON TABLE silver.wheat_tender IS 'Standardized wheat tender data. Volume in MT, prices in USD/MT.';

-- =============================================================================
-- REFERENCE DATA: Importing agencies
-- =============================================================================

CREATE TABLE public.tender_agency (
    id SERIAL PRIMARY KEY,

    code VARCHAR(50) NOT NULL UNIQUE,
    name VARCHAR(200) NOT NULL,
    full_name VARCHAR(500),
    country_code VARCHAR(10) NOT NULL REFERENCES public.location(code),

    -- Agency details
    agency_type VARCHAR(50),                 -- 'government', 'state_enterprise', 'military'
    parent_ministry VARCHAR(200),

    -- Tender patterns
    typical_volume_mt_min NUMERIC(12, 2),
    typical_volume_mt_max NUMERIC(12, 2),
    typical_frequency VARCHAR(100),          -- 'Every 10-12 days', 'Monthly'
    active_months VARCHAR(100),              -- 'Jun-Feb', 'Year-round'

    -- Commodities purchased
    commodities_purchased TEXT[],

    -- Contact/URL
    website VARCHAR(500),

    -- Status
    is_active BOOLEAN DEFAULT TRUE,
    notes TEXT,

    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Seed importing agencies
INSERT INTO public.tender_agency (code, name, full_name, country_code, agency_type, typical_volume_mt_min, typical_volume_mt_max, typical_frequency, commodities_purchased) VALUES
    ('MOSTAKBAL_MISR', 'Mostakbal Misr', 'Mostakbal Misr for Sustainable Development', 'EG', 'military', 50000, 60000, 'Every 10-12 days', ARRAY['wheat', 'vegetable_oil', 'sugar']),
    ('GASC', 'GASC', 'General Authority for Supply Commodities', 'EG', 'government', 50000, 60000, 'Every 10-12 days (legacy)', ARRAY['wheat']),
    ('OAIC', 'OAIC', 'Office Algérien Interprofessionnel des Céréales', 'DZ', 'government', 400000, 600000, 'Monthly', ARRAY['wheat', 'durum', 'barley']),
    ('SAGO', 'SAGO', 'Saudi Grains Organization', 'SA', 'government', 500000, 600000, 'Periodic', ARRAY['wheat', 'barley']),
    ('GBI', 'Grain Board of Iraq', 'Grain Board of Iraq', 'IQ', 'government', 100000, 300000, 'Periodic', ARRAY['wheat', 'rice']),
    ('OC_TUNISIA', 'OC Tunisia', 'Office des Céréales', 'TN', 'government', 50000, 100000, 'Monthly', ARRAY['wheat', 'barley']),
    ('ONICL', 'ONICL', 'Office National Interprofessionnel des Céréales et des Légumineuses', 'MA', 'government', 50000, 200000, 'Periodic', ARRAY['wheat', 'durum']),
    ('MIT_JORDAN', 'MIT Jordan', 'Ministry of Industry and Trade', 'JO', 'government', 50000, 100000, 'Regular', ARRAY['wheat', 'barley']),
    ('DGF_BANGLADESH', 'DGF Bangladesh', 'Directorate General of Food', 'BD', 'government', 50000, 150000, 'Periodic', ARRAY['wheat', 'rice']),
    ('BULOG', 'BULOG', 'Badan Urusan Logistik', 'ID', 'state_enterprise', 100000, 500000, 'Periodic', ARRAY['wheat', 'rice']),
    ('NFA_PH', 'NFA Philippines', 'National Food Authority', 'PH', 'government', 50000, 150000, 'Periodic', ARRAY['wheat', 'rice']),
    ('PASSCO', 'PASSCO', 'Pakistan Agricultural Storage & Services Corporation', 'PK', 'government', 100000, 300000, 'Periodic', ARRAY['wheat'])
ON CONFLICT (code) DO UPDATE SET
    name = EXCLUDED.name,
    full_name = EXCLUDED.full_name,
    typical_volume_mt_min = EXCLUDED.typical_volume_mt_min,
    typical_volume_mt_max = EXCLUDED.typical_volume_mt_max,
    commodities_purchased = EXCLUDED.commodities_purchased,
    updated_at = NOW();

-- Add missing countries to location table
INSERT INTO public.location (code, name, location_type, iso_alpha2, iso_alpha3) VALUES
    ('DZ', 'Algeria', 'COUNTRY', 'DZ', 'DZA'),
    ('SA', 'Saudi Arabia', 'COUNTRY', 'SA', 'SAU'),
    ('IQ', 'Iraq', 'COUNTRY', 'IQ', 'IRQ'),
    ('TN', 'Tunisia', 'COUNTRY', 'TN', 'TUN'),
    ('MA', 'Morocco', 'COUNTRY', 'MA', 'MAR'),
    ('JO', 'Jordan', 'COUNTRY', 'JO', 'JOR'),
    ('BD', 'Bangladesh', 'COUNTRY', 'BD', 'BGD'),
    ('PK', 'Pakistan', 'COUNTRY', 'PK', 'PAK')
ON CONFLICT (code) DO NOTHING;

-- =============================================================================
-- GOLD LAYER: Business-ready views
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Recent Tenders View: Latest tender activity
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW gold.recent_wheat_tenders AS
SELECT
    wt.id,
    wt.tender_type,
    COALESCE(wt.result_date, wt.announcement_date) AS tender_date,
    wt.country_name,
    wt.agency_name,
    wt.wheat_class,
    wt.volume_mt,
    wt.num_cargoes,
    wt.price_usd_mt,
    wt.price_type,
    array_to_string(wt.origins, ', ') AS origins,
    array_to_string(wt.suppliers, ', ') AS suppliers,
    wt.shipment_start,
    wt.shipment_end,
    wt.data_quality_score,
    wt.is_verified,
    wt.created_at
FROM silver.wheat_tender wt
WHERE wt.result_date >= CURRENT_DATE - INTERVAL '90 days'
   OR wt.announcement_date >= CURRENT_DATE - INTERVAL '30 days'
ORDER BY COALESCE(wt.result_date, wt.announcement_date) DESC;

COMMENT ON VIEW gold.recent_wheat_tenders IS 'Recent wheat tender activity - results from last 90 days, announcements from last 30 days';

-- -----------------------------------------------------------------------------
-- Tender Summary by Country: Aggregate statistics
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW gold.wheat_tender_by_country AS
SELECT
    wt.country_code,
    wt.country_name,
    COUNT(*) AS tender_count,
    SUM(wt.volume_mt) AS total_volume_mt,
    AVG(wt.volume_mt) AS avg_volume_mt,
    AVG(wt.price_usd_mt) AS avg_price_usd_mt,
    MIN(wt.price_usd_mt) AS min_price_usd_mt,
    MAX(wt.price_usd_mt) AS max_price_usd_mt,
    MIN(wt.result_date) AS first_tender_date,
    MAX(wt.result_date) AS last_tender_date
FROM silver.wheat_tender wt
WHERE wt.tender_type = 'result'
  AND wt.result_date >= CURRENT_DATE - INTERVAL '365 days'
GROUP BY wt.country_code, wt.country_name
ORDER BY total_volume_mt DESC;

COMMENT ON VIEW gold.wheat_tender_by_country IS 'Wheat tender statistics by importing country (last 12 months)';

-- -----------------------------------------------------------------------------
-- Origin Market Share: Track which origins are winning tenders
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW gold.wheat_tender_origin_share AS
SELECT
    origin,
    COUNT(*) AS tender_count,
    SUM(wt.volume_mt) AS total_volume_mt,
    ROUND(100.0 * SUM(wt.volume_mt) /
        NULLIF(SUM(SUM(wt.volume_mt)) OVER (), 0), 2) AS volume_share_pct,
    AVG(wt.price_usd_mt) AS avg_price_usd_mt
FROM silver.wheat_tender wt
CROSS JOIN LATERAL UNNEST(wt.origins) AS origin
WHERE wt.tender_type = 'result'
  AND wt.result_date >= CURRENT_DATE - INTERVAL '365 days'
GROUP BY origin
ORDER BY total_volume_mt DESC;

COMMENT ON VIEW gold.wheat_tender_origin_share IS 'Market share by origin country in wheat tenders (last 12 months)';

-- =============================================================================
-- ALERT CONFIGURATION
-- =============================================================================

CREATE TABLE public.tender_alert_config (
    id SERIAL PRIMARY KEY,

    alert_name VARCHAR(100) NOT NULL UNIQUE,
    description TEXT,

    -- Trigger conditions
    country_codes TEXT[],                    -- Trigger for specific countries (NULL = all)
    agency_codes TEXT[],                     -- Trigger for specific agencies
    volume_threshold_mt NUMERIC(12, 2),      -- Minimum volume to trigger

    -- Notification channels
    notify_email BOOLEAN DEFAULT FALSE,
    notify_slack BOOLEAN DEFAULT FALSE,
    notify_sms BOOLEAN DEFAULT FALSE,

    -- Recipients (JSON array)
    email_recipients TEXT[],
    slack_channels TEXT[],
    sms_recipients TEXT[],

    -- Status
    is_active BOOLEAN DEFAULT TRUE,

    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Seed default alerts
INSERT INTO public.tender_alert_config (alert_name, description, country_codes, volume_threshold_mt, notify_email, notify_slack) VALUES
    ('egypt_tender', 'Egypt wheat tender alerts', ARRAY['EG'], 50000, TRUE, TRUE),
    ('algeria_tender', 'Algeria wheat tender alerts', ARRAY['DZ'], 200000, TRUE, FALSE),
    ('large_tender', 'Any tender over 500K MT', NULL, 500000, TRUE, TRUE),
    ('all_tenders', 'All wheat tender activity', NULL, NULL, TRUE, FALSE)
ON CONFLICT (alert_name) DO NOTHING;

-- Alert history table
CREATE TABLE audit.tender_alert_history (
    id BIGSERIAL PRIMARY KEY,

    alert_config_id INT REFERENCES public.tender_alert_config(id),
    tender_id INT REFERENCES silver.wheat_tender(id),

    -- Alert details
    alert_triggered_at TIMESTAMPTZ DEFAULT NOW(),
    alert_message TEXT,

    -- Notification status
    email_sent BOOLEAN DEFAULT FALSE,
    email_sent_at TIMESTAMPTZ,
    slack_sent BOOLEAN DEFAULT FALSE,
    slack_sent_at TIMESTAMPTZ,
    sms_sent BOOLEAN DEFAULT FALSE,
    sms_sent_at TIMESTAMPTZ,

    -- Error tracking
    notification_errors JSONB
);

CREATE INDEX idx_tender_alert_history_tender ON audit.tender_alert_history(tender_id);
CREATE INDEX idx_tender_alert_history_triggered ON audit.tender_alert_history(alert_triggered_at DESC);

-- =============================================================================
-- DATA SOURCE REGISTRATION
-- =============================================================================

INSERT INTO public.data_source (code, name, description, api_type, update_frequency) VALUES
    ('WHEAT_TENDER', 'Wheat Tender Monitor', 'International wheat tender announcements and results', 'SCRAPE', 'HOURLY')
ON CONFLICT (code) DO UPDATE SET
    name = EXCLUDED.name,
    description = EXCLUDED.description,
    updated_at = NOW();

-- =============================================================================
-- END OF WHEAT TENDER MIGRATION
-- =============================================================================
