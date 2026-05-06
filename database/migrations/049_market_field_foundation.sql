-- Migration 049: Market Field foundation tables
--
-- Date: 2026-05-06
--
-- Purpose: schema for the Market Field proprietary layer. Stores news,
-- per-article classification, facility-graph edge weights, daily
-- per-facility sentiment vectors, and a pluggable per-market topic
-- taxonomy so the same machinery generalises across commodity markets
-- (US oilseed crush first, then EU rapeseed, BR soy, etc.).
--
-- See docs/specs/market_field_spec.md for the full architecture.
--
-- Conventions:
--   - market_id is a TEXT key like 'us_oilseed_crush', 'eu_rapeseed',
--     'br_soy', etc. Each market has its own taxonomy + edge weights;
--     the math (DeGroot-style update) is shared.
--   - facility_id keys match reference.oilseed_crush_facilities.facility_id
--     for the IA pilot. For other markets, point at whatever facility
--     reference table covers them.
--   - bronze stores raw articles; silver stores per-article classification
--     output (one row per article); gold stores the daily sentiment
--     vectors keyed by (market, facility, date).

-- =============================================================================
-- bronze.news_article — raw articles fetched from any source
-- =============================================================================

CREATE TABLE IF NOT EXISTS bronze.news_article (
    id                  BIGSERIAL PRIMARY KEY,
    source_type         TEXT NOT NULL,                  -- 'rss' / 'gdelt' / 'anthropic_search' / 'manual' / 'newsapi'
    source_name         TEXT NOT NULL,                  -- 'biofuelsdigest' / 'agweek' / etc.
    article_url         TEXT NOT NULL,
    article_id_hash     TEXT NOT NULL,                  -- sha256 of normalised url for dedup
    title               TEXT,
    body                TEXT,
    snippet             TEXT,                           -- short preview, e.g. RSS description
    published_at        TIMESTAMP WITH TIME ZONE,       -- best-effort original publish time
    fetched_at          TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    language            TEXT DEFAULT 'en',
    raw_metadata        JSONB,
    UNIQUE (article_id_hash)
);

CREATE INDEX IF NOT EXISTS idx_news_article_published
    ON bronze.news_article (published_at DESC);
CREATE INDEX IF NOT EXISTS idx_news_article_source
    ON bronze.news_article (source_type, source_name);

COMMENT ON TABLE bronze.news_article IS
'Raw news articles ingested for the Market Field. Dedup by article_id_hash (sha256 of normalised URL). Multiple source_types coexist (rss, gdelt, anthropic_search, newsapi, manual).';


-- =============================================================================
-- silver.news_classified — per-article LLM classification output
-- =============================================================================

CREATE TABLE IF NOT EXISTS silver.news_classified (
    id                          BIGSERIAL PRIMARY KEY,
    news_article_id             BIGINT NOT NULL REFERENCES bronze.news_article(id) ON DELETE CASCADE,
    market_id                   TEXT NOT NULL,          -- which market this classification is for
    topic_scores                JSONB NOT NULL,         -- {"weather": {"polarity": -0.4, "intensity": 0.7}, ...}
    locality                    TEXT NOT NULL,          -- 'national' / 'regional' / 'local' / 'facility'
    facility_relevance_keys     TEXT[],                 -- which facility_ids the article touches (empty if national/sectoral only)
    confidence_score            NUMERIC,                -- LLM self-reported, 0-1
    classifier_version          TEXT NOT NULL,          -- 'mf-v1', 'mf-v2', etc. for reproducibility
    llm_model                   TEXT,
    llm_prompt_tokens           INTEGER,
    llm_completion_tokens       INTEGER,
    classified_at               TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    UNIQUE (news_article_id, market_id, classifier_version)
);

CREATE INDEX IF NOT EXISTS idx_news_classified_market_locality
    ON silver.news_classified (market_id, locality);
CREATE INDEX IF NOT EXISTS idx_news_classified_facility_relevance
    ON silver.news_classified USING GIN (facility_relevance_keys);
CREATE INDEX IF NOT EXISTS idx_news_classified_classified_at
    ON silver.news_classified (classified_at DESC);

COMMENT ON TABLE silver.news_classified IS
'Per-article topic + sentiment classification by Claude. One row per (article, market, classifier_version). topic_scores keys must match reference.market_topic_taxonomy for the given market.';


-- =============================================================================
-- reference.market_topic_taxonomy — pluggable taxonomy per market
-- =============================================================================

CREATE TABLE IF NOT EXISTS reference.market_topic_taxonomy (
    id                      BIGSERIAL PRIMARY KEY,
    market_id               TEXT NOT NULL,
    topic_key               TEXT NOT NULL,              -- 'weather', 'soybean_supply', 'veg_oil_demand', etc.
    topic_name              TEXT NOT NULL,
    category                TEXT NOT NULL,              -- 'inputs' / 'outputs' / 'policy' / 'industry'
    weighting_function      TEXT NOT NULL DEFAULT 'flat', -- 'flat' / 'oil_share' / 'meal_share' / custom
    description             TEXT,
    is_active               BOOLEAN NOT NULL DEFAULT TRUE,
    sort_order              INTEGER NOT NULL DEFAULT 0,
    created_at              TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at              TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    UNIQUE (market_id, topic_key)
);

COMMENT ON TABLE reference.market_topic_taxonomy IS
'Per-market topic taxonomy. weighting_function = ''flat'' uses constant 1.0; ''oil_share'' multiplies by current soybean oil_share; ''meal_share'' multiplies by 1-oil_share. Extensible for other markets.';


-- =============================================================================
-- reference.facility_edge_weights — the facility graph
-- =============================================================================

CREATE TABLE IF NOT EXISTS reference.facility_edge_weights (
    id                      BIGSERIAL PRIMARY KEY,
    market_id               TEXT NOT NULL,
    source_facility_id      TEXT NOT NULL,
    target_facility_id      TEXT NOT NULL,
    edge_type               TEXT NOT NULL,              -- 'parent_company' / 'draw_region' / 'industry' / 'trade' / 'weak_random'
    weight                  NUMERIC NOT NULL,           -- 0-1 typically; multiple edge types can coexist
    notes                   TEXT,
    is_active               BOOLEAN NOT NULL DEFAULT TRUE,
    created_at              TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at              TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    UNIQUE (market_id, source_facility_id, target_facility_id, edge_type),
    CHECK (source_facility_id <> target_facility_id),
    CHECK (weight >= 0)
);

CREATE INDEX IF NOT EXISTS idx_facility_edge_source
    ON reference.facility_edge_weights (market_id, source_facility_id);
CREATE INDEX IF NOT EXISTS idx_facility_edge_target
    ON reference.facility_edge_weights (market_id, target_facility_id);

COMMENT ON TABLE reference.facility_edge_weights IS
'Directed facility-to-facility influence weights. Multiple edge_types between the same pair coexist (e.g., same-parent-co AND same-region). Aggregate weight at query time = sum across active edge types. weak_random edges are sampled per-update, persisted for traceability.';


-- =============================================================================
-- reference.news_source — registered news sources
-- =============================================================================

CREATE TABLE IF NOT EXISTS reference.news_source (
    id                          BIGSERIAL PRIMARY KEY,
    source_name                 TEXT NOT NULL UNIQUE,
    source_type                 TEXT NOT NULL,          -- 'rss' / 'gdelt' / 'newsapi' / 'anthropic_search' / 'manual'
    url_template                TEXT,                   -- RSS URL or API endpoint
    polling_frequency_minutes   INTEGER NOT NULL DEFAULT 60,
    default_locality            TEXT NOT NULL DEFAULT 'national',  -- bias hint when classifier uncertain
    default_topic_focus         TEXT[],                 -- e.g., {'veg_oil_demand', 'policy_federal'} for biofuelsdigest
    source_weight               NUMERIC NOT NULL DEFAULT 1.0,  -- trust multiplier on intensity
    last_fetched_at             TIMESTAMP WITH TIME ZONE,
    is_active                   BOOLEAN NOT NULL DEFAULT TRUE,
    created_at                  TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

COMMENT ON TABLE reference.news_source IS
'Registered news feeds. source_weight allows down-weighting noisy sources without removing them.';


-- =============================================================================
-- gold.facility_sentiment_daily — the output of the daily update loop
-- =============================================================================

CREATE TABLE IF NOT EXISTS gold.facility_sentiment_daily (
    id                          BIGSERIAL PRIMARY KEY,
    market_id                   TEXT NOT NULL,
    facility_id                 TEXT NOT NULL,
    as_of_date                  DATE NOT NULL,
    topic_sentiments            JSONB NOT NULL,         -- {"weather": 0.32, "veg_oil_demand": -0.18, ...}
    oil_share                   NUMERIC,                -- snapshot of weighting input on this date
    news_count                  INTEGER NOT NULL DEFAULT 0, -- articles touching this facility on this date
    contribution_breakdown      JSONB,                  -- per topic: {decay: x, exogenous: y, network: z, jump: w}
    classifier_version          TEXT NOT NULL,
    computed_at                 TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    UNIQUE (market_id, facility_id, as_of_date, classifier_version)
);

CREATE INDEX IF NOT EXISTS idx_facility_sentiment_date
    ON gold.facility_sentiment_daily (market_id, as_of_date DESC);
CREATE INDEX IF NOT EXISTS idx_facility_sentiment_facility_date
    ON gold.facility_sentiment_daily (facility_id, as_of_date DESC);

COMMENT ON TABLE gold.facility_sentiment_daily IS
'Daily output: per-facility sentiment vector across topics. topic_sentiments values in [-1, +1]. contribution_breakdown captures the four DeGroot terms (decay, exogenous, network, jump) for diagnostics.';


-- =============================================================================
-- Seed: us_oilseed_crush topic taxonomy (8 topics in 4 categories)
-- =============================================================================

INSERT INTO reference.market_topic_taxonomy
    (market_id, topic_key, topic_name, category, weighting_function, description, sort_order)
VALUES
    ('us_oilseed_crush', 'weather',                'Weather and growing conditions',                                       'inputs',   'flat',        'Drought, freeze, excess moisture, growing-season anomalies in the US Corn/Soy Belt and exporter geographies that affect supply.', 10),
    ('us_oilseed_crush', 'soybean_supply',         'Soybean supply (input)',                                               'inputs',   'flat',        'US production, stocks, Brazil/Argentina harvest pace, trade flows of beans into US crushers.', 20),
    ('us_oilseed_crush', 'veg_oil_demand',         'Veg oil demand (RD/SAF/biofuel/food/industrial)',                      'outputs',  'oil_share',   'Renewable diesel, sustainable aviation fuel, food/industrial veg oil consumption. Scaled by current oil_share.', 30),
    ('us_oilseed_crush', 'meal_livestock_demand',  'Meal and livestock feed demand',                                       'outputs',  'meal_share',  'Hog, poultry, cattle feed demand and meal exports. Scaled by 1 - oil_share.', 40),
    ('us_oilseed_crush', 'policy_federal',         'Policy — Federal (45Z, RFS, CFR, EPA, USDA)',                          'policy',   'flat',        'US federal policy: 45Z biofuel credits, RFS RVO, EPA waivers, USDA programs, CFR rules.', 50),
    ('us_oilseed_crush', 'policy_state_local',     'Policy — State and local',                                             'policy',   'flat',        'State biofuel mandates, LCFS, state air permits, county-level zoning, local incentives.', 60),
    ('us_oilseed_crush', 'policy_industry',        'Policy — Industry mandates and voluntary standards',                   'policy',   'flat',        'CORSIA aviation mandate, ReFuelEU, voluntary corporate sustainability commitments. Non-governmental but binding.', 70),
    ('us_oilseed_crush', 'competitor_activity',    'Competitor activity (capacity, M&A, plant openings/closures)',         'industry', 'flat',        'Other crush facilities — capacity additions, idlings, M&A, ownership changes, technology shifts.', 80)
ON CONFLICT (market_id, topic_key) DO UPDATE SET
    topic_name = EXCLUDED.topic_name,
    category = EXCLUDED.category,
    weighting_function = EXCLUDED.weighting_function,
    description = EXCLUDED.description,
    sort_order = EXCLUDED.sort_order,
    updated_at = NOW();
